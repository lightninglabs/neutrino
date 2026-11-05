package neutrino

// This file holds the header executor's serial header sync: the path a node
// uses to follow the tip from inv and headers messages, including reorgs, and
// the only path on regtest and simnet.

import (
	"container/list"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/lightninglabs/neutrino/headersync"
)

// handleInvMsg handles inv messages from all peers. We examine the inventory
// advertised by the remote peer and act accordingly.
func (b *blockManager) handleInvMsg(imsg *invMsg) {
	// Attempt to find the final block in the inventory list. There may not
	// be one.
	lastBlock := -1
	invVects := imsg.inv.InvList
	for i := len(invVects) - 1; i >= 0; i-- {
		if invVects[i].Type == wire.InvTypeBlock {
			lastBlock = i
			break
		}
	}

	// If this inv contains a block announcement, and this isn't coming
	// from our current sync peer or we're current, then update the last
	// announced block for this peer. We'll use this information later to
	// update the heights of peers based on blocks we've accepted that they
	// previously announced.
	if lastBlock != -1 &&
		(imsg.peer != b.SyncPeer() || b.BlockHeadersSynced()) {

		imsg.peer.UpdateLastAnnouncedBlock(&invVects[lastBlock].Hash)
	}

	// Ignore invs from peers that aren't the sync if we are not current.
	// Helps prevent dealing with orphans.
	if imsg.peer != b.SyncPeer() && !b.BlockHeadersSynced() {
		return
	}

	// If our chain is current and a peer announces a block we already know
	// of, then update their current block height.
	if lastBlock != -1 && b.BlockHeadersSynced() {
		height, err := b.cfg.BlockHeaders.HeightFromHash(
			&invVects[lastBlock].Hash,
		)
		if err == nil {
			imsg.peer.UpdateLastBlockHeight(int32(height))
		}
	}

	// Add blocks to the cache of known inventory for the peer.
	for _, iv := range invVects {
		if iv.Type == wire.InvTypeBlock {
			imsg.peer.AddKnownInventory(iv)
		}
	}

	// If this is the sync peer or we're current, get the headers for the
	// announced blocks and update the last announced block.
	if lastBlock != -1 &&
		(imsg.peer == b.SyncPeer() || b.BlockHeadersSynced()) {

		lastEl := b.driver().Tip()
		var lastHash chainhash.Hash
		if lastEl != nil {
			lastHash = lastEl.Header.BlockHash()
		}

		// Only send getheaders if we don't already know about the last
		// block hash being announced.
		if lastHash != invVects[lastBlock].Hash && lastEl != nil &&
			b.lastRequested != invVects[lastBlock].Hash {

			// Make a locator starting from the latest known header
			// we've processed.
			locator := make(blockchain.BlockLocator, 0,
				wire.MaxBlockLocatorsPerMsg)
			locator = append(locator, &lastHash)

			// Add locator from the database as backup.
			store := b.cfg.BlockHeaders
			knownLocator, err := store.LatestBlockLocator()
			if err == nil {
				locator = append(locator, knownLocator...)
			}

			// Get headers based on locator.
			_, startHeight, err := b.cfg.BlockHeaders.ChainTip()
			if err != nil {
				log.Warnf(
					"Failed to get block header chain "+
						"tip "+
						"before requesting headers "+
						"from peer %s: "+
						"%s",
					imsg.peer.Addr(),
					err,
				)
				return
			}

			err = b.requestHeaders(
				imsg.peer, locator, &invVects[lastBlock].Hash,
				startHeight,
			)
			if err != nil {
				log.Warnf("Failed to send getheaders message "+
					"to peer %s: %s", imsg.peer.Addr(), err)
				return
			}
			b.lastRequested = invVects[lastBlock].Hash
		}
	}
}

// handleHeadersMsg handles headers messages from all peers.
func (b *blockManager) handleHeadersMsg(hmsg *headersMsg) {
	b.handleHeadersMsgWithPeers(list.New(), hmsg)
}

func (b *blockManager) handleHeadersMsgWithPeers(peers *list.List,
	hmsg *headersMsg) {

	// The driver decides what the headers answer: an anchor discovery
	// request, a range request, or the tip. We perform the effects it
	// returns. An empty message that answered no request depends on the
	// serial deadline, which only we can see, so it comes back to us as a
	// NoProgressReply effect.
	b.handleDriverEvent(peers, headersync.HeadersReceived{
		PeerID:        hmsg.peer.Addr(),
		PeerHeight:    hmsg.peer.LastBlock(),
		Headers:       hmsg.headers.Headers,
		HeadersSynced: b.BlockHeadersSynced(),
	}, hmsg.peer)
}

// finishSerialHeaderFollowup advances the peer request after durable progress,
// as the driver asked with ContinueSerial.
func (b *blockManager) finishSerialHeaderFollowup(peers *list.List,
	peer *ServerPeer, effect headersync.ContinueSerial) {

	finalHash := &effect.FinalHash
	finalHeight := effect.FinalHeight

	// The batch made progress, so it answered the sync peer's request.
	// A follow-up request below re-arms the deadline.
	if peer == b.SyncPeer() {
		b.disarmHeaderSyncTimeout()
	}

	// If not current, request the next batch of headers starting from the
	// latest known header and ending with the next checkpoint.
	if b.cfg.ChainParams.Net == chaincfg.SimNetParams.Net ||
		!b.BlockHeadersSynced() {

		nextHash := effect.StopHash

		// Ask the sync peer, not whoever sent this batch. There is a
		// single serial deadline and it only acts for the sync peer,
		// so arming it for another peer cancelled the sync peer's
		// deadline and left that one with nothing in flight.
		target := b.SyncPeer()
		if target == nil {
			target = peer
		}
		if peerDisconnected(target) {
			b.startSync(peers)
			return
		}

		// The batch's last hash is a fine locator for its sender. The
		// sync peer may not know that block, and peers answer an
		// unknown locator from genesis: an all-known reply that
		// leaves the deadline armed until the honest sync peer is
		// quarantined. Give it our full locator instead.
		locator := blockchain.BlockLocator([]*chainhash.Hash{finalHash})
		if target != peer {
			var err error
			locator, err = b.cfg.BlockHeaders.LatestBlockLocator()
			if err != nil {
				log.Errorf("Unable to build block locator: %v",
					err)
				return
			}
		}
		err := b.requestHeaders(
			target, locator, &nextHash, uint32(finalHeight),
		)
		if err != nil {
			log.Warnf("Failed to send getheaders message to "+
				"peer %s: %s", target.Addr(), err)

			// Nothing is in flight now, so move on to another
			// sync peer as startSync does when its send fails,
			// rather than wait for a deadline that was never
			// armed.
			removeSyncCandidate(peers, target)
			if b.syncPeer.Load() == target {
				b.setSyncPeer(nil)
			}
			b.startSync(peers)
		}
	}
}

// submitSerialExtension sends an extending batch to the chain store actor,
// without waiting for the write.
func (b *blockManager) submitSerialExtension(sp *ServerPeer,
	effect headersync.CommitExtension) {

	prepared := effect.Extension
	b.pendingHeaderCommit = &pendingChainHeaderCommit{
		serial: &prepared, peer: sp,
	}
	b.chainStore.TryAskFunc(b.quitCtx, chainstore.BlockCommit{
		ExpectedParent: prepared.Parent,
		Headers:        prepared.Headers,
	}, b.postChainResult)
}

// submitSerialHeaderReorg sends a validated fork as one actor mutation, so that
// no filter commit can land between the rollback and the replacement branch.
// The actor also decides whether the branch has enough work, so nothing here
// may act on the branch until its verdict arrives. A branch that failed a
// checkpoint only has its work compared.
func (b *blockManager) submitSerialHeaderReorg(sp *ServerPeer,
	effect headersync.CommitReorg) {

	prepared := effect.Reorg
	if effect.Mismatch != 0 {
		b.pendingHeaderCommit = &pendingChainHeaderCommit{
			reorg:         &prepared,
			reorgMismatch: effect.Mismatch,
			peer:          sp,
		}
		b.chainStore.TryAskFunc(b.quitCtx, chainstore.Reorg{
			ExpectedTip: prepared.ExpectedTip,
			ForkHeight:  uint32(prepared.Fork.Height),
			Work:        effect.Work,
			CompareOnly: true,
		}, b.postChainResult)

		return
	}

	b.pendingHeaderCommit = &pendingChainHeaderCommit{
		reorg:      &prepared,
		reorgNodes: effect.Nodes,
		peer:       sp,
	}
	b.chainStore.TryAskFunc(b.quitCtx, chainstore.Reorg{
		ExpectedTip: prepared.ExpectedTip,
		ForkHeight:  uint32(prepared.Fork.Height),
		Headers:     effect.Headers,
		Work:        effect.Work,
	}, b.postChainResult)
}

// submitCheckpointRollback rolls the chain back to the last verified checkpoint
// after a header failed a checkpoint. The peer that sent it is dropped once the
// rollback lands.
func (b *blockManager) submitCheckpointRollback(sp *ServerPeer,
	effect headersync.CheckpointRollback) {

	node := effect.Node
	command := chainstore.Rollback{
		ExpectedTip: effect.ExpectedTip,
		Height:      uint32(node.Height),
	}
	b.pendingHeaderCommit = &pendingChainHeaderCommit{
		checkpoint: &node, peer: sp,
	}
	b.chainStore.TryAskFunc(b.quitCtx, command, b.postChainResult)
}

// submitRangeCommit sends a prepared batch of ranges to the chain store
// actor, without waiting for the write.
func (b *blockManager) submitRangeCommit(prepared headersync.PreparedCommit) {
	b.pendingHeaderCommit = &pendingChainHeaderCommit{
		ranges: &prepared,
	}
	b.chainStore.TryAskFunc(b.quitCtx, chainstore.BlockCommit{
		ExpectedParent: prepared.ExpectedParent,
		Headers:        prepared.Headers,
	}, b.postChainResult)
}
