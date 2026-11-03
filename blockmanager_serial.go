package neutrino

// This file holds serial header sync: the path a node uses to follow the tip
// from inv and headers messages, including reorgs, and the only path on regtest
// and simnet.

import (
	"container/list"
	"errors"
	"fmt"
	"math/big"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
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

		lastEl := b.headerList.Back()
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

	msg := hmsg.headers
	numHeaders := len(msg.Headers)

	if b.handleHeaderSyncAnchorHeaders(peers, hmsg) {
		return
	}

	if b.handleHeaderSyncRangeHeaders(peers, hmsg) {
		return
	}

	// Nothing to do for an empty headers message, beyond settling the
	// sync peer's outstanding request.
	if numHeaders == 0 {
		b.handleNoProgressReply(peers, hmsg.peer)
		return
	}

	// Explicitly check that each header in msg.Headers builds off of the
	// previous one. This is a quick sanity check to avoid doing the more
	// expensive checks below if we know the headers are invalid.
	if !headersync.HeadersConnected(msg.Headers) {
		log.Warnf("Headers received from peer don't connect")
		hmsg.peer.Disconnect()
		return
	}

	// A batch that extends the validation chain is committed through the
	// chain store actor. What remains here are headers that do not connect
	// to our tip: known headers, a reorg, or garbage.
	if b.handleSerialHeaderExtension(hmsg) {
		return
	}

	// Late responses may contain only headers already in the store. They
	// must not publish a new tip or start a request from an old hash, so
	// the loop skips them. Such a reply usually answers an older request,
	// so it says nothing about the outstanding one: its deadline stays
	// armed.
	for i, blockHeader := range msg.Headers {
		blockHash := blockHeader.BlockHash()

		// Ensure there is a previous header to compare against.
		prevNode := b.headerList.Back()
		if prevNode == nil {
			log.Warnf("Header list does not contain a previous" +
				"element as expected -- disconnecting peer")
			hmsg.peer.Disconnect()
			return
		}
		prevHash := prevNode.Header.BlockHash()

		// The block doesn't connect to the last block we know.
		// We will need to do some additional checks to process
		// possible reorganizations or incorrect chain on
		// either our or the peer's side.
		//
		// If we got these headers from a peer that's not our
		// sync peer, they might not be aligned correctly or
		// even on the right chain. Just ignore the rest of the
		// message. However, if we're current, this might be a
		// reorg, in which case we'll either change our sync
		// peer or disconnect the peer that sent us these bad
		// headers.
		if hmsg.peer != b.SyncPeer() && !b.BlockHeadersSynced() {
			return
		}

		// Check if this is the last block we know of. This is
		// a shortcut for sendheaders so that each redundant
		// header doesn't cause a disk read.
		if blockHash == prevHash {
			continue
		}

		// Check if this block is known. If so, we continue to
		// the next one.
		_, _, err := b.cfg.BlockHeaders.FetchHeader(&blockHash)
		if err == nil {
			continue
		}

		// Check if the previous block is known. If it is, this
		// is probably a reorg based on the estimated latest
		// block that matches between us and the peer as
		// derived from the block locator we sent to request
		// these headers. Otherwise, the headers don't connect
		// to anything we know and we should disconnect the
		// peer.
		backHead, backHeight, err := b.cfg.BlockHeaders.FetchHeader(
			&blockHeader.PrevBlock,
		)
		if err != nil {
			log.Warnf("Received block header that does "+
				"not"+
				" properly connect to the chain from"+
				" peer %s (%s) -- disconnecting",
				hmsg.peer.Addr(), err)
			hmsg.peer.Disconnect()
			return
		}

		// We've found a branch we weren't aware of. If the
		// branch is earlier than the latest synchronized
		// checkpoint, it's invalid and we need to disconnect
		// the reporting peer.
		prevCheckpoint := b.findPreviousHeaderCheckpoint(
			prevNode.Height,
		)
		if backHeight < uint32(prevCheckpoint.Height) {
			log.Errorf("Attempt at a reorg earlier than "+
				"a "+
				"checkpoint past which we've already "+
				"synchronized -- disconnecting peer "+
				"%s", hmsg.peer.Addr())
			hmsg.peer.Disconnect()
			return
		}

		// Check the sanity of the new branch. If any of the
		// blocks don't pass sanity checks, disconnect the
		// peer. We also keep track of the work represented by
		// these headers so we can compare it to the work in
		// the known good chain.
		b.reorgList.ResetHeaderState(headerlist.Node{
			Header: *backHead,
			Height: int32(backHeight),
		})
		totalWork := big.NewInt(0)
		for j, reorgHeader := range msg.Headers[i:] {
			// We have to get the parent's height and
			// header to be able to contextually validate
			// this header.
			prevNodeHeight := backHeight + uint32(j)

			var prevNodeHeader *wire.BlockHeader
			if i+j == 0 {
				// Use backHead if we are using the
				// first header in the Headers slice.
				prevNodeHeader = backHead
			} else {
				// We can find the parent in the
				// Headers slice by getting the header
				// at index i+j-1.
				prevNodeHeader = msg.Headers[i+j-1]
			}

			err = b.checkHeaderSanity(
				reorgHeader, true,
				int32(
					prevNodeHeight,
				), prevNodeHeader, nil,
			)
			if err != nil {
				log.Warnf("Header doesn't pass sanity"+
					" check: %s -- disconnecting "+
					"peer", err)
				hmsg.peer.Disconnect()
				return
			}
			totalWork.Add(totalWork,
				blockchain.CalcWork(reorgHeader.Bits))
			b.reorgList.PushBack(headerlist.Node{
				Header: *reorgHeader,
				Height: int32(backHeight+1) + int32(j),
			})
		}
		log.Tracef("Sane reorg attempted. Total work from "+
			"reorg chain: %v", totalWork)

		// All the headers pass sanity checks. Now we calculate
		// the total work for the known chain.
		knownWork := big.NewInt(0)

		// This should NEVER be nil because the most recent
		// block is always pushed back by resetHeaderState
		knownEl := b.headerList.Back()
		var knownHead *wire.BlockHeader
		for j := uint32(prevNode.Height); j > backHeight; j-- {
			if knownEl != nil {
				knownHead = &knownEl.Header
				knownEl = knownEl.Prev()
			} else {
				// The walk left the validation list, so read
				// the parent from the store. A read error
				// says nothing about the peer, so we abandon
				// this message and let the retry request the
				// headers again.
				parentHash := knownHead.PrevBlock
				parent, _, err := b.cfg.BlockHeaders.
					FetchHeader(&parentHash)
				if err != nil {
					log.Errorf("Can't get block header "+
						"for hash %s while comparing "+
						"reorg work: %v", parentHash,
						err)
					b.scheduleHeaderCommitRetry()
					return
				}
				knownHead = parent
			}
			knownWork.Add(knownWork,
				blockchain.CalcWork(knownHead.Bits))
		}

		log.Tracef("Total work from known chain: %v", knownWork)

		// Compare the two work totals and reject the new chain
		// if it doesn't have more work than the previously
		// known chain. Disconnect if it's actually less than
		// the known chain.
		switch knownWork.Cmp(totalWork) {
		case 1:
			log.Warnf("Reorg attempt that has less work "+
				"than known chain from peer %s -- "+
				"disconnecting", hmsg.peer.Addr())
			hmsg.peer.Disconnect()
			fallthrough
		case 0:
			return
		default:
		}

		// Preserve the validated fork and replacement as an immutable
		// payload for the chain store.
		prepared := prepareSerialHeaderReorg(
			*prevNode, *backHead, backHeight, *blockHeader,
		)
		b.submitSerialHeaderReorg(hmsg, prepared, msg.Headers[i:])

		return
	}
}

// finishSerialHeaderFollowup advances the peer request after durable progress.
func (b *blockManager) finishSerialHeaderFollowup(peers *list.List,
	hmsg *headersMsg, finalHash *chainhash.Hash, finalHeight int32) {

	// The batch made progress, so it answered the sync peer's request.
	// A follow-up request below re-arms the deadline.
	if hmsg.peer == b.SyncPeer() {
		b.disarmHeaderSyncTimeout()
	}

	// If not current, request the next batch of headers starting from the
	// latest known header and ending with the next checkpoint.
	if b.cfg.ChainParams.Net == chaincfg.SimNetParams.Net ||
		!b.BlockHeadersSynced() {

		nextHash := zeroHash
		if b.nextCheckpoint != nil {
			nextHash = *b.nextCheckpoint.Hash
		}

		// Ask the sync peer, not whoever sent this batch. There is a
		// single serial deadline and it only acts for the sync peer,
		// so arming it for another peer cancelled the sync peer's
		// deadline and left that one with nothing in flight.
		target := b.SyncPeer()
		if target == nil {
			target = hmsg.peer
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
		if target != hmsg.peer {
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
			b.syncPeerMutex.Lock()
			if b.syncPeer == target {
				b.syncPeer = nil
			}
			b.syncPeerMutex.Unlock()
			b.startSync(peers)
		}
	}
}

// serialHeaderReorg binds a validated replacement to the durable tip and fork
// that the handler observed when preparing it.
type serialHeaderReorg struct {
	// expectedTip is the durable tip the fork was compared against.
	expectedTip headerfs.BlockStamp

	// fork is the last header both branches share.
	fork headerlist.Node

	// replacement is the first header of the new branch.
	replacement headerfs.BlockHeader
}

// nodeStamp returns the block stamp of a validation cache entry.
func nodeStamp(node headerlist.Node) headerfs.BlockStamp {
	return headerfs.BlockStamp{
		Height:    node.Height,
		Hash:      node.Header.BlockHash(),
		Timestamp: node.Header.Timestamp,
	}
}

// prepareSerialHeaderReorg captures the tip and fork a reorg was validated
// against, along with its first replacement header.
func prepareSerialHeaderReorg(tip headerlist.Node, fork wire.BlockHeader,
	forkHeight uint32, replacement wire.BlockHeader) serialHeaderReorg {

	return serialHeaderReorg{
		expectedTip: nodeStamp(tip),
		fork: headerlist.Node{
			Header: fork, Height: int32(forkHeight),
		},
		replacement: headerfs.BlockHeader{
			BlockHeader: &replacement, Height: forkHeight + 1,
		},
	}
}

// submitSerialHeaderReorg sends a validated fork as one actor mutation, so that
// no filter commit can land between the rollback and the replacement branch.
func (b *blockManager) submitSerialHeaderReorg(hmsg *headersMsg,
	prepared serialHeaderReorg, headers []*wire.BlockHeader) {

	// A branch that crosses a checkpoint must match it. If it doesn't, we
	// roll back instead of taking the branch. A batch that reaches the
	// checkpoint stops there, so the next one validates against the
	// following checkpoint.
	var (
		batch []headerfs.BlockHeader
		nodes []headerlist.Node
	)
	for i, header := range headers {
		height := prepared.fork.Height + int32(i) + 1
		if b.nextCheckpoint != nil &&
			height == b.nextCheckpoint.Height &&
			header.BlockHash() != *b.nextCheckpoint.Hash {

			log.Warnf(
				"Reorg header does not match checkpoint at %d",
				height,
			)
			b.submitCheckpointRollback(hmsg, height)
			return
		}
		copyHeader := *header
		batch = append(batch, headerfs.BlockHeader{
			BlockHeader: &copyHeader,
			Height:      uint32(height),
		})
		nodes = append(
			nodes,
			headerlist.Node{Header: copyHeader, Height: height},
		)
		if b.nextCheckpoint != nil &&
			height == b.nextCheckpoint.Height {

			break
		}
	}
	hmsg.peer.UpdateLastBlockHeight(nodes[len(nodes)-1].Height)

	command := chainstore.Reorg{
		ExpectedTip: prepared.expectedTip,
		ForkHeight:  uint32(prepared.fork.Height),
		Headers:     batch,
	}
	if hmsg.peer == b.SyncPeer() {
		b.disarmHeaderSyncTimeout()
	}
	b.pendingHeaderCommit = &pendingChainHeaderCommit{
		reorg:      &prepared,
		reorgNodes: nodes,
		message:    hmsg,
	}

	// The peer that delivered the winning branch becomes the sync peer,
	// unless it has already gone away.
	if !peerDisconnected(hmsg.peer) {
		b.syncPeerMutex.Lock()
		b.syncPeer = hmsg.peer
		b.syncPeerMutex.Unlock()
	}
	b.chainStore.TryAsk(b.quitCtx, command, b.chainStoreResult)
}

// submitCheckpointRollback rolls the chain back to the last verified checkpoint
// after a header failed the checkpoint at height. The peer that sent it is
// dropped once the rollback lands.
func (b *blockManager) submitCheckpointRollback(hmsg *headersMsg,
	height int32) {

	checkpoint := b.findPreviousHeaderCheckpoint(height)
	header, err := b.cfg.BlockHeaders.FetchHeaderByHeight(
		uint32(checkpoint.Height),
	)
	if err != nil {
		log.Warnf("Unable to load checkpoint for rollback: %v", err)
		b.scheduleHeaderCommitRetry()
		return
	}
	tip := b.headerList.Back()
	if tip == nil {
		return
	}

	node := headerlist.Node{Header: *header, Height: checkpoint.Height}
	command := chainstore.Rollback{
		ExpectedTip: nodeStamp(*tip),
		Height:      uint32(checkpoint.Height),
	}
	b.pendingHeaderCommit = &pendingChainHeaderCommit{
		checkpoint: &node, message: hmsg,
	}
	b.chainStore.TryAsk(b.quitCtx, command, b.chainStoreResult)
}

// serialHeaderExtension is a batch of headers validated against the handler's
// validation cache, ready for the durable write. It holds its own copies, since
// the write completes on another goroutine.
type serialHeaderExtension struct {
	// parent is the tip the batch was validated against.
	parent headerfs.BlockStamp

	// headers is the batch for the store.
	headers []headerfs.BlockHeader

	// nodes is the same batch for the validation cache.
	nodes []headerlist.Node
}

// serialCheckpointMismatch requests rollback to the last verified checkpoint.
type serialCheckpointMismatch struct{ height int32 }

// Error returns the height of the failed checkpoint.
func (e *serialCheckpointMismatch) Error() string {
	return fmt.Sprintf("header does not match checkpoint at %d", e.height)
}

// prepareSerialHeaderExtension validates an extending batch on scratch state.
// Reorganizations and checkpoint rejection remain on the legacy path.
func (b *blockManager) prepareSerialHeaderExtension(
	headers []*wire.BlockHeader) (serialHeaderExtension, bool, error) {

	var prepared serialHeaderExtension
	back := b.headerList.Back()
	if back == nil || headers[0].PrevBlock != back.Header.BlockHash() {
		return prepared, false, nil
	}
	prepared.parent = headerfs.BlockStamp{Height: back.Height,
		Hash: back.Header.BlockHash(), Timestamp: back.Header.Timestamp}
	validation := newHeaderSyncValidationChain(b.headerList, *back)
	previous := back.Header
	height := back.Height
	for _, header := range headers {
		if err := b.checkHeaderSanity(
			header, false, height, &previous, validation,
		); err != nil {
			return serialHeaderExtension{}, true, err
		}
		height++
		if b.nextCheckpoint != nil &&
			height == b.nextCheckpoint.Height &&
			header.BlockHash() != *b.nextCheckpoint.Hash {

			return serialHeaderExtension{}, true,
				&serialCheckpointMismatch{
					height: height,
				}
		}
		copyHeader := *header
		node := headerlist.Node{Header: copyHeader, Height: height}
		prepared.headers = append(
			prepared.headers,
			headerfs.BlockHeader{
				BlockHeader: &copyHeader,
				Height:      uint32(height),
			},
		)
		prepared.nodes = append(prepared.nodes, node)
		validation.PushBack(node)
		previous = copyHeader
		if b.nextCheckpoint != nil &&
			height == b.nextCheckpoint.Height {

			break
		}
	}
	return prepared, true, nil
}

// handleSerialHeaderExtension commits a batch that extends the validation
// chain through the chain store actor, without waiting for the write. It
// returns false when the batch does not extend the tip, so the caller can
// treat it as known headers or a reorg.
func (b *blockManager) handleSerialHeaderExtension(
	hmsg *headersMsg) bool {

	headers := hmsg.headers.Headers
	back := b.headerList.Back()
	if back == nil {
		return false
	}
	backHash := back.Header.BlockHash()
	if headers[0].PrevBlock != backHash {
		// A connected response may contain old headers before the
		// current tip. Find that tip without querying the store.
		found := false
		for i, header := range headers {
			if header.BlockHash() == backHash {
				headers = headers[i+1:]
				found = true
				break
			}
		}
		if !found || len(headers) == 0 {
			return false
		}
	}
	prepared, handled, err := b.prepareSerialHeaderExtension(headers)
	if !handled {
		return false
	}
	if err != nil {
		var mismatch *serialCheckpointMismatch
		if errors.As(err, &mismatch) {
			b.submitCheckpointRollback(hmsg, mismatch.height)
			return true
		}
		log.Warnf("Header doesn't pass sanity check: %s -- "+
			"disconnecting peer", err)
		hmsg.peer.Disconnect()
		return true
	}
	hmsg.peer.UpdateLastBlockHeight(
		prepared.nodes[len(prepared.nodes)-1].Height,
	)
	if hmsg.peer == b.SyncPeer() {
		b.disarmHeaderSyncTimeout()
	}
	b.pendingHeaderCommit = &pendingChainHeaderCommit{
		serial: &prepared, message: hmsg,
	}
	b.chainStore.TryAsk(b.quitCtx, chainstore.BlockCommit{
		ExpectedParent: prepared.parent,
		Headers:        prepared.headers,
	}, b.chainStoreResult)

	return true
}

// finishSerialHeaderExtension updates validation and peer state only after
// the prepared headers have been durably written.
func (b *blockManager) finishSerialHeaderExtension(peers *list.List,
	hmsg *headersMsg, prepared serialHeaderExtension) {

	for _, node := range prepared.nodes {
		b.blkHeaderProgressLogger.LogBlockHeight(
			node.Header.Timestamp,
			node.Height,
		)
		b.headerList.PushBack(node)
	}
	last := prepared.nodes[len(prepared.nodes)-1]
	hash := last.Header.BlockHash()
	b.advanceHeaderSyncTip(uint32(last.Height), hash)
	b.updateNextCheckpoint(uint32(last.Height))
	b.finishSerialHeaderFollowup(peers, hmsg, &hash, last.Height)
}
