package neutrino

// This file holds the block manager's side of parallel header sync: it runs the
// driver's events, keeps peers registered, and turns ready ranges into chain
// store commits.

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/lightninglabs/neutrino/headersync"
)

func (b *blockManager) headerSyncOpCtx() (context.Context,
	context.CancelFunc) {

	return context.WithTimeout(b.quitCtx, headerSyncOpTimeout)
}

func (b *blockManager) registerHeaderSyncPeer(sp *ServerPeer) {
	if b.headerSync == nil {
		return
	}

	peerID := sp.Addr()
	rtt := time.Duration(sp.LastPingMicros()) * time.Microsecond
	outcome := b.handleDriverEvent(nil, headersync.PeerConnected{
		Peer: headersync.PeerSnapshot{
			ID:    peerID,
			Rank:  headerSyncPeerRank(sp),
			RTT:   rtt,
			State: headersync.PeerReady,
		},
	})
	if outcome.err != nil {
		log.Warnf("Unable to start headersync peer for %s: %v",
			peerID, outcome.err)
		return
	}
	if !outcome.result.PeerCreated {
		return
	}

	log.Debugf("Registered headersync peer peer=%s rank=%d "+
		"last_block=%d rtt=%v", peerID, headerSyncPeerRank(sp),
		sp.LastBlock(), rtt)
}

func (b *blockManager) unregisterHeaderSyncPeer(sp *ServerPeer) {
	peerID := sp.Addr()
	if b.headerSync == nil {
		return
	}

	outcome := b.handleDriverEvent(nil, headersync.PeerDisconnected{
		PeerID: peerID,
	})
	if outcome.err != nil {
		log.Warnf("Unable to remove headersync peer %s: %v", peerID,
			outcome.err)
		return
	}

	log.Debugf("Unregistered headersync peer peer=%s", peerID)
}

func (b *blockManager) updateHeaderSyncPeerState(sp *ServerPeer,
	state headersync.PeerState) {

	b.handleDriverEvent(nil, headersync.PeerStateChanged{
		PeerID: sp.Addr(),
		State:  state,
	})
}

func (b *blockManager) quarantineHeaderSyncPeer(sp *ServerPeer) {
	b.updateHeaderSyncPeerState(sp, headersync.PeerQuarantined)
	b.quarantineHeaderSyncAddress(sp.Addr())
}

// quarantineHeaderSyncAddress asks the node to stop using a peer's address for
// a short time.
func (b *blockManager) quarantineHeaderSyncAddress(peerID string) {
	if b.cfg.QuarantinePeer != nil {
		b.cfg.QuarantinePeer(peerID, defaultHeaderPeerQueryQuarantine)
	}
}

// armHeaderCooldownWake arms one wakeup for the earliest peer cooldown expiry,
// unless one is already armed.
func (b *blockManager) armHeaderCooldownWake(until time.Time) {
	if b.headerCooldownTimer != nil {
		return
	}

	b.headerCooldownID++
	generation := b.headerCooldownID
	b.headerCooldownTimer = time.AfterFunc(time.Until(until), func() {
		message := &headerSyncCooldownMsg{generation: generation}
		select {
		case b.timerChan <- message:
		case <-b.quit:
		}
	})
}

// cancelHeaderCooldownWakeup stops a pending cooldown wakeup.
func (b *blockManager) cancelHeaderCooldownWakeup() {
	if b.headerCooldownTimer != nil {
		b.headerCooldownTimer.Stop()
		b.headerCooldownTimer = nil
		b.headerCooldownID++
	}
}

func (b *blockManager) recoverHeaderSyncPeers(peers *list.List) {
	candidates := headerSyncPeerCandidates(peers)
	b.handleDriverEvent(nil, headersync.RecoverPeers{Peers: candidates})
}

func headerSyncPeerRank(sp *ServerPeer) int {
	return -int(sp.LastBlock())
}

func (b *blockManager) planHeaderSyncRanges(bestHeight uint32) (int, error) {
	if b.headerSync == nil {
		return 0, nil
	}

	// Planning moves the FSM to the durable tip, so bring the cache and
	// the next checkpoint along first. Otherwise a write that landed but
	// reported an error leaves planning aimed at a stale checkpoint.
	if err := b.tip.ReconcileWithStore(); err != nil {
		return 0, err
	}

	header, height, err := b.cfg.BlockHeaders.ChainTip()
	if err != nil {
		return 0, err
	}
	event := headersync.PlanRanges{
		Tip: headersync.ChainPoint{
			Height: height,
			Hash:   header.BlockHash(),
		},
		Checkpoint: b.nextCheckpointPoint(),
		BestHeight: bestHeight,
	}

	outcome := b.handleDriverEvent(nil, event)
	if outcome.err != nil {
		return 0, outcome.err
	}

	return outcome.result.Planned, nil
}

// nextCheckpointPoint returns the next checkpoint as a chain point, or nil
// when the chain is past its last checkpoint.
func (b *blockManager) nextCheckpointPoint() *headersync.ChainPoint {
	next := b.tip.NextCheckpoint()
	if next == nil {
		return nil
	}

	return &headersync.ChainPoint{
		Height: uint32(next.Height),
		Hash:   *next.Hash,
	}
}

func (b *blockManager) dispatchHeaderSyncWork(peers *list.List) bool {
	if b.headerSync == nil {
		return false
	}

	b.recoverHeaderSyncPeers(peers)

	b.handleDriverEvent(nil, headersync.AssignRanges{
		PeerCount: peers.Len(),
	})

	var started bool
	for e := peers.Front(); e != nil; e = e.Next() {
		sp := e.Value.(*ServerPeer)
		outcome := b.handleDriverEvent(
			peers, headersync.DispatchPeer{PeerID: sp.Addr()},
		)
		if outcome.sent > 0 {
			started = true
		}
	}

	return started
}

func (b *blockManager) startHeaderAnchorDiscovery(peers *list.List,
	bestHeight uint32) bool {

	if b.headerSync == nil || b.tip.NextCheckpoint() == nil {
		return false
	}
	b.recoverHeaderSyncPeers(peers)

	header, height, err := b.cfg.BlockHeaders.ChainTip()
	if err != nil {
		log.Warnf("headersync unable to get chain tip for anchor "+
			"discovery: %v", err)
		return false
	}

	candidates := headerSyncPeerCandidates(peers)
	outcome := b.handleDriverEvent(peers, headersync.DiscoverAnchors{
		Tip: headersync.ChainPoint{
			Height: height,
			Hash:   header.BlockHash(),
		},
		Stop:       *b.nextCheckpointPoint(),
		BestHeight: bestHeight,
		Peers:      candidates,
	})

	return b.finishHeaderAnchorDiscovery(outcome)
}

func (b *blockManager) startHeaderFrontierDiscovery(peers *list.List,
	bestHeight uint32) bool {

	if b.headerSync == nil {
		return false
	}
	b.recoverHeaderSyncPeers(peers)

	header, height, err := b.cfg.BlockHeaders.ChainTip()
	if err != nil {
		log.Warnf("headersync unable to get chain tip for frontier "+
			"discovery: %v", err)
		return false
	}

	candidates := headerSyncPeerCandidates(peers)
	outcome := b.handleDriverEvent(peers, headersync.DiscoverFrontier{
		Tip: headersync.ChainPoint{
			Height: height,
			Hash:   header.BlockHash(),
		},
		BestHeight: bestHeight,
		Peers:      candidates,
	})

	return b.finishHeaderAnchorDiscovery(outcome)
}

func (b *blockManager) pipelineHeaderFrontierDiscovery(peers *list.List,
	response headersync.AnchorResponse) bool {

	if b.headerSync == nil || b.tip.NextCheckpoint() != nil {
		return false
	}

	bestHeight := bestHeaderSyncHeight(peers)
	candidates := headerSyncPeerCandidates(peers)
	outcome := b.handleDriverEvent(peers, headersync.PipelineFrontier{
		After: headersync.ChainPoint{
			Height: response.Height,
			Hash:   response.Hash,
		},
		BestHeight: bestHeight,
		Peers:      candidates,
	})

	return b.finishHeaderAnchorDiscovery(outcome)
}

func (b *blockManager) startHeaderAnchorDiscoveryFrom(peers *list.List,
	span headersync.AnchorRequest, excludedPeers ...string) bool {

	candidates := headerSyncPeerCandidates(peers)
	outcome := b.handleDriverEvent(peers, headersync.DiscoverFrom{
		Span:     span,
		Peers:    candidates,
		Excluded: excludedPeers,
	})

	return b.finishHeaderAnchorDiscovery(outcome)
}

// finishHeaderAnchorDiscovery reports whether anchor discovery is running
// after an event that planned it and the executor sent its requests.
func (b *blockManager) finishHeaderAnchorDiscovery(
	outcome driverOutcome) bool {

	discovery := outcome.result.Discovery
	if discovery.Settled {
		return discovery.Active
	}

	started := outcome.sent
	if started > 0 {
		log.Infof("headersync anchor_discovery_started peers=%d "+
			"active_peers=%d start_height=%d stop_height=%d",
			started, discovery.ActivePeers,
			discovery.Span.StartHeight, discovery.Span.StopHeight)
	}

	return discovery.ActivePeers+started > 0
}

func (b *blockManager) startHeaderSync(peers *list.List,
	bestPeer *ServerPeer, bestHeight uint32) bool {

	// The multi-peer scheduler needs multiple independent peers to
	// confirm discovered anchors. The local sim/reg test harnesses often
	// run with a single peer, so keep those networks on the serial path.
	if b.allowSerialHeaderFallback() {
		return false
	}

	planned, err := b.planHeaderSyncRanges(bestHeight)
	if err != nil {
		log.Warnf(
			"headersync failed to plan checkpoint ranges: %v",
			err,
		)
		return false
	}
	if planned == 0 {
		var started bool
		if b.tip.NextCheckpoint() != nil {
			started = b.startHeaderAnchorDiscovery(
				peers,
				bestHeight,
			)
		} else {
			started = b.startHeaderFrontierDiscovery(
				peers,
				bestHeight,
			)
		}
		if started {
			planned = 1
		}
	}

	b.syncPeerMutex.Lock()
	b.syncPeer = bestPeer
	b.syncPeerMutex.Unlock()

	started := b.dispatchHeaderSyncWork(peers)
	activity := b.headerSync.Driver().Activity()
	if started || activity.HasWork() || planned > 0 {
		return true
	}

	b.syncPeerMutex.Lock()
	if b.syncPeer == bestPeer {
		b.syncPeer = nil
	}
	b.syncPeerMutex.Unlock()

	return false
}

func removeSyncCandidate(peers *list.List, peer *ServerPeer) {
	for e := peers.Front(); e != nil; e = e.Next() {
		if e.Value == peer {
			peers.Remove(e)
			return
		}
	}
}

func bestHeaderSyncHeight(peers *list.List) uint32 {
	var bestHeight uint32
	for e := peers.Front(); e != nil; e = e.Next() {
		height := e.Value.(*ServerPeer).LastBlock()
		if height < 0 {
			continue
		}
		if uint32(height) > bestHeight {
			bestHeight = uint32(height)
		}
	}

	return bestHeight
}

func headerSyncPeerCandidates(peers *list.List) []headersync.PeerCandidate {
	candidates := make([]headersync.PeerCandidate, 0, peers.Len())
	for e := peers.Front(); e != nil; e = e.Next() {
		sp := e.Value.(*ServerPeer)
		peerID := sp.Addr()
		rtt := time.Duration(sp.LastPingMicros()) * time.Microsecond
		candidates = append(candidates, headersync.PeerCandidate{
			ID:     peerID,
			Height: sp.LastBlock(),
			Rank:   headerSyncPeerRank(sp),
			RTT:    rtt,
		})
	}

	return candidates
}

func (b *blockManager) continueHeaderSync(peers *list.List) {
	if b.resumeHeaderSyncWork(peers) {
		return
	}

	b.syncPeerMutex.Lock()
	b.syncPeer = nil
	b.syncPeerMutex.Unlock()

	b.startSync(peers)
}

// resumeHeaderSyncWork plans ranges, dispatches queued work and restarts
// anchor discovery, and reports whether any work is in flight. It never starts
// the serial path, so it is safe to run while a serial request is in flight.
func (b *blockManager) resumeHeaderSyncWork(peers *list.List) bool {
	// The asynchronous commit in flight owns the header tip, and planning
	// before it settles would build on a stale tip.
	if b.pendingHeaderCommit != nil {
		return true
	}

	bestHeight := bestHeaderSyncHeight(peers)
	_, err := b.planHeaderSyncRanges(bestHeight)
	if err != nil {
		log.Warnf("headersync failed to plan follow-up ranges: %v", err)
	}

	started := b.dispatchHeaderSyncWork(peers)
	var discoveryActive bool
	if b.tip.NextCheckpoint() != nil {
		discoveryActive = b.startHeaderAnchorDiscovery(
			peers,
			bestHeight,
		)
	} else {
		discoveryActive = b.startHeaderFrontierDiscovery(
			peers,
			bestHeight,
		)
	}

	return started || discoveryActive ||
		b.headerSync.Driver().Activity().HasWork()
}

func (b *blockManager) allowSerialHeaderFallback() bool {
	return b.cfg.ChainParams.Net == chaincfg.SimNetParams.Net ||
		b.cfg.ChainParams.Net == chaincfg.RegressionNetParams.Net
}

func (b *blockManager) resetHeaderListToTip() error {
	header, height, err := b.cfg.BlockHeaders.ChainTip()
	if err != nil {
		return err
	}

	// The visible tip must describe the same header as the validation
	// cache, or a later write can start from one and report the other.
	b.tip.Reset(headerlist.Node{
		Header: *header,
		Height: int32(height),
	})

	// A failed serial write may still have landed. Planning stops
	// advancing the FSM once the tip reaches the best height, so move it
	// here, or ranges the write covered stay staged forever.
	b.advanceHeaderSyncTip(height, header.BlockHash())

	return nil
}

func (b *blockManager) handleHeaderSyncTimeoutMsg(peers *list.List,
	msg *headerSyncTimeoutMsg) {

	if msg.anchor {
		b.handleHeaderSyncAnchorTimeoutMsg(peers, msg)
		return
	}

	if msg.rangeID != headersync.NoRange {
		b.handleHeaderSyncRangeTimeoutMsg(peers, msg)
		return
	}

	if msg.requestID != b.serialRequestID {
		return
	}

	if b.SyncPeer() != msg.peer {
		return
	}

	if b.BlockHeadersSynced() {
		return
	}

	var height uint32
	if b.pendingHeaderCommit != nil {
		tip := b.publishedBlockTip.Load()
		if tip == nil {
			return
		}
		height = uint32(tip.Height)
	} else {
		_, currentHeight, err := b.cfg.BlockHeaders.ChainTip()
		if err != nil {
			log.Errorf("Failed to get block header chain tip "+
				"while handling header sync timeout: %v", err)
			return
		}
		height = currentHeight
	}
	if height > msg.startHeight {
		return
	}

	log.Warnf("Header sync from peer %s stalled for %v at height %d; "+
		"quarantining peer and selecting a new sync peer",
		msg.peer.Addr(), msg.timeout, height)

	// A slow or stalled peer is quarantined, never IP-banned: on regtest
	// and behind NAT or Tor one address can be the only usable peer.
	b.quarantineHeaderSyncPeer(msg.peer)
	removeSyncCandidate(peers, msg.peer)
	msg.peer.Disconnect()

	b.syncPeerMutex.Lock()
	if b.syncPeer == msg.peer {
		b.syncPeer = nil
	}
	b.syncPeerMutex.Unlock()

	if b.pendingHeaderCommit != nil {
		return
	}

	if err := b.resetHeaderListToTip(); err != nil {
		log.Errorf("Failed to reset header state after header sync "+
			"timeout: %v", err)
		return
	}

	b.startSync(peers)
}

func (b *blockManager) handleHeaderSyncAnchorTimeoutMsg(peers *list.List,
	msg *headerSyncTimeoutMsg) {

	outcome := b.handleDriverEvent(peers, headersync.RequestTimedOut{
		PeerID:      msg.peer.Addr(),
		Kind:        headersync.RequestAnchor,
		RequestID:   msg.requestID,
		StartHeight: msg.startHeight,
		Timeout:     msg.timeout,
	}, msg.peer)
	if !outcome.result.Timeout.Matched {
		return
	}

	if b.startHeaderAnchorDiscoveryFrom(
		peers, outcome.result.Timeout.Anchor,
	) {

		return
	}

	b.syncPeerMutex.Lock()
	if b.syncPeer == msg.peer ||
		b.headerSync.Driver().Activity().Anchors == 0 {

		b.syncPeer = nil
	}
	b.syncPeerMutex.Unlock()

	b.startSync(peers)
}

func (b *blockManager) handleHeaderSyncRangeTimeoutMsg(peers *list.List,
	msg *headerSyncTimeoutMsg) {

	outcome := b.handleDriverEvent(peers, headersync.RequestTimedOut{
		PeerID:      msg.peer.Addr(),
		Kind:        headersync.RequestRange,
		RequestID:   msg.requestID,
		StartHeight: msg.startHeight,
		RangeID:     msg.rangeID,
		LeaseEpoch:  msg.leaseEpoch,
		Timeout:     msg.timeout,
		StaleFor:    b.headerTimeoutForPeer(msg.peer),
	}, msg.peer)
	if !outcome.result.Timeout.Matched {
		return
	}

	b.dispatchHeaderSyncWork(peers)
}

func (b *blockManager) rejectHeaderSyncAnchor(peers *list.List,
	peer *ServerPeer, request headersync.AnchorRequest, reason string) {

	log.Warnf("headersync anchor_rejected peer=%s start_height=%d "+
		"stop_height=%d reason=%s", peer.Addr(), request.StartHeight,
		request.StopHeight, reason)

	b.quarantineHeaderSyncPeer(peer)
	removeSyncCandidate(peers, peer)

	b.replanHeaderSyncAnchor(peers, request, peer.Addr())
}

// conflictHeaderSyncAnchor handles a well formed discovery reply that names a
// different boundary than the anchor already recorded at its height. Nothing
// proves which reporter lied, and the earlier one may well be the liar, so
// neither is punished: the reporter is neither quarantined nor held back. The
// recorded anchor is rejected along with the unstaged work built on it, and
// the span is discovered again. Commit-time contextual validation of any
// staged payload finds the liar.
//
// A conflict against a trusted anchor, say a committed boundary after a short
// reorg in frontier mode, rejects nothing and so replans nothing. It is not
// punished either.
//
// A fast liar that always answers first can repeat this under a policy that
// needs two observations, since each round rejects the honest anchor again.
// The production policy needs one, so the first reply for a span is staged
// and committed whatever a later conflict does to the anchor, and the loop
// cannot stall it. We accept the loop for the stricter test-only policy.
func (b *blockManager) conflictHeaderSyncAnchor(peers *list.List,
	peer *ServerPeer, request headersync.AnchorRequest,
	anchor headersync.Anchor) {

	log.Warnf("headersync anchor_conflict peer=%s start_height=%d "+
		"stop_height=%d anchor_height=%d recorded_hash=%s",
		peer.Addr(), request.StartHeight, request.StopHeight,
		anchor.Height, anchor.Hash)

	if !b.rejectUntrustedHeaderSyncAnchor(
		"anchor_rejected_by_conflict", anchor.Height, anchor.Hash,
	) {

		if !b.headerSync.Driver().Activity().HasWork() {
			b.continueHeaderSync(peers)
		}

		return
	}

	b.replanHeaderSyncAnchor(peers, request)
}

// rejectUntrustedHeaderSyncAnchor rejects an untrusted anchor and the unstaged
// work built on it, and logs the outcome under tag. It reports false only when
// the anchor was trusted, replaced or unknown, so nothing changed. A failed
// call reports true, since the anchor may have been rejected.
func (b *blockManager) rejectUntrustedHeaderSyncAnchor(tag string,
	height uint32, hash chainhash.Hash) bool {

	outcome := b.handleDriverEvent(nil, headersync.RejectUntrustedAnchor{
		Tag:     tag,
		Height:  height,
		Hash:    hash,
		Timeout: b.headerSyncTimeout,
	})

	return outcome.result.AnchorRejected
}

// replanHeaderSyncAnchor discovers the span of a rejected anchor reply again,
// leaving the excluded peers out of the new round, and falls back to the
// regular sync start when no other peer can answer.
func (b *blockManager) replanHeaderSyncAnchor(peers *list.List,
	request headersync.AnchorRequest, excluded ...string) {

	if b.startHeaderAnchorDiscoveryFrom(peers, request, excluded...) {
		return
	}

	b.syncPeerMutex.Lock()
	if b.headerSync.Driver().Activity().Anchors == 0 {
		b.syncPeer = nil
	}
	b.syncPeerMutex.Unlock()

	b.startSync(peers)
}

func (b *blockManager) maybeContinueHeaderAnchorDiscovery(peers *list.List,
	request headersync.AnchorRequest, startHeight uint32,
	startHash chainhash.Hash, excludePeerID string) {

	if b.startHeaderAnchorDiscoveryFrom(
		peers, headersync.AnchorRequest{
			StartHeight: startHeight,
			StartHash:   startHash,
			StopHeight:  request.StopHeight,
			StopHash:    request.StopHash,
		}, excludePeerID,
	) {

		return
	}

	if b.headerSync.Driver().Activity().HasWork() {
		return
	}

	b.syncPeerMutex.Lock()
	b.syncPeer = nil
	b.syncPeerMutex.Unlock()

	if b.allowSerialHeaderFallback() {
		b.startSync(peers)
		return
	}

	log.Debugf("headersync anchor_discovery_waiting start_height=%d "+
		"stop_height=%d excluded_peer=%s", startHeight,
		request.StopHeight, excludePeerID)
}

func (b *blockManager) handleHeaderSyncAnchorHeaders(peers *list.List,
	hmsg *headersMsg) bool {

	peerID := hmsg.peer.Addr()

	reply := b.handleDriverEvent(peers, headersync.AnchorReply{
		PeerID:  peerID,
		Headers: hmsg.headers.Headers,
	}).result.Anchor
	if !reply.Matched {
		return reply.Consumed
	}
	result := reply.Finished

	request := result.Request
	if result.Rejected {
		if errors.Is(result.Err, headersync.ErrRangeStartMismatch) {
			log.Infof("headersync stale_anchor_mismatch peer=%s "+
				"start_height=%d stop_height=%d reason=%s",
				peerID, request.StartHeight, request.StopHeight,
				result.Reason)

			b.maybeContinueHeaderAnchorDiscovery(
				peers, request, request.StartHeight,
				request.StartHash, "",
			)

			return true
		}

		if result.Conflict {
			b.conflictHeaderSyncAnchor(
				peers, hmsg.peer, request, result.Anchor,
			)
			return true
		}

		b.rejectHeaderSyncAnchor(
			peers, hmsg.peer, request, result.Reason,
		)
		return true
	}

	_, tipHeight, tipErr := b.cfg.BlockHeaders.ChainTip()
	if tipErr != nil {
		log.Warnf("headersync unable to check tip for anchor "+
			"response "+
			"peer=%s start_height=%d stop_height=%d: %v", peerID,
			request.StartHeight, request.StopHeight, tipErr)
	} else if !result.Staged && result.Response.Height <= tipHeight {
		pruned := b.handleDriverEvent(nil, headersync.PruneAnchors{
			AtOrBefore: tipHeight,
		}).result.Pruned
		log.Infof("headersync stale_anchor_completion peer=%s "+
			"start_height=%d response_height=%d stop_height=%d "+
			"tip_height=%d pruned_anchors=%d", peerID,
			request.StartHeight, result.Response.Height,
			request.StopHeight, tipHeight, pruned)

		if !b.headerSync.Driver().Activity().HasWork() {
			b.continueHeaderSync(peers)
		}

		return true
	}

	log.Infof("headersync anchor_discovered peer=%s height=%d hash=%s "+
		"trusted=%v confirmations=%d confirmed=%v start_height=%d "+
		"stop_height=%d", peerID, result.Response.Height,
		result.Response.Hash, result.Anchor.Trusted,
		result.Anchor.Confirmations, result.Confirmed,
		request.StartHeight, request.StopHeight)

	if result.Staged {
		b.handleDriverEvent(nil, headersync.StageRange{
			RangeID: result.Range.ID,
			PeerID:  peerID,
			Headers: hmsg.headers.Headers,
		})
		log.Infof("headersync range_staged_from_anchor peer=%s "+
			"range_id=%d start_height=%d stop_height=%d "+
			"header_count=%d", peerID, result.Range.ID,
			result.Range.StartHeight, result.Range.StopHeight,
			len(hmsg.headers.Headers))

		if err := b.commitReadyHeaderSyncRanges(); err != nil {
			log.Warnf("headersync failed to commit anchor-backed "+
				"range_id=%d: %v", result.Range.ID, err)
			if b.rejectInvalidHeaderCommit(peers, err) {
				return true
			}
		}

		if b.pendingHeaderCommit != nil {
			return true
		}

		if result.Confirmed && b.pipelineHeaderFrontierDiscovery(
			peers, result.Response,
		) {

			log.Debugf("headersync frontier_discovery_overlapped "+
				"range_id=%d stop_height=%d", result.Range.ID,
				result.Range.StopHeight)
		}
	}

	if !result.Confirmed {
		b.maybeContinueHeaderAnchorDiscovery(
			peers, request, request.StartHeight, request.StartHash,
			peerID,
		)
		return true
	}

	planned, err := b.planHeaderSyncRanges(request.StopHeight)
	if err != nil {
		log.Warnf("headersync unable to plan ranges after anchor "+
			"height=%d: %v", result.Response.Height, err)
	} else if planned > 0 {
		b.dispatchHeaderSyncWork(peers)
	}

	if result.Response.Height < request.StopHeight {
		b.maybeContinueHeaderAnchorDiscovery(
			peers, request, result.Response.Height,
			result.Response.Hash, "",
		)
		return true
	}

	if planned == 0 && !b.headerSync.Driver().Activity().HasWork() {
		b.syncPeerMutex.Lock()
		b.syncPeer = nil
		b.syncPeerMutex.Unlock()
		b.startSync(peers)
	}

	return true
}

func (b *blockManager) rejectHeaderSyncRangePeer(peers *list.List,
	peer *ServerPeer, rangeID headersync.RangeID) {

	log.Warnf("headersync peer_rejected peer=%s range_id=%d reason=%s",
		peer.Addr(), rangeID, banman.InvalidBlock)

	b.quarantineHeaderSyncPeer(peer)
	removeSyncCandidate(peers, peer)
	peer.Disconnect()
}

func (b *blockManager) handleHeaderSyncRangeHeaders(peers *list.List,
	hmsg *headersMsg) bool {

	outcome := b.handleDriverEvent(peers, headersync.RangeReply{
		PeerID:        hmsg.peer.Addr(),
		PeerHeight:    hmsg.peer.LastBlock(),
		Headers:       hmsg.headers.Headers,
		AnchorTimeout: b.headerSyncTimeout,
	}, hmsg.peer)

	return outcome.result.Handled
}

func (b *blockManager) commitReadyHeaderSyncRanges() error {
	if b.pendingHeaderCommit != nil {
		return nil
	}
	// A write that reported an error may still have landed, so the store,
	// not the validation cache, decides where the next write starts.
	if err := b.tip.ReconcileWithStore(); err != nil {
		b.scheduleHeaderCommitRetry()
		return err
	}

	listed := b.handleDriverEvent(nil, headersync.ListReadyRanges{})
	if listed.err != nil {
		return listed.err
	}
	ready := listed.result.Ready
	if len(ready) == 0 {
		return nil
	}

	commitRanges := make([]headersync.RangeCommit, 0, len(ready))
	for _, rangeID := range ready {
		event := headersync.SnapshotReadyRange{RangeID: rangeID}
		fetched := b.handleDriverEvent(nil, event)
		if fetched.err != nil {
			return fetched.err
		}

		rng := fetched.result.ReadyRange.Range
		staged := fetched.result.ReadyRange.Staged

		// Skip whatever part of the range is already durable. The
		// FSM still commits the whole range below.
		commitRange, pending, err := b.trimDurableHeaderSyncRange(
			rng, staged,
		)
		if err != nil {
			b.scheduleHeaderCommitRetry()
			return err
		}
		if pending {
			commitRanges = append(commitRanges, commitRange)
		}
	}

	// The actor writes the batch. Completion arrives on chainStoreResult,
	// so the handler never waits on the store.
	if len(commitRanges) == 0 {
		return b.finishReadyHeaderSyncRanges()
	}
	prepared, commitErr := b.tip.PrepareRangeHeaders(commitRanges)
	if commitErr == nil {
		b.pendingHeaderCommit = &pendingChainHeaderCommit{
			ranges: &prepared,
		}
		b.chainStore.TryAsk(b.quitCtx, chainstore.BlockCommit{
			ExpectedParent: prepared.ExpectedParent,
			Headers:        prepared.Headers,
		}, b.chainStoreResult)

		return nil
	}
	if err := commitErr; err != nil {
		var invalid *headersync.ValidationError
		if !errors.As(err, &invalid) {
			b.scheduleHeaderCommitRetry()
			return err
		}
		outcome := b.handleDriverEvent(nil, headersync.InvalidateRange{
			RangeID:       invalid.RangeID,
			CooldownUntil: time.Now().Add(b.headerSyncTimeout),
		})
		result, recoveryErr := outcome.result.Invalidated, outcome.err
		log.Warnf("headersync range_invalidated range_id=%d peer=%s "+
			"invalidated_ranges=%v invalidated_anchors=%d "+
			"reason=%v",
			invalid.RangeID, invalid.PeerID, result.Ranges,
			len(result.Anchors), err)
		if recoveryErr != nil {
			return fmt.Errorf("%w; invalidation failed: %v",
				err, recoveryErr)
		}
		return err
	}

	return nil
}

// pendingChainHeaderCommit is the one chain store mutation the block handler
// has in flight. The handler submits it without waiting and keeps serving
// peers, so this holds what it needs to finish the work when the reply arrives.
// Exactly one of ranges, serial, reorg and checkpoint is set, which says what
// the mutation was.
type pendingChainHeaderCommit struct {
	// ranges is set for a commit of ready parallel header sync ranges.
	ranges *headersync.PreparedCommit

	// serial is set for a serial extension of the tip.
	serial *headersync.SerialExtension

	// reorg is set for a serial reorg, with reorgNodes holding the headers
	// of the new branch for the validation cache.
	reorg      *serialHeaderReorg
	reorgNodes []headerlist.Node

	// reorgMismatch is the height of a checkpoint the reorg's branch
	// failed, or zero. The command only compares work in that case.
	reorgMismatch int32

	// checkpoint is set for a rollback to the last verified checkpoint
	// after a peer's header failed a checkpoint. It's the new tip.
	checkpoint *headerlist.Node

	// snapshot is set once the mutation has failed. Its outcome is unknown
	// from then on, so we're waiting on a snapshot instead of its reply.
	snapshot bool

	// message is the headers message that caused a serial, reorg or
	// checkpoint mutation.
	message *headersMsg
}

// requestChainStoreSnapshot asks the actor to finish any retained rollback and
// report the durable tip. The reply arrives on chainStoreResult.
func (b *blockManager) requestChainStoreSnapshot() {
	b.chainStore.TryAsk(
		b.quitCtx, chainstore.SnapshotRequest{}, b.chainStoreResult,
	)
}

// finishAsyncHeaderCommit applies the outcome of the pending chain store
// mutation to the state the block handler owns: the validation cache, the
// checkpoint cursor and the FSM's committed tip. The chain store actor has
// already updated the durable and published state by the time we get here.
func (b *blockManager) finishAsyncHeaderCommit(peers *list.List,
	result chainstore.Result) {

	prepared := b.pendingHeaderCommit
	if prepared == nil {
		return
	}

	switch {
	// The timer from a failed snapshot fired, so we try it again.
	case result.Retry:
		b.requestChainStoreSnapshot()

		return

	case prepared.snapshot:
		b.finishChainStoreSnapshot(peers, prepared, result)

		return

	// The mutation may have applied, fully or in part. We reconcile with
	// the chain store before reading or publishing any durable state. This
	// covers range, serial, reorg and checkpoint rollback commits alike.
	case result.Reply.Err != nil:
		prepared.snapshot = true
		b.requestChainStoreSnapshot()

		return
	}

	b.pendingHeaderCommit = nil

	// The command never ran, so nothing changed and the staged work is
	// still there for the retry.
	if result.Err != nil {
		log.Warnf("headersync chain store admission failed: %v",
			result.Err)
		b.scheduleHeaderCommitRetry()

		return
	}

	switch {
	case prepared.checkpoint != nil:
		b.finishCheckpointRollback(peers, prepared)

	case prepared.reorg != nil:
		b.finishSerialHeaderReorg(peers, prepared, result.Reply.Verdict)

	case prepared.serial != nil:
		b.finishSerialHeaderExtension(
			peers, prepared.message, *prepared.serial,
		)

	default:
		b.finishHeaderSyncRangeHeaders(*prepared.ranges)

		err := b.finishReadyHeaderSyncRanges()
		if err != nil {
			log.Warnf("headersync completion failed: %v", err)
			b.scheduleHeaderCommitRetry()

			return
		}
		b.continueHeaderSync(peers)
	}
}

// finishChainStoreSnapshot adopts the durable tip after a failed mutation. Our
// view of the chain is unknown until a snapshot succeeds, so this is the one
// thing that retries by itself: every other failure reconciles through it.
//
// BlockTipValid means the chain store finished any retained rollback and read
// the block tip. A failure on the filter side leaves Err set, but we can still
// adopt the block tip.
func (b *blockManager) finishChainStoreSnapshot(peers *list.List,
	prepared *pendingChainHeaderCommit, result chainstore.Result) {

	if result.Err != nil || !result.Reply.BlockTipValid {
		log.Warnf("chain store snapshot failed: %v %v", result.Err,
			result.Reply.Err)

		time.AfterFunc(time.Second, func() {
			select {
			case b.chainStoreResult <- chainstore.Result{
				Retry: true,
			}:
			case <-b.quit:
			}
		})

		return
	}

	b.pendingHeaderCommit = nil

	node := headerlist.Node{
		Header: result.Reply.BlockHeader,
		Height: result.Reply.BlockTip.Height,
	}
	b.tip.Reset(node)
	b.advanceHeaderSyncTip(uint32(node.Height), node.Header.BlockHash())

	if prepared.checkpoint != nil {
		b.rejectCheckpointPeer(peers, prepared.message.peer)
	}

	b.scheduleHeaderCommitRetry()
}

// finishCheckpointRollback resets the validation cache to the last verified
// checkpoint, then drops the peer whose header failed the next one.
func (b *blockManager) finishCheckpointRollback(peers *list.List,
	prepared *pendingChainHeaderCommit) {

	node := *prepared.checkpoint
	b.tip.Reset(node)
	b.advanceHeaderSyncTip(uint32(node.Height), node.Header.BlockHash())

	b.rejectCheckpointPeer(peers, prepared.message.peer)
}

// finishSerialHeaderReorg rebuilds the validation cache on the new branch, then
// asks for whatever comes after it.
func (b *blockManager) finishSerialHeaderReorg(peers *list.List,
	prepared *pendingChainHeaderCommit, verdict chainstore.ReorgVerdict) {

	peer := prepared.message.peer

	switch {
	// A branch with less work is an invalid claim, so we drop the peer.
	case verdict == chainstore.ReorgLessWork:
		log.Warnf("Reorg attempt that has less work than known chain "+
			"from peer %s -- disconnecting", peer.Addr())
		peer.Disconnect()

		return

	// A tie changes nothing, and says nothing bad about the peer.
	case verdict == chainstore.ReorgEqualWork:
		return

	// The branch would win, but it failed a checkpoint.
	case prepared.reorgMismatch != 0:
		b.submitCheckpointRollback(
			prepared.message, prepared.reorgMismatch,
		)

		return
	}

	// The branch won. It answered the sync peer's request, and the peer
	// that delivered it becomes the sync peer, unless it has gone away.
	last := prepared.reorgNodes[len(prepared.reorgNodes)-1]
	peer.UpdateLastBlockHeight(last.Height)
	if peer == b.SyncPeer() {
		b.disarmHeaderSyncTimeout()
	}
	if !peerDisconnected(peer) {
		b.syncPeerMutex.Lock()
		b.syncPeer = peer
		b.syncPeerMutex.Unlock()
	}

	b.tip.Reset(prepared.reorg.fork)
	b.tip.Extend(prepared.reorgNodes)
	for _, node := range prepared.reorgNodes {
		b.blkHeaderProgressLogger.LogBlockHeight(
			node.Header.Timestamp, node.Height,
		)
	}

	hash := last.Header.BlockHash()
	b.advanceHeaderSyncTip(uint32(last.Height), hash)

	b.finishSerialHeaderFollowup(
		peers, prepared.message, &hash, last.Height,
	)
}

// rejectCheckpointPeer disconnects the peer whose header failed a checkpoint
// after the rollback completed. If that peer was the sync peer and already
// went away while the rollback was pending, no peer-done handling restarts
// sync, so it restarts here.
func (b *blockManager) rejectCheckpointPeer(peers *list.List,
	peer *ServerPeer) {

	peer.Disconnect()
	if b.SyncPeer() == nil {
		b.startSync(peers)
	}
}

// finishReadyHeaderSyncRanges retires staged ranges after durable completion.
func (b *blockManager) finishReadyHeaderSyncRanges() error {
	// Each driver event has its own deadline, so a slow write can't expire
	// the commit.
	outcome := b.handleDriverEvent(nil, headersync.CommitDurableRanges{})
	if outcome.err != nil {
		b.scheduleHeaderCommitRetry()
		return outcome.err
	}
	commitResult := outcome.result.Commit
	b.cancelHeaderCommitRetry()
	commit := commitResult.Commit
	if commitResult.PrunedAnchors > 0 {
		log.Infof("headersync pruned_obsolete_anchors "+
			"tip_height=%d pruned=%d", commit.TipHeight,
			commitResult.PrunedAnchors)
	}
	b.handleDriverEvent(nil, headersync.RetireStaged{
		RangeIDs: commit.Committed,
	})
	for _, rangeID := range commit.Committed {
		log.Infof(
			"headersync range_committed range_id=%d "+
				"tip_height=%d "+
				"tip_hash=%s",
			rangeID,
			commit.TipHeight,
			commit.TipHash,
		)
	}

	return nil
}

// trimDurableHeaderSyncRange drops the prefix of a ready range that is already
// durable, so a retry after an ambiguous write appends only the missing
// suffix. It reports pending=false when the whole range is durable; the
// caller then skips the write and only commits the range in the FSM. A range
// whose durable prefix disagrees with its staged headers is returned
// unchanged, so validation classifies it as before.
func (b *blockManager) trimDurableHeaderSyncRange(rng headersync.HeaderRange,
	staged headersync.StagedRange) (headersync.RangeCommit, bool, error) {

	commit := headersync.RangeCommit{Range: rng, Staged: staged}
	snapshot := b.publishedBlockTip.Load()
	if snapshot == nil {
		return commit, false, fmt.Errorf(
			"published block tip unavailable",
		)
	}
	tip := uint32(snapshot.Height)
	if tip <= rng.StartHeight || len(staged.Headers) == 0 {
		return commit, true, nil
	}

	durable := min(tip-rng.StartHeight, uint32(len(staged.Headers)))
	last := staged.Headers[durable-1]
	stored, err := b.cfg.BlockHeaders.FetchHeaderByHeight(
		rng.StartHeight + durable,
	)
	if err != nil {
		return commit, false, err
	}
	if stored.BlockHash() != last.BlockHash() {
		return commit, true, nil
	}
	if int(durable) == len(staged.Headers) {
		return commit, false, nil
	}

	commit.Range.StartHeight += durable
	commit.Range.StartHash = last.BlockHash()
	commit.Staged.Headers = staged.Headers[durable:]

	return commit, true, nil
}

// scheduleHeaderCommitRetry keeps one wakeup while staging waits for storage.
// A recovered store must not need another network response to drain staging.
func (b *blockManager) scheduleHeaderCommitRetry() {
	if b.headerCommitRetryTimer != nil {
		return
	}
	b.headerCommitRetryID++
	generation := b.headerCommitRetryID
	b.headerCommitRetryTimer = time.AfterFunc(time.Second, func() {
		message := &headerSyncCommitRetryMsg{generation: generation}
		select {
		case b.timerChan <- message:
		case <-b.quit:
		}
	})
}

// cancelHeaderCommitRetry stops a pending commit retry wakeup. Bumping the
// generation voids a timer that has already fired and is waiting to be read.
func (b *blockManager) cancelHeaderCommitRetry() {
	if b.headerCommitRetryTimer != nil {
		b.headerCommitRetryTimer.Stop()
		b.headerCommitRetryTimer = nil
		b.headerCommitRetryID++
	}
}

func (b *blockManager) rejectInvalidHeaderCommit(peers *list.List,
	err error) bool {

	var invalid *headersync.ValidationError
	if !errors.As(err, &invalid) {
		return false
	}
	for element := peers.Front(); element != nil; element = element.Next() {
		peer := element.Value.(*ServerPeer)
		if peer.Addr() == invalid.PeerID {
			b.rejectHeaderSyncRangePeer(
				peers, peer, invalid.RangeID,
			)
			break
		}
	}
	b.continueHeaderSync(peers)
	return true
}

// finishHeaderSyncRangeHeaders updates handler-owned validation state after a
// prepared batch has been durably committed.
func (b *blockManager) finishHeaderSyncRangeHeaders(
	prepared headersync.PreparedCommit) {

	for _, node := range prepared.Nodes {
		b.blkHeaderProgressLogger.LogBlockHeight(
			node.Header.Timestamp, node.Height,
		)
	}

	b.tip.FinishRange(prepared)
}
