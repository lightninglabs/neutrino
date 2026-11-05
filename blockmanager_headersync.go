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

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/lightninglabs/neutrino/headerfs"
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
	ctx, cancel := b.headerSyncOpCtx()
	defer cancel()

	rtt := time.Duration(sp.LastPingMicros()) * time.Microsecond
	_, created, err := b.headerSync.RegisterPeer(ctx,
		headersync.PeerSnapshot{
			ID:    peerID,
			Rank:  headerSyncPeerRank(sp),
			RTT:   rtt,
			State: headersync.PeerReady,
		},
	)
	if err != nil {
		log.Warnf("Unable to start headersync peer actor for %s: %v",
			peerID, err)
		return
	}
	if !created {
		b.updateHeaderSyncPeerRTT(sp)
		return
	}

	b.handleDriverEvent(nil, headersync.PeerRegistered{PeerID: peerID})

	log.Debugf("Registered headersync peer actor peer=%s rank=%d "+
		"last_block=%d rtt=%v", peerID, headerSyncPeerRank(sp),
		sp.LastBlock(), time.Duration(sp.LastPingMicros())*
			time.Microsecond)
}

func (b *blockManager) unregisterHeaderSyncPeer(sp *ServerPeer) {
	peerID := sp.Addr()
	if b.headerSync == nil {
		return
	}

	ctx, cancel := b.headerSyncOpCtx()
	defer cancel()

	if err := b.headerSync.UnregisterPeer(ctx, peerID); err != nil {
		log.Warnf("Unable to remove headersync peer %s: %v", peerID,
			err)
		return
	}

	log.Debugf("Unregistered headersync peer actor peer=%s", peerID)
}

func (b *blockManager) updateHeaderSyncPeerRTT(sp *ServerPeer) {
	rtt := time.Duration(sp.LastPingMicros()) * time.Microsecond
	if rtt <= 0 {
		return
	}

	ctx, cancel := b.headerSyncOpCtx()
	defer cancel()

	if err := b.headerSync.UpdatePeerRTT(ctx, sp.Addr(), rtt); err != nil {
		log.Warnf("Unable to update headersync peer RTT for %s: %v",
			sp.Addr(), err)
	}
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
	if b.headerSync.Controller() == nil {
		return 0, nil
	}

	// Planning moves the FSM to the durable tip, so bring the cache and
	// the next checkpoint along first. Otherwise a write that landed but
	// reported an error leaves planning aimed at a stale checkpoint.
	if err := b.reconcileHeaderTipWithStore(); err != nil {
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
	if b.nextCheckpoint == nil {
		return nil
	}

	return &headersync.ChainPoint{
		Height: uint32(b.nextCheckpoint.Height),
		Hash:   *b.nextCheckpoint.Hash,
	}
}

func (b *blockManager) dispatchHeaderSyncWork(peers *list.List) bool {
	if b.headerSync.Controller() == nil {
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

	if b.headerSync.Controller() == nil || b.nextCheckpoint == nil {
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

	if b.headerSync.Controller() == nil {
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

	if b.headerSync.Controller() == nil || b.nextCheckpoint != nil {
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

	// The actor-backed scheduler needs multiple independent peers to
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
		if b.nextCheckpoint != nil {
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
	if b.nextCheckpoint != nil {
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

	b.headerList.ResetHeaderState(headerlist.Node{
		Header: *header,
		Height: int32(height),
	})

	// The visible tip must describe the same header as the validation
	// cache, or a later write can start from one and report the other.
	b.updateNextCheckpoint(height)

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
// the anchor was trusted, replaced or unknown, so nothing changed. A failed ask
// reports true, since the anchor may have been rejected.
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
	ctx, cancel := b.headerSyncOpCtx()
	defer cancel()

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

		if err := b.commitReadyHeaderSyncRanges(ctx); err != nil {
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

func (b *blockManager) commitReadyHeaderSyncRanges(ctx context.Context) error {
	if b.pendingHeaderCommit != nil {
		return nil
	}
	// A write that reported an error may still have landed, so the store,
	// not the validation cache, decides where the next write starts.
	if err := b.reconcileHeaderTipWithStore(); err != nil {
		b.scheduleHeaderCommitRetry()
		return err
	}

	ready, err := b.headerSync.Manager().ReadyRanges(ctx)
	if err != nil {
		return err
	}
	if len(ready) == 0 {
		// A commit ask that applied but reported an error leaves its
		// payloads behind with nothing left to commit them.
		err := b.headerSync.Controller().PruneRetiredStaging(ctx)
		if err != nil {
			b.scheduleHeaderCommitRetry()
			return err
		}
		b.cancelHeaderCommitRetry()
		return nil
	}

	commitRanges := make([]headerSyncRangeCommit, 0, len(ready))
	for _, rangeID := range ready {
		// The FSM staged the range but its headers never reached the
		// session: the completion ask applied and then reported an
		// error. Fetch the span again rather than stop here forever.
		staged, ok := b.headerSync.Driver().StagedRange(rangeID)
		if !ok {
			_, err := b.headerSync.Manager().RequeueStagedRange(
				ctx, rangeID,
			)
			log.Warnf("headersync range_id=%d ready without "+
				"staged headers, requeued: %v", rangeID, err)
			break
		}

		rng, ok, err := b.headerSync.Manager().
			SnapshotRange(ctx, rangeID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("headersync range_id=%d disappeared",
				rangeID)
		}

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
	prepared, commitErr := b.prepareHeaderSyncRangeHeaders(commitRanges)
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
		var invalid *headerSyncValidationError
		if !errors.As(err, &invalid) {
			b.scheduleHeaderCommitRetry()
			return err
		}
		recoveryCtx, cancel := b.headerSyncOpCtx()
		defer cancel()
		controller := b.headerSync.Controller()
		result, recoveryErr := controller.InvalidateRange(
			recoveryCtx, invalid.rangeID,
			time.Now().Add(b.headerSyncTimeout),
		)
		log.Warnf("headersync range_invalidated range_id=%d peer=%s "+
			"invalidated_ranges=%v invalidated_anchors=%d "+
			"reason=%v",
			invalid.rangeID, invalid.peerID, result.Ranges,
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
	ranges *headerSyncPreparedCommit

	// serial is set for a serial extension of the tip.
	serial *serialHeaderExtension

	// reorg is set for a serial reorg, with reorgNodes holding the headers
	// of the new branch for the validation cache.
	reorg      *serialHeaderReorg
	reorgNodes []headerlist.Node

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
// checkpoint cursor and the FSM's committed tip. The actor has already updated
// the durable and published state by the time we get here.
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

	// The mutation may have applied, fully or in part. We reconcile on the
	// actor before reading or publishing any durable state. This covers
	// range, serial, reorg and checkpoint rollback commits alike.
	case result.Reply.Err != nil:
		prepared.snapshot = true
		b.requestChainStoreSnapshot()

		return
	}

	b.pendingHeaderCommit = nil

	// The command never ran, so nothing changed and the staged work is
	// still there for the retry.
	if result.Err != nil {
		log.Warnf("headersync actor admission failed: %v", result.Err)
		b.scheduleHeaderCommitRetry()

		return
	}

	switch {
	case prepared.checkpoint != nil:
		b.finishCheckpointRollback(peers, prepared)

	case prepared.reorg != nil:
		b.finishSerialHeaderReorg(peers, prepared)

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
// BlockTipValid means the actor finished any retained rollback and read the
// block tip. A failure on the filter side leaves Err set, but we can still
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
	b.headerList.ResetHeaderState(node)
	b.nextCheckpoint = b.findNextHeaderCheckpoint(node.Height)
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
	b.headerList.ResetHeaderState(node)
	b.updateNextCheckpoint(uint32(node.Height))
	b.advanceHeaderSyncTip(uint32(node.Height), node.Header.BlockHash())

	b.rejectCheckpointPeer(peers, prepared.message.peer)
}

// finishSerialHeaderReorg rebuilds the validation cache on the new branch, then
// asks for whatever comes after it.
func (b *blockManager) finishSerialHeaderReorg(peers *list.List,
	prepared *pendingChainHeaderCommit) {

	b.headerList.ResetHeaderState(prepared.reorg.fork)
	for _, node := range prepared.reorgNodes {
		b.headerList.PushBack(node)
	}
	for _, node := range prepared.reorgNodes {
		b.blkHeaderProgressLogger.LogBlockHeight(
			node.Header.Timestamp, node.Height,
		)
	}

	last := prepared.reorgNodes[len(prepared.reorgNodes)-1]
	hash := last.Header.BlockHash()
	b.advanceHeaderSyncTip(uint32(last.Height), hash)
	b.updateNextCheckpoint(uint32(last.Height))

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
// It uses a fresh context because persistence can outlast the original ask.
func (b *blockManager) finishReadyHeaderSyncRanges() error {
	// Persistence and header validation can legitimately consume the short
	// actor-operation deadline under heavy sqlite write pressure. Once the
	// headers are durably written and the visible tip has advanced, use a
	// fresh actor context so the FSM cannot lag behind the block manager
	// just because the pre-write context expired.
	commitCtx, commitCancel := b.headerSyncOpCtx()
	defer commitCancel()

	var commitResult headersync.CommitReadyResult
	err := b.headerSyncAsk(headerSyncAskCommitReady, func() error {
		var err error
		commitResult, err = b.headerSync.Controller().
			CommitReadyRanges(commitCtx)
		return err
	})
	if err != nil {
		b.scheduleHeaderCommitRetry()
		return err
	}
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

// reconcileHeaderTipWithStore fast-forwards the visible tip and validation
// cache to the durable block header tip.
//
// A header write can report an error after it landed: a SQL commit whose
// acknowledgement was lost, or a store that wrote part of a batch before
// failing. Memory then trails disk. Retrying the same batch from memory would
// either rewrite durable heights (a primary-key conflict on SQL, a duplicate
// append that corrupts the legacy flat file) or, once planning has moved the
// FSM to the durable tip, stage ranges that never connect to the stale cache.
// Either way header sync stalls until restart. Every header on disk passed
// contextual validation before it was written, so adopting it here is safe.
func (b *blockManager) reconcileHeaderTipWithStore() error {
	back := b.headerList.Back()
	if back == nil {
		return fmt.Errorf("header list missing previous element")
	}
	_, height, err := b.cfg.BlockHeaders.ChainTip()
	if err != nil {
		return err
	}
	if height <= uint32(back.Height) {
		return nil
	}

	prev := *back
	for next := uint32(back.Height) + 1; next <= height; next++ {
		header, err := b.cfg.BlockHeaders.FetchHeaderByHeight(next)
		if err != nil {
			return err
		}

		// A durable chain that does not extend the cache is a reorg,
		// which the serial path owns. Leave the cache alone.
		if header.PrevBlock != prev.Header.BlockHash() {
			return fmt.Errorf("durable header height=%d does not "+
				"extend cached tip height=%d", next,
				prev.Height)
		}
		prev = headerlist.Node{Header: *header, Height: int32(next)}
		b.headerList.PushBack(prev)
	}

	log.Infof("headersync reconciled visible tip height=%d -> %d with "+
		"durable store", back.Height, height)

	b.updateNextCheckpoint(height)

	return nil
}

// updateNextCheckpoint moves the handler-owned checkpoint cursor past height.
// The chain store actor publishes the visible tip when it commits headers.
func (b *blockManager) updateNextCheckpoint(height uint32) {
	b.nextCheckpoint = b.findNextHeaderCheckpoint(int32(height))
}

// trimDurableHeaderSyncRange drops the prefix of a ready range that is already
// durable, so a retry after an ambiguous write appends only the missing
// suffix. It reports pending=false when the whole range is durable; the
// caller then skips the write and only commits the range in the FSM. A range
// whose durable prefix disagrees with its staged headers is returned
// unchanged, so validation classifies it as before.
func (b *blockManager) trimDurableHeaderSyncRange(rng headersync.HeaderRange,
	staged headersync.StagedRange) (headerSyncRangeCommit, bool, error) {

	commit := headerSyncRangeCommit{Range: rng, Staged: staged}
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

type headerSyncRangeCommit struct {
	Range  headersync.HeaderRange
	Staged headersync.StagedRange
}

// headerSyncValidationError distinguishes permanently invalid peer data from
// retryable storage or lookup failures. Only the former invalidates staging.
type headerSyncValidationError struct {
	rangeID headersync.RangeID
	peerID  string
	err     error
}

func (e *headerSyncValidationError) Error() string { return e.err.Error() }

func (e *headerSyncValidationError) Unwrap() error { return e.err }

func (b *blockManager) rejectInvalidHeaderCommit(peers *list.List,
	err error) bool {

	var invalid *headerSyncValidationError
	if !errors.As(err, &invalid) {
		return false
	}
	for element := peers.Front(); element != nil; element = element.Next() {
		peer := element.Value.(*ServerPeer)
		if peer.Addr() == invalid.peerID {
			b.rejectHeaderSyncRangePeer(
				peers, peer, invalid.rangeID,
			)
			break
		}
	}
	b.continueHeaderSync(peers)
	return true
}

type headerSyncVerifiedCheckpoint struct {
	Height  int32
	Hash    chainhash.Hash
	PeerID  string
	RangeID headersync.RangeID
}

type headerSyncPreparedCommit struct {
	ExpectedParent      headerfs.BlockStamp
	Headers             []headerfs.BlockHeader
	Nodes               []headerlist.Node
	VerifiedCheckpoints []headerSyncVerifiedCheckpoint
	FinalHeight         int32
	FinalHash           chainhash.Hash
	NextCheckpoint      *chaincfg.Checkpoint
}

// headerSyncValidationChain overlays headers prepared in the current commit
// batch on top of the long-lived header cache. Contextual validation can then
// see freshly prepared parents without losing access to older cached
// ancestors.
type headerSyncValidationChain struct {
	base    headerlist.Chain
	pending *headerlist.BoundedMemoryChain
}

func newHeaderSyncValidationChain(base headerlist.Chain,
	seed headerlist.Node) *headerSyncValidationChain {

	pending := headerlist.NewBoundedMemoryChain(numMaxMemHeaders)
	pending.ResetHeaderState(seed)

	return &headerSyncValidationChain{
		base:    base,
		pending: pending,
	}
}

func (h *headerSyncValidationChain) ResetHeaderState(n headerlist.Node) {
	h.pending.ResetHeaderState(n)
}

func (h *headerSyncValidationChain) Back() *headerlist.Node {
	if back := h.pending.Back(); back != nil {
		return back
	}

	return h.base.Back()
}

func (h *headerSyncValidationChain) Front() *headerlist.Node {
	return h.base.Front()
}

func (h *headerSyncValidationChain) PushBack(
	n headerlist.Node) *headerlist.Node {

	return h.pending.PushBack(n)
}

func (h *headerSyncValidationChain) AtHeight(
	height int32) (*headerlist.Node, bool) {

	if node, ok := h.pending.AtHeight(height); ok {
		return node, true
	}

	return h.base.AtHeight(height)
}

func (b *blockManager) prepareHeaderSyncRangeHeaders(
	ranges []headerSyncRangeCommit) (headerSyncPreparedCommit, error) {

	var prepared headerSyncPreparedCommit
	if len(ranges) == 0 {
		return prepared, nil
	}

	totalHeaders := 0
	for _, commitRange := range ranges {
		totalHeaders += len(commitRange.Staged.Headers)
	}
	if totalHeaders == 0 {
		return prepared, headersync.ErrNoHeaders
	}

	prepared.Headers = make([]headerfs.BlockHeader, 0, totalHeaders)
	prepared.Nodes = make([]headerlist.Node, 0, totalHeaders)
	prepared.NextCheckpoint = b.nextCheckpoint

	prevNodeEl := b.headerList.Back()
	if prevNodeEl == nil {
		return prepared, fmt.Errorf(
			"header list missing previous element",
		)
	}

	prevHash := prevNodeEl.Header.BlockHash()
	prevHeight := prevNodeEl.Height
	prevHeader := prevNodeEl.Header
	prepared.ExpectedParent = headerfs.BlockStamp{
		Height:    prevHeight,
		Hash:      prevHash,
		Timestamp: prevHeader.Timestamp,
	}

	validationList := newHeaderSyncValidationChain(
		b.headerList, *prevNodeEl,
	)

	for _, commitRange := range ranges {
		rng := commitRange.Range
		headers := commitRange.Staged.Headers
		peerID := commitRange.Staged.PeerID
		invalid := func(err error) error {
			return &headerSyncValidationError{
				rangeID: rng.ID, peerID: peerID, err: err,
			}
		}

		if len(headers) == 0 {
			return prepared, invalid(headersync.ErrNoHeaders)
		}

		var rangeFinalHeight int32
		for _, blockHeader := range headers {
			blockHash := blockHeader.BlockHash()
			if !prevHash.IsEqual(&blockHeader.PrevBlock) {
				err := fmt.Errorf("headersync staged "+
					"range_id=%d does not connect to "+
					"current "+
					"tip hash=%s prev=%s", rng.ID, prevHash,
					blockHeader.PrevBlock)
				if uint32(prevHeight) == rng.StartHeight &&
					prevHash == rng.StartHash {

					return prepared, invalid(err)
				}
				return prepared, err
			}

			if err := b.checkHeaderSanity(
				blockHeader, false, prevHeight, &prevHeader,
				validationList,
			); err != nil {
				var ruleErr blockchain.RuleError
				if errors.As(err, &ruleErr) {
					err = invalid(err)
				}
				return prepared, fmt.Errorf("headersync "+
					"range_id=%d header sanity failed: %w",
					rng.ID, err)
			}

			node := headerlist.Node{
				Header: *blockHeader,
				Height: prevHeight + 1,
			}
			rangeFinalHeight = node.Height

			headerCopy := *blockHeader
			prepared.Headers = append(prepared.Headers,
				headerfs.BlockHeader{
					BlockHeader: &headerCopy,
					Height:      uint32(node.Height),
				},
			)
			prepared.Nodes = append(prepared.Nodes, node)
			validationList.PushBack(node)

			if prepared.NextCheckpoint != nil &&
				node.Height == prepared.NextCheckpoint.Height {

				if !blockHash.IsEqual(
					prepared.NextCheckpoint.Hash,
				) {

					err := fmt.Errorf("headersync "+
						"checkpoint mismatch "+
						"height=%d "+
						"got=%s want=%s peer=%s",
						node.Height, blockHash,
						prepared.NextCheckpoint.Hash,
						peerID)
					return prepared, invalid(err)
				}

				prepared.VerifiedCheckpoints = append(
					prepared.VerifiedCheckpoints,
					headerSyncVerifiedCheckpoint{
						Height:  node.Height,
						Hash:    blockHash,
						PeerID:  peerID,
						RangeID: rng.ID,
					},
				)
				checkpoint := b.findNextHeaderCheckpoint(
					node.Height,
				)
				prepared.NextCheckpoint = checkpoint
			}

			prepared.FinalHash = blockHash
			prepared.FinalHeight = node.Height
			prevHash = blockHash
			prevHeight = node.Height
			prevHeader = node.Header
		}

		if rangeFinalHeight != int32(rng.StopHeight) {
			err := fmt.Errorf("headersync range_id=%d "+
				"final_height=%d stop_height=%d", rng.ID,
				rangeFinalHeight, rng.StopHeight)
			return prepared, invalid(err)
		}
	}

	return prepared, nil
}

// finishHeaderSyncRangeHeaders updates handler-owned validation state after a
// prepared batch has been durably committed.
func (b *blockManager) finishHeaderSyncRangeHeaders(
	prepared headerSyncPreparedCommit) {

	verifiedByHeight := make(
		map[int32]headerSyncVerifiedCheckpoint,
		len(prepared.VerifiedCheckpoints),
	)
	for _, checkpoint := range prepared.VerifiedCheckpoints {
		verifiedByHeight[checkpoint.Height] = checkpoint
	}

	for _, node := range prepared.Nodes {
		b.blkHeaderProgressLogger.LogBlockHeight(
			node.Header.Timestamp, node.Height,
		)

		b.headerList.PushBack(node)
		if checkpoint, ok := verifiedByHeight[node.Height]; ok {
			log.Infof("headersync verified checkpoint height=%d "+
				"hash=%s peer=%s range_id=%d",
				checkpoint.Height,
				checkpoint.Hash, checkpoint.PeerID,
				checkpoint.RangeID)
		}
	}

	b.updateNextCheckpoint(uint32(prepared.FinalHeight))
}
