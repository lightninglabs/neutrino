package neutrino

// This file holds the header executor's side of parallel header sync: it runs
// the driver's events, keeps peers registered, and turns ready ranges into
// chain store commits.

import (
	"container/list"
	"context"
	"errors"
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

func headerSyncPeerRank(sp *ServerPeer) int {
	return -int(sp.LastBlock())
}

func (b *blockManager) pipelineHeaderFrontierDiscovery(peers *list.List,
	response headersync.AnchorResponse) bool {

	if b.headerSync == nil || b.driver().NextCheckpoint() != nil {
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

// continueHeaderSync asks the driver to carry on: plan, dispatch, restart
// discovery, and start syncing again if nothing is left in flight.
func (b *blockManager) continueHeaderSync(peers *list.List) {
	b.handleDriverEvent(peers, headersync.Continue{})
}

func (b *blockManager) allowSerialHeaderFallback() bool {
	return b.cfg.ChainParams.Net == chaincfg.SimNetParams.Net ||
		b.cfg.ChainParams.Net == chaincfg.RegressionNetParams.Net
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

	b.handleDriverEvent(peers, headersync.SerialTimedOut{
		PeerID:      msg.peer.Addr(),
		StartHeight: msg.startHeight,
		Timeout:     msg.timeout,
	}, msg.peer)
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

	if b.syncPeer.Load() == msg.peer ||
		b.headerSync.Driver().Activity().Anchors == 0 {

		b.setSyncPeer(nil)
	}

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

	b.handleDriverEvent(peers, headersync.DispatchWork{})
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

	if b.headerSync.Driver().Activity().Anchors == 0 {
		b.setSyncPeer(nil)
	}

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

	b.setSyncPeer(nil)

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

	planning := b.handleDriverEvent(nil, headersync.PlanRanges{
		BestHeight: request.StopHeight,
	})
	planned := planning.result.Planned
	if planning.err != nil {
		log.Warnf("headersync unable to plan ranges after anchor "+
			"height=%d: %v", result.Response.Height, planning.err)
	} else if planned > 0 {
		b.handleDriverEvent(peers, headersync.DispatchWork{})
	}

	if result.Response.Height < request.StopHeight {
		b.maybeContinueHeaderAnchorDiscovery(
			peers, request, result.Response.Height,
			result.Response.Hash, "",
		)
		return true
	}

	if planned == 0 && !b.headerSync.Driver().Activity().HasWork() {
		b.setSyncPeer(nil)
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

// commitReadyHeaderSyncRanges asks the driver to commit the staged ranges
// that are ready. The error is a ValidationError when a peer's data is at
// fault.
func (b *blockManager) commitReadyHeaderSyncRanges() error {
	return b.handleDriverEvent(nil, headersync.CommitReady{}).err
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
	reorg      *headersync.SerialReorg
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

	// peer is the peer whose headers caused a serial, reorg or checkpoint
	// mutation.
	peer *ServerPeer
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

	// The driver finishes the mutation: it adopts the result into the
	// validation cache and the FSM, and decides what to ask for next.
	var landed headersync.DriverEvent
	switch {
	case prepared.checkpoint != nil:
		landed = headersync.CheckpointRolledBack{
			PeerID: prepared.peer.Addr(),
			Node:   *prepared.checkpoint,
		}

	case prepared.reorg != nil:
		landed = headersync.ReorgCommitted{
			PeerID:   prepared.peer.Addr(),
			Reorg:    *prepared.reorg,
			Nodes:    prepared.reorgNodes,
			Mismatch: prepared.reorgMismatch,
			Verdict:  result.Reply.Verdict,
		}

	case prepared.serial != nil:
		landed = headersync.ExtensionCommitted{
			PeerID:    prepared.peer.Addr(),
			Extension: *prepared.serial,
		}

	default:
		landed = headersync.RangesCommitted{Prepared: *prepared.ranges}
	}

	b.handleDriverEvent(peers, landed, prepared.peer)
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

	var checkpointPeer string
	if prepared.checkpoint != nil {
		checkpointPeer = prepared.peer.Addr()
	}
	b.handleDriverEvent(peers, headersync.SnapshotAdopted{
		Node: headerlist.Node{
			Header: result.Reply.BlockHeader,
			Height: result.Reply.BlockTip.Height,
		},
		CheckpointPeerID: checkpointPeer,
	}, prepared.peer)
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
