package neutrino

// This file holds the header executor's side of header sync: it registers
// peers with the driver, reports request deadlines to it, and finishes the one
// chain store mutation in flight by handing its outcome to the driver.

import (
	"container/list"
	"context"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
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
	}
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

func removeSyncCandidate(peers *list.List, peer *ServerPeer) {
	for e := peers.Front(); e != nil; e = e.Next() {
		if e.Value == peer {
			peers.Remove(e)
			return
		}
	}
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

func (b *blockManager) allowSerialHeaderFallback() bool {
	return b.cfg.ChainParams.Net == chaincfg.SimNetParams.Net ||
		b.cfg.ChainParams.Net == chaincfg.RegressionNetParams.Net
}

// handleHeaderSyncTimeoutMsg reports an elapsed request deadline to the
// driver. A serial deadline only counts while it is the one in flight, from
// the sync peer, and headers are not synced.
func (b *blockManager) handleHeaderSyncTimeoutMsg(peers *list.List,
	msg *headerSyncTimeoutMsg) {

	if msg.anchor {
		b.handleDriverEvent(peers, headersync.RequestTimedOut{
			PeerID:      msg.peer.Addr(),
			Kind:        headersync.RequestAnchor,
			RequestID:   msg.requestID,
			StartHeight: msg.startHeight,
			Timeout:     msg.timeout,
		}, msg.peer)
		return
	}

	if msg.rangeID != headersync.NoRange {
		b.handleDriverEvent(peers, headersync.RequestTimedOut{
			PeerID:      msg.peer.Addr(),
			Kind:        headersync.RequestRange,
			RequestID:   msg.requestID,
			StartHeight: msg.startHeight,
			RangeID:     msg.rangeID,
			LeaseEpoch:  msg.leaseEpoch,
			Timeout:     msg.timeout,
			StaleFor:    b.headerTimeoutForPeer(msg.peer),
		}, msg.peer)
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
