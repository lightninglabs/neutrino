package neutrino

// This file holds the header executor's glue to the headersync driver: it runs
// one driver event and reports its outcome to the block handler.

import (
	"container/list"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/lightninglabs/neutrino/headersync"
)

// driverOutcome is what executing one driver event produced.
type driverOutcome struct {
	// err is the error the driver reported for the event, if any.
	err error

	// sent is the number of getheaders requests of the event that were
	// written to a peer.
	sent int
}

// driver returns the header sync driver, which owns the validation cache.
func (b *blockManager) driver() *headersync.Driver {
	return b.headerSync.Driver()
}

// handleDriverEvent hands one event to the header sync driver and executes
// the effects it returns, in order. The block manager is the driver's
// executor: it owns peer I/O, timers and the chain store, and reports the
// outcome of an effect that can fail back as a further event. It must be
// called from the block handler's goroutine, which serializes the driver.
//
// The effects are the outbox of the driver's state machine, which never
// dispatches them itself. A Then effect carries follow-up events, which run
// here rather than inside the machine so that each sees a fresh view and the
// state the effects before it produced.
//
// Effects find their peer among the candidate peers, or among known, which
// holds the peer that an event came from when it may not be a candidate.
func (b *blockManager) handleDriverEvent(peers *list.List,
	event headersync.DriverEvent, known ...*ServerPeer) driverOutcome {

	ctx, cancel := b.headerSyncOpCtx()
	defer cancel()

	var outcome driverOutcome
	effects, err := b.driver().ProcessEvent(
		ctx, event, b.driverView(peers),
	)
	outcome.err = err

	for _, effect := range effects {
		if then, ok := effect.(headersync.Then); ok {
			for _, next := range then.Events {
				nested := b.handleDriverEvent(
					peers, next, known...,
				)
				outcome.sent += nested.sent
			}

			continue
		}
		if b.executeDriverEffect(peers, known, effect) {
			outcome.sent++
		}
	}

	return outcome
}

// driverView snapshots the state the driver reads but the block manager owns.
func (b *blockManager) driverView(peers *list.List) headersync.View {
	view := headersync.View{
		CommitPending: b.pendingHeaderCommit != nil,
	}
	if peers != nil {
		view.Peers = headerSyncPeerCandidates(peers)
	}

	return view
}

// driverPeer finds the peer an effect is about.
func driverPeer(peers *list.List, known []*ServerPeer,
	peerID string) *ServerPeer {

	for _, sp := range known {
		if sp != nil && sp.Addr() == peerID {
			return sp
		}
	}
	if peers != nil {
		for e := peers.Front(); e != nil; e = e.Next() {
			sp := e.Value.(*ServerPeer)
			if sp.Addr() == peerID {
				return sp
			}
		}
	}

	return nil
}

// executeDriverEffect performs one driver effect. It reports whether the
// effect was a getheaders request that was written to its peer.
func (b *blockManager) executeDriverEffect(peers *list.List,
	known []*ServerPeer, effect headersync.DriverEffect) bool {

	switch effect := effect.(type) {
	case headersync.ArmCooldownWake:
		b.armHeaderCooldownWake(effect.Until)

	case headersync.SendGetHeaders:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			log.Warnf("headersync driver asked for a request to "+
				"unknown peer %s", effect.PeerID)
			return false
		}

		startHash := effect.StartHash
		stopHash := effect.StopHash
		locator := blockchain.BlockLocator(
			[]*chainhash.Hash{&startHash},
		)
		err := b.sendHeaderRequest(sp, locator, &stopHash)
		if err != nil {
			b.handleDriverEvent(peers, headersync.RequestFailed{
				Request: effect,
				Err:     err,
			}, known...)
			return false
		}

		b.handleDriverEvent(peers, headersync.RequestSent{
			Request: effect,
		}, known...)

		return true

	case headersync.ArmRequestTimer:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false
		}
		timer := b.armHeaderSyncRequestTimer(sp, effect)
		b.handleDriverEvent(peers, headersync.TimerArmed{
			PeerID:    effect.PeerID,
			Kind:      effect.Kind,
			RequestID: effect.RequestID,
			Timer:     timer,
		}, known...)

	case headersync.QuarantinePeer:
		b.quarantineHeaderSyncAddress(effect.PeerID)

	case headersync.DisconnectPeer:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false
		}
		removeSyncCandidate(peers, sp)
		sp.Disconnect()

	case headersync.DropCandidate:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false
		}
		removeSyncCandidate(peers, sp)

	case headersync.DisconnectTipPeer:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false
		}
		sp.Disconnect()

	case headersync.UpdatePeerHeight:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false
		}
		sp.UpdateLastBlockHeight(effect.Height)

	case headersync.DisarmSerialTimeout:
		b.disarmHeaderSyncTimeout()

	case headersync.NoProgressReply:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false
		}
		b.handleNoProgressReply(peers, sp)

	case headersync.CommitExtension:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false
		}
		b.submitSerialExtension(sp, effect)

	case headersync.CommitReorg:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false
		}
		b.submitSerialHeaderReorg(sp, effect)

	case headersync.CheckpointRollback:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false
		}
		b.submitCheckpointRollback(sp, effect)

	case headersync.ScheduleCommitRetry:
		b.scheduleHeaderCommitRetry()

	case headersync.CancelCommitRetry:
		b.cancelHeaderCommitRetry()

	case headersync.SetSyncPeer:
		if effect.PeerID == "" {
			b.setSyncPeer(nil)
			return false
		}
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			log.Warnf("headersync driver named unknown sync "+
				"peer %s", effect.PeerID)
			return false
		}
		if effect.UnlessGone && peerDisconnected(sp) {
			return false
		}
		b.setSyncPeer(sp)

	case headersync.ContinueSerial:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false
		}
		b.finishSerialHeaderFollowup(peers, sp, effect)

	case headersync.SubmitRangeCommit:
		b.submitRangeCommit(effect.Prepared)

	case headersync.StartSync:
		b.startSync(peers)

	case headersync.StartSerialSync:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			log.Warnf("headersync driver named unknown serial "+
				"sync peer %s", effect.PeerID)
			return false
		}
		b.startSerialSync(peers, sp, effect)

	case headersync.ClearBootstrapDelay:
		b.clearHeaderSyncBootstrapDelay()

	default:
		log.Warnf("headersync driver emitted unknown effect %T", effect)
	}

	return false
}

// armHeaderSyncRequestTimer arms the deadline of a tracked request. When it
// fires, the timeout is queued for the block handler.
func (b *blockManager) armHeaderSyncRequestTimer(sp *ServerPeer,
	effect headersync.ArmRequestTimer) *time.Timer {

	timeout := b.headerTimeoutForPeer(sp)

	return time.AfterFunc(timeout, func() {
		b.sendHeaderTimeout(&headerSyncTimeoutMsg{
			peer:        sp,
			requestID:   effect.RequestID,
			startHeight: effect.StartHeight,
			timeout:     timeout,
			rangeID:     effect.RangeID,
			leaseEpoch:  effect.LeaseEpoch,
			anchor:      effect.Kind == headersync.RequestAnchor,
		})
	})
}
