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

// driverItem is one step of the agenda handleDriverEvent works through: an
// event to hand to the driver, or an effect to execute.
type driverItem struct {
	event  headersync.DriverEvent
	effect headersync.DriverEffect
}

// handleDriverEvent hands one event to the header sync driver and executes
// the effects it returns, in order. The block manager is the driver's
// executor: it owns peer I/O, timers and the chain store, and reports the
// outcome of an effect that can fail back as a further event. It must be
// called from the block handler's goroutine, which serializes the driver.
//
// The effects are the outbox of the driver's state machine, which never
// dispatches them itself. The follow-up events of an effect, and the events
// of a Then effect, run here in a loop over one agenda rather than inside the
// driver or by calling back into this function, so that each sees a fresh view
// and the state the effects before it produced, and the driver is never
// entered from one of its own handlers or effects. An event's effects run
// before whatever followed the event on the agenda.
//
// Effects find their peer among the candidate peers, or among known, which
// holds the peer that an event came from when it may not be a candidate.
func (b *blockManager) handleDriverEvent(peers *list.List,
	event headersync.DriverEvent, known ...*ServerPeer) driverOutcome {

	var (
		outcome driverOutcome
		agenda  = []driverItem{{event: event}}
		first   = true
	)
	push := func(items ...driverItem) {
		// The next item to run is the last one on the agenda.
		for i := len(items) - 1; i >= 0; i-- {
			agenda = append(agenda, items[i])
		}
	}

	for len(agenda) > 0 {
		item := agenda[len(agenda)-1]
		agenda = agenda[:len(agenda)-1]

		switch {
		case item.event != nil:
			effects, err := b.processDriverEvent(peers, item.event)
			if first {
				outcome.err = err
				first = false
			}

			items := make([]driverItem, 0, len(effects))
			for _, eff := range effects {
				items = append(items, driverItem{effect: eff})
			}
			push(items...)

		default:
			if then, ok := item.effect.(headersync.Then); ok {
				items := make([]driverItem, 0, len(then.Events))
				for _, ev := range then.Events {
					item := driverItem{event: ev}
					items = append(items, item)
				}
				push(items...)

				continue
			}

			sent, followUps := b.executeDriverEffect(
				peers, known, item.effect,
			)
			if sent {
				outcome.sent++
			}

			items := make([]driverItem, 0, len(followUps))
			for _, next := range followUps {
				items = append(items, driverItem{event: next})
			}
			push(items...)
		}
	}

	return outcome
}

// processDriverEvent runs one event through the driver under the deadline of
// a header sync operation, with a view of the world as it is now.
func (b *blockManager) processDriverEvent(peers *list.List,
	event headersync.DriverEvent) ([]headersync.DriverEffect, error) {

	ctx, cancel := b.headerSyncOpCtx()
	defer cancel()

	return b.driver().ProcessEvent(ctx, event, b.driverView(peers))
}

// driverView snapshots the state the driver reads but the block manager owns.
func (b *blockManager) driverView(peers *list.List) headersync.View {
	var view headersync.View
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
// effect was a getheaders request that was written to its peer, and returns the
// events that report the outcome of the effect back to the driver.
func (b *blockManager) executeDriverEffect(peers *list.List,
	known []*ServerPeer, effect headersync.DriverEffect) (bool,
	[]headersync.DriverEvent) {

	switch effect := effect.(type) {
	case headersync.Deliver:
		b.handlePeerMessage(peers, effect.Message)

	case headersync.ArmCooldownWake:
		b.armHeaderCooldownWake(effect.Until)

	case headersync.SendGetHeaders:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			log.Warnf("headersync driver asked for a request to "+
				"unknown peer %s", effect.PeerID)
			return false, nil
		}

		startHash := effect.StartHash
		stopHash := effect.StopHash
		locator := blockchain.BlockLocator(
			[]*chainhash.Hash{&startHash},
		)
		err := b.sendHeaderRequest(sp, locator, &stopHash)
		if err != nil {
			return false, []headersync.DriverEvent{
				headersync.RequestFailed{
					Request: effect,
					Err:     err,
				},
			}
		}

		return true, []headersync.DriverEvent{headersync.RequestSent{
			Request: effect,
		}}

	case headersync.ArmRequestTimer:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false, nil
		}
		timer := b.armHeaderSyncRequestTimer(sp, effect)

		return false, []headersync.DriverEvent{headersync.TimerArmed{
			PeerID:    effect.PeerID,
			Kind:      effect.Kind,
			RequestID: effect.RequestID,
			Timer:     timer,
		}}

	case headersync.QuarantinePeer:
		b.quarantineHeaderSyncAddress(effect.PeerID)

	case headersync.DisconnectPeer:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false, nil
		}
		removeSyncCandidate(peers, sp)
		sp.Disconnect()

	case headersync.DropCandidate:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false, nil
		}
		removeSyncCandidate(peers, sp)

	case headersync.DisconnectTipPeer:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false, nil
		}
		sp.Disconnect()

	case headersync.UpdatePeerHeight:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false, nil
		}
		sp.UpdateLastBlockHeight(effect.Height)

	case headersync.DisarmSerialTimeout:
		b.disarmHeaderSyncTimeout()

	case headersync.NoProgressReply:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false, nil
		}
		b.handleNoProgressReply(peers, sp)

	case headersync.CommitExtension:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false, abandonedCommit()
		}
		b.submitSerialExtension(sp, effect)

	case headersync.CommitReorg:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false, abandonedCommit()
		}
		b.submitSerialHeaderReorg(sp, effect)

	case headersync.CheckpointRollback:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false, abandonedCommit()
		}
		b.submitCheckpointRollback(sp, effect)

	case headersync.ScheduleCommitRetry:
		b.scheduleHeaderCommitRetry()

	case headersync.CancelCommitRetry:
		b.cancelHeaderCommitRetry()

	case headersync.SetSyncPeer:
		if effect.PeerID == "" {
			b.setSyncPeer(nil)
			return false, nil
		}
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			log.Warnf("headersync driver named unknown sync "+
				"peer %s", effect.PeerID)
			return false, nil
		}
		if effect.UnlessGone && peerDisconnected(sp) {
			return false, nil
		}
		b.setSyncPeer(sp)

	case headersync.ContinueSerial:
		sp := driverPeer(peers, known, effect.PeerID)
		if sp == nil {
			return false, nil
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
			return false, nil
		}
		b.startSerialSync(peers, sp, effect)

	case headersync.ClearBootstrapDelay:
		b.clearHeaderSyncBootstrapDelay()

	default:
		log.Warnf("headersync driver emitted unknown effect %T", effect)
	}

	return false, nil
}

// abandonedCommit returns the event that tells the driver a mutation it asked
// for was never submitted, so it stops waiting on a result that will not come.
func abandonedCommit() []headersync.DriverEvent {
	log.Warnf("headersync driver asked for a commit from an unknown peer")

	return []headersync.DriverEvent{headersync.CommitAbandoned{}}
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
