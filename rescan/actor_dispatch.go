package rescan

import (
	"context"
	"fmt"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2"

	lndactor "github.com/lightningnetwork/lnd/actor"
	fn "github.com/lightningnetwork/lnd/fn/v2"
)

// Receive processes mailbox messages one at a time in the lnd actor runtime.
func (a *RescanActor) Receive(_ context.Context,
	msg RescanActorMsg) fn.Result[RescanActorResp] {

	a.turnMu.Lock()
	defer a.turnMu.Unlock()

	// Once Stop has cancelled the actor, only the stop barrier and state
	// queries still run, so no callback can start after WaitForShutdown.
	if a.ctx.Err() != nil {
		switch msg.(type) {
		case *stopRescanMsg, *currentStateReq:
		default:
			return fn.Err[RescanActorResp](ErrStopped)
		}
	}

	switch m := msg.(type) {
	case *processNextBlockMsg:
		a.continuationPending = false
		return a.handleContinuation()

	case *blockConnectedMsg:
		// A request waits behind every notification queued ahead of
		// it, and Current scans each one. While one waits, hand the
		// scan to Syncing instead, which catches up from the header
		// store and yields after the block in progress, so the
		// request still waits for one block at most.
		if a.requests.Load() > 0 {
			_, current := a.machine.CurrentState().(*StateCurrent)
			if current {
				return a.handleContinuation()
			}
		}

		return a.handleFSMEvent(BlockConnectedEvent{
			Header: m.Header,
			Height: m.Height,
		})

	case *blockDisconnectedMsg:
		return a.handleFSMEvent(BlockDisconnectedEvent{
			Header:   m.Header,
			Height:   m.Height,
			ChainTip: m.ChainTip,
		})

	case *addWatchAddrsMsg:
		silent := m.DisableDisconnectedNtfns
		return a.handleFSMEvent(AddWatchAddrsEvent{
			Addrs:                    m.Addrs,
			Inputs:                   m.Inputs,
			RewindTo:                 m.RewindTo,
			DisableDisconnectedNtfns: silent,
		})

	case *currentStateReq:
		return fn.Ok[RescanActorResp](&currentStateResp{
			State: snapshotState(a.machine.CurrentState()),
		})

	case *subscriptionFailedMsg:
		return a.handleSubscriptionFailed(m)

	case *stopRescanMsg:
		return a.handleStop()

	default:
		return fn.Err[RescanActorResp](fmt.Errorf(
			"unknown message type: %T", msg,
		))
	}
}

// handleContinuation runs a batch of up to BatchSize Syncing or Rewinding
// steps, one block each. It dispatches each step's callbacks before it takes
// the next, as the legacy rescan notified each block before it scanned the
// next, and it ends the batch early once a request is waiting, so that
// request waits for one block at most. A failed step leaves the state
// unchanged. It is reported through OnError and retried after
// RetryInterval, unless Stop caused it by cancelling the step's fetch.
func (a *RescanActor) handleContinuation() fn.Result[RescanActorResp] {
	for i := 0; i < a.cfg.BatchSize; i++ {
		outbox, err := a.machine.ProcessEvent(
			a.ctx, ProcessNextBlockEvent{},
		)
		if err != nil {
			return a.stepFailed(err)
		}

		if !a.dispatchOutbox(outbox) {
			return fn.Ok[RescanActorResp](&rescanActorAck{})
		}

		if a.requests.Load() > 0 || a.ctx.Err() != nil {
			break
		}
	}

	a.enqueueContinuation()

	return fn.Ok[RescanActorResp](&rescanActorAck{})
}

// stepFailed reports a failed step and schedules its retry. A step that
// failed because Stop cancelled its fetch is not an error the caller needs to
// see: it reports nothing, and nothing retries it.
func (a *RescanActor) stepFailed(err error) fn.Result[RescanActorResp] {
	if a.ctx.Err() != nil {
		log.Debugf("RescanActor: step cancelled by stop: %v", err)
		return fn.Err[RescanActorResp](ErrStopped)
	}

	log.Warnf("RescanActor: error processing next block: %v, "+
		"retrying in %v", err, a.cfg.RetryInterval)

	if a.cfg.OnError != nil {
		a.cfg.OnError(err)
	}

	a.scheduleRetry()

	return fn.Err[RescanActorResp](err)
}

// handleFSMEvent applies one event to the FSM in the current turn and, once
// the transition has committed, dispatches its outbox. A failed event leaves
// the state unchanged and dispatches nothing.
func (a *RescanActor) handleFSMEvent(
	event RescanEvent) fn.Result[RescanActorResp] {

	outbox, err := a.machine.ProcessEvent(a.ctx, event)
	if err != nil {
		return fn.Err[RescanActorResp](err)
	}

	if a.dispatchOutbox(outbox) {
		a.enqueueContinuation()
	}

	return fn.Ok[RescanActorResp](&rescanActorAck{})
}

// scheduleRetry sends a continuation after RetryInterval, unless the actor
// stops first.
func (a *RescanActor) scheduleRetry() {
	// Once Stop has cancelled the actor, a turn that outlived the stop
	// barrier must not add to asyncWG while WaitForShutdown may already be
	// waiting on it. Stop cancels before it returns, so a retry added
	// after this check is still added before WaitForShutdown can run.
	if a.ctx.Err() != nil {
		return
	}

	a.asyncWG.Add(1)
	go func() {
		defer a.asyncWG.Done()

		timer := time.NewTimer(a.cfg.RetryInterval)
		defer timer.Stop()

		select {
		case <-a.ctx.Done():
			return

		case <-timer.C:
		}

		a.ref.Tell(a.ctx, &processNextBlockMsg{})
	}()
}

// handleStop moves the FSM to Terminal. Stop sends it as a barrier: its
// answer means the turn that was in flight when Stop began has finished.
func (a *RescanActor) handleStop() fn.Result[RescanActorResp] {

	return a.handleFSMEvent(StopEvent{})
}

// handleSubscriptionFailed ends the rescan when its block subscription could
// not be created or closed underneath it. Both happen only once the
// ChainService has stopped, so the rescan reports the error once and moves
// to Terminal, as the legacy rescan loop does, rather than retrying.
func (a *RescanActor) handleSubscriptionFailed(
	msg *subscriptionFailedMsg) fn.Result[RescanActorResp] {

	// Only one subscriptionFailedMsg is ever sent: Start sends it instead
	// of starting the bridge, or the bridge sends it and exits. Receive
	// drops it once Stop has run.
	log.Errorf("RescanActor: %v, stopping", msg.Err)
	if a.cfg.OnError != nil {
		a.cfg.OnError(msg.Err)
	}
	a.cancelBridge()
	a.finish(msg.Err)

	return a.handleFSMEvent(StopEvent{})
}

// dispatchOutbox delivers the outbox of a committed transition: wallet
// callbacks in emission order, and the request for a retry. It reports
// whether the transition asked for a continuation, which the caller sends.
// Delivery stops once Stop has cancelled the actor.
func (a *RescanActor) dispatchOutbox(outbox []OutboxEvent) bool {
	callbacks := a.cfg.Callbacks
	continued := false
	for _, event := range outbox {
		select {
		case <-a.ctx.Done():
			return false
		default:
		}

		switch e := event.(type) {
		case FilteredBlockOutbox:
			a.dispatchMatches(e.RelevantTxs, e.Matches)

			if callbacks.OnFilteredBlockConnected != nil {
				callbacks.OnFilteredBlockConnected(
					e.Height, e.Header, e.RelevantTxs,
				)
			}

		case BlockConnectedOutbox:
			if callbacks.OnBlockConnected != nil {
				callbacks.OnBlockConnected(
					&e.Hash, e.Height, e.Header.Timestamp,
				)
			}

		case BlockDisconnectedOutbox:
			a.dispatchDisconnect(e)

		case RescanProgressOutbox:
			if callbacks.OnRescanProgress != nil {
				callbacks.OnRescanProgress(e.Height)
			}

		case RescanFinishedOutbox:
			if callbacks.OnRescanFinished != nil {
				callbacks.OnRescanFinished(e.Height)
			}

		case EndReachedOutbox:
			log.Debugf("RescanActor: reached the end block")
			a.cancelBridge()
			a.finish(nil)

		case WaitOutbox:
			a.scheduleRetry()

		case ContinueOutbox:
			continued = true
		}
	}

	return continued
}

// dispatchMatches calls the per-transaction callbacks of a scanned block and
// marks each relevant transaction confirmed, in the legacy rescan's order:
// for each transaction, OnRedeemingTx, OnRecvTx, then MarkAsConfirmed.
func (a *RescanActor) dispatchMatches(txs []*btcutil.Tx, matches []TxMatch) {
	callbacks := a.cfg.Callbacks
	for i, tx := range txs {
		match := matches[i]
		if match.Spends && callbacks.OnRedeemingTx != nil {
			details := match.Details
			callbacks.OnRedeemingTx(tx, &details)
		}
		if match.Pays && callbacks.OnRecvTx != nil {
			details := match.Details
			callbacks.OnRecvTx(tx, &details)
		}

		a.cfg.Chain.MarkAsConfirmed(*tx.Hash())
	}
}

// dispatchDisconnect calls the two disconnect callbacks of an undone block.
// A disconnect notification reports the filtered callback first, and a
// rewind the plain one first, as the legacy rescan does.
func (a *RescanActor) dispatchDisconnect(e BlockDisconnectedOutbox) {
	callbacks := a.cfg.Callbacks
	filtered := func() {
		if callbacks.OnFilteredBlockDisconnected != nil {
			callbacks.OnFilteredBlockDisconnected(
				e.Height, e.Header,
			)
		}
	}
	plain := func() {
		if callbacks.OnBlockDisconnected != nil {
			callbacks.OnBlockDisconnected(
				&e.Hash, e.Height, e.Header.Timestamp,
			)
		}
	}

	if e.Rewind {
		plain()
		filtered()
		return
	}

	filtered()
	plain()
}

// enqueueContinuation sends a continuation to the actor's own mailbox. The
// send runs on its own goroutine, because a Tell from inside the turn would
// block forever if the mailbox were full. continuationPending keeps at most one
// such continuation in flight, so a burst of outboxes cannot pile them up.
func (a *RescanActor) enqueueContinuation() {
	if a.continuationPending {
		return
	}
	a.continuationPending = true
	a.asyncWG.Add(1)
	go func() {
		defer a.asyncWG.Done()
		a.ref.Tell(a.ctx, &processNextBlockMsg{})
	}()
}

// snapshotState returns a copy of state whose watch list the caller may
// mutate without touching the state the machine holds.
func snapshotState(state RescanState) RescanState {
	switch s := state.(type) {
	case *StateInitializing:
		snapshot := *s
		snapshot.Watch = s.Watch.Clone()
		return &snapshot
	case *StateSyncing:
		snapshot := *s
		snapshot.Watch = s.Watch.Clone()
		return &snapshot
	case *StateCurrent:
		snapshot := *s
		snapshot.Watch = s.Watch.Clone()
		return &snapshot
	case *StateRewinding:
		snapshot := *s
		snapshot.Watch = s.Watch.Clone()
		return &snapshot
	default:
		return state
	}
}

// Ensure the mailbox behavior implements the lnd actor behavior interface.
var _ lndactor.ActorBehavior[RescanActorMsg, RescanActorResp] = (*RescanActor)(
	nil,
)
