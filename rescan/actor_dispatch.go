package rescan

import (
	"context"
	"fmt"
	"time"

	lndactor "github.com/lightningnetwork/lnd/actor"
	fn "github.com/lightningnetwork/lnd/fn/v2"
)

// Receive processes mailbox messages one at a time in the lnd actor runtime.
func (a *RescanActor) Receive(actorCtx context.Context,
	msg RescanActorMsg) fn.Result[RescanActorResp] {

	switch m := msg.(type) {
	case *processNextBlockMsg:
		a.continuationPending = false
		return a.handleContinuation(actorCtx)

	case *blockConnectedMsg:
		return a.handleFSMEvent(actorCtx, BlockConnectedEvent{
			Header: m.Header,
			Height: m.Height,
		})

	case *blockDisconnectedMsg:
		return a.handleFSMEvent(actorCtx, BlockDisconnectedEvent{
			Header:   m.Header,
			Height:   m.Height,
			ChainTip: m.ChainTip,
		})

	case *addWatchAddrsMsg:
		return a.handleFSMEvent(
			actorCtx,
			AddWatchAddrsEvent{
				Addrs:    m.Addrs,
				Inputs:   m.Inputs,
				RewindTo: m.RewindTo,
			},
		)

	case *currentStateReq:
		return fn.Ok[RescanActorResp](&currentStateResp{
			State: snapshotState(a.machine.CurrentState()),
		})

	case *subscriptionFailedMsg:
		return a.handleSubscriptionFailed(actorCtx, m)

	case *stopRescanMsg:
		return a.handleStop(actorCtx)

	default:
		return fn.Err[RescanActorResp](fmt.Errorf(
			"unknown message type: %T", msg,
		))
	}
}

// handleContinuation runs the next batch. A failed batch leaves the state
// unchanged. It is reported through OnError and retried after
// retryInterval.
func (a *RescanActor) handleContinuation(
	actorCtx context.Context) fn.Result[RescanActorResp] {

	result := a.handleFSMEvent(actorCtx, ProcessNextBlockEvent{})
	if result.IsErr() {
		err := result.Err()
		log.Warnf("RescanActor: error processing next block: %v, "+
			"retrying in %v", err, retryInterval)

		if a.cfg.OnError != nil {
			a.cfg.OnError(err)
		}

		a.scheduleRetry()
	}

	return result
}

// handleFSMEvent applies one event to the FSM in the current turn and, once
// the transition has committed, dispatches its outbox. A failed event leaves
// the state unchanged and dispatches nothing.
func (a *RescanActor) handleFSMEvent(actorCtx context.Context,
	event RescanEvent) fn.Result[RescanActorResp] {

	outbox, err := a.machine.ProcessEvent(actorCtx, event)
	if err != nil {
		return fn.Err[RescanActorResp](err)
	}

	a.dispatchOutbox(outbox)

	return fn.Ok[RescanActorResp](&rescanActorAck{})
}

// scheduleRetry sends a continuation after retryInterval, unless the
// actor stops first.
func (a *RescanActor) scheduleRetry() {
	a.asyncWG.Add(1)
	go func() {
		defer a.asyncWG.Done()

		timer := time.NewTimer(retryInterval)
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
func (a *RescanActor) handleStop(
	actorCtx context.Context) fn.Result[RescanActorResp] {

	return a.handleFSMEvent(actorCtx, StopEvent{})
}

// handleSubscriptionFailed ends the rescan when its block subscription could
// not be created or closed underneath it. Both happen only once the
// ChainService has stopped, so the rescan reports the error once and moves
// to Terminal, as the legacy rescan loop does, rather than retrying.
func (a *RescanActor) handleSubscriptionFailed(actorCtx context.Context,
	msg *subscriptionFailedMsg) fn.Result[RescanActorResp] {

	if a.ctx.Err() != nil || a.machine.CurrentState().IsTerminal() {
		return fn.Ok[RescanActorResp](&rescanActorAck{})
	}

	log.Errorf("RescanActor: %v, stopping", msg.Err)
	if a.cfg.OnError != nil {
		a.cfg.OnError(msg.Err)
	}
	a.cancelBridge()

	return a.handleFSMEvent(actorCtx, StopEvent{})
}

// dispatchOutbox delivers the outbox of a committed transition: wallet
// callbacks in emission order, and at most one continuation. Delivery stops
// once Stop has cancelled the actor.
func (a *RescanActor) dispatchOutbox(outbox []OutboxEvent) {
	callbacks := a.cfg.Callbacks
	for _, event := range outbox {
		select {
		case <-a.ctx.Done():
			return
		default:
		}

		switch e := event.(type) {
		case FilteredBlockOutbox:
			if callbacks.OnFilteredBlockConnected != nil {
				callbacks.OnFilteredBlockConnected(
					e.Height, e.Header, e.RelevantTxs,
				)
			}

			if callbacks.OnTxConfirmed != nil {
				for _, tx := range e.RelevantTxs {
					callbacks.OnTxConfirmed(*tx.Hash())
				}
			}

		case BlockConnectedOutbox:
			if callbacks.OnBlockConnected != nil {
				callbacks.OnBlockConnected(
					e.Height, e.Header,
					time.Unix(e.Timestamp, 0),
				)
			}

		case BlockDisconnectedOutbox:
			if callbacks.OnFilteredBlockDisconnected != nil {
				callbacks.OnFilteredBlockDisconnected(
					e.Height, e.Header,
				)
			}
			if callbacks.OnBlockDisconnected != nil {
				callbacks.OnBlockDisconnected(
					e.Height, e.Header,
				)
			}

		case RescanProgressOutbox:
			if callbacks.OnRescanProgress != nil {
				callbacks.OnRescanProgress(e.Height)
			}

		case RescanFinishedOutbox:
			if callbacks.OnRescanFinished != nil {
				callbacks.OnRescanFinished(e.Height)
			}

		case ContinueOutbox:
			a.enqueueContinuation()
		}
	}
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
