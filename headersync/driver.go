package headersync

import (
	"context"
	"fmt"
	"time"

	"github.com/lightninglabs/neutrino/protofsm"
	"github.com/lightningnetwork/lnd/fn/v2"
)

// DefaultPeerCooldown is the time a blocked or quarantined header peer is
// held out of assignment before the driver recovers it.
const DefaultPeerCooldown = 2 * time.Minute

// DriverEvent is an input to the driver: a peer reply, a timeout, a chain
// store result, or a step the driver asked to take once the effects before it
// ran. The view that comes with each event carries what the driver needs to
// know about the world that it does not own, and the driver never reads it
// from anywhere else.
type DriverEvent interface {
	driverEvent()
}

// DriverEffect is a side effect the driver asks its executor to perform. The
// driver itself sends no message, arms no timer and punishes no peer. The
// executor reports the outcome of an effect that can fail back as an event.
//
// The effects of one event are the outbox of the driver's state machine. The
// executor runs them in order, after ProcessEvent returns, and a nested
// ProcessEvent call sees the state those effects produced. An effect that
// re-enters the driver, such as Then or StartSync, must therefore be the last
// effect of its batch. Every emitter follows this.
type DriverEffect interface {
	driverEffect()
}

// Activity is a point-in-time view of the in-flight requests the driver
// tracks. It is the read side of the driver: callers use it to decide whether
// any work is outstanding.
type Activity struct {
	// Ranges is the number of in-flight range requests.
	Ranges int

	// Anchors is the number of in-flight anchor discovery requests.
	Anchors int
}

// HasWork reports whether any request is in flight.
func (a Activity) HasWork() bool {
	return a.Ranges > 0 || a.Anchors > 0
}

// Driver is the header-sync decision maker. It owns the request and timer
// bookkeeping, the staged payloads, the peer cooldowns and the range ID
// allocation, and it drives the controller, the manager and the peers. It is
// a protofsm state machine with two states, idle and commit pending, so it
// speaks the same event and outbox vocabulary as the manager and the peers:
// ProcessEvent runs one event to completion and returns the effects for the
// caller to execute. While a chain store mutation is in flight the driver is
// in the commit pending state and defers the events that come from outside.
//
// A driver has no goroutine of its own: the caller must serialize every call
// to ProcessEvent. Today that caller is the block manager's block handler.
// An event handler must never call ProcessEvent itself, since the machine's
// lock isn't reentrant and the call would deadlock. A handler that needs a
// follow-up emits a Then effect instead.
type Driver struct {
	cfg     DriverConfig
	runtime *Runtime
	session *Session

	// tip is the header validation cache and checkpoint cursor. Only the
	// driver touches it, which keeps it on the same serialization as the
	// request bookkeeping. Readers go through Tip, NextCheckpoint and
	// FindPreviousCheckpoint, and writers through events.
	tip *TipState

	// requestID numbers the range and anchor requests the driver tracks.
	requestID uint64

	// machine runs the driver's events. The driver is its environment:
	// the states hold no bookkeeping of their own, only the events the
	// commit pending state deferred, and read and write the bookkeeping
	// above through the driver.
	machine *protofsm.StateMachine[DriverEvent, DriverEffect, *Driver]

	// view is the executor's state for the event being processed. It
	// lives in the environment rather than in each event because it
	// describes the world an event runs in, not the event: the executor
	// sets it once per ProcessEvent call, and the follow-up events of a
	// Then effect come back through ProcessEvent with a fresh one. An
	// event that carried its own view could be replayed with a stale one.
	view View

	// commitPending is whether a chain store mutation is in flight for the
	// event being processed. The state sets it for the handlers, which
	// plan nothing that would build on the stale tip of an unsettled
	// mutation. Like the view, it describes the world the event runs in.
	commitPending bool

	// err is the error of the event being processed, recorded by the
	// state for ProcessEvent to return next to the effects. It does not
	// fail the transition: the driver has already dealt with the failure,
	// by logging it or by scheduling a retry, and the effects it emitted
	// on the way must still run, which the machine would drop with a
	// failed transition.
	err error
}

// newDriver creates a driver over the runtime's session and validation cache.
func newDriver(runtime *Runtime, session *Session, tip *TipState,
	cfg DriverConfig) *Driver {

	driver := &Driver{
		cfg:     cfg,
		runtime: runtime,
		session: session,
		tip:     tip,
	}

	machine, err := protofsm.NewStateMachine[
		DriverEvent, DriverEffect,
	](protofsm.StateMachineCfg[DriverEvent, DriverEffect, *Driver]{
		InitialState: driverState{},
		Env:          driver,
	})
	if err != nil {
		// The machine only refuses a nil initial state.
		panic(fmt.Sprintf("unable to create headersync driver FSM: %v",
			err))
	}
	driver.machine = machine

	// The machine has no lifecycle beyond the driver's: it starts with it
	// and is never stopped, since the runtime's stop reaches the driver as
	// ErrActorStopped from the manager, and the machine runs on its
	// caller's goroutine with nothing of its own to wind down.
	machine.Start(context.Background())

	return driver
}

// Activity returns the driver's in-flight request counts.
func (d *Driver) Activity() Activity {
	return Activity{
		Ranges:  d.session.ActiveRangeCount(),
		Anchors: d.session.ActiveAnchorCount(),
	}
}

// ProcessEvent applies one event with the executor's view and returns the
// effects it produced, in order. A non-nil error means the event could not be
// handled; the effects returned with it still stand. Failures of individual
// manager calls inside an event are handled by the driver and logged, as they
// were by the block manager before the driver took over.
func (d *Driver) ProcessEvent(ctx context.Context, event DriverEvent,
	view View) ([]DriverEffect, error) {

	d.view = view

	effects, err := d.machine.ProcessEvent(ctx, event)
	if err != nil {
		return nil, err
	}

	return effects, d.err
}

// driverState is the driver's idle state: no chain store mutation is in
// flight. The driver tracks no sync phases of its own: those live in the
// manager's FSM and in the peers' machines, and the driver decides each event
// from the bookkeeping it owns. The one thing it does track is whether a
// mutation is in flight, which is the difference between its two states.
type driverState struct{}

// String returns the state's name.
func (driverState) String() string {
	return "headersync-driver"
}

// IsTerminal reports that the driver never finishes.
func (driverState) IsTerminal() bool {
	return false
}

// ProcessEvent applies one event to the driver and emits the effects it
// produced as the machine's outbox. An effect that submits a chain store
// mutation moves the driver to the commit pending state.
func (s driverState) ProcessEvent(ctx context.Context, event DriverEvent,
	d *Driver) (*protofsm.StateTransition[
	DriverEvent, DriverEffect, *Driver], error) {

	effects, err := d.dispatch(ctx, event, false)
	d.err = err

	var next driverStates = s
	if submitsCommit(effects) {
		next = commitPendingState{}
	}

	return transition(next, effects), nil
}

// commitPendingState is the driver's state while a chain store mutation is in
// flight. The mutation owns the durable tip, so the driver plans nothing that
// would build on a stale one, and the executor must not read the stores it
// holds. The driver still handles what the mutation's own follow-ups and the
// local bookkeeping need, such as the next step of an anchor span, but it
// defers the events that come from outside until the mutation lands: the
// messages from peers and the deadlines of requests. The deferred events
// replay in the order they arrived once the driver leaves the state. The queue
// is not bounded: what it holds is the replies to requests already in flight,
// and the messages that arrive while the store is down, which lasts as long as
// the mutation does.
type commitPendingState struct {
	// deferred are the events held back, oldest first.
	deferred []DriverEvent
}

// String returns the state's name.
func (commitPendingState) String() string {
	return "headersync-driver-commit-pending"
}

// IsTerminal reports that the driver never finishes.
func (commitPendingState) IsTerminal() bool {
	return false
}

// ProcessEvent defers the events that come from outside, and applies the rest.
// An event that reports how the mutation ended leaves the state, and the driver
// replays what it deferred as a follow-up of that event, so each replayed
// event sees the state the effects before it produced. If the event submitted
// the next mutation, the driver stays in the state and keeps what it deferred.
func (s commitPendingState) ProcessEvent(ctx context.Context,
	event DriverEvent, d *Driver) (*protofsm.StateTransition[
	DriverEvent, DriverEffect, *Driver], error) {

	if defersWhileCommitPending(event) {
		d.err = nil

		// The state it replaces is discarded, so it can give up its
		// slice.
		return transition(commitPendingState{
			deferred: append(s.deferred, event),
		}, nil), nil
	}

	// The mutation has ended by the time its outcome event arrives, so the
	// handlers decide it as an idle driver does.
	landed := endsCommit(event)
	effects, err := d.dispatch(ctx, event, !landed)
	d.err = err

	var next driverStates = s
	if landed {
		next = driverState{}
		if submitsCommit(effects) {
			next = commitPendingState{deferred: s.deferred}
		}
	}

	if _, idle := next.(driverState); idle && len(s.deferred) > 0 {
		effects = append(effects, Then{Events: s.deferred})
	}

	return transition(next, effects), nil
}

// driverStates is the set of the driver's states. Both are values, so a state
// holds only the events it deferred and the machine can swap one for the
// other.
type driverStates = protofsm.State[DriverEvent, DriverEffect, *Driver]

// transition returns the machine's transition to next with the effects as its
// outbox.
func transition(next driverStates,
	effects []DriverEffect) *protofsm.StateTransition[
	DriverEvent, DriverEffect, *Driver] {

	return &protofsm.StateTransition[DriverEvent, DriverEffect, *Driver]{
		NextState: next,
		NewEvents: fn.Some(protofsm.EmittedEvent[
			DriverEvent, DriverEffect,
		]{
			Outbox: effects,
		}),
	}
}

// submitsCommit reports whether the effects hand a mutation to the chain
// store, which the driver then waits on in the commit pending state.
func submitsCommit(effects []DriverEffect) bool {
	for _, effect := range effects {
		switch effect.(type) {
		case SubmitRangeCommit, CommitExtension, CommitReorg,
			CheckpointRollback:

			return true
		}
	}

	return false
}

// endsCommit reports whether the event says how the mutation in flight ended.
// The executor sends one of these for every mutation it was given, whether it
// landed, failed or never ran.
func endsCommit(event DriverEvent) bool {
	switch event.(type) {
	case RangesCommitted, ExtensionCommitted, ReorgCommitted,
		CheckpointRolledBack, SnapshotAdopted, CommitAbandoned:

		return true
	}

	return false
}

// defersWhileCommitPending reports whether the event comes from outside the
// driver and so waits for the mutation in flight: a message from a peer or the
// deadline of a request. Everything else is either the driver's own follow-up
// or bookkeeping that cannot wait, such as a peer's departure.
func defersWhileCommitPending(event DriverEvent) bool {
	_, ok := event.(Inbound)

	return ok
}

// dispatch applies one event to the driver's bookkeeping and returns the
// effects it produced. The pending flag tells the handlers whether a chain
// store mutation is in flight, as the state the event arrived in decides it.
func (d *Driver) dispatch(ctx context.Context, event DriverEvent,
	pending bool) ([]DriverEffect, error) {

	d.commitPending = pending

	var (
		effects []DriverEffect
		err     error
		view    = d.view
	)
	switch event := event.(type) {
	case PeerStateChanged:
		effects = d.handlePeerStateChanged(ctx, event)

	case PeerConnected:
		effects, err = d.handlePeerConnected(ctx, event)

	case PeerDisconnected:
		err = d.handlePeerDisconnected(ctx, event)

	case PeerRTTUpdated:
		err = d.handlePeerRTTUpdated(ctx, event)

	case CooldownWakeDue:
		effects = d.handleCooldownWakeDue()

	case RecoverPeers:
		effects = d.handleRecoverPeers(ctx, event)

	case PeerDone:
		effects, _ = d.handlePeerDone(ctx, event)

	case BeginSync:
		effects = d.handleBeginSync(ctx, event, view)

	case syncDiscovered:
		effects = d.handleSyncDiscovered(event)

	case syncDispatched:
		effects = d.handleSyncDispatched(event)

	case Continue:
		effects = d.resumeWork(ctx, view, true)

	case ResumeWork:
		effects = d.resumeWork(ctx, view, false)

	case continueDiscover:
		effects = d.handleContinueDiscover(ctx, event, view)

	case continueDiscovered:
		effects = d.handleContinueDiscovered(event)

	case CooldownElapsed:
		effects = d.handleCooldownElapsed(event)

	case PlanRanges:
		_, err = d.planRanges(ctx, event.BestHeight)

	case DispatchWork:
		effects = d.handleDispatchWork(ctx, view)

	case DispatchPeer:
		effects = d.handleDispatchPeer(ctx, event)

	case RequestSent:
		effects = d.handleRequestSent(event)

	case RequestFailed:
		effects = d.handleRequestFailed(ctx, event)

	case TimerArmed:
		d.handleTimerArmed(event)

	case RequestTimedOut:
		effects = d.handleRequestTimedOut(ctx, event, view)

	case anchorTimedOut:
		effects = d.handleAnchorTimedOut(event)

	case HeadersReceived:
		effects = d.handleHeadersReceived(ctx, event, view)

	case anchorPipelined:
		effects = d.handleAnchorPipelined(ctx, event, view)

	case anchorPlanned:
		effects = d.handleAnchorPlanned(event, view)

	case anchorWaiting:
		effects = d.handleAnchorWaiting(event)

	case anchorReplanned:
		effects = d.handleAnchorReplanned(event)

	case CommitReady:
		effects, _, err = d.commitReadyRanges(ctx)

	case RetryCommit:
		effects = d.handleRetryCommit(ctx, view)

	case RangesCommitted:
		effects = d.handleRangesCommitted(ctx, event)

	case ExtensionCommitted:
		effects = d.handleExtensionCommitted(ctx, event)

	case ReorgCommitted:
		effects = d.handleReorgCommitted(ctx, event)

	case CheckpointRolledBack:
		effects = d.handleCheckpointRolledBack(ctx, event)

	case SnapshotAdopted:
		effects = d.handleSnapshotAdopted(ctx, event)

	case PeerGone:
		effects = d.handlePeerGone(ctx, event)

	case SerialTimedOut:
		effects = d.handleSerialTimedOut(ctx, event)

	case TipHeaders:
		effects = d.handleTipHeaders(event)

	case FollowTip:
		d.handleFollowTip(event)

	case Inbound:
		effects = []DriverEffect{Deliver(event)}
	}

	return effects, err
}
