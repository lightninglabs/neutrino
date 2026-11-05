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

// driverResult collects the effects of one event, in order. The state hands
// them to the machine as its outbox.
type driverResult struct {
	// Effects are the side effects to perform, in order.
	Effects []DriverEffect
}

// Driver is the header-sync decision maker. It owns the request and timer
// bookkeeping, the staged payloads, the peer cooldowns and the range ID
// allocation, and it drives the controller, the manager and the peers. It is
// a protofsm state machine with a single state, so it speaks the same event
// and outbox vocabulary as the manager and the peers: ProcessEvent runs one
// event to completion and returns the effects for the caller to execute.
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
	// the state holds no data of its own and reads and writes the
	// bookkeeping above through the driver.
	machine *protofsm.StateMachine[DriverEvent, DriverEffect, *Driver]

	// view is the executor's state for the event being processed. It
	// lives in the environment rather than in each event because it
	// describes the world an event runs in, not the event: the executor
	// sets it once per ProcessEvent call, and the follow-up events of a
	// Then effect come back through ProcessEvent with a fresh one. An
	// event that carried its own view could be replayed with a stale one.
	view View

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

// driverState is the driver's one state. The driver tracks no phases of its
// own: the sync's phases live in the manager's FSM and in the peers' machines,
// and the driver decides each event from the bookkeeping it owns. So a single
// state holds the dispatch, and the machine gives the driver the same event
// and outbox contract as the rest of the package.
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
// produced as the machine's outbox. The view is the executor's state at the
// time of the call, set by the driver's ProcessEvent.
func (s driverState) ProcessEvent(ctx context.Context, event DriverEvent,
	d *Driver) (*protofsm.StateTransition[
	DriverEvent, DriverEffect, *Driver], error) {

	var (
		result driverResult
		err    error
		view   = d.view
	)
	switch event := event.(type) {
	case PeerStateChanged:
		d.handlePeerStateChanged(ctx, event, &result)

	case PeerConnected:
		err = d.handlePeerConnected(ctx, event, &result)

	case PeerDisconnected:
		err = d.handlePeerDisconnected(ctx, event)

	case PeerRTTUpdated:
		err = d.handlePeerRTTUpdated(ctx, event)

	case CooldownWakeDue:
		d.handleCooldownWakeDue(&result)

	case RecoverPeers:
		d.handleRecoverPeers(ctx, event, &result)

	case PeerDone:
		d.handlePeerDone(ctx, event, &result)

	case BeginSync:
		d.handleBeginSync(ctx, event, view, &result)

	case syncDiscovered:
		d.handleSyncDiscovered(event, &result)

	case syncDispatched:
		d.handleSyncDispatched(event, &result)

	case Continue:
		d.resumeWork(ctx, view, true, &result)

	case ResumeWork:
		d.resumeWork(ctx, view, false, &result)

	case continueDiscover:
		d.handleContinueDiscover(ctx, event, view, &result)

	case continueDiscovered:
		d.handleContinueDiscovered(event, &result)

	case CooldownElapsed:
		d.handleCooldownElapsed(event, &result)

	case PlanRanges:
		_, err = d.planRanges(ctx, event.BestHeight)

	case DispatchWork:
		d.handleDispatchWork(ctx, view, &result)

	case DispatchPeer:
		d.handleDispatchPeer(ctx, event, &result)

	case RequestSent:
		d.handleRequestSent(event, &result)

	case RequestFailed:
		d.handleRequestFailed(ctx, event, &result)

	case TimerArmed:
		d.handleTimerArmed(event)

	case RequestTimedOut:
		d.handleRequestTimedOut(ctx, event, view, &result)

	case anchorTimedOut:
		d.handleAnchorTimedOut(event, &result)

	case HeadersReceived:
		d.handleHeadersReceived(ctx, event, view, &result)

	case anchorPipelined:
		d.handleAnchorPipelined(ctx, event, view, &result)

	case anchorPlanned:
		d.handleAnchorPlanned(event, view, &result)

	case anchorWaiting:
		d.handleAnchorWaiting(event, &result)

	case anchorReplanned:
		d.handleAnchorReplanned(event, &result)

	case CommitReady:
		_, err = d.commitReadyRanges(ctx, view, &result)

	case RetryCommit:
		d.handleRetryCommit(ctx, view, &result)

	case RangesCommitted:
		d.handleRangesCommitted(ctx, event, &result)

	case ExtensionCommitted:
		d.handleExtensionCommitted(ctx, event, &result)

	case ReorgCommitted:
		d.handleReorgCommitted(ctx, event, &result)

	case CheckpointRolledBack:
		d.handleCheckpointRolledBack(ctx, event, &result)

	case SnapshotAdopted:
		d.handleSnapshotAdopted(ctx, event, &result)

	case PeerGone:
		d.handlePeerGone(ctx, event, view, &result)

	case SerialTimedOut:
		d.handleSerialTimedOut(ctx, event, view, &result)

	case TipHeaders:
		d.handleTipHeaders(event, &result)

	case FollowTip:
		d.handleFollowTip(event)
	}

	d.err = err

	return &protofsm.StateTransition[DriverEvent, DriverEffect, *Driver]{
		NextState: s,
		NewEvents: fn.Some(protofsm.EmittedEvent[
			DriverEvent, DriverEffect,
		]{
			Outbox: result.Effects,
		}),
	}, nil
}
