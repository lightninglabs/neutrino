package headersync

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DefaultPeerCooldown is the time a blocked or quarantined header peer is
// held out of assignment before the driver recovers it.
const DefaultPeerCooldown = 2 * time.Minute

// DriverAskOp names a peer actor ask the driver makes on the caller's behalf.
// A test can fault the outcome of each one through the hook set with
// SetAskHook.
type DriverAskOp uint8

const (
	// DriverAskRequestRange is the ask that starts a peer's next range.
	DriverAskRequestRange DriverAskOp = iota

	// DriverAskCompleteRange is the range completion ask made when a
	// reply arrives.
	DriverAskCompleteRange
)

// ErrUnknownDriverEvent is returned by Handle for an event type it does not
// know.
var ErrUnknownDriverEvent = errors.New("unknown header sync driver event")

// DriverEvent is an input to the driver. Events carry everything the driver
// needs to know about the world that it does not own: peer snapshots, chain
// tips and the checkpoint cursor. The driver never reads them from anywhere
// else.
type DriverEvent interface {
	driverEvent()
}

// DriverEffect is a side effect the driver asks its executor to perform. The
// driver itself sends no message, arms no timer and punishes no peer. The
// executor reports the outcome of an effect that can fail back as an event.
//
// The executor runs the effects of one event in order, after Handle returns,
// and a nested Handle call sees the state those effects produced. An effect
// that re-enters dispatch, such as ContinueSync or CommitReadyRanges, must
// therefore be the last effect of its batch. Every emitter follows this.
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

// DriverResult is the outcome of handling one event.
type DriverResult struct {
	// Effects are the side effects to perform, in order.
	Effects []DriverEffect

	// HadWork is set by PeerDone when the peer had a request in flight.
	HadWork bool

	// Planned is the number of ranges PlanRanges planned.
	Planned int

	// Discovery is the outcome of the events that plan anchor discovery.
	Discovery DiscoveryOutcome

	// Timeout is the outcome of RequestTimedOut.
	Timeout TimeoutOutcome
}

// Driver is the header-sync decision maker. It owns the request and timer
// bookkeeping, the staged payloads, the peer cooldowns and the range ID
// allocation, and it drives the controller, the manager actor and the peer
// actors. Handle runs one event to completion and returns the effects for the
// caller to execute.
//
// A driver has no goroutine and no lock: the caller must serialize every call
// to Handle. Today that caller is the block manager's block handler.
type Driver struct {
	runtime *Runtime
	session *Session

	// askHook wraps the peer actor asks the driver makes. Production wires
	// a passthrough, and only tests inject faults that make an ask apply
	// and then report a deadline.
	askHook func(DriverAskOp, func() error) error

	// requestID numbers the range and anchor requests the driver tracks.
	requestID uint64
}

// newDriver creates a driver over the runtime's session.
func newDriver(runtime *Runtime, session *Session) *Driver {
	return &Driver{
		runtime: runtime,
		session: session,
	}
}

// SetAskHook sets the test-only seam that wraps the driver's peer actor asks.
// The hook must run fn exactly as the driver would, except for the fault it
// injects.
func (d *Driver) SetAskHook(hook func(DriverAskOp, func() error) error) {
	d.askHook = hook
}

// ask runs one peer actor ask through the test hook.
func (d *Driver) ask(op DriverAskOp, fn func() error) error {
	if d.askHook == nil {
		return fn()
	}

	return d.askHook(op, fn)
}

// Activity returns the driver's in-flight request counts.
func (d *Driver) Activity() Activity {
	return Activity{
		Ranges:  d.session.ActiveRangeCount(),
		Anchors: d.session.ActiveAnchorCount(),
	}
}

// Handle applies one event and returns the effects it produced. A non-nil
// error means the event could not be handled at all. Failures of individual
// actor asks inside an event are handled by the driver and logged, as they
// were by the block manager before the driver took over.
func (d *Driver) Handle(ctx context.Context,
	event DriverEvent) (DriverResult, error) {

	var (
		result DriverResult
		err    error
	)
	switch event := event.(type) {
	case PeerStateChanged:
		d.handlePeerStateChanged(ctx, event, &result)

	case PeerRegistered:
		d.handlePeerRegistered(ctx, event, &result)

	case CooldownWakeDue:
		d.handleCooldownWakeDue(&result)

	case RecoverPeers:
		d.handleRecoverPeers(ctx, event, &result)

	case PeerDone:
		d.handlePeerDone(ctx, event, &result)

	case PlanRanges:
		err = d.handlePlanRanges(ctx, event, &result)

	case AssignRanges:
		d.handleAssignRanges(ctx, event)

	case DispatchPeer:
		d.handleDispatchPeer(ctx, event, &result)

	case RequestSent:
		d.handleRequestSent(event, &result)

	case RequestFailed:
		d.handleRequestFailed(ctx, event, &result)

	case TimerArmed:
		d.handleTimerArmed(event)

	case DiscoverAnchors:
		d.handleDiscoverAnchors(ctx, event, &result)

	case DiscoverFrontier:
		d.handleDiscoverFrontier(ctx, event, &result)

	case PipelineFrontier:
		d.handlePipelineFrontier(event, &result)

	case DiscoverFrom:
		d.discoverFrom(event.Span, event.Peers, event.Excluded, &result)

	case RequestTimedOut:
		d.handleRequestTimedOut(ctx, event, &result)

	default:
		err = fmt.Errorf("%w: %T", ErrUnknownDriverEvent, event)
	}

	return result, err
}
