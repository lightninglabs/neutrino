package headersync

import (
	"context"
	"time"
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
// The executor runs the effects of one event in order, after Handle returns,
// and a nested Handle call sees the state those effects produced. An effect
// that re-enters the driver, such as Then or StartSync, must therefore be the
// last effect of its batch. Every emitter follows this.
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

	// PeerCreated is set by PeerConnected when the peer is new.
	PeerCreated bool

	// HadWork is set by PeerDone when the peer had a request in flight.
	HadWork bool

	// Planned is the number of ranges PlanRanges planned.
	Planned int

	// Handled is set by HeadersReceived when the driver took the message.
	// An empty message that answered no request is left to the executor,
	// which settles the serial request it may answer.
	Handled bool
}

// Driver is the header-sync decision maker. It owns the request and timer
// bookkeeping, the staged payloads, the peer cooldowns and the range ID
// allocation, and it drives the controller, the manager and the
// peers. Handle runs one event to completion and returns the effects for the
// caller to execute.
//
// A driver has no goroutine and no lock: the caller must serialize every call
// to Handle. Today that caller is the block manager's block handler.
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
}

// newDriver creates a driver over the runtime's session and validation cache.
func newDriver(runtime *Runtime, session *Session, tip *TipState,
	cfg DriverConfig) *Driver {

	return &Driver{
		cfg:     cfg,
		runtime: runtime,
		session: session,
		tip:     tip,
	}
}

// Activity returns the driver's in-flight request counts.
func (d *Driver) Activity() Activity {
	return Activity{
		Ranges:  d.session.ActiveRangeCount(),
		Anchors: d.session.ActiveAnchorCount(),
	}
}

// Handle applies one event and returns the effects it produced. The view is
// the executor's state at the time of the call. A non-nil error means the
// event could not be handled at all. Failures of individual manager calls
// inside an event are handled by the driver and logged, as they were by the
// block manager before the driver took over.
func (d *Driver) Handle(ctx context.Context, event DriverEvent,
	view View) (DriverResult, error) {

	var (
		result DriverResult
		err    error
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
		result.Planned, err = d.planRanges(ctx, event.BestHeight)

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

	return result, err
}
