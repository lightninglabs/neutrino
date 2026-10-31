package headersync

import (
	"context"
	"time"
)

// RequestKind identifies the kind of getheaders request the driver tracks.
type RequestKind uint8

const (
	// RequestRange is a getheaders request for a leased header range.
	RequestRange RequestKind = iota

	// RequestAnchor is a getheaders request for anchor discovery.
	RequestAnchor
)

// PlanRanges asks the driver to plan the ranges between the durable tip and
// the next boundary. Checkpoint is the next checkpoint the chain must reach,
// or nil once the chain is past its last checkpoint and ranges are planned
// against the best peer height alone.
type PlanRanges struct {
	Tip        ChainPoint
	Checkpoint *ChainPoint
	BestHeight uint32
}

func (PlanRanges) driverEvent() {}

// AssignRanges asks the driver to hand queued ranges to peers. PeerCount is
// the number of candidate peers.
type AssignRanges struct {
	PeerCount int
}

func (AssignRanges) driverEvent() {}

// DispatchPeer asks the driver to start the next range for one peer, stealing
// one from a blocked or slow peer if its own queue is empty. The driver emits
// a SendGetHeaders effect when the peer got a range.
type DispatchPeer struct {
	PeerID string
}

func (DispatchPeer) driverEvent() {}

// RequestSent reports that a SendGetHeaders effect was written to the peer.
// The driver starts tracking the request, and asks for its timer.
type RequestSent struct {
	Request SendGetHeaders
}

func (RequestSent) driverEvent() {}

// RequestFailed reports that a SendGetHeaders effect could not be written to
// the peer. The driver blocks the peer, and tracks nothing.
type RequestFailed struct {
	Request SendGetHeaders
	Err     error
}

func (RequestFailed) driverEvent() {}

// TimerArmed reports the timer the executor armed for an ArmRequestTimer
// effect. The driver hands it to the request, which owns it from then on.
type TimerArmed struct {
	PeerID    string
	Kind      RequestKind
	RequestID uint64
	Timer     *time.Timer
}

func (TimerArmed) driverEvent() {}

// SendGetHeaders asks the executor to write a getheaders message to a peer,
// with a locator of just StartHash and the given stop hash. The executor
// answers with RequestSent or RequestFailed.
type SendGetHeaders struct {
	PeerID string
	Kind   RequestKind

	// Range is set for a RequestRange.
	Range RangeAssignment

	// StartHeight, StartHash, StopHeight and StopHash describe the span.
	// For a RequestRange they repeat the assignment.
	StartHeight uint32
	StartHash   Hash
	StopHeight  uint32
	StopHash    Hash
}

func (SendGetHeaders) driverEffect() {}

// ArmRequestTimer asks the executor to arm the deadline of a tracked request.
// The executor posts a RequestTimedOut event when it fires, and answers with
// TimerArmed.
type ArmRequestTimer struct {
	PeerID      string
	Kind        RequestKind
	RequestID   uint64
	StartHeight uint32
	RangeID     RangeID
	LeaseEpoch  uint64
}

func (ArmRequestTimer) driverEffect() {}

// handlePlanRanges plans ranges against the next checkpoint, or against the
// best peer height when there is none.
func (d *Driver) handlePlanRanges(ctx context.Context, event PlanRanges,
	result *DriverResult) error {

	var (
		ranges     []HeaderRange
		span       uint32
		maxHeaders uint32
		wait       bool
	)
	controller := d.runtime.Controller()
	height := event.Tip.Height

	if event.Checkpoint == nil {
		if height >= event.BestHeight {
			return nil
		}

		plan, err := controller.PlanFrontierRanges(
			ctx, event.Tip, event.BestHeight,
		)
		if err != nil {
			return err
		}
		ranges = plan.Ranges
	} else {
		stopHeight := event.Checkpoint.Height
		if height >= stopHeight || event.BestHeight < stopHeight {
			return nil
		}

		plan, err := controller.PlanCheckpointRanges(
			ctx, event.Tip, *event.Checkpoint, event.BestHeight,
		)
		if err != nil {
			return err
		}

		ranges = plan.Ranges
		span = plan.Span
		maxHeaders = plan.MaxRangeHeaders
		wait = plan.WaitingForAnchors
	}

	if len(ranges) == 0 {
		if wait {
			log.Debugf(
				"headersync waiting for confirmed anchors "+
					"height=%d checkpoint=%d span=%d "+
					"max_headers=%d",
				height,
				event.Checkpoint.Height,
				span,
				maxHeaders,
			)
		}
		return nil
	}

	for _, rng := range ranges {
		log.Infof(
			"headersync range_planned range_id=%d "+
				"start_height=%d "+
				"stop_height=%d stop_hash=%s",
			rng.ID,
			rng.StartHeight,
			rng.StopHeight,
			rng.StopHash,
		)
	}
	result.Planned = len(ranges)

	return nil
}

// handleAssignRanges hands queued ranges to the peer actors.
func (d *Driver) handleAssignRanges(ctx context.Context,
	event AssignRanges) {

	assignments, err := d.runtime.Controller().AssignQueuedRanges(
		ctx, event.PeerCount,
	)
	if err != nil {
		log.Warnf("headersync range assignment failed: %v", err)
	}
	for _, assignment := range assignments {
		log.Debugf("headersync range_assigned peer=%s range_id=%d "+
			"epoch=%d start_height=%d stop_height=%d",
			assignment.PeerID, assignment.RangeID,
			assignment.LeaseEpoch, assignment.StartHeight,
			assignment.StopHeight)
	}
}

// handleDispatchPeer starts the next range for one peer.
func (d *Driver) handleDispatchPeer(ctx context.Context, event DispatchPeer,
	result *DriverResult) {

	peerID := event.PeerID
	if d.session.RangeActive(peerID) {
		return
	}
	if d.session.AnchorActive(peerID) {
		return
	}

	peerActor, ok := d.runtime.Peer(peerID)
	if !ok {
		return
	}

	assignment, ok, err := d.requestPeerRange(
		ctx, peerID, peerActor, result,
	)
	if err != nil {
		log.Warnf(
			"headersync peer %s failed to request range: "+
				"%v",
			peerID,
			err,
		)
		return
	}
	if !ok {
		steal, stealOK, err := peerActor.RequestStolenRange(ctx)
		if err != nil {
			log.Warnf(
				"headersync peer %s failed to steal "+
					"range: %v",
				peerID,
				err,
			)
		} else if stealOK {
			log.Infof("headersync range_stolen donor=%s "+
				"thief=%s "+
				"range_id=%d old_epoch=%d "+
				"new_epoch=%d "+
				"reason=%s", steal.DonorID, peerID,
				steal.RangeID, steal.OldLeaseEpoch,
				steal.LeaseEpoch, steal.Reason)
			d.markRangeStolen(ctx, steal)

			assignment, ok, err = d.requestPeerRange(
				ctx, peerID, peerActor, result,
			)
			if err != nil {
				log.Warnf("headersync peer %s failed "+
					"to start "+
					"stolen range: %v", peerID, err)
				return
			}
		}
	}
	if !ok {
		return
	}

	result.Effects = append(result.Effects, SendGetHeaders{
		PeerID:      peerID,
		Kind:        RequestRange,
		Range:       assignment,
		StartHeight: assignment.StartHeight,
		StartHash:   assignment.StartHash,
		StopHeight:  assignment.StopHeight,
		StopHash:    assignment.StopHash,
	})
}

// requestPeerRange asks a peer actor to start its next range. The ask may
// have started a range in the actor even though it reported an error, which
// leaves an active lease with no request behind it. On an error the peer is
// blocked, so the steal of blocked owners or the release on cooldown recovery
// takes the range back.
func (d *Driver) requestPeerRange(ctx context.Context, peerID string,
	peerActor *PeerActor, result *DriverResult) (RangeAssignment, bool,
	error) {

	var (
		assignment RangeAssignment
		ok         bool
	)
	err := d.ask(DriverAskRequestRange, func() error {
		var err error
		assignment, ok, err = peerActor.RequestRange(ctx)
		return err
	})
	if err != nil {
		d.setPeerState(ctx, peerID, PeerBlocked, result)
	}

	return assignment, ok, err
}

// markRangeStolen tells the donor of a stolen range that it lost the lease.
func (d *Driver) markRangeStolen(ctx context.Context, steal StealResult) {
	switch steal.Reason {
	case StealReasonActiveBlocked, StealReasonActiveQuarantined:

	default:
		return
	}

	donorActor, ok := d.runtime.Peer(steal.DonorID)
	if !ok {
		return
	}

	err := donorActor.Send(ctx, PeerRangeStolenEvent{
		RangeID:       steal.RangeID,
		OldLeaseEpoch: steal.OldLeaseEpoch,
	})
	if err != nil {
		log.Warnf("headersync failed to notify donor=%s of stolen "+
			"range_id=%d old_epoch=%d: %v", steal.DonorID,
			steal.RangeID, steal.OldLeaseEpoch, err)
	}
}

// handleRequestSent starts tracking a request that went out, and asks for its
// deadline.
func (d *Driver) handleRequestSent(event RequestSent, result *DriverResult) {
	request := event.Request
	d.requestID++
	requestID := d.requestID

	arm := ArmRequestTimer{
		PeerID:      request.PeerID,
		Kind:        request.Kind,
		RequestID:   requestID,
		StartHeight: request.StartHeight,
	}

	switch request.Kind {
	case RequestRange:
		assignment := request.Range
		arm.RangeID = assignment.RangeID
		arm.LeaseEpoch = assignment.LeaseEpoch

		d.session.TrackRange(request.PeerID, RangeRequest{
			Assignment: assignment,
			RequestID:  requestID,
		})
		log.Infof("headersync range_started peer=%s range_id=%d "+
			"epoch=%d start_height=%d stop_height=%d stop_hash=%s",
			request.PeerID, assignment.RangeID,
			assignment.LeaseEpoch, assignment.StartHeight,
			assignment.StopHeight, assignment.StopHash)

	case RequestAnchor:
		d.session.TrackAnchor(request.PeerID, AnchorRequest{
			StartHeight: request.StartHeight,
			StartHash:   request.StartHash,
			StopHeight:  request.StopHeight,
			StopHash:    request.StopHash,
			RequestID:   requestID,
		})
		log.Debugf("headersync anchor_request peer=%s "+
			"start_height=%d stop_height=%d stop_hash=%s",
			request.PeerID, request.StartHeight, request.StopHeight,
			request.StopHash)
	}

	result.Effects = append(result.Effects, arm)
}

// handleRequestFailed blocks a peer that a request could not be written to.
func (d *Driver) handleRequestFailed(ctx context.Context,
	event RequestFailed, result *DriverResult) {

	request := event.Request
	switch request.Kind {
	case RequestRange:
		log.Warnf("headersync failed to request range_id=%d "+
			"from peer=%s: %v", request.Range.RangeID,
			request.PeerID, event.Err)

	case RequestAnchor:
		log.Warnf("headersync unable to request anchor "+
			"discovery "+
			"from peer=%s: %v", request.PeerID, event.Err)
	}

	d.setPeerState(ctx, request.PeerID, PeerBlocked, result)
}

// handleTimerArmed hands the deadline timer to the request it belongs to. A
// request that was replaced or finished in the meantime no longer wants it.
func (d *Driver) handleTimerArmed(event TimerArmed) {
	var attached bool
	switch event.Kind {
	case RequestRange:
		attached = d.session.AttachRangeTimer(
			event.PeerID, event.RequestID, event.Timer,
		)

	case RequestAnchor:
		attached = d.session.AttachAnchorTimer(
			event.PeerID, event.RequestID, event.Timer,
		)
	}
	if !attached {
		stopTimer(event.Timer)
	}
}
