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
// the next boundary: the next checkpoint the chain must reach, or the best
// peer height once the chain is past its last checkpoint. Each range planned
// takes one range ID from the session. Production plans through the events
// that discover anchors; tests drive this directly.
type PlanRanges struct {
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

// planRangesFrom plans ranges from the tip against the checkpoint, or against
// the best peer height when there is none, and reports how many it planned.
func (d *Driver) planRangesFrom(ctx context.Context, tip ChainPoint,
	checkpoint *ChainPoint, bestHeight uint32) (int, error) {

	var (
		ranges     []HeaderRange
		span       uint32
		maxHeaders uint32
		wait       bool
	)
	controller := d.runtime.controller
	height := tip.Height

	if checkpoint == nil {
		if height >= bestHeight {
			return 0, nil
		}

		plan, err := controller.PlanFrontierRanges(
			ctx, tip, bestHeight,
		)
		if err != nil {
			return 0, err
		}
		ranges = plan.Ranges
	} else {
		stopHeight := checkpoint.Height
		if height >= stopHeight || bestHeight < stopHeight {
			return 0, nil
		}

		plan, err := controller.PlanCheckpointRanges(
			ctx, tip, *checkpoint, bestHeight,
		)
		if err != nil {
			return 0, err
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
				checkpoint.Height,
				span,
				maxHeaders,
			)
		}
		return 0, nil
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

	return len(ranges), nil
}

// handleAssignRanges hands queued ranges to the peers.
func (d *Driver) handleAssignRanges(ctx context.Context,
	event AssignRanges) {

	assignments, err := d.runtime.controller.AssignQueuedRanges(
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
func (d *Driver) handleDispatchPeer(ctx context.Context,
	event DispatchPeer) []DriverEffect {

	var effects []DriverEffect

	peerID := event.PeerID
	if d.session.RangeActive(peerID) {
		return effects
	}
	if d.session.AnchorActive(peerID) {
		return effects
	}

	peerActor, ok := d.runtime.Peer(peerID)
	if !ok {
		return effects
	}

	requestEffects, assignment, ok, err := d.requestPeerRange(
		ctx, peerID, peerActor,
	)
	effects = append(effects, requestEffects...)
	if err != nil {
		log.Warnf(
			"headersync peer %s failed to request range: "+
				"%v",
			peerID,
			err,
		)
		return effects
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

			var stealEffects []DriverEffect
			stealEffects, assignment, ok, err = d.requestPeerRange(
				ctx, peerID, peerActor,
			)
			effects = append(effects, stealEffects...)
			if err != nil {
				log.Warnf("headersync peer %s failed "+
					"to start "+
					"stolen range: %v", peerID, err)
				return effects
			}
		}
	}
	if !ok {
		return effects
	}

	effects = append(effects, SendGetHeaders{
		PeerID:      peerID,
		Kind:        RequestRange,
		Range:       assignment,
		StartHeight: assignment.StartHeight,
		StartHash:   assignment.StartHash,
		StopHeight:  assignment.StopHeight,
		StopHash:    assignment.StopHash,
	})

	return effects
}

// requestPeerRange asks a peer to start its next range. On an error, which
// means the manager is stopped or the event's deadline passed, the peer is
// blocked until its cooldown ends.
func (d *Driver) requestPeerRange(ctx context.Context, peerID string,
	peerActor *Peer) ([]DriverEffect, RangeAssignment, bool, error) {

	var effects []DriverEffect

	assignment, ok, err := peerActor.RequestRange(ctx)
	if err != nil {
		effects = append(effects, d.setPeerState(
			ctx, peerID, PeerBlocked,
		)...)
	}

	return effects, assignment, ok, err
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
func (d *Driver) handleRequestSent(event RequestSent) []DriverEffect {
	var effects []DriverEffect

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

	effects = append(effects, arm)

	return effects
}

// handleRequestFailed blocks a peer that a request could not be written to.
func (d *Driver) handleRequestFailed(ctx context.Context,
	event RequestFailed) []DriverEffect {

	var effects []DriverEffect

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

	effects = append(effects, d.setPeerState(
		ctx, request.PeerID, PeerBlocked,
	)...)

	return effects
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
