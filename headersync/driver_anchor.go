package headersync

// This file holds the follow-up to an anchor discovery reply: what the node
// discovers, plans and commits once a peer has named a boundary, and what it
// does when the reply is stale, rejected, or contradicts a recorded anchor.

import (
	"context"
	"errors"

	"github.com/btcsuite/btcd/wire/v2"
)

// DropCandidate asks the executor to drop a peer from the sync candidates
// without disconnecting it.
type DropCandidate struct {
	// PeerID is the address of the peer.
	PeerID string
}

func (DropCandidate) driverEffect() {}

// anchorPipelined follows the frontier discovery that an anchor reply
// overlapped with its commit, once its requests are written.
type anchorPipelined struct {
	PeerID   string
	Finished AnchorHeadersResult
	Round    DiscoveryOutcome
}

func (anchorPipelined) driverEvent() {}

// anchorPlanned follows the dispatch of the ranges an anchor reply planned.
type anchorPlanned struct {
	PeerID   string
	Finished AnchorHeadersResult
	Planned  int
}

func (anchorPlanned) driverEvent() {}

// anchorWaiting follows a discovery round that continues a span, once its
// requests are written.
type anchorWaiting struct {
	Round    DiscoveryOutcome
	Span     AnchorRequest
	Excluded string
}

func (anchorWaiting) driverEvent() {}

// anchorReplanned follows the discovery round that replaces a rejected anchor
// reply, once its requests are written.
type anchorReplanned struct {
	Round DiscoveryOutcome
}

func (anchorReplanned) driverEvent() {}

// handleAnchorHeaders finishes the anchor request a headers message answers
// and carries on from what the reply named. It reports whether the message
// was for anchor discovery.
func (d *Driver) handleAnchorHeaders(ctx context.Context, view View,
	peerID string, headers []*wire.BlockHeader, result *DriverResult) bool {

	finished, ok, err := d.runtime.controller.FinishAnchorHeaders(
		ctx, peerID, headers, DefaultMaxRangeHeaders,
	)

	// Check the error before the match flag, so a manager failure is never
	// mistaken for an unmatched reply. The request is already finished, so
	// keep sync moving: otherwise nothing is in flight and nothing ever
	// schedules the next request.
	if err != nil {
		log.Warnf("headersync unable to add discovered anchor "+
			"peer=%s "+
			"height=%d hash=%s: %v", peerID,
			finished.Response.Height, finished.Response.Hash, err)

		// The reply is consumed, so keep sync moving.
		result.Effects = append(result.Effects, ScheduleCommitRetry{})
		then(result, Continue{})

		return true
	}
	if !ok {
		// Keep unmatched replies out of the compatibility path while
		// discovery waits for a different locator response.
		return d.session.AnchorActive(peerID)
	}

	request := finished.Request
	if finished.Rejected {
		if errors.Is(finished.Err, ErrRangeStartMismatch) {
			log.Infof("headersync stale_anchor_mismatch peer=%s "+
				"start_height=%d stop_height=%d reason=%s",
				peerID, request.StartHeight, request.StopHeight,
				finished.Reason)

			d.waitForAnchor(
				view, request, request.StartHeight,
				request.StartHash, "", result,
			)

			return true
		}

		if finished.Conflict {
			d.conflictAnchor(
				ctx, view, peerID, request, finished.Anchor,
				result,
			)

			return true
		}

		d.rejectAnchor(
			ctx, view, peerID, request, finished.Reason, result,
		)

		return true
	}

	_, tipHeight, tipErr := d.tip.StoreTip()
	if tipErr != nil {
		log.Warnf("headersync unable to check tip for anchor "+
			"response "+
			"peer=%s start_height=%d stop_height=%d: %v", peerID,
			request.StartHeight, request.StopHeight, tipErr)
	} else if !finished.Staged && finished.Response.Height <= tipHeight {
		pruned := d.session.PruneAnchorsAtOrBefore(tipHeight)
		log.Infof("headersync stale_anchor_completion peer=%s "+
			"start_height=%d response_height=%d stop_height=%d "+
			"tip_height=%d pruned_anchors=%d", peerID,
			request.StartHeight, finished.Response.Height,
			request.StopHeight, tipHeight, pruned)

		if !d.session.HasActiveWork() {
			then(result, Continue{})
		}

		return true
	}

	log.Infof("headersync anchor_discovered peer=%s height=%d hash=%s "+
		"trusted=%v confirmations=%d confirmed=%v start_height=%d "+
		"stop_height=%d", peerID, finished.Response.Height,
		finished.Response.Hash, finished.Anchor.Trusted,
		finished.Anchor.Confirmations, finished.Confirmed,
		request.StartHeight, request.StopHeight)

	if finished.Staged {
		d.session.StageRange(finished.Range.ID, headers, peerID)
		log.Infof("headersync range_staged_from_anchor peer=%s "+
			"range_id=%d start_height=%d stop_height=%d "+
			"header_count=%d", peerID, finished.Range.ID,
			finished.Range.StartHeight, finished.Range.StopHeight,
			len(headers))

		submitted, err := d.commitReadyRanges(ctx, view, result)
		if err != nil {
			log.Warnf("headersync failed to commit anchor-backed "+
				"range_id=%d: %v", finished.Range.ID, err)
			if d.rejectInvalidCommit(ctx, view, err, result) {
				return true
			}
		}

		if view.CommitPending || submitted {
			return true
		}

		// Discover the span above the boundary while the span below
		// it commits, once the chain is past its checkpoints.
		if finished.Confirmed && d.tip.NextCheckpoint() == nil {
			round := d.pipelineFrontier(
				view, finished.Response, result,
			)
			then(result, anchorPipelined{
				PeerID:   peerID,
				Finished: finished,
				Round:    round,
			})

			return true
		}
	}

	d.planAfterAnchor(ctx, view, peerID, finished, result)

	return true
}

// handleAnchorPipelined notes the overlapped discovery, then plans the ranges
// the anchor made possible.
func (d *Driver) handleAnchorPipelined(ctx context.Context,
	event anchorPipelined, view View, result *DriverResult) {

	if d.discoveryActive(event.Round) {
		log.Debugf("headersync frontier_discovery_overlapped "+
			"range_id=%d stop_height=%d", event.Finished.Range.ID,
			event.Finished.Range.StopHeight)
	}

	d.planAfterAnchor(ctx, view, event.PeerID, event.Finished, result)
}

// planAfterAnchor plans the ranges a confirmed anchor made possible and
// dispatches them, or keeps discovering while the anchor waits for
// confirmation.
func (d *Driver) planAfterAnchor(ctx context.Context, view View,
	peerID string, finished AnchorHeadersResult, result *DriverResult) {

	request := finished.Request
	if !finished.Confirmed {
		d.waitForAnchor(
			view, request, request.StartHeight, request.StartHash,
			peerID, result,
		)

		return
	}

	planned, err := d.planRanges(ctx, request.StopHeight)
	if err != nil {
		log.Warnf("headersync unable to plan ranges after anchor "+
			"height=%d: %v", finished.Response.Height, err)
	} else if planned > 0 {
		then(result, DispatchWork{}, anchorPlanned{
			PeerID: peerID, Finished: finished, Planned: planned,
		})

		return
	}

	d.finishAnchorPlan(view, finished, planned, result)
}

// handleAnchorPlanned carries on once the planned ranges are dispatched.
func (d *Driver) handleAnchorPlanned(event anchorPlanned, view View,
	result *DriverResult) {

	d.finishAnchorPlan(view, event.Finished, event.Planned, result)
}

// finishAnchorPlan keeps discovering the rest of the span, or starts over
// when the anchor finished the span and nothing is in flight.
func (d *Driver) finishAnchorPlan(view View, finished AnchorHeadersResult,
	planned int, result *DriverResult) {

	request := finished.Request
	response := finished.Response
	if response.Height < request.StopHeight {
		d.waitForAnchor(
			view, request, response.Height, response.Hash, "",
			result,
		)

		return
	}

	if planned == 0 && !d.session.HasActiveWork() {
		d.restartSync(result)
	}
}

// waitForAnchor discovers the rest of the span from the start, leaving the
// excluded peer out of the round.
func (d *Driver) waitForAnchor(view View, request AnchorRequest,
	startHeight uint32, startHash Hash, excludePeerID string,
	result *DriverResult) {

	span := AnchorRequest{
		StartHeight: startHeight,
		StartHash:   startHash,
		StopHeight:  request.StopHeight,
		StopHash:    request.StopHash,
	}
	var excluded []string
	if excludePeerID != "" {
		excluded = []string{excludePeerID}
	}

	round := d.discoverFrom(span, view.Peers, excluded, result)
	then(result, anchorWaiting{
		Round: round, Span: span, Excluded: excludePeerID,
	})
}

// handleAnchorWaiting gives the sync peer up when the span has nothing in
// flight. The serial networks start over; the others wait for a peer that
// can answer.
func (d *Driver) handleAnchorWaiting(event anchorWaiting,
	result *DriverResult) {

	if d.discoveryActive(event.Round) {
		return
	}

	if d.session.HasActiveWork() {
		return
	}

	result.Effects = append(result.Effects, SetSyncPeer{})

	if d.cfg.SerialFallback {
		result.Effects = append(result.Effects, StartSync{})

		return
	}

	log.Debugf("headersync anchor_discovery_waiting start_height=%d "+
		"stop_height=%d excluded_peer=%s", event.Span.StartHeight,
		event.Span.StopHeight, event.Excluded)
}

// rejectAnchor punishes a peer whose discovery reply was malformed, and
// discovers the span again without it.
func (d *Driver) rejectAnchor(ctx context.Context, view View, peerID string,
	request AnchorRequest, reason string, result *DriverResult) {

	log.Warnf("headersync anchor_rejected peer=%s start_height=%d "+
		"stop_height=%d reason=%s", peerID, request.StartHeight,
		request.StopHeight, reason)

	d.quarantinePeer(ctx, peerID, result)
	result.Effects = append(result.Effects, DropCandidate{PeerID: peerID})

	d.replanAnchor(view, request, []string{peerID}, result)
}

// conflictAnchor handles a well formed discovery reply that names a different
// boundary than the anchor already recorded at its height. Nothing proves
// which reporter lied, and the earlier one may well be the liar, so neither
// is punished: the reporter is neither quarantined nor held back. The
// recorded anchor is rejected along with the unstaged work built on it, and
// the span is discovered again. Commit-time contextual validation of any
// staged payload finds the liar.
//
// A conflict against a trusted anchor, say a committed boundary after a short
// reorg in frontier mode, rejects nothing and so replans nothing. It is not
// punished either.
//
// A fast liar that always answers first can repeat this under a policy that
// needs two observations, since each round rejects the honest anchor again.
// The production policy needs one, so the first reply for a span is staged
// and committed whatever a later conflict does to the anchor, and the loop
// cannot stall it. We accept the loop for the stricter test-only policy.
func (d *Driver) conflictAnchor(ctx context.Context, view View,
	peerID string, request AnchorRequest, anchor Anchor,
	result *DriverResult) {

	log.Warnf("headersync anchor_conflict peer=%s start_height=%d "+
		"stop_height=%d anchor_height=%d recorded_hash=%s",
		peerID, request.StartHeight, request.StopHeight,
		anchor.Height, anchor.Hash)

	rejected := d.rejectUntrustedAnchor(
		ctx, "anchor_rejected_by_conflict", anchor.Height, anchor.Hash,
		d.cfg.RequestTimeout(),
	)
	if !rejected {
		if !d.session.HasActiveWork() {
			then(result, Continue{})
		}

		return
	}

	d.replanAnchor(view, request, nil, result)
}

// replanAnchor discovers the span of a rejected anchor reply again, leaving
// the excluded peers out of the new round, and falls back to the regular sync
// start when no other peer can answer.
func (d *Driver) replanAnchor(view View, request AnchorRequest,
	excluded []string, result *DriverResult) {

	round := d.discoverFrom(request, view.Peers, excluded, result)
	then(result, anchorReplanned{Round: round})
}

// handleAnchorReplanned starts over when no peer took the span.
func (d *Driver) handleAnchorReplanned(event anchorReplanned,
	result *DriverResult) {

	if d.discoveryActive(event.Round) {
		return
	}

	if d.session.ActiveAnchorCount() == 0 {
		result.Effects = append(result.Effects, SetSyncPeer{})
	}

	result.Effects = append(result.Effects, StartSync{})
}
