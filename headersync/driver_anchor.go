package headersync

import (
	"context"

	"github.com/btcsuite/btcd/wire/v2"
)

// AnchorReplyOutcome is the result of an AnchorReply event.
type AnchorReplyOutcome struct {
	// Matched is set when the reply finished an anchor request. Finished
	// then holds how the controller handled it, and the caller carries on
	// from there.
	Matched  bool
	Finished AnchorHeadersResult

	// Consumed tells an unmatched reply apart. It is set when the driver
	// took the reply anyway, for example because the controller failed, or
	// because it is a reply that the request it might answer is still
	// waiting for. Otherwise the reply is not for anchor discovery.
	Consumed bool
}

// AnchorReply delivers a headers message to the driver to see whether it
// answers an anchor discovery request. The controller records the anchor the
// reply names, and stages the range it discovered if it is the first to reach
// that boundary.
type AnchorReply struct {
	PeerID  string
	Headers []*wire.BlockHeader
}

func (AnchorReply) driverEvent() {}

// StageRange asks the driver to keep the headers of a completed range until
// the range can be committed in order.
type StageRange struct {
	RangeID RangeID
	PeerID  string
	Headers []*wire.BlockHeader
}

func (StageRange) driverEvent() {}

// RetireStaged asks the driver to drop the headers of committed ranges.
type RetireStaged struct {
	RangeIDs []RangeID
}

func (RetireStaged) driverEvent() {}

// PruneAnchors asks the driver to drop in-flight anchor discovery requests
// whose stop height is already committed. Hedged discovery can leave
// duplicate requests for an old span after the first peer commits it; those
// completions should not keep the scheduler waiting on obsolete work.
type PruneAnchors struct {
	AtOrBefore uint32
}

func (PruneAnchors) driverEvent() {}

// handleAnchorReply finishes the anchor request a headers message answers.
func (d *Driver) handleAnchorReply(ctx context.Context, event AnchorReply,
	result *DriverResult) {

	peerID := event.PeerID
	finished, ok, err := d.runtime.Controller().FinishAnchorHeaders(
		ctx, peerID, event.Headers, DefaultMaxRangeHeaders,
	)

	// Check the error before the match flag, so an actor failure is never
	// mistaken for an unmatched reply. The request is already finished, so
	// keep sync moving: otherwise nothing is in flight and nothing ever
	// schedules the next request.
	if err != nil {
		log.Warnf("headersync unable to add discovered anchor "+
			"peer=%s "+
			"height=%d hash=%s: %v", peerID,
			finished.Response.Height, finished.Response.Hash, err)

		// The ask may have staged a discovered range without its
		// headers. The commit retry requeues such a range.
		result.Effects = append(
			result.Effects, ScheduleCommitRetry{}, ContinueSync{},
		)
		result.Anchor = AnchorReplyOutcome{Consumed: true}

		return
	}
	if !ok {
		// Keep unmatched replies out of the compatibility path while
		// discovery waits for a different locator response.
		result.Anchor = AnchorReplyOutcome{
			Consumed: d.session.AnchorActive(peerID),
		}

		return
	}

	result.Anchor = AnchorReplyOutcome{
		Matched:  true,
		Consumed: true,
		Finished: finished,
	}
}

// handleStageRange stages the headers of a range.
func (d *Driver) handleStageRange(event StageRange) {
	d.session.StageRange(event.RangeID, event.Headers, event.PeerID)
}

// handleRetireStaged drops committed staging.
func (d *Driver) handleRetireStaged(event RetireStaged) {
	for _, rangeID := range event.RangeIDs {
		d.session.DeleteStagedRange(rangeID)
	}
}

// handlePruneAnchors drops obsolete anchor requests.
func (d *Driver) handlePruneAnchors(event PruneAnchors, result *DriverResult) {
	result.Pruned = d.session.PruneAnchorsAtOrBefore(event.AtOrBefore)
}

// StagedRange returns the staged headers of a range. The commit path reads
// them to build its write. This read goes away when the commit moves behind
// effects.
func (d *Driver) StagedRange(rangeID RangeID) (StagedRange, bool) {
	return d.session.StagedRange(rangeID)
}
