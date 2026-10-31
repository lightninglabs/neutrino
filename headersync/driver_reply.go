package headersync

import (
	"context"
	"time"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/banman"
)

// RangeReply delivers a headers message to the driver to see whether it
// answers a range request. The driver judges the reply against the range it
// was asked for, completes the range, and stages the headers of a valid one.
type RangeReply struct {
	PeerID string

	// PeerHeight is the height the peer advertises.
	PeerHeight int32

	Headers []*wire.BlockHeader

	// AnchorTimeout is how long the rejection of an anchor that the
	// reply refutes stays recorded.
	AnchorTimeout time.Duration
}

func (RangeReply) driverEvent() {}

// RejectUntrustedAnchor asks the driver to reject an untrusted anchor and the
// unstaged work built on it. Tag names the reason in the log.
type RejectUntrustedAnchor struct {
	Tag     string
	Height  uint32
	Hash    Hash
	Timeout time.Duration
}

func (RejectUntrustedAnchor) driverEvent() {}

// ScheduleCommitRetry asks the executor to keep one wakeup while staging
// waits for storage. A recovered store must not need another network
// response to drain staging.
type ScheduleCommitRetry struct{}

func (ScheduleCommitRetry) driverEffect() {}

// ContinueSync asks the executor to plan, dispatch and, if nothing is left to
// run, start syncing from the best peer again.
type ContinueSync struct{}

func (ContinueSync) driverEffect() {}

// CommitReadyRanges asks the executor to commit the staged ranges that are
// ready, and then to continue syncing, unless the commit failed on a range
// that was invalid and the executor already dealt with it.
type CommitReadyRanges struct{}

func (CommitReadyRanges) driverEffect() {}

// handleRangeReply judges a headers message against the range request of the
// peer, if it has one.
func (d *Driver) handleRangeReply(ctx context.Context, event RangeReply,
	result *DriverResult) {

	peerID := event.PeerID
	headers := event.Headers
	request, ok := d.session.FinishRangeHeaders(peerID, headers)
	if !ok {
		if d.session.RangeActive(peerID) {
			log.Debugf("headersync unmatched_range_reply peer=%s",
				peerID)
			result.Handled = true
			return
		}
		result.Handled = d.handleStaleRangeReply(peerID, result)
		return
	}
	result.Handled = true

	assignment := request.Assignment
	manager := d.runtime.Manager()
	rng, ok, err := manager.SnapshotRange(ctx, assignment.RangeID)
	if err != nil {
		log.Warnf("headersync unable to snapshot range_id=%d: %v",
			assignment.RangeID, err)
		return
	}
	if !ok {
		log.Warnf(
			"headersync received headers for unknown range_id=%d "+
				"peer=%s",
			assignment.RangeID,
			peerID,
		)
		return
	}

	// The lease may have ended while the reply was in flight, for example
	// when a serial batch committed the span. Judging the reply against a
	// range it no longer serves would punish an honest peer, so treat it
	// as stale.
	if rng.State != RangeActive ||
		rng.OwnerPeer != peerID ||
		rng.LeaseEpoch != assignment.LeaseEpoch {

		log.Infof("headersync stale_completion peer=%s range_id=%d "+
			"epoch=%d state=%s", peerID, assignment.RangeID,
			assignment.LeaseEpoch, rng.State)
		result.Effects = append(result.Effects, ContinueSync{})
		return
	}

	// Replies are matched by their first parent hash, so a late answer to
	// an earlier, longer request from the same locator (say an anchor
	// discovery request the session already dropped) lands here. If it
	// reaches the range's stop hash at the right height, the range is in
	// it: keep that prefix rather than punish an honest peer.
	expected := int(rng.StopHeight - rng.StartHeight)
	if len(headers) > expected &&
		expected > 0 &&
		headers[expected-1].BlockHash() == rng.StopHash {

		headers = headers[:expected]
	}

	valid := true
	if err := ValidateHeaderRange(rng, headers); err != nil {
		log.Warnf("headersync range_failed peer=%s range_id=%d "+
			"epoch=%d: %v", peerID, assignment.RangeID,
			assignment.LeaseEpoch, err)
		valid = false
	}
	expectedHeaders := int(rng.StopHeight - rng.StartHeight)
	if len(headers) != expectedHeaders {
		log.Warnf("headersync range_failed peer=%s range_id=%d "+
			"expected_headers=%d got=%d",
			peerID, assignment.RangeID,
			expectedHeaders, len(headers))
		valid = false
	}

	// A peer asked for blocks past its advertised height answers with
	// nothing, which is honest. Hold it back for a cooldown instead of
	// quarantining and disconnecting it; the range is replanned either
	// way.
	lagging := len(headers) == 0 &&
		event.PeerHeight < int32(rng.StopHeight)

	// A well formed reply that holds a different header at the range's
	// stop height refutes the stop hash. When only an unvalidated
	// discovery reply vouches for that hash, the reply is evidence against
	// the anchor: the peer may be the honest one. Block it for the
	// cooldown, during which it is leased no range, and reject the anchor
	// below instead of quarantining the peer. A liar can contradict an
	// anchor only with a range reply, so the cooldown bounds how often it
	// forces a replan.
	//
	// The cost falls on honest peers when a persistent liar acts as a
	// discovery reporter: its reply arrives after the first honest one,
	// extends a longer fork, and is never staged, so commit-time
	// validation never convicts it. Each round an honest lessee then
	// contradicts the liar's anchor and is blocked for the cooldown.
	// Liveness holds, since discovery planning ignores peer state, so
	// blocked honest peers still answer discovery and commit. Recording
	// the reporter per anchor would let us block the liar instead, and is
	// the follow-up if this cost matters.
	var contradicted bool
	if !valid && !lagging {
		contradicted, err = d.runtime.Controller().
			StopAnchorContradicted(ctx, rng, headers)
		if err != nil {
			log.Warnf("headersync unable to check stop anchor "+
				"range_id=%d: %v", assignment.RangeID, err)
			contradicted = false
		}
	}
	switch {
	case !valid && lagging:
		log.Infof("headersync range_unavailable peer=%s range_id=%d "+
			"peer_height=%d stop_height=%d", peerID,
			assignment.RangeID, event.PeerHeight, rng.StopHeight)
		d.setPeerState(ctx, peerID, PeerBlocked, result)

	case contradicted:
		log.Warnf("headersync stop_anchor_contradicted peer=%s "+
			"range_id=%d stop_height=%d stop_hash=%s", peerID,
			assignment.RangeID, rng.StopHeight, rng.StopHash)
		d.setPeerState(ctx, peerID, PeerBlocked, result)

	case !valid:
		d.rejectRangePeer(ctx, peerID, assignment.RangeID, result)
	}

	var complete CompleteResult
	err = d.ask(DriverAskCompleteRange, func() error {
		var err error
		if peerActor, ok := d.runtime.Peer(peerID); ok {
			complete, err = peerActor.CompleteRange(
				ctx, assignment.RangeID, assignment.LeaseEpoch,
				valid, time.Now(),
			)
			return err
		}
		complete, err = manager.CompleteRange(
			ctx, peerID, assignment.RangeID, assignment.LeaseEpoch,
			valid, time.Now(),
		)
		return err
	})
	if err != nil {
		log.Warnf(
			"headersync failed to complete range_id=%d peer=%s: "+
				"%v",
			assignment.RangeID,
			peerID,
			err,
		)

		// The ask may have staged the range without its headers;
		// the commit retry requeues such a range. Or it may not have
		// applied at all, leaving the range active with its request
		// already consumed. Blocking the peer lets the existing steal
		// of blocked owners recover that case.
		result.Effects = append(
			result.Effects, ScheduleCommitRetry{},
		)
		d.setPeerState(ctx, peerID, PeerBlocked, result)
		result.Effects = append(result.Effects, ContinueSync{})
		return
	}

	if complete.Stale {
		log.Infof("headersync stale_completion peer=%s range_id=%d "+
			"epoch=%d state=%s", peerID, assignment.RangeID,
			assignment.LeaseEpoch, complete.RangeState)
		result.Effects = append(result.Effects, ContinueSync{})
		return
	}
	if !complete.Accepted || !valid {
		switch {
		case contradicted:
			d.rejectUntrustedAnchor(
				ctx, "anchor_rejected_by_replies",
				rng.StopHeight, rng.StopHash,
				event.AnchorTimeout,
			)

		case !valid:
			d.replanRange(ctx, rng)
		}
		result.Effects = append(result.Effects, ContinueSync{})
		return
	}

	d.session.StageRange(assignment.RangeID, headers, peerID)

	stats, err := manager.CommitStats(ctx, time.Now())
	if err == nil {
		log.Infof(
			"headersync range_staged peer=%s range_id=%d "+
				"start_height=%d stop_height=%d "+
				"staged_depth=%d "+
				"commit_lag=%d oldest_staged_age=%v",
			peerID,
			assignment.RangeID,
			rng.StartHeight,
			rng.StopHeight,
			stats.StagedCount,
			stats.CommitLag,
			stats.OldestStagedAge,
		)
	}

	result.Effects = append(result.Effects, CommitReadyRanges{})
}

// handleStaleRangeReply classifies a reply from a peer that has no range
// request in flight. It reports whether the reply answered a request that
// timed out. Any other reply is not the driver's.
func (d *Driver) handleStaleRangeReply(peerID string,
	result *DriverResult) bool {

	staleRequest, ok := d.session.PopStale(peerID)
	if !ok {
		return false
	}

	if time.Now().After(staleRequest.ExpiresAt) {
		log.Infof("headersync expired_stale_completion peer=%s "+
			"range_id=%d epoch=%d", peerID,
			staleRequest.Assignment.RangeID,
			staleRequest.Assignment.LeaseEpoch)

		if !d.session.HasActiveWork() {
			result.Effects = append(result.Effects, ContinueSync{})
		}

		return true
	}

	assignment := staleRequest.Assignment
	// A timed-out request may still own its old epoch until another peer
	// steals it. Reporting completion here would stage unvalidated work
	// without retaining its headers, permanently blocking ordered commit.
	log.Infof("headersync stale_completion peer=%s range_id=%d "+
		"epoch=%d stale=true", peerID, assignment.RangeID,
		assignment.LeaseEpoch)

	result.Effects = append(result.Effects, ContinueSync{})

	return true
}

// rejectRangePeer punishes a peer that answered a range with invalid headers.
func (d *Driver) rejectRangePeer(ctx context.Context, peerID string,
	rangeID RangeID, result *DriverResult) {

	log.Warnf("headersync peer_rejected peer=%s range_id=%d reason=%s",
		peerID, rangeID, banman.InvalidBlock)

	d.quarantinePeer(ctx, peerID, result)
	result.Effects = append(result.Effects, DisconnectPeer{PeerID: peerID})
}

// handleRejectUntrustedAnchor rejects an untrusted anchor on request.
func (d *Driver) handleRejectUntrustedAnchor(ctx context.Context,
	event RejectUntrustedAnchor, result *DriverResult) {

	result.AnchorRejected = d.rejectUntrustedAnchor(
		ctx, event.Tag, event.Height, event.Hash, event.Timeout,
	)
}

// rejectUntrustedAnchor rejects an untrusted anchor and the unstaged work
// built on it, and logs the outcome under tag. It reports false only when the
// anchor was trusted, replaced or unknown, so nothing changed. A failed ask
// reports true, since the anchor may have been rejected.
func (d *Driver) rejectUntrustedAnchor(ctx context.Context, tag string,
	height uint32, hash Hash, timeout time.Duration) bool {

	result, err := d.runtime.Controller().RejectAnchor(
		ctx, height, hash, time.Now().Add(timeout),
	)
	if err != nil {
		log.Warnf("headersync unable to reject anchor height=%d: %v",
			height, err)
		return true
	}
	if len(result.Anchors) == 0 {
		return false
	}

	log.Infof("headersync %s height=%d invalidated_ranges=%v", tag,
		height, result.Ranges)

	return true
}
