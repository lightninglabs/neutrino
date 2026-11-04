package headersync

import (
	"context"
	"time"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/banman"
)

// HeadersReceived delivers a headers message. The driver matches it against
// the peer's anchor request, then its range request, and otherwise reads it
// as headers that follow the tip. An empty message that answers no request
// depends on the executor's serial deadline, so the driver hands it back with
// NoProgressReply and the executor settles it.
type HeadersReceived struct {
	// PeerID is the address of the peer that sent the headers.
	PeerID string

	// PeerHeight is the height the peer advertises.
	PeerHeight int32

	// Headers are the headers of the message.
	Headers []*wire.BlockHeader

	// HeadersSynced is set when the node believes it is current.
	HeadersSynced bool
}

func (HeadersReceived) driverEvent() {}

// rangeReply is a headers message judged against the peer's range request.
type rangeReply struct {
	peerID string

	// peerHeight is the height the peer advertises.
	peerHeight int32

	headers []*wire.BlockHeader

	// anchorTimeout is how long the rejection of an anchor that the reply
	// refutes stays recorded.
	anchorTimeout time.Duration
}

// ScheduleCommitRetry asks the executor to keep one wakeup while staging
// waits for storage. A recovered store must not need another network
// response to drain staging.
type ScheduleCommitRetry struct{}

func (ScheduleCommitRetry) driverEffect() {}

// NoProgressReply tells the executor that a headers message carried no
// headers and answered none of the driver's requests. Whether it answers the
// serial request, and whether that costs the sync peer its role, depends on
// the serial deadline, which only the executor sees, so the executor settles
// it. It is the only effect of its event.
type NoProgressReply struct {
	// PeerID is the address of the peer that sent the message.
	PeerID string
}

func (NoProgressReply) driverEffect() {}

// handleHeadersReceived finds the request a headers message answers.
func (d *Driver) handleHeadersReceived(ctx context.Context,
	event HeadersReceived, view View, result *DriverResult) {

	peerID := event.PeerID
	headers := event.Headers
	if d.handleAnchorHeaders(ctx, view, peerID, headers, result) {
		return
	}

	handled := d.handleRangeReply(ctx, rangeReply{
		peerID:        peerID,
		peerHeight:    event.PeerHeight,
		headers:       headers,
		anchorTimeout: d.cfg.RequestTimeout(),
	}, view, result)
	if handled {
		return
	}

	// Nothing to do for an empty headers message, beyond settling the
	// sync peer's outstanding request, which the executor does.
	if len(headers) == 0 {
		result.Effects = append(result.Effects, NoProgressReply{
			PeerID: peerID,
		})

		return
	}

	// The headers follow the tip: an extension, known headers, a reorg, a
	// checkpoint failure or garbage.
	d.handleTipHeaders(TipHeaders{
		PeerID:        peerID,
		Headers:       headers,
		HeadersSynced: event.HeadersSynced,
	}, result)
}

// handleRangeReply judges a headers message against the range request of the
// peer, if it has one. It reports whether the message was the driver's to
// take, as a range reply or as the late answer to a request that timed out.
func (d *Driver) handleRangeReply(ctx context.Context, event rangeReply,
	view View, result *DriverResult) bool {

	peerID := event.peerID
	headers := event.headers
	request, ok := d.session.FinishRangeHeaders(peerID, headers)
	if !ok {
		if d.session.RangeActive(peerID) {
			log.Debugf("headersync unmatched_range_reply peer=%s",
				peerID)

			return true
		}

		return d.handleStaleRangeReply(peerID, result)
	}

	assignment := request.Assignment
	manager := d.runtime.manager
	rng, ok, err := manager.SnapshotRange(ctx, assignment.RangeID)
	if err != nil {
		log.Warnf("headersync unable to snapshot range_id=%d: %v",
			assignment.RangeID, err)
		return true
	}
	if !ok {
		log.Warnf(
			"headersync received headers for unknown range_id=%d "+
				"peer=%s",
			assignment.RangeID,
			peerID,
		)
		return true
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
		then(result, Continue{})
		return true
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
		event.peerHeight < int32(rng.StopHeight)

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
		contradicted, err = d.runtime.controller.
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
			assignment.RangeID, event.peerHeight, rng.StopHeight)
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
	if peerActor, ok := d.runtime.Peer(peerID); ok {
		complete, err = peerActor.CompleteRange(
			ctx, assignment.RangeID, assignment.LeaseEpoch,
			valid, time.Now(),
		)
	} else {
		complete, err = manager.CompleteRange(
			ctx, peerID, assignment.RangeID, assignment.LeaseEpoch,
			valid, time.Now(),
		)
	}
	if err != nil {
		log.Warnf(
			"headersync failed to complete range_id=%d peer=%s: "+
				"%v",
			assignment.RangeID,
			peerID,
			err,
		)

		// The completion didn't apply, which leaves the range active
		// with its request already consumed. Blocking the peer lets the
		// steal of blocked owners recover it.
		result.Effects = append(
			result.Effects, ScheduleCommitRetry{},
		)
		d.setPeerState(ctx, peerID, PeerBlocked, result)
		then(result, Continue{})
		return true
	}

	if complete.Stale {
		log.Infof("headersync stale_completion peer=%s range_id=%d "+
			"epoch=%d state=%s", peerID, assignment.RangeID,
			assignment.LeaseEpoch, complete.RangeState)
		then(result, Continue{})
		return true
	}
	if !complete.Accepted || !valid {
		switch {
		case contradicted:
			d.rejectUntrustedAnchor(
				ctx, "anchor_rejected_by_replies",
				rng.StopHeight, rng.StopHash,
				event.anchorTimeout,
			)

		case !valid:
			d.replanRange(ctx, rng)
		}
		then(result, Continue{})
		return true
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

	d.commitAndContinue(ctx, view, result)

	return true
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
			then(result, Continue{})
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

	then(result, Continue{})

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

// rejectUntrustedAnchor rejects an untrusted anchor and the unstaged work
// built on it, and logs the outcome under tag. It reports false only when the
// anchor was trusted, replaced or unknown, so nothing changed. A failed call
// reports true, since the anchor may have been rejected.
func (d *Driver) rejectUntrustedAnchor(ctx context.Context, tag string,
	height uint32, hash Hash, timeout time.Duration) bool {

	result, err := d.runtime.controller.RejectAnchor(
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
