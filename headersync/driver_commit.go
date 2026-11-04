package headersync

// This file holds the commit path of parallel header sync: which staged ranges
// are ready, how their already durable prefix is trimmed, how the batch is
// validated, and what happens once the store has written it. The executor
// writes the batch through the chain store and reports the outcome.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lightninglabs/neutrino/headerlist"
)

// CommitReady asks the driver to commit the staged ranges that are ready:
// validate them as one batch against the cache and ask the executor to write
// it. The error of the result is a ValidationError when a peer's data is at
// fault, and worth retrying otherwise. Production commits through the events
// that stage ranges and through RetryCommit; tests drive this directly.
type CommitReady struct{}

func (CommitReady) driverEvent() {}

// RetryCommit reports that the executor's commit retry wakeup fired. The
// driver commits what is ready and carries on.
type RetryCommit struct{}

func (RetryCommit) driverEvent() {}

// RangesCommitted reports that the chain store wrote a prepared batch. The
// driver adopts it into the cache, retires the ranges and carries on.
type RangesCommitted struct {
	// Prepared is the batch the store wrote.
	Prepared PreparedCommit
}

func (RangesCommitted) driverEvent() {}

// SubmitRangeCommit asks the executor to write a prepared batch through the
// chain store, without waiting for the write. The executor reports the
// outcome with RangesCommitted, or through its reconciliation snapshot when
// the write failed.
type SubmitRangeCommit struct {
	// Prepared is the validated batch.
	Prepared PreparedCommit
}

func (SubmitRangeCommit) driverEffect() {}

// CancelCommitRetry asks the executor to stop its commit retry wakeup, since
// nothing is waiting on storage any more.
type CancelCommitRetry struct{}

func (CancelCommitRetry) driverEffect() {}

// ReadyRange is one range ready to commit, with its headers.
type ReadyRange struct {
	// Range is the FSM's view of the range.
	Range HeaderRange

	// Staged is the range's staged headers.
	Staged StagedRange
}

// advanceTip moves the FSM's committed tip to a durable tip the serial path
// wrote, and drops the staging of the ranges it retired.
func (d *Driver) advanceTip(ctx context.Context, height uint32, hash Hash) {
	err := d.runtime.controller.AdvanceCommittedTip(ctx, ChainPoint{
		Height: height, Hash: hash,
	})
	if err != nil {
		log.Warnf("headersync unable to advance to serial tip "+
			"height=%d: %v", height, err)
	}
}

// resetTipToStore replaces the validation cache with the durable tip and
// moves the FSM to it.
func (d *Driver) resetTipToStore(ctx context.Context) error {
	header, height, err := d.tip.StoreTip()
	if err != nil {
		return err
	}

	// The visible tip must describe the same header as the validation
	// cache, or a later write can start from one and report the other.
	d.tip.Reset(headerlist.Node{Header: *header, Height: int32(height)})

	// A failed serial write may still have landed. Planning stops
	// advancing the FSM once the tip reaches the best height, so move it
	// here, or ranges the write covered stay staged forever.
	d.advanceTip(ctx, height, header.BlockHash())

	return nil
}

// handleRetryCommit commits what is ready and carries on. A commit that failed
// on an invalid range drops its peer instead.
func (d *Driver) handleRetryCommit(ctx context.Context, view View,
	result *driverResult) {

	if _, err := d.commitReadyRanges(ctx, view, result); err != nil {
		log.Warnf("headersync commit_retry_failed: %v", err)
		d.rejectInvalidCommit(ctx, view, err, result)

		return
	}

	then(result, Continue{})
}

// commitAndContinue commits what is ready, then carries on, unless the commit
// failed on an invalid range and the peer was dealt with.
func (d *Driver) commitAndContinue(ctx context.Context, view View,
	result *driverResult) {

	if _, err := d.commitReadyRanges(ctx, view, result); err != nil {
		log.Warnf("headersync failed to commit ready ranges: %v", err)
		if d.rejectInvalidCommit(ctx, view, err, result) {
			return
		}
	}

	then(result, Continue{})
}

// commitReadyRanges validates the ready ranges as one batch and asks the
// executor to write it. It reports whether a write was submitted. A failure
// that is not the peer's fault schedules a retry; one that is invalidates the
// range and is returned as a ValidationError.
func (d *Driver) commitReadyRanges(ctx context.Context, view View,
	result *driverResult) (bool, error) {

	if view.CommitPending {
		return false, nil
	}

	// A write that reported an error may still have landed, so the store,
	// not the validation cache, decides where the next write starts.
	if err := d.tip.ReconcileWithStore(); err != nil {
		result.Effects = append(result.Effects, ScheduleCommitRetry{})

		return false, err
	}

	ready, err := d.listReadyRanges(ctx, result)
	if err != nil {
		return false, err
	}
	if len(ready) == 0 {
		return false, nil
	}

	commitRanges := make([]RangeCommit, 0, len(ready))
	for _, rangeID := range ready {
		readyRange, err := d.snapshotReadyRange(ctx, rangeID)
		if err != nil {
			return false, err
		}

		// Skip whatever part of the range is already durable. The
		// FSM still commits the whole range below.
		commitRange, pending, err := d.trimDurableRange(
			readyRange.Range, readyRange.Staged,
		)
		if err != nil {
			result.Effects = append(
				result.Effects, ScheduleCommitRetry{},
			)

			return false, err
		}
		if pending {
			commitRanges = append(commitRanges, commitRange)
		}
	}

	// The actor writes the batch. Completion arrives as an event, so the
	// handler never waits on the store.
	if len(commitRanges) == 0 {
		return false, d.finishReadyRanges(ctx, result)
	}
	prepared, commitErr := d.tip.PrepareRangeHeaders(commitRanges)
	if commitErr == nil {
		result.Effects = append(result.Effects, SubmitRangeCommit{
			Prepared: prepared,
		})

		return true, nil
	}

	var invalid *ValidationError
	if !errors.As(commitErr, &invalid) {
		result.Effects = append(result.Effects, ScheduleCommitRetry{})

		return false, commitErr
	}
	invalidated, recoveryErr := d.runtime.controller.InvalidateRange(
		ctx, invalid.RangeID,
		time.Now().Add(d.cfg.RequestTimeout()),
	)
	log.Warnf("headersync range_invalidated range_id=%d peer=%s "+
		"invalidated_ranges=%v invalidated_anchors=%d reason=%v",
		invalid.RangeID, invalid.PeerID, invalidated.Ranges,
		len(invalidated.Anchors), commitErr)
	if recoveryErr != nil {
		return false, fmt.Errorf("%w; invalidation failed: %v",
			commitErr, recoveryErr)
	}

	return false, commitErr
}

// listReadyRanges reports the ready ranges. With none, an ambiguous store
// write may have left payloads behind that the durable tip has passed, so we
// prune those.
func (d *Driver) listReadyRanges(ctx context.Context,
	result *driverResult) ([]RangeID, error) {

	ready, err := d.runtime.manager.ReadyRanges(ctx)
	if err != nil {
		return nil, err
	}
	if len(ready) > 0 {
		return ready, nil
	}

	err = d.runtime.controller.PruneRetiredStaging(ctx)
	if err != nil {
		result.Effects = append(result.Effects, ScheduleCommitRetry{})

		return nil, err
	}
	result.Effects = append(result.Effects, CancelCommitRetry{})

	return nil, nil
}

// snapshotReadyRange fetches one ready range and its headers. A range's
// headers are stored right after the FSM stages it, on the same goroutine and
// before any commit can run, so a ready range always has them.
func (d *Driver) snapshotReadyRange(ctx context.Context,
	rangeID RangeID) (ReadyRange, error) {

	staged, ok := d.session.StagedRange(rangeID)
	if !ok {
		return ReadyRange{}, fmt.Errorf("headersync range_id=%d ready "+
			"without staged headers", rangeID)
	}

	rng, ok, err := d.runtime.manager.SnapshotRange(ctx, rangeID)
	if err != nil {
		return ReadyRange{}, err
	}
	if !ok {
		return ReadyRange{}, fmt.Errorf("headersync range_id=%d "+
			"disappeared", rangeID)
	}

	return ReadyRange{Range: rng, Staged: staged}, nil
}

// trimDurableRange drops the prefix of a ready range that is already durable,
// so a retry after an ambiguous write appends only the missing suffix. It
// reports pending=false when the whole range is durable; the caller then
// skips the write and only commits the range in the FSM. A range whose
// durable prefix disagrees with its staged headers is returned unchanged, so
// validation classifies it as before.
func (d *Driver) trimDurableRange(rng HeaderRange,
	staged StagedRange) (RangeCommit, bool, error) {

	commit := RangeCommit{Range: rng, Staged: staged}
	tip := d.cfg.PublishedTip()
	if tip <= rng.StartHeight || len(staged.Headers) == 0 {
		return commit, true, nil
	}

	durable := min(tip-rng.StartHeight, uint32(len(staged.Headers)))
	last := staged.Headers[durable-1]
	stored, err := d.tip.FetchHeaderByHeight(rng.StartHeight + durable)
	if err != nil {
		return commit, false, err
	}
	if stored.BlockHash() != last.BlockHash() {
		return commit, true, nil
	}
	if int(durable) == len(staged.Headers) {
		return commit, false, nil
	}

	commit.Range.StartHeight += durable
	commit.Range.StartHash = last.BlockHash()
	commit.Staged.Headers = staged.Headers[durable:]

	return commit, true, nil
}

// finishReadyRanges advances the FSM past the ready ranges, which are now
// durable, and retires their staging.
func (d *Driver) finishReadyRanges(ctx context.Context,
	result *driverResult) error {

	commitResult, err := d.runtime.controller.CommitReadyRanges(ctx)
	if err != nil {
		result.Effects = append(result.Effects, ScheduleCommitRetry{})

		return err
	}
	result.Effects = append(result.Effects, CancelCommitRetry{})

	commit := commitResult.Commit
	if commitResult.PrunedAnchors > 0 {
		log.Infof("headersync pruned_obsolete_anchors tip_height=%d "+
			"pruned=%d", commit.TipHeight,
			commitResult.PrunedAnchors)
	}
	for _, rangeID := range commit.Committed {
		d.session.DeleteStagedRange(rangeID)
	}
	for _, rangeID := range commit.Committed {
		log.Infof("headersync range_committed range_id=%d "+
			"tip_height=%d tip_hash=%s", rangeID, commit.TipHeight,
			commit.TipHash)
	}

	return nil
}

// handleRangesCommitted adopts a written batch into the validation cache,
// retires its ranges and carries on.
func (d *Driver) handleRangesCommitted(ctx context.Context,
	event RangesCommitted, result *driverResult) {

	d.logProgress(event.Prepared.Nodes)
	d.tip.FinishRange(event.Prepared)

	if err := d.finishReadyRanges(ctx, result); err != nil {
		log.Warnf("headersync completion failed: %v", err)

		return
	}

	then(result, Continue{})
}

// rejectInvalidCommit drops the peer whose range failed validation at commit
// and carries on. It reports false when the error was not a peer's fault.
func (d *Driver) rejectInvalidCommit(ctx context.Context, view View,
	err error, result *driverResult) bool {

	var invalid *ValidationError
	if !errors.As(err, &invalid) {
		return false
	}
	for _, peer := range view.Peers {
		if peer.ID == invalid.PeerID {
			d.rejectRangePeer(ctx, peer.ID, invalid.RangeID, result)
			break
		}
	}

	then(result, Continue{})

	return true
}

// logProgress reports headers that became durable to the executor's progress
// logger.
func (d *Driver) logProgress(nodes []headerlist.Node) {
	if d.cfg.LogProgress == nil {
		return
	}
	for _, node := range nodes {
		d.cfg.LogProgress(node.Header.Timestamp, node.Height)
	}
}
