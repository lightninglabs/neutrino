package headersync

import (
	"context"
	"fmt"
	"time"
)

// AdvanceTip tells the driver about a durable tip the serial path wrote, so
// that the FSM's committed tip follows it.
type AdvanceTip struct {
	// Tip is the durable tip.
	Tip ChainPoint
}

func (AdvanceTip) driverEvent() {}

// ListReadyRanges asks which staged ranges are next in line to commit. When
// none are, it also drops staging that a commit left behind.
type ListReadyRanges struct{}

func (ListReadyRanges) driverEvent() {}

// SnapshotReadyRange fetches what the executor needs to write one ready range.
type SnapshotReadyRange struct {
	// RangeID is the range to fetch.
	RangeID RangeID
}

func (SnapshotReadyRange) driverEvent() {}

// InvalidateRange reports that a staged range failed validation at commit.
type InvalidateRange struct {
	// RangeID is the range that failed.
	RangeID RangeID

	// CooldownUntil is when the range's peer can be used again.
	CooldownUntil time.Time
}

func (InvalidateRange) driverEvent() {}

// CommitDurableRanges tells the driver that the ready ranges are durable, so
// that the FSM can advance past them and retire their staging.
type CommitDurableRanges struct{}

func (CommitDurableRanges) driverEvent() {}

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

// handleAdvanceTip moves the FSM's committed tip to a durable tip.
func (d *Driver) handleAdvanceTip(ctx context.Context,
	event AdvanceTip) error {

	return d.runtime.controller.AdvanceCommittedTip(ctx, event.Tip)
}

// handleListReadyRanges reports the ready ranges. With none, a commit that
// applied but reported an error may have left payloads behind, with nothing
// left to commit them, so we prune those.
func (d *Driver) handleListReadyRanges(ctx context.Context,
	result *DriverResult) error {

	ready, err := d.runtime.manager.ReadyRanges(ctx)
	if err != nil {
		return err
	}
	result.Ready = ready
	if len(ready) > 0 {
		return nil
	}

	err = d.runtime.controller.PruneRetiredStaging(ctx)
	if err != nil {
		result.Effects = append(result.Effects, ScheduleCommitRetry{})

		return err
	}
	result.Effects = append(result.Effects, CancelCommitRetry{})

	return nil
}

// handleSnapshotReadyRange fetches one ready range and its headers. If the FSM
// staged the range but its headers never reached the session, because the
// completion applied and then reported an error, we requeue the range so that
// it's fetched again, and report it missing.
func (d *Driver) handleSnapshotReadyRange(ctx context.Context,
	event SnapshotReadyRange, result *DriverResult) error {

	staged, ok := d.session.StagedRange(event.RangeID)
	if !ok {
		_, err := d.runtime.manager.RequeueStagedRange(
			ctx, event.RangeID,
		)
		log.Warnf("headersync range_id=%d ready without staged "+
			"headers, requeued: %v", event.RangeID, err)

		return nil
	}

	rng, ok, err := d.runtime.manager.SnapshotRange(ctx, event.RangeID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("headersync range_id=%d disappeared",
			event.RangeID)
	}

	result.ReadyRange = &ReadyRange{Range: rng, Staged: staged}

	return nil
}

// handleInvalidateRange gives up on a range that failed validation at commit.
func (d *Driver) handleInvalidateRange(ctx context.Context,
	event InvalidateRange, result *DriverResult) error {

	invalidated, err := d.runtime.controller.InvalidateRange(
		ctx, event.RangeID, event.CooldownUntil,
	)
	result.Invalidated = invalidated

	return err
}

// handleCommitDurableRanges advances the FSM past ranges that are now durable.
func (d *Driver) handleCommitDurableRanges(ctx context.Context,
	result *DriverResult) error {

	var err error
	result.Commit, err = d.runtime.controller.CommitReadyRanges(ctx)

	return err
}
