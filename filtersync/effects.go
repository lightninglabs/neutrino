package filtersync

import (
	"time"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

// Effect is a side effect the Syncer asks its run loop to perform. The Syncer
// itself reads no store, sends no message, arms no timer and never blocks.
// The run loop reports the outcome of each effect back as an event.
//
// The run loop performs the effects of one batch in order. Every effect but
// PollQuit yields an event, and PollQuit yields one only when the quit
// channel is closed, so the Syncer places the effect that always yields an
// event last in its batch. Once an effect yields an event, the rest of the
// batch is dropped.
type Effect interface {
	syncEffect()
}

// WaitCondition names the condition a WaitForTips effect waits for.
type WaitCondition uint8

const (
	// WaitLagOrSynced waits until the filter header tip lags the block
	// header tip by at least a checkpoint interval, or the block header
	// sync is current.
	WaitLagOrSynced WaitCondition = iota

	// WaitTipMismatch waits until the filter header tip hash differs from
	// the block header tip hash.
	WaitTipMismatch
)

// String returns the name of the condition.
func (c WaitCondition) String() string {
	switch c {
	case WaitLagOrSynced:
		return "lag_or_synced"
	case WaitTipMismatch:
		return "tip_mismatch"
	default:
		return "unknown"
	}
}

// WaitForTips blocks until Condition holds for the visible tips, and yields
// TipsChanged, or Quit if the block manager stops first.
type WaitForTips struct {
	// Condition is the condition to wait for.
	Condition WaitCondition
}

func (WaitForTips) syncEffect() {}

// ReadBlockTip reads the block header tip from the store, then snapshots the
// tip feed, and yields BlockTipRead.
type ReadBlockTip struct{}

func (ReadBlockTip) syncEffect() {}

// FetchCheckpoints asks every peer for its filter checkpoints up to StopHash,
// and yields CheckpointsFetched.
type FetchCheckpoints struct {
	// FilterType is the filter type to ask for.
	FilterType wire.FilterType

	// StopHash is the block hash the checkpoints are requested up to.
	StopHash chainhash.Hash

	// Height is the height of StopHash.
	Height uint32
}

func (FetchCheckpoints) syncEffect() {}

// ResolveConflict compares the checkpoints the peers served with each other,
// with the hardcoded checkpoints and with the store, bans the peers that lie,
// and yields ConflictResolved.
type ResolveConflict struct {
	// FilterType is the filter type the checkpoints are for.
	FilterType wire.FilterType

	// Checkpoints maps each peer to its checkpoints, capped at the block
	// header tip. The resolver may remove peers from it.
	Checkpoints map[string][]*chainhash.Hash
}

func (ResolveConflict) syncEffect() {}

// FetchCheckpointed catches the filter header store up with Checkpoints, and
// yields CheckpointedFetched.
type FetchCheckpointed struct {
	// FilterType is the filter type to fetch.
	FilterType wire.FilterType

	// Checkpoints are the agreed checkpoints. They are empty when the
	// chain is shorter than one checkpoint interval.
	Checkpoints []*chainhash.Hash
}

func (FetchCheckpointed) syncEffect() {}

// FetchUncheckpointed fetches the filter headers between the filter header
// tip and the block header tip from every peer, and yields
// UncheckpointedFetched.
type FetchUncheckpointed struct {
	// FilterType is the filter type to fetch.
	FilterType wire.FilterType
}

func (FetchUncheckpointed) syncEffect() {}

// RetryAfter sleeps for Delay and yields RetryElapsed, or Quit if the block
// manager stops first.
type RetryAfter struct {
	// Delay is how long to sleep.
	Delay time.Duration
}

func (RetryAfter) syncEffect() {}

// PollQuit checks the quit channel without blocking. It yields Quit if the
// channel is closed and nothing otherwise, so the run loop carries on with
// the rest of the batch.
type PollQuit struct{}

func (PollQuit) syncEffect() {}
