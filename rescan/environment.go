package rescan

import (
	"time"
)

// Environment holds the immutable dependencies available to all FSM states
// during event processing. The environment is never mutated. All mutable
// state, the Cursor and its watch list, lives in the state structs and is
// carried forward through state transitions.
type Environment struct {
	// Chain provides access to block headers, filters, blocks, and
	// subscriptions.
	Chain ChainSource

	// StartTime is the wallet birthday. Blocks before this time are
	// skipped during scanning (only headers are advanced without filter
	// checks).
	StartTime time.Time

	// BatchSize controls how many blocks are processed per
	// ProcessNextBlockEvent while syncing or rewinding. This amortizes the
	// continuation overhead while still allowing AddWatchAddrs to
	// interleave between batches. Zero means DefaultBatchSize.
	BatchSize int
}

// DefaultBatchSize is the number of blocks processed per batch during
// historical sync. Chosen to balance throughput (avoiding per-block mailbox
// overhead) with responsiveness to filter updates.
const DefaultBatchSize = 100

// ProgressInterval controls how frequently RescanProgressOutbox events are
// emitted during historical sync (every N blocks).
const ProgressInterval = 10_000

// batchSize returns BatchSize, or DefaultBatchSize when it is not set.
func (e *Environment) batchSize() int {
	if e.BatchSize <= 0 {
		return DefaultBatchSize
	}

	return e.BatchSize
}
