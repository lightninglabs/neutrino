package rescan

import (
	"time"

	"github.com/lightninglabs/neutrino/headerfs"
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

	// EndBlock is the block the scan stops at. The zero value never
	// matches, so the scan follows the chain until it is stopped.
	EndBlock headerfs.BlockStamp
}

// reachedEnd reports whether the cursor at stamp has reached the end block.
// It matches the legacy rescan's test: the end block's hash, or its height
// when that is set.
func (e *Environment) reachedEnd(stamp headerfs.BlockStamp) bool {
	return stamp.Hash == e.EndBlock.Hash ||
		(e.EndBlock.Height > 0 && stamp.Height == e.EndBlock.Height)
}

// DefaultBatchSize is the number of blocks the actor scans per mailbox turn
// during historical sync, unless a request is waiting. It amortizes the
// per-turn overhead.
const DefaultBatchSize = 100

// ProgressInterval controls how frequently RescanProgressOutbox events are
// emitted during historical sync (every N blocks).
const ProgressInterval = 10_000
