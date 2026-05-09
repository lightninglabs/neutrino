package rescan

import (
	"time"

	neutrino "github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/headerfs"
)

// Environment holds the immutable dependencies available to all FSM states
// during event processing. The environment is NEVER mutated — all mutable
// state (CurStamp, WatchAddrs, WatchList, etc.) lives in the state structs
// themselves and is carried forward via state transitions.
type Environment struct {
	// Chain provides access to block headers, filters, blocks, and
	// subscriptions.
	Chain neutrino.ChainSource

	// StartTime is the wallet birthday. Blocks before this time are
	// skipped during scanning (only headers are advanced without filter
	// checks).
	StartTime time.Time

	// EndBlock is an optional block stamp at which the rescan should
	// stop. If nil, the rescan runs until stopped. This is used for
	// bounded historical scans (e.g., utxoscanner).
	EndBlock *headerfs.BlockStamp

	// QueryOpts are query options passed through to chain queries
	// (e.g., timeout, peer selection).
	QueryOpts []neutrino.QueryOption

	// BatchSize controls how many blocks are processed per
	// ProcessNextBlockEvent during syncing. This amortizes the self-Tell
	// overhead while still allowing AddWatchAddrs to interleave between
	// batches.
	BatchSize int
}

// DefaultBatchSize is the number of blocks processed per batch during
// historical sync. Chosen to balance throughput (avoiding per-block mailbox
// overhead) with responsiveness to filter updates.
const DefaultBatchSize = 100

// ProgressInterval controls how frequently RescanProgressOutbox events are
// emitted during historical sync (every N blocks).
const ProgressInterval = 10_000
