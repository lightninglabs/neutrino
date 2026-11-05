package filtersync

import (
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/lightninglabs/neutrino/chainstore"
)

// Event is an input to the Syncer. Events carry everything the Syncer needs
// to know about the world that it does not own: the visible tips, the block
// header tip it will sync up to, and the outcome of each network round. The
// Syncer never reads them from anywhere else.
type Event interface {
	syncEvent()
}

// Start begins filter header sync. It is the first event the run loop feeds.
type Start struct {
	// Tips is the current snapshot of the tip feed.
	Tips chainstore.Tips
}

func (Start) syncEvent() {}

// TipsChanged reports that the wait requested by WaitForTips returned because
// its condition holds.
type TipsChanged struct {
	// Tips is the snapshot that satisfied the condition.
	Tips chainstore.Tips
}

func (TipsChanged) syncEvent() {}

// BlockTipRead is the outcome of ReadBlockTip.
type BlockTipRead struct {
	// Hash is the hash of the block header tip. It is only set when Err
	// is nil.
	Hash chainhash.Hash

	// Height is the height of the block header tip. It is only set when
	// Err is nil.
	Height uint32

	// Tips is the snapshot of the tip feed taken after the read. It is
	// only set when Err is nil.
	Tips chainstore.Tips

	// Err is the store error, if the read failed.
	Err error
}

func (BlockTipRead) syncEvent() {}

// CheckpointsFetched is the outcome of FetchCheckpoints.
type CheckpointsFetched struct {
	// ByPeer maps each peer that answered to the checkpoints it served.
	// It is empty when no peer answered.
	ByPeer map[string][]*chainhash.Hash
}

func (CheckpointsFetched) syncEvent() {}

// ConflictResolved is the outcome of ResolveConflict.
type ConflictResolved struct {
	// Good is the checkpoint list the peers and the store agree on. It is
	// empty when no agreement was reached.
	Good []*chainhash.Hash

	// Err is the error the resolution reported, if any.
	Err error
}

func (ConflictResolved) syncEvent() {}

// CheckpointedFetched is the outcome of FetchCheckpointed.
type CheckpointedFetched struct {
	// Err is the store or response error, if the fetch failed.
	Err error

	// HeadersSynced reports whether the block header sync is current. It
	// is read after the fetch and is only meaningful when Err is nil.
	HeadersSynced bool

	// Tips is the snapshot of the tip feed taken after the fetch. It is
	// only meaningful when Err is nil.
	Tips chainstore.Tips
}

func (CheckpointedFetched) syncEvent() {}

// UncheckpointedFetched is the outcome of FetchUncheckpointed.
type UncheckpointedFetched struct {
	// Err is the error the fetch reported, if any.
	Err error
}

func (UncheckpointedFetched) syncEvent() {}

// RetryElapsed reports that the delay requested by RetryAfter has passed.
type RetryElapsed struct {
	// Tips is the snapshot of the tip feed taken after the delay.
	Tips chainstore.Tips
}

func (RetryElapsed) syncEvent() {}

// Quit reports that the block manager is shutting down. The run loop feeds it
// when a wait, a delay or a PollQuit observes the quit channel closed.
type Quit struct{}

func (Quit) syncEvent() {}
