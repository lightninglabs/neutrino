package rescan

import (
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

// OutboxEvent is a sealed interface representing side effects produced by
// FSM state transitions. The actor dispatches them, in order, once the
// transition that produced them has committed.
type OutboxEvent interface {
	outboxEventSealed()
}

// FilteredBlockOutbox signals that a block was scanned. It is emitted for
// every scanned block, matched or not, immediately before the block's
// BlockConnectedOutbox.
type FilteredBlockOutbox struct {
	// Height is the block height.
	Height int32

	// Header is the block header.
	Header *wire.BlockHeader

	// RelevantTxs contains the transactions that matched the watch list.
	// It is empty for a block with no match.
	RelevantTxs []*btcutil.Tx
}

func (FilteredBlockOutbox) outboxEventSealed() {}

// BlockConnectedOutbox signals that a block was scanned. It is emitted for
// every scanned block, right after its FilteredBlockOutbox. The wallet uses
// it to advance its sync state.
type BlockConnectedOutbox struct {
	// Header is the exact block header processed by the FSM.
	Header *wire.BlockHeader

	// Hash is the block hash.
	Hash chainhash.Hash

	// Height is the block height.
	Height int32

	// Timestamp is the block timestamp.
	Timestamp int64
}

func (BlockConnectedOutbox) outboxEventSealed() {}

// BlockDisconnectedOutbox signals that a block was disconnected during a
// reorg or rewind.
type BlockDisconnectedOutbox struct {
	// Hash is the disconnected block's hash.
	Hash chainhash.Hash

	// Height is the disconnected block's height.
	Height int32

	// Header is the disconnected block's header.
	Header *wire.BlockHeader
}

func (BlockDisconnectedOutbox) outboxEventSealed() {}

// RescanProgressOutbox reports progress during historical catch-up scanning.
type RescanProgressOutbox struct {
	// Height is the last processed block height.
	Height int32
}

func (RescanProgressOutbox) outboxEventSealed() {}

// RescanFinishedOutbox signals that the rescan has caught up to the chain
// tip and is now current.
type RescanFinishedOutbox struct {
	// Hash is the hash of the block at which the rescan became current.
	Hash chainhash.Hash

	// Height is the height at which the rescan became current.
	Height int32
}

func (RescanFinishedOutbox) outboxEventSealed() {}

// ContinueOutbox instructs the actor wrapper to send a ProcessNextBlockEvent
// back through its own mailbox. A state emits it instead of looping so the
// mailbox can interleave AddWatchAddrs and notifications between batches.
type ContinueOutbox struct{}

func (ContinueOutbox) outboxEventSealed() {}
