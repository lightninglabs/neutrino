package rescan

import (
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

// OutboxEvent is a sealed interface representing side effects produced by
// FSM state transitions. These are dispatched by the actor wrapper after
// all internal events have been processed.
type OutboxEvent interface {
	outboxEventSealed()
}

// FilteredBlockOutbox signals that a block was connected and contained
// transactions relevant to the current watch list.
type FilteredBlockOutbox struct {
	// Height is the block height.
	Height int32

	// Header is the block header.
	Header *wire.BlockHeader

	// RelevantTxs contains the transactions that matched the watch list.
	RelevantTxs []*btcutil.Tx
}

func (FilteredBlockOutbox) outboxEventSealed() {}

// BlockConnectedOutbox signals that a block was connected but contained no
// relevant transactions. The wallet uses this to advance its sync state.
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

// StartSubscriptionOutbox instructs the actor wrapper to create a new block
// subscription starting from the given height. The bridge goroutine will
// convert subscription notifications into FSM events.
type StartSubscriptionOutbox struct {
	// BestHeight is the height from which the subscription backlog should
	// begin.
	BestHeight uint32
}

func (StartSubscriptionOutbox) outboxEventSealed() {}

// CancelSubscriptionOutbox instructs the actor wrapper to cancel the current
// block subscription and stop the bridge goroutine.
type CancelSubscriptionOutbox struct{}

func (CancelSubscriptionOutbox) outboxEventSealed() {}

// SelfTellOutbox instructs the actor wrapper to send the enclosed event back
// through the actor mailbox. This is used instead of internal events when we
// want the actor to check its mailbox between iterations (e.g., to
// interleave AddWatchAddrs between block processing).
type SelfTellOutbox struct {
	// Event is the event to re-send through the actor mailbox.
	Event RescanEvent
}

func (SelfTellOutbox) outboxEventSealed() {}
