package rescan

import (
	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/filtermatch"
)

// RescanEvent is a sealed interface representing all events that can be
// processed by the rescan FSM. The unexported marker method prevents external
// packages from implementing this interface.
type RescanEvent interface {
	rescanEventSealed()
}

// BlockConnectedEvent signals that a new block has been connected to the
// chain. The subscription bridge delivers it in every state. Only Current
// scans it: the other states catch up from the header store instead, which
// already holds the block by the time its notification arrives.
type BlockConnectedEvent struct {
	// Header is the header of the connected block.
	Header wire.BlockHeader

	// Height is the height of the connected block.
	Height uint32
}

func (BlockConnectedEvent) rescanEventSealed() {}

// BlockDisconnectedEvent signals that a block has been disconnected from the
// chain tip due to a reorg. Delivered from the subscription bridge goroutine.
type BlockDisconnectedEvent struct {
	// Header is the header of the disconnected block.
	Header wire.BlockHeader

	// Height is the height of the disconnected block.
	Height uint32

	// ChainTip is the new chain tip header after the disconnection.
	ChainTip wire.BlockHeader
}

func (BlockDisconnectedEvent) rescanEventSealed() {}

// AddWatchAddrsEvent requests that new addresses and/or outpoints be added
// to the rescan watch list. If RewindTo is non-nil, the FSM will rewind to
// that height and re-scan all subsequent blocks with the updated watch list.
type AddWatchAddrsEvent struct {
	// Addrs contains new addresses to watch for incoming payments.
	Addrs []address.Address

	// Inputs contains new outpoints to watch for spends.
	Inputs []filtermatch.InputWithScript

	// RewindTo specifies the height to rewind to after updating the watch
	// list. The block at RewindTo is kept, and every block above it is
	// disconnected and scanned again. If nil, or not below the scanned
	// tip, no rewind is performed and only future blocks are scanned with
	// the updated filter.
	RewindTo *uint32
}

func (AddWatchAddrsEvent) rescanEventSealed() {}

// ProcessNextBlockEvent is the continuation event. It leaves Initializing,
// and runs the next batch of up to Environment.BatchSize blocks in Syncing
// or Rewinding. Current ignores it. A state asks for it with a
// ContinueOutbox rather than looping, so the actor mailbox can interleave
// AddWatchAddrs and notifications between batches.
type ProcessNextBlockEvent struct{}

func (ProcessNextBlockEvent) rescanEventSealed() {}

// StopEvent moves every state to Terminal. The actor sends it when Stop runs
// and when the block subscription fails.
type StopEvent struct{}

func (StopEvent) rescanEventSealed() {}
