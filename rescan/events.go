package rescan

import (
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/wire"
	neutrino "github.com/lightninglabs/neutrino"
)

// RescanEvent is a sealed interface representing all events that can be
// processed by the rescan FSM. The unexported marker method prevents external
// packages from implementing this interface.
type RescanEvent interface {
	rescanEventSealed()
}

// BlockConnectedEvent signals that a new block has been connected to the
// chain. This is delivered either from the subscription bridge goroutine
// (when current) or synthesized during historical catch-up.
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
	Addrs []btcutil.Address

	// Inputs contains new outpoints to watch for spends.
	Inputs []neutrino.InputWithScript

	// RewindTo specifies the height to rewind to after updating the watch
	// list. If nil, no rewind is performed and only future blocks are
	// scanned with the updated filter.
	RewindTo *uint32
}

func (AddWatchAddrsEvent) rescanEventSealed() {}

// ProcessNextBlockEvent is a self-directed event that triggers processing
// of the next block during syncing or rewinding. It is emitted as an outbox
// event (SelfTellOutbox) rather than an internal event so that the actor
// mailbox can interleave AddWatchAddrs messages between blocks.
type ProcessNextBlockEvent struct{}

func (ProcessNextBlockEvent) rescanEventSealed() {}

// StopEvent signals the rescan should shut down gracefully.
type StopEvent struct{}

func (StopEvent) rescanEventSealed() {}
