package headersync

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/stretchr/testify/require"
)

// serialHeaderChain returns a chain of headers that connect to each other,
// where header i has nonce i+1.
func serialHeaderChain(count int) []*wire.BlockHeader {
	headers := make([]*wire.BlockHeader, 0, count)
	var prev wire.BlockHeader
	for i := 0; i < count; i++ {
		header := wire.BlockHeader{Nonce: uint32(i + 1)}
		if i > 0 {
			header.PrevBlock = prev.BlockHash()
		}
		headers = append(headers, &header)
		prev = header
	}

	return headers
}

// TestSerialReorgPreparationCopiesHeaders binds the fork and replacement to
// the observed tip without retaining the caller's mutable headers. The bound
// tip lets the store owner reject a reorg that was prepared against a stale
// chain, and the copies keep a later mutation of the peer's message from
// changing what is written.
func TestSerialReorgPreparationCopiesHeaders(t *testing.T) {
	t.Parallel()

	chain := serialHeaderChain(4)
	tip := headerlist.Node{Header: *chain[3], Height: 3}
	fork := *chain[1]
	replacement := wire.BlockHeader{
		PrevBlock: fork.BlockHash(), Nonce: 100,
	}
	forkHash, replacementHash := fork.BlockHash(), replacement.BlockHash()
	prepared := NewSerialReorg(tip, fork, 1, replacement)

	// The request is bound to the observed tip.
	require.Equal(t, tip.Header.BlockHash(), prepared.ExpectedTip.Hash)
	require.Equal(t, int32(3), prepared.ExpectedTip.Height)

	// Mutating the inputs leaves the prepared copies intact.
	fork.Nonce++
	replacement.Nonce++
	require.Equal(t, forkHash, prepared.Fork.Header.BlockHash())
	require.Equal(t, replacementHash, prepared.Replacement.BlockHash())
}

// TestHeadersAfterTip checks which part of a connected batch follows the
// cache tip.
func TestHeadersAfterTip(t *testing.T) {
	t.Parallel()

	chain := serialHeaderChain(4)
	tip := &headerlist.Node{Header: *chain[1], Height: 1}

	// A batch that starts at the tip's child is taken whole.
	rest, ok := headersAfterTip(tip, chain[2:])
	require.True(t, ok)
	require.Equal(t, chain[2:], rest)

	// Old headers before the tip are skipped.
	rest, ok = headersAfterTip(tip, chain)
	require.True(t, ok)
	require.Equal(t, chain[2:], rest)

	// A batch that ends at the tip has nothing new.
	_, ok = headersAfterTip(tip, chain[:2])
	require.False(t, ok)

	// A batch that never meets the tip does not extend it.
	_, ok = headersAfterTip(tip, chain[3:])
	require.False(t, ok)
}

// newSerialTestDriver returns a driver whose validation cache starts at the
// first header of the chain.
func newSerialTestDriver(chain []*wire.BlockHeader) *Driver {
	tip := NewTipState(TipConfig{Params: &chaincfg.SimNetParams})
	tip.Reset(headerlist.Node{Header: *chain[0], Height: 0})

	fsm := NewHeaderSyncFSM(0, chain[0].BlockHash(), DefaultConfig())
	runtime := &Runtime{manager: NewManager(fsm)}

	return newDriver(runtime, NewSession(), tip, DriverConfig{})
}

// TestTipHeadersDisconnectsUnconnected checks that a message whose headers do
// not build on each other costs the peer its connection, and nothing else.
func TestTipHeadersDisconnectsUnconnected(t *testing.T) {
	t.Parallel()

	chain := serialHeaderChain(3)
	driver := newSerialTestDriver(chain)

	result, err := driver.Handle(t.Context(), TipHeaders{
		PeerID:  "peer",
		Headers: []*wire.BlockHeader{chain[1], chain[0]},
	}, View{})
	require.NoError(t, err)
	require.Equal(
		t, []DriverEffect{DisconnectTipPeer{PeerID: "peer"}},
		result.Effects,
	)
}

// TestTipLeaseFollowsTheSyncPeer checks that the lease names the peer the
// executor last reported, that granting it again keeps the epoch, and that only
// the holder is trusted.
func TestTipLeaseFollowsTheSyncPeer(t *testing.T) {
	t.Parallel()

	chain := serialHeaderChain(3)
	driver := newSerialTestDriver(chain)
	manager := driver.runtime.manager

	_, err := driver.Handle(t.Context(), FollowTip{PeerID: "a"}, View{})
	require.NoError(t, err)
	first := manager.TipLease()
	require.Equal(t, "a", first.PeerID)

	_, err = driver.Handle(t.Context(), FollowTip{PeerID: "a"}, View{})
	require.NoError(t, err)
	require.Equal(t, first, manager.TipLease())

	_, err = driver.Handle(t.Context(), FollowTip{PeerID: ""}, View{})
	require.NoError(t, err)
	released := manager.TipLease()
	require.Empty(t, released.PeerID)
	require.Greater(t, released.Epoch, first.Epoch)
	require.False(t, manager.fsm.TrustsTipPeer(""))
}

// TestTipHeadersIgnoresForeignPeerWhileSyncing checks that headers which do
// not extend the tip are dropped without a store read when they come from a
// peer that is not the sync peer and we are not current.
func TestTipHeadersIgnoresForeignPeerWhileSyncing(t *testing.T) {
	t.Parallel()

	chain := serialHeaderChain(3)
	driver := newSerialTestDriver(chain)

	result, err := driver.Handle(t.Context(), TipHeaders{
		PeerID:  "peer",
		Headers: chain[:1],
	}, View{})
	require.NoError(t, err)
	require.Empty(t, result.Effects)
}
