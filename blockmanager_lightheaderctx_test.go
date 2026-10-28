package neutrino

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/stretchr/testify/require"
)

// buildSkipListHeaderChain populates a BoundedMemoryChain with nHeaders
// sequential, distinct headers and returns the chain. Headers are constructed
// with monotonically increasing timestamps and difficulty bits so each node is
// easily distinguishable when asserting on ancestor results.
func buildSkipListHeaderChain(maxNodes,
	nHeaders uint32) *headerlist.BoundedMemoryChain {

	chain := headerlist.NewBoundedMemoryChain(maxNodes)
	for i := uint32(0); i < nHeaders; i++ {
		chain.PushBack(headerlist.Node{
			Height: int32(i),
			Header: wire.BlockHeader{
				Version:   1,
				Timestamp: time.Unix(int64(1_700_000_000+i), 0),
				Bits:      0x1d00ffff + i,
			},
		})
	}

	return chain
}

// TestLightHeaderCtxRelativeAncestorSkipList ensures that
// lightHeaderCtx.RelativeAncestorCtx satisfies ancestor lookups through the
// in-memory headerList skip-list when the target is in range, and that the
// returned context carries the resolved headerList node so further ancestor
// walks can keep amortizing along the skip-list. The block header store is
// configured to fail the test if it is consulted on this path.
func TestLightHeaderCtxRelativeAncestorSkipList(t *testing.T) {
	t.Parallel()

	const nHeaders = 256
	chain := buildSkipListHeaderChain(nHeaders, nHeaders)
	tip := chain.Back()
	require.NotNil(t, tip)
	require.Equal(t, int32(nHeaders-1), tip.Height)

	store := &headerfs.MockBlockHeaderStore{}

	ctx := newLightHeaderCtx(tip.Height, &tip.Header, store, chain)

	// Walking back one step at a time from the tip must reach height 0
	// without ever touching the store, and every returned context must
	// carry a non-nil headerList node so the skip-list inheritance from
	// the original tip is preserved across the chain of Parent calls.
	cur := ctx
	for h := int32(nHeaders - 2); h >= 0; h-- {
		expectedHeight := h
		parent := cur.RelativeAncestorCtx(1)
		require.NotNil(t, parent, "no parent at expected height %d",
			expectedHeight)
		require.Equal(t, expectedHeight, parent.Height())

		lhc, ok := parent.(*lightHeaderCtx)
		require.True(t, ok, "RelativeAncestorCtx returned a "+
			"non-lightHeaderCtx value")
		require.NotNil(t, lhc.node, "skip-list node not propagated "+
			"into ancestor at height %d", expectedHeight)
		require.Equal(t, expectedHeight, lhc.node.Height)

		cur = lhc
	}

	// A single deep skip from the tip must also resolve through the
	// skip-list and carry the resolved node.
	deep := ctx.RelativeAncestorCtx(int32(nHeaders - 1))
	require.NotNil(t, deep)
	require.Equal(t, int32(0), deep.Height())

	deepLhc, ok := deep.(*lightHeaderCtx)
	require.True(t, ok)
	require.NotNil(t, deepLhc.node)
	require.Equal(t, int32(0), deepLhc.node.Height)

	// At no point above did we need the store; FetchHeaderByHeight must
	// not have been called.
	store.AssertNotCalled(t, "FetchHeaderByHeight")
}

// TestLightHeaderCtxRelativeAncestorStoreFallback ensures that when the
// requested ancestor has aged out of the bounded in-memory headerList,
// lightHeaderCtx.RelativeAncestorCtx falls back to the block header store and
// returns a context whose node is nil (since there is no in-memory node to
// thread through). This locks in the documented degradation path: results
// are still correct, but subsequent ancestor walks from that context will
// re-anchor on the chain tip rather than skipping from the resolved node.
func TestLightHeaderCtxRelativeAncestorStoreFallback(t *testing.T) {
	t.Parallel()

	// Build a chain that wraps: 256 headers pushed into a 32-slot
	// BoundedMemoryChain. Only heights 224..255 remain reachable in
	// memory; anything below 224 must come from the store.
	const (
		capacity = 32
		nHeaders = 256
	)
	chain := buildSkipListHeaderChain(capacity, nHeaders)
	tip := chain.Back()
	require.NotNil(t, tip)
	require.Equal(t, int32(nHeaders-1), tip.Height)

	// The ancestor we'll ask for is height 100, which is far below the
	// in-memory window. The store must be consulted to satisfy the
	// request.
	const targetHeight = uint32(100)
	storedHeader := &wire.BlockHeader{
		Version:   1,
		Timestamp: time.Unix(int64(1_700_000_000+targetHeight), 0),
		Bits:      0x1d00ffff + targetHeight,
	}

	store := &headerfs.MockBlockHeaderStore{}
	store.On("FetchHeaderByHeight", targetHeight).Return(storedHeader, nil)

	ctx := newLightHeaderCtx(tip.Height, &tip.Header, store, chain)

	// Distance from the tip down to targetHeight pushes us through the
	// store-fallback path. The resulting context describes the correct
	// height and header, but its skip-list node must be nil because the
	// header is not reachable in the bounded in-memory chain.
	distance := tip.Height - int32(targetHeight)
	ancestor := ctx.RelativeAncestorCtx(distance)
	require.NotNil(t, ancestor)
	require.Equal(t, int32(targetHeight), ancestor.Height())
	require.Equal(t, storedHeader.Bits, ancestor.Bits())

	lhc, ok := ancestor.(*lightHeaderCtx)
	require.True(t, ok)
	require.Nil(t, lhc.node, "expected nil skip-list node on store-"+
		"fallback path")

	store.AssertExpectations(t)
}
