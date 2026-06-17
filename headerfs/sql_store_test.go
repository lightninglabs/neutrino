package headerfs

import (
	"context"
	"reflect"
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"

	"github.com/lightninglabs/neutrino/sqldb"
)

// newSQLBlockHeaderStore creates a SQLBlockHeaderStore backed by a fresh
// in-memory SQLite database. It uses the SimNet chain params to match the
// chain context used by the existing kvdb-based test helpers.
func newSQLBlockHeaderStore(t *testing.T) *SQLBlockHeaderStore {
	t.Helper()

	backend := sqldb.NewTestBackend(t)
	store, err := NewSQLBlockHeaderStore(
		context.Background(), backend.HeaderTxer,
		&chaincfg.SimNetParams,
	)
	require.NoError(t, err)
	return store
}

// newSQLFilterHeaderStore creates a SQLFilterHeaderStore against a fresh
// SQLite database.
func newSQLFilterHeaderStore(t *testing.T,
	assertion *FilterHeader) *SQLFilterHeaderStore {

	t.Helper()

	backend := sqldb.NewTestBackend(t)
	store, err := NewSQLFilterHeaderStore(
		context.Background(), backend.HeaderTxer, RegularFilter,
		&chaincfg.SimNetParams, assertion,
	)
	require.NoError(t, err)
	return store
}

// TestSQLBlockHeaderStoreOperations is a SQL analog of
// TestBlockHeaderStoreOperations. It writes a header chain and verifies
// reads, ChainTip, and rollback semantics.
func TestSQLBlockHeaderStoreOperations(t *testing.T) {
	t.Parallel()

	store := newSQLBlockHeaderStore(t)

	const numHeaders = 100
	blockHeaders := createTestBlockHeaderChain(numHeaders)

	require.NoError(t, store.WriteHeaders(blockHeaders...))

	last := blockHeaders[len(blockHeaders)-1]
	tipHeader, tipHeight, err := store.ChainTip()
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(last.BlockHeader, tipHeader))
	require.Equal(t, last.Height, tipHeight)

	for _, hdr := range blockHeaders {
		got, err := store.FetchHeaderByHeight(hdr.Height)
		require.NoError(t, err)
		require.True(t, reflect.DeepEqual(*hdr.BlockHeader, *got),
			"FetchHeaderByHeight mismatch at height %d: %s",
			hdr.Height, spew.Sdump(got))

		blockHash := hdr.BlockHash()
		gotByHash, gotHeight, err := store.FetchHeader(&blockHash)
		require.NoError(t, err)
		require.True(t, reflect.DeepEqual(*hdr.BlockHeader, *gotByHash))
		require.Equal(t, hdr.Height, gotHeight)
	}

	// Rollback by one and verify tip moves to the second-to-last header.
	secondToLast := blockHeaders[len(blockHeaders)-2]
	stamp, err := store.RollbackLastBlock()
	require.NoError(t, err)
	require.Equal(t, int32(secondToLast.Height), stamp.Height)
	stlHash := secondToLast.BlockHash()
	require.Equal(t, stlHash[:], stamp.Hash[:])

	tipHeader, tipHeight, err = store.ChainTip()
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(secondToLast.BlockHeader, tipHeader))
	require.Equal(t, secondToLast.Height, tipHeight)
}

// TestSQLBlockHeaderRollbackAtomic verifies that a failed WriteHeaders batch
// does not leave any rows behind (full transactional rollback).
func TestSQLBlockHeaderRollbackAtomic(t *testing.T) {
	t.Parallel()

	store := newSQLBlockHeaderStore(t)

	chain := createTestBlockHeaderChain(10)
	require.NoError(t, store.WriteHeaders(chain[:5]...))

	// Now construct a batch where the second header collides on height
	// with an already-inserted row. The legacy bdb impl rolls back the
	// full batch on failure. With SQL we get the same atomicity for free
	// from the surrounding transaction.
	collide := chain[3] // height 4, already inserted
	collide.Height = chain[7].Height
	bad := []BlockHeader{chain[5], collide}

	err := store.WriteHeaders(bad...)
	require.Error(t, err)

	// Tip must still be at height 5 (the last successful write), and
	// height 6 must NOT be present (the first insert in the failed batch).
	tip, height, err := store.ChainTip()
	require.NoError(t, err)
	require.Equal(t, chain[4].Height, height)
	require.True(t, reflect.DeepEqual(chain[4].BlockHeader, tip))

	_, err = store.FetchHeaderByHeight(chain[5].Height)
	require.Error(t, err)
}

// TestSQLBlockHeaderReadThroughCache verifies the SQL-backed header store
// caches the decoded header after a height lookup. lnd's missed-block scan
// calls GetBlockHash(height) followed by GetBlockHeader(hash), so this keeps
// the second lookup off the sqlite read path.
func TestSQLBlockHeaderReadThroughCache(t *testing.T) {
	t.Parallel()

	store := newSQLBlockHeaderStore(t)

	chain := createTestBlockHeaderChain(10)
	require.NoError(t, store.WriteHeaders(chain...))
	store.clearBlockHeaderCache()

	target := chain[5]
	targetHash := target.BlockHash()

	_, _, ok := store.cachedBlockHeader(&targetHash)
	require.False(t, ok)

	header, err := store.FetchHeaderByHeight(target.Height)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(*target.BlockHeader, *header))

	next := chain[6]
	nextHash := next.BlockHash()
	nextHeader, ok := store.cachedBlockHeaderByHeight(next.Height)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(*next.BlockHeader, *nextHeader))

	cachedNextHeader, cachedNextHeight, ok := store.cachedBlockHeader(
		&nextHash,
	)
	require.True(t, ok)
	require.Equal(t, next.Height, cachedNextHeight)
	require.True(t, reflect.DeepEqual(*next.BlockHeader, *cachedNextHeader))

	cachedHeader, cachedHeight, ok := store.cachedBlockHeader(&targetHash)
	require.True(t, ok)
	require.Equal(t, target.Height, cachedHeight)
	require.True(t, reflect.DeepEqual(*target.BlockHeader, *cachedHeader))

	header, height, err := store.FetchHeader(&targetHash)
	require.NoError(t, err)
	require.Equal(t, target.Height, height)
	require.True(t, reflect.DeepEqual(*target.BlockHeader, *header))
}

// TestSQLBlockHeaderReadCacheClearedOnRollback verifies rollback never leaves
// stale cached headers available after the SQL rows have been removed.
func TestSQLBlockHeaderReadCacheClearedOnRollback(t *testing.T) {
	t.Parallel()

	store := newSQLBlockHeaderStore(t)

	chain := createTestBlockHeaderChain(10)
	require.NoError(t, store.WriteHeaders(chain...))
	store.clearBlockHeaderCache()

	tip := chain[len(chain)-1]
	tipHash := tip.BlockHash()

	_, err := store.FetchHeaderByHeight(tip.Height)
	require.NoError(t, err)

	_, _, ok := store.cachedBlockHeader(&tipHash)
	require.True(t, ok)

	_, err = store.RollbackLastBlock()
	require.NoError(t, err)

	_, _, ok = store.cachedBlockHeader(&tipHash)
	require.False(t, ok)

	_, _, err = store.FetchHeader(&tipHash)
	require.Error(t, err)
}

// TestSQLFilterHeaderRollbackAtomic verifies that a failed filter header
// batch does not leave any rows behind.
func TestSQLFilterHeaderRollbackAtomic(t *testing.T) {
	t.Parallel()

	store := newSQLFilterHeaderStore(t, nil)

	chain := createTestFilterHeaderChain(10)
	require.NoError(t, store.WriteHeaders(chain[:5]...))

	collide := chain[3] // header hash already inserted at height 4.
	collide.Height = chain[7].Height
	bad := []FilterHeader{chain[5], collide}

	err := store.WriteHeaders(bad...)
	require.Error(t, err)

	tip, height, err := store.ChainTip()
	require.NoError(t, err)
	require.Equal(t, chain[4].Height, height)
	require.Equal(t, chain[4].FilterHash, *tip)

	_, err = store.FetchHeaderByHeight(chain[5].Height)
	require.Error(t, err)
}

// TestSQLBlockHeaderGenesisIdempotent verifies that constructing the SQL
// block header store twice against the same database leaves a single
// genesis row.
func TestSQLBlockHeaderGenesisIdempotent(t *testing.T) {
	t.Parallel()

	backend := sqldb.NewTestBackend(t)

	_, err := NewSQLBlockHeaderStore(
		context.Background(), backend.HeaderTxer,
		&chaincfg.SimNetParams,
	)
	require.NoError(t, err)

	store, err := NewSQLBlockHeaderStore(
		context.Background(), backend.HeaderTxer,
		&chaincfg.SimNetParams,
	)
	require.NoError(t, err)

	tip, height, err := store.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(0), height)
	require.NotNil(t, tip)
	expectedHash := chaincfg.SimNetParams.GenesisHash
	tipHash := tip.BlockHash()
	require.Equal(t, expectedHash[:], tipHash[:])
}

// TestSQLBlockHeaderFetchAncestors verifies the ancestor-range query.
func TestSQLBlockHeaderFetchAncestors(t *testing.T) {
	t.Parallel()

	store := newSQLBlockHeaderStore(t)

	const numHeaders = 50
	chain := createTestBlockHeaderChain(numHeaders)
	require.NoError(t, store.WriteHeaders(chain...))

	stop := chain[40].BlockHash()
	headers, startHeight, err := store.FetchHeaderAncestors(10, &stop)
	require.NoError(t, err)
	require.Len(t, headers, 11) // 10 ancestors + stop
	require.Equal(t, uint32(31), startHeight)
	for i, h := range headers {
		require.True(t, reflect.DeepEqual(*chain[31+i-1].BlockHeader,
			h), "ancestor %d mismatch", i)
	}
}

// TestSQLFilterHeaderStoreOperations is a SQL analog of the existing
// TestFilterHeaderStoreOperations test.
func TestSQLFilterHeaderStoreOperations(t *testing.T) {
	t.Parallel()

	store := newSQLFilterHeaderStore(t, nil)

	const numHeaders = 100
	chain := createTestFilterHeaderChain(numHeaders)
	require.NoError(t, store.WriteHeaders(chain...))

	last := chain[len(chain)-1]
	tipHash, tipHeight, err := store.ChainTip()
	require.NoError(t, err)
	require.Equal(t, last.FilterHash, *tipHash)
	require.Equal(t, last.Height, tipHeight)

	for _, hdr := range chain {
		got, err := store.FetchHeaderByHeight(hdr.Height)
		require.NoError(t, err)
		require.Equal(t, hdr.FilterHash, *got)

		blockHash := hdr.HeaderHash
		gotByHash, err := store.FetchHeader(&blockHash)
		require.NoError(t, err)
		require.Equal(t, hdr.FilterHash, *gotByHash)
	}

	// Roll back one header.
	prev := chain[len(chain)-2]
	prevFilterHash := prev.FilterHash
	stamp, err := store.RollbackLastBlock(&prev.HeaderHash)
	require.NoError(t, err)
	require.Equal(t, int32(prev.Height), stamp.Height)
	require.Equal(t, prevFilterHash[:], stamp.Hash[:])
}

// TestSQLFilterHeaderAssertReset verifies that constructing the store with a
// matching assertion leaves the chain intact, while a mismatched assertion
// purges every row and re-seeds genesis — all transactionally.
func TestSQLFilterHeaderAssertReset(t *testing.T) {
	t.Parallel()

	backend := sqldb.NewTestBackend(t)
	chain := createTestFilterHeaderChain(20)

	store, err := NewSQLFilterHeaderStore(
		context.Background(), backend.HeaderTxer, RegularFilter,
		&chaincfg.SimNetParams, nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.WriteHeaders(chain...))

	// Matching assertion: should be a no-op.
	matching := chain[10]
	store, err = NewSQLFilterHeaderStore(
		context.Background(), backend.HeaderTxer, RegularFilter,
		&chaincfg.SimNetParams, &matching,
	)
	require.NoError(t, err)
	tipHash, tipHeight, err := store.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(20), tipHeight)
	require.Equal(t, chain[19].FilterHash[:], tipHash[:])

	// Mismatched assertion: must purge and re-seed genesis.
	bad := matching
	bad.FilterHash[0] ^= 0xFF
	store, err = NewSQLFilterHeaderStore(
		context.Background(), backend.HeaderTxer, RegularFilter,
		&chaincfg.SimNetParams, &bad,
	)
	require.NoError(t, err)
	tipHash, tipHeight, err = store.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(0), tipHeight)
	require.NotEqual(t, chain[19].FilterHash[:], tipHash[:])

	// All written rows should be gone.
	for _, hdr := range chain {
		_, err := store.FetchHeaderByHeight(hdr.Height)
		if hdr.Height == 0 {
			require.NoError(t, err)
			continue
		}
		require.Error(t, err)
	}
}

// TestSQLFilterHeaderAssertMissingHeight verifies that asserting at a
// not-yet-known height is treated as a no-op (matches legacy behavior in
// maybeResetHeaderState that returns false when the assertion height isn't
// stored).
func TestSQLFilterHeaderAssertMissingHeight(t *testing.T) {
	t.Parallel()

	backend := sqldb.NewTestBackend(t)
	chain := createTestFilterHeaderChain(5)

	store, err := NewSQLFilterHeaderStore(
		context.Background(), backend.HeaderTxer, RegularFilter,
		&chaincfg.SimNetParams, nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.WriteHeaders(chain...))

	bogus := FilterHeader{Height: 999}
	bogus.FilterHash[0] = 0xCD
	bogus.HeaderHash[0] = 0xCD

	store, err = NewSQLFilterHeaderStore(
		context.Background(), backend.HeaderTxer, RegularFilter,
		&chaincfg.SimNetParams, &bogus,
	)
	require.NoError(t, err)
	_, tipHeight, err := store.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(5), tipHeight)
}
