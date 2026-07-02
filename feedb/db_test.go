package feedb

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/lightninglabs/neutrino/sqldb"
)

// newTestStore returns a SQLFeeStore backed by a fresh in-process database
// (sqlite by default, postgres under the test_db_postgres build tag).
func newTestStore(t *testing.T) *SQLFeeStore {
	t.Helper()

	backend := sqldb.NewTestBackend(t)
	store, err := NewSQLStore(backend.FeeTxer)
	require.NoError(t, err)
	return store
}

func makeSample(height uint32) *FeeSample {
	var hash chainhash.Hash
	hash[0] = byte(height)
	hash[1] = byte(height >> 8)
	return &FeeSample{
		Height:    height,
		BlockHash: hash,
		// Pretend blocks arrive on a 10-minute cadence.
		Timestamp:      int64(height) * 600,
		TotalFees:      10_000 + uint64(height),
		TotalWeight:    4_000_000,
		CoinbaseWeight: 800,
		MinKnownTxRate: 250 + uint64(height),
		KnownTxCount:   3,
	}
}

// TestPutFetchRoundTrip exercises the basic Put/Fetch path and confirms every
// stored field survives the round trip through the SQL row.
func TestPutFetchRoundTrip(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	in := makeSample(100)
	in.Flags = FlagSpam
	require.NoError(t, store.PutSample(in))

	got, err := store.FetchSample(100)
	require.NoError(t, err)
	require.Equal(t, in.Height, got.Height)
	require.Equal(t, in.BlockHash, got.BlockHash)
	require.Equal(t, in.Timestamp, got.Timestamp)
	require.Equal(t, in.TotalFees, got.TotalFees)
	require.Equal(t, in.TotalWeight, got.TotalWeight)
	require.Equal(t, in.CoinbaseWeight, got.CoinbaseWeight)
	require.Equal(t, in.MinKnownTxRate, got.MinKnownTxRate)
	require.Equal(t, in.KnownTxCount, got.KnownTxCount)
	require.Equal(t, in.Flags, got.Flags)
	require.Equal(t, in.FeeRatePerKW(), got.FeeRatePerKW())
}

// TestPutSampleReplacesHeight confirms the upsert semantics: a second sample
// at the same height (e.g. a competing block observed during a reorg)
// replaces the first row rather than accumulating alongside it.
func TestPutSampleReplacesHeight(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	first := makeSample(100)
	require.NoError(t, store.PutSample(first))

	second := makeSample(100)
	second.BlockHash[31] = 0xff
	second.TotalFees = first.TotalFees * 2
	require.NoError(t, store.PutSample(second))

	got, err := store.FetchSample(100)
	require.NoError(t, err)
	require.Equal(t, second.BlockHash, got.BlockHash)
	require.Equal(t, second.TotalFees, got.TotalFees)

	// Still exactly one row at that height.
	all, err := store.FetchRange(0, 200)
	require.NoError(t, err)
	require.Len(t, all, 1)
}

// TestFetchSampleNotFound confirms the lookup returns ErrSampleNotFound for
// an absent height.
func TestFetchSampleNotFound(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	_, err := store.FetchSample(42)
	require.ErrorIs(t, err, ErrSampleNotFound)
}

// TestFetchTipNOrdering inserts samples out of order and confirms FetchTipN
// returns them newest-first regardless of insertion order.
func TestFetchTipNOrdering(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	for _, h := range []uint32{50, 10, 30, 40, 20} {
		require.NoError(t, store.PutSample(makeSample(h)))
	}

	got, err := store.FetchTipN(3)
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, uint32(50), got[0].Height)
	require.Equal(t, uint32(40), got[1].Height)
	require.Equal(t, uint32(30), got[2].Height)
}

// TestFetchRange confirms inclusive-range scans return samples in ascending
// height order and exclude entries outside the window.
func TestFetchRange(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	for h := uint32(1); h <= 10; h++ {
		require.NoError(t, store.PutSample(makeSample(h)))
	}

	got, err := store.FetchRange(3, 7)
	require.NoError(t, err)
	require.Len(t, got, 5)
	for i, s := range got {
		require.Equal(t, uint32(3+i), s.Height)
	}
}

// TestTipTracking verifies the tip advances when newer samples land and
// stays put when an older backfill is written.
func TestTipTracking(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	tip, err := store.Tip()
	require.NoError(t, err)
	require.Equal(t, uint32(0), tip)

	require.NoError(t, store.PutSample(makeSample(100)))
	tip, err = store.Tip()
	require.NoError(t, err)
	require.Equal(t, uint32(100), tip)

	// Backfill at lower height should not move the tip.
	require.NoError(t, store.PutSample(makeSample(50)))
	tip, err = store.Tip()
	require.NoError(t, err)
	require.Equal(t, uint32(100), tip)

	// Newer sample advances the tip.
	require.NoError(t, store.PutSample(makeSample(200)))
	tip, err = store.Tip()
	require.NoError(t, err)
	require.Equal(t, uint32(200), tip)
}

// TestPurgeBefore deletes the older suffix and leaves the tip alone.
func TestPurgeBefore(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	for h := uint32(1); h <= 10; h++ {
		require.NoError(t, store.PutSample(makeSample(h)))
	}

	require.NoError(t, store.PurgeBefore(6))

	got, err := store.FetchRange(0, 100)
	require.NoError(t, err)
	require.Len(t, got, 5)
	for i, s := range got {
		require.Equal(t, uint32(6+i), s.Height)
	}

	tip, err := store.Tip()
	require.NoError(t, err)
	require.Equal(t, uint32(10), tip)
}

// TestPurgeFrom drops the orphaned suffix; the tip follows the highest
// surviving row.
func TestPurgeFrom(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	for h := uint32(1); h <= 10; h++ {
		require.NoError(t, store.PutSample(makeSample(h)))
	}

	require.NoError(t, store.PurgeFrom(7))

	got, err := store.FetchRange(0, 100)
	require.NoError(t, err)
	require.Len(t, got, 6)
	for i, s := range got {
		require.Equal(t, uint32(1+i), s.Height)
	}

	tip, err := store.Tip()
	require.NoError(t, err)
	require.Equal(t, uint32(6), tip)
}

// TestPurgeFromEmpty drops everything and resets the tip to zero.
func TestPurgeFromEmpty(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	for h := uint32(1); h <= 5; h++ {
		require.NoError(t, store.PutSample(makeSample(h)))
	}

	require.NoError(t, store.PurgeFrom(0))

	got, err := store.FetchRange(0, 100)
	require.NoError(t, err)
	require.Empty(t, got)

	tip, err := store.Tip()
	require.NoError(t, err)
	require.Equal(t, uint32(0), tip)
}

// TestNilExecutor confirms the constructor rejects a nil transaction
// executor.
func TestNilExecutor(t *testing.T) {
	t.Parallel()
	_, err := NewSQLStore(nil)
	require.Error(t, err)
}

// TestFeeRatePerKWZeroWeight returns zero rather than dividing by zero.
func TestFeeRatePerKWZeroWeight(t *testing.T) {
	t.Parallel()
	s := &FeeSample{TotalFees: 1000, TotalWeight: 0}
	require.Equal(t, uint64(0), s.FeeRatePerKW())
}

// TestFeeRatePerKWExcludesCoinbase confirms the block-average rate is
// computed over the fee-paying weight only when the coinbase weight is known.
func TestFeeRatePerKWExcludesCoinbase(t *testing.T) {
	t.Parallel()

	// 1000 sats over 5_000 WU total, 1_000 WU of which is the coinbase:
	// 1000 * 1000 / 4000 = 250 sat/kW.
	s := &FeeSample{
		TotalFees:      1000,
		TotalWeight:    5_000,
		CoinbaseWeight: 1_000,
	}
	require.Equal(t, uint64(250), s.FeeRatePerKW())

	// A sample with no recorded coinbase weight falls back to the full
	// weight.
	s.CoinbaseWeight = 0
	require.Equal(t, uint64(200), s.FeeRatePerKW())

	// Degenerate: coinbase weight >= total weight must not underflow.
	s.CoinbaseWeight = 5_000
	require.Equal(t, uint64(200), s.FeeRatePerKW())
}
