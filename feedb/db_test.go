package feedb

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *FeeStore {
	t.Helper()
	db, err := walletdb.Create(
		"bdb", t.TempDir()+"/test.db", true, time.Second*10,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	store, err := New(db)
	require.NoError(t, err)
	return store
}

func makeSample(height uint32) *FeeSample {
	var hash chainhash.Hash
	hash[0] = byte(height)
	hash[1] = byte(height >> 8)
	return &FeeSample{
		Height:      height,
		BlockHash:   hash,
		Timestamp:   int64(height) * 600, // pretend 10-min blocks
		TotalFees:   10_000 + uint64(height),
		TotalWeight: 4_000_000,
	}
}

// TestPutFetchRoundTrip exercises the basic Put/Fetch path and confirms the
// stored fields survive a re-encode.
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
	require.Equal(t, in.Flags, got.Flags)
	require.Equal(t, in.FeeRatePerKW(), got.FeeRatePerKW())
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

// TestTipTracking verifies the tip key advances when newer samples land and
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

// TestPurgeFrom drops the orphaned suffix and recomputes the tip from what
// remains.
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

// TestEncodingUnknownVersion confirms a corrupted record yields a clear
// decoding error rather than silent garbage.
func TestEncodingUnknownVersion(t *testing.T) {
	t.Parallel()
	bad := make([]byte, encodingSize)
	bad[0] = 99 // unknown version
	_, err := decodeSample(1, bad)
	require.Error(t, err)
}

// TestFeeRatePerKWZeroWeight returns zero rather than dividing by zero.
func TestFeeRatePerKWZeroWeight(t *testing.T) {
	t.Parallel()
	s := &FeeSample{TotalFees: 1000, TotalWeight: 0}
	require.Equal(t, uint64(0), s.FeeRatePerKW())
}

// TestNewIdempotent confirms repeatedly calling New on the same DB is a
// no-op after the first call.
func TestNewIdempotent(t *testing.T) {
	t.Parallel()
	db, err := walletdb.Create(
		"bdb", t.TempDir()+"/test.db", true, time.Second*10,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	for i := 0; i < 3; i++ {
		_, err := New(db)
		require.NoError(t, err)
	}
}
