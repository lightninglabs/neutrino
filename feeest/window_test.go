package feeest

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightninglabs/neutrino/feedb"
	"github.com/stretchr/testify/require"
)

// mkSample returns a FeeSample with a unique BlockHash derived from the
// height, so repeated heights with distinct content need mkForkSample.
func mkSample(h uint32) feedb.FeeSample {
	var hash chainhash.Hash
	hash[0] = byte(h)
	hash[1] = byte(h >> 8)
	hash[2] = byte(h >> 16)
	hash[3] = byte(h >> 24)
	return feedb.FeeSample{
		Height:      h,
		BlockHash:   hash,
		Timestamp:   int64(h) * 600,
		TotalFees:   uint64(h * 1000),
		TotalWeight: 4_000_000,
	}
}

// mkForkSample returns a sample at the given height with a hash distinct from
// mkSample's, emulating a competing block at the same height.
func mkForkSample(h uint32) feedb.FeeSample {
	s := mkSample(h)
	s.BlockHash[31] = 0xff
	return s
}

// TestWindowEmpty confirms snapshot on a fresh window returns nil.
func TestWindowEmpty(t *testing.T) {
	t.Parallel()
	w := newSampleWindow(4)
	require.Nil(t, w.snapshot())
	require.Equal(t, 0, w.len())
}

// TestWindowFillBelowCapacity stores fewer entries than the capacity and
// confirms snapshot is height-ordered.
func TestWindowFillBelowCapacity(t *testing.T) {
	t.Parallel()
	w := newSampleWindow(4)
	require.True(t, w.add(mkSample(2)))
	require.True(t, w.add(mkSample(1)))

	got := w.snapshot()
	require.Len(t, got, 2)
	require.Equal(t, uint32(1), got[0].Height)
	require.Equal(t, uint32(2), got[1].Height)
}

// TestWindowEvictsLowestHeight overflows the window and confirms the highest
// N heights survive regardless of insertion order.
func TestWindowEvictsLowestHeight(t *testing.T) {
	t.Parallel()
	w := newSampleWindow(3)
	for _, h := range []uint32{5, 1, 7, 3, 6} {
		w.add(mkSample(h))
	}

	got := w.snapshot()
	require.Len(t, got, 3)
	require.Equal(t, uint32(5), got[0].Height)
	require.Equal(t, uint32(6), got[1].Height)
	require.Equal(t, uint32(7), got[2].Height)
}

// TestWindowRejectsHistoricalWhenFull emulates a rescan pulling old blocks:
// once the window is full, samples at or below the minimum height must be
// rejected instead of evicting recent data.
func TestWindowRejectsHistoricalWhenFull(t *testing.T) {
	t.Parallel()
	w := newSampleWindow(3)
	for h := uint32(100); h < 103; h++ {
		require.True(t, w.add(mkSample(h)))
	}

	// A burst of historical blocks must not displace the recent window.
	for h := uint32(1); h <= 50; h++ {
		require.False(t, w.add(mkSample(h)))
	}

	got := w.snapshot()
	require.Len(t, got, 3)
	require.Equal(t, uint32(100), got[0].Height)
	require.Equal(t, uint32(102), got[2].Height)

	// A newer block still gets in and evicts the oldest.
	require.True(t, w.add(mkSample(103)))
	got = w.snapshot()
	require.Equal(t, uint32(101), got[0].Height)
	require.Equal(t, uint32(103), got[2].Height)
}

// TestWindowPrune removes filtered entries.
func TestWindowPrune(t *testing.T) {
	t.Parallel()
	w := newSampleWindow(5)
	for h := uint32(1); h <= 5; h++ {
		w.add(mkSample(h))
	}

	w.prune(func(s feedb.FeeSample) bool {
		return s.Height >= 3 // drop heights 3, 4, 5
	})

	got := w.snapshot()
	require.Len(t, got, 2)
	require.Equal(t, uint32(1), got[0].Height)
	require.Equal(t, uint32(2), got[1].Height)

	// New writes after a prune should resume correctly.
	w.add(mkSample(10))
	got = w.snapshot()
	require.Len(t, got, 3)
	require.Equal(t, uint32(10), got[2].Height)
}

// TestWindowAddIdempotent verifies that add deduplicates by block hash and
// that the check-and-add is atomic (two calls with the same hash only insert
// one entry).
func TestWindowAddIdempotent(t *testing.T) {
	t.Parallel()
	w := newSampleWindow(5)

	s := mkSample(1)
	require.True(t, w.add(s), "first add should return true")
	require.Equal(t, 1, w.len())

	require.False(t, w.add(s), "second add with same hash should return false")
	require.Equal(t, 1, w.len(), "window size must not grow on duplicate")

	// Different hash should still be added.
	require.True(t, w.add(mkSample(2)))
	require.Equal(t, 2, w.len())
}

// TestWindowSameHeightDistinctHash admits two blocks at the same height with
// different hashes (transient reorg state before pruning).
func TestWindowSameHeightDistinctHash(t *testing.T) {
	t.Parallel()
	w := newSampleWindow(5)
	require.True(t, w.add(mkSample(7)))
	require.True(t, w.add(mkForkSample(7)))
	require.Equal(t, 2, w.len())
}

// TestWindowZeroCapacityCoercedToOne ensures the constructor doesn't panic on
// a non-positive capacity.
func TestWindowZeroCapacityCoercedToOne(t *testing.T) {
	t.Parallel()
	w := newSampleWindow(0)
	require.True(t, w.add(mkSample(1)))
	require.True(t, w.add(mkSample(2)))
	got := w.snapshot()
	require.Len(t, got, 1)
	require.Equal(t, uint32(2), got[0].Height)
}
