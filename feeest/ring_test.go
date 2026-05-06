package feeest

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightninglabs/neutrino/feedb"
	"github.com/stretchr/testify/require"
)

// mkSample returns a FeeSample with a zero BlockHash. Suitable for tests that
// do not exercise hash-based deduplication.
func mkSample(h uint32) feedb.FeeSample {
	return feedb.FeeSample{
		Height:      h,
		Timestamp:   int64(h) * 600,
		TotalFees:   uint64(h * 1000),
		TotalWeight: 4_000_000,
	}
}

// mkHashedSample returns a FeeSample with a unique BlockHash derived from the
// height. Used by tests that exercise addIfNew deduplication.
func mkHashedSample(h uint32) feedb.FeeSample {
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

// TestRingEmpty confirms snapshot on a fresh ring returns nil.
func TestRingEmpty(t *testing.T) {
	t.Parallel()
	r := newRing(4)
	require.Nil(t, r.snapshot())
	require.Equal(t, 0, r.len())
}

// TestRingFillBelowCapacity stores fewer entries than the capacity and
// confirms snapshot order matches insertion order.
func TestRingFillBelowCapacity(t *testing.T) {
	t.Parallel()
	r := newRing(4)
	r.add(mkSample(1))
	r.add(mkSample(2))

	got := r.snapshot()
	require.Len(t, got, 2)
	require.Equal(t, uint32(1), got[0].Height)
	require.Equal(t, uint32(2), got[1].Height)
}

// TestRingWrapAround overflows the buffer and confirms snapshot returns the
// most-recent N entries in chronological order.
func TestRingWrapAround(t *testing.T) {
	t.Parallel()
	r := newRing(3)
	for h := uint32(1); h <= 7; h++ {
		r.add(mkSample(h))
	}

	got := r.snapshot()
	require.Len(t, got, 3)
	require.Equal(t, uint32(5), got[0].Height)
	require.Equal(t, uint32(6), got[1].Height)
	require.Equal(t, uint32(7), got[2].Height)
}

// TestRingPrune removes filtered entries and re-packs the buffer.
func TestRingPrune(t *testing.T) {
	t.Parallel()
	r := newRing(5)
	for h := uint32(1); h <= 5; h++ {
		r.add(mkSample(h))
	}

	r.prune(func(s feedb.FeeSample) bool {
		return s.Height >= 3 // drop heights 3, 4, 5
	})

	got := r.snapshot()
	require.Len(t, got, 2)
	require.Equal(t, uint32(1), got[0].Height)
	require.Equal(t, uint32(2), got[1].Height)

	// New writes after a prune should resume correctly.
	r.add(mkSample(10))
	got = r.snapshot()
	require.Len(t, got, 3)
	require.Equal(t, uint32(10), got[2].Height)
}

// TestRingPruneAfterWrap confirms prune handles a buffer that has already
// wrapped around.
func TestRingPruneAfterWrap(t *testing.T) {
	t.Parallel()
	r := newRing(3)
	for h := uint32(1); h <= 5; h++ {
		r.add(mkSample(h)) // ring now contains 3, 4, 5 with wrap
	}

	r.prune(func(s feedb.FeeSample) bool {
		return s.Height == 4
	})

	got := r.snapshot()
	require.Len(t, got, 2)
	require.Equal(t, uint32(3), got[0].Height)
	require.Equal(t, uint32(5), got[1].Height)
}

// TestRingAddIfNewIdempotent verifies that addIfNew deduplicates by block hash
// and that the check-and-add is atomic (two calls with the same hash only
// insert one entry).
func TestRingAddIfNewIdempotent(t *testing.T) {
	t.Parallel()
	r := newRing(5)

	s := mkHashedSample(1)
	require.True(t, r.addIfNew(s), "first add should return true")
	require.Equal(t, 1, r.len())

	require.False(t, r.addIfNew(s), "second add with same hash should return false")
	require.Equal(t, 1, r.len(), "ring size must not grow on duplicate")

	// Different hash should still be added.
	require.True(t, r.addIfNew(mkHashedSample(2)))
	require.Equal(t, 2, r.len())
}

// TestRingAddIfNewAfterWrap confirms dedup still works when the ring has
// wrapped around and the duplicate is not the most-recently-written entry.
func TestRingAddIfNewAfterWrap(t *testing.T) {
	t.Parallel()
	r := newRing(3)
	for h := uint32(1); h <= 4; h++ { // wraps: ring holds 2,3,4
		r.add(mkHashedSample(h))
	}
	// height 3 is still in the ring even after the wrap.
	require.False(t, r.addIfNew(mkHashedSample(3)), "duplicate in wrapped ring")
	require.Equal(t, 3, r.len())
}

// TestRingZeroCapacityCoercedToOne ensures the constructor doesn't panic on
// a non-positive capacity.
func TestRingZeroCapacityCoercedToOne(t *testing.T) {
	t.Parallel()
	r := newRing(0)
	r.add(mkSample(1))
	r.add(mkSample(2))
	got := r.snapshot()
	require.Len(t, got, 1)
	require.Equal(t, uint32(2), got[0].Height)
}
