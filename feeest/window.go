package feeest

import (
	"sort"
	"sync"

	"github.com/lightninglabs/neutrino/feedb"
)

// sampleWindow is a fixed-capacity, height-ordered window of fee samples.
// Samples are kept sorted by block height and, once the window is full, the
// lowest-height entry is evicted to make room for a higher one. Samples at or
// below the current minimum height are rejected outright when the window is
// full.
//
// Height ordering (rather than insertion ordering) is load-bearing: blocks
// arrive at the sampler in whatever order the rest of the chain service
// happens to fetch them. A rescan can pull thousands of historical blocks in
// a burst, and with insertion-order eviction those would flush every recent
// sample out of the window and leave the estimator looking "stale" even
// though it had fresh tip data moments earlier. Ordering by height makes the
// window always represent the most recent slice of the chain we have
// observed, no matter the fetch order.
//
// The window is the hot path for the estimator; the underlying feedb store is
// the durable history (and warm-load source on startup).
type sampleWindow struct {
	mu   sync.RWMutex
	data []feedb.FeeSample // sorted ascending by height
	cap  int
}

// newSampleWindow allocates a window with the given capacity. Capacities
// below 1 are bumped to 1.
func newSampleWindow(capacity int) *sampleWindow {
	if capacity < 1 {
		capacity = 1
	}
	return &sampleWindow{
		data: make([]feedb.FeeSample, 0, capacity),
		cap:  capacity,
	}
}

// add inserts a sample in height order. It returns false without modifying
// the window when a sample with the same block hash is already present, or
// when the window is full and the sample's height does not exceed the current
// minimum (i.e. it is older than everything we already hold).
func (w *sampleWindow) add(s feedb.FeeSample) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := range w.data {
		if w.data[i].BlockHash == s.BlockHash {
			return false
		}
	}

	if len(w.data) == w.cap {
		// Full: only accept samples strictly newer than the oldest
		// entry, then evict that entry.
		if s.Height <= w.data[0].Height {
			return false
		}
		copy(w.data, w.data[1:])
		w.data = w.data[:len(w.data)-1]
	}

	// Insert at the sorted position. Samples almost always arrive in
	// increasing height order, so this is typically an append.
	idx := sort.Search(len(w.data), func(i int) bool {
		return w.data[i].Height > s.Height
	})
	w.data = append(w.data, feedb.FeeSample{})
	copy(w.data[idx+1:], w.data[idx:])
	w.data[idx] = s
	return true
}

// snapshot returns a copy of the current contents in ascending height order.
// An empty window returns nil.
func (w *sampleWindow) snapshot() []feedb.FeeSample {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if len(w.data) == 0 {
		return nil
	}
	out := make([]feedb.FeeSample, len(w.data))
	copy(out, w.data)
	return out
}

// len returns the number of valid entries.
func (w *sampleWindow) len() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.data)
}

// prune removes any sample for which pred returns true. Used for reorg
// handling.
func (w *sampleWindow) prune(pred func(feedb.FeeSample) bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	kept := w.data[:0]
	for _, s := range w.data {
		if !pred(s) {
			kept = append(kept, s)
		}
	}
	w.data = kept
}
