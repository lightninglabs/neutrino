package feeest

import (
	"sync"

	"github.com/lightninglabs/neutrino/feedb"
)

// ring is a fixed-capacity in-memory ring buffer of fee samples. Writes are
// O(1) and overwrite the oldest entry once full. Reads return a stable copy
// so the estimator can iterate without holding the lock.
//
// The ring is the hot path for the estimator; the underlying feedb store is
// the durable history (and warm-load source on startup).
type ring struct {
	mu   sync.RWMutex
	data []feedb.FeeSample
	head int // next write index
	size int // number of valid entries (<= cap)
	cap  int
}

// newRing allocates a ring with the given capacity. capacity must be > 0.
func newRing(capacity int) *ring {
	if capacity < 1 {
		capacity = 1
	}
	return &ring{
		data: make([]feedb.FeeSample, capacity),
		cap:  capacity,
	}
}

// add inserts a sample, evicting the oldest entry once the buffer is full.
func (r *ring) add(s feedb.FeeSample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[r.head] = s
	r.head = (r.head + 1) % r.cap
	if r.size < r.cap {
		r.size++
	}
}

// addIfNew inserts the sample only if no existing entry has the same block
// hash. It returns true if the sample was inserted, false if a duplicate was
// found and the call was a no-op.
//
// The check-and-add is atomic under the ring's mutex, which prevents two
// concurrent Observe calls for the same block from both inserting a copy.
// The scan is O(r.size) but the ring is small (<=144 entries by default).
func (r *ring) addIfNew(s feedb.FeeSample) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.size; i++ {
		idx := (r.head - r.size + i + r.cap) % r.cap
		if r.data[idx].BlockHash == s.BlockHash {
			return false
		}
	}
	r.data[r.head] = s
	r.head = (r.head + 1) % r.cap
	if r.size < r.cap {
		r.size++
	}
	return true
}

// snapshot returns a copy of the current contents in chronological order
// (oldest first). Empty ring returns nil.
func (r *ring) snapshot() []feedb.FeeSample {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.size == 0 {
		return nil
	}

	out := make([]feedb.FeeSample, r.size)
	if r.size < r.cap {
		copy(out, r.data[:r.size])
		return out
	}

	// Buffer is full: oldest entry is at r.head, then wraps.
	tail := r.cap - r.head
	copy(out[:tail], r.data[r.head:])
	copy(out[tail:], r.data[:r.head])
	return out
}

// len returns the number of valid entries.
func (r *ring) len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.size
}

// prune removes any sample for which pred returns true and re-packs the
// remaining entries. Used for reorg handling.
func (r *ring) prune(pred func(feedb.FeeSample) bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size == 0 {
		return
	}

	// Linearise.
	flat := make([]feedb.FeeSample, 0, r.size)
	if r.size < r.cap {
		flat = append(flat, r.data[:r.size]...)
	} else {
		flat = append(flat, r.data[r.head:]...)
		flat = append(flat, r.data[:r.head]...)
	}

	// Filter.
	kept := flat[:0]
	for _, s := range flat {
		if !pred(s) {
			kept = append(kept, s)
		}
	}

	// Refill.
	r.head = 0
	r.size = 0
	for _, s := range kept {
		r.data[r.head] = s
		r.head = (r.head + 1) % r.cap
		if r.size < r.cap {
			r.size++
		}
	}
}
