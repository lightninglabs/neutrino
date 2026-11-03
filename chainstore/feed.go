package chainstore

import (
	"sync"
	"time"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// Tips is an immutable snapshot of the block header tip and the filter header
// tip that the chain store has made visible.
type Tips struct {
	// BlockHeight is the height of the visible block header tip.
	BlockHeight uint32

	// BlockHash is the hash of the visible block header tip.
	BlockHash chainhash.Hash

	// BlockTime is the timestamp of the visible block header tip.
	BlockTime time.Time

	// FilterHeight is the height of the visible filter header tip.
	FilterHeight uint32

	// FilterHash is the hash of the block at FilterHeight.
	FilterHash chainhash.Hash
}

// BlockStamp returns the visible block header tip as a block stamp.
func (t Tips) BlockStamp() headerfs.BlockStamp {
	return headerfs.BlockStamp{
		Height:    int32(t.BlockHeight),
		Hash:      t.BlockHash,
		Timestamp: t.BlockTime,
	}
}

// TipFeed holds the tips that the chain store has made visible, and tells
// waiters when they change. The chain store publishes to it after each write
// becomes durable, and the handlers read snapshots or wait for a condition on
// them. A snapshot never waits on a store write, because the feed's lock is
// held only while a value is copied.
//
// The zero value is ready to use and holds the zero tips.
type TipFeed struct {
	// mtx guards the fields below.
	mtx sync.Mutex

	// tips is the current snapshot.
	tips Tips

	// changed is closed and replaced on every update. It is nil until the
	// first call to Changed.
	changed chan struct{}
}

// Snapshot returns the current tips.
func (f *TipFeed) Snapshot() Tips {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	return f.tips
}

// Changed returns a channel that is closed on the next update. A caller that
// needs to react to every update should take a snapshot after it obtains the
// channel, so no update can fall between the two.
func (f *TipFeed) Changed() <-chan struct{} {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	return f.changedLocked()
}

// changedLocked returns the current change channel, creating it if needed.
// The caller must hold mtx.
func (f *TipFeed) changedLocked() chan struct{} {
	if f.changed == nil {
		f.changed = make(chan struct{})
	}

	return f.changed
}

// updateLocked wakes everything waiting on the current change channel. The
// caller must hold mtx.
func (f *TipFeed) updateLocked() {
	close(f.changedLocked())
	f.changed = make(chan struct{})
}

// SetBlockTip makes the block at height the visible block header tip.
func (f *TipFeed) SetBlockTip(height uint32, hash chainhash.Hash,
	timestamp time.Time) {

	f.mtx.Lock()
	defer f.mtx.Unlock()

	f.tips.BlockHeight = height
	f.tips.BlockHash = hash
	f.tips.BlockTime = timestamp
	f.updateLocked()
}

// SetFilterTip makes the block at height the visible filter header tip.
func (f *TipFeed) SetFilterTip(height uint32, hash chainhash.Hash) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	f.tips.FilterHeight = height
	f.tips.FilterHash = hash
	f.updateLocked()
}

// RewindFilterTip lowers the visible filter header tip to height, and reports
// whether it did. A height at or above the visible tip leaves the tip alone,
// because filter headers that were durable but never published must stay
// unpublished.
func (f *TipFeed) RewindFilterTip(height uint32, hash chainhash.Hash) bool {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	if height >= f.tips.FilterHeight {
		return false
	}

	f.tips.FilterHeight = height
	f.tips.FilterHash = hash
	f.updateLocked()

	return true
}

// Wait blocks until ready returns true for the current tips, and returns
// those tips. It returns false, with the last snapshot, if quit closes first.
// ready is checked once before any wait, so a condition that already holds
// returns immediately, even after quit has closed.
func (f *TipFeed) Wait(quit <-chan struct{},
	ready func(Tips) bool) (Tips, bool) {

	for {
		// We take the channel before the snapshot, so an update that
		// lands between the two still wakes us.
		changed := f.Changed()

		tips := f.Snapshot()
		if ready(tips) {
			return tips, true
		}

		select {
		case <-changed:
		case <-quit:
			return tips, false
		}
	}
}
