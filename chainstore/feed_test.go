package chainstore

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/stretchr/testify/require"
)

// TestTipFeedSnapshot checks that the setters change only the tip they name.
func TestTipFeedSnapshot(t *testing.T) {
	var feed TipFeed

	blockHash := chainhash.Hash{1}
	filterHash := chainhash.Hash{2}
	stamp := time.Unix(1000, 0)

	feed.SetBlockTip(10, blockHash, stamp)
	feed.SetFilterTip(4, filterHash)

	require.Equal(t, Tips{
		BlockHeight:  10,
		BlockHash:    blockHash,
		BlockTime:    stamp,
		FilterHeight: 4,
		FilterHash:   filterHash,
	}, feed.Snapshot())

	block := feed.Snapshot().BlockStamp()
	require.Equal(t, int32(10), block.Height)
	require.Equal(t, blockHash, block.Hash)
	require.Equal(t, stamp, block.Timestamp)
}

// TestTipFeedRewindFilterTip checks that a rewind lowers the filter tip, and
// never raises it.
func TestTipFeedRewindFilterTip(t *testing.T) {
	var feed TipFeed

	high := chainhash.Hash{9}
	low := chainhash.Hash{3}
	feed.SetFilterTip(8, high)

	// A rewind above or at the tip leaves it alone and wakes no one.
	changed := feed.Changed()
	require.False(t, feed.RewindFilterTip(8, low))
	require.False(t, feed.RewindFilterTip(12, low))
	require.Equal(t, uint32(8), feed.Snapshot().FilterHeight)
	require.Equal(t, high, feed.Snapshot().FilterHash)
	select {
	case <-changed:
		t.Fatal("a rewind that did not apply woke a waiter")
	default:
	}

	// A rewind below the tip lowers it and closes the channel.
	require.True(t, feed.RewindFilterTip(5, low))
	require.Equal(t, uint32(5), feed.Snapshot().FilterHeight)
	require.Equal(t, low, feed.Snapshot().FilterHash)
	select {
	case <-changed:
	default:
		t.Fatal("a rewind that applied did not wake a waiter")
	}
}

// TestTipFeedWait checks that Wait returns immediately for a condition that
// holds, wakes on a change, and gives up when quit closes.
func TestTipFeedWait(t *testing.T) {
	var feed TipFeed
	quit := make(chan struct{})

	atHeight := func(height uint32) func(Tips) bool {
		return func(tips Tips) bool {
			return tips.BlockHeight >= height
		}
	}

	// A condition that already holds returns without waiting, even after
	// quit has closed.
	feed.SetBlockTip(3, chainhash.Hash{3}, time.Time{})
	tips, ok := feed.Wait(quit, atHeight(3))
	require.True(t, ok)
	require.Equal(t, uint32(3), tips.BlockHeight)

	// A later change wakes the waiter.
	type result struct {
		tips Tips
		ok   bool
	}
	done := make(chan result)
	go func() {
		tips, ok := feed.Wait(quit, atHeight(5))
		done <- result{tips, ok}
	}()

	// An update that leaves the condition false must not release it.
	feed.SetBlockTip(4, chainhash.Hash{4}, time.Time{})
	select {
	case <-done:
		t.Fatal("Wait returned before its condition held")
	case <-time.After(50 * time.Millisecond):
	}

	feed.SetBlockTip(5, chainhash.Hash{5}, time.Time{})
	select {
	case res := <-done:
		require.True(t, res.ok)
		require.Equal(t, uint32(5), res.tips.BlockHeight)
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not wake on a change")
	}

	// A waiter whose condition never holds returns false on quit.
	go func() {
		tips, ok := feed.Wait(quit, atHeight(100))
		done <- result{tips, ok}
	}()
	close(quit)
	select {
	case res := <-done:
		require.False(t, res.ok)
		require.Equal(t, uint32(5), res.tips.BlockHeight)
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return on quit")
	}
}
