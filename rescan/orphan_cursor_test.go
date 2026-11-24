package rescan

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// reorgAbove replaces every block above fork, up to tipHeight, with a
// competing block at the same height, and makes the new branch's tip the best
// block. The replaced blocks stay readable by hash, but are no longer the
// active block at their height. chain.mu must be held. It returns the new
// tip's hash.
func reorgAbove(t *testing.T, chain *mockChainWithTxs, fork,
	tipHeight int32) chainhash.Hash {

	t.Helper()

	genesisTime := chaincfg.MainNetParams.GenesisBlock.Header.Timestamp
	forkHash := *chain.blockHashesByHeight[uint32(fork)]
	tip := headerfs.BlockStamp{
		Hash:      forkHash,
		Height:    fork,
		Timestamp: chain.blockHeaders[forkHash].Timestamp,
	}

	for height := fork + 1; height <= tipHeight; height++ {
		filter, err := gcs.FromBytes(
			0, builder.DefaultP, builder.DefaultM, nil,
		)
		require.NoError(t, err)
		filterHeader, err := builder.MakeHeaderForFilter(
			filter, *chain.filterHeadersByHeight[uint32(height-1)],
		)
		require.NoError(t, err)

		// A different nonce gives a different block at the same
		// height.
		header := &wire.BlockHeader{
			PrevBlock: tip.Hash,
			Timestamp: genesisTime.Add(
				time.Duration(height) * 10 * time.Minute,
			),
			Nonce: uint32(height) + 1000,
		}
		hash := header.BlockHash()

		chain.blockHeaders[hash] = header
		chain.blockHeightIndex[hash] = uint32(height)
		chain.blockHashesByHeight[uint32(height)] = &hash
		chain.blocks[hash] = btcutil.NewBlock(wire.NewMsgBlock(header))
		chain.filters[hash] = filter
		chain.filterHeadersByHeight[uint32(height)] = &filterHeader

		tip = headerfs.BlockStamp{
			Hash:      hash,
			Height:    height,
			Timestamp: header.Timestamp,
		}
	}
	chain.bestBlock = tip

	return tip.Hash
}

// TestRescanStepsOffOrphanedStartBlock checks that a rescan whose start block
// was reorged out before it subscribed catches up to the new tip, and reports
// each orphaned block as disconnected. No disconnect notification reaches the
// rescan for those blocks, so it has to notice the orphan itself rather than
// retry forever waiting for one.
func TestRescanStepsOffOrphanedStartBlock(t *testing.T) {
	t.Parallel()

	// A rollback removes orphaned headers from the store, so a start
	// block orphaned before the subscription can only be stepped off when
	// its parent is still active, a reorg one block deep. Deeper ones end
	// the rescan, see TestOrphanWithMissingParentEndsRescan.
	const depth = int32(1)
	synctest.Test(t, func(t *testing.T) {
		chain := &dstChain{
			mockChainWithTxs: newMockChainWithTxs(4),
		}
		best, err := chain.BestBlock()
		require.NoError(t, err)

		chain.mu.Lock()
		startHeader := *chain.blockHeaders[best.Hash]
		newTip := reorgAbove(
			t, chain.mockChainWithTxs, best.Height-depth,
			best.Height,
		)
		chain.mu.Unlock()

		addr, err := address.NewAddressPubKeyHash(
			make([]byte, 20), &chaincfg.MainNetParams,
		)
		require.NoError(t, err)

		var connects, disconnects atomic.Int32
		actor := mustNewRescanActor(t, ActorConfig{
			Chain:         chain,
			StartStamp:    *best,
			StartHeader:   startHeader,
			WatchAddrs:    []address.Address{addr},
			RetryInterval: 100 * time.Millisecond,
			Callbacks: Callbacks{
				OnBlockConnected: func(*chainhash.Hash,
					int32, time.Time) {

					connects.Add(1)
				},
				OnBlockDisconnected: func(
					*chainhash.Hash, int32,
					time.Time) {

					disconnects.Add(1)
				},
			},
		})
		actor.Start()
		t.Cleanup(func() {
			actor.Stop()
			actor.WaitForShutdown()
		})

		// A handful of retry intervals is more than enough
		// for a rescan that notices the orphan.
		for i := 0; i < 5; i++ {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
		}

		state, err := actor.CurrentState(t.Context())
		require.NoError(t, err)
		current, ok := state.(*StateCurrent)
		require.True(t, ok, "rescan is %v, not Current", state)
		require.Equal(t, newTip, current.CurStamp.Hash)
		require.Equal(t, depth, disconnects.Load())
		require.Equal(t, depth, connects.Load())
	})
}

// TestInitializingFollowsStartBlockDisconnect checks that a disconnect of the
// start block that arrives before the first scan moves the start back to its
// parent, and is reported when the first scan starts instead of being
// dropped.
func TestInitializingFollowsStartBlockDisconnect(t *testing.T) {
	t.Parallel()

	chain := newMockChainWithTxs(3)
	best, err := chain.BestBlock()
	require.NoError(t, err)
	curHeader := *chain.blockHeaders[best.Hash]
	prevHeader := *chain.blockHeaders[curHeader.PrevBlock]

	state := &StateInitializing{Cursor: Cursor{
		CurStamp:  *best,
		CurHeader: curHeader,
	}}
	tr, err := state.ProcessEvent(t.Context(), BlockDisconnectedEvent{
		Header:   curHeader,
		Height:   uint32(best.Height),
		ChainTip: prevHeader,
	}, &Environment{Chain: chain})
	require.NoError(t, err)

	next, ok := tr.NextState.(*StateInitializing)
	require.True(t, ok)
	require.Equal(t, best.Height-1, next.CurStamp.Height)
	require.Equal(t, curHeader.PrevBlock, next.CurStamp.Hash)

	// Nothing is reported before the chain is ready.
	require.Empty(t, outboxOf(tr))

	// The first scan reports the disconnect, then continues.
	tr, err = next.ProcessEvent(
		t.Context(), ProcessNextBlockEvent{},
		&Environment{Chain: chain},
	)
	require.NoError(t, err)
	require.IsType(t, &StateSyncing{}, tr.NextState)

	outbox := outboxOf(tr)
	require.Len(t, outbox, 2)
	undone, ok := outbox[0].(BlockDisconnectedOutbox)
	require.True(t, ok)
	require.Equal(t, best.Hash, undone.Hash)
	require.IsType(t, ContinueOutbox{}, outbox[1])
}

// TestOrphanWithMissingParentEndsRescan checks that an orphaned cursor whose
// parent can't be read ends the rescan with a header store error, rather than
// being retried forever.
func TestOrphanWithMissingParentEndsRescan(t *testing.T) {
	t.Parallel()

	chain := newMockChainWithTxs(3)
	chain.mu.Lock()
	best := chain.bestBlock
	header := *chain.blockHeaders[best.Hash]
	reorgAbove(t, chain, best.Height-1, best.Height)

	// The orphan's parent is gone from the store, as it is after a
	// rollback deeper than one block.
	orphan := header
	orphan.PrevBlock = chainhash.Hash{0xff}
	chain.mu.Unlock()

	state := &StateSyncing{Cursor: Cursor{
		CurStamp: headerfs.BlockStamp{
			Hash:   orphan.BlockHash(),
			Height: best.Height,
		},
		CurHeader: orphan,
	}}
	env := &Environment{Chain: chain}

	// The first steps wait, as the disconnects of a deep reorg can still
	// be on their way, and a retry armed for an earlier cursor can fire
	// early.
	var next RescanState = state
	for i := 0; i < maxParentWaits; i++ {
		tr, err := next.ProcessEvent(
			t.Context(), ProcessNextBlockEvent{}, env,
		)
		require.NoError(t, err)
		require.Equal(t, []OutboxEvent{WaitOutbox{}}, outboxOf(tr))
		waiting, ok := tr.NextState.(*StateSyncing)
		require.True(t, ok)
		require.Equal(t, state.Cursor, waiting.Cursor)
		next = waiting
	}

	// A disconnect in the meantime would have moved the cursor. With none,
	// the next step ends the rescan.
	_, err := next.ProcessEvent(t.Context(), ProcessNextBlockEvent{}, env)

	var storeErr *HeaderStoreError
	require.ErrorAs(t, err, &storeErr)
}

// TestDeepReorgDisconnectsArriveAfterWait checks the order a deep reorg runs
// in: the rollback removes the orphaned headers first, so a step can find the
// cursor orphaned with its parent gone, and the disconnects arrive after. The
// rescan must wait for them and follow them, not end.
func TestDeepReorgDisconnectsArriveAfterWait(t *testing.T) {
	t.Parallel()

	chain := newMockChainWithTxs(5)
	chain.mu.Lock()
	best := chain.bestBlock
	cur := *chain.blockHeaders[best.Hash]
	mid := *chain.blockHeaders[cur.PrevBlock]
	fork := *chain.blockHeaders[mid.PrevBlock]
	reorgAbove(t, chain, best.Height-2, best.Height)

	// The rollback removed both orphans from the store.
	delete(chain.blockHeaders, best.Hash)
	delete(chain.blockHeaders, cur.PrevBlock)
	chain.mu.Unlock()

	state := &StateSyncing{Cursor: Cursor{
		CurStamp: headerfs.BlockStamp{
			Hash: best.Hash, Height: best.Height,
		},
		CurHeader: cur,
	}}
	env := &Environment{Chain: chain}

	tr, err := state.ProcessEvent(t.Context(), ProcessNextBlockEvent{}, env)
	require.NoError(t, err)
	require.Equal(t, []OutboxEvent{WaitOutbox{}}, outboxOf(tr))

	// The disconnects arrive, from the top down.
	tr, err = tr.NextState.ProcessEvent(
		t.Context(), BlockDisconnectedEvent{
			Header:   cur,
			Height:   uint32(best.Height),
			ChainTip: mid,
		}, env,
	)
	require.NoError(t, err)
	tr, err = tr.NextState.ProcessEvent(
		t.Context(), BlockDisconnectedEvent{
			Header:   mid,
			Height:   uint32(best.Height - 1),
			ChainTip: fork,
		}, env,
	)
	require.NoError(t, err)

	// The cursor is on the fork point, and the next step scans the new
	// branch.
	next, ok := tr.NextState.(*StateSyncing)
	require.True(t, ok)
	require.Equal(t, fork.BlockHash(), next.CurStamp.Hash)
	_, err = next.ProcessEvent(t.Context(), ProcessNextBlockEvent{}, env)
	require.NoError(t, err)
}
