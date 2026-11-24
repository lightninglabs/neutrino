package rescan

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// orphanWithParentGone reorgs the top two blocks of chain out and removes
// both from the store, as a rollback deeper than one block does before its
// disconnects are queued. It returns a cursor on the old best block.
func orphanWithParentGone(t *testing.T, chain *mockChainWithTxs) Cursor {
	t.Helper()

	chain.mu.Lock()
	defer chain.mu.Unlock()

	best := chain.bestBlock
	cur := *chain.blockHeaders[best.Hash]
	reorgAbove(t, chain, best.Height-2, best.Height)
	delete(chain.blockHeaders, best.Hash)
	delete(chain.blockHeaders, cur.PrevBlock)

	return Cursor{
		CurStamp: headerfs.BlockStamp{
			Hash: best.Hash, Height: best.Height,
		},
		CurHeader: cur,
	}
}

// TestParentWaitsCapped checks that both states that step back from the
// cursor block wait a bounded number of steps for the disconnects of a deep
// reorg, without reporting an error, and then end the rescan with a
// HeaderStoreError. An update that doesn't move the cursor must not restart
// the count, or a wallet adding addresses often enough keeps the rescan
// waiting forever.
func TestParentWaitsCapped(t *testing.T) {
	t.Parallel()

	addr, err := address.NewAddressPubKeyHash(
		make([]byte, 20), &chaincfg.MainNetParams,
	)
	require.NoError(t, err)
	add := AddWatchAddrsEvent{Addrs: []address.Address{addr}}

	states := map[string]func(Cursor) RescanState{
		"syncing": func(c Cursor) RescanState {
			return &StateSyncing{Cursor: c}
		},
		"rewinding": func(c Cursor) RescanState {
			return newRewinding(c, rewindPlan{{Target: 1}})
		},
	}

	for name, newState := range states {
		for _, addWatch := range []bool{false, true} {
			if addWatch {
				name += " with updates"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				chain := newMockChainWithTxs(6)
				cursor := orphanWithParentGone(t, chain)
				env := &Environment{Chain: chain}

				state := newState(cursor)
				for i := 0; i < maxParentWaits; i++ {
					tr, err := state.ProcessEvent(
						t.Context(),
						ProcessNextBlockEvent{}, env,
					)
					require.NoErrorf(t, err, "step %d", i)
					require.Equal(t,
						[]OutboxEvent{WaitOutbox{}},
						outboxOf(tr))
					state = tr.NextState

					if !addWatch {
						continue
					}
					tr, err = state.ProcessEvent(
						t.Context(), add, env,
					)
					require.NoError(t, err)
					state = tr.NextState
				}

				_, err := state.ProcessEvent(
					t.Context(), ProcessNextBlockEvent{},
					env,
				)
				var storeErr *HeaderStoreError
				require.ErrorAs(t, err, &storeErr)
			})
		}
	}
}

// TestParentWaitsFollowCursor checks that the wait count belongs to the block
// the cursor is on: a cursor that moves to another block starts counting
// again even if it carries the old count along.
func TestParentWaitsFollowCursor(t *testing.T) {
	t.Parallel()

	chain := newMockChainWithTxs(6)
	cursor := orphanWithParentGone(t, chain)
	env := &Environment{Chain: chain}

	for i := 0; i < maxParentWaits-1; i++ {
		_, _, wait, err := cursor.parent(env)
		require.NoError(t, err)
		require.NotNil(t, wait)
		cursor = *wait
	}
	require.Equal(t, maxParentWaits-1, cursor.waits.n)

	// The same count on another orphaned block whose parent is gone
	// starts over.
	moved := cursor
	moved.CurHeader.Nonce++
	moved.CurStamp.Hash = moved.CurHeader.BlockHash()

	_, _, wait, err := moved.parent(env)
	require.NoError(t, err)
	require.NotNil(t, wait)
	require.Equal(t, 1, wait.waits.n)
}

// notCurrentChain is a dstChain whose IsCurrent answer the test sets, so
// the rewind can be asked for while the rescan is still Initializing.
type notCurrentChain struct {
	*dstChain
	current atomic.Bool
}

func (c *notCurrentChain) IsCurrent() bool { return c.current.Load() }

// TestRewindOnOrphanedStartEnds checks a rescan whose start block was
// reorged out two deep before it subscribed, so no disconnect is coming,
// and whose wallet asks for a rewind before the first scan. The rescan must
// end with a HeaderStoreError rather than retry in Rewinding forever.
func TestRewindOnOrphanedStartEnds(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		chain := &notCurrentChain{dstChain: &dstChain{
			mockChainWithTxs: newMockChainWithTxs(6),
		}}
		start := orphanWithParentGone(t, chain.mockChainWithTxs)

		addr, err := address.NewAddressPubKeyHash(
			make([]byte, 20), &chaincfg.MainNetParams,
		)
		require.NoError(t, err)

		var errs atomic.Int32
		actor := mustNewRescanActor(t, ActorConfig{
			Chain:         chain,
			StartStamp:    start.CurStamp,
			StartHeader:   start.CurHeader,
			WatchAddrs:    []address.Address{addr},
			RetryInterval: 100 * time.Millisecond,
			OnError:       func(error) { errs.Add(1) },
		})
		actor.Start()
		t.Cleanup(func() {
			actor.Stop()
			actor.WaitForShutdown()
		})

		rewindTo := uint32(1)
		require.NoError(t, addWatch(actor, WatchUpdate{
			Addrs:    []address.Address{addr},
			RewindTo: &rewindTo,
		}))
		chain.current.Store(true)

		// The rewind was taken before the first scan, so the rescan
		// waits in Rewinding rather than Syncing.
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		state, err := actor.CurrentState(t.Context())
		require.NoError(t, err)
		require.IsType(t, &StateRewinding{}, state)

		for i := 0; i < 20; i++ {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
		}

		select {
		case <-actor.Done():
		default:
			state, _ := actor.CurrentState(t.Context())
			t.Fatalf("rescan still %v after 20 retry intervals, "+
				"OnError called %d times", state, errs.Load())
		}

		var storeErr *HeaderStoreError
		require.ErrorAs(t, actor.Err(), &storeErr)
	})
}
