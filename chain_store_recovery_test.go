package neutrino

import (
	"fmt"
	"math/big"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// winningReorgWork beats any chain a test builds, for tests about what happens
// after a reorg wins.
var winningReorgWork = new(big.Int).Lsh(big.NewInt(1), 256)

// newChainStoreBackend returns the production chain store backend bound to
// the given block manager, so tests exercise the same request path that the
// block manager's handlers use.
func newChainStoreBackend(bm *blockManager) *chainstore.Store {
	return chainstore.NewStore(&chainStoreHost{manager: bm})
}

// blockStoreFault wraps a block header store and injects the ambiguous
// outcomes that a SQL backend can produce: a write or rollback that is applied
// but reports an error, a rollback that fails before changing anything, and a
// tip read that fails right after a rollback. Each fault is one-shot unless a
// count is given. The chain store must recover from all of them from durable
// state alone, because a lost acknowledgement hides whether the mutation
// landed.
type blockStoreFault struct {
	headerfs.BlockHeaderStore
	failWrite, failRollback bool
	failRollbackBefore      bool
	failRollbackCount       int
	failReadAfterRollback   bool
	failNextRead            bool
}

// WriteHeaders applies the batch and then reports a lost acknowledgement when
// failWrite is armed.
func (s *blockStoreFault) WriteHeaders(h ...headerfs.BlockHeader) error {
	if err := s.BlockHeaderStore.WriteHeaders(h...); err != nil {
		return err
	}
	if s.failWrite {
		s.failWrite = false
		return errDSTWrite
	}
	return nil
}

// RollbackLastBlock either fails before touching the store, or applies the
// rollback and reports a lost acknowledgement, depending on which fault is
// armed.
func (s *blockStoreFault) RollbackLastBlock() (*headerfs.BlockStamp, error) {
	if s.failRollbackBefore {
		s.failRollbackBefore = false
		return nil, errDSTWrite
	}
	tip, err := s.BlockHeaderStore.RollbackLastBlock()
	if err == nil && (s.failRollback || s.failRollbackCount > 0) {
		if s.failRollbackCount > 0 {
			s.failRollbackCount--
		}
		s.failRollback = false
		s.failNextRead = s.failReadAfterRollback
		return nil, errDSTWrite
	}
	return tip, err
}

// ChainTip fails once when a read failure was armed by a lost rollback
// acknowledgement, which forces the recovery path to retry its readback.
func (s *blockStoreFault) ChainTip() (*wire.BlockHeader, uint32, error) {
	if s.failNextRead {
		s.failNextRead = false
		return nil, 0, errCFHeaderRead
	}
	return s.BlockHeaderStore.ChainTip()
}

// chainStoreFixture returns a block manager over in-memory stores that hold n
// honest block headers and fn filter headers, together with the stores and a
// channel that collects every block notification the manager emits. Tests
// count and inspect the collected events to check what the chain store
// published.
func chainStoreFixture(
	t *testing.T,
	n, fn int,
) (
	*blockManager, headerfs.BlockHeaderStore,
	headerfs.FilterHeaderStore, chan blockntfns.BlockNtfn) {

	t.Helper()
	p := &chaincfg.SimNetParams
	blocks := newMemBlockHeaderStore(p)
	filters, err := newMemFilterHeaderStore(p)
	require.NoError(t, err)
	fixture := dstChains()
	for i := 1; i <= n; i++ {
		require.NoError(
			t,
			blocks.WriteHeaders(
				headerfs.BlockHeader{
					BlockHeader: fixture.honest[i],
					Height:      uint32(i),
				},
			),
		)
	}
	prev, _, err := filters.ChainTip()
	require.NoError(t, err)
	for i := 1; i <= fn; i++ {
		f := chainhash.DoubleHashH([]byte{byte(i)})
		next := chainhash.DoubleHashH(append(f[:], prev[:]...))
		require.NoError(
			t,
			filters.WriteHeaders(
				headerfs.FilterHeader{
					HeaderHash: fixture.honest[i].
						BlockHash(),
					Height:     uint32(i),
					FilterHash: next,
				},
			),
		)
		prev = &next
	}
	bm := newCFHeaderTestManager(t, blocks, filters)
	events := make(chan blockntfns.BlockNtfn, 16)
	go func() {
		for {
			select {
			case n := <-bm.blockNtfnChan:
				events <- n
			case <-bm.quit:
				return
			}
		}
	}()
	return bm, blocks, filters, events
}

// filterCommitMessage builds a cfheaders message that extends the filter
// store's current tip with the filter hashes for blocks start through end.
func filterCommitMessage(
	t *testing.T,
	s headerfs.FilterHeaderStore,
	start, end int,
) *wire.MsgCFHeaders {

	t.Helper()
	prev, _, err := s.ChainTip()
	require.NoError(t, err)
	fixture := dstChains()
	m := &wire.MsgCFHeaders{
		StopHash:         fixture.honest[end].BlockHash(),
		PrevFilterHeader: *prev,
	}
	for i := start; i <= end; i++ {
		hash := chainhash.DoubleHashH([]byte{byte(i)})
		m.FilterHashes = append(m.FilterHashes, &hash)
	}
	return m
}

// TestFilterSuffixRecovers checks that a filter commit whose write was
// applied but acknowledged with an error is reconciled before the next commit
// extends it. Without the reconciliation, the suffix would be written on top
// of a prefix that the manager never published, and the notification stream
// would skip blocks.
func TestFilterSuffixRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, _, filters, events := chainStoreFixture(t, 5, 0)
		fault := &cfHeaderLostAckStore{
			FilterHeaderStore: filters,
			failWrite:         true,
		}
		bm.cfg.RegFilterHeaders = fault
		backend := newChainStoreBackend(bm)
		_, err := backend.CommitFilters(
			chainstore.FilterCommit{
				Messages: []*wire.MsgCFHeaders{
					filterCommitMessage(t, filters, 1, 3),
				},
			},
		)
		require.ErrorIs(t, err, errDSTWrite)
		require.ErrorIs(t, err, errCFHeaderRead)
		_, err = backend.CommitFilters(
			chainstore.FilterCommit{
				Messages: []*wire.MsgCFHeaders{
					filterCommitMessage(t, filters, 4, 5),
				},
			},
		)
		require.NoError(t, err)
		synctest.Wait()
		require.Len(
			t,
			events,
			5,
			"backend must publish the committed prefix before "+
				"advancing with a suffix",
		)
	})
}

// TestRollbackAppliedErrorRetainsDisconnect checks that a rollback step that
// was applied but reported an error still yields its disconnect notification
// once recovery completes. Losing it would leave subscribers believing that
// the disconnected block is still part of the chain.
func TestRollbackAppliedErrorRetainsDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := chainStoreFixture(t, 3, 3)
		bm.cfg.BlockHeaders = &blockStoreFault{
			BlockHeaderStore: blocks,
			failRollback:     true,
		}
		backend := newChainStoreBackend(bm)
		request := chainstore.Rollback{
			ExpectedTip: bm.tipFeed.Snapshot().BlockStamp(),
			Height:      0,
		}
		_, err := backend.Rollback(request)
		require.ErrorIs(t, err, errDSTWrite)
		_, err = backend.Snapshot()
		require.NoError(t, err)

		// Even supplying the freshly read durable tip cannot recover
		// the lost event.
		tip, height, err := blocks.ChainTip()
		require.NoError(t, err)
		request.ExpectedTip = headerfs.BlockStamp{
			Height:    int32(height),
			Hash:      tip.BlockHash(),
			Timestamp: tip.Timestamp,
		}
		_, err = backend.Rollback(request)
		require.NoError(t, err)
		synctest.Wait()
		require.Len(
			t,
			events,
			3,
			"disconnect for the applied-but-failed top step must "+
				"survive retry",
		)
	})
}

// TestSnapshotHealsBlockWrite checks that a snapshot repairs the published
// block tip after a write that was applied but acknowledged with an error.
// The published tip is what status queries read, so a stale value would make
// the node report a height that differs from the durable chain.
func TestSnapshotHealsBlockWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, _ := chainStoreFixture(t, 0, 0)
		bm.cfg.BlockHeaders = &blockStoreFault{
			BlockHeaderStore: blocks,
			failWrite:        true,
		}
		backend := newChainStoreBackend(bm)
		fixture := dstChains()
		_, err := backend.CommitBlocks(
			chainstore.BlockCommit{
				ExpectedParent: bm.tipFeed.Snapshot().
					BlockStamp(),
				Headers: []headerfs.BlockHeader{
					{
						BlockHeader: fixture.honest[1],
						Height:      1,
					},
				},
			},
		)
		require.ErrorIs(t, err, errDSTWrite)
		snapshot, err := backend.Snapshot()
		require.NoError(t, err)
		require.Equal(t, uint32(1), snapshot.Filter.BlockHeight)
		require.Equal(
			t,
			int32(1),
			bm.tipFeed.Snapshot().BlockStamp().Height,
			"snapshot must heal actor-owned visible block status "+
				"after an applied write",
		)
	})
}

// TestChainStoreRollbackLostAckAndReadback checks the recovery of a rollback
// whose acknowledgement was lost and whose first readback also fails. No
// disconnect may be published until a snapshot confirms the durable tip, and
// the event must then be published exactly once.
func TestChainStoreRollbackLostAckAndReadback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := chainStoreFixture(t, 3, 3)
		bm.cfg.BlockHeaders = &blockStoreFault{
			BlockHeaderStore: blocks, failRollback: true,
			failReadAfterRollback: true,
		}
		backend := newChainStoreBackend(bm)
		reply, err := backend.Rollback(chainstore.Rollback{
			ExpectedTip: bm.tipFeed.Snapshot().BlockStamp(),
			Height:      2,
		})
		require.ErrorIs(t, err, errDSTWrite)
		require.False(t, reply.BlockTipValid)
		synctest.Wait()
		require.Empty(t, events)

		// The first recovery snapshot hits the failing readback and
		// must not publish anything.
		reply, err = backend.Snapshot()
		require.ErrorIs(t, err, errCFHeaderRead)
		require.False(t, reply.BlockTipValid)

		// The second snapshot reads the durable tip and completes the
		// admitted rollback.
		reply, err = backend.Snapshot()
		require.NoError(t, err)
		require.True(t, reply.BlockTipValid)
		require.Equal(t, int32(2), reply.BlockTip.Height)
		synctest.Wait()
		require.Len(t, events, 1)

		// The single disconnect names the block that was removed and
		// its new parent.
		event := <-events
		disconnected, ok := event.(*blockntfns.Disconnected)
		require.True(t, ok)
		fixture := dstChains()
		header := disconnected.Header()
		require.Equal(
			t,
			fixture.honest[3].BlockHash(),
			header.BlockHash(),
		)
		require.Equal(t, uint32(3), disconnected.Height())
		tip := disconnected.ChainTip()
		require.Equal(t, fixture.honest[2].BlockHash(), tip.BlockHash())

		// A further snapshot must not replay the disconnect.
		_, err = backend.Snapshot()
		require.NoError(t, err)
		synctest.Wait()
		require.Empty(t, events)
	})
}

// TestChainStoreReorgWriteOutcomes checks the durable replacement prefix and
// ordered disconnects when the replacement write returns an ambiguous error.
// Whatever the store reports, the reply must describe the chain that is
// durable afterwards, and the two replaced blocks must be disconnected from
// the highest to the lowest. Subscribers rely on that order to unwind their
// own state.
func TestChainStoreReorgWriteOutcomes(t *testing.T) {
	type disconnect = blockntfns.Disconnected
	for _, tc := range []struct {
		name       string
		mode       dstStoreMode
		wantHeight int32
		wantError  bool
	}{
		{"success", dstStoreOK, 3, false},
		{"fail before write", dstStoreDown, 1, true},
		{"lost acknowledgement", dstStoreAckLost, 3, true},
		{"partial write", dstStoreTorn, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				bm, blocks, _, events := chainStoreFixture(
					t,
					3,
					0,
				)
				fault := &dstHeaderStore{
					BlockHeaderStore: blocks,
					mode:             tc.mode,
				}
				bm.cfg.BlockHeaders = fault
				fixture := dstChains()
				backend := newChainStoreBackend(bm)

				// Replace blocks 2 and 3 with the fork.
				tip := bm.tipFeed.Snapshot().BlockStamp()
				reply, err := backend.Reorg(chainstore.Reorg{
					Work:        winningReorgWork,
					ExpectedTip: tip,
					ForkHeight:  1,
					Headers: []headerfs.BlockHeader{
						{
							BlockHeader: fixture.
								fork[1][0],
							Height: 2,
						},
						{
							BlockHeader: fixture.
								fork[1][1],
							Height: 3,
						},
					},
				})
				if tc.wantError {
					require.ErrorIs(t, err, errDSTWrite)
				} else {
					require.NoError(t, err)
				}

				// The reply must name the durable tip.
				require.True(t, reply.BlockTipValid)
				require.Equal(
					t,
					tc.wantHeight,
					reply.BlockTip.Height,
				)
				var expected *wire.BlockHeader
				if tc.wantHeight == 1 {
					expected = fixture.honest[1]
				} else {
					fork := fixture.fork[1]
					expected = fork[tc.wantHeight-2]
				}
				require.Equal(
					t,
					expected.BlockHash(),
					reply.BlockTip.Hash,
				)
				// Both replaced blocks are disconnected, top
				// first.
				synctest.Wait()
				require.Len(t, events, 2)
				for height := 3; height >= 2; height-- {
					event := <-events
					_, ok := event.(*disconnect)
					require.True(t, ok)
					require.Equal(
						t,
						uint32(height),
						event.Height(),
					)
					header := event.Header()
					parent := event.ChainTip()
					require.Equal(
						t,
						fixture.honest[height].
							BlockHash(),
						header.BlockHash(),
					)
					require.Equal(
						t,
						fixture.honest[height-1].
							BlockHash(),
						parent.BlockHash(),
					)
				}
			})
		})
	}
}

// TestChainStoreRollbackFailureBeforeWrite checks that no disconnect is
// published when the rollback failed before changing the store, because the
// block is still part of the durable chain. The later snapshot must then
// complete the admitted rollback and publish the disconnect once.
func TestChainStoreRollbackFailureBeforeWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := chainStoreFixture(t, 3, 3)
		bm.cfg.BlockHeaders = &blockStoreFault{
			BlockHeaderStore: blocks, failRollbackBefore: true,
		}
		backend := newChainStoreBackend(bm)
		request := chainstore.Rollback{
			ExpectedTip: bm.tipFeed.Snapshot().BlockStamp(),
			Height:      2,
		}
		reply, err := backend.Rollback(request)
		require.ErrorIs(t, err, errDSTWrite)
		require.False(t, reply.BlockTipValid)
		synctest.Wait()
		require.Empty(t, events)

		// The handler reconciles through a snapshot, which
		// completes the admitted rollback.
		reply, err = backend.Snapshot()
		require.NoError(t, err)
		require.Equal(t, int32(2), reply.BlockTip.Height)
		synctest.Wait()
		require.Len(t, events, 1)
		event := <-events
		_, ok := event.(*blockntfns.Disconnected)
		require.True(t, ok)
		header := event.Header()
		require.Equal(
			t,
			dstChains().honest[3].BlockHash(),
			header.BlockHash(),
		)
	})
}

// chainStoreFilterRollbackFault fails the next filter header rollback. With
// applied set, the rollback is performed first, so the failure looks like a
// lost acknowledgement. With failRead set, the tip read that follows the
// failure also fails, which forces a second recovery attempt.
type chainStoreFilterRollbackFault struct {
	headerfs.FilterHeaderStore
	applied      bool
	armed        bool
	failRead     bool
	failNextRead bool
}

// RollbackLastBlock fails once while armed, applying the rollback first when
// the fault models a lost acknowledgement.
func (s *chainStoreFilterRollbackFault) RollbackLastBlock(
	parent *chainhash.Hash) (*headerfs.BlockStamp, error) {

	if !s.armed {
		return s.FilterHeaderStore.RollbackLastBlock(parent)
	}
	s.armed = false
	if s.applied {
		_, err := s.FilterHeaderStore.RollbackLastBlock(parent)
		if err != nil {
			return nil, err
		}
	}
	s.failNextRead = s.failRead
	return nil, errDSTWrite
}

// ChainTip fails once after an injected rollback failure when failRead is set.
func (s *chainStoreFilterRollbackFault) ChainTip() (
	*chainhash.Hash, uint32, error) {

	if s.failNextRead {
		s.failNextRead = false
		return nil, 0, errCFHeaderRead
	}
	return s.FilterHeaderStore.ChainTip()
}

// TestChainStorePairedRollbackRetry covers a failed filter rollback before
// and after the filter store applied it, before the block store is changed.
// The block and filter stores must be rolled back as a pair, so the retry has
// to resume from whichever side is ahead and still publish every disconnect.
func TestChainStorePairedRollbackRetry(t *testing.T) {
	type disconnect = blockntfns.Disconnected
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%t", applied), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				bm, blocks, filters, events :=
					chainStoreFixture(
						t,
						3,
						3,
					)

				// The first filter rollback fails, either
				// before or after it was applied.
				fault := &chainStoreFilterRollbackFault{
					FilterHeaderStore: filters,
					applied:           applied,
					armed:             true,
				}
				bm.cfg.RegFilterHeaders = fault
				backend := newChainStoreBackend(bm)
				tip := bm.tipFeed.Snapshot().BlockStamp()
				request := chainstore.Rollback{
					ExpectedTip: tip,
					Height:      1,
				}
				reply, err := backend.Rollback(request)
				require.ErrorIs(t, err, errDSTWrite)
				require.False(t, reply.BlockTipValid)

				// The block store is untouched, and the filter
				// store is one block lower only if the failed
				// rollback was applied.
				_, blockHeight, err := blocks.ChainTip()
				require.NoError(t, err)
				require.Equal(t, uint32(3), blockHeight)
				_, filterHeight, err := filters.ChainTip()
				require.NoError(t, err)
				want := uint32(3)
				if applied {
					want = 2
				}
				require.Equal(t, want, filterHeight)
				synctest.Wait()
				require.Empty(t, events)

				// The handler reconciles through a snapshot,
				// which completes the admitted rollback.
				reply, err = backend.Snapshot()
				require.NoError(t, err)
				require.Equal(
					t,
					int32(1),
					reply.BlockTip.Height,
				)
				synctest.Wait()
				require.Len(t, events, 2)

				// Both removed blocks are disconnected, top
				// first.
				fixture := dstChains()
				for height := 3; height >= 2; height-- {
					event := <-events
					_, ok := event.(*disconnect)
					require.True(t, ok)
					require.Equal(
						t,
						uint32(height),
						event.Height(),
					)
					header := event.Header()
					parent := event.ChainTip()
					require.Equal(
						t,
						fixture.honest[height].
							BlockHash(),
						header.BlockHash(),
					)
					require.Equal(
						t,
						fixture.honest[height-1].
							BlockHash(),
						parent.BlockHash(),
					)
				}
			})
		})
	}
}

// TestChainStoreFilterRollbackLostAckAndReadback retains the paired step
// until both stores and the published filter watermark agree. The filter
// rollback is applied but its acknowledgement and the following readback are
// lost, so only a later snapshot can establish the outcome. Until then the
// block store must stay untouched, and afterwards the filter watermark must
// match the rolled back chain.
func TestChainStoreFilterRollbackLostAckAndReadback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, filters, events := chainStoreFixture(t, 3, 3)
		fixture := dstChains()

		// Start with the filter watermark published at the tip.
		host := &chainStoreHost{manager: bm}
		host.PublishFilterTip(3, fixture.honest[3].BlockHash())
		fault := &chainStoreFilterRollbackFault{
			FilterHeaderStore: filters,
			applied:           true,
			armed:             true,
			failRead:          true,
		}
		bm.cfg.RegFilterHeaders = fault
		backend := newChainStoreBackend(bm)
		request := chainstore.Rollback{
			ExpectedTip: bm.tipFeed.Snapshot().BlockStamp(),
			Height:      1,
		}
		reply, err := backend.Rollback(request)
		require.ErrorIs(t, err, errDSTWrite)
		require.ErrorIs(t, err, errCFHeaderRead)
		require.False(t, reply.BlockTipValid)

		// Nothing is published while the outcome is unknown.
		_, height, err := blocks.ChainTip()
		require.NoError(t, err)
		require.Equal(t, uint32(3), height)
		synctest.Wait()
		require.Empty(t, events)

		// The snapshot completes the rollback and moves the filter
		// watermark down with it.
		reply, err = backend.Snapshot()
		require.NoError(t, err)
		require.True(t, reply.BlockTipValid)
		require.Equal(t, int32(1), reply.BlockTip.Height)
		require.Equal(
			t, uint32(1), bm.tipFeed.Snapshot().FilterHeight,
		)
		synctest.Wait()
		require.Len(t, events, 2)
		for height := 3; height >= 2; height-- {
			event := <-events
			_, ok := event.(*blockntfns.Disconnected)
			require.True(t, ok)
			require.Equal(t, uint32(height), event.Height())
			header := event.Header()
			parent := event.ChainTip()
			require.Equal(
				t,
				fixture.honest[height].BlockHash(),
				header.BlockHash(),
			)
			require.Equal(
				t,
				fixture.honest[height-1].BlockHash(),
				parent.BlockHash(),
			)
		}
	})
}

// TestChainStoreRollbackRepeatedLostAcks completes the admitted rollback
// even when recovery itself encounters another ambiguous rollback. The
// recovery state must survive each failed attempt, otherwise a store that
// keeps losing acknowledgements could strand the chain between two heights.
func TestChainStoreRollbackRepeatedLostAcks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := chainStoreFixture(t, 3, 3)
		bm.cfg.BlockHeaders = &blockStoreFault{
			BlockHeaderStore: blocks, failRollbackCount: 2,
		}
		backend := newChainStoreBackend(bm)
		request := chainstore.Rollback{
			ExpectedTip: bm.tipFeed.Snapshot().BlockStamp(),
			Height:      0,
		}
		reply, err := backend.Rollback(request)
		require.ErrorIs(t, err, errDSTWrite)
		require.False(t, reply.BlockTipValid)

		// Recovery hits a second ambiguous rollback, then completes.
		reply, err = backend.Snapshot()
		require.ErrorIs(t, err, errDSTWrite)
		require.False(t, reply.BlockTipValid)
		reply, err = backend.Snapshot()
		require.NoError(t, err)
		require.True(t, reply.BlockTipValid)
		require.Zero(t, reply.BlockTip.Height)
		synctest.Wait()
		require.Len(t, events, 3)

		// All three blocks are disconnected, top first.
		fixture := dstChains()
		for height := 3; height >= 1; height-- {
			event := <-events
			_, ok := event.(*blockntfns.Disconnected)
			require.True(t, ok)
			require.Equal(t, uint32(height), event.Height())
			header := event.Header()
			parent := event.ChainTip()
			require.Equal(
				t,
				fixture.honest[height].BlockHash(),
				header.BlockHash(),
			)
			require.Equal(
				t,
				fixture.honest[height-1].BlockHash(),
				parent.BlockHash(),
			)
		}
	})
}

// TestChainStoreReorgRetainsReplacement prevents a filter commit from
// overtaking a replacement batch after a partial rollback or failed write.
// Filter headers extend the block chain, so committing them while the block
// store sits at the fork point would bind them to the wrong branch. The
// retained replacement must be written first.
func TestChainStoreReorgRetainsReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := chainStoreFixture(t, 3, 0)
		fault := &dstHeaderStore{
			BlockHeaderStore: blocks,
			mode:             dstStoreDown,
		}
		bm.cfg.BlockHeaders = fault
		fixture := dstChains()
		backend := newChainStoreBackend(bm)
		reply, err := backend.Reorg(chainstore.Reorg{
			Work:        winningReorgWork,
			ExpectedTip: bm.tipFeed.Snapshot().BlockStamp(),
			ForkHeight:  1,
			Headers: []headerfs.BlockHeader{
				{BlockHeader: fixture.
					fork[1][0], Height: 2},
				{BlockHeader: fixture.
					fork[1][1], Height: 3},
			},
		})
		require.ErrorIs(t, err, errDSTWrite)
		require.True(t, reply.BlockTipValid)
		require.Equal(t, int32(1), reply.BlockTip.Height)

		// A filter commit is refused while the replacement is pending.
		_, err = backend.CommitFilters(chainstore.FilterCommit{})
		require.ErrorIs(t, err, errDSTWrite)

		// Once the store recovers, the snapshot writes the replacement.
		fault.mode = dstStoreOK
		reply, err = backend.Snapshot()
		require.NoError(t, err)
		require.True(t, reply.BlockTipValid)
		require.Equal(t, int32(3), reply.BlockTip.Height)
		require.Equal(
			t,
			fixture.fork[1][1].BlockHash(),
			reply.BlockTip.Hash,
		)
		synctest.Wait()
		require.Len(t, events, 2)
		for height := 3; height >= 2; height-- {
			event := <-events
			_, ok := event.(*blockntfns.Disconnected)
			require.True(t, ok)
			header := event.Header()
			require.Equal(
				t,
				fixture.honest[height].
					BlockHash(),
				header.BlockHash(),
			)
		}
	})
}

// TestChainStoreReorgResumesReplacementPrefix keeps an applied replacement
// prefix and writes only the remaining suffix after a lost acknowledgement.
// Rewriting the prefix would fail the store's parent check, and dropping it
// would silently shorten the chain, so the write log must show each height
// exactly once.
func TestChainStoreReorgResumesReplacementPrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := chainStoreFixture(t, 3, 0)
		fault := &dstHeaderStore{
			BlockHeaderStore: blocks,
			mode:             dstStoreTorn,
		}
		bm.cfg.BlockHeaders = fault
		fixture := dstChains()
		backend := newChainStoreBackend(bm)
		reply, err := backend.Reorg(chainstore.Reorg{
			Work:        winningReorgWork,
			ExpectedTip: bm.tipFeed.Snapshot().BlockStamp(),
			ForkHeight:  1,
			Headers: []headerfs.BlockHeader{
				{BlockHeader: fixture.
					fork[1][0], Height: 2},
				{BlockHeader: fixture.
					fork[1][1], Height: 3},
			},
		})
		require.ErrorIs(t, err, errDSTWrite)
		require.True(t, reply.BlockTipValid)
		require.Equal(t, int32(2), reply.BlockTip.Height)

		// The snapshot writes only the missing block.
		reply, err = backend.Snapshot()
		require.NoError(t, err)
		require.Equal(
			t,
			fixture.fork[1][1].BlockHash(),
			reply.BlockTip.Hash,
		)
		require.Equal(t, []uint32{2, 3}, fault.writes)
		synctest.Wait()
		require.Len(t, events, 2)
		for height := 3; height >= 2; height-- {
			event := <-events
			_, ok := event.(*blockntfns.Disconnected)
			require.True(t, ok)
			header := event.Header()
			require.Equal(
				t,
				fixture.honest[height].
					BlockHash(),
				header.BlockHash(),
			)
		}
	})
}

// chainStoreTipReadLimit counts tip reads and, once a positive limit is set,
// fails every read beyond it. It lets a test bound how many store reads a
// request may perform.
type chainStoreTipReadLimit struct {
	headerfs.BlockHeaderStore
	reads int
	limit int
}

// ChainTip counts the read and fails it when the limit is exceeded.
func (s *chainStoreTipReadLimit) ChainTip() (
	*wire.BlockHeader, uint32, error) {

	s.reads++
	if s.limit > 0 && s.reads > s.limit {
		return nil, 0, errCFHeaderRead
	}
	return s.BlockHeaderStore.ChainTip()
}

// TestChainStoreCompletedIntentDoesNotReplay checks that a completed reorg
// leaves no intent behind, so later snapshots do not re-read its prefix. A
// stale intent would make every snapshot repeat the recovery reads for the
// lifetime of the node.
func TestChainStoreCompletedIntentDoesNotReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, _ := chainStoreFixture(t, 3, 0)
		writer := &dstHeaderStore{
			BlockHeaderStore: blocks,
			mode:             dstStoreTorn,
		}
		reads := &chainStoreTipReadLimit{BlockHeaderStore: writer}
		bm.cfg.BlockHeaders = reads
		fixture := dstChains()
		backend := newChainStoreBackend(bm)
		_, err := backend.Reorg(chainstore.Reorg{
			Work:        winningReorgWork,
			ExpectedTip: bm.tipFeed.Snapshot().BlockStamp(),
			ForkHeight:  1,
			Headers: []headerfs.BlockHeader{
				{BlockHeader: fixture.
					fork[1][0], Height: 2},
				{BlockHeader: fixture.
					fork[1][1], Height: 3},
			},
		})
		require.ErrorIs(t, err, errDSTWrite)

		// The first snapshot completes the reorg.
		_, err = backend.Snapshot()
		require.NoError(t, err)
		require.False(t, backend.HasPendingRollback())

		// A second snapshot performs only its two ordinary tip reads.
		reads.reads = 0
		reads.limit = 2
		_, err = backend.Snapshot()
		require.NoError(t, err)
		require.Equal(t, 2, reads.reads)
	})
}

// lostRollbackReadFault fails the second tip read after a lost rollback
// acknowledgement.
type lostRollbackReadFault struct {
	headerfs.BlockHeaderStore
	armed bool
	reads int
}

// RollbackLastBlock applies the first rollback and then reports a lost
// acknowledgement.
func (s *lostRollbackReadFault) RollbackLastBlock() (
	*headerfs.BlockStamp, error) {

	tip, err := s.BlockHeaderStore.RollbackLastBlock()
	if err == nil && !s.armed {
		s.armed = true
		return nil, errDSTWrite
	}
	return tip, err
}

// ChainTip fails the second read after the lost acknowledgement.
func (s *lostRollbackReadFault) ChainTip() (
	*wire.BlockHeader, uint32, error) {

	if s.armed {
		s.reads++
		if s.reads == 2 {
			return nil, 0, errCFHeaderRead
		}
	}
	return s.BlockHeaderStore.ChainTip()
}

// TestRollbackRecoveryReadRetainsWholeTarget checks that a failed readback
// during recovery does not shrink the admitted rollback target. The first
// snapshot fails, and the second must still roll back to genesis and publish
// a disconnect for every removed block, not only for the blocks above the
// height that the failed read last reported.
func TestRollbackRecoveryReadRetainsWholeTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := chainStoreFixture(t, 3, 3)
		bm.cfg.BlockHeaders = &lostRollbackReadFault{
			BlockHeaderStore: blocks,
		}
		backend := newChainStoreBackend(bm)
		request := chainstore.Rollback{
			ExpectedTip: bm.tipFeed.Snapshot().BlockStamp(),
			Height:      0,
		}
		// The rollback is applied, but its acknowledgement is lost.
		_, err := backend.Rollback(request)
		require.ErrorIs(t, err, errDSTWrite)

		// The first recovery snapshot hits the failing readback, and
		// the second one completes the rollback.
		_, err = backend.Snapshot()
		require.ErrorIs(t, err, errCFHeaderRead)
		reply, err := backend.Snapshot()
		require.NoError(t, err)
		synctest.Wait()
		require.Zero(
			t,
			reply.BlockTip.Height,
			"healed snapshot must finish the admitted rollback to "+
				"genesis",
		)
		require.Len(t, events, 3)
	})
}

// rollbackParentReadFault fails the first lookup of one header hash. The
// rollback path looks up the parent of each block that it removes, so this
// fault interrupts a multi-step rollback part way through.
type rollbackParentReadFault struct {
	headerfs.BlockHeaderStore
	failHash chainhash.Hash
	failed   bool
}

// FetchHeader fails once for the configured hash.
func (s *rollbackParentReadFault) FetchHeader(
	hash *chainhash.Hash,
) (*wire.BlockHeader, uint32, error) {

	if *hash == s.failHash && !s.failed {
		s.failed = true
		return nil, 0, errCFHeaderRead
	}
	return s.BlockHeaderStore.FetchHeader(hash)
}

// TestRollbackParentReadRetainsTarget checks that a rollback interrupted by a
// failed parent lookup keeps its target. The partial rollback is reported as
// an error, and the next snapshot must finish the rollback to genesis rather
// than report the intermediate height as success.
func TestRollbackParentReadRetainsTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, _ := chainStoreFixture(t, 3, 3)
		bm.cfg.BlockHeaders = &rollbackParentReadFault{
			BlockHeaderStore: blocks,
			failHash:         dstChains().honest[1].BlockHash(),
		}
		backend := newChainStoreBackend(bm)
		request := chainstore.Rollback{
			ExpectedTip: bm.tipFeed.Snapshot().BlockStamp(),
			Height:      0,
		}
		_, err := backend.Rollback(request)
		require.ErrorIs(t, err, errCFHeaderRead)
		synctest.Wait()
		snapshot, snapshotErr := backend.Snapshot()
		require.NoError(t, snapshotErr)
		require.Zero(
			t,
			snapshot.BlockTip.Height,
			"snapshot must finish pending rollback before "+
				"reporting success",
		)
	})
}

// TestChainStoreReorgRejectsWeakBranch verifies that the store compares a
// branch's work on its own turn, and that a branch without more work changes
// nothing. A tie keeps the durable chain too, since switching gains nothing.
// A CompareOnly request that wins also changes nothing.
func TestChainStoreReorgRejectsWeakBranch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := chainStoreFixture(t, 3, 3)
		backend := newChainStoreBackend(bm)
		fixture := dstChains()
		fork := fixture.fork[1]
		tip := bm.tipFeed.Snapshot().BlockStamp()

		// The two blocks above the fork are the known work.
		header2, err := blocks.FetchHeaderByHeight(2)
		require.NoError(t, err)
		header3, err := blocks.FetchHeaderByHeight(3)
		require.NoError(t, err)
		known := new(big.Int).Add(
			blockchain.CalcWork(header2.Bits),
			blockchain.CalcWork(header3.Bits),
		)

		request := func(work *big.Int, compare bool) chainstore.Reorg {
			return chainstore.Reorg{
				ExpectedTip: tip,
				ForkHeight:  1,
				Work:        work,
				CompareOnly: compare,
				Headers: []headerfs.BlockHeader{
					{BlockHeader: fork[0], Height: 2},
					{BlockHeader: fork[1], Height: 3},
				},
			}
		}

		reply, err := backend.Reorg(request(big.NewInt(1), false))
		require.NoError(t, err)
		require.Equal(t, chainstore.ReorgLessWork, reply.Verdict)

		reply, err = backend.Reorg(request(known, false))
		require.NoError(t, err)
		require.Equal(t, chainstore.ReorgEqualWork, reply.Verdict)

		reply, err = backend.Reorg(request(winningReorgWork, true))
		require.NoError(t, err)
		require.Equal(t, chainstore.ReorgNotRejected, reply.Verdict)

		// None of them moved the chain or sent a notification.
		_, height, err := blocks.ChainTip()
		require.NoError(t, err)
		require.EqualValues(t, 3, height)
		synctest.Wait()
		require.Empty(t, events)
		require.False(t, backend.HasPendingRollback())
	})
}

// TestChainStoreSuccessfulRollbackAndReorgAvoidReadback checks that a
// rollback or reorg that succeeds performs no tip readback beyond the reads
// made before it begins: two, plus one for a reorg's work comparison. The
// readback exists only to resolve ambiguous errors, so reading after every
// success would add a store round trip, and a failure point, to the common
// path.
func TestChainStoreSuccessfulRollbackAndReorgAvoidReadback(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "rollback"
		if replace {
			name = "reorg"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				bm, blocks, _, events := chainStoreFixture(
					t,
					3,
					3,
				)
				limit := 2
				if replace {
					limit = 3
				}
				reads := &chainStoreTipReadLimit{
					BlockHeaderStore: blocks,
					limit:            limit,
				}
				bm.cfg.BlockHeaders = reads
				backend := newChainStoreBackend(bm)

				// Run either a reorg that replaces blocks 2 and
				// 3, or a rollback to height 1.
				var reply chainstore.Reply
				var err error
				if replace {
					fixture := dstChains()
					fork := fixture.fork[1]
					headers := []headerfs.BlockHeader{
						{
							BlockHeader: fork[0],
							Height:      2,
						},
						{
							BlockHeader: fork[1],
							Height:      3,
						},
					}
					tip := bm.tipFeed.Snapshot().
						BlockStamp()
					request := chainstore.Reorg{
						Work:        winningReorgWork,
						ExpectedTip: tip,
						ForkHeight:  1,
						Headers:     headers,
					}
					reply, err = backend.Reorg(request)
					require.Equal(
						t,
						fixture.fork[1][1].BlockHash(),
						reply.BlockTip.Hash,
					)
				} else {
					tip := bm.tipFeed.Snapshot().
						BlockStamp()
					request := chainstore.Rollback{
						ExpectedTip: tip,
						Height:      1,
					}
					reply, err = backend.Rollback(request)
					require.Equal(
						t,
						int32(1),
						reply.BlockTip.Height,
					)
				}

				// The reply is valid and the store was read
				// only before the mutation.
				require.NoError(t, err)
				require.True(t, reply.BlockTipValid)
				require.Equal(t, limit, reads.reads)
				require.False(t, backend.HasPendingRollback())
				synctest.Wait()
				require.Len(t, events, 2)
				require.Equal(t, uint32(3), (<-events).Height())
				require.Equal(t, uint32(2), (<-events).Height())
			})
		})
	}
}

// TestChainStoreBackendBlockMutation verifies durable parent checks and the
// visible snapshot around a block write and paired rollback. A second commit
// prepared on the old parent must be rejected, since accepting it would fork
// the store from the chain the caller validated, and the published tip must
// follow every accepted mutation.
func TestChainStoreBackendBlockMutation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := newMemBlockHeaderStore(params)
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		bm := newCFHeaderTestManager(t, blocks, filters)
		client := bm.chainStore

		// Capture the one notification that the rollback publishes.
		notifications := make(chan blockntfns.BlockNtfn, 1)
		go func() {
			select {
			case n := <-bm.blockNtfnChan:
				notifications <- n
			case <-bm.quit:
			}
		}()

		// Commit one block on top of genesis.
		genesis := params.GenesisBlock.Header
		header := wire.BlockHeader{
			PrevBlock: genesis.BlockHash(),
			Timestamp: genesis.Timestamp.Add(time.Minute),
		}
		request := chainstore.BlockCommit{
			ExpectedParent: headerfs.BlockStamp{
				Height: 0, Hash: genesis.BlockHash(),
			},
			Headers: []headerfs.BlockHeader{{
				BlockHeader: &header, Height: 1,
			}},
		}
		result := client.Ask(t.Context(), request)
		require.False(t, result.IsErr())
		reply := result.UnwrapOr(chainstore.Reply{})
		require.NoError(t, reply.Err)
		require.Equal(t, header.BlockHash(), reply.BlockTip.Hash)
		require.Equal(t, int32(1), reply.BlockTip.Height)
		visible := bm.tipFeed.Snapshot().BlockStamp()
		require.Equal(t, reply.BlockTip, visible)

		// A second command prepared on the old parent is rejected.
		result = client.Ask(t.Context(), request)
		require.False(t, result.IsErr())
		require.Error(t, result.UnwrapOr(chainstore.Reply{}).Err)
		_, height, err := blocks.ChainTip()
		require.NoError(t, err)
		require.Equal(t, uint32(1), height)

		// Rolling back to genesis restores the visible tip and
		// publishes a disconnect.
		result = client.Ask(t.Context(), chainstore.Rollback{
			ExpectedTip: reply.BlockTip, Height: 0,
		})
		require.False(t, result.IsErr())
		rolledBack := result.UnwrapOr(chainstore.Reply{})
		require.NoError(t, rolledBack.Err)
		require.Equal(t, genesis.BlockHash(), rolledBack.BlockTip.Hash)
		visible = bm.tipFeed.Snapshot().BlockStamp()
		require.Equal(t, rolledBack.BlockTip, visible)
		n := <-notifications
		_, ok := n.(*blockntfns.Disconnected)
		require.True(t, ok)
		require.Equal(t, uint32(1), n.Height())
	})
}

// chainStoreBlockedTip blocks every tip read once armed. A status path that
// reads the store therefore hangs or trips the entered signal, which lets a
// test tell it apart from a path that uses the published tip.
type chainStoreBlockedTip struct {
	headerfs.BlockHeaderStore
	armed   bool
	entered chan struct{}
	release chan struct{}
}

// ChainTip signals the read and blocks until release is closed while armed.
func (s *chainStoreBlockedTip) ChainTip() (
	*wire.BlockHeader, uint32, error) {

	if s.armed {
		close(s.entered)
		<-s.release
	}
	return s.BlockHeaderStore.ChainTip()
}

// TestBlockHeadersSyncedUsesPublishedTip checks that status is independent of
// a blocked store read. Status queries come from many goroutines, so reading
// the store for them would let a slow write stall every caller. This does not
// model a SQL transaction lock.
func TestBlockHeadersSyncedUsesPublishedTip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := &chainStoreBlockedTip{
			BlockHeaderStore: newMemBlockHeaderStore(params),
			entered:          make(chan struct{}),
			release:          make(chan struct{}),
		}
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		bm := newCFHeaderTestManager(t, blocks, filters)
		blocks.armed = true
		done := make(chan bool, 1)
		go func() { done <- bm.BlockHeadersSynced() }()
		synctest.Wait()
		select {
		case <-done:
		case <-blocks.entered:
			close(blocks.release)
			<-done
			t.Fatal("status attempted a store tip read")
		default:
			t.Fatal("status did not return")
		}
	})
}
