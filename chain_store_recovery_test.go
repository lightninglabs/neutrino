package neutrino

import (
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

type reviewBlockFault struct {
	headerfs.BlockHeaderStore
	failWrite, failRollback bool
	failRollbackBefore      bool
	failRollbackCount       int
	failReadAfterRollback   bool
	failNextRead            bool
}

func (s *reviewBlockFault) WriteHeaders(h ...headerfs.BlockHeader) error {
	if err := s.BlockHeaderStore.WriteHeaders(h...); err != nil {
		return err
	}
	if s.failWrite {
		s.failWrite = false
		return errDSTWrite
	}
	return nil
}
func (s *reviewBlockFault) RollbackLastBlock() (*headerfs.BlockStamp, error) {
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
func (s *reviewBlockFault) ChainTip() (*wire.BlockHeader, uint32, error) {
	if s.failNextRead {
		s.failNextRead = false
		return nil, 0, errCFHeaderRead
	}
	return s.BlockHeaderStore.ChainTip()
}

func reviewStoreFixture(
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

func reviewFilterMessage(
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
func TestReviewBackendFilterSuffixRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, _, filters, events := reviewStoreFixture(t, 5, 0)
		fault := &cfHeaderLostAckStore{
			FilterHeaderStore: filters,
			failWrite:         true,
		}
		bm.cfg.RegFilterHeaders = fault
		backend := &blockManagerChainStore{manager: bm}
		_, err := backend.commitFilters(
			chainStoreFilterCommit{
				Messages: []*wire.MsgCFHeaders{
					reviewFilterMessage(t, filters, 1, 3),
				},
			},
		)
		require.ErrorIs(t, err, errDSTWrite)
		require.ErrorIs(t, err, errCFHeaderRead)
		_, err = backend.commitFilters(
			chainStoreFilterCommit{
				Messages: []*wire.MsgCFHeaders{
					reviewFilterMessage(t, filters, 4, 5),
				},
			},
		)
		require.NoError(t, err)
		synctest.Wait()
		t.Logf(
			"published=%d, events=%d",
			bm.filterHeaderTip,
			len(events),
		)
		require.Len(
			t,
			events,
			5,
			"backend must publish the committed prefix before "+
				"advancing with a suffix",
		)
	})
}
func TestReviewBackendRollbackAppliedErrorRetainsDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := reviewStoreFixture(t, 3, 3)
		bm.cfg.BlockHeaders = &reviewBlockFault{
			BlockHeaderStore: blocks,
			failRollback:     true,
		}
		backend := &blockManagerChainStore{manager: bm}
		request := chainStoreRollback{
			ExpectedTip: *bm.publishedBlockTip.Load(),
			Height:      0,
		}
		reply, err := backend.rollback(request)
		require.ErrorIs(t, err, errDSTWrite)
		snapshot, err := backend.snapshot()
		require.NoError(t, err)
		t.Logf(
			"after applied rollback error: replyHeight=%d "+
				"snapshotHeight=%d snapshotQueryHeight=%d "+
				"visibleHeight=%d events=%d",
			reply.BlockTip.Height,
			snapshot.BlockTip.Height,
			snapshot.Filter.blockHeight,
			bm.publishedBlockTip.Load().Height,
			len(events),
		)
		// Even supplying the freshly read durable tip cannot recover
		// the lost event.
		tip, height, err := blocks.ChainTip()
		require.NoError(t, err)
		request.ExpectedTip = headerfs.BlockStamp{
			Height:    int32(height),
			Hash:      tip.BlockHash(),
			Timestamp: tip.Timestamp,
		}
		_, err = backend.rollback(request)
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
func TestReviewBackendSnapshotHealsBlockWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, _ := reviewStoreFixture(t, 0, 0)
		bm.cfg.BlockHeaders = &reviewBlockFault{
			BlockHeaderStore: blocks,
			failWrite:        true,
		}
		backend := &blockManagerChainStore{manager: bm}
		fixture := dstChains()
		_, err := backend.commitBlocks(
			chainStoreBlockCommit{
				ExpectedParent: *bm.publishedBlockTip.Load(),
				Headers: []headerfs.BlockHeader{
					{
						BlockHeader: fixture.honest[1],
						Height:      1,
					},
				},
			},
		)
		require.ErrorIs(t, err, errDSTWrite)
		snapshot, err := backend.snapshot()
		require.NoError(t, err)
		require.Equal(t, uint32(1), snapshot.Filter.blockHeight)
		require.Equal(
			t,
			int32(1),
			bm.publishedBlockTip.Load().Height,
			"snapshot must heal actor-owned visible block status "+
				"after an applied write",
		)
	})
}

func TestChainStoreRollbackLostAckAndReadback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := reviewStoreFixture(t, 3, 3)
		bm.cfg.BlockHeaders = &reviewBlockFault{
			BlockHeaderStore: blocks, failRollback: true,
			failReadAfterRollback: true,
		}
		backend := &blockManagerChainStore{manager: bm}
		reply, err := backend.rollback(chainStoreRollback{
			ExpectedTip: *bm.publishedBlockTip.Load(), Height: 2,
		})
		require.ErrorIs(t, err, errDSTWrite)
		require.False(t, reply.BlockTipValid)
		synctest.Wait()
		require.Empty(t, events)

		reply, err = backend.snapshot()
		require.ErrorIs(t, err, errCFHeaderRead)
		require.False(t, reply.BlockTipValid)
		reply, err = backend.snapshot()
		require.NoError(t, err)
		require.True(t, reply.BlockTipValid)
		require.Equal(t, int32(2), reply.BlockTip.Height)
		synctest.Wait()
		require.Len(t, events, 1)
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

		_, err = backend.snapshot()
		require.NoError(t, err)
		synctest.Wait()
		require.Empty(t, events)
	})
}

// TestChainStoreReorgWriteOutcomes checks the durable replacement prefix and
// ordered disconnects when the replacement write returns an ambiguous error.
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
				bm, blocks, _, events := reviewStoreFixture(
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
				backend := &blockManagerChainStore{manager: bm}
				tip := *bm.publishedBlockTip.Load()
				reply, err := backend.reorg(chainStoreReorg{
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

// TestChainStoreRollbackFailureBeforeWrite does not publish a disconnect
// when the failed mutation leaves the child at the durable tip.
func TestChainStoreRollbackFailureBeforeWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := reviewStoreFixture(t, 3, 3)
		bm.cfg.BlockHeaders = &reviewBlockFault{
			BlockHeaderStore: blocks, failRollbackBefore: true,
		}
		backend := &blockManagerChainStore{manager: bm}
		request := chainStoreRollback{
			ExpectedTip: *bm.publishedBlockTip.Load(), Height: 2,
		}
		reply, err := backend.rollback(request)
		require.ErrorIs(t, err, errDSTWrite)
		require.False(t, reply.BlockTipValid)
		synctest.Wait()
		require.Empty(t, events)

		// The handler reconciles through a snapshot, which completes the
		// admitted rollback.
		reply, err = backend.snapshot()
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

type chainStoreFilterRollbackFault struct {
	headerfs.FilterHeaderStore
	applied      bool
	armed        bool
	failRead     bool
	failNextRead bool
}

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
func TestChainStorePairedRollbackRetry(t *testing.T) {
	type disconnect = blockntfns.Disconnected
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%t", applied), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				bm, blocks, filters, events :=
					reviewStoreFixture(
						t,
						3,
						3,
					)
				fault := &chainStoreFilterRollbackFault{
					FilterHeaderStore: filters,
					applied:           applied,
					armed:             true,
				}
				bm.cfg.RegFilterHeaders = fault
				backend := &blockManagerChainStore{manager: bm}
				tip := *bm.publishedBlockTip.Load()
				request := chainStoreRollback{
					ExpectedTip: tip,
					Height:      1,
				}
				reply, err := backend.rollback(request)
				require.ErrorIs(t, err, errDSTWrite)
				require.False(t, reply.BlockTipValid)
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
				reply, err = backend.snapshot()
				require.NoError(t, err)
				require.Equal(
					t,
					int32(1),
					reply.BlockTip.Height,
				)
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
						dstChains().honest[height].
							BlockHash(),
						header.BlockHash(),
					)
					require.Equal(
						t,
						dstChains().honest[height-1].
							BlockHash(),
						parent.BlockHash(),
					)
				}
			})
		})
	}
}

// TestChainStoreFilterRollbackLostAckAndReadback retains the paired step
// until both stores and the published filter watermark agree.
func TestChainStoreFilterRollbackLostAckAndReadback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, filters, events :=
			reviewStoreFixture(t, 3, 3)
		bm.setFilterHeaderTip(3, dstChains().honest[3].BlockHash())
		fault := &chainStoreFilterRollbackFault{
			FilterHeaderStore: filters,
			applied:           true,
			armed:             true,
			failRead:          true,
		}
		bm.cfg.RegFilterHeaders = fault
		backend := &blockManagerChainStore{manager: bm}
		request := chainStoreRollback{
			ExpectedTip: *bm.publishedBlockTip.Load(),
			Height:      1,
		}
		reply, err := backend.rollback(request)
		require.ErrorIs(t, err, errDSTWrite)
		require.ErrorIs(t, err, errCFHeaderRead)
		require.False(t, reply.BlockTipValid)
		_, height, err := blocks.ChainTip()
		require.NoError(t, err)
		require.Equal(t, uint32(3), height)
		synctest.Wait()
		require.Empty(t, events)
		reply, err = backend.snapshot()
		require.NoError(t, err)
		require.True(t, reply.BlockTipValid)
		require.Equal(t, int32(1), reply.BlockTip.Height)
		require.Equal(t, uint32(1), bm.filterHeaderTip)
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
				dstChains().honest[height].
					BlockHash(),
				header.BlockHash(),
			)
			require.Equal(
				t,
				dstChains().honest[height-1].
					BlockHash(),
				parent.BlockHash(),
			)
		}
	})
}

// TestChainStoreRollbackRepeatedLostAcks completes the admitted rollback
// even when recovery itself encounters another ambiguous rollback.
func TestChainStoreRollbackRepeatedLostAcks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := reviewStoreFixture(t, 3, 3)
		bm.cfg.BlockHeaders = &reviewBlockFault{
			BlockHeaderStore: blocks, failRollbackCount: 2,
		}
		backend := &blockManagerChainStore{manager: bm}
		request := chainStoreRollback{
			ExpectedTip: *bm.publishedBlockTip.Load(), Height: 0,
		}
		reply, err := backend.rollback(request)
		require.ErrorIs(t, err, errDSTWrite)
		require.False(t, reply.BlockTipValid)

		// Recovery hits a second ambiguous rollback, then completes.
		reply, err = backend.snapshot()
		require.ErrorIs(t, err, errDSTWrite)
		require.False(t, reply.BlockTipValid)
		reply, err = backend.snapshot()
		require.NoError(t, err)
		require.True(t, reply.BlockTipValid)
		require.Zero(t, reply.BlockTip.Height)
		synctest.Wait()
		require.Len(t, events, 3)
		for height := 3; height >= 1; height-- {
			event := <-events
			_, ok := event.(*blockntfns.Disconnected)
			require.True(t, ok)
			require.Equal(t, uint32(height), event.Height())
			header := event.Header()
			parent := event.ChainTip()
			require.Equal(
				t,
				dstChains().honest[height].
					BlockHash(),
				header.BlockHash(),
			)
			require.Equal(
				t,
				dstChains().honest[height-1].
					BlockHash(),
				parent.BlockHash(),
			)
		}
	})
}

// TestChainStoreReorgRetainsReplacement prevents a filter commit from
// overtaking a replacement batch after a partial rollback or failed write.
func TestChainStoreReorgRetainsReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := reviewStoreFixture(t, 3, 0)
		fault := &dstHeaderStore{
			BlockHeaderStore: blocks,
			mode:             dstStoreDown,
		}
		bm.cfg.BlockHeaders = fault
		fixture := dstChains()
		backend := &blockManagerChainStore{manager: bm}
		reply, err := backend.reorg(chainStoreReorg{
			ExpectedTip: *bm.publishedBlockTip.Load(),
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
		_, err = backend.commitFilters(chainStoreFilterCommit{})
		require.ErrorIs(t, err, errDSTWrite)
		fault.mode = dstStoreOK
		reply, err = backend.snapshot()
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
func TestChainStoreReorgResumesReplacementPrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := reviewStoreFixture(t, 3, 0)
		fault := &dstHeaderStore{
			BlockHeaderStore: blocks,
			mode:             dstStoreTorn,
		}
		bm.cfg.BlockHeaders = fault
		fixture := dstChains()
		backend := &blockManagerChainStore{manager: bm}
		reply, err := backend.reorg(chainStoreReorg{
			ExpectedTip: *bm.publishedBlockTip.Load(),
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
		reply, err = backend.snapshot()
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

type chainStoreTipReadLimit struct {
	headerfs.BlockHeaderStore
	reads int
	limit int
}

func (s *chainStoreTipReadLimit) ChainTip() (*wire.BlockHeader, uint32, error) {
	s.reads++
	if s.limit > 0 && s.reads > s.limit {
		return nil, 0, errCFHeaderRead
	}
	return s.BlockHeaderStore.ChainTip()
}

// TestChainStoreCompletedIntentDoesNotReplay checks that a completed reorg
// leaves no intent behind, so later snapshots do not re-read its prefix.
func TestChainStoreCompletedIntentDoesNotReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, _ := reviewStoreFixture(t, 3, 0)
		writer := &dstHeaderStore{
			BlockHeaderStore: blocks,
			mode:             dstStoreTorn,
		}
		reads := &chainStoreTipReadLimit{BlockHeaderStore: writer}
		bm.cfg.BlockHeaders = reads
		fixture := dstChains()
		backend := &blockManagerChainStore{manager: bm}
		_, err := backend.reorg(chainStoreReorg{
			ExpectedTip: *bm.publishedBlockTip.Load(),
			ForkHeight:  1,
			Headers: []headerfs.BlockHeader{
				{BlockHeader: fixture.
					fork[1][0], Height: 2},
				{BlockHeader: fixture.
					fork[1][1], Height: 3},
			},
		})
		require.ErrorIs(t, err, errDSTWrite)
		_, err = backend.snapshot()
		require.NoError(t, err)
		require.Nil(t, backend.pending)
		reads.reads = 0
		reads.limit = 2
		_, err = backend.snapshot()
		require.NoError(t, err)
		require.Equal(t, 2, reads.reads)
	})
}
