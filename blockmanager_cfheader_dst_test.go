package neutrino

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
	"github.com/lightninglabs/neutrino/sqldb"
	"github.com/stretchr/testify/require"
)

// cfHeaderTipGate pauses rollback after it has read the filter tip.
type cfHeaderTipGate struct {
	headerfs.FilterHeaderStore
	entered chan struct{}
	release chan struct{}
	armed   bool
}

func (s *cfHeaderTipGate) ChainTip() (*chainhash.Hash, uint32, error) {
	hash, height, err := s.FilterHeaderStore.ChainTip()
	if s.armed {
		s.armed = false
		close(s.entered)
		<-s.release
	}
	return hash, height, err
}

// cfHeaderAncestorGate pauses a writer after it has read the old branch.
type cfHeaderAncestorGate struct {
	headerfs.BlockHeaderStore
	entered chan struct{}
	release chan struct{}
	armed   bool
}

func (s *cfHeaderAncestorGate) FetchHeaderAncestors(
	n uint32,
	hash *chainhash.Hash,
) ([]wire.BlockHeader, uint32, error) {

	headers, height, err := s.BlockHeaderStore.FetchHeaderAncestors(
		n,
		hash,
	)
	if s.armed {
		s.armed = false
		close(s.entered)
		<-s.release
	}
	return headers, height, err
}

// TestCFHeaderWriteRollbackSerializes exercises both sides of the handoff
// between filter header publication and chain rollback. Gates hold only one
// operation, so a serialized implementation can finish after release.
func TestCFHeaderWriteRollbackSerializes(t *testing.T) {
	for _, backend := range []string{"memory", "sql"} {
		for _, phase := range []string{
			"rollback_tip", "writer_ancestors",
		} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				// External database startup must use real time.
				var db *sqldb.Backend
				if backend == "sql" {
					db = sqldb.NewTestBackend(t)
				}
				synctest.Test(t, func(t *testing.T) {
					closeTestBackendInBubble(t, db)
					testCFHeaderWriteRollback(
						t, phase, db,
					)
				})
			})
		}
	}
}

func testCFHeaderWriteRollback(
	t *testing.T, phase string, db *sqldb.Backend,
) {

	t.Helper()
	params := chaincfg.SimNetParams
	params.Checkpoints = nil
	fixture := dstChains()
	var blocks headerfs.BlockHeaderStore
	var filters headerfs.FilterHeaderStore
	if db != nil {
		var err error
		blocks, err = headerfs.NewSQLBlockHeaderStore(
			t.Context(), db.HeaderTxer, &params,
		)
		require.NoError(t, err)
		filters, err = headerfs.NewSQLFilterHeaderStore(
			t.Context(), db.HeaderTxer, headerfs.RegularFilter,
			&params, nil,
		)
		require.NoError(t, err)
	} else {
		blocks = newMemBlockHeaderStore(&params)
		var err error
		filters, err = newMemFilterHeaderStore(&params)
		require.NoError(t, err)
	}
	const oldTip = dstChainHeight + dstTipExtra
	var headers []headerfs.BlockHeader
	for height := uint32(1); height <= oldTip; height++ {
		headers = append(headers, headerfs.BlockHeader{
			BlockHeader: fixture.honest[height], Height: height,
		})
	}
	require.NoError(t, blocks.WriteHeaders(headers...))
	filterTip, _, err := filters.ChainTip()
	require.NoError(t, err)
	prevFilter := *filterTip
	for height := uint32(1); height <= dstReorgFork; height++ {
		f := chainhash.DoubleHashH([]byte(fmt.Sprint(height)))
		prevFilter = chainhash.DoubleHashH(
			append(f[:], prevFilter[:]...),
		)
		hash := fixture.honest[height].BlockHash()
		require.NoError(t, filters.WriteHeaders(headerfs.FilterHeader{
			HeaderHash: hash,
			Height:     height,
			FilterHash: prevFilter,
		}))
	}
	blockGate := &cfHeaderAncestorGate{
		BlockHeaderStore: blocks, entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	filterGate := &cfHeaderTipGate{
		FilterHeaderStore: filters, entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	bm, err := newBlockManager(&blockManagerCfg{
		ChainParams: params, BlockHeaders: blockGate,
		RegFilterHeaders: filterGate,
		QueryDispatcher:  &mockDispatcher{},
		TimeSource: dstMedianTime{
			MedianTimeSource: blockchain.NewMedianTime(),
			offset: time.Until(
				params.GenesisBlock.Header.Timestamp.Add(
					time.Hour,
				),
			),
		},
		BanPeer: func(string, banman.Reason) error { return nil },
	})
	require.NoError(t, err)
	peer := newTestServerPeer(t, "127.0.0.1:18444", int32(oldTip+1))
	bm.syncPeer = peer
	notifications := make(chan blockntfns.BlockNtfn, 32)
	go func() {
		for {
			select {
			case n := <-bm.blockNtfnChan:
				notifications <- n
			case <-bm.quit:
				return
			}
		}
	}()
	bm.wg.Add(1)
	go bm.blockHandler()
	defer func() { require.NoError(t, bm.Stop()) }()
	var hashes []chainhash.Hash
	for height := uint32(dstReorgFork + 1); height <= oldTip; height++ {
		hashes = append(
			hashes,
			chainhash.DoubleHashH([]byte(fmt.Sprint(height))),
		)
	}
	var hashPtrs []*chainhash.Hash
	for i := range hashes {
		hashPtrs = append(hashPtrs, &hashes[i])
	}
	msg := &wire.MsgCFHeaders{StopHash: fixture.honest[oldTip].BlockHash(),
		PrevFilterHeader: prevFilter, FilterHashes: hashPtrs}
	reorg := wire.NewMsgHeaders()
	for _, header := range fixture.reorg[dstReorgFork+1:] {
		require.NoError(t, reorg.AddBlockHeader(header))
	}
	writeDone := make(chan error, 1)
	write := func() {
		_, _, err := bm.submitCFHeaders(
			[]*wire.MsgCFHeaders{msg},
		)
		writeDone <- err
	}
	if phase == "rollback_tip" {
		filterGate.armed = true
		bm.QueueHeaders(reorg, peer)
		<-filterGate.entered
		go write()
		synctest.Wait()
		close(filterGate.release)
	} else {
		blockGate.armed = true
		go write()
		<-blockGate.entered
		bm.QueueHeaders(reorg, peer)
		synctest.Wait()
		close(blockGate.release)
	}
	writeErr := <-writeDone
	// A second owner request is a FIFO barrier after the queued reorg.
	_, _, err = bm.submitCFHeaders(nil)
	require.ErrorContains(t, err, "no cfheaders")
	synctest.Wait()
	blockTip, blockHeight, err := blocks.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(len(fixture.reorg)-1), blockHeight)
	require.Equal(
		t,
		fixture.reorg[len(fixture.reorg)-1].BlockHash(),
		blockTip.BlockHash(),
	)
	_, filterHeight, err := filters.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(dstReorgFork), filterHeight)
	bm.newFilterHeadersMtx.RLock()
	cachedHeight, cachedHash := bm.filterHeaderTip, bm.filterHeaderTipHash
	bm.newFilterHeadersMtx.RUnlock()
	require.Equal(t, filterHeight, cachedHeight)
	require.Equal(t, fixture.honest[dstReorgFork].BlockHash(), cachedHash)
	for height := dstReorgFork + 1; height <= oldTip; height++ {
		hash := fixture.honest[height].BlockHash()
		_, err := filters.FetchHeader(&hash)
		require.Error(t, err)
	}
	if writeErr == nil {
		for height := dstReorgFork + 1; height <= oldTip; height++ {
			n := <-notifications
			_, ok := n.(*blockntfns.Connected)
			require.True(t, ok)
			require.Equal(t, uint32(height), n.Height())
			h := n.Header()
			require.Equal(
				t,
				fixture.honest[height].BlockHash(),
				h.BlockHash(),
			)
		}
	}
	for height := oldTip; height > dstReorgFork; height-- {
		n := <-notifications
		_, ok := n.(*blockntfns.Disconnected)
		require.True(t, ok)
		require.Equal(t, uint32(height), n.Height())
		h, tip := n.Header(), n.ChainTip()
		require.Equal(
			t,
			fixture.honest[height].BlockHash(),
			h.BlockHash(),
		)
		require.Equal(
			t,
			fixture.honest[height-1].BlockHash(),
			tip.BlockHash(),
		)
	}
	require.Empty(t, notifications)
}

// cfHeaderFaultStore injects an error before or after the underlying mutation.
type cfHeaderFaultStore struct {
	headerfs.FilterHeaderStore
	writeFault    int
	rollbackFault int
}

func (s *cfHeaderFaultStore) WriteHeaders(
	headers ...headerfs.FilterHeader,
) error {

	if s.writeFault == 1 {
		return errDSTWrite
	}
	if err := s.FilterHeaderStore.WriteHeaders(headers...); err != nil {
		return err
	}
	if s.writeFault == 2 {
		return errDSTWrite
	}
	return nil
}

func (s *cfHeaderFaultStore) RollbackLastBlock(
	tip *chainhash.Hash) (*headerfs.BlockStamp, error) {

	if s.rollbackFault == 1 {
		return nil, errDSTWrite
	}
	stamp, err := s.FilterHeaderStore.RollbackLastBlock(tip)
	if err == nil && s.rollbackFault == 2 {
		return nil, errDSTWrite
	}
	return stamp, err
}

// TestCFHeaderMutationErrors checks cached tips and notifications against
// durable state when a store fails before or after applying a mutation.
func TestCFHeaderMutationErrors(t *testing.T) {
	for _, test := range []struct {
		name          string
		writeFault    int
		rollbackFault int
		wantHeight    uint32
		wantConnected int
	}{
		{name: "write_before", writeFault: 1},
		{name: "write_after", writeFault: 2,
			wantHeight: 3, wantConnected: 3},
		{name: "rollback_before", rollbackFault: 1,
			wantHeight: 3, wantConnected: 3},
		{name: "rollback_after", rollbackFault: 2,
			wantHeight: 2, wantConnected: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				params := &chaincfg.SimNetParams
				blocks := newMemBlockHeaderStore(
					params,
				)
				underlying, err := newMemFilterHeaderStore(
					params,
				)
				require.NoError(t, err)
				store := &cfHeaderFaultStore{
					FilterHeaderStore: underlying,
				}
				bm := newCFHeaderTestManager(t, blocks, store)
				prev := params.GenesisBlock.Header
				var headers []headerfs.BlockHeader
				for height := uint32(1); height <= 3; height++ {
					h := wire.BlockHeader{
						PrevBlock: prev.BlockHash(),
						Timestamp: prev.Timestamp.Add(
							time.Minute,
						),
						Nonce: height,
					}
					headers = append(
						headers,
						headerfs.BlockHeader{
							BlockHeader: &h,
							Height:      height,
						},
					)
					prev = h
				}
				require.NoError(
					t,
					blocks.WriteHeaders(
						headers...),
				)
				tip, _, err := store.ChainTip()
				require.NoError(t, err)
				f := []chainhash.Hash{
					chainhash.DoubleHashH(
						[]byte("1"),
					),
					chainhash.DoubleHashH(
						[]byte("2"),
					),
					chainhash.DoubleHashH(
						[]byte("3"),
					),
				}
				msg := &wire.MsgCFHeaders{
					StopHash:         prev.BlockHash(),
					PrevFilterHeader: *tip,
					FilterHashes: []*chainhash.Hash{
						&f[0],
						&f[1],
						&f[2],
					},
				}
				notifications := make(
					chan blockntfns.BlockNtfn,
					10,
				)
				stop := make(chan struct{})
				done := make(chan struct{})
				go func() {
					defer close(done)
					for {
						select {
						case n := <-bm.blockNtfnChan:
							notifications <- n
						case <-stop:
							return
						}
					}
				}()
				defer func() { close(stop); <-done }()
				store.writeFault = test.writeFault
				_, _, err = bm.submitCFHeaders(
					[]*wire.MsgCFHeaders{msg},
				)
				if test.writeFault != 0 {
					require.ErrorIs(
						t,
						err,
						errDSTWrite,
					)
				} else {
					require.NoError(t, err)
					store.rollbackFault = test.rollbackFault
					err = rollbackToHeight(t, bm, 0)
					require.ErrorIs(t, err, errDSTWrite)
				}
				synctest.Wait()
				_, durableHeight, err := store.ChainTip()
				require.NoError(t, err)
				require.Equal(
					t,
					test.wantHeight,
					durableHeight,
				)
				bm.newFilterHeadersMtx.RLock()
				cachedHeight := bm.filterHeaderTip
				cachedHash := bm.filterHeaderTipHash
				bm.newFilterHeadersMtx.RUnlock()
				require.Equal(
					t,
					durableHeight,
					cachedHeight,
				)
				header, err := blocks.FetchHeaderByHeight(
					durableHeight,
				)
				require.NoError(t, err)
				require.Equal(
					t,
					header.BlockHash(),
					cachedHash,
				)
				require.Len(
					t,
					notifications,
					test.wantConnected,
				)
				for i := 0; i < test.wantConnected; i++ {
					n := <-notifications
					_, ok := n.(*blockntfns.Connected)
					require.True(t, ok)
					require.Equal(
						t,
						uint32(i+1),
						n.Height(),
					)
					h := n.Header()
					require.Equal(
						t,
						headers[i].BlockHash(),
						h.BlockHash(),
					)
				}
			})
		})
	}
}

// cfHeaderSubscriptionGate pauses the subscription manager before its backlog
// read while a writer reaches the notification send.
type cfHeaderSubscriptionGate struct {
	*blockManager
	entered chan struct{}
	release chan struct{}
}

func (s *cfHeaderSubscriptionGate) NotificationsSinceHeight(
	height uint32) ([]blockntfns.BlockNtfn, uint32, error) {

	close(s.entered)
	<-s.release
	return s.blockManager.NotificationsSinceHeight(height)
}

// TestCFHeaderSubscriptionWhilePublishing checks that backlog registration
// can read the cached tip while a writer is blocked on the unbuffered send.
func TestCFHeaderSubscriptionWhilePublishing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := newMemBlockHeaderStore(params)
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		bm, err := newBlockManager(&blockManagerCfg{
			ChainParams:      *params,
			BlockHeaders:     blocks,
			RegFilterHeaders: filters,
			QueryDispatcher:  &mockDispatcher{},
			TimeSource:       blockchain.NewMedianTime(),
			BanPeer: func(string, banman.Reason) error {
				return nil
			},
		})
		require.NoError(t, err)
		h := wire.BlockHeader{
			PrevBlock: *params.GenesisHash,
			Timestamp: params.GenesisBlock.Header.Timestamp.Add(
				time.Minute,
			),
		}
		require.NoError(
			t,
			blocks.WriteHeaders(headerfs.BlockHeader{
				BlockHeader: &h, Height: 1,
			}),
		)
		filterTip, _, err := filters.ChainTip()
		require.NoError(t, err)
		filterHash := chainhash.DoubleHashH(
			[]byte("subscription"),
		)
		msg := &wire.MsgCFHeaders{
			StopHash:         h.BlockHash(),
			PrevFilterHeader: *filterTip,
			FilterHashes: []*chainhash.Hash{
				&filterHash,
			},
		}
		source := &cfHeaderSubscriptionGate{
			blockManager: bm,
			entered: make(
				chan struct{},
			),
			release: make(chan struct{}),
		}
		bm.wg.Add(1)
		go bm.blockHandler()
		defer func() { require.NoError(t, bm.Stop()) }()
		manager := blockntfns.NewSubscriptionManager(
			source,
		)
		manager.Start()
		defer manager.Stop()
		subDone := make(chan error, 1)
		go func() {
			sub, err := manager.NewSubscription(1)
			if err == nil {
				defer sub.Cancel()
			}
			subDone <- err
		}()
		<-source.entered
		writerDone := make(chan error, 1)
		go func() {
			_, _, err := bm.submitCFHeaders(
				[]*wire.MsgCFHeaders{msg},
			)
			writerDone <- err
		}()
		synctest.Wait()
		close(source.release)
		require.NoError(t, <-writerDone)
		require.NoError(t, <-subDone)
	})
}

// TestCFHeaderSubmitShutdown checks that a filter commit submitted to the
// chain store actor returns once shutdown cancels the manager's context while
// the actor is mid-mutation, and that a commit submitted after the actor has
// stopped reports ErrShuttingDown.
func TestCFHeaderSubmitShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ops := &blockedChainStoreOps{
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		client, err := newChainStoreClient(ops)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		bm := &blockManager{
			chainStore: client, quitCtx: ctx, quitCancel: cancel,
		}

		// Hold the actor inside a block commit so the filter commit
		// waits in its mailbox.
		held := make(chan chainStoreResult, 1)
		client.tryAsk(t.Context(), chainStoreBlockCommit{}, held)
		<-ops.entered
		done := make(chan error, 1)
		go func() {
			_, _, err := bm.submitCFHeaders(nil)
			done <- err
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)

		close(ops.release)
		<-held
		require.NoError(t, client.stop())
		_, _, err = bm.submitCFHeaders(nil)
		require.ErrorIs(t, err, ErrShuttingDown)
	})
}

// newCFHeaderTestManager constructs a manager backed by the given stores.
func newCFHeaderTestManager(t *testing.T, blocks headerfs.BlockHeaderStore,
	filters headerfs.FilterHeaderStore) *blockManager {

	t.Helper()
	bm, err := newBlockManager(&blockManagerCfg{
		ChainParams:      chaincfg.SimNetParams,
		BlockHeaders:     blocks,
		RegFilterHeaders: filters,
		QueryDispatcher:  &mockDispatcher{},
		TimeSource:       blockchain.NewMedianTime(),
		BanPeer: func(string, banman.Reason) error {
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bm.Stop() })

	return bm
}

// TestCFHeaderRollbackPublicationWatermark checks that a rollback preserves
// unpublished surviving headers without republishing previously delivered ones.
func TestCFHeaderRollbackPublicationWatermark(t *testing.T) {
	for _, test := range []struct {
		name          string
		published     bool
		target        uint32
		wantConnected int
	}{
		{name: "above_watermark", target: 2, wantConnected: 2},
		{name: "below_watermark", published: true, target: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				testCFHeaderWatermark(
					t,
					test.published,
					test.target,
					test.wantConnected,
				)
			})
		})
	}
}

// testCFHeaderWatermark exercises one rollback publication boundary.
func testCFHeaderWatermark(t *testing.T, published bool, target uint32,
	wantConnected int) {

	params := &chaincfg.SimNetParams
	blocks := newMemBlockHeaderStore(params)
	filters, err := newMemFilterHeaderStore(params)
	require.NoError(t, err)
	fixture := dstChains()
	var headers []headerfs.BlockHeader
	for i := 1; i <= 3; i++ {
		headers = append(
			headers,
			headerfs.BlockHeader{
				BlockHeader: fixture.honest[i],
				Height:      uint32(i),
			},
		)
	}
	require.NoError(
		t,
		blocks.WriteHeaders(headers...),
	)
	var bm *blockManager
	if !published {
		bm = newCFHeaderTestManager(
			t,
			blocks,
			filters,
		)
	}
	tip, _, err := filters.ChainTip()
	require.NoError(t, err)
	prev := *tip
	for i, h := range headers {
		f := chainhash.DoubleHashH(
			[]byte{byte(i)},
		)
		prev = chainhash.DoubleHashH(
			append(f[:], prev[:]...),
		)
		require.NoError(
			t,
			filters.WriteHeaders(
				headerfs.FilterHeader{
					HeaderHash: h.BlockHash(),
					Height:     h.Height,
					FilterHash: prev,
				},
			),
		)
	}
	if published {
		bm = newCFHeaderTestManager(
			t,
			blocks,
			filters,
		)
	}
	notifications := make(
		chan blockntfns.BlockNtfn,
		10,
	)
	go func() {
		for {
			select {
			case n := <-bm.blockNtfnChan:
				notifications <- n
			case <-bm.quit:
				return
			}
		}
	}()
	require.NoError(
		t,
		rollbackToHeight(t, bm, target),
	)
	synctest.Wait()
	for len(notifications) > 0 {
		n := <-notifications
		_, ok := n.(*blockntfns.Disconnected)
		require.True(t, ok)
	}
	bm.wg.Add(1)
	go bm.blockHandler()
	defer func() { require.NoError(t, bm.Stop()) }()
	_, err = bm.submitCFHeaderReconciliation()
	require.NoError(t, err)
	synctest.Wait()
	require.Len(
		t,
		notifications,
		wantConnected,
	)
	for i := 0; i < wantConnected; i++ {
		n := <-notifications
		_, ok := n.(*blockntfns.Connected)
		require.True(t, ok)
		require.Equal(
			t,
			uint32(i+1),
			n.Height(),
		)
		h := n.Header()
		require.Equal(
			t,
			headers[i].BlockHash(),
			h.BlockHash(),
		)
	}
	bm.newFilterHeadersMtx.RLock()
	defer bm.newFilterHeadersMtx.RUnlock()
	require.Equal(
		t,
		target,
		bm.filterHeaderTip,
	)
}

// TestCFHeaderConflictRejectsOrphanedStop verifies that conflict resolution
// does not request filters for a replacement branch using an old response.
func TestCFHeaderConflictRejectsOrphanedStop(t *testing.T) {
	params := &chaincfg.SimNetParams
	fixture := dstChains()
	blocks := newMemBlockHeaderStore(params)
	for i := 1; i <= 3; i++ {
		require.NoError(t, blocks.WriteHeaders(headerfs.BlockHeader{
			BlockHeader: fixture.honest[i],
			Height:      uint32(i),
		}))
	}
	oldStop := fixture.honest[3].BlockHash()
	_, err := blocks.RollbackBlockHeaders(2)
	require.NoError(t, err)
	for i := 2; i <= 3; i++ {
		require.NoError(t, blocks.WriteHeaders(headerfs.BlockHeader{
			BlockHeader: fixture.fork[1][i], Height: uint32(i),
		}))
	}
	broadcasts := 0
	bm := &blockManager{cfg: &blockManagerCfg{
		BlockHeaders: blocks,
		Broadcaster: broadcastFunc(func(_ *querysyncwm.Broadcast,
			_ ...querysyncwm.QueryOption) error {

			broadcasts++
			return nil
		}),
	}}
	hash := chainhash.DoubleHashH([]byte("filter"))
	msg := &wire.MsgCFHeaders{
		StopHash:     oldStop,
		FilterHashes: []*chainhash.Hash{&hash, &hash},
	}
	_, _, err = bm.detectBadPeers(
		map[string]*wire.MsgCFHeaders{"honest": msg},
		2, 0, wire.GCSFilterRegular,
	)
	require.Error(t, err)
	require.Zero(t, broadcasts)
}

// rollbackToHeight rolls both stores back through the chain store actor.
func rollbackToHeight(t *testing.T, bm *blockManager, height uint32) error {
	t.Helper()

	header, tip, err := bm.cfg.BlockHeaders.ChainTip()
	require.NoError(t, err)

	result := bm.chainStore.ask(t.Context(), chainStoreRollback{
		ExpectedTip: headerfs.BlockStamp{
			Height: int32(tip), Hash: header.BlockHash(),
			Timestamp: header.Timestamp,
		},
		Height: height,
	})
	if result.IsErr() {
		return result.Err()
	}

	return result.UnwrapOr(chainStoreReply{}).Err
}
