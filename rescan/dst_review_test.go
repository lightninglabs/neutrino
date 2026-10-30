package rescan

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// TestDSTCanonicalRetrySuccessor verifies that a successor notification is
// retained while its predecessor's filter fetch is retried.
func TestDSTCanonicalRetrySuccessor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		chain := &dstChain{mockChainWithTxs: newMockChainWithTxs(3)}
		genesis := chain.ChainParams().GenesisBlock.Header
		addr, err := address.NewAddressPubKeyHash(
			make([]byte, 20), &chaincfg.MainNetParams,
		)
		require.NoError(t, err)
		oracle := &dstOracle{tip: []chainhash.Hash{genesis.BlockHash()}}
		actor := mustNewRescanActor(t, ActorConfig{
			Chain: chain,
			StartStamp: headerfs.BlockStamp{
				Hash: genesis.BlockHash(),
			},
			StartHeader: genesis,
			WatchAddrs:  []address.Address{addr},
			Callbacks: Callbacks{
				OnBlockConnected: func(h int32,
					hdr *wire.BlockHeader,
					_ time.Time) {

					oracle.connect(h, hdr)
				},
			},
		})
		actor.Start()
		defer func() { actor.Stop(); actor.WaitForShutdown() }()
		synctest.Wait()
		chain.muFault.Lock()
		chain.filterFailures = 1
		chain.muFault.Unlock()
		first := chain.addBlock()
		chain.ntfnChan <- blockntfns.NewBlockConnected(
			*chain.blockHeaders[first.Hash], uint32(first.Height),
		)
		synctest.Wait()
		time.Sleep(time.Nanosecond)
		second := chain.addBlock()
		chain.ntfnChan <- blockntfns.NewBlockConnected(
			*chain.blockHeaders[second.Hash], uint32(second.Height),
		)
		synctest.Wait()
		for i := 0; i < 3; i++ {
			time.Sleep(retryInterval)
			synctest.Wait()
		}
		state, err := actor.CurrentState()
		require.NoError(t, err)
		current, ok := state.(*StateCurrent)
		require.True(t, ok, "state %T", state)
		require.Equal(t, second.Height, current.CurStamp.Height)
		require.Equal(t, second.Hash, current.CurStamp.Hash)
		require.Empty(t, oracle.err)
		require.Equal(t, second.Hash, oracle.tip[len(oracle.tip)-1])
		require.Len(t, oracle.events, int(second.Height))
	})
}

// TestDSTCatchupMailboxBurst fills the real mailbox while historical scanning
// is blocked, then requires the actor to process the entire canonical chain.
func runDSTCatchupMailboxBurst(t *testing.T, seed byte) []dstEvent {
	t.Helper()
	var result []dstEvent
	synctest.Test(t, func(t *testing.T) {
		chain := &dstChain{mockChainWithTxs: newMockChainWithTxs(2)}
		entered, release := make(chan struct{}), make(chan struct{})
		chain.filterEntered, chain.filterRelease = entered, release
		genesis := chain.ChainParams().GenesisBlock.Header
		addr, err := address.NewAddressPubKeyHash(
			make([]byte, 20), &chaincfg.MainNetParams,
		)
		require.NoError(t, err)
		oracle := &dstOracle{tip: []chainhash.Hash{genesis.BlockHash()}}
		var payments []chainhash.Hash
		actor := mustNewRescanActor(t, ActorConfig{
			Chain: chain,
			StartStamp: headerfs.BlockStamp{
				Hash: genesis.BlockHash(),
			},
			StartHeader: genesis,
			WatchAddrs:  []address.Address{addr},
			BatchSize:   1,
			Callbacks: Callbacks{
				OnBlockConnected: func(h int32,
					hdr *wire.BlockHeader,
					_ time.Time) {

					oracle.connect(h, hdr)
				},
				OnFilteredBlockConnected: func(_ int32,
					_ *wire.BlockHeader,
					txs []*btcutil.Tx) {

					for _, tx := range txs {
						hash := *tx.Hash()
						payments = append(
							payments, hash,
						)
					}
				},
			},
		})
		actor.Start()
		defer func() { actor.Stop(); actor.WaitForShutdown() }()
		<-entered
		count := 101 + int(seed%64)
		var paymentHash chainhash.Hash
		for i := 0; i < count; i++ {
			var stamp headerfs.BlockStamp
			if i == count/2 {
				var tx *wire.MsgTx
				stamp, tx = chain.addBlockWithTx(addr)
				paymentHash = tx.TxHash()
			} else {
				stamp = chain.addBlock()
			}
			header := *chain.blockHeaders[stamp.Hash]
			chain.ntfnChan <- blockntfns.NewBlockConnected(
				header, uint32(stamp.Height),
			)
		}
		synctest.Wait()
		close(release)
		synctest.Wait()
		for i := 0; i < 3; i++ {
			time.Sleep(retryInterval)
			synctest.Wait()
		}
		state, err := actor.CurrentState()
		require.NoError(t, err)
		current, ok := state.(*StateCurrent)
		require.True(t, ok, "state %T", state)
		best, err := chain.BestBlock()
		require.NoError(t, err)
		require.Equal(t, best.Hash, current.CurStamp.Hash)
		require.Equal(t, best.Height, current.CurStamp.Height)
		require.Empty(t, oracle.err)
		require.Equal(t, best.Hash, oracle.tip[len(oracle.tip)-1])
		require.Len(t, oracle.events, int(best.Height))
		require.Equal(t, []chainhash.Hash{paymentHash}, payments)
		result = append([]dstEvent(nil), oracle.events...)
	})
	return result
}

func TestDSTCatchupMailboxBurst(t *testing.T) {
	for _, seed := range []byte{0, 49, 63} {
		first := runDSTCatchupMailboxBurst(t, seed)
		require.Equal(t, first, runDSTCatchupMailboxBurst(t, seed))
	}
}

func FuzzDSTCatchupMailboxBurst(f *testing.F) {
	for _, seed := range []byte{0, 49, 63} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed byte) {
		first := runDSTCatchupMailboxBurst(t, seed)
		require.Equal(t, first, runDSTCatchupMailboxBurst(t, seed))
	})
}

// TestDSTInitializingRewind checks that an update sent before Start, which
// Initializing buffers, is kept and scans blocks below the original start
// stamp.
func TestDSTInitializingRewind(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		chain := &dstChain{mockChainWithTxs: newMockChainWithTxs(4)}
		start, err := chain.GetBlockHeaderByHeight(2)
		require.NoError(t, err)
		var heights []int32
		actor := mustNewRescanActor(t, ActorConfig{
			Chain: chain,
			StartStamp: headerfs.BlockStamp{
				Hash: start.BlockHash(), Height: 2,
			},
			StartHeader: *start,
			Callbacks: Callbacks{
				OnBlockConnected: func(h int32,
					_ *wire.BlockHeader,
					_ time.Time) {

					heights = append(heights, h)
				},
			},
		})
		defer func() { actor.Stop(); actor.WaitForShutdown() }()

		// The update waits in the mailbox ahead of the first batch.
		rewind := uint32(0)
		done := make(chan error, 1)
		go func() { done <- actor.AddWatchAddrs(nil, nil, &rewind) }()
		synctest.Wait()
		actor.Start()
		require.NoError(t, <-done)
		for i := 0; i < 4; i++ {
			time.Sleep(retryInterval)
			synctest.Wait()
		}
		state, err := actor.CurrentState()
		require.NoError(t, err)
		current, ok := state.(*StateCurrent)
		require.True(t, ok, "state %T", state)
		best, err := chain.BestBlock()
		require.NoError(t, err)
		require.Equal(t, best.Hash, current.CurStamp.Hash)
		require.Equal(t, []int32{1, 2, 3}, heights)
	})
}

// TestDSTWatchUpdateAfterTimeout records whether an already enqueued update
// takes effect after its caller's deadline expires.
func TestDSTWatchUpdateAfterTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		chain := &dstChain{mockChainWithTxs: newMockChainWithTxs(2)}
		entered, release := make(chan struct{}), make(chan struct{})
		chain.filterEntered, chain.filterRelease = entered, release
		genesis := chain.ChainParams().GenesisBlock.Header
		initial, err := address.NewAddressPubKeyHash(
			make([]byte, 20), &chaincfg.MainNetParams,
		)
		require.NoError(t, err)
		nextBytes := make([]byte, 20)
		nextBytes[0] = 1
		next, err := address.NewAddressPubKeyHash(
			nextBytes, &chaincfg.MainNetParams,
		)
		require.NoError(t, err)
		actor := mustNewRescanActor(t, ActorConfig{
			Chain: chain,
			StartStamp: headerfs.BlockStamp{
				Hash: genesis.BlockHash(),
			},
			StartHeader: genesis,
			WatchAddrs:  []address.Address{initial},
		})
		actor.Start()
		defer func() { actor.Stop(); actor.WaitForShutdown() }()
		<-entered
		done := make(chan error, 1)
		go func() {
			done <- actor.AddWatchAddrs(
				[]address.Address{next}, nil, nil,
			)
		}()
		synctest.Wait()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		require.Error(t, <-done)
		close(release)
		synctest.Wait()
		state, err := actor.CurrentState()
		require.NoError(t, err)
		current, ok := state.(*StateCurrent)
		require.True(t, ok, "state %T", state)
		require.Contains(t, current.Watch.Addrs, next)
	})
}

// TestDSTDelayedConnectAfterCatchup checks a notification captured during
// historical scanning but delivered after the scan reaches the tip.
func TestDSTDelayedConnectAfterCatchup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		chain := &dstChain{mockChainWithTxs: newMockChainWithTxs(7)}
		genesis := chain.ChainParams().GenesisBlock.Header
		second, err := chain.GetBlockHeaderByHeight(2)
		require.NoError(t, err)
		third, err := chain.GetBlockHeaderByHeight(3)
		require.NoError(t, err)
		entered, release := make(chan struct{}), make(chan struct{})
		chain.onFilter = func(hash chainhash.Hash) error {
			if hash == third.BlockHash() {
				chain.muFault.Lock()
				chain.onFilter = nil
				chain.muFault.Unlock()
				close(entered)
				<-release
			}
			return nil
		}
		addr, err := address.NewAddressPubKeyHash(
			make([]byte, 20), &chaincfg.MainNetParams,
		)
		require.NoError(t, err)
		oracle := &dstOracle{tip: []chainhash.Hash{genesis.BlockHash()}}
		var errorsReported int
		actor := mustNewRescanActor(t, ActorConfig{
			Chain: chain,
			StartStamp: headerfs.BlockStamp{
				Hash: genesis.BlockHash(),
			},
			StartHeader: genesis,
			WatchAddrs:  []address.Address{addr},
			BatchSize:   1,
			OnError:     func(error) { errorsReported++ },
			Callbacks: Callbacks{
				OnBlockConnected: func(h int32,
					hdr *wire.BlockHeader, _ time.Time) {

					oracle.connect(h, hdr)
				},
			},
		})
		actor.Start()
		defer func() { actor.Stop(); actor.WaitForShutdown() }()
		<-entered
		close(release)
		synctest.Wait()
		state, err := actor.CurrentState()
		require.NoError(t, err)
		_, ok := state.(*StateCurrent)
		require.True(t, ok, "state %T", state)
		chain.ntfnChan <- blockntfns.NewBlockConnected(*second, 2)
		synctest.Wait()
		for i := 0; i < 3; i++ {
			time.Sleep(retryInterval)
			synctest.Wait()
		}
		state, err = actor.CurrentState()
		require.NoError(t, err)
		current, ok := state.(*StateCurrent)
		require.True(t, ok, "state %T", state)
		best, err := chain.BestBlock()
		require.NoError(t, err)
		require.Equal(t, best.Hash, current.CurStamp.Hash)
		require.Equal(t, best.Height, current.CurStamp.Height)
		require.Empty(t, oracle.err)
		require.Len(t, oracle.events, int(best.Height))
		require.Zero(t, errorsReported)
	})
}

// TestDSTDelayedOrphanConnectRejectsCachedFilter exercises a filter-header
// notification captured before rollback and delivered after its disconnect.
func TestDSTDelayedOrphanConnectRejectsCachedFilter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		chain := &dstChain{mockChainWithTxs: newMockChainWithTxs(3)}
		genesis := chain.ChainParams().GenesisBlock.Header
		addr, err := address.NewAddressPubKeyHash(
			make([]byte, 20), &chaincfg.MainNetParams,
		)
		require.NoError(t, err)
		oracle := &dstOracle{tip: []chainhash.Hash{genesis.BlockHash()}}
		var payments []chainhash.Hash
		actor := mustNewRescanActor(t, ActorConfig{
			Chain: chain,
			StartStamp: headerfs.BlockStamp{
				Hash: genesis.BlockHash(),
			},
			StartHeader: genesis,
			WatchAddrs:  []address.Address{addr},
			Callbacks: Callbacks{
				OnBlockConnected: func(h int32,
					hdr *wire.BlockHeader, _ time.Time) {

					oracle.connect(h, hdr)
				},
				OnBlockDisconnected: func(h int32,
					hdr *wire.BlockHeader) {

					oracle.disconnect(h, hdr)
				},
				OnFilteredBlockConnected: func(_ int32,
					_ *wire.BlockHeader,
					txs []*btcutil.Tx) {

					for _, tx := range txs {
						payments = append(
							payments,
							*tx.Hash(),
						)
					}
				},
			},
		})
		actor.Start()
		defer func() { actor.Stop(); actor.WaitForShutdown() }()
		synctest.Wait()
		state, err := actor.CurrentState()
		require.NoError(t, err)
		current, ok := state.(*StateCurrent)
		require.True(t, ok, "state %T", state)
		require.Equal(t, int32(2), current.CurStamp.Height)
		chain.mu.Lock()
		orphan := *chain.blockHeaders[chain.bestBlock.Hash]
		orphanHash := orphan.BlockHash()
		orphanFilter := chain.filters[orphanHash]
		parent := *chain.blockHeaders[orphan.PrevBlock]
		chain.bestBlock = headerfs.BlockStamp{
			Hash:      parent.BlockHash(),
			Height:    1,
			Timestamp: parent.Timestamp,
		}
		chain.mu.Unlock()
		stamp, payment := chain.addBlockWithTx(addr)
		chain.mu.Lock()
		block := chain.blocks[stamp.Hash].MsgBlock().Copy()
		block.Header.Nonce = orphan.Nonce + 1
		replacement := block.Header
		replacementHash := replacement.BlockHash()
		filter, err := builder.BuildBasicFilter(block, nil)
		if err != nil {
			chain.mu.Unlock()
			t.Fatal(err)
		}
		chain.filters[orphanHash] = orphanFilter
		chain.blockHeaders[replacementHash] = &replacement
		chain.blockHeightIndex[replacementHash] = 2
		chain.blockHashesByHeight[2] = &replacementHash
		chain.blocks[replacementHash] = btcutil.NewBlock(block)
		chain.filters[replacementHash] = filter
		chain.bestBlock.Hash = replacementHash
		delete(chain.blockHeaders, orphanHash)
		chain.mu.Unlock()
		require.NotEqual(t, orphanHash, replacementHash)
		chain.ntfnChan <- blockntfns.NewBlockDisconnected(
			orphan, 2, parent,
		)
		synctest.Wait()
		chain.ntfnChan <- blockntfns.NewBlockConnected(orphan, 2)
		synctest.Wait()
		require.Empty(t, oracle.err)
		require.Len(t, oracle.events, 3)
		chain.ntfnChan <- blockntfns.NewBlockConnected(replacement, 2)
		synctest.Wait()
		for i := 0; i < 3; i++ {
			time.Sleep(retryInterval)
			synctest.Wait()
		}
		state, err = actor.CurrentState()
		require.NoError(t, err)
		current, ok = state.(*StateCurrent)
		require.True(t, ok, "state %T", state)
		require.Equal(t, int32(2), current.CurStamp.Height)
		require.Equal(t, replacementHash, current.CurStamp.Hash)
		require.Empty(t, oracle.err)
		require.Len(t, oracle.events, 4)
		require.Equal(
			t, dstEvent{"disconnect", 2, orphanHash},
			oracle.events[2],
		)
		require.Equal(
			t, dstEvent{"connect", 2, replacementHash},
			oracle.events[3],
		)
		require.Equal(t, []chainhash.Hash{payment.TxHash()}, payments)
	})
}
