package rescan

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcutil/v2"
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
		actor := NewRescanActor(ActorConfig{
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
			time.Sleep(filterRetryInterval)
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
		if seed&128 != 0 {
			chain.subscribeFailures = 1
		}
		entered, release := make(chan struct{}), make(chan struct{})
		chain.filterEntered, chain.filterRelease = entered, release
		genesis := chain.ChainParams().GenesisBlock.Header
		addr, err := address.NewAddressPubKeyHash(
			make([]byte, 20), &chaincfg.MainNetParams,
		)
		require.NoError(t, err)
		oracle := &dstOracle{tip: []chainhash.Hash{genesis.BlockHash()}}
		var payments []chainhash.Hash
		actor := NewRescanActor(ActorConfig{
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
			time.Sleep(filterRetryInterval)
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
	for _, seed := range []byte{0, 49, 63, 128} {
		first := runDSTCatchupMailboxBurst(t, seed)
		require.Equal(t, first, runDSTCatchupMailboxBurst(t, seed))
	}
}

func FuzzDSTCatchupMailboxBurst(f *testing.F) {
	for _, seed := range []byte{0, 49, 63, 128} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed byte) {
		first := runDSTCatchupMailboxBurst(t, seed)
		require.Equal(t, first, runDSTCatchupMailboxBurst(t, seed))
	})
}

// TestDSTInitializingRewind checks that an accepted update survives a failed
// initial subscription and scans blocks below the original start stamp.
func TestDSTInitializingRewind(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		chain := &dstChain{
			mockChainWithTxs:  newMockChainWithTxs(4),
			subscribeFailures: 1,
		}
		start, err := chain.GetBlockHeaderByHeight(2)
		require.NoError(t, err)
		var heights []int32
		actor := NewRescanActor(ActorConfig{
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
		actor.Start()
		defer func() { actor.Stop(); actor.WaitForShutdown() }()
		rewind := uint32(0)
		require.NoError(t, actor.AddWatchAddrs(nil, nil, &rewind))
		for i := 0; i < 4; i++ {
			time.Sleep(filterRetryInterval)
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
