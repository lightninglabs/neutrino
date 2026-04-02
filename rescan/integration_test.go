package rescan

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// TestIntegrationFullLifecycle exercises the complete actor lifecycle:
// create → sync → receive blocks via subscription → add addresses →
// rewind → re-sync → find transactions → stop.
func TestIntegrationFullLifecycle(t *testing.T) {
	// Phase 1: Build a chain with a known transaction.
	chain := newMockChainWithTxs(10)

	addr, _ := btcutil.NewAddressPubKeyHash(
		chainhash.HashB([]byte("integration-test"))[:20],
		&chaincfg.MainNetParams,
	)

	// Add a block with a tx paying our address at height 11.
	txStamp, expectedTx := chain.addBlockWithTx(addr)
	expectedTxHash := expectedTx.TxHash()

	// Add a few more blocks after the tx block.
	for i := 0; i < 3; i++ {
		chain.addBlock()
	}

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	// Phase 2: Start actor WITHOUT the address — it should sync but
	// not find the tx.
	var (
		finished       atomic.Bool
		filteredCount  atomic.Int32
		foundTxHash    atomic.Value
		blocksReceived atomic.Int32
	)

	actor := NewRescanActor(ActorConfig{
		Chain: chain.mockChainSource,
		StartStamp: headerfs.BlockStamp{
			Hash:   *genesisHash,
			Height: 0,
		},
		StartHeader: *genesisHeader,
		StartTime:   time.Time{},
		// Note: NO WatchAddrs — address not watched yet.
		Callbacks: Callbacks{
			OnFilteredBlockConnected: func(height int32,
				header *wire.BlockHeader,
				relevantTxs []*btcutil.Tx) {

				blocksReceived.Add(1)
				for _, tx := range relevantTxs {
					filteredCount.Add(1)
					foundTxHash.Store(tx.Hash())
				}
			},
			OnRescanFinished: func(height int32) {
				finished.Store(true)
			},
		},
		BatchSize: 50,
	})

	actor.Start()

	// Wait for sync to finish.
	require.Eventually(t, func() bool {
		return finished.Load()
	}, 5*time.Second, 10*time.Millisecond)

	// Verify: blocks were received but no tx found (no address watched).
	require.Greater(t, blocksReceived.Load(), int32(0))
	require.Equal(t, int32(0), filteredCount.Load(),
		"should not find tx without watching the address")

	// Phase 3: Add the address with rewind to before the tx block.
	rewindTo := uint32(txStamp.Height - 1)
	pkScript, _ := txscript.PayToAddrScript(addr)
	_ = pkScript

	err := actor.AddWatchAddrs(
		[]btcutil.Address{addr}, nil, &rewindTo,
	)
	require.NoError(t, err)

	// Wait for the rewind and re-sync to find the tx.
	require.Eventually(t, func() bool {
		return filteredCount.Load() > 0
	}, 10*time.Second, 10*time.Millisecond,
		"should find tx after rewind")

	// Verify the correct transaction was found.
	rawHash := foundTxHash.Load()
	require.NotNil(t, rawHash)
	require.Equal(t, expectedTxHash, *rawHash.(*chainhash.Hash))

	// Phase 4: Receive new blocks via subscription after re-sync.
	// The actor should be back in Current state.
	require.Eventually(t, func() bool {
		state, _ := actor.CurrentState()
		_, ok := state.(*StateCurrent)
		return ok
	}, 5*time.Second, 10*time.Millisecond,
		"should be back in Current after rewind")

	// Add more blocks through the subscription bridge.
	for i := 0; i < 5; i++ {
		newBest := chain.addBlock()
		newHeader := chain.mockChainSource.blockHeaders[newBest.Hash]
		chain.ntfnChan <- blockntfns.NewBlockConnected(
			*newHeader, uint32(newBest.Height),
		)
	}

	// Wait for the new blocks to be processed.
	bestBlock, _ := chain.BestBlock()
	require.Eventually(t, func() bool {
		state, _ := actor.CurrentState()
		if cur, ok := state.(*StateCurrent); ok {
			return cur.CurStamp.Height == bestBlock.Height
		}
		return false
	}, 5*time.Second, 10*time.Millisecond,
		"should process all subscription blocks")

	// Phase 5: Graceful shutdown.
	actor.Stop()
	actor.WaitForShutdown()
}

// TestIntegrationRaceConditionPrevention directly tests the scenario from
// rescan-issues.md §2: AddWatchAddrs arriving at the same time as a block
// notification. The old select-based rescan could miss the block; the FSM
// must not.
func TestIntegrationRaceConditionPrevention(t *testing.T) {
	chain := newMockChainWithTxs(20)

	addr, _ := btcutil.NewAddressPubKeyHash(
		chainhash.HashB([]byte("race-prevent"))[:20],
		&chaincfg.MainNetParams,
	)

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	var (
		finished      atomic.Bool
		filteredCount atomic.Int32
	)

	actor := NewRescanActor(ActorConfig{
		Chain: chain.mockChainSource,
		StartStamp: headerfs.BlockStamp{
			Hash:   *genesisHash,
			Height: 0,
		},
		StartHeader: *genesisHeader,
		StartTime:   time.Time{},
		Callbacks: Callbacks{
			OnFilteredBlockConnected: func(height int32,
				header *wire.BlockHeader,
				relevantTxs []*btcutil.Tx) {

				if len(relevantTxs) > 0 {
					filteredCount.Add(1)
				}
			},
			OnRescanFinished: func(height int32) {
				finished.Store(true)
			},
		},
		BatchSize: 100,
	})

	actor.Start()

	// Wait for sync to complete.
	require.Eventually(t, func() bool {
		return finished.Load()
	}, 5*time.Second, 10*time.Millisecond)

	// Now simultaneously: add a block with a tx AND add the address.
	// In the old rescan, if the select picked the block first, the tx
	// would be missed forever. With the FSM + rewind, it should always
	// be found.
	txStamp, _ := chain.addBlockWithTx(addr)
	txHeader := chain.mockChainSource.blockHeaders[txStamp.Hash]

	// Send both the block notification and the AddWatchAddrs
	// concurrently.
	rewindTo := uint32(txStamp.Height - 1)

	done := make(chan struct{})
	go func() {
		chain.ntfnChan <- blockntfns.NewBlockConnected(
			*txHeader, uint32(txStamp.Height),
		)
		close(done)
	}()

	// Add the address with rewind right after (or simultaneously).
	err := actor.AddWatchAddrs(
		[]btcutil.Address{addr}, nil, &rewindTo,
	)
	require.NoError(t, err)

	<-done

	// The rewind ensures the tx is found regardless of message ordering.
	require.Eventually(t, func() bool {
		return filteredCount.Load() > 0
	}, 10*time.Second, 10*time.Millisecond,
		"RACE CONDITION TEST: tx should always be found "+
			"regardless of message ordering")

	actor.Stop()
	actor.WaitForShutdown()
}
