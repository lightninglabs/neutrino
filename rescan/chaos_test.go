package rescan

import (
	"sync"
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

// TestChaosRapidAddWatchDuringSync tests that multiple goroutines calling
// AddWatchAddrs concurrently during sync don't cause races or missed
// transactions.
func TestChaosRapidAddWatchDuringSync(t *testing.T) {
	chain := newMockChain(50)

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	var (
		finished atomic.Bool
		mu       sync.Mutex
		addrs    []btcutil.Address
	)

	// Generate addresses we'll add concurrently.
	for i := 0; i < 10; i++ {
		key := chainhash.HashB([]byte{byte(i), 0xca, 0xfe})
		addr, _ := btcutil.NewAddressPubKeyHash(
			key[:20], &chaincfg.MainNetParams,
		)
		addrs = append(addrs, addr)
	}

	actor := NewRescanActor(ActorConfig{
		Chain: chain,
		StartStamp: headerfs.BlockStamp{
			Hash:   *genesisHash,
			Height: 0,
		},
		StartHeader: *genesisHeader,
		StartTime:   time.Time{},
		Callbacks: Callbacks{
			OnRescanFinished: func(height int32) {
				finished.Store(true)
			},
		},
		BatchSize: 5, // Small batches to maximize interleaving.
	})

	actor.Start()

	// Concurrently add addresses from multiple goroutines while the
	// actor is syncing.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Small random delay to vary timing.
			time.Sleep(time.Duration(idx) * time.Millisecond)

			mu.Lock()
			addr := addrs[idx]
			mu.Unlock()

			err := actor.AddWatchAddrs(
				[]btcutil.Address{addr}, nil, nil,
			)
			if err != nil {
				t.Errorf("AddWatchAddrs failed: %v", err)
			}
		}(i)
	}

	// Wait for sync to finish.
	require.Eventually(t, func() bool {
		return finished.Load()
	}, 10*time.Second, 10*time.Millisecond,
		"actor should finish syncing")

	// Wait for all concurrent AddWatchAddrs to complete.
	wg.Wait()

	// Verify the actor is in Current state and has addresses.
	state, err := actor.CurrentState()
	require.NoError(t, err)

	cur, ok := state.(*StateCurrent)
	require.True(t, ok, "expected StateCurrent, got %T", state)

	// All 10 addresses should be in the watch list.
	require.GreaterOrEqual(t, len(cur.Watch.Addrs), 10,
		"all addresses should be in watch list")

	actor.Stop()
	actor.WaitForShutdown()
}

// TestChaosBlockNotificationBurst tests handling of rapid block
// notifications through the subscription bridge.
func TestChaosBlockNotificationBurst(t *testing.T) {
	chain := newMockChain(5)

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	var (
		finished        atomic.Bool
		blocksProcessed atomic.Int32
	)

	actor := NewRescanActor(ActorConfig{
		Chain: chain,
		StartStamp: headerfs.BlockStamp{
			Hash:   *genesisHash,
			Height: 0,
		},
		StartHeader: *genesisHeader,
		StartTime:   time.Time{},
		Callbacks: Callbacks{
			OnBlockConnected: func(height int32,
				header *wire.BlockHeader,
				timestamp time.Time) {

				blocksProcessed.Add(1)
			},
			OnRescanFinished: func(height int32) {
				finished.Store(true)
			},
		},
		BatchSize: 100,
	})

	actor.Start()

	// Wait for initial sync.
	require.Eventually(t, func() bool {
		return finished.Load()
	}, 5*time.Second, 10*time.Millisecond)

	initialBlocks := blocksProcessed.Load()

	// Now burst 20 blocks through the subscription channel.
	for i := 0; i < 20; i++ {
		newBest := chain.addBlock()
		newHeader := chain.blockHeaders[newBest.Hash]
		chain.ntfnChan <- blockntfns.NewBlockConnected(
			*newHeader, uint32(newBest.Height),
		)
	}

	// Wait for all blocks to be processed.
	require.Eventually(t, func() bool {
		return blocksProcessed.Load() >= initialBlocks+20
	}, 10*time.Second, 10*time.Millisecond,
		"all burst blocks should be processed")

	// Verify state is at the new tip.
	state, err := actor.CurrentState()
	require.NoError(t, err)

	cur, ok := state.(*StateCurrent)
	require.True(t, ok)

	bestBlock, _ := chain.BestBlock()
	require.Equal(t, bestBlock.Height, cur.CurStamp.Height)

	actor.Stop()
	actor.WaitForShutdown()
}

// TestChaosAddWatchWithRewindDuringCurrent tests that adding an address
// with a rewind while in Current state correctly rewinds and re-syncs.
func TestChaosAddWatchWithRewindDuringCurrent(t *testing.T) {
	chain := newMockChainWithTxs(10)

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	// Add a block with a tx at height 11.
	addr, _ := btcutil.NewAddressPubKeyHash(
		chainhash.HashB([]byte("chaos-rewind"))[:20],
		&chaincfg.MainNetParams,
	)
	chain.addBlockWithTx(addr)

	var (
		finished       atomic.Bool
		filteredBlocks atomic.Int32
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
					filteredBlocks.Add(1)
				}
			},
			OnRescanFinished: func(height int32) {
				finished.Store(true)
			},
		},
		BatchSize: 100,
	})

	actor.Start()

	// Wait for initial sync (no addresses watched, so no matches).
	require.Eventually(t, func() bool {
		return finished.Load()
	}, 5*time.Second, 10*time.Millisecond)

	require.Equal(t, int32(0), filteredBlocks.Load(),
		"should have no matches initially")

	// Now add the address with a rewind to height 9 (before the tx
	// block at height 11).
	rewindTo := uint32(9)
	pkScript, _ := txscript.PayToAddrScript(addr)
	_ = pkScript

	err := actor.AddWatchAddrs(
		[]btcutil.Address{addr}, nil, &rewindTo,
	)
	require.NoError(t, err)

	// Wait for the rewind and re-sync to find the tx.
	require.Eventually(t, func() bool {
		return filteredBlocks.Load() > 0
	}, 10*time.Second, 10*time.Millisecond,
		"should find the tx after rewind")

	actor.Stop()
	actor.WaitForShutdown()
}

// TestChaosStopDuringSync tests graceful shutdown while syncing.
func TestChaosStopDuringSync(t *testing.T) {
	// Create a large chain to ensure we're mid-sync when we stop.
	chain := newMockChain(500)

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	actor := NewRescanActor(ActorConfig{
		Chain: chain,
		StartStamp: headerfs.BlockStamp{
			Hash:   *genesisHash,
			Height: 0,
		},
		StartHeader: *genesisHeader,
		StartTime:   time.Time{},
		Callbacks:   Callbacks{},
		BatchSize:   10, // Small batches so we stop mid-sync.
	})

	actor.Start()

	// Give a small window for processing to start, then stop.
	time.Sleep(50 * time.Millisecond)

	// Stop should not hang.
	done := make(chan struct{})
	go func() {
		actor.Stop()
		actor.WaitForShutdown()
		close(done)
	}()

	select {
	case <-done:
		// Success — shutdown completed.
	case <-time.After(5 * time.Second):
		t.Fatal("actor shutdown timed out — possible deadlock")
	}
}
