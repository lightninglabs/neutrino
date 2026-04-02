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

type invalidTestAddress struct{}

func (invalidTestAddress) String() string { return "invalid-test-address" }

func (invalidTestAddress) EncodeAddress() string {
	return "invalid-test-address"
}

func (invalidTestAddress) ScriptAddress() []byte { return nil }

func (invalidTestAddress) IsForNet(*chaincfg.Params) bool { return true }

func newActorReceiveHarness(t *testing.T) *RescanActor {
	t.Helper()

	chain := newMockChain(3)
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
		BatchSize:   100,
	})

	actor.fsm.Start()
	t.Cleanup(func() {
		actor.cancel()
		actor.fsm.Stop()
	})

	return actor
}

// TestRescanActorReceiveProcessNextBlock exercises the actor behavior directly,
// independent of the runtime mailbox, to verify message-to-FSM translation.
func TestRescanActorReceiveProcessNextBlock(t *testing.T) {
	actor := newActorReceiveHarness(t)

	result := actor.Receive(t.Context(), &processNextBlockMsg{})
	require.True(t, result.IsOk())

	stateResult := actor.Receive(t.Context(), &currentStateReq{})
	require.True(t, stateResult.IsOk())

	resp, ok := stateResult.UnwrapOr(nil).(*currentStateResp)
	require.True(t, ok)
	_, ok = resp.State.(*StateSyncing)
	require.True(t, ok, "expected StateSyncing, got %T", resp.State)
}

// TestRescanActorReceiveAddWatchAddrs exercises the actor behavior directly to
// verify the typed actor message updates FSM state as expected.
func TestRescanActorReceiveAddWatchAddrs(t *testing.T) {
	actor := newActorReceiveHarness(t)

	addr, err := btcutil.NewAddressPubKeyHash(
		chainhash.HashB([]byte("receive-test"))[:20],
		&chaincfg.MainNetParams,
	)
	require.NoError(t, err)

	result := actor.Receive(
		t.Context(), newAddWatchAddrsMsg(
			[]btcutil.Address{addr}, nil, nil,
		),
	)
	require.True(t, result.IsOk())

	stateResult := actor.Receive(t.Context(), &currentStateReq{})
	require.True(t, stateResult.IsOk())

	resp, ok := stateResult.UnwrapOr(nil).(*currentStateResp)
	require.True(t, ok)

	state, ok := resp.State.(*StateInitializing)
	require.True(t, ok, "expected StateInitializing, got %T", resp.State)
	require.Len(t, state.Watch.Addrs, 1)
}

// TestRescanActorLifecycle tests the full actor lifecycle: create, start,
// sync, and stop.
func TestRescanActorLifecycle(t *testing.T) {
	chain := newMockChain(10)

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	var (
		blocksConnected atomic.Int32
		finished        atomic.Bool
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

				blocksConnected.Add(1)
			},
			OnRescanFinished: func(height int32) {
				finished.Store(true)
			},
		},
		BatchSize: 100,
	})

	actor.Start()

	// Wait for the actor to finish syncing.
	require.Eventually(t, func() bool {
		return finished.Load()
	}, 5*time.Second, 10*time.Millisecond,
		"actor should finish syncing")

	// Should have connected blocks from genesis to tip.
	require.Greater(t, blocksConnected.Load(), int32(0),
		"should have connected at least 1 block")

	// Should be in Current state.
	state, err := actor.CurrentState()
	require.NoError(t, err)
	_, ok := state.(*StateCurrent)
	require.True(t, ok, "expected StateCurrent, got %T", state)

	actor.Stop()
	actor.WaitForShutdown()
}

// TestRescanActorAddWatchAddrs tests adding addresses while the actor is
// running.
func TestRescanActorAddWatchAddrs(t *testing.T) {
	chain := newMockChain(5)

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	var finished atomic.Bool

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
		BatchSize: 100,
	})

	actor.Start()

	// Wait for sync to complete.
	require.Eventually(t, func() bool {
		return finished.Load()
	}, 5*time.Second, 10*time.Millisecond)

	// Add a watch address (no rewind).
	addr, _ := btcutil.NewAddressPubKeyHash(
		chainhash.HashB([]byte("test-addr"))[:20],
		&chaincfg.MainNetParams,
	)
	err := actor.AddWatchAddrs([]btcutil.Address{addr}, nil, nil)
	require.NoError(t, err)

	// Wait for the address to appear in state.
	require.Eventually(t, func() bool {
		state, err := actor.CurrentState()
		if err != nil {
			return false
		}
		cur, ok := state.(*StateCurrent)
		if !ok {
			return false
		}
		return len(cur.Watch.Addrs) == 1
	}, 5*time.Second, 10*time.Millisecond,
		"watch list should include the new address")

	actor.Stop()
	actor.WaitForShutdown()
}

// TestRescanActorAddWatchAddrsPropagatesError verifies that mailbox-backed
// AddWatchAddrs requests surface FSM errors to the caller.
func TestRescanActorAddWatchAddrsPropagatesError(t *testing.T) {
	chain := newMockChain(1)

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
		BatchSize:   100,
	})

	actor.Start()
	t.Cleanup(func() {
		actor.Stop()
		actor.WaitForShutdown()
	})

	err := actor.AddWatchAddrs(
		[]btcutil.Address{invalidTestAddress{}}, nil, nil,
	)
	require.Error(t, err)
}

// TestRescanActorWithTxMatch tests that the actor correctly reports
// matched transactions.
func TestRescanActorWithTxMatch(t *testing.T) {
	chain := newMockChainWithTxs(5)

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	// Create a watched address and add a block with a matching tx.
	addr, _ := btcutil.NewAddressPubKeyHash(
		chainhash.HashB([]byte("match-test"))[:20],
		&chaincfg.MainNetParams,
	)
	chain.addBlockWithTx(addr)

	pkScript, _ := txscript.PayToAddrScript(addr)

	var (
		filteredBlocks atomic.Int32
		finished       atomic.Bool
	)

	actor := NewRescanActor(ActorConfig{
		Chain: chain.mockChainSource,
		StartStamp: headerfs.BlockStamp{
			Hash:   *genesisHash,
			Height: 0,
		},
		StartHeader: *genesisHeader,
		StartTime:   time.Time{},
		WatchAddrs:  []btcutil.Address{addr},
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

	_ = pkScript // Used implicitly via WatchAddrs → PayToAddrScript.

	actor.Start()

	require.Eventually(t, func() bool {
		return finished.Load()
	}, 5*time.Second, 10*time.Millisecond)

	// Should have found the matching transaction.
	require.Greater(t, filteredBlocks.Load(), int32(0),
		"should have found at least 1 filtered block")

	actor.Stop()
	actor.WaitForShutdown()
}

// TestRescanActorBridgeBlockNotification tests that block notifications
// from the subscription bridge are processed correctly.
func TestRescanActorBridgeBlockNotification(t *testing.T) {
	chain := newMockChain(5)

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	var (
		finished        atomic.Bool
		blocksConnected atomic.Int32
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

				blocksConnected.Add(1)
			},
			OnRescanFinished: func(height int32) {
				finished.Store(true)
			},
		},
		BatchSize: 100,
	})

	actor.Start()

	require.Eventually(t, func() bool {
		return finished.Load()
	}, 5*time.Second, 10*time.Millisecond)

	// Now add a block and send it through the subscription channel.
	newBest := chain.addBlock()
	newHeader := chain.blockHeaders[newBest.Hash]

	// Send block notification through the mock subscription channel
	// using the real blockntfns type.
	chain.ntfnChan <- blockntfns.NewBlockConnected(
		*newHeader, uint32(newBest.Height),
	)

	// Wait for the block to be processed.
	require.Eventually(t, func() bool {
		state, _ := actor.CurrentState()
		if cur, ok := state.(*StateCurrent); ok {
			return cur.CurStamp.Height == newBest.Height
		}
		return false
	}, 5*time.Second, 10*time.Millisecond,
		"actor should process the bridge notification")

	actor.Stop()
	actor.WaitForShutdown()
}

// TestStateStrings tests that all states have correct String() output.
func TestStateStrings(t *testing.T) {
	require.Equal(t, "Initializing", (&StateInitializing{}).String())
	require.Equal(t, "Syncing", (&StateSyncing{}).String())
	require.Equal(t, "Current", (&StateCurrent{}).String())
	require.Equal(t, "Rewinding", (&StateRewinding{}).String())
	require.Equal(t, "Terminal", (&StateTerminal{}).String())
}

// TestStateIsTerminal tests that only Terminal state is terminal.
func TestStateIsTerminal(t *testing.T) {
	require.False(t, (&StateInitializing{}).IsTerminal())
	require.False(t, (&StateSyncing{}).IsTerminal())
	require.False(t, (&StateCurrent{}).IsTerminal())
	require.False(t, (&StateRewinding{}).IsTerminal())
	require.True(t, (&StateTerminal{}).IsTerminal())
}

// TestRewindingAddWatchLowerTarget tests that AddWatchAddrs in Rewinding
// state can lower the rewind target.
func TestRewindingAddWatchLowerTarget(t *testing.T) {
	state := &StateRewinding{
		CurStamp: headerfs.BlockStamp{
			Height: 10,
		},
		Watch:        WatchState{},
		TargetHeight: 5,
	}

	env := &Environment{}

	// Add watch with a lower rewind target.
	newTarget := uint32(2)
	transition, err := state.ProcessEvent(
		nil,
		AddWatchAddrsEvent{RewindTo: &newTarget},
		env,
	)
	require.NoError(t, err)

	next, ok := transition.NextState.(*StateRewinding)
	require.True(t, ok)
	require.Equal(t, uint32(2), next.TargetHeight)
}

// TestWatchStateClone tests that Clone produces an independent copy.
func TestWatchStateClone(t *testing.T) {
	original := WatchState{
		Addrs: []btcutil.Address{},
		List:  [][]byte{{0x01, 0x02}},
	}

	clone := original.Clone()

	// Modify clone and verify original is unaffected.
	clone.List = append(clone.List, []byte{0x03})
	require.Len(t, original.List, 1)
	require.Len(t, clone.List, 2)
}
