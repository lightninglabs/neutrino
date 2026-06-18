package rescan

import (
	"context"
	"testing"
	"time"

	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// TestStateInitializingToSyncing tests that Initializing transitions to
// Syncing on ProcessNextBlockEvent and emits a SelfTellOutbox.
func TestStateInitializingToSyncing(t *testing.T) {
	chain := newMockChain(10)

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	state := &StateInitializing{
		StartStamp: headerfs.BlockStamp{
			Hash:   *genesisHash,
			Height: 0,
		},
		StartHeader: *genesisHeader,
		Watch:       WatchState{},
	}

	env := &Environment{
		Chain:     chain,
		StartTime: time.Time{},
		BatchSize: DefaultBatchSize,
	}

	transition, err := state.ProcessEvent(
		context.Background(), ProcessNextBlockEvent{}, env,
	)
	require.NoError(t, err)

	// Should transition to StateSyncing.
	_, ok := transition.NextState.(*StateSyncing)
	require.True(t, ok, "expected StateSyncing, got %T",
		transition.NextState)

	// Should emit a SelfTellOutbox with ProcessNextBlockEvent.
	require.NotNil(t, transition.Events)
	require.Len(t, transition.Events.Outbox, 1)

	selfTell, ok := transition.Events.Outbox[0].(SelfTellOutbox)
	require.True(t, ok)
	_, ok = selfTell.Event.(ProcessNextBlockEvent)
	require.True(t, ok)
}

// TestStateInitializingAddWatch tests that AddWatchAddrs in Initializing
// buffers addresses and stays in Initializing.
func TestStateInitializingAddWatch(t *testing.T) {
	state := &StateInitializing{
		Watch: WatchState{},
	}

	env := &Environment{}

	transition, err := state.ProcessEvent(
		context.Background(),
		AddWatchAddrsEvent{
			Addrs: nil, // No addresses for this test.
		},
		env,
	)
	require.NoError(t, err)

	// Should stay in Initializing.
	_, ok := transition.NextState.(*StateInitializing)
	require.True(t, ok)
}

// TestStateInitializingStop tests that StopEvent transitions to Terminal.
func TestStateInitializingStop(t *testing.T) {
	state := &StateInitializing{}
	env := &Environment{}

	transition, err := state.ProcessEvent(
		context.Background(), StopEvent{}, env,
	)
	require.NoError(t, err)

	_, ok := transition.NextState.(*StateTerminal)
	require.True(t, ok)
	require.True(t, transition.NextState.IsTerminal())
}

// TestStateSyncingCatchUp tests that Syncing processes blocks and
// transitions to Current when caught up.
func TestStateSyncingCatchUp(t *testing.T) {
	chain := newMockChain(5)

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	state := &StateSyncing{
		CurStamp: headerfs.BlockStamp{
			Hash:   *genesisHash,
			Height: 0,
		},
		CurHeader: *genesisHeader,
		Watch:     WatchState{},
		Scanning:  false,
	}

	env := &Environment{
		Chain:     chain,
		StartTime: time.Time{},
		BatchSize: 100, // Larger than chain so we finish in one batch.
	}

	transition, err := state.ProcessEvent(
		context.Background(), ProcessNextBlockEvent{}, env,
	)
	require.NoError(t, err)

	// Should transition to StateCurrent since chain is only 5 blocks.
	nextState, ok := transition.NextState.(*StateCurrent)
	require.True(t, ok, "expected StateCurrent, got %T",
		transition.NextState)

	// CurStamp should be at the chain tip.
	require.Equal(t, int32(4), nextState.CurStamp.Height)

	// Should emit StartSubscriptionOutbox and RescanFinishedOutbox.
	require.NotNil(t, transition.Events)

	var hasStartSub, hasFinished bool
	for _, out := range transition.Events.Outbox {
		switch out.(type) {
		case StartSubscriptionOutbox:
			hasStartSub = true
		case RescanFinishedOutbox:
			hasFinished = true
		}
	}
	require.True(t, hasStartSub, "expected StartSubscriptionOutbox")
	require.True(t, hasFinished, "expected RescanFinishedOutbox")
}

// TestStateSyncingBatchContinue tests that Syncing processes a batch and
// emits SelfTellOutbox when not yet current.
func TestStateSyncingBatchContinue(t *testing.T) {
	chain := newMockChain(20) // 20 blocks.

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	state := &StateSyncing{
		CurStamp: headerfs.BlockStamp{
			Hash:   *genesisHash,
			Height: 0,
		},
		CurHeader: *genesisHeader,
		Watch:     WatchState{},
		Scanning:  false,
	}

	env := &Environment{
		Chain:     chain,
		StartTime: time.Time{},
		BatchSize: 5, // Only process 5 blocks per batch.
	}

	transition, err := state.ProcessEvent(
		context.Background(), ProcessNextBlockEvent{}, env,
	)
	require.NoError(t, err)

	// Should stay in StateSyncing.
	nextState, ok := transition.NextState.(*StateSyncing)
	require.True(t, ok, "expected StateSyncing, got %T",
		transition.NextState)

	// Should have advanced by 5 blocks.
	require.Equal(t, int32(5), nextState.CurStamp.Height)

	// Last outbox event should be SelfTellOutbox.
	require.NotNil(t, transition.Events)
	lastOutbox := transition.Events.Outbox[len(transition.Events.Outbox)-1]
	_, ok = lastOutbox.(SelfTellOutbox)
	require.True(t, ok, "expected SelfTellOutbox as last event")
}

// TestStateSyncingAddWatchRewind tests that AddWatchAddrs with a rewind
// target transitions to Rewinding.
func TestStateSyncingAddWatchRewind(t *testing.T) {
	chain := newMockChain(10)

	state := &StateSyncing{
		CurStamp: headerfs.BlockStamp{
			Height: 5,
		},
		Watch:    WatchState{},
		Scanning: true,
	}

	env := &Environment{
		Chain: chain,
	}

	rewindTo := uint32(2)
	transition, err := state.ProcessEvent(
		context.Background(),
		AddWatchAddrsEvent{RewindTo: &rewindTo},
		env,
	)
	require.NoError(t, err)

	nextState, ok := transition.NextState.(*StateRewinding)
	require.True(t, ok, "expected StateRewinding, got %T",
		transition.NextState)
	require.Equal(t, uint32(2), nextState.TargetHeight)
}

// TestStateCurrentBlockConnected tests that Current processes a connected
// block and emits appropriate outbox events.
func TestStateCurrentBlockConnected(t *testing.T) {
	chain := newMockChain(10)

	// Set up state at block 9 (0-indexed, so height 9 = 10th block).
	bestBlock, err := chain.BestBlock()
	require.NoError(t, err)

	header := chain.blockHeaders[bestBlock.Hash]

	state := &StateCurrent{
		CurStamp: headerfs.BlockStamp{
			Hash:   bestBlock.Hash,
			Height: bestBlock.Height,
		},
		CurHeader: *header,
		Watch:     WatchState{},
	}

	env := &Environment{
		Chain: chain,
	}

	// Add a new block to the chain.
	newBest := chain.addBlock()
	newHeader := chain.blockHeaders[newBest.Hash]

	transition, err := state.ProcessEvent(
		context.Background(),
		BlockConnectedEvent{
			Header: *newHeader,
			Height: uint32(newBest.Height),
		},
		env,
	)
	require.NoError(t, err)

	nextState, ok := transition.NextState.(*StateCurrent)
	require.True(t, ok)
	require.Equal(t, newBest.Height, nextState.CurStamp.Height)
	require.Equal(t, newBest.Hash, nextState.CurStamp.Hash)

	// Should emit BlockConnectedOutbox.
	require.NotNil(t, transition.Events)
	var hasBlockConnected bool
	for _, out := range transition.Events.Outbox {
		if _, ok := out.(BlockConnectedOutbox); ok {
			hasBlockConnected = true
		}
	}
	require.True(t, hasBlockConnected)
}

// TestStateCurrentBlockDisconnected tests that Current handles a block
// disconnection correctly.
func TestStateCurrentBlockDisconnected(t *testing.T) {
	chain := newMockChain(10)

	bestBlock, err := chain.BestBlock()
	require.NoError(t, err)

	curHeader := chain.blockHeaders[bestBlock.Hash]
	prevHeader := chain.blockHeaders[curHeader.PrevBlock]

	state := &StateCurrent{
		CurStamp: headerfs.BlockStamp{
			Hash:   bestBlock.Hash,
			Height: bestBlock.Height,
		},
		CurHeader: *curHeader,
		Watch:     WatchState{},
	}

	env := &Environment{Chain: chain}

	transition, err := state.ProcessEvent(
		context.Background(),
		BlockDisconnectedEvent{
			Header:   *curHeader,
			Height:   uint32(bestBlock.Height),
			ChainTip: *prevHeader,
		},
		env,
	)
	require.NoError(t, err)

	nextState, ok := transition.NextState.(*StateCurrent)
	require.True(t, ok)
	require.Equal(t, bestBlock.Height-1, nextState.CurStamp.Height)

	// Should emit BlockDisconnectedOutbox.
	require.NotNil(t, transition.Events)
	var hasDisconnected bool
	for _, out := range transition.Events.Outbox {
		if _, ok := out.(BlockDisconnectedOutbox); ok {
			hasDisconnected = true
		}
	}
	require.True(t, hasDisconnected)
}

// TestStateCurrentAddWatchRewind tests that AddWatchAddrs with rewind in
// Current transitions to Rewinding and cancels subscription.
func TestStateCurrentAddWatchRewind(t *testing.T) {
	state := &StateCurrent{
		CurStamp: headerfs.BlockStamp{
			Height: 10,
		},
		Watch: WatchState{},
	}

	env := &Environment{}

	rewindTo := uint32(5)
	transition, err := state.ProcessEvent(
		context.Background(),
		AddWatchAddrsEvent{RewindTo: &rewindTo},
		env,
	)
	require.NoError(t, err)

	_, ok := transition.NextState.(*StateRewinding)
	require.True(t, ok)

	// Should emit CancelSubscriptionOutbox and SelfTellOutbox.
	require.NotNil(t, transition.Events)

	var hasCancelSub, hasSelfTell bool
	for _, out := range transition.Events.Outbox {
		switch out.(type) {
		case CancelSubscriptionOutbox:
			hasCancelSub = true
		case SelfTellOutbox:
			hasSelfTell = true
		}
	}
	require.True(t, hasCancelSub)
	require.True(t, hasSelfTell)
}

// TestStateRewindingToSyncing tests that Rewinding walks back to the target
// and transitions to Syncing.
func TestStateRewindingToSyncing(t *testing.T) {
	chain := newMockChain(10)

	// Start at height 8.
	hash := chain.blockHashesByHeight[8]
	header := chain.blockHeaders[*hash]

	state := &StateRewinding{
		CurStamp: headerfs.BlockStamp{
			Hash:   *hash,
			Height: 8,
		},
		CurHeader:    *header,
		Watch:        WatchState{},
		TargetHeight: 5,
	}

	env := &Environment{
		Chain:     chain,
		BatchSize: 100, // Large enough to finish in one batch.
	}

	transition, err := state.ProcessEvent(
		context.Background(), ProcessNextBlockEvent{}, env,
	)
	require.NoError(t, err)

	// Should transition to StateSyncing at or below target height.
	nextState, ok := transition.NextState.(*StateSyncing)
	require.True(t, ok, "expected StateSyncing, got %T",
		transition.NextState)
	require.LessOrEqual(t, nextState.CurStamp.Height, int32(5))

	// Should emit BlockDisconnectedOutbox events.
	require.NotNil(t, transition.Events)

	disconnectCount := 0
	for _, out := range transition.Events.Outbox {
		if _, ok := out.(BlockDisconnectedOutbox); ok {
			disconnectCount++
		}
	}
	require.Greater(t, disconnectCount, 0)
}

// TestStateMachineFullCycle tests the full state machine lifecycle:
// Initializing -> Syncing -> Current.
func TestStateMachineFullCycle(t *testing.T) {
	chain := newMockChain(5)

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header

	fsm := NewStateMachine(StateMachineCfg{
		ErrorReporter: &logErrorReporter{},
		InitialState: &StateInitializing{
			StartStamp: headerfs.BlockStamp{
				Hash:   *genesisHash,
				Height: 0,
			},
			StartHeader: *genesisHeader,
			Watch:       WatchState{},
		},
		Env: &Environment{
			Chain:     chain,
			StartTime: time.Time{},
			BatchSize: 100,
		},
	})

	fsm.Start()
	defer fsm.Stop()

	// Send ProcessNextBlockEvent to kick off.
	outbox, err := fsm.AskEvent(
		context.Background(), ProcessNextBlockEvent{},
	)
	require.NoError(t, err)

	// The first event transitions Initializing -> Syncing and emits a
	// SelfTell. Process the self-tell manually.
	require.NotEmpty(t, outbox)

	// Find and process the SelfTell.
	for _, out := range outbox {
		if selfTell, ok := out.(SelfTellOutbox); ok {
			outbox2, err := fsm.AskEvent(
				context.Background(), selfTell.Event,
			)
			require.NoError(t, err)

			// This should process all blocks and reach Current.
			// Check for StartSubscriptionOutbox.
			var hasStartSub bool
			for _, out2 := range outbox2 {
				if _, ok := out2.(StartSubscriptionOutbox); ok {
					hasStartSub = true
				}
			}
			require.True(t, hasStartSub,
				"expected StartSubscriptionOutbox")
		}
	}

	// Verify we're in Current state.
	state, err := fsm.CurrentState()
	require.NoError(t, err)

	_, ok := state.(*StateCurrent)
	require.True(t, ok, "expected StateCurrent, got %T", state)
}

// TestStateTerminal tests that Terminal is truly terminal.
func TestStateTerminal(t *testing.T) {
	state := &StateTerminal{}

	require.True(t, state.IsTerminal())
	require.Equal(t, "Terminal", state.String())

	transition, err := state.ProcessEvent(
		context.Background(), ProcessNextBlockEvent{}, &Environment{},
	)
	require.NoError(t, err)
	require.Equal(t, state, transition.NextState)
}
