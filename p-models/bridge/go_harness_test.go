package bridge

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightninglabs/neutrino/rescan"
	"github.com/stretchr/testify/require"
)

// TestRealActorLifecycleConformsToModelInvariants exercises the real actor
// through the bridge harness and checks a subset of the P-model invariants
// against the actual Go callbacks.
func TestRealActorLifecycleConformsToModelInvariants(t *testing.T) {
	harness, err := NewActorHarness(6, ActorHarnessConfig{
		BatchSize: 100,
	})
	require.NoError(t, err)

	harness.Start()
	require.NoError(t, harness.WaitForCurrent(5*time.Second))

	events := harness.Recorder.Events()

	errs := CheckFilteredBlockForEveryBlock(events)
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	errs = CheckBlockCallbackConsistency(events)
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	errs = CheckMonotonicFilteredProgress(events)
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	harness.StopAndWait()

	subErrs := CheckSubscriptionAlternation(
		harness.Chain.SubscriptionEvents(),
	)
	require.Empty(t, subErrs, "subscription lifecycle errors: %v", subErrs)

	require.EqualValues(t, 1, harness.Chain.SubscribeCount(),
		"expected exactly one subscription start")
	require.EqualValues(t, 1, harness.Chain.CancelCount(),
		"expected stop to cancel the active subscription")
}

// TestRealActorRewindFindsHistoricalTx proves the bridge can drive a
// model-style "add watch + rewind" scenario against the actual Go actor.
func TestRealActorRewindFindsHistoricalTx(t *testing.T) {
	chain := NewMockChain(6)

	addr, err := btcutil.NewAddressPubKeyHash(
		chainhash.HashB([]byte("bridge-rewind"))[:20],
		&chaincfg.MainNetParams,
	)
	require.NoError(t, err)

	txStamp, _, err := chain.AddBlockWithTx(addr)
	require.NoError(t, err)

	harness, err := NewActorHarnessWithChain(chain, ActorHarnessConfig{
		BatchSize: 100,
	})
	require.NoError(t, err)

	harness.Start()
	require.NoError(t, harness.WaitForCurrent(5*time.Second))

	rewindTo := uint32(txStamp.Height - 1)
	err = harness.Actor.AddWatchAddrs(
		[]btcutil.Address{addr}, nil, &rewindTo,
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, event := range harness.Recorder.Events() {
			if event.Kind == ObservedFilteredBlockConnected &&
				event.Height == txStamp.Height &&
				event.RelevantTxCount > 0 {

				return true
			}
		}

		return false
	}, 10*time.Second, 10*time.Millisecond,
		"expected rewind to rediscover the historical transaction")

	errs := CheckFilteredBlockForEveryBlock(harness.Recorder.Events())
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	errs = CheckMonotonicFilteredProgress(harness.Recorder.Events())
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	harness.StopAndWait()
}

// TestCurrentAddWatchNoOpRewindDoesNotRestartSubscription checks a direct
// model/code consistency rule: in the P model, a rewind request at or above
// the current height is a no-op while Current. The real actor should not
// cancel and recreate its subscription for that case.
func TestCurrentAddWatchNoOpRewindDoesNotRestartSubscription(t *testing.T) {
	harness, err := NewActorHarness(6, ActorHarnessConfig{
		BatchSize: 100,
	})
	require.NoError(t, err)

	harness.Start()
	require.NoError(t, harness.WaitForCurrent(5*time.Second))

	initialSubscribeCount := harness.Chain.SubscribeCount()
	initialCancelCount := harness.Chain.CancelCount()
	initialFinishedCount := harness.Recorder.FinishedCount()

	// Use a no-op rewind target equal to the current tip.
	state, err := harness.Actor.CurrentState()
	require.NoError(t, err)

	cur, ok := state.(*rescan.StateCurrent)
	require.True(t, ok)

	addr, err := btcutil.NewAddressPubKeyHash(
		chainhash.HashB([]byte("bridge-noop-rewind"))[:20],
		&chaincfg.MainNetParams,
	)
	require.NoError(t, err)

	rewindTo := uint32(cur.CurStamp.Height)
	err = harness.Actor.AddWatchAddrs(
		[]btcutil.Address{addr}, nil, &rewindTo,
	)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	require.Equal(t, initialSubscribeCount, harness.Chain.SubscribeCount(),
		"no-op rewind should not restart subscription")
	require.Equal(t, initialCancelCount, harness.Chain.CancelCount(),
		"no-op rewind should not cancel subscription")
	require.Equal(t, initialFinishedCount, harness.Recorder.FinishedCount(),
		"no-op rewind should not emit an extra rescan-finished callback")

	harness.StopAndWait()
}

// TestBlockCallbacksPreserveHeaderIdentity verifies that the actor dispatches
// both block callbacks for the same concrete block even if the chain's
// height-based lookup changes between them.
func TestBlockCallbacksPreserveHeaderIdentity(t *testing.T) {
	harness, err := NewActorHarness(3, ActorHarnessConfig{
		BatchSize: 100,
	})
	require.NoError(t, err)

	var rewrote atomic.Bool
	harness.Recorder.SetOnObserved(func(event ObservedEvent) {
		if event.Kind != ObservedFilteredBlockConnected ||
			event.Height != 1 ||
			rewrote.Swap(true) {

			return
		}

		err := harness.Chain.RewriteHeaderAtHeight(uint32(event.Height))
		require.NoError(t, err)
	})

	harness.Start()
	require.NoError(t, harness.WaitForCurrent(5*time.Second))

	errs := CheckBlockCallbackConsistency(harness.Recorder.Events())
	require.Empty(t, errs, "callback consistency errors: %v", errs)

	harness.StopAndWait()
}

// TestRecoveredBlockRetryDoesNotReError ensures a block that was recovered via
// rewind/sync does not later trip the retry timer as a stale error.
func TestRecoveredBlockRetryDoesNotReError(t *testing.T) {
	chain := NewMockChain(3)

	var errorCount atomic.Int32
	dummyAddr, err := btcutil.NewAddressPubKeyHash(
		chainhash.HashB([]byte("bridge-retry-dummy"))[:20],
		&chaincfg.MainNetParams,
	)
	require.NoError(t, err)

	harness, err := NewActorHarnessWithChain(chain, ActorHarnessConfig{
		BatchSize:  100,
		WatchAddrs: []btcutil.Address{dummyAddr},
		OnError: func(err error) {
			errorCount.Add(1)
		},
	})
	require.NoError(t, err)

	harness.Start()
	require.NoError(t, harness.WaitForCurrent(5*time.Second))

	newTip := chain.AddBlock()
	require.NoError(t, chain.FailFilterFetches(uint32(newTip.Height), 1))
	require.NoError(t, chain.NotifyBlockConnected(uint32(newTip.Height)))

	require.Eventually(t, func() bool {
		return errorCount.Load() >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"expected the injected filter failure to hit the actor retry path")

	rewindTo := uint32(newTip.Height - 2)
	err = harness.Actor.AddWatchAddrs(nil, nil, &rewindTo)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		state, err := harness.Actor.CurrentState()
		if err != nil {
			return false
		}

		cur, ok := state.(*rescan.StateCurrent)
		return ok && cur.CurStamp.Height == newTip.Height
	}, 5*time.Second, 10*time.Millisecond,
		"expected rewind/sync path to recover the failed block")

	stableErrors := errorCount.Load()
	time.Sleep(3500 * time.Millisecond)

	require.Equal(t, stableErrors, errorCount.Load(),
		"stale retry should not re-error after block was recovered")

	errs := CheckMonotonicFilteredProgress(harness.Recorder.Events())
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	harness.StopAndWait()
}

// TestDuplicateCurrentTipNotificationIsIgnored ensures that replaying the
// current tip through the notification bridge does not emit duplicate forward
// callbacks or kick the actor into an error/retry loop.
func TestDuplicateCurrentTipNotificationIsIgnored(t *testing.T) {
	var errorCount atomic.Int32

	harness, err := NewActorHarness(4, ActorHarnessConfig{
		BatchSize: 100,
		OnError: func(err error) {
			errorCount.Add(1)
		},
	})
	require.NoError(t, err)

	harness.Start()
	require.NoError(t, harness.WaitForCurrent(5*time.Second))

	state, err := harness.Actor.CurrentState()
	require.NoError(t, err)

	cur, ok := state.(*rescan.StateCurrent)
	require.True(t, ok)

	initialEvents := len(harness.Recorder.Events())
	initialFinishedCount := harness.Recorder.FinishedCount()

	require.NoError(t, harness.Chain.NotifyBlockConnected(
		uint32(cur.CurStamp.Height),
	))

	time.Sleep(200 * time.Millisecond)

	require.Zero(t, errorCount.Load(),
		"duplicate current-tip notification should be ignored")
	require.Len(t, harness.Recorder.Events(), initialEvents,
		"duplicate current-tip notification should not emit callbacks")
	require.Equal(t, initialFinishedCount, harness.Recorder.FinishedCount(),
		"duplicate current-tip notification should not emit rescan-finished")

	nextTip := harness.Chain.AddBlock()
	require.NoError(t, harness.Chain.NotifyBlockConnected(
		uint32(nextTip.Height),
	))

	require.Eventually(t, func() bool {
		events := harness.Recorder.Events()
		if len(events) == 0 {
			return false
		}

		last := events[len(events)-1]
		return last.Kind == ObservedBlockConnected &&
			last.Height == nextTip.Height
	}, 5*time.Second, 10*time.Millisecond,
		"expected actor to keep advancing after duplicate notification")

	errs := CheckFilteredBlockForEveryBlock(harness.Recorder.Events())
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	errs = CheckBlockCallbackConsistency(harness.Recorder.Events())
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	errs = CheckMonotonicFilteredProgress(harness.Recorder.Events())
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	harness.StopAndWait()
}

// TestSubscriptionLifecycleAlternatesAcrossRewind proves that the real actor
// preserves strict start/cancel ordering through a rewind cycle and shutdown.
func TestSubscriptionLifecycleAlternatesAcrossRewind(t *testing.T) {
	harness, err := NewActorHarness(6, ActorHarnessConfig{
		BatchSize: 100,
	})
	require.NoError(t, err)

	harness.Start()
	require.NoError(t, harness.WaitForCurrent(5*time.Second))

	state, err := harness.Actor.CurrentState()
	require.NoError(t, err)

	cur, ok := state.(*rescan.StateCurrent)
	require.True(t, ok)

	initialFinished := harness.Recorder.FinishedCount()
	rewindTo := uint32(2)

	err = harness.Actor.AddWatchAddrs(nil, nil, &rewindTo)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return harness.Recorder.FinishedCount() >= initialFinished+1
	}, 5*time.Second, 10*time.Millisecond,
		"expected rewind cycle to finish and restart subscription")

	harness.StopAndWait()

	subEvents := harness.Chain.SubscriptionEvents()
	subErrs := CheckSubscriptionAlternation(subEvents)
	require.Empty(t, subErrs, "subscription lifecycle errors: %v", subErrs)

	require.Len(t, subEvents, 4,
		"expected start, cancel, start, cancel across rewind and stop")
	require.Equal(t, SubscriptionStarted, subEvents[0].Kind)
	require.Equal(t, uint32(cur.CurStamp.Height), subEvents[0].Height)
	require.Equal(t, SubscriptionCanceled, subEvents[1].Kind)
	require.Equal(t, SubscriptionStarted, subEvents[2].Kind)
	require.Equal(t, uint32(cur.CurStamp.Height), subEvents[2].Height)
	require.Equal(t, SubscriptionCanceled, subEvents[3].Kind)
}

// TestDisconnectCallbacksStayPairedAcrossReattach proves the real actor emits
// both disconnect callbacks for the same block and can resume forward progress
// after the tip is detached and reattached through the bridge.
func TestDisconnectCallbacksStayPairedAcrossReattach(t *testing.T) {
	harness, err := NewActorHarness(6, ActorHarnessConfig{
		BatchSize: 100,
	})
	require.NoError(t, err)

	harness.Start()
	require.NoError(t, harness.WaitForCurrent(5*time.Second))

	state, err := harness.Actor.CurrentState()
	require.NoError(t, err)

	cur, ok := state.(*rescan.StateCurrent)
	require.True(t, ok)

	tipHeight := uint32(cur.CurStamp.Height)

	require.NoError(t, harness.Chain.NotifyBlockDisconnected(tipHeight))

	require.Eventually(t, func() bool {
		state, err := harness.Actor.CurrentState()
		if err != nil {
			return false
		}

		cur, ok := state.(*rescan.StateCurrent)
		return ok && cur.CurStamp.Height == int32(tipHeight-1)
	}, 5*time.Second, 10*time.Millisecond,
		"expected actor to rewind one block after disconnect")

	require.NoError(t, harness.Chain.NotifyBlockConnected(tipHeight))

	require.Eventually(t, func() bool {
		state, err := harness.Actor.CurrentState()
		if err != nil {
			return false
		}

		cur, ok := state.(*rescan.StateCurrent)
		return ok && cur.CurStamp.Height == int32(tipHeight)
	}, 5*time.Second, 10*time.Millisecond,
		"expected actor to reattach the disconnected block")

	errs := CheckDisconnectCallbackConsistency(harness.Recorder.Events())
	require.Empty(t, errs, "disconnect callback consistency errors: %v", errs)

	errs = CheckFilteredBlockForEveryBlock(harness.Recorder.Events())
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	errs = CheckBlockCallbackConsistency(harness.Recorder.Events())
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	errs = CheckMonotonicFilteredProgress(harness.Recorder.Events())
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	harness.StopAndWait()
}

// TestSubscriptionClosureResubscribesAndRecoversBacklog proves the real actor
// can recover when the subscription channel dies unexpectedly while Current.
// The chain advances while the actor is blind, so successful recovery requires
// a fresh subscription plus backlog delivery from the current height.
func TestSubscriptionClosureResubscribesAndRecoversBacklog(t *testing.T) {
	var errorCount atomic.Int32

	harness, err := NewActorHarness(6, ActorHarnessConfig{
		BatchSize: 100,
		OnError: func(err error) {
			errorCount.Add(1)
		},
	})
	require.NoError(t, err)

	harness.Start()
	require.NoError(t, harness.WaitForCurrent(5*time.Second))

	initialSubscribeCount := harness.Chain.SubscribeCount()
	newTip := harness.Chain.CloseActiveSubscriptionAndAddBlock()

	require.Eventually(t, func() bool {
		return harness.Chain.SubscribeCount() == initialSubscribeCount+1
	}, 5*time.Second, 10*time.Millisecond,
		"expected actor to restart the subscription after unexpected closure")

	require.Eventually(t, func() bool {
		return errorCount.Load() >= 1
	}, 5*time.Second, 10*time.Millisecond,
		"expected unexpected subscription closure to surface as an actor error")

	require.Eventually(t, func() bool {
		state, err := harness.Actor.CurrentState()
		if err != nil {
			return false
		}

		cur, ok := state.(*rescan.StateCurrent)
		if !ok || cur.CurStamp.Height != newTip.Height {
			return false
		}

		events := harness.Recorder.Events()
		if len(events) == 0 {
			return false
		}

		last := events[len(events)-1]
		return last.Kind == ObservedBlockConnected &&
			last.Height == newTip.Height
	}, 5*time.Second, 10*time.Millisecond,
		"expected actor to recover backlog from the restarted subscription")

	errs := CheckFilteredBlockForEveryBlock(harness.Recorder.Events())
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	errs = CheckBlockCallbackConsistency(harness.Recorder.Events())
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	errs = CheckMonotonicFilteredProgress(harness.Recorder.Events())
	require.Empty(t, errs, "bridge invariant errors: %v", errs)

	harness.StopAndWait()
}
