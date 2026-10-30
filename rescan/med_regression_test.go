package rescan

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/filtermatch"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// gatedChain is a mock chain whose IsCurrent answer the test sets, and
// whose filter fetches can be slowed down or held until the actor cancels
// them, as a production fetch honours its cancellation.
type gatedChain struct {
	*mockChainSource

	current atomic.Bool

	// delay is how long each filter fetch takes.
	delay time.Duration

	// hold makes a filter fetch wait for its context, and entered is
	// closed when the first such fetch starts.
	hold    atomic.Bool
	entered chan struct{}
}

// IsCurrent is part of the ChainSource interface.
func (c *gatedChain) IsCurrent() bool {
	return c.current.Load()
}

// GetCFilter is part of the ChainSource interface.
func (c *gatedChain) GetCFilter(ctx context.Context, hash chainhash.Hash,
	batch bool) (*gcs.Filter, error) {

	if c.hold.Load() {
		close(c.entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return c.mockChainSource.GetCFilter(ctx, hash, batch)
}

// newGatedActor returns an actor that scans chain from genesis with a
// watched input, so every block's filter is fetched, and counts its
// connects and errors.
func newGatedActor(t *testing.T, chain *gatedChain, connects,
	errs *atomic.Int32) *RescanActor {

	genesis := chain.ChainParams().GenesisBlock.Header
	return mustNewRescanActor(t, ActorConfig{
		Chain:       chain,
		StartStamp:  headerfs.BlockStamp{Hash: genesis.BlockHash()},
		StartHeader: genesis,
		WatchInputs: []filtermatch.InputWithScript{
			{PkScript: []byte{0x51}},
		},
		OnError: func(error) { errs.Add(1) },
		Callbacks: Callbacks{
			OnBlockConnected: func(*chainhash.Hash, int32,
				time.Time) {

				connects.Add(1)
			},
		},
	})
}

// TestActorWaitsForCurrentChain is a regression test: like the legacy rescan,
// the actor does not scan, report an error, or report finished until the
// chain is current.
func TestActorWaitsForCurrentChain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		chain := &gatedChain{mockChainSource: newMockChain(5)}
		var connects, errs atomic.Int32
		actor := newGatedActor(t, chain, &connects, &errs)
		actor.Start()
		defer func() { actor.Stop(); actor.WaitForShutdown() }()

		time.Sleep(10 * retryInterval)
		synctest.Wait()
		require.Zero(t, connects.Load())
		require.Zero(t, errs.Load())
		state, err := actor.CurrentState(t.Context())
		require.NoError(t, err)
		require.IsType(t, &StateInitializing{}, state)

		chain.current.Store(true)
		time.Sleep(retryInterval)
		synctest.Wait()
		require.Equal(t, int32(4), connects.Load())
		require.Zero(t, errs.Load())
	})
}

// TestActorStopDuringFetchReportsNoError is a regression test: a Stop that
// cancels a fetch in progress is not reported through OnError, and nothing
// retries the cancelled step.
func TestActorStopDuringFetchReportsNoError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		chain := &gatedChain{
			mockChainSource: newMockChain(5),
			entered:         make(chan struct{}),
		}
		chain.current.Store(true)
		chain.hold.Store(true)
		var connects, errs atomic.Int32
		actor := newGatedActor(t, chain, &connects, &errs)
		actor.Start()

		<-chain.entered
		actor.Stop()
		actor.WaitForShutdown()
		time.Sleep(10 * retryInterval)
		synctest.Wait()
		require.Zero(t, errs.Load())
		require.Zero(t, connects.Load())
	})
}

// TestActorUpdateDuringSlowBatch is a regression test: an update sent while
// a batch of slow fetches runs waits for the block in progress only, not for
// the rest of the batch, and has no fixed timeout.
func TestActorUpdateDuringSlowBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const delay = 10 * time.Second
		chain := &gatedChain{
			mockChainSource: newMockChain(50),
			delay:           delay,
		}
		chain.current.Store(true)
		var connects, errs atomic.Int32
		actor := newGatedActor(t, chain, &connects, &errs)
		actor.Start()
		defer func() { actor.Stop(); actor.WaitForShutdown() }()

		// Let the batch start, then send the update a little into
		// the third fetch.
		time.Sleep(2*delay + time.Second)
		start := time.Now()
		addr := newCallbackTestAddr(t, 7)
		require.NoError(t, addWatch(actor, WatchUpdate{
			Addrs: []address.Address{addr},
		}))
		require.LessOrEqual(t, time.Since(start), delay)
		require.Less(t, connects.Load(), int32(5))

		state, err := actor.CurrentState(t.Context())
		require.NoError(t, err)
		require.Contains(t, cursorOf(t, state).Watch.Addrs, addr)
		require.Zero(t, errs.Load())
	})
}

// Ensure gatedChain implements ChainSource.
var _ ChainSource = (*gatedChain)(nil)

// TestActorUpdateBehindNotificationBacklog is a regression test: an update
// sent while a current scan has a backlog of slow connect notifications in
// its mailbox waits for the block in progress only, as the legacy rescan's
// select did, not for every queued notification.
func TestActorUpdateBehindNotificationBacklog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const delay = 10 * time.Second
		chain := &gatedChain{mockChainSource: newMockChain(3)}
		chain.current.Store(true)
		var connects, errs atomic.Int32
		actor := newGatedActor(t, chain, &connects, &errs)
		actor.Start()
		defer func() { actor.Stop(); actor.WaitForShutdown() }()

		time.Sleep(retryInterval)
		synctest.Wait()
		require.Equal(t, int32(2), connects.Load())

		// Thirty blocks arrive at once, each with a slow filter
		// fetch.
		chain.delay = delay
		for i := 0; i < 30; i++ {
			stamp := chain.addBlock()
			header := *chain.blockHeaders[stamp.Hash]
			chain.ntfnChan <- blockntfns.NewBlockConnected(
				header, uint32(stamp.Height),
			)
		}
		time.Sleep(delay / 2)

		began := time.Now()
		require.NoError(t, addWatch(actor, WatchUpdate{
			Addrs: []address.Address{newCallbackTestAddr(t, 8)},
		}))
		require.LessOrEqual(t, time.Since(began), delay)

		// The scan still catches up with every block, in order.
		time.Sleep(40 * delay)
		synctest.Wait()
		require.Equal(t, int32(32), connects.Load())
		require.IsType(t, &StateCurrent{}, mustState(t, actor))
		require.Zero(t, errs.Load())
	})
}

// mustState returns the actor's current state.
func mustState(t *testing.T, actor *RescanActor) RescanState {
	t.Helper()

	state, err := actor.CurrentState(t.Context())
	require.NoError(t, err)

	return state
}
