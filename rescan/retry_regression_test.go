package rescan

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
	neutrino "github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

type failingHeaderChain struct {
	*mockChainSource
	failHeight uint32
}

func (c *failingHeaderChain) GetBlockHeaderByHeight(
	height uint32,
) (*wire.BlockHeader, error) {

	if height == c.failHeight {
		return nil, errors.New("transient header failure")
	}
	return c.mockChainSource.GetBlockHeaderByHeight(height)
}

func TestSyncBatchRetainsCompletedPrefix(t *testing.T) {
	base := newMockChain(5)
	chain := &failingHeaderChain{
		mockChainSource: base,
		failHeight:      3,
	}
	genesis := base.ChainParams().GenesisBlock.Header
	state := &StateSyncing{
		Cursor: Cursor{
			CurStamp: headerfs.BlockStamp{
				Hash: genesis.BlockHash(),
			},
			CurHeader: genesis,
		},
	}
	env := &Environment{Chain: chain, BatchSize: 10}
	transition, err := state.ProcessEvent(
		t.Context(),
		ProcessNextBlockEvent{},
		env,
	)
	require.NoError(t, err)
	next := transition.NextState.(*StateSyncing)
	require.Equal(t, int32(2), next.CurStamp.Height)
	var heights []int32
	for _, event := range outboxOf(transition) {
		if connected, ok := event.(FilteredBlockOutbox); ok {
			heights = append(heights, connected.Height)
		}
	}
	require.Equal(t, []int32{1, 2}, heights)
	_, err = next.ProcessEvent(
		t.Context(),
		ProcessNextBlockEvent{},
		env,
	)
	require.Error(t, err)
	chain.failHeight = 0
	transition, err = next.ProcessEvent(
		t.Context(),
		ProcessNextBlockEvent{},
		env,
	)
	require.NoError(t, err)
	require.IsType(t, &StateCurrent{}, transition.NextState)
}

func TestStateSnapshotOwnsWatchScripts(t *testing.T) {
	original := &StateCurrent{
		Cursor: Cursor{
			Watch: WatchState{
				Inputs: []neutrino.InputWithScript{
					{PkScript: []byte{1, 2}},
				},
				List: [][]byte{{3, 4}},
			},
		},
	}
	snapshot := snapshotState(original).(*StateCurrent)
	snapshot.Watch.Inputs[0].PkScript[0] = 9
	snapshot.Watch.List[0][0] = 9
	require.Equal(
		t,
		byte(1),
		original.Watch.Inputs[0].PkScript[0],
	)
	require.Equal(t, byte(3), original.Watch.List[0][0])
}

type countingFilterChain struct {
	*mockChainSource
	calls int
}

func (c *countingFilterChain) GetCFilter(
	hash chainhash.Hash,
	filterType wire.FilterType,
	opts ...neutrino.QueryOption,
) (*gcs.Filter, error) {

	c.calls++
	return c.mockChainSource.GetCFilter(hash, filterType, opts...)
}

func TestCurrentRetainsBirthdayGate(t *testing.T) {
	base := newMockChain(3)
	chain := &countingFilterChain{mockChainSource: base}
	first := base.blockHeaders[*base.blockHashesByHeight[1]]
	second := base.blockHeaders[*base.blockHashesByHeight[2]]
	genesis := base.ChainParams().GenesisBlock.Header
	state := &StateCurrent{
		Cursor: Cursor{
			CurStamp: headerfs.BlockStamp{
				Hash: genesis.BlockHash(),
			},
			CurHeader: genesis,
			Watch:     WatchState{List: [][]byte{{1}}},
		},
	}
	env := &Environment{
		Chain:     chain,
		StartTime: first.Timestamp.Add(time.Second),
	}
	transition, err := state.ProcessEvent(
		t.Context(),
		BlockConnectedEvent{
			Header: *first, Height: 1,
		},
		env,
	)
	require.NoError(t, err)
	require.Zero(t, chain.calls)
	state = transition.NextState.(*StateCurrent)
	transition, err = state.ProcessEvent(
		t.Context(),
		BlockConnectedEvent{
			Header: *second, Height: 2,
		},
		env,
	)
	require.NoError(t, err)
	require.Equal(t, 1, chain.calls)
	require.True(t, transition.NextState.(*StateCurrent).Scanning)
}

func TestWatchedScriptCacheTracksUpdates(t *testing.T) {
	addr1, err := address.NewAddressPubKeyHash(
		make([]byte, 20),
		&chaincfg.MainNetParams,
	)
	require.NoError(t, err)
	key2 := make([]byte, 20)
	key2[0] = 1
	addr2, err := address.NewAddressPubKeyHash(
		key2,
		&chaincfg.MainNetParams,
	)
	require.NoError(t, err)
	watch, err := (&WatchState{}).ApplyUpdate(
		&AddWatchAddrsEvent{Addrs: []address.Address{addr1}},
	)
	require.NoError(t, err)
	updated, err := watch.ApplyUpdate(
		&AddWatchAddrsEvent{Addrs: []address.Address{addr2}},
	)
	require.NoError(t, err)
	script1, err := txscript.PayToAddrScript(addr1)
	require.NoError(t, err)
	script2, err := txscript.PayToAddrScript(addr2)
	require.NoError(t, err)
	tx := btcutil.NewTx(&wire.MsgTx{TxOut: []*wire.TxOut{
		{
			PkScript: script1,
		}, {PkScript: script2}, {PkScript: []byte{0x51}},
	}})
	matched, inputs := paysWatchedScripts(tx, updated.addrScripts)
	require.True(t, matched)
	require.Len(t, inputs, 2)
	require.Equal(t, uint32(0), inputs[0].OutPoint.Index)
	require.Equal(t, uint32(1), inputs[1].OutPoint.Index)
	_, inputs = paysWatchedScripts(tx, watch.addrScripts)
	require.Len(t, inputs, 1)
}

type shutdownFilterChain struct {
	*mockChainSource
	entered chan struct{}
	cancel  <-chan struct{}
}

func (c *shutdownFilterChain) GetCFilter(
	_ chainhash.Hash,
	_ wire.FilterType,
	_ ...neutrino.QueryOption,
) (*gcs.Filter, error) {

	close(c.entered)
	<-c.cancel
	return nil, context.Canceled
}

// TestStopCancelsBeforeWaitingForMailbox checks the shutdown ordering while a
// chain query is blocked. The ChainService tests separately verify that the
// cancellation query option interrupts production network queries.
func TestStopCancelsBeforeWaitingForMailbox(t *testing.T) {
	base := newMockChain(2)
	chain := &shutdownFilterChain{
		mockChainSource: base,
		entered:         make(chan struct{}),
	}
	genesis := base.ChainParams().GenesisBlock.Header
	actor := NewRescanActor(ActorConfig{
		Chain: chain,
		StartStamp: headerfs.BlockStamp{
			Hash: genesis.BlockHash(),
		},
		StartHeader: genesis,
		WatchInputs: []neutrino.InputWithScript{
			{PkScript: []byte{0x51}},
		},
	})
	chain.cancel = actor.ctx.Done()
	actor.Start()
	select {
	case <-chain.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("filter query did not start")
	}
	done := make(chan struct{})
	go func() {
		actor.Stop()
		actor.WaitForShutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(
			"actor did not stop while a filter query was blocked",
		)
	}
}

// TestCurrentIgnoresOlderConnect checks a notification queued during catch-up
// after the scanner has already processed a later tip.
func TestCurrentIgnoresOlderConnect(t *testing.T) {
	chain := newMockChain(4)
	tip, err := chain.BestBlock()
	require.NoError(t, err)
	state := &StateCurrent{
		Cursor: Cursor{
			CurStamp:  *tip,
			CurHeader: *chain.blockHeaders[tip.Hash],
		},
	}
	header := chain.blockHeaders[*chain.blockHashesByHeight[1]]
	transition, err := state.ProcessEvent(
		t.Context(),
		BlockConnectedEvent{
			Header: *header, Height: 1,
		},
		&Environment{Chain: chain},
	)
	require.NoError(t, err)
	require.Equal(t, state, transition.NextState)
	require.True(t, transition.NewEvents.IsNone())
}

// TestCurrentFallsBackOnTransientHeaderRead ensures an active block is not
// discarded when its canonical header lookup temporarily fails. Current falls
// back to Syncing, which scans the block from the header store once the read
// succeeds, and does not report finished a second time.
func TestCurrentFallsBackOnTransientHeaderRead(t *testing.T) {
	base := newMockChain(2)
	chain := &failingHeaderChain{
		mockChainSource: base,
		failHeight:      1,
	}
	env := &Environment{Chain: chain}
	genesis := base.ChainParams().GenesisBlock.Header
	header := *base.blockHeaders[*base.blockHashesByHeight[1]]
	state := &StateCurrent{
		Cursor: Cursor{
			CurStamp: headerfs.BlockStamp{
				Hash: genesis.BlockHash(),
			},
			CurHeader: genesis,
		},
	}
	event := BlockConnectedEvent{Header: header, Height: 1}
	transition, err := state.ProcessEvent(t.Context(), event, env)
	require.NoError(t, err)
	require.Equal(
		t, &StateSyncing{Cursor: state.Cursor, finished: true},
		transition.NextState,
	)
	require.Equal(t, []OutboxEvent{ContinueOutbox{}}, outboxOf(transition))

	chain.failHeight = math.MaxUint32
	transition, err = transition.NextState.ProcessEvent(
		t.Context(), ProcessNextBlockEvent{}, env,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		header.BlockHash(),
		transition.NextState.(*StateCurrent).CurStamp.Hash,
	)
	for _, out := range outboxOf(transition) {
		_, finished := out.(RescanFinishedOutbox)
		require.False(t, finished, "finished reported twice")
	}
}
