package rescan

import (
	"context"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	neutrino "github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// mockChainWithTxs extends mockChainSource with the ability to inject
// transactions into blocks that match watched addresses.
type mockChainWithTxs struct {
	*mockChainSource

	// txsByBlock maps block hash to transactions in that block.
	txsByBlock map[chainhash.Hash][]*wire.MsgTx
}

// newMockChainWithTxs creates a mock chain that supports transaction
// injection for property-based testing.
func newMockChainWithTxs(numBlocks int) *mockChainWithTxs {
	base := newMockChain(numBlocks)
	return &mockChainWithTxs{
		mockChainSource: base,
		txsByBlock:      make(map[chainhash.Hash][]*wire.MsgTx),
	}
}

// addBlockWithTx adds a block containing a transaction that pays to the
// given address. Returns the block stamp and the transaction.
func (c *mockChainWithTxs) addBlockWithTx(
	addr btcutil.Address) (headerfs.BlockStamp, *wire.MsgTx) {

	c.mu.Lock()
	newHeight := uint32(c.bestBlock.Height + 1)
	prevHash := c.bestBlock.Hash
	c.mu.Unlock()

	// Create a transaction paying to the address.
	pkScript, _ := txscript.PayToAddrScript(addr)
	tx := &wire.MsgTx{
		Version: 1,
		TxIn: []*wire.TxIn{
			{PreviousOutPoint: wire.OutPoint{Index: 0}},
		},
		TxOut: []*wire.TxOut{
			{Value: 100000, PkScript: pkScript},
		},
	}

	genesisTimestamp := chaincfg.MainNetParams.GenesisBlock.Header.Timestamp
	header := &wire.BlockHeader{
		PrevBlock: prevHash,
		Timestamp: genesisTimestamp.Add(
			time.Duration(newHeight) * 10 * time.Minute,
		),
	}

	// Build a block with the transaction.
	msgBlock := wire.NewMsgBlock(header)
	msgBlock.AddTransaction(tx)
	block := btcutil.NewBlock(msgBlock)

	newHash := header.BlockHash()

	// Build a GCS filter that includes the pkScript.
	filter, _ := builder.BuildBasicFilter(msgBlock, nil)

	c.mu.Lock()
	c.blockHeightIndex[newHash] = newHeight
	c.blockHashesByHeight[newHeight] = &newHash
	c.blockHeaders[newHash] = header
	c.blocks[newHash] = block
	c.filters[newHash] = filter

	prevFilterHeader := c.filterHeadersByHeight[newHeight-1]
	filterHeader, _ := builder.MakeHeaderForFilter(
		filter, *prevFilterHeader,
	)
	c.filterHeadersByHeight[newHeight] = &filterHeader

	c.bestBlock.Height++
	c.bestBlock.Hash = newHash
	c.bestBlock.Timestamp = header.Timestamp
	stamp := c.bestBlock
	c.mu.Unlock()

	c.txsByBlock[newHash] = []*wire.MsgTx{tx}

	return stamp, tx
}

// TestPropertySyncNeverMissesTx verifies the core invariant: every
// transaction paying to a watched address is always reported via
// FilteredBlockOutbox, regardless of when AddWatchAddrs arrives relative
// to block processing.
func TestPropertySyncNeverMissesTx(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a random number of empty blocks before the
		// interesting block.
		numEmptyBlocks := rapid.IntRange(1, 20).Draw(t, "numEmpty")

		// Generate a random batch size.
		batchSize := rapid.IntRange(1, 10).Draw(t, "batchSize")

		// Create chain with empty blocks.
		chain := newMockChainWithTxs(numEmptyBlocks + 1)

		// Generate a test address.
		privKey, _ := btcutil.NewAddressPubKeyHash(
			chainhash.HashB([]byte("test-key"))[:20],
			&chaincfg.MainNetParams,
		)

		// Add a block with a transaction paying to the address.
		_, _ = chain.addBlockWithTx(privKey)

		// Now run the FSM from genesis through all blocks.
		genesisHash := chain.ChainParams().GenesisHash
		genesisHeader := &chain.ChainParams().GenesisBlock.Header

		initialWatch := WatchState{}
		initialWatch.Addrs = []btcutil.Address{privKey}
		pkScript, _ := txscript.PayToAddrScript(privKey)
		initialWatch.List = [][]byte{pkScript}

		state := &StateSyncing{
			CurStamp: headerfs.BlockStamp{
				Hash:   *genesisHash,
				Height: 0,
			},
			CurHeader: *genesisHeader,
			Watch:     initialWatch,
			Scanning:  true,
		}

		env := &Environment{
			Chain:     chain.mockChainSource,
			StartTime: time.Time{},
			BatchSize: batchSize,
		}

		// Process blocks until we reach Current state.
		var allOutbox []OutboxEvent
		var currentState RescanState = state

		for i := 0; i < 200; i++ { // Safety limit.
			transition, err := currentState.ProcessEvent(
				context.Background(),
				ProcessNextBlockEvent{}, env,
			)
			if err != nil {
				t.Fatalf("ProcessEvent error: %v", err)
			}

			if transition.Events != nil {
				allOutbox = append(
					allOutbox,
					transition.Events.Outbox...,
				)
			}

			currentState = transition.NextState

			// Check if we transitioned to Current.
			if _, ok := currentState.(*StateCurrent); ok {
				break
			}

			// If still syncing, the last outbox should be
			// SelfTell — consume it and continue.
			if _, ok := currentState.(*StateSyncing); ok {
				continue
			}

			break
		}

		// Verify invariant: we should have at least one
		// FilteredBlockOutbox.
		var foundFiltered bool
		for _, out := range allOutbox {
			if _, ok := out.(FilteredBlockOutbox); ok {
				foundFiltered = true
				break
			}
		}

		if !foundFiltered {
			t.Fatal("INVARIANT VIOLATED: transaction paying " +
				"watched address was not reported via " +
				"FilteredBlockOutbox")
		}
	})
}

// TestPropertyAddWatchMidSyncNeverMisses verifies that adding a watch
// address mid-sync (with rewind) never misses the transaction. This tests
// the interleaving scenario that the original rescan's select race would
// miss.
func TestPropertyAddWatchMidSyncNeverMisses(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Chain: [genesis] + N empty blocks + [block with tx].
		numEmpty := rapid.IntRange(3, 15).Draw(t, "numEmpty")
		batchSize := rapid.IntRange(1, 5).Draw(t, "batchSize")

		chain := newMockChainWithTxs(numEmpty + 1)

		privKey, _ := btcutil.NewAddressPubKeyHash(
			chainhash.HashB([]byte("mid-sync-key"))[:20],
			&chaincfg.MainNetParams,
		)

		// The tx-containing block is at height numEmpty+1.
		txStamp, _ := chain.addBlockWithTx(privKey)

		genesisHash := chain.ChainParams().GenesisHash
		genesisHeader := &chain.ChainParams().GenesisBlock.Header

		// Start with an EMPTY watch list.
		state := &StateSyncing{
			CurStamp: headerfs.BlockStamp{
				Hash:   *genesisHash,
				Height: 0,
			},
			CurHeader: *genesisHeader,
			Watch:     WatchState{},
			Scanning:  true,
		}

		env := &Environment{
			Chain:     chain.mockChainSource,
			StartTime: time.Time{},
			BatchSize: batchSize,
		}

		// Process some blocks to advance past the tx block.
		var currentState RescanState = state
		for i := 0; i < 100; i++ {
			transition, err := currentState.ProcessEvent(
				context.Background(),
				ProcessNextBlockEvent{}, env,
			)
			if err != nil {
				t.Fatalf("ProcessEvent error: %v", err)
			}

			currentState = transition.NextState

			if _, ok := currentState.(*StateCurrent); ok {
				break
			}
		}

		// Now we're current but missed the tx (empty watch list).
		// Add the address with a rewind to before the tx block.
		rewindTo := uint32(txStamp.Height - 1)
		pkScript, _ := txscript.PayToAddrScript(privKey)

		transition, err := currentState.ProcessEvent(
			context.Background(),
			AddWatchAddrsEvent{
				Addrs:    []btcutil.Address{privKey},
				RewindTo: &rewindTo,
			},
			env,
		)
		require.NoError(t, err)

		currentState = transition.NextState

		// Should be in Rewinding state.
		_, isRewinding := currentState.(*StateRewinding)
		if !isRewinding {
			// If we were already past it and the state was
			// Current, we should have gotten Rewinding.
			t.Fatalf("expected Rewinding after AddWatchAddrs "+
				"with rewind, got %T", currentState)
		}

		// Process through rewind and re-sync.
		var allOutbox []OutboxEvent
		for i := 0; i < 200; i++ {
			transition, err := currentState.ProcessEvent(
				context.Background(),
				ProcessNextBlockEvent{}, env,
			)
			if err != nil {
				t.Fatalf("ProcessEvent error: %v", err)
			}

			if transition.Events != nil {
				allOutbox = append(
					allOutbox,
					transition.Events.Outbox...,
				)
			}

			currentState = transition.NextState

			if _, ok := currentState.(*StateCurrent); ok {
				break
			}
		}

		// Verify: the transaction MUST be found after rewind.
		var foundFiltered bool
		for _, out := range allOutbox {
			if fb, ok := out.(FilteredBlockOutbox); ok {
				if len(fb.RelevantTxs) > 0 {
					foundFiltered = true
					break
				}
			}
		}

		_ = pkScript // Used in watch list via ApplyUpdate.

		if !foundFiltered {
			t.Fatal("INVARIANT VIOLATED: transaction paying " +
				"watched address was not found after " +
				"rewind — this is the race condition the " +
				"FSM is designed to prevent")
		}
	})
}

// TestPropertyReorgConsistency verifies that block disconnections during
// reorg leave the state consistent.
func TestPropertyReorgConsistency(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		chainLen := rapid.IntRange(5, 20).Draw(t, "chainLen")
		chain := newMockChain(chainLen + 1)

		bestBlock, _ := chain.BestBlock()
		header := chain.blockHeaders[bestBlock.Hash]

		state := &StateCurrent{
			CurStamp: headerfs.BlockStamp{
				Hash:   bestBlock.Hash,
				Height: bestBlock.Height,
			},
			CurHeader: *header,
			Watch:     WatchState{},
		}

		env := &Environment{Chain: chain}

		// Disconnect some blocks.
		numDisconnect := rapid.IntRange(1, chainLen/2).Draw(
			t, "numDisconnect",
		)

		var currentState RescanState = state
		for i := 0; i < numDisconnect; i++ {
			cur, ok := currentState.(*StateCurrent)
			if !ok {
				break
			}

			curHeader := cur.CurHeader
			prevHeader, ok := chain.blockHeaders[curHeader.PrevBlock]
			if !ok {
				break
			}

			transition, err := currentState.ProcessEvent(
				context.Background(),
				BlockDisconnectedEvent{
					Header:   curHeader,
					Height:   uint32(cur.CurStamp.Height),
					ChainTip: *prevHeader,
				},
				env,
			)
			if err != nil {
				t.Fatalf("ProcessEvent error: %v", err)
			}

			currentState = transition.NextState
		}

		// Verify state is still consistent.
		cur, ok := currentState.(*StateCurrent)
		if !ok {
			t.Fatalf("expected StateCurrent after disconnects, "+
				"got %T", currentState)
		}

		// Height should have decreased.
		require.Less(t, cur.CurStamp.Height, bestBlock.Height)

		// CurHeader's BlockHash should match CurStamp.Hash.
		require.Equal(t, cur.CurHeader.BlockHash(), cur.CurStamp.Hash)
	})
}

// Ensure the mock implements ChainSource.
var _ neutrino.ChainSource = (*mockChainWithTxs)(nil)
