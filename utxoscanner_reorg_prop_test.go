package neutrino

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// utxoBranch is the content of one branch of a generated chain: whether its
// block at height 1 creates the outpoint, and the height of the block that
// spends it, or zero for none.
type utxoBranch struct {
	creates bool
	spendAt int64
}

// TestUtxoScannerReorgProperty checks that whatever reorg lands while a block
// is being fetched, of whatever depth, GetUtxo answers for the chain the
// store holds once the scan is done: the output is unknown if the active
// block at its birth height doesn't create it, spent at the height of the
// active block that spends it, and unspent otherwise.
func TestUtxoScannerReorgProperty(t *testing.T) {
	t.Parallel()

	created := wire.NewMsgTx(2)
	created.AddTxOut(wire.NewTxOut(5000, []byte{0x51}))
	outpoint := wire.OutPoint{Hash: created.TxHash()}

	spend := wire.NewMsgTx(2)
	spend.AddTxIn(&wire.TxIn{PreviousOutPoint: outpoint})
	spend.AddTxOut(wire.NewTxOut(4000, []byte{0x51}))

	rapid.Check(t, func(rt *rapid.T) {
		tip := rapid.Int64Range(2, 7).Draw(rt, "tip")

		// The fork is the highest block both branches share, zero
		// when even the birth block is replaced.
		fork := rapid.Int64Range(0, tip-1).Draw(rt, "fork")

		old := utxoBranch{creates: true}
		if rapid.Bool().Draw(rt, "oldSpends") {
			old.spendAt = rapid.Int64Range(2, tip).Draw(
				rt, "oldSpendAt",
			)
		}

		// The new branch shares everything up to the fork, and only
		// draws what lies above it.
		replacement := old
		if fork == 0 {
			replacement.creates = rapid.Bool().Draw(rt, "creates")
		}
		if old.spendAt == 0 || old.spendAt > fork {
			replacement.spendAt = 0
			low := max(fork+1, 2)
			if replacement.creates && low <= tip &&
				rapid.Bool().Draw(rt, "newSpends") {

				replacement.spendAt = rapid.Int64Range(
					low, tip,
				).Draw(rt, "newSpendAt")
			}
		}

		// The reorg lands while the old block at this height is
		// fetched.
		reorgAt := rapid.Int64Range(1, tip).Draw(rt, "reorgAt")

		block := func(b utxoBranch, height int64, prev chainhash.Hash,
			nonce uint32) *wire.MsgBlock {

			txs := []*wire.MsgTx{wire.NewMsgTx(2)}
			if height == 1 && b.creates {
				txs = append(txs, created)
			}
			if height == b.spendAt {
				txs = append(txs, spend)
			}

			return &wire.MsgBlock{
				Header: wire.BlockHeader{
					PrevBlock: prev,
					Nonce:     nonce,
				},
				Transactions: txs,
			}
		}

		chain := newSwapChain(tip)
		oldHashes := make(map[int64]chainhash.Hash)
		var prev chainhash.Hash
		for h := int64(1); h <= tip; h++ {
			prev = chain.add(h, block(old, h, prev, uint32(h)))
			oldHashes[h] = prev
		}

		// Build the replacement blocks up front, so the reorg swaps
		// them all in at once, as a rollback and reconnect do.
		var newBlocks []*wire.MsgBlock
		prev = oldHashes[fork]
		for h := fork + 1; h <= tip; h++ {
			blk := block(replacement, h, prev, uint32(100+h))
			newBlocks = append(newBlocks, blk)
			prev = blk.BlockHash()
		}

		// The reorg lands when the scanner touches the old block at
		// reorgAt, whether it fetches the block or only checks its
		// filter.
		var once sync.Once
		touch := func(hash chainhash.Hash) {
			if hash != oldHashes[reorgAt] {
				return
			}
			once.Do(func() {
				for i, blk := range newBlocks {
					chain.add(fork+1+int64(i), blk)
				}
			})
		}
		chain.onFetch = func(hash chainhash.Hash) error {
			touch(hash)
			return nil
		}

		// Filters match only blocks that create or spend the output,
		// so empty blocks take the path that skips them unfetched.
		cfg := chain.config()
		cfg.BlockFilterMatches = func(_ *rescanOptions,
			hash *chainhash.Hash) (bool, error) {

			touch(*hash)

			chain.mu.Lock()
			defer chain.mu.Unlock()

			return len(chain.blocks[*hash].Transactions) > 1, nil
		}

		report, err := runScan(t, cfg, outpoint, 1)
		require.NoError(rt, err)

		// The answer is for the chain as it ended up: the old branch
		// if the reorg never landed, the new one if it did. The one
		// exception is a spend in an old block the scanner processed
		// before the reorg landed: it was answered then, and no
		// scanner can take an answer back.
		chain.mu.Lock()
		landed := chain.canonical[tip] != oldHashes[tip]
		birthHash := chain.canonical[1]
		chain.mu.Unlock()
		require.True(rt, landed, "the reorg never landed")

		final := replacement
		if old.spendAt != 0 && old.spendAt < reorgAt {
			final = old
		}

		if !final.creates {
			require.Nil(rt, report, "reported an output the "+
				"active chain doesn't create")
			return
		}
		require.NotNil(rt, report)

		if final.spendAt == 0 {
			require.Nil(rt, report.SpendingTx, "reported a spend "+
				"the active chain doesn't have")
			require.Equal(rt, birthHash, *report.BlockHash,
				"birth block isn't the active one")
			return
		}

		require.NotNil(rt, report.SpendingTx, "missed the spend at "+
			"height %d", final.spendAt)
		require.Equal(rt, uint32(final.spendAt),
			report.SpendingTxHeight)
	})
}

// TestUtxoScannerReorgBelowCursor checks the two shapes the property test
// covers, by name: a reorg two blocks deep that lands while the scanner is
// fetching the upper block, and one that replaces the birth block after it
// was scanned.
func TestUtxoScannerReorgBelowCursor(t *testing.T) {
	t.Parallel()

	created := wire.NewMsgTx(2)
	created.AddTxOut(wire.NewTxOut(5000, []byte{0x51}))
	outpoint := wire.OutPoint{Hash: created.TxHash()}

	spend := wire.NewMsgTx(2)
	spend.AddTxIn(&wire.TxIn{PreviousOutPoint: outpoint})
	spend.AddTxOut(wire.NewTxOut(4000, []byte{0x51}))

	empty := func(prev chainhash.Hash, nonce uint32) *wire.MsgBlock {
		return &wire.MsgBlock{
			Header: wire.BlockHeader{
				PrevBlock: prev, Nonce: nonce,
			},
			Transactions: []*wire.MsgTx{wire.NewMsgTx(2)},
		}
	}

	t.Run("spend below the scan position", func(t *testing.T) {
		t.Parallel()

		chain := newSwapChain(3)
		birth := chain.add(1, &wire.MsgBlock{
			Header:       wire.BlockHeader{Nonce: 1},
			Transactions: []*wire.MsgTx{created},
		})
		old2 := chain.add(2, empty(birth, 2))
		old3 := chain.add(3, empty(old2, 3))

		new2 := &wire.MsgBlock{
			Header: wire.BlockHeader{
				PrevBlock: birth, Nonce: 20,
			},
			Transactions: []*wire.MsgTx{spend},
		}
		new3 := empty(new2.BlockHash(), 30)

		var once sync.Once
		chain.onFetch = func(hash chainhash.Hash) error {
			if hash == old3 {
				once.Do(func() {
					chain.add(2, new2)
					chain.add(3, new3)
				})
			}

			return nil
		}

		report, err := runScan(t, chain.config(), outpoint, 1)
		require.NoError(t, err)
		require.NotNil(t, report)
		require.NotNil(t, report.SpendingTx, "missed the spend in "+
			"the replaced block below the scan position")
		require.EqualValues(t, 2, report.SpendingTxHeight)
	})

	t.Run("birth block replaced", func(t *testing.T) {
		t.Parallel()

		chain := newSwapChain(2)
		old1 := chain.add(1, &wire.MsgBlock{
			Header:       wire.BlockHeader{Nonce: 1},
			Transactions: []*wire.MsgTx{created},
		})
		old2 := chain.add(2, empty(old1, 2))

		// The new birth block doesn't create the output.
		new1 := empty(chainhash.Hash{}, 10)
		new2 := empty(new1.BlockHash(), 20)

		var once sync.Once
		chain.onFetch = func(hash chainhash.Hash) error {
			if hash == old2 {
				once.Do(func() {
					chain.add(1, new1)
					chain.add(2, new2)
				})
			}

			return nil
		}

		report, err := runScan(t, chain.config(), outpoint, 1)
		require.NoError(t, err)
		require.Nil(t, report, "reported an output the active chain "+
			"doesn't create")
	})
}

// TestUtxoScannerWaitsForTip checks that a request starting above the tip
// makes the scanner wait for the tip to reach it, rather than look at the tip
// again in a loop. A reorg that sends a batch back to a birth height above
// the fork leaves the scanner in this position until the new branch's filter
// headers arrive.
func TestUtxoScannerWaitsForTip(t *testing.T) {
	t.Parallel()

	chain := newSwapChain(1)
	chain.add(1, &wire.MsgBlock{
		Transactions: []*wire.MsgTx{wire.NewMsgTx(2)},
	})

	var reads atomic.Int32
	cfg := chain.config()
	best := cfg.BestSnapshot
	cfg.BestSnapshot = func() (*headerfs.BlockStamp, error) {
		reads.Add(1)
		return best()
	}

	scanner := NewUtxoScanner(cfg)
	require.NoError(t, scanner.Start())
	t.Cleanup(func() { require.NoError(t, scanner.Stop()) })

	_, err := scanner.Enqueue(&InputWithScript{
		OutPoint: wire.OutPoint{Index: 1},
		PkScript: []byte{0x51},
	}, 5, nil)
	require.NoError(t, err)

	time.Sleep(tipPollInterval / 2)
	require.LessOrEqual(t, reads.Load(), int32(2), "the scanner kept "+
		"reading the tip while the request was above it")
}
