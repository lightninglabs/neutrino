package filtermatch

import (
	"testing"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// TestVerifyRejectsAnyMissingOutput checks, for random blocks, that the
// honest BIP-158 filter verifies, and that a filter missing the script of any
// one output, the coinbase's included, does not.
func TestVerifyRejectsAnyMissingOutput(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		numTxs := rapid.IntRange(1, 4).Draw(rt, "num_txs")

		block := wire.NewMsgBlock(&wire.BlockHeader{
			Nonce: rapid.Uint32().Draw(rt, "nonce"),
		})
		var scripts [][]byte
		for i := 0; i < numTxs; i++ {
			tx := wire.NewMsgTx(2)
			tx.AddTxIn(&wire.TxIn{
				PreviousOutPoint: wire.OutPoint{
					Index: uint32(i),
				},
			})
			numOuts := rapid.IntRange(1, 3).Draw(rt, "num_outs")
			for j := 0; j < numOuts; j++ {
				// A P2WSH script with a random program, so
				// every script is distinct.
				program := rapid.SliceOfN(
					rapid.Byte(), 32, 32,
				).Draw(rt, "program")
				script := append([]byte{0x00, 0x20}, program...)
				tx.AddTxOut(wire.NewTxOut(1, script))
				scripts = append(scripts, script)
			}
			require.NoError(rt, block.AddTransaction(tx))
		}
		blk := btcutil.NewBlock(block)
		key := builder.DeriveKey(blk.Hash())

		build := func(entries [][]byte) *gcs.Filter {
			filter, err := gcs.BuildGCSFilter(
				builder.DefaultP, builder.DefaultM, key,
				entries,
			)
			require.NoError(rt, err)

			return filter
		}

		_, err := VerifyBasicBlockFilter(build(scripts), blk)
		require.NoError(rt, err)

		missing := rapid.IntRange(0, len(scripts)-1).Draw(
			rt, "missing",
		)
		var partial [][]byte
		partial = append(partial, scripts[:missing]...)
		partial = append(partial, scripts[missing+1:]...)
		if len(partial) == 0 {
			return
		}

		// A false positive could still match the left out script, but
		// at P=19 that is vanishingly rare for these sizes.
		_, err = VerifyBasicBlockFilter(build(partial), blk)
		require.Error(rt, err, "filter missing output %d verified",
			missing)
	})
}
