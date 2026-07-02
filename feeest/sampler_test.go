package feeest

import (
	"testing"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/feedb"
	"github.com/stretchr/testify/require"
)

// buildTestBlock assembles a minimal block: a coinbase claiming the given
// fees on top of the subsidy, one transaction spending an unknown (external)
// prevout, and one transaction spending the previous transaction's output
// within the block, paying knownFee.
func buildTestBlock(t *testing.T, height uint32, params *chaincfg.Params,
	claimedFees, knownFee int64) (*btcutil.Block, *wire.MsgTx) {

	t.Helper()

	subsidy := blockchain.CalcBlockSubsidy(int32(height), params)
	coinbase := wire.NewMsgTx(wire.TxVersion)
	coinbase.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Index: 0xffffffff},
		SignatureScript:  []byte{0x01, 0x02},
	})
	coinbase.AddTxOut(&wire.TxOut{
		Value:    subsidy + claimedFees,
		PkScript: []byte{0x51},
	})

	// txA spends a prevout we cannot resolve from the block alone.
	txA := wire.NewMsgTx(wire.TxVersion)
	txA.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Hash: chainhash.Hash{0xaa}, Index: 0,
		},
	})
	txA.AddTxOut(&wire.TxOut{Value: 100_000, PkScript: []byte{0x51}})

	// txB spends txA's output inside the same block; its fee is exactly
	// computable.
	txB := wire.NewMsgTx(wire.TxVersion)
	txB.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Hash: txA.TxHash(), Index: 0,
		},
	})
	txB.AddTxOut(&wire.TxOut{
		Value:    100_000 - knownFee,
		PkScript: []byte{0x51},
	})

	msg := &wire.MsgBlock{
		Header: wire.BlockHeader{
			PrevBlock:  chainhash.Hash{0x01},
			MerkleRoot: chainhash.Hash{0x02},
		},
		Transactions: []*wire.MsgTx{coinbase, txA, txB},
	}
	return btcutil.NewBlock(msg), txB
}

// TestComputeSampleKnownTxRates confirms the sampler extracts the exact fee
// rate of intra-block spends, the coinbase weight, and the spam flag when a
// single known tx dominates the block's fees.
func TestComputeSampleKnownTxRates(t *testing.T) {
	t.Parallel()
	params := &chaincfg.RegressionNetParams

	// Total claimed fees 15k, of which 10k comes from the one computable
	// tx: it dominates (>50%), so the sample must carry FlagSpam.
	block, txB := buildTestBlock(t, 100, params, 15_000, 10_000)

	sample, err := computeSample(block, 100, params)
	require.NoError(t, err)

	require.Equal(t, uint64(15_000), sample.TotalFees)
	require.Equal(t,
		uint64(blockchain.GetBlockWeight(block)), sample.TotalWeight)
	require.Equal(t,
		uint64(blockchain.GetTransactionWeight(block.Transactions()[0])),
		sample.CoinbaseWeight)

	wantRate := uint64(10_000) * 1000 /
		uint64(blockchain.GetTransactionWeight(btcutil.NewTx(txB)))
	require.Equal(t, wantRate, sample.MinKnownTxRate)
	require.Equal(t, uint16(1), sample.KnownTxCount)
	require.NotZero(t, sample.Flags&feedb.FlagSpam)

	// The block-average rate must be computed over the non-coinbase
	// weight.
	feeWeight := sample.TotalWeight - sample.CoinbaseWeight
	require.Equal(t, uint64(15_000)*1000/feeWeight, sample.FeeRatePerKW())
}

// TestComputeSampleNoKnownTx confirms blocks without intra-block spends leave
// the known-tx fields zeroed and carry no spam flag when fees are modest.
func TestComputeSampleNoKnownTx(t *testing.T) {
	t.Parallel()
	params := &chaincfg.RegressionNetParams

	block, _ := buildTestBlock(t, 100, params, 5_000, 1_000)

	// Drop txB so only the unknown-prevout tx remains.
	msg := block.MsgBlock()
	msg.Transactions = msg.Transactions[:2]
	block = btcutil.NewBlock(msg)

	sample, err := computeSample(block, 100, params)
	require.NoError(t, err)
	require.Zero(t, sample.MinKnownTxRate)
	require.Zero(t, sample.KnownTxCount)
	require.Zero(t, sample.Flags&feedb.FlagSpam)
}
