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

// memStore is an in-memory FeeSampleStore for sampler and estimator tests,
// avoiding the database dance when we only care about the window.
type memStore struct {
	samples []*feedb.FeeSample
}

func (m *memStore) PutSample(s *feedb.FeeSample) error {
	cp := *s
	m.samples = append(m.samples, &cp)
	return nil
}

func (m *memStore) FetchSample(h uint32) (*feedb.FeeSample, error) {
	for _, s := range m.samples {
		if s.Height == h {
			return s, nil
		}
	}
	return nil, feedb.ErrSampleNotFound
}

func (m *memStore) FetchTipN(n int) ([]*feedb.FeeSample, error) {
	if n <= 0 {
		return nil, nil
	}
	out := make([]*feedb.FeeSample, 0, n)
	for i := len(m.samples) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, m.samples[i])
	}
	return out, nil
}

func (m *memStore) FetchRange(min, max uint32) ([]*feedb.FeeSample, error) {
	var out []*feedb.FeeSample
	for _, s := range m.samples {
		if s.Height >= min && s.Height <= max {
			out = append(out, s)
		}
	}
	return out, nil
}

func (m *memStore) Tip() (uint32, error) {
	var tip uint32
	for _, s := range m.samples {
		if s.Height > tip {
			tip = s.Height
		}
	}
	return tip, nil
}

func (m *memStore) PurgeBefore(cutoff uint32) error {
	kept := m.samples[:0]
	for _, s := range m.samples {
		if s.Height >= cutoff {
			kept = append(kept, s)
		}
	}
	m.samples = kept
	return nil
}

func (m *memStore) PurgeFrom(cutoff uint32) error {
	kept := m.samples[:0]
	for _, s := range m.samples {
		if s.Height < cutoff {
			kept = append(kept, s)
		}
	}
	m.samples = kept
	return nil
}

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

// TestSamplerLifecycle confirms Start and Stop are idempotent and that Stop
// is safe to call whether or not Start ever ran, in any order.
func TestSamplerLifecycle(t *testing.T) {
	t.Parallel()

	// Stop before Start, twice: must not hang or panic.
	s1, err := NewSampler(SamplerConfig{
		Store:  &memStore{},
		Params: &chaincfg.RegressionNetParams,
	})
	require.NoError(t, err)
	s1.Stop()
	s1.Stop()

	// Full lifecycle with doubled calls at each step.
	s2, err := NewSampler(SamplerConfig{
		Store:  &memStore{},
		Params: &chaincfg.RegressionNetParams,
	})
	require.NoError(t, err)
	s2.Start()
	s2.Start()
	s2.Stop()
	s2.Stop()
}

// TestObserveSkipsBelowRetentionHorizon confirms that blocks below the
// retention horizon are dropped before any sample computation or persistence:
// a deep historical rescan must not churn the store with rows the next GC
// pass would delete.
func TestObserveSkipsBelowRetentionHorizon(t *testing.T) {
	t.Parallel()
	params := &chaincfg.RegressionNetParams

	store := &memStore{}
	sampler, err := NewSampler(SamplerConfig{
		Store:      store,
		Params:     params,
		Retention:  100,
		BestHeight: func() (uint32, error) { return 1_000, nil },
	})
	require.NoError(t, err)

	// Height 100 sits far below the horizon (100 + 100 < 1000): skipped
	// from both the window and the store.
	block, _ := buildTestBlock(t, 100, params, 15_000, 1_000)
	sampler.Observe(block, 100)
	require.Empty(t, sampler.Snapshot())
	require.Empty(t, store.samples)

	// Height 950 is inside the horizon: observed and persisted.
	block2, _ := buildTestBlock(t, 950, params, 15_000, 1_000)
	sampler.Observe(block2, 950)
	require.Len(t, sampler.Snapshot(), 1)
	require.Len(t, store.samples, 1)
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
