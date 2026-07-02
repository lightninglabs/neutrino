package feeest

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightninglabs/neutrino/feedb"
	"github.com/stretchr/testify/require"
)

// fakePeerRater returns a fixed list of peer feefilter rates for tests.
type fakePeerRater struct {
	rates []SatPerKW
}

func (f *fakePeerRater) PeerFeeFilters() []SatPerKW {
	out := make([]SatPerKW, len(f.rates))
	copy(out, f.rates)
	return out
}

// newTestEstimator wires up a fixed-time estimator backed by a memory store.
func newTestEstimator(t *testing.T, peers []SatPerKW, now time.Time) (
	*Estimator, *Sampler) {

	t.Helper()
	store := &memStore{}
	sampler, err := NewSampler(SamplerConfig{
		Store:    store,
		Params:   &chaincfg.RegressionNetParams,
		RingSize: 50,
	})
	require.NoError(t, err)

	est := New(EstimatorConfig{
		Sampler: sampler,
		Peers:   &fakePeerRater{rates: peers},
	})
	est.nowFn = func() time.Time { return now }
	return est, sampler
}

// addSample writes a synthetic block sample directly to the sampler's window
// and store, bypassing block-level computation.
func addSample(s *Sampler, height uint32, ts time.Time, fees, weight uint64) {
	addFullSample(s, feedb.FeeSample{
		Height:      height,
		BlockHash:   chainhash.Hash{byte(height), byte(height >> 8)},
		Timestamp:   ts.Unix(),
		TotalFees:   fees,
		TotalWeight: weight,
	})
}

// addFullSample writes a caller-constructed sample to the sampler's window
// and store.
func addFullSample(s *Sampler, sample feedb.FeeSample) {
	s.window.add(sample)
	_ = s.store.PutSample(&sample)
}

// TestEstimateColdStartNoPeers returns a zero rate when there is neither
// sample data nor any peer feefilters.
func TestEstimateColdStartNoPeers(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, _ := newTestEstimator(t, nil, now)

	got, err := withErr(est.Estimate(6), nil)
	require.NoError(t, err)
	require.Equal(t, FeeSourceCold, got.Source)
	require.Equal(t, SatPerKW(0), got.Rate)
	require.InDelta(t, DefaultColdConfidence, got.Confidence, 1e-9)
}

// TestEstimateColdStartWithPeers uses the peer feefilter floor multiplied by
// DefaultColdStartMult when no block samples are available.
func TestEstimateColdStartWithPeers(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, _ := newTestEstimator(t, []SatPerKW{1000, 2000, 1500}, now)

	got := est.Estimate(6)
	require.Equal(t, FeeSourceCold, got.Source)
	// Median of {1000, 1500, 2000} → 1500. Cold-start multiplier 3.0 and
	// target-6 cushion 1.0 → 4500.
	require.Equal(t, SatPerKW(4500), got.Rate)
	require.InDelta(t, DefaultColdConfidence, got.Confidence, 1e-9)

	// Cold-start answers must still price short targets above long ones.
	require.Greater(t, est.Estimate(1).Rate, est.Estimate(24).Rate)
}

// TestEstimateBelowMinSamples falls back to cold start until the threshold
// is reached.
func TestEstimateBelowMinSamples(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, sampler := newTestEstimator(
		t, []SatPerKW{1000}, now,
	)

	for i := 0; i < DefaultMinBlocksA-1; i++ {
		addSample(sampler, uint32(100+i),
			now.Add(-time.Duration(i)*10*time.Minute),
			10_000, 4_000_000)
	}

	got := est.Estimate(6)
	require.Equal(t, FeeSourceCold, got.Source)
}

// TestEstimateTierAActivates uses the block samples once the threshold is
// satisfied.
func TestEstimateTierAActivates(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, sampler := newTestEstimator(t, []SatPerKW{1}, now)

	// Each sample is 10_000 sat / 4_000_000 wu = 2.5 sat/kW.
	for i := 0; i < DefaultMinBlocksA; i++ {
		addSample(sampler, uint32(100+i),
			now.Add(-time.Duration(i+1)*5*time.Minute),
			10_000, 4_000_000)
	}

	got := est.Estimate(6)
	require.Equal(t, FeeSourceBlock, got.Source)
	// All samples share the same 2 sat/kW block-average rate, so every
	// quantile is 2 and the target-6 cushion is 1.0. The blocks are full,
	// so the congestion gate leaves the projection alone.
	require.Equal(t, SatPerKW(2), got.Rate)
	require.InDelta(t, 1.0, got.Congestion, 1e-9)
	require.GreaterOrEqual(t, got.Confidence, 0.0)
	require.LessOrEqual(t, got.Confidence, 1.0)
}

// TestEstimateStaleFallsBackToColdStart returns to the cold-start path when
// the most recent sample is older than the stale window.
func TestEstimateStaleFallsBackToColdStart(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	// Stale is older than DefaultStaleWindow.
	stale := now.Add(-3 * time.Hour)
	est, sampler := newTestEstimator(t, []SatPerKW{1000}, now)

	for i := 0; i < DefaultMinBlocksA*2; i++ {
		addSample(sampler, uint32(100+i),
			stale.Add(-time.Duration(i)*10*time.Minute),
			10_000, 4_000_000)
	}

	got := est.Estimate(6)
	require.Equal(t, FeeSourceCold, got.Source)
}

// TestTargetMappingMonotone confirms tighter targets recommend higher rates
// when the window contains a spread of rates.
func TestTargetMappingMonotone(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, sampler := newTestEstimator(t, nil, now)

	// Alternate two rates (125 and 25 sat/kW) so the quantiles spread.
	for i := 0; i < DefaultMinBlocksA*2; i++ {
		fees := uint64(100_000)
		if i%2 == 0 {
			fees = uint64(500_000)
		}
		addSample(sampler, uint32(100+i),
			now.Add(-time.Duration(i+1)*5*time.Minute),
			fees, 4_000_000)
	}

	r1 := est.Estimate(1).Rate
	r3 := est.Estimate(3).Rate
	r6 := est.Estimate(6).Rate
	r24 := est.Estimate(24).Rate

	require.Greater(t, r1, r3, "1-block should exceed 3-block")
	require.Greater(t, r3, r6, "3-block should exceed 6-block")
	require.Greater(t, r6, r24, "6-block should exceed 24-block")
}

// TestRateNeverBelowFloor confirms the relay-floor lower bound is enforced
// even when the σ-projection would go below it.
func TestRateNeverBelowFloor(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, sampler := newTestEstimator(t, []SatPerKW{1_000_000}, now)

	// Cheap samples below the relay floor.
	for i := 0; i < DefaultMinBlocksA*2; i++ {
		addSample(sampler, uint32(100+i),
			now.Add(-time.Duration(i+1)*5*time.Minute),
			10, 4_000_000)
	}

	got := est.Estimate(24) // the low quantile exacerbates the underflow.
	require.GreaterOrEqual(t, got.Rate, est.RelayFee())
}

// TestPercentileNearestRank confirms the helper picks expected indices.
func TestPercentileNearestRank(t *testing.T) {
	t.Parallel()
	rates := []SatPerKW{100, 200, 300, 400, 500}
	require.Equal(t, SatPerKW(100), percentile(rates, 0.0))
	require.Equal(t, SatPerKW(300), percentile(rates, 0.5))
	require.Equal(t, SatPerKW(500), percentile(rates, 1.0))
}

// TestRelayFeeNoPeers returns zero with no peers, never panics.
func TestRelayFeeNoPeers(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, _ := newTestEstimator(t, nil, now)
	require.Equal(t, SatPerKW(0), est.RelayFee())
}

// TestClampTarget covers the full mapping table.
func TestClampTarget(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint32(1), clampTarget(0))
	require.Equal(t, uint32(1), clampTarget(1))
	require.Equal(t, uint32(3), clampTarget(2))
	require.Equal(t, uint32(3), clampTarget(3))
	require.Equal(t, uint32(6), clampTarget(4))
	require.Equal(t, uint32(6), clampTarget(6))
	require.Equal(t, uint32(24), clampTarget(7))
	require.Equal(t, uint32(24), clampTarget(99))
}

// TestSetHalfLifeClamped verifies SetHalfLife clamps to [min, max].
func TestSetHalfLifeClamped(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, _ := newTestEstimator(t, nil, now)

	est.SetHalfLife(time.Hour)
	require.Equal(t, time.Hour, est.HalfLife())

	// A zero duration is ignored.
	est.SetHalfLife(0)
	require.Equal(t, time.Hour, est.HalfLife())

	// Below the minimum: clamped to DefaultMinHalfLife.
	est.SetHalfLife(time.Second)
	require.Equal(t, DefaultMinHalfLife, est.HalfLife())

	// Above the maximum: clamped to DefaultMaxHalfLife.
	est.SetHalfLife(24 * time.Hour)
	require.Equal(t, DefaultMaxHalfLife, est.HalfLife())
}

// TestAdaptiveHalfLifeShortensOnVolatility confirms the half-life shrinks when
// recent samples are more volatile than prior samples.
func TestAdaptiveHalfLifeShortensOnVolatility(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, sampler := newTestEstimator(t, nil, now)
	est.SetHalfLife(DefaultHalfLife)
	est.minBlocksA = 3 // lower threshold so we need fewer samples

	// Prior window: stable ~2.5 sat/kW (same rate).
	for i := 0; i < 6; i++ {
		addSample(sampler, uint32(100+i),
			now.Add(-time.Duration(DefaultHalfLife+time.Duration(i)*10*time.Minute)),
			10_000, 4_000_000)
	}

	// Recent window: high variance (alternating 1 sat/kW and 20 sat/kW).
	for i := 0; i < 6; i++ {
		fees := uint64(4_000) // ~1 sat/kW
		if i%2 == 0 {
			fees = 80_000 // ~20 sat/kW
		}
		addSample(sampler, uint32(110+i),
			now.Add(-time.Duration(i)*5*time.Minute),
			fees, 4_000_000)
	}

	// Trigger an estimate so maybeAdaptHalfLife runs.
	est.Estimate(6)

	// Half-life should have shortened below the default.
	adapted := est.HalfLife()
	require.Less(t, adapted, DefaultHalfLife,
		"half-life should shorten on high-volatility window")

	// Re-estimating over the same window must not adapt again: without a
	// new sample the half-life would otherwise ratchet down to the clamp.
	est.Estimate(6)
	require.Equal(t, adapted, est.HalfLife(),
		"half-life must adapt at most once per new sample")
}

// TestAdaptiveHalfLifeRecoversOnStability confirms the half-life can climb
// back up from the minimum clamp once the market stabilises. This is the
// regression test for the one-way ratchet: with a wall-clock split, a
// 10-minute half-life meant the recent bucket could never repopulate and the
// doubling branch was unreachable until restart.
func TestAdaptiveHalfLifeRecoversOnStability(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, sampler := newTestEstimator(t, nil, now)
	est.minBlocksA = 3
	est.SetHalfLife(DefaultMinHalfLife)

	// Older half: high variance (the tail of a fee storm).
	for i := 0; i < 6; i++ {
		fees := uint64(4_000)
		if i%2 == 0 {
			fees = 80_000
		}
		addSample(sampler, uint32(100+i),
			now.Add(-time.Duration(60-i*5)*time.Minute),
			fees, 4_000_000)
	}

	// Newer half: flat rates, the market has calmed down.
	for i := 0; i < 6; i++ {
		addSample(sampler, uint32(110+i),
			now.Add(-time.Duration(28-i*5)*time.Minute),
			10_000, 4_000_000)
	}

	est.Estimate(6)
	require.Greater(t, est.HalfLife(), DefaultMinHalfLife,
		"half-life must recover from the clamp once CV falls")
}

// TestCurrentStats returns a populated Stats snapshot without full estimation.
func TestCurrentStats(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, sampler := newTestEstimator(t, []SatPerKW{1000}, now)

	s := est.CurrentStats()
	require.Equal(t, 0, s.SampleCount)
	require.False(t, s.WarmWindow)
	require.Equal(t, SatPerKW(1000), s.RelayFloor)

	for i := 0; i < DefaultMinBlocksA; i++ {
		addSample(sampler, uint32(100+i),
			now.Add(-time.Duration(i+1)*5*time.Minute),
			10_000, 4_000_000)
	}

	s = est.CurrentStats()
	require.Equal(t, DefaultMinBlocksA, s.SampleCount)
	require.True(t, s.WarmWindow)
}

// TestCongestionGateIdleMarket confirms that when recent blocks have spare
// capacity, the recommendation collapses toward the relay floor even if the
// block-average rates are high (e.g. a few large-fee txs in small blocks).
func TestCongestionGateIdleMarket(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	floor := SatPerKW(250)
	est, sampler := newTestEstimator(t, []SatPerKW{floor}, now)

	// Half-empty blocks with a high average rate: 2M sats of fees over
	// 500k WU → 4000 sat/kW average, but no competition for space.
	for i := 0; i < DefaultMinBlocksA*2; i++ {
		addSample(sampler, uint32(100+i),
			now.Add(-time.Duration(i+1)*5*time.Minute),
			2_000_000, 500_000)
	}

	got := est.Estimate(1)
	require.Equal(t, FeeSourceBlock, got.Source)
	require.InDelta(t, 0.0, got.Congestion, 1e-9)

	// With zero congestion the blend lands exactly on the floor.
	require.Equal(t, floor, got.Rate)
}

// TestCongestionGateBlendsPartially confirms a mixed window of full and
// non-full blocks lands between the floor and the full projection.
func TestCongestionGateBlendsPartially(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	floor := SatPerKW(250)
	est, sampler := newTestEstimator(t, []SatPerKW{floor}, now)

	// Alternate full and near-empty blocks, all at 4000 sat/kW average.
	for i := 0; i < DefaultMinBlocksA*2; i++ {
		weight := uint64(4_000_000)
		fees := uint64(16_000_000)
		if i%2 == 0 {
			weight = 500_000
			fees = 2_000_000
		}
		addSample(sampler, uint32(100+i),
			now.Add(-time.Duration(i+1)*5*time.Minute),
			fees, weight)
	}

	got := est.Estimate(6)
	require.Greater(t, got.Congestion, 0.0)
	require.Less(t, got.Congestion, 1.0)
	require.Greater(t, got.Rate, floor)
	// Full projection would be mult(6)=1.0 × 4000; the blend must sit
	// strictly below it.
	require.Less(t, got.Rate, SatPerKW(4000))
}

// TestEntryProxyUsesKnownTxRate confirms a block whose average is inflated by
// one huge payer is corrected by the exact per-tx bound from intra-block
// spends.
func TestEntryProxyUsesKnownTxRate(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, sampler := newTestEstimator(t, nil, now)

	// Full blocks with a wildly inflated 40k sat/kW average, but each
	// carries an intra-block spend chain showing a tx got in at 1000
	// sat/kW.
	for i := 0; i < DefaultMinBlocksA*2; i++ {
		addFullSample(sampler, feedb.FeeSample{
			Height:         uint32(100 + i),
			BlockHash:      chainhash.Hash{byte(100 + i), 1},
			Timestamp:      now.Add(-time.Duration(i+1) * 5 * time.Minute).Unix(),
			TotalFees:      160_000_000,
			TotalWeight:    4_000_000,
			MinKnownTxRate: 1000,
			KnownTxCount:   2,
		})
	}

	got := est.Estimate(1)
	require.Equal(t, FeeSourceBlock, got.Source)
	// Projection is bounded by the known entry rate, not the block
	// average: 1.2 × 1000 vs 1.2 × 40_000.
	require.LessOrEqual(t, got.Rate, SatPerKW(1200))
}

// TestWeightedQuantile exercises the interpolated weighted quantile helper.
func TestWeightedQuantile(t *testing.T) {
	t.Parallel()

	vals := []float64{10, 20, 30, 40}
	unit := []float64{1, 1, 1, 1}

	require.Equal(t, 0.0, weightedQuantile(nil, nil, 0.5))
	require.Equal(t, 10.0, weightedQuantile(vals, unit, 0))
	require.Equal(t, 40.0, weightedQuantile(vals, unit, 1))
	// Midpoints sit at 0.125, 0.375, 0.625, 0.875; p=0.5 interpolates
	// halfway between 20 and 30.
	require.InDelta(t, 25.0, weightedQuantile(vals, unit, 0.5), 1e-9)

	// Skewed weights pull the quantile toward the heavy value: with
	// weights {1, 1, 1, 97} the p=0.5 quantile sits just below 40.
	heavy := []float64{1, 1, 1, 97}
	require.InDelta(t, 39.7, weightedQuantile(vals, heavy, 0.5), 0.1)

	// Zero total weight cannot be projected.
	require.Equal(t, 0.0, weightedQuantile(vals, []float64{0, 0, 0, 0}, 0.5))

	// Monotone in p.
	prev := 0.0
	for p := 0.0; p <= 1.0; p += 0.05 {
		q := weightedQuantile(vals, unit, p)
		require.GreaterOrEqual(t, q, prev)
		prev = q
	}
}

// TestEstimateRescanDoesNotPolluteWindow reproduces the historical-block
// pollution scenario end to end: a warm estimator must keep answering from
// fresh tip samples even after a rescan floods the sampler with old blocks.
func TestEstimateRescanDoesNotPolluteWindow(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, sampler := newTestEstimator(t, nil, now)

	// Warm window at the tip. The test sampler's window capacity is 50.
	for i := 0; i < 50; i++ {
		addSample(sampler, uint32(100_000+i),
			now.Add(-time.Duration(50-i)*5*time.Minute),
			16_000_000, 4_000_000)
	}
	require.Equal(t, FeeSourceBlock, est.Estimate(6).Source)

	// A rescan pulls in a year-old range of blocks.
	old := now.Add(-365 * 24 * time.Hour)
	for i := 0; i < 200; i++ {
		sampler.window.add(feedb.FeeSample{
			Height:      uint32(40_000 + i),
			BlockHash:   chainhash.Hash{byte(i), byte(i >> 8), 2},
			Timestamp:   old.Add(time.Duration(i) * 10 * time.Minute).Unix(),
			TotalFees:   16_000_000,
			TotalWeight: 4_000_000,
		})
	}

	got := est.Estimate(6)
	require.Equal(t, FeeSourceBlock, got.Source,
		"historical rescan blocks must not evict the fresh window")
	require.Zero(t, got.StaleBlocks)
}

// withErr is a tiny helper used to thread (Estimate, error) through one-line
// assertions without the boilerplate.
func withErr(e Estimate, err error) (Estimate, error) {
	return e, err
}
