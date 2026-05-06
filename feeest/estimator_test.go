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

// memStore is an in-memory FeeSampleStore for estimator tests, avoiding the
// boltdb dance when we only care about the ring.
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

// addSample writes a synthetic block sample directly to the sampler's ring
// and store, bypassing block-level computation.
func addSample(s *Sampler, height uint32, ts time.Time, fees, weight uint64) {
	sample := feedb.FeeSample{
		Height:      height,
		BlockHash:   chainhash.Hash{byte(height)},
		Timestamp:   ts.Unix(),
		TotalFees:   fees,
		TotalWeight: weight,
	}
	s.ring.add(sample)
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
	// 75th-pct of {1000, 1500, 2000} → 2000 (nearest-rank).
	// Cold-start multiplier 3.0 → 6000.
	require.Equal(t, SatPerKW(6000), got.Rate)
	require.InDelta(t, DefaultColdConfidence, got.Confidence, 1e-9)
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
	// All samples have identical rate, std=0, mean=2.5 → result = mult*mean
	// for target 6 (mult=1, kSigma=0).
	require.Equal(t, SatPerKW(2), got.Rate) // truncation of 2.5
	require.GreaterOrEqual(t, got.Confidence, 0.0)
	require.LessOrEqual(t, got.Confidence, 1.0)
}

// TestEstimateStaleFallsBackToColdStart returns to the cold-start path when
// the most recent sample is older than the stale window.
func TestEstimateStaleFallsBackToColdStart(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	stale := now.Add(-3 * time.Hour) // older than DefaultStaleWindow
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
// when σ > 0. Build a window with two distinct rates so std > 0.
func TestTargetMappingMonotone(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, sampler := newTestEstimator(t, nil, now)

	// Alternate two rates so std > 0.
	for i := 0; i < DefaultMinBlocksA*2; i++ {
		fees := uint64(10_000)
		if i%2 == 0 {
			fees = uint64(50_000)
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

	got := est.Estimate(24) // negative kSigma exacerbates the underflow.
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

// TestSetHalfLife updates the EWMA tunable atomically.
func TestSetHalfLife(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	est, _ := newTestEstimator(t, nil, now)
	require.Equal(t, DefaultHalfLife, est.HalfLife())

	est.SetHalfLife(time.Hour)
	require.Equal(t, time.Hour, est.HalfLife())

	est.SetHalfLife(0) // ignored
	require.Equal(t, time.Hour, est.HalfLife())
}

// withErr is a tiny helper used to thread (Estimate, error) through one-line
// assertions without the boilerplate.
func withErr(e Estimate, err error) (Estimate, error) {
	return e, err
}
