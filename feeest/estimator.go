package feeest

import (
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/lightninglabs/neutrino/feedb"
)

// Default tunables. Exported so callers can override at construction time
// once we learn the right values from telemetry.
const (
	// DefaultHalfLife is the EWMA half-life used to weight block samples.
	// 30 minutes ≈ 3 blocks at mainnet cadence; matches the
	// regime-shift speed observed in mempool data.
	DefaultHalfLife = 30 * time.Minute

	// DefaultMinBlocksA is the minimum number of recent block samples
	// required before the tier-A estimator activates. Below this, the
	// cold-start path runs.
	DefaultMinBlocksA = 6

	// DefaultStaleWindow is how old the most recent sample can be before
	// the tier-A estimator declines to answer (and the cold-start path
	// kicks in instead).
	DefaultStaleWindow = 2 * time.Hour

	// DefaultColdStartMult is the multiplier applied to the peer-feefilter
	// floor when no usable block samples exist.
	DefaultColdStartMult = 3.0

	// DefaultColdConfidence is the fixed confidence assigned to cold-start
	// answers. Below the recommended-fallback threshold so callers know
	// to consult an external source if available.
	DefaultColdConfidence = 0.10

	// DefaultMinConfidence is the recommended threshold below which a
	// caller should consider an external estimator. Exposed only for
	// callers that want the same constant; not used internally.
	DefaultMinConfidence = 0.4
)

// PeerFeeRater is the contract the estimator requires from its host (the
// chain service) to get a snapshot of currently-connected peer feefilters.
// Implementations should return a slice of rates already converted to
// SatPerKW, drawn from peers we are currently connected to.
type PeerFeeRater interface {
	PeerFeeFilters() []SatPerKW
}

// Estimator combines the rolling window of block-level fee samples with the
// peer-feefilter floor to produce a fee-rate recommendation for a target
// confirmation window.
//
// The Estimator is read-mostly and safe for concurrent use. The Sampler it
// reads from has its own locking, and the only mutable state on the
// estimator itself is the half-life, which is updated atomically.
type Estimator struct {
	sampler *Sampler
	peers   PeerFeeRater

	// halfLifeNanos holds the EWMA half-life in nanoseconds. Stored as an
	// int64 so callers (or future adaptive logic) can update it
	// atomically without blocking estimate queries.
	halfLifeNanos int64

	// minBlocksA is the threshold below which the cold-start path runs.
	// Mutable so tests can lower it without rebuilding a Sampler with a
	// custom-sized warm-load.
	minBlocksA int

	staleWindow     time.Duration
	coldStartMult   float64
	coldConfidence  float64
	relayFloorPct   float64
	confDensityNorm float64

	// nowFn is injectable for deterministic tests.
	nowFn func() time.Time
}

// EstimatorConfig configures a new Estimator.
type EstimatorConfig struct {
	// Sampler is the source of block-level fee observations. Required.
	Sampler *Sampler

	// Peers exposes peer feefilters. Required; supply an implementation
	// that returns nil when no peers are connected.
	Peers PeerFeeRater

	// HalfLife overrides the EWMA half-life. Zero defaults to
	// DefaultHalfLife.
	HalfLife time.Duration

	// MinBlocksA overrides the minimum block-sample count for the tier-A
	// path. Zero defaults to DefaultMinBlocksA.
	MinBlocksA int
}

// New constructs an Estimator with sane defaults filled in.
func New(cfg EstimatorConfig) *Estimator {
	hl := cfg.HalfLife
	if hl <= 0 {
		hl = DefaultHalfLife
	}
	min := cfg.MinBlocksA
	if min <= 0 {
		min = DefaultMinBlocksA
	}

	return &Estimator{
		sampler:         cfg.Sampler,
		peers:           cfg.Peers,
		halfLifeNanos:   int64(hl),
		minBlocksA:      min,
		staleWindow:     DefaultStaleWindow,
		coldStartMult:   DefaultColdStartMult,
		coldConfidence:  DefaultColdConfidence,
		relayFloorPct:   0.75, // 75th-pct peer feefilter
		confDensityNorm: 20,   // target effective sample count
		nowFn:           time.Now,
	}
}

// HalfLife returns the current EWMA half-life.
func (e *Estimator) HalfLife() time.Duration {
	return time.Duration(atomic.LoadInt64(&e.halfLifeNanos))
}

// SetHalfLife atomically updates the half-life. Used by adaptive logic in a
// future PR; exposed for tests today.
func (e *Estimator) SetHalfLife(d time.Duration) {
	if d <= 0 {
		return
	}
	atomic.StoreInt64(&e.halfLifeNanos, int64(d))
}

// RelayFee returns the current network relay floor derived from connected
// peers' BIP133 feefilters. Returns 0 when no peers are connected.
func (e *Estimator) RelayFee() SatPerKW {
	rates := e.peers.PeerFeeFilters()
	if len(rates) == 0 {
		return 0
	}
	return percentile(rates, e.relayFloorPct)
}

// Estimate returns a fee-rate recommendation for the given confirmation
// target. Supported targets: 1, 3, 6, 24. Other values clamp to the nearest
// supported target.
func (e *Estimator) Estimate(target uint32) Estimate {
	target = clampTarget(target)

	all := e.sampler.Snapshot()
	usable := filterUsable(all)
	floor := e.RelayFee()
	now := e.nowFn()

	// Cold start: not enough usable samples or the most recent is stale.
	if len(usable) < e.minBlocksA || isStale(usable, now, e.staleWindow) {
		rate := SatPerKW(float64(floor) * e.coldStartMult)
		if floor > 0 && rate < floor {
			rate = floor
		}
		return Estimate{
			Rate:        rate,
			Confidence:  e.coldConfidence,
			Source:      FeeSourceCold,
			SampleCount: len(usable),
			StaleBlocks: 0,
		}
	}

	hl := e.HalfLife()
	weights := ewmaWeights(usable, now, hl)
	mean, std := weightedStats(usable, weights)

	kSigma := kSigmaFor(target)
	mult := multFor(target)
	rate := SatPerKW(mult * (mean + kSigma*std))
	if floor > 0 && rate < floor {
		rate = floor
	}

	conf := e.confidence(usable, weights, mean, std, floor, hl, now)

	return Estimate{
		Rate:        rate,
		Confidence:  conf,
		Source:      FeeSourceBlock,
		SampleCount: len(usable),
		StaleBlocks: staleBlocks(usable, now),
	}
}

// confidence returns the [0,1] composite confidence score described in the
// design doc:
//
//	conf = 0.4·density + 0.3·recency + 0.2·stability + 0.1·agreement
func (e *Estimator) confidence(samples []feedb.FeeSample, weights []float64,
	mean, std float64, floor SatPerKW, hl time.Duration,
	now time.Time) float64 {

	effN := 0.0
	for _, w := range weights {
		effN += w
	}
	density := math.Min(1, effN/e.confDensityNorm)

	// Recency: time since the most recent sample, decayed by half-life.
	recency := 0.0
	if len(samples) > 0 {
		newest := samples[len(samples)-1]
		dt := now.Sub(time.Unix(newest.Timestamp, 0))
		if dt < 0 {
			dt = 0
		}
		recency = math.Exp(-float64(dt) / float64(hl))
	}

	cv := 0.0
	if mean > 0 {
		cv = std / mean
	}
	stability := math.Exp(-2 * cv)

	agreement := 1.0
	if floor > 0 && mean > 0 {
		gap := math.Abs(mean - float64(floor))
		denom := math.Max(mean, float64(floor))
		agreement = 1 - math.Min(1, gap/denom)
	}

	conf := 0.4*density + 0.3*recency + 0.2*stability + 0.1*agreement
	if conf < 0 {
		return 0
	}
	if conf > 1 {
		return 1
	}
	return conf
}

// filterUsable drops samples flagged as empty (no fee signal) or otherwise
// inadmissible. A future extension should also drop FlagSpam samples once
// the spam detector has access to per-tx fee data.
func filterUsable(in []feedb.FeeSample) []feedb.FeeSample {
	out := in[:0:0]
	for _, s := range in {
		if s.Flags&feedb.FlagEmpty != 0 {
			continue
		}
		if s.TotalWeight == 0 {
			continue
		}
		out = append(out, s)
	}
	return out
}

// isStale reports whether the most recent sample is older than window. An
// empty slice is treated as stale.
func isStale(samples []feedb.FeeSample, now time.Time,
	window time.Duration) bool {

	if len(samples) == 0 {
		return true
	}
	newest := samples[len(samples)-1]
	return now.Sub(time.Unix(newest.Timestamp, 0)) > window
}

// staleBlocks returns an approximate count of blocks elapsed since the most
// recent sample, derived from wall-clock time and the 10-minute target block
// interval. Used purely for caller observability.
func staleBlocks(samples []feedb.FeeSample, now time.Time) uint32 {
	if len(samples) == 0 {
		return 0
	}
	newest := samples[len(samples)-1]
	dt := now.Sub(time.Unix(newest.Timestamp, 0))
	if dt < 0 {
		return 0
	}
	const blockInterval = 10 * time.Minute
	return uint32(dt / blockInterval)
}

// ewmaWeights assigns each sample an exponential-decay weight based on its
// age relative to now. Newer samples weigh more; the half-life parameter
// controls the decay rate.
func ewmaWeights(samples []feedb.FeeSample, now time.Time,
	halfLife time.Duration) []float64 {

	weights := make([]float64, len(samples))
	if len(samples) == 0 || halfLife <= 0 {
		return weights
	}

	lambda := math.Ln2 / float64(halfLife)
	for i, s := range samples {
		dt := now.Sub(time.Unix(s.Timestamp, 0))
		if dt < 0 {
			dt = 0
		}
		weights[i] = math.Exp(-lambda * float64(dt))
	}
	return weights
}

// weightedStats returns the EWMA-weighted mean and (uncorrected) standard
// deviation of the per-sample fee rates.
func weightedStats(samples []feedb.FeeSample, weights []float64) (mean,
	std float64) {

	if len(samples) == 0 {
		return 0, 0
	}

	var sumW, sumWX, sumWX2 float64
	for i, s := range samples {
		x := float64(s.FeeRatePerKW())
		w := weights[i]
		sumW += w
		sumWX += w * x
		sumWX2 += w * x * x
	}
	if sumW == 0 {
		return 0, 0
	}
	mean = sumWX / sumW
	variance := sumWX2/sumW - mean*mean
	if variance < 0 {
		variance = 0
	}
	std = math.Sqrt(variance)
	return mean, std
}

// percentile returns the p-th percentile (p in [0, 1]) of the input rates.
// Implemented with a simple nearest-rank method; input is sorted in place.
func percentile(rates []SatPerKW, p float64) SatPerKW {
	if len(rates) == 0 {
		return 0
	}
	if p < 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}

	// Copy so the caller's slice is not mutated.
	cp := make([]SatPerKW, len(rates))
	copy(cp, rates)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })

	idx := int(math.Round(p * float64(len(cp)-1)))
	return cp[idx]
}

// kSigmaFor returns the σ multiplier used to project the block-average
// "median paid" toward the percentile that maps to the requested
// confirmation target. Tighter targets push higher into the tail; looser
// targets pull below the mean.
func kSigmaFor(target uint32) float64 {
	switch target {
	case 1:
		return 1.65
	case 3:
		return 0.85
	case 6:
		return 0.0
	default: // 24+
		return -0.5
	}
}

// multFor returns the safety cushion applied on top of the σ projection.
// We deliberately lean conservative on short targets to mitigate the
// underconfidence cost of a coarse light-client estimator.
func multFor(target uint32) float64 {
	switch target {
	case 1:
		return 1.5
	case 3:
		return 1.2
	case 6:
		return 1.0
	default: // 24+
		return 0.85
	}
}

// clampTarget rounds the request to the nearest supported target.
func clampTarget(t uint32) uint32 {
	switch {
	case t <= 1:
		return 1
	case t <= 3:
		return 3
	case t <= 6:
		return 6
	default:
		return 24
	}
}
