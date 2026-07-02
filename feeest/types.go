package feeest

// SatPerKW represents a fee rate in satoshis per kilo-weight unit (sat/kW).
//
// The unit is intentionally chosen to be byte-compatible with lnd's
// chainfee.SatPerKWeight (both are int64 sat/kW), which lets lnd's neutrino
// chain backend convert with a single cast and avoids forcing an lnd
// dependency on this package.
type SatPerKW int64

// FeePerKWFloor is the lowest fee rate in sat/kW that estimates handed to
// wallet-facing consumers should ever report. It is the sat/kW equivalent of
// the 1 sat/vbyte network relay minimum and mirrors lnd's
// chainfee.FeePerKwFloor, so rates crossing the package boundary stay
// broadcastable even when no peer feefilter data is available.
const FeePerKWFloor SatPerKW = 253

// ChainEstimator mirrors the method set of lnd's lnwallet/chainfee.Estimator
// interface. Neutrino cannot import that package directly (lnd depends on
// neutrino, so the import would be circular); instead we declare a
// structurally identical contract here, with SatPerKW standing in for
// chainfee.SatPerKWeight. The lnd side satisfies chainfee.Estimator with a
// thin wrapper that converts between the two int64-based types.
type ChainEstimator interface {
	// EstimateFeePerKW takes in a target for the number of blocks until
	// an initial confirmation and returns the estimated fee expressed in
	// sat/kW.
	EstimateFeePerKW(numBlocks uint32) (SatPerKW, error)

	// Start signals the estimator to start any processes or goroutines
	// it needs to perform its duty.
	Start() error

	// Stop stops any spawned goroutines and cleans up the resources used
	// by the estimator.
	Stop() error

	// RelayFeePerKW returns the minimum fee rate required for
	// transactions to be relayed.
	RelayFeePerKW() SatPerKW
}

// FeeSource describes which signal the estimator used to produce a result.
type FeeSource uint8

const (
	// FeeSourceCold indicates the estimator had no usable block samples
	// and fell back to the peer feefilter floor with a fixed multiplier.
	// Confidence will be low.
	FeeSourceCold FeeSource = iota

	// FeeSourceBlock indicates the estimator used the rolling window of
	// block-level fee samples (tier A).
	FeeSourceBlock
)

// String implements fmt.Stringer for log output.
func (s FeeSource) String() string {
	switch s {
	case FeeSourceCold:
		return "cold"
	case FeeSourceBlock:
		return "block"
	default:
		return "unknown"
	}
}

// Estimate is the result of a single fee-rate query.
type Estimate struct {
	// Rate is the recommended fee rate.
	Rate SatPerKW

	// Confidence is a value in [0, 1] expressing how much faith the
	// estimator has in this result. Callers may treat values below ~0.4
	// as a signal to consult an external estimator instead.
	Confidence float64

	// Source identifies which tier of input produced the answer.
	Source FeeSource

	// SampleCount is the number of block samples in the window when the
	// answer was produced. Zero in the cold-start case.
	SampleCount int

	// StaleBlocks is the number of blocks elapsed since the most recent
	// sample. Zero in the cold-start case (no samples).
	StaleBlocks uint32

	// Congestion is the EWMA-weighted fraction of observed blocks that
	// were full, in [0, 1]. Low values mean the fee market was idle and
	// the recommendation is anchored near the relay floor. Zero in the
	// cold-start case.
	Congestion float64
}
