package feeest

// SatPerKW represents a fee rate in satoshis per kilo-weight unit (sat/kW).
//
// The unit is intentionally chosen to be byte-compatible with lnd's
// chainfee.SatPerKWeight (both are int64 sat/kW), which lets lnd's neutrino
// chain backend convert with a single cast and avoids forcing an lnd
// dependency on this package.
type SatPerKW int64

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
}
