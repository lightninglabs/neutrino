// Package feedb implements persistent storage for per-block fee-rate samples
// observed by the chain service. The store is intentionally narrow: one fixed
// row per fetched block, indexed by height for cheap range scans and ordered
// pruning.
//
// The samples are consumed by the feeest package, which builds a fee-rate
// estimator over a rolling window of recent observations.
package feedb

import (
	"errors"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
)

// ErrSampleNotFound is returned when a sample is requested for a height that
// has no stored observation.
var ErrSampleNotFound = errors.New("fee sample not found")

// SampleFlag is a bitset describing properties of a stored sample.
type SampleFlag uint8

const (
	// FlagEmpty marks blocks containing only the coinbase transaction
	// (no fee-paying txs). These contribute no useful signal and are
	// dropped by the estimator.
	FlagEmpty SampleFlag = 1 << iota

	// FlagSpam marks blocks whose total fees are dominated by a single
	// transaction (defined as one tx accounting for >50% of the block's
	// fees). The estimator compensates for such blocks by preferring the
	// tighter MinKnownTxRate bound over the inflated block average; the
	// flag itself is recorded for diagnostics and future policy.
	FlagSpam
)

// FeeSample is one observation derived from a fully fetched block. The block
// average fee rate is implicitly TotalFees * 1000 / TotalWeight (sat/kW).
type FeeSample struct {
	// Height is the block height the sample came from.
	Height uint32

	// BlockHash identifies the block; required to detect reorgs against a
	// stored sample.
	BlockHash chainhash.Hash

	// Timestamp is the block header timestamp (Unix seconds).
	Timestamp int64

	// TotalFees is the sum of fees paid by all transactions in the block,
	// derived as coinbase output minus subsidy.
	TotalFees uint64

	// TotalWeight is the sum of weight units across all transactions in
	// the block (including the coinbase).
	TotalWeight uint64

	// CoinbaseWeight is the weight of the coinbase transaction alone. It
	// pays no fee, so subtracting it from TotalWeight gives the weight of
	// the fee-paying remainder of the block. Zero for samples written
	// before this field was recorded.
	CoinbaseWeight uint64

	// MinKnownTxRate is the lowest exact per-transaction fee rate
	// (sat/kW) observed among transactions whose prevouts were all
	// created within the same block (intra-block spend chains). Every
	// transaction in a block paid at least the block's marginal entry
	// rate, so this is a tighter upper bound on that entry rate than the
	// block average. Zero when no such transaction was found.
	MinKnownTxRate uint64

	// KnownTxCount is the number of transactions that contributed to
	// MinKnownTxRate. A larger count makes the bound more trustworthy.
	KnownTxCount uint16

	// Flags captures qualitative properties of the sample.
	Flags SampleFlag
}

// FeeRatePerKW returns the block-average fee rate in satoshis per kilo-weight,
// computed over the fee-paying portion of the block (total weight minus the
// coinbase weight when known). Returns 0 if the effective weight is zero.
func (s *FeeSample) FeeRatePerKW() uint64 {
	weight := s.TotalWeight
	if s.CoinbaseWeight > 0 && s.CoinbaseWeight < weight {
		weight -= s.CoinbaseWeight
	}
	if weight == 0 {
		return 0
	}
	return s.TotalFees * 1000 / weight
}

// FeeSampleStore is the interface exposed to consumers of the package. The
// estimator depends on this interface, allowing in-memory mocks for tests.
type FeeSampleStore interface {
	// PutSample writes a sample to disk, replacing any existing sample at
	// the same height.
	PutSample(*FeeSample) error

	// FetchSample retrieves a sample by height. Returns ErrSampleNotFound
	// if no sample exists at that height.
	FetchSample(height uint32) (*FeeSample, error)

	// FetchTipN returns up to n most recent samples in descending height
	// order. Fewer samples may be returned if the store has fewer rows.
	FetchTipN(n int) ([]*FeeSample, error)

	// FetchRange returns all samples with heights in [min, max] inclusive,
	// in ascending height order.
	FetchRange(min, max uint32) ([]*FeeSample, error)

	// Tip returns the highest height with a stored sample. Returns 0 and
	// no error when the store is empty.
	Tip() (uint32, error)

	// PurgeBefore deletes all samples with height < cutoff. Used for the
	// retention-window GC.
	PurgeBefore(cutoff uint32) error

	// PurgeFrom deletes all samples with height >= cutoff. Used on reorg
	// to drop observations from the orphaned chain.
	PurgeFrom(cutoff uint32) error
}
