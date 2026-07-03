package neutrino

import (
	"github.com/lightninglabs/neutrino/feeest"
)

// ChainFeeEstimator adapts a ChainService's fee estimator to the
// feeest.ChainEstimator interface, which mirrors lnd's chainfee.Estimator
// method set. lnd can wrap this type with a single cast per method to obtain
// a chainfee.Estimator backed entirely by neutrino's own observations, with
// no external fee API and no extra bandwidth.
//
// The adapter is a thin view: the sampler, store, and estimator it exposes
// are owned by the ChainService, whose Start/Stop drive their lifecycle.
// Start and Stop here are therefore no-ops kept only to satisfy the
// interface contract.
type ChainFeeEstimator struct {
	cs *ChainService
}

// Compile-time check that the adapter matches the mirrored lnd interface.
var _ feeest.ChainEstimator = (*ChainFeeEstimator)(nil)

// NewChainFeeEstimator returns a ChainFeeEstimator backed by the given chain
// service. The chain service must have been created with a SQL backend and
// without DisableFeeEstimator for estimates to be available; otherwise
// EstimateFeePerKW returns ErrFeeEstimatorDisabled.
func NewChainFeeEstimator(cs *ChainService) *ChainFeeEstimator {
	return &ChainFeeEstimator{cs: cs}
}

// Start signals the estimator to start any processes or goroutines it needs.
// The underlying sampler and estimator are started by ChainService.Start, so
// this is a no-op.
//
// NOTE: This method mirrors part of lnd's chainfee.Estimator interface.
func (c *ChainFeeEstimator) Start() error {
	return nil
}

// Stop stops any spawned goroutines. The underlying sampler is stopped by
// ChainService.Stop, so this is a no-op.
//
// NOTE: This method mirrors part of lnd's chainfee.Estimator interface.
func (c *ChainFeeEstimator) Stop() error {
	return nil
}

// EstimateFeePerKW takes in a target for the number of blocks until an
// initial confirmation and returns the estimated fee expressed in sat/kW.
// The result is floored at feeest.FeePerKWFloor so a cold estimator with no
// peer data can never recommend a rate that would fail to relay.
//
// NOTE: This method mirrors part of lnd's chainfee.Estimator interface.
func (c *ChainFeeEstimator) EstimateFeePerKW(numBlocks uint32) (
	feeest.SatPerKW, error) {

	est, err := c.cs.EstimateFeeRate(numBlocks)
	if err != nil {
		return 0, err
	}

	if est.Rate < feeest.FeePerKWFloor {
		return feeest.FeePerKWFloor, nil
	}

	return est.Rate, nil
}

// RelayFeePerKW returns the minimum fee rate required for transactions to be
// relayed, derived from connected peers' BIP133 feefilter advertisements and
// floored at feeest.FeePerKWFloor when no peer data is available.
//
// NOTE: This method mirrors part of lnd's chainfee.Estimator interface.
func (c *ChainFeeEstimator) RelayFeePerKW() feeest.SatPerKW {
	relayFee := c.cs.RelayFeePerKW()
	if relayFee < feeest.FeePerKWFloor {
		return feeest.FeePerKWFloor
	}

	return relayFee
}
