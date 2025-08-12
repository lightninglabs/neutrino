package chainimport

import (
	"fmt"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/chainsync"
)

// FilterHeadersImportSourceValidator implements HeaderValidator for filter
// headers.
type filterHeadersImportSourceValidator struct {
	// targetChainParams contains the blockchain network parameters for
	// validation against the target chain.
	targetChainParams chaincfg.Params
}

// Compile-time assertion to ensure filterHeadersImportSourceValidator
// implements headersValidator interface.
var _ HeadersValidator = (*filterHeadersImportSourceValidator)(nil)

// newFilterHeadersImportSourceValidator creates a new validator for filter
// headers import source.
func newFilterHeadersImportSourceValidator(
	targetChainParams chaincfg.Params) HeadersValidator {

	return &filterHeadersImportSourceValidator{
		targetChainParams: targetChainParams,
	}
}

// Validate performs validation on a batch of filter headers using hardcoded
// checkpoints.
//
// The validation utilizes the existing checkpointing mechanism based on the
// hardcoded filter headers at different checkpoints. Individual headers are
// validated using ValidateSingle which returns ErrCheckpointMismatch on any
// mismatch with known checkpoints.
//
// Note: Full cryptographic validation is not possible during import since we
// don't have access to the compact filters. Filter headers are normally
// calculated using:
//
// FilterHeader_N = double-SHA256(FilterHeader_N-1 || double-SHA256(Filter_N)).
//
// where FilterHeader_N-1 is the previous filter header in little-endian byte
// order, Filter_N is the compact filter for the block, and || represents
// concatenation.
func (v *filterHeadersImportSourceValidator) Validate(it HeaderIterator) error {
	var (
		start     = it.GetStartIndex()
		end       = it.GetEndIndex()
		batchSize = it.GetBatchSize()
		count     = 0
	)

	for batch, err := range it.BatchIterator(start, end, batchSize) {
		if err != nil {
			return fmt.Errorf("failed to get next batch for "+
				"validation: %w", err)
		}

		if err = v.ValidateBatch(batch); err != nil {
			return fmt.Errorf("batch validation failed at "+
				"position %d: %w", count, err)
		}

		count += len(batch)
	}

	log.Debugf("Successfully validated %d filter headers", count)
	return nil
}

// ValidateBatch performs validation on a batch of filter headers.
func (v *filterHeadersImportSourceValidator) ValidateBatch(
	headers []Header) error {

	for _, header := range headers {
		if err := v.ValidateSingle(header); err != nil {
			return err
		}
	}

	return nil
}

// ValidateSingle validates a single filter header. It utilizes the existing
// checkpointing mechanism based on the hardcoded filter headers at different
// checkpoints. On any mismatch it returns ErrCheckpointMismatch. If the given
// header doesn't exist in the hardcoded checkpoints, it returns no error.
func (v *filterHeadersImportSourceValidator) ValidateSingle(h Header) error {
	filterHeader, err := assertFilterHeader(h)
	if err != nil {
		return err
	}

	if err := chainsync.ValidateCFHeader(
		v.targetChainParams, wire.GCSFilterRegular, filterHeader.Height,
		&filterHeader.FilterHash,
	); err != nil {
		return err
	}

	return nil
}

// ValidatePair checks if two consecutive filter headers maintain the correct
// cryptographic relationship.
//
// In a full validation, we would verify that each filter header is correctly
// derived by hashing the previous filter header with the current block's
// compact filter.
//
// However, during import, we don't have access to the compact filters, so we
// can't validate if two consecutive filter headers maintain the correct
// cryptographic relationship.
func (v *filterHeadersImportSourceValidator) ValidatePair(prev,
	current Header) error {

	return nil
}
