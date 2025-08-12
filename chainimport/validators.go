package chainimport

import (
	"fmt"
	"io"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/chainsync"
)

// blockHeadersImportSourceValidator implements headersValidator for block
// headers.
type blockHeadersImportSourceValidator struct{}

// Compile-time assertion to ensure blockHeadersImportSourceValidator implements
// headersValidator interface.
var _ HeadersValidator = (*blockHeadersImportSourceValidator)(nil)

// newBlockHeadersImportSourceValidator creates a new block headers import
// source validator.
func newBlockHeadersImportSourceValidator() HeadersValidator {
	return &blockHeadersImportSourceValidator{}
}

// Validate performs thorough validation of a batch of block headers.
func (v *blockHeadersImportSourceValidator) Validate(
	iterator HeaderIterator,
	targetChainParams chaincfg.Params) error {

	count := 0
	var lastHeader Header

	for {
		batch, err := iterator.NextBatch()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to get next batch for "+
				"validation: %w", err)
		}

		err = v.ValidateBatch(batch, targetChainParams)
		if err != nil {
			return fmt.Errorf("batch validation failed at "+
				"position %d: %w", count, err)
		}

		// If this is not the first batch, validate header connection
		// points between batches.
		if lastHeader != nil && len(batch) > 0 {
			if err := v.ValidatePair(
				lastHeader, batch[0], targetChainParams,
			); err != nil {
				return fmt.Errorf("cross-batch validation "+
					"failed at position %d: %w", count, err)
			}
		}

		count += len(batch)
		if len(batch) > 0 {
			lastHeader = batch[len(batch)-1]
		}
	}

	log.Debugf("Successfully validated %d block headers", count)
	return nil
}

// ValidateSingle validates a single block header for basic sanity.
func (v *blockHeadersImportSourceValidator) ValidateSingle(
	header Header, targetChainParams chaincfg.Params) error {

	h, err := assertBlockHeader(header)
	if err != nil {
		return err
	}

	// Validate block header sanity
	return blockchain.CheckBlockHeaderSanity(
		h.BlockHeader.BlockHeader, targetChainParams.PowLimit,
		blockchain.NewMedianTime(), blockchain.BFFastAdd,
	)
}

// ValidatePair verifies that two consecutive block headers form a valid chain
// link.
func (v *blockHeadersImportSourceValidator) ValidatePair(prev,
	current Header, targetChainParams chaincfg.Params) error {

	// Type assert prev and current headers to be block headers.
	prevBlk, err := assertBlockHeader(prev)
	if err != nil {
		return err
	}
	currentBlk, err := assertBlockHeader(current)
	if err != nil {
		return err
	}

	// Extract the block headers and heights directly.
	prevBlockHeader := prevBlk.BlockHeader
	currBlockHeader := currentBlk.BlockHeader
	prevHeight, currHeight := prevBlk.Height, currentBlk.Height

	// 1. Verify that heights are consecutive.
	if currHeight != prevHeight+1 {
		return fmt.Errorf("height mismatch: previous height=%d, "+
			"current height=%d", prevHeight, currHeight)
	}

	// 2. Verify hash chain. Current header's PrevBlock must match previous
	// header's hash.
	prevHash := prevBlockHeader.BlockHash()
	if !currBlockHeader.PrevBlock.IsEqual(&prevHash) {
		return fmt.Errorf("header chain broken: current header's "+
			"PrevBlock (%v) doesn't match previous header's hash "+
			"(%v)", currBlockHeader.PrevBlock, prevHash)
	}

	// 3. Validate current header sanity using ValidateSingle.
	if err := v.ValidateSingle(current, targetChainParams); err != nil {
		return err
	}

	return nil
}

// ValidateBatch performs validation on a batch of block headers.
func (v *blockHeadersImportSourceValidator) ValidateBatch(
	headers []Header, targetChainParams chaincfg.Params) error {

	if len(headers) == 1 {
		// Single header batch - just validate the header itself.
		return v.ValidateSingle(headers[0], targetChainParams)
	}

	// Validate all consecutive pairs in the batch
	for i := 1; i < len(headers); i++ {
		if err := v.ValidatePair(
			headers[i-1], headers[i], targetChainParams,
		); err != nil {
			return fmt.Errorf("validation failed at batch "+
				"position %d: %w", i, err)
		}
	}

	return nil
}

// FilterHeadersImportSourceValidator implements HeaderValidator for filter
// headers.
type filterHeadersImportSourceValidator struct{}

// Compile-time assertion to ensure filterHeadersImportSourceValidator
// implements headersValidator interface.
var _ HeadersValidator = (*filterHeadersImportSourceValidator)(nil)

// newFilterHeadersImportSourceValidator creates a new filter headers import
// source validator.
func newFilterHeadersImportSourceValidator() HeadersValidator {
	return &filterHeadersImportSourceValidator{}
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
func (v *filterHeadersImportSourceValidator) Validate(iterator HeaderIterator,
	targetChainParams chaincfg.Params) error {

	count := 0
	for {
		batch, err := iterator.NextBatch()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to get next batch for "+
				"validation: %w", err)
		}

		err = v.ValidateBatch(batch, targetChainParams)
		if err != nil {
			return fmt.Errorf("batch validation failed at "+
				"position %d: %w", count, err)
		}

		count += len(batch)
	}

	log.Debugf("Successfully validated %d filter headers", count)
	return nil
}

// ValidateBatch performs validation on a batch of filter headers. Currently
// returns nil since full validation requires compact filters.
func (v *filterHeadersImportSourceValidator) ValidateBatch(
	headers []Header, targetChainParams chaincfg.Params) error {

	for _, header := range headers {
		err := v.ValidateSingle(header, targetChainParams)
		if err != nil {
			return err
		}
	}

	return nil
}

// ValidateSingle validates a single filter header. It utilizes the existing
// checkpointing mechanism based on the hardcoded filter headers at different
// checkpoints. On any mismatch it returns ErrCheckpointMismatch. If the given
// header doesn't exist in the hardcoded checkpoints, it returns no error.
func (v *filterHeadersImportSourceValidator) ValidateSingle(
	header Header, targetChainParams chaincfg.Params) error {

	filterHeader, err := assertFilterHeader(header)
	if err != nil {
		return err
	}

	if err := chainsync.ControlCFHeader(
		targetChainParams, wire.GCSFilterRegular, filterHeader.Height,
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
	current Header, targetChainParams chaincfg.Params) error {

	return nil
}
