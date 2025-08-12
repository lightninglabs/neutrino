package chainimport

import (
	"fmt"
	"io"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
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
