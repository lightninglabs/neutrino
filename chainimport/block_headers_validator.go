package chainimport

import (
	"context"
	"fmt"
	"math"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightninglabs/neutrino/headerfs"
)

// blockHeadersImportSourceValidator implements headersValidator for block
// headers.
type blockHeadersImportSourceValidator struct {
	// targetChainParams contains the blockchain network parameters for
	// validation against the target chain.
	targetChainParams chaincfg.Params

	// targetBlockHeaderStore is the destination store where validated
	// headers will be written later in the import process.
	targetBlockHeaderStore headerfs.BlockHeaderStore

	// flags defines the behavior flags used during header validation.
	flags blockchain.BehaviorFlags

	// blockHeadersImportSource provides access to the source headers
	// for validation and lookup operations.
	blockHeadersImportSource HeaderImportSource
}

// Compile-time assertion to ensure blockHeadersImportSourceValidator implements
// headersValidator interface.
var _ HeadersValidator = (*blockHeadersImportSourceValidator)(nil)

// newBlockHeadersImportSourceValidator creates a new validator for block
// headers import source.
func newBlockHeadersImportSourceValidator(targetChainParams chaincfg.Params,
	targetBlockHeaderStore headerfs.BlockHeaderStore,
	flags blockchain.BehaviorFlags,
	blockHeadersImportSource HeaderImportSource) HeadersValidator {

	return &blockHeadersImportSourceValidator{
		targetChainParams:        targetChainParams,
		targetBlockHeaderStore:   targetBlockHeaderStore,
		flags:                    flags,
		blockHeadersImportSource: blockHeadersImportSource,
	}
}

// Validate performs thorough validation of a batch of block headers.
func (v *blockHeadersImportSourceValidator) Validate(ctx context.Context,
	it HeaderIterator) error {

	var (
		start      = it.GetStartIndex()
		end        = it.GetEndIndex()
		batchSize  = it.GetBatchSize()
		count      = 0
		lastHeader Header
	)

	for batch, err := range it.BatchIterator(start, end, batchSize) {
		if err != nil {
			return fmt.Errorf("failed to get next batch for "+
				"validation: %w", err)
		}

		if err := ctxCancelled(ctx); err != nil {
			return nil
		}

		if err = v.ValidateBatch(batch); err != nil {
			return fmt.Errorf("batch validation failed at "+
				"position %d: %w", count, err)
		}

		// If this is not the first batch, validate header connection
		// points between batches.
		if lastHeader != nil && len(batch) > 0 {
			err := v.ValidatePair(lastHeader, batch[0])
			if err != nil {
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
func (v *blockHeadersImportSourceValidator) ValidateSingle(h Header) error {
	header, err := assertBlockHeader(h)
	if err != nil {
		return err
	}

	return blockchain.CheckBlockHeaderSanity(
		header.BlockHeader.BlockHeader, v.targetChainParams.PowLimit,
		blockchain.NewMedianTime(), v.flags,
	)
}

// ValidatePair verifies that two consecutive block headers form a valid chain
// link.
func (v *blockHeadersImportSourceValidator) ValidatePair(prev,
	current Header) error {

	prevBlk, err := assertBlockHeader(prev)
	if err != nil {
		return err
	}
	currentBlk, err := assertBlockHeader(current)
	if err != nil {
		return err
	}

	prevBlockHeader := prevBlk.BlockHeader
	currBlockHeader := currentBlk.BlockHeader
	prevHeight, currHeight := prevBlk.Height, currentBlk.Height

	if currHeight != prevHeight+1 {
		return fmt.Errorf("height mismatch: previous height=%d, "+
			"current height=%d", prevHeight, currHeight)
	}

	prevHash := prevBlockHeader.BlockHash()
	if !currBlockHeader.PrevBlock.IsEqual(&prevHash) {
		return fmt.Errorf("header chain broken: current header's "+
			"PrevBlock (%v) doesn't match previous header's hash "+
			"(%v)", currBlockHeader.PrevBlock, prevHash)
	}

	parentCtx := &lightHeaderCtx{
		height:    int32(prevHeight),
		bits:      prevBlockHeader.Bits,
		timestamp: prevBlockHeader.Timestamp.Unix(),
		validator: v,
	}

	tCP := v.targetChainParams

	chainCtx := &lightChainCtx{
		params: &tCP,
		blocksPerRetarget: int32(tCP.TargetTimespan.Seconds() /
			tCP.TargetTimePerBlock.Seconds()),
		minRetargetTimespan: int64(tCP.TargetTimespan.Seconds() /
			float64(tCP.RetargetAdjustmentFactor)),
		maxRetargetTimespan: int64(tCP.TargetTimespan.Seconds() *
			float64(tCP.RetargetAdjustmentFactor)),
	}

	if err := blockchain.CheckBlockHeaderContext(
		currBlockHeader.BlockHeader, parentCtx, v.flags, chainCtx, true,
	); err != nil {
		return fmt.Errorf("block header contextual validation "+
			"failed: %w", err)
	}

	if err := v.ValidateSingle(current); err != nil {
		return err
	}

	return nil
}

// ValidateBatch performs validation on a batch of block headers.
func (v *blockHeadersImportSourceValidator) ValidateBatch(
	headers []Header) error {

	if len(headers) == 1 {
		return v.ValidateSingle(headers[0])
	}

	for i := 1; i < len(headers); i++ {
		if err := v.ValidatePair(headers[i-1], headers[i]); err != nil {
			return fmt.Errorf("validation failed at batch "+
				"position %d: %w", i, err)
		}
	}

	return nil
}

// lightHeaderCtx implements the blockchain.HeaderCtx interface.
type lightHeaderCtx struct {
	height    int32
	bits      uint32
	timestamp int64
	validator *blockHeadersImportSourceValidator
}

// Compile-time assertion to ensure lightHeaderCtx implements
// blockchain.HeaderCtx interface.
var _ blockchain.HeaderCtx = (*lightHeaderCtx)(nil)

// Height returns the height for the underlying header this context was created
// from.
func (l *lightHeaderCtx) Height() int32 {
	return l.height
}

// Bits returns the difficulty bits for the underlying header this context was
// created from.
func (l *lightHeaderCtx) Bits() uint32 {
	return l.bits
}

// Timestamp returns the timestamp for the underlying header this context was
// created from.
func (l *lightHeaderCtx) Timestamp() int64 {
	return l.timestamp
}

// RelativeAncestorCtx returns the ancestor header context that is distance
// blocks before the current header.
func (l *lightHeaderCtx) RelativeAncestorCtx(
	distance int32) blockchain.HeaderCtx {

	ancestorHeight := uint32(math.Max(0, float64(l.height-distance)))

	ancestorIndex := targetHeightToImportSourceIndex(
		ancestorHeight, 0,
	)

	// Lookup the ancestor in the target store.
	targetStore := l.validator.targetBlockHeaderStore
	ancestor, err := targetStore.FetchHeaderByHeight(ancestorHeight)
	if err == nil {
		return &lightHeaderCtx{
			height:    int32(ancestorHeight),
			bits:      ancestor.Bits,
			timestamp: ancestor.Timestamp.Unix(),
		}
	}

	// Fallback to import source if ancestor not found in target store.
	importAncestor, err := l.validator.blockHeadersImportSource.GetHeader(
		ancestorIndex,
	)
	if err != nil {
		return nil
	}

	importBlockAncestor, err := assertBlockHeader(importAncestor)
	if err != nil {
		return nil
	}

	return &lightHeaderCtx{
		height:    int32(ancestorHeight),
		bits:      importBlockAncestor.BlockHeader.Bits,
		timestamp: importBlockAncestor.BlockHeader.Timestamp.Unix(),
	}
}

// Parent returns the parent header context.
func (l *lightHeaderCtx) Parent() blockchain.HeaderCtx {
	return nil
}

// lightChainCtx implements the blockchain.ChainCtx interface.
type lightChainCtx struct {
	params              *chaincfg.Params
	blocksPerRetarget   int32
	minRetargetTimespan int64
	maxRetargetTimespan int64
}

// Compile-time assertion to ensure lightChainCtx implements
// blockchain.ChainCtx interface.
var _ blockchain.ChainCtx = (*lightChainCtx)(nil)

// ChainParams returns the chain parameters for the underlying chain this
// context was created from.
func (l *lightChainCtx) ChainParams() *chaincfg.Params {
	return l.params
}

// BlocksPerRetarget returns the number of blocks before retargeting occurs.
func (l *lightChainCtx) BlocksPerRetarget() int32 {
	return l.blocksPerRetarget
}

// MinRetargetTimespan returns the minimum amount of time to use in the
// difficulty calculation.
func (l *lightChainCtx) MinRetargetTimespan() int64 {
	return l.minRetargetTimespan
}

// MaxRetargetTimespan returns the maximum amount of time to use in the
// difficulty calculation.
func (l *lightChainCtx) MaxRetargetTimespan() int64 {
	return l.maxRetargetTimespan
}

// VerifyCheckpoint returns whether the passed height and hash match the
// checkpoint data.
func (l *lightChainCtx) VerifyCheckpoint(height int32,
	hash *chainhash.Hash) bool {

	return false
}

// FindPreviousCheckpoint returns the most recent checkpoint that we have
// validated.
func (l *lightChainCtx) FindPreviousCheckpoint() (blockchain.HeaderCtx, error) {
	return nil, nil
}
