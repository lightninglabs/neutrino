package chainimport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightninglabs/neutrino/headerfs"
)

const (
	// defaultWriteBatchSizePerRegion defines the default number of headers
	// to process in a single batch when no specific batch size is provided.
	defaultWriteBatchSizePerRegion = 16384
)

// processingRegions currently contains regions to process. Those regions
// overlap, divergence, and new headers region are all detected but only new
// headers region is processed.
type processingRegions struct {
	// importStartHeight defines the starting block height for the import
	// process.
	importStartHeight uint32

	// importEndHeight defines the ending block height for the import
	// process.
	importEndHeight uint32

	// effectiveTip represents the current chain tip height that is
	// effective for processing.
	effectiveTip uint32

	// overlap contains the region of headers that overlap with the target
	// chain.
	overlap headerRegion

	// divergence contains the region of headers that diverge from the
	// target chain.
	divergence headerRegion

	// newHeaders contains the region of new headers that need to be
	// processed.
	newHeaders headerRegion
}

// headerRegion represents a contiguous range of headers.
type headerRegion struct {
	// start is the beginning height of this header region.
	start uint32

	// end is the ending height of this header region.
	end uint32

	// exists indicates whether this region has headers to process.
	exists bool
}

// headersImport orchestrates the import of blockchain headers from external
// sources into local header stores. It handles validation, processing, and
// atomic writes of both block headers and filter headers while maintaining
// chain integrity and consistency between stores.
type headersImport struct {
	// blockHeadersImportSource provides access to block headers from import
	// source.
	blockHeadersImportSource HeaderImportSource

	// blockHeadersImportSource provides access to filter headers from
	// import source.
	filterHeadersImportSource HeaderImportSource

	// blockHeadersValidator validates the imported block headers.
	blockHeadersValidator HeadersValidator

	// filterHeadersValidator validates the imported filter headers.
	filterHeadersValidator HeadersValidator

	// options contains configuration parameters for the import process.
	options *ImportOptions
}

// NewHeadersImport creates a new headersImport instance with the given options.
func NewHeadersImport(options *ImportOptions) (*headersImport, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}

	if options.WriteBatchSizePerRegion <= 0 {
		options.WriteBatchSizePerRegion = defaultWriteBatchSizePerRegion
		log.Infof("Using default write batch size of %d "+
			"headers per region", options.WriteBatchSizePerRegion)
	}

	blockHeadersSource := options.createBlockHeaderImportSrc()
	filterHeadersSource := options.createFilterHeaderImportSrc()

	blockheadersValidator := options.createBlockHeaderValidator(
		blockHeadersSource,
	)
	filterheadersValidator := options.createFilterHeaderValidator()

	importer := &headersImport{
		blockHeadersImportSource:  blockHeadersSource,
		filterHeadersImportSource: filterHeadersSource,
		blockHeadersValidator:     blockheadersValidator,
		filterHeadersValidator:    filterheadersValidator,
		options:                   options,
	}

	return importer, nil
}

// Import is a multi-pass algorithm that loads, validates, and processes headers
// from the configured import sources into the target header stores. The Import
// process is currently performed only if the target stores are completely empty
// except for gensis block/filter header otherwise it is entirely skipped. On
// first development iteration, it is designed to serve new users who don't yet
// have headers data, or existing users who are willing to reset their headers
// data.
func (h *headersImport) Import(ctx context.Context) (*ImportResult, error) {
	isFresh, err := h.isTargetFresh(
		h.options.TargetBlockHeaderStore,
		h.options.TargetFilterHeaderStore,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to detect if target stores "+
			"are fresh import failed: %w", err)
	}
	if !isFresh {
		log.Info("Skipping headers import: target header stores are " +
			"not empty")
		return &ImportResult{}, nil
	}

	result := &ImportResult{
		StartTime: time.Now(),
	}

	if err := h.openSources(); err != nil {
		return nil, fmt.Errorf("failed to open sources: %w", err)
	}
	defer h.closeSources()

	if err := h.validateSourcesCompatibility(); err != nil {
		return nil, fmt.Errorf("failed to validate compatibility of "+
			"header import sources with each other: %w", err)
	}

	if err := h.validateChainContinuity(); err != nil {
		return nil, fmt.Errorf("failed to validate continuity of "+
			"import headers chain with target chain: %v", err)
	}

	// Get header metadata from import souces. We can safely use this header
	// metadata for both block headers and filter headers since we've
	// already validated that those header metadata are compatible with each
	// other.
	metadata, err := h.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return nil, err
	}

	log.Debugf("Validating %d block headers", metadata.headersCount)
	blockHeadersIterator := h.blockHeadersImportSource.Iterator(
		0, metadata.headersCount-1,
		uint32(h.options.WriteBatchSizePerRegion),
	)

	err = h.blockHeadersValidator.Validate(ctx, blockHeadersIterator)
	if err != nil {
		return nil, fmt.Errorf("failed to validate block "+
			"headers: %w", err)
	}

	log.Debugf("Validating %d filter headers", metadata.headersCount)
	filterHeadersIterator := h.filterHeadersImportSource.Iterator(
		0, metadata.headersCount-1,
		uint32(h.options.WriteBatchSizePerRegion),
	)

	err = h.filterHeadersValidator.Validate(ctx, filterHeadersIterator)
	if err != nil {
		return nil, fmt.Errorf("failed to validate filter "+
			"headers: %w", err)
	}

	// Determine processing regions that partition the import task into
	// disjoint height ranges.
	regions, err := h.determineProcessingRegions()
	if err != nil {
		return nil, fmt.Errorf("failed to determine processing "+
			"regions: %w", err)
	}
	result.StartHeight = regions.importStartHeight
	result.EndHeight = regions.importEndHeight

	// TODO(mohamedawnallah): Process the divergence region. This includes
	// strategy/strategies for handling divergence region that may exist in
	// the target header stores while importing.

	// TODO(mohamedawnallah): process the overlap region. This mainly
	// includes a validation strategy for the overlap region between headers
	// from import and target sources.

	// Process new headers region.
	// Add headers from the import source to the target stores, extending
	// from their highest existing header up to the import source's end
	// height. This assumes the target stores are consistent and valid
	// (i.e., no divergence), allowing for safe extension with new data.
	err = h.processNewHeadersRegion(ctx, regions.newHeaders, result)
	if err != nil {
		return nil, fmt.Errorf("headers import failed: new headers "+
			"processing failed: %w", err)
	}

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	log.Infof("Headers import completed: processed %d headers "+
		"(block and filter) (added: %d, skipped: %d) from height "+
		"%d to %d in %s (%.2f headers/sec, %.2f%% new)",
		result.ProcessedCount, result.AddedCount, result.SkippedCount,
		result.StartHeight, result.EndHeight, result.Duration,
		result.HeadersPerSecond(), result.NewHeadersPercentage())

	return result, nil
}

// validateChainContinuity ensures that headers from import sources can be
// properly connected to the existing headers in the target stores.
func (h *headersImport) validateChainContinuity() error {
	// Get metadata from block header source. We can safely use this count
	// for both block headers and filter headers since we've already
	// validated that the counts match across all import sources.
	sourceMetadata, err := h.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	_, blockTipHeight, err := h.options.TargetBlockHeaderStore.ChainTip()
	if err != nil {
		return fmt.Errorf("failed to get target block header chain "+
			"tip: %w", err)
	}

	_, filterTipHeight, err := h.options.TargetFilterHeaderStore.ChainTip()
	if err != nil {
		return fmt.Errorf("failed to get target filter header chain "+
			"tip: %w", err)
	}

	// Ensure that both target header stores have the same tip height.
	// A mismatch indicates a divergence region that has not yet been
	// processed. Once a resolution strategy is implemented, this check
	// will no longer return an error, and the effective tip height will be
	// defined as the minimum of the two.
	if blockTipHeight != filterTipHeight {
		return fmt.Errorf("divergence detected between target header "+
			"store tip heights (block=%d, filter=%d)",
			blockTipHeight, filterTipHeight)
	}

	importStartHeight := sourceMetadata.startHeight
	importEndHeight := sourceMetadata.endHeight

	// If import wants to start after height 1, we'd have a gap.
	if importStartHeight > 1 {
		return fmt.Errorf("target stores contain only genesis block "+
			"(height 0) but import data starts at height %d, "+
			"creating a gap", importStartHeight)
	}

	// If import includes genesis block (starts at 0), verify it matches.
	if importStartHeight == 0 {
		err := h.verifyHeadersAtTargetHeight(importStartHeight)
		if err != nil {
			return fmt.Errorf("genesis header mismatch: %v", err)
		}
		log.Infof("Genesis headers verified, import data will extend " +
			"chain from genesis")
	} else {
		// Import starts at height 1, which connects to genesis.
		// Validate that the block header at height 1 from the import
		// source connects with the previous header in the target block
		// store.
		if err := h.validateHeaderConnection(
			importStartHeight, blockTipHeight, sourceMetadata,
		); err != nil {
			return fmt.Errorf("failed to validate header "+
				"connection: %v", err)
		}

		log.Info("Target stores contain only genesis block, import " +
			"data will extend chain from height 1")
	}

	log.Infof("Chain continuity validation successful: import data "+
		"(%d-%d) connects properly with target chain",
		importStartHeight, importEndHeight)

	return nil
}

// verifyHeadersAtTargetHeight ensures headers at the specified height match
// exactly between import and target sources by performing a byte-level
// comparison. It retrieves the header from both sources at the given height and
// verifies they are identical, returning an error if any discrepancy is found.
func (h *headersImport) verifyHeadersAtTargetHeight(height uint32) error {
	// Get header metadata from import souces. We can safely use this header
	// metadata for both block headers and filter headers since we've
	// already validated that those header metadata are compatible with each
	// other.
	headerMetadata, err := h.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	importSourceIndex := targetHeightToImportSourceIndex(
		height, headerMetadata.startHeight,
	)

	importSourceHeader, err := h.blockHeadersImportSource.GetHeader(
		importSourceIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from import "+
			"source at height %d: %w", height, err)
	}

	importSourceBlkHeader, err := assertBlockHeader(importSourceHeader)
	if err != nil {
		return err
	}

	targetBlkHeaderStore := h.options.TargetBlockHeaderStore
	targetBlkHeader, err := targetBlkHeaderStore.FetchHeaderByHeight(
		height,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from target at "+
			"height %d: %w", height, err)
	}

	sourceBlkHeaderHash := importSourceBlkHeader.BlockHash()
	targetBlkHeaderHash := targetBlkHeader.BlockHash()
	if !sourceBlkHeaderHash.IsEqual(&targetBlkHeaderHash) {
		return fmt.Errorf("block header mismatch at height %d: source "+
			"has %v but target has %v", height,
			sourceBlkHeaderHash, targetBlkHeaderHash)
	}

	importSourceHeader, err = h.filterHeadersImportSource.GetHeader(
		importSourceIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get filter header from import "+
			"source at height %d: %w", height, err)
	}

	importSourceFilterHeader, err := assertFilterHeader(importSourceHeader)
	if err != nil {
		return err
	}

	filterHeaderStore := h.options.TargetFilterHeaderStore
	targetFilterHeader, err := filterHeaderStore.FetchHeaderByHeight(
		height,
	)
	if err != nil {
		return fmt.Errorf("failed to get filter header from target at "+
			"height %d: %w", height, err)
	}

	sourceFilterHeaderHash := importSourceFilterHeader.FilterHash
	targetFilterHeaderHash := *targetFilterHeader
	if !sourceFilterHeaderHash.IsEqual(&targetFilterHeaderHash) {
		return fmt.Errorf("filter header mismatch at height %d: "+
			"source has %v but target has %v", height,
			sourceFilterHeaderHash, targetFilterHeaderHash)
	}

	log.Debugf("Headers from %s (block) and %s (filter) verified at "+
		"height %d", h.blockHeadersImportSource.GetURI(),
		h.filterHeadersImportSource.GetURI(), height)

	return nil
}

// determineProcessingRegions partitions the header height space into regions
// satisfying the MECE property (Mutually Exclusive, Collectively Exhaustive).
// This strict partitioning is critical for ensuring the idempotence of the
// overall import operation - repeated imports with the same parameters will
// produce identical results without side effects. By cleanly separating heights
// into non-overlapping regions with distinct processing logic, we can ensure
// consistent application of import policies regardless of how many times the
// operation is performed. The regions are:
//  1. Overlap: Heights common to both source and targets
//  2. Divergence: Heights where target stores differ within import range
//  3. NewHeaders: Heights in source not yet in targets
//
//nolint:lll
func (h *headersImport) determineProcessingRegions() (*processingRegions, error) {
	metadata, err := h.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get header metadata: %v", err)
	}
	importStartHeight := metadata.startHeight
	importEndHeight := metadata.endHeight

	_, bTipHeight, err := h.options.TargetBlockHeaderStore.ChainTip()
	if err != nil {
		return nil, fmt.Errorf("failed to get target block header "+
			"store chain tip: %v", err)
	}
	_, fTipHeight, err := h.options.TargetFilterHeaderStore.ChainTip()
	if err != nil {
		return nil, fmt.Errorf("failed to get target filter header "+
			"store chain tip: %v", err)
	}
	effectiveTipHeight := min(bTipHeight, fTipHeight)

	regions := &processingRegions{
		importStartHeight: importStartHeight,
		importEndHeight:   importEndHeight,
		effectiveTip:      effectiveTipHeight,
	}

	// 1. Overlap region.
	// This region contains headers that exist in both the import source and
	// target stores, from the start of the import range up to the effective
	// tip height.
	overlapStart := importStartHeight
	overlapEnd := min(effectiveTipHeight, importEndHeight)
	regions.overlap = headerRegion{
		start:  overlapStart,
		end:    overlapEnd,
		exists: overlapStart <= overlapEnd,
	}

	// 2. Divergence region.
	// This region contains headers where one store extends beyond the
	// effective tip but still within the import range. It represents
	// heights where targetblock and filter headers are out of sync and need
	// reconciliation.
	divergeStart := effectiveTipHeight + 1
	divergeEnd := min(max(bTipHeight, fTipHeight), importEndHeight)
	regions.divergence = headerRegion{
		start:  divergeStart,
		end:    divergeEnd,
		exists: bTipHeight != fTipHeight && divergeStart <= divergeEnd,
	}

	// 3. New Headers region.
	// This region contains headers that are in the import source but not
	// yet in either target store. They start one height beyond the highest
	// tip of either store (ensuring no overlap with divergence region) and
	// extend to the end of the import data. These headers need to be added
	// to both stores. It only exists if there are headers beyond both tips.
	//
	// Note: This region is supposed to be processed after handling the
	// overlap, and divergence regions, ensuring that any potential
	// inconsistencies in existing data are resolved before adding new
	// headers. This sequential processing guarantees that new headers
	// are only added on top of a verified and consistent chain state.
	newStart := max(bTipHeight, fTipHeight) + 1
	newEnd := importEndHeight
	regions.newHeaders = headerRegion{
		start:  newStart,
		end:    newEnd,
		exists: newStart <= newEnd,
	}

	return regions, nil
}

// processNewHeadersRegion imports headers from the specified region into the
// target stores. This method handles the case where headers exist in the import
// source but not in the target stores.
func (h *headersImport) processNewHeadersRegion(ctx context.Context,
	region headerRegion, result *ImportResult) error {

	if !region.exists {
		return nil
	}

	log.Infof("Adding %d new headers (block and filter) from heights "+
		"%d to %d", region.end-region.start+1, region.start, region.end)

	err := h.appendNewHeaders(ctx, region.start, region.end)
	if err != nil {
		return fmt.Errorf("failed to append new headers: %w", err)
	}

	result.ProcessedCount += int(region.end - region.start + 1)
	result.AddedCount += int(region.end - region.start + 1)

	return nil
}

// appendNewHeaders adds new headers from import source.
func (h *headersImport) appendNewHeaders(ctx context.Context, startHeight,
	endHeight uint32) error {

	metadata, err := h.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	totalHeaders := endHeight - startHeight + 1
	log.Infof("Appending %d new headers in batches of %d", totalHeaders,
		h.options.WriteBatchSizePerRegion)

	sourceStartIdx := targetHeightToImportSourceIndex(
		startHeight, metadata.startHeight,
	)
	sourceEndIdx := targetHeightToImportSourceIndex(
		endHeight, metadata.startHeight,
	)

	blockIter := h.blockHeadersImportSource.Iterator(
		sourceStartIdx, sourceEndIdx,
		uint32(h.options.WriteBatchSizePerRegion),
	)

	filterIter := h.filterHeadersImportSource.Iterator(
		sourceStartIdx, sourceEndIdx,
		uint32(h.options.WriteBatchSizePerRegion),
	)

	batchStart := startHeight
	for {
		if err := ctxCancelled(ctx); err != nil {
			return err
		}

		batchEnd, err := h.processBatch(
			blockIter, filterIter, batchStart,
		)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Move to next batch.
		batchStart = batchEnd + 1
	}

	log.Infof("Successfully added %d new headers from heights %d to %d",
		totalHeaders, startHeight, endHeight)

	return nil
}

// processBatch processes a single batch of headers from the iterators. It
// returns the batch end height on success, or an error including io.EOF when no
// more batches.
func (h *headersImport) processBatch(blockIter, filterIter HeaderIterator,
	batchStart uint32) (uint32, error) {

	blockBatch, blockErr := blockIter.ReadBatch(
		batchStart, blockIter.GetEndIndex(), blockIter.GetBatchSize(),
	)
	if blockErr == io.EOF {
		return 0, io.EOF
	}
	if blockErr != nil {
		return 0, fmt.Errorf("failed to read block headers "+
			"batch at height %d: %w", batchStart, blockErr)
	}

	// Get corresponding filter headers batch.
	filterBatch, filterErr := filterIter.ReadBatch(
		batchStart, blockIter.GetEndIndex(), blockIter.GetBatchSize(),
	)
	if filterErr == io.EOF {
		return 0, errors.New("filter headers ended before block " +
			"headers")
	}
	if filterErr != nil {
		return 0, fmt.Errorf("failed to read filter headers "+
			"batch at height %d: %w", batchStart, filterErr)
	}

	// Convert block header batches to target store format.
	blockHeaders := make([]headerfs.BlockHeader, 0, len(blockBatch))
	for _, header := range blockBatch {
		blkHeader, err := assertBlockHeader(header)
		if err != nil {
			return 0, err
		}
		blockHeaders = append(
			blockHeaders, blkHeader.BlockHeader,
		)
	}

	// Convert filter header batches to target store format.
	var filterHeaders []headerfs.FilterHeader
	filterHeaders = make(
		[]headerfs.FilterHeader, 0, len(filterBatch),
	)
	for _, header := range filterBatch {
		fHeader, err := assertFilterHeader(header)
		if err != nil {
			return 0, err
		}
		filterHeaders = append(
			filterHeaders, fHeader.FilterHeader,
		)
	}

	// The length check conditions should never be triggered during normal
	// import operations as validation occurs earlier. They serve as sanity
	// checks to catch unexpected inconsistencies.
	if len(blockHeaders) != len(filterHeaders) {
		return 0, fmt.Errorf("mismatch between block headers "+
			"(%d) and filter headers (%d)", len(blockHeaders),
			len(filterHeaders))
	}

	chainTipBlockHeader := blockHeaders[len(blockHeaders)-1]
	setLastFilterHeaderHash(filterHeaders, chainTipBlockHeader)

	batchEnd := batchStart + uint32(len(blockBatch)) - 1

	err := h.writeHeadersToTargetStores(
		blockHeaders, filterHeaders, batchStart, batchEnd,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to write headers to target "+
			"stores: %v", err)
	}

	log.Debugf("Wrote headers batch from height %d to %d "+
		"(%d headers)", batchStart, batchEnd,
		batchEnd-batchStart+1)

	return batchEnd, nil
}

// Write block and filter headers to the target stores in a specific order to
// ensure proper rollback operations if needed. It also rollbacks any headers
// written if any to target stores incase of any failures.
func (h *headersImport) writeHeadersToTargetStores(
	blockHeaders []headerfs.BlockHeader,
	filterHeaders []headerfs.FilterHeader,
	batchStart, batchEnd uint32) error {

	if err := h.options.TargetBlockHeaderStore.WriteHeaders(
		blockHeaders...,
	); err != nil {
		return fmt.Errorf("failed to write block headers "+
			"batch %d-%d: %w", batchStart, batchEnd, err)
	}

	if err := h.options.TargetFilterHeaderStore.WriteHeaders(
		filterHeaders...,
	); err != nil {
		// If we've reached here, the headers failed to be written to
		// target filter store because of I/O errors regarding binary
		// file or filter db store, and it is automatically rolled back
		// upstream.
		//
		// The whole import operation needs to satisfy the conjunction
		// property for both block and filter header stores - it's all
		// or nothing, so they need to be at the same length. We must
		// rollback the block headers to maintain this consistency.
		blkStore := h.options.TargetBlockHeaderStore
		blockHeadersToTruncate := uint32(len(blockHeaders))
		_, rollbackErr := blkStore.RollbackBlockHeaders(
			blockHeadersToTruncate,
		)
		if rollbackErr != nil {
			return fmt.Errorf("failed to rollback %d headers from "+
				"target block header store after filter "+
				"headers write failure. Block error: %w, "+
				"filter error: %v", blockHeadersToTruncate,
				rollbackErr, err)
		}

		return fmt.Errorf("failed to write filter headers "+
			"batch %d-%d: %w", batchStart, batchEnd, err)
	}

	return nil
}

// validateHeaderConnection verifies that a block header from the import source
// properly connects with the previous header in the target block store.
func (h *headersImport) validateHeaderConnection(targetStartHeight,
	prevTargetBlockHeight uint32, headerMetadata *headerMetadata) error {

	// Get the previous block header from target store.
	prevBlkHdr, err := h.options.TargetBlockHeaderStore.FetchHeaderByHeight(
		prevTargetBlockHeight,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from "+
			"target at height %d: %w", prevTargetBlockHeight, err)
	}

	// Convert target height to the equivalent index for import sources.
	importSourceIndex := targetHeightToImportSourceIndex(
		targetStartHeight, headerMetadata.startHeight,
	)

	// Get block header at that index from the import source.
	currHeader, err := h.blockHeadersImportSource.GetHeader(
		importSourceIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from "+
			"import source at height %d: %w",
			targetStartHeight, err)
	}

	currBlkHeader, err := assertBlockHeader(currHeader)
	if err != nil {
		return err
	}

	// Ensure the current header's previous block hash matches the
	// hash of the previously fetched block header to maintain chain
	// integrity.
	prevHash := prevBlkHdr.BlockHash()
	if !currBlkHeader.PrevBlock.IsEqual(&prevHash) {
		return fmt.Errorf("header chain broken: current "+
			"header's PrevBlock (%v) doesn't match "+
			"previous header's hash (%v)",
			currBlkHeader.PrevBlock, prevHash)
	}

	log.Debugf("Validated block header connection: import height %d "+
		"properly connects to target chain at height %d "+
		"(prev hash: %v)", targetStartHeight, prevTargetBlockHeight,
		prevHash)

	return nil
}

// isTargetFresh checks if the target header stores are in their initial state,
// meaning they contain only the genesis header (height 0).
func (h *headersImport) isTargetFresh(
	targetBlockHeaderStore headerfs.BlockHeaderStore,
	targetFilterHeaderStore headerfs.FilterHeaderStore) (bool, error) {

	// Get the chain tip from both target stores.
	_, blockTipHeight, err := targetBlockHeaderStore.ChainTip()
	if err != nil {
		return false, fmt.Errorf("failed to get target block header "+
			"chain tip: %w", err)
	}

	_, filterTipHeight, err := targetFilterHeaderStore.ChainTip()
	if err != nil {
		return false, fmt.Errorf("failed to get target filter header "+
			"chain tip: %w", err)
	}

	if blockTipHeight == 0 && filterTipHeight == 0 {
		return true, nil
	}

	return false, nil
}

// openSources initializes and opens all required header import sources. It
// verifies that all necessary import sources and validators are properly
// configured, then opens each source to prepare for data reading. Returns an
// error if any source is missing or fails to open.
func (h *headersImport) openSources() error {
	if h.blockHeadersImportSource == nil ||
		h.filterHeadersImportSource == nil {

		return fmt.Errorf("missing required header sources - block "+
			"headers source: %v, filter headers source: %v",
			h.blockHeadersImportSource != nil,
			h.filterHeadersImportSource != nil)
	}

	if h.blockHeadersValidator == nil || h.filterHeadersValidator == nil {
		return fmt.Errorf("missing required header validators - block "+
			"headers validator: %v, filter headers "+
			"validator: %v", h.blockHeadersValidator != nil,
			h.filterHeadersValidator != nil)
	}

	if err := h.blockHeadersImportSource.Open(); err != nil {
		return err
	}

	if err := h.filterHeadersImportSource.Open(); err != nil {
		return err
	}

	return nil
}

// closeSources safely closes all open header sources and logs any warnings
// encountered during cleanup.
func (h *headersImport) closeSources() {
	if err := h.blockHeadersImportSource.Close(); err != nil {
		log.Warnf("Failed to close block headers source: %v", err)
	}

	if err := h.filterHeadersImportSource.Close(); err != nil {
		log.Warnf("Failed to close filter headers source: %v", err)
	}
}

// validateSourcesCompatibility ensures that block and filter header sources
// are compatible with each other and with the target chain.
func (h *headersImport) validateSourcesCompatibility() error {
	blockMetadata, err := h.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return err
	}
	filterMetadata, err := h.filterHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return err
	}

	if blockMetadata.headerType != headerfs.Block {
		return fmt.Errorf("incorrect block header type: expected %s, "+
			"got %s", headerfs.Block, blockMetadata.headerType)
	}

	if filterMetadata.headerType != headerfs.RegularFilter {
		return fmt.Errorf("incorrect filter header type: expected %v, "+
			"got %v", headerfs.RegularFilter,
			filterMetadata.headerType)
	}

	if blockMetadata.networkMagic != filterMetadata.networkMagic {
		return fmt.Errorf("network type mismatch: block headers "+
			"from %s (%v), filter headers from "+
			"%s (%v)", h.blockHeadersImportSource.GetURI(),
			blockMetadata.networkMagic,
			h.filterHeadersImportSource.GetURI(),
			filterMetadata.networkMagic)
	}

	if blockMetadata.networkMagic != h.options.TargetChainParams.Net {
		return fmt.Errorf("network mismatch: headers from import "+
			"sources are for %v, but target is %v",
			blockMetadata.networkMagic,
			h.options.TargetChainParams.Net)
	}

	if blockMetadata.startHeight != filterMetadata.startHeight {
		return fmt.Errorf("start height mismatch: block headers start "+
			"at %d, filter headers start at %d",
			blockMetadata.startHeight, filterMetadata.startHeight)
	}

	if filterMetadata.headersCount != blockMetadata.headersCount {
		return fmt.Errorf("headers count mismatch: block headers "+
			"import source %s (%d), filter headers import source "+
			"%s (%d)", h.blockHeadersImportSource.GetURI(),
			blockMetadata.headersCount,
			h.filterHeadersImportSource.GetURI(),
			filterMetadata.headersCount)
	}

	return nil
}

// ImportOptions defines parameters for the import process.
type ImportOptions struct {
	// TargetChainParams specifies the blockchain network parameters for the
	// chain into which headers will be imported.
	TargetChainParams chaincfg.Params

	// TargetBlockHeaderStore is the storage backend where block headers
	// will be written during the import.
	TargetBlockHeaderStore headerfs.BlockHeaderStore

	// TargetFilterHeaderStore is the storage backend where filter headers
	// will be written during the import.
	TargetFilterHeaderStore headerfs.FilterHeaderStore

	// BlockHeadersSource is the file path or source location for block
	// headers to be imported.
	BlockHeadersSource string

	// FilterHeadersSource is the file path or source location for filter
	// headers to be imported.
	FilterHeadersSource string

	// WriteBatchSizePerRegion specifies the number of headers to write in
	// each batch per region. This controls performance characteristics of
	// the import.
	WriteBatchSizePerRegion int

	// ValidationFlags specifies the behavior flags used during header
	// validation. It defaults to BFNone.
	ValidationFlags blockchain.BehaviorFlags
}

// validate checks that all required fields in import options are properly set
// and returns an error if any validation fails.
func (options *ImportOptions) validate() error {
	if options.BlockHeadersSource == "" {
		return errors.New("missing block headers source path")
	}

	if options.FilterHeadersSource == "" {
		return errors.New("missing filter headers source path")
	}

	return nil
}

// createBlockHeaderImportSrc creates the appropriate import source for block
// headers.
func (options *ImportOptions) createBlockHeaderImportSrc() HeaderImportSource {
	// Check if the block headers source is a HTTP(s) URI.
	if strings.HasPrefix(options.BlockHeadersSource, "http") {
		// The empty string ("") URI passed will be replaced by the
		// temporary file name once the file has been downloaded from
		// the HTTP source.
		return newHTTPHeaderImportSource(
			options.BlockHeadersSource, newHTTPClient(),
			newFileHeaderImportSource("", newBlockHeader),
		)
	}

	// Otherwise, fallback to file headers import source.
	return newFileHeaderImportSource(
		options.BlockHeadersSource, newBlockHeader,
	)
}

// createFilterHeaderImportSrc creates the appropriate import source for
// filter headers.
func (options *ImportOptions) createFilterHeaderImportSrc() HeaderImportSource {
	// Check if the filter headers source is a HTTP(s) URI.
	if strings.HasPrefix(options.FilterHeadersSource, "http") {
		return newHTTPHeaderImportSource(
			options.FilterHeadersSource, newHTTPClient(),
			newFileHeaderImportSource("", newFilterHeader),
		)
	}

	// Otherwise, fallback to file headers import source.
	return newFileHeaderImportSource(
		options.FilterHeadersSource, newFilterHeader,
	)
}

// createBlockHeaderValidator creates the appropriate validator for block
// headers.
func (options *ImportOptions) createBlockHeaderValidator(
	blockHeadersImportSource HeaderImportSource) HeadersValidator {

	return newBlockHeadersImportSourceValidator(
		options.TargetChainParams, options.TargetBlockHeaderStore,
		options.ValidationFlags, blockHeadersImportSource,
	)
}

// createFilterHeaderValidator creates the appropriate validator for filter
// headers.
func (options *ImportOptions) createFilterHeaderValidator() HeadersValidator {
	return newFilterHeadersImportSourceValidator(options.TargetChainParams)
}

// ImportResult contains statistics about a header import operation.
type ImportResult struct {
	// ProcessedCount is the total number of headers examined.
	ProcessedCount int

	// AddedCount is the number of headers newly added to destination.
	AddedCount int

	// SkippedCount is the number of headers already in destination.
	SkippedCount int

	// StartHeight is the first height processed.
	StartHeight uint32

	// EndHeight is the last height processed.
	EndHeight uint32

	// StartTime is the time when import operation started.
	StartTime time.Time

	// EndTime is the time when import operation completed.
	EndTime time.Time

	// Duration is the total time taken for the import operation.
	Duration time.Duration
}

// HeadersPerSecond calculates the processing rate in headers per second as a
// performance metric. Returns 0 if Duration is zero to avoid division by zero.
func (r *ImportResult) HeadersPerSecond() float64 {
	if r.Duration.Seconds() > 0 {
		return float64(r.ProcessedCount) / r.Duration.Seconds()
	}
	return 0
}

// NewHeadersPercentage calculates the percentage of processed headers that were
// newly added (not already present in the target). Returns 0 if no headers were
// processed to avoid division by zero.
func (r *ImportResult) NewHeadersPercentage() float64 {
	if r.ProcessedCount > 0 {
		return float64(r.AddedCount) / float64(r.ProcessedCount) * 100
	}
	return 0
}
