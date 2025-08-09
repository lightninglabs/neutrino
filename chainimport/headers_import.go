package chainimport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightninglabs/neutrino/headerfs"
)

const (
	// defaultWriteBatchSizePerRegion defines the default number of headers
	// to process in a single batch when no specific batch size is provided.
	defaultWriteBatchSizePerRegion = 16384
)

// processingRegions currently contains regions to process. Those regions
// overlap, divergence, and new headers region are all detected
// but only new headers region processed.
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
	blockHeadersImportSource  HeaderImportSource
	filterHeadersImportSource HeaderImportSource
	blockheadersValidator     HeadersValidator
	filterheadersValidator    HeadersValidator
	options                   *ImportOptions
}

// NewHeadersImport creates a new headersImport instance with the given options.
// It validates required sources, sets defaults, and constructs the necessary
// components for header import operations.
func NewHeadersImport(options *ImportOptions) (*headersImport, error) {
	// Validate required sources.
	if options.BlockHeadersSource == "" {
		return nil, errors.New("missing block headers source path")
	}

	if options.FilterHeadersSource == "" {
		return nil, errors.New("missing filter headers source path")
	}

	// Set default batch size if not specified.
	if options.WriteBatchSizePerRegion <= 0 {
		options.WriteBatchSizePerRegion = defaultWriteBatchSizePerRegion
		log.Infof("Using default write batch size of %d "+
			"headers per region", options.WriteBatchSizePerRegion)
	}

	// Construct header import sources.
	blockHeadersSource := options.createBlockHeaderImportSrc()
	filterHeadersSource := options.createFilterHeaderImportSrc()

	// Create validators for headers data.
	blockheadersValidator := options.createBlockHeaderValidator()
	filterheadersValidator := options.createFilterHeaderValidator()

	// Construct the importer with all required components.
	importer := &headersImport{
		blockHeadersImportSource:  blockHeadersSource,
		filterHeadersImportSource: filterHeadersSource,
		blockheadersValidator:     blockheadersValidator,
		filterheadersValidator:    filterheadersValidator,
		options:                   options,
	}

	return importer, nil
}

// Import is a multi-pass algorithm that loads, validates, and processes
// headers from the configured import sources into the target header stores. The
// Import process is currently performed only if the target stores are
// completely empty except for gensis block/filter header otherwise it is
// entirely skipped. On first development iteration, it is designed to serve new
// users who don't yet have headers data, or existing users who are willing to
// reset their headers data.
func (s *headersImport) Import(ctx context.Context) (*ImportResult, error) {
	// Check first if the target header stores are fresh.
	isFresh, err := s.isTargetFresh(
		s.options.TargetBlockHeaderStore,
		s.options.TargetFilterHeaderStore,
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

	// Create result structure at the beginning to capture total duration.
	result := &ImportResult{
		StartTime: time.Now(),
	}

	// Initialize and open header sources before proceeding with validation
	// and import operations.
	if err := s.openSources(); err != nil {
		return nil, fmt.Errorf("failed to open sources: %w", err)
	}
	defer s.closeSources()

	// Check if the sources are compatible with each other.
	if err := s.validateSourcesCompatibility(); err != nil {
		return nil, fmt.Errorf("failed to validate compatibility of "+
			"header import sources with each other: %w", err)
	}

	// Validate chain continuity with the target chain.
	if err := s.validateChainContinuity(); err != nil {
		return nil, fmt.Errorf("failed to validate continuity of "+
			"import headers chain with target chain: %v", err)
	}

	// Get header metadata from import souces. We can safely use this header
	// metadata for both block headers and filter headers since we've
	// already validated that those header metadata are compatible with each
	// other.
	metadata, err := s.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return nil, err
	}

	// Validate all block headers from import source.
	log.Debugf("Validating %d block headers", metadata.headersCount)
	blockHeadersIterator := s.blockHeadersImportSource.Iterator(
		0, metadata.headersCount-1,
		uint32(s.options.WriteBatchSizePerRegion),
	)

	// Reset block header iterator for stateful iteration.
	blockHeadersIterator.Reset(0, metadata.headersCount-1)

	if err := s.blockheadersValidator.Validate(
		ctx, blockHeadersIterator, s.options.TargetChainParams,
	); err != nil {
		return nil, fmt.Errorf("failed to validate block "+
			"headers: %w", err)
	}

	// Validate all filter headers from import source.
	log.Debugf("Validating %d filter headers", metadata.headersCount)
	filterHeadersIterator := s.filterHeadersImportSource.Iterator(
		0, metadata.headersCount-1,
		uint32(s.options.WriteBatchSizePerRegion),
	)

	// Reset filter header iterator for stateful iteration.
	filterHeadersIterator.Reset(0, metadata.headersCount-1)

	if err := s.filterheadersValidator.Validate(
		ctx, filterHeadersIterator, s.options.TargetChainParams,
	); err != nil {
		return nil, fmt.Errorf("failed to validate filter "+
			"headers: %w", err)
	}

	// Determine processing regions that partition the import task into
	// disjoint height ranges.
	regions, err := s.determineProcessingRegions()
	if err != nil {
		return nil, fmt.Errorf("failed to determine processing "+
			"regions: %w", err)
	}
	result.StartHeight = regions.importStartHeight
	result.EndHeight = regions.importEndHeight

	// TODO(mohamedawnallah): process the overlap region. This mainly
	// includes a validation strategy for the overlap region between headers
	// from import and target sources.

	// TODO(mohamedawnallah): Process the divergence region. This includes
	// strategy/strategies for handling divergence region that may exist in
	// the target header stores while importing.

	// Process new headers region.
	// Add headers from the import source to the target stores, extending
	// from their highest existing header up to the import source's end
	// height. This assumes the target stores are consistent and valid
	// (i.e., no divergence), allowing for safe extension with new data.
	err = s.processNewHeadersRegion(ctx, regions.newHeaders, result)
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

// isTargetFresh checks if the target header stores are in their initial state,
// meaning they contain only the genesis header (height 0).
func (s *headersImport) isTargetFresh(
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
func (s *headersImport) openSources() error {
	// Check if required import sources are provided.
	if s.blockHeadersImportSource == nil ||
		s.filterHeadersImportSource == nil {

		return fmt.Errorf("missing required header sources - block "+
			"headers source: %v, filter headers source: %v",
			s.blockHeadersImportSource != nil,
			s.filterHeadersImportSource != nil)
	}

	// Check if required validators are provided.
	if s.blockheadersValidator == nil || s.filterheadersValidator == nil {
		return fmt.Errorf("missing required header validators - block "+
			"headers validator: %v, filter headers "+
			"validator: %v", s.blockheadersValidator != nil,
			s.filterheadersValidator != nil)
	}

	// Open block headers import source.
	if err := s.blockHeadersImportSource.Open(); err != nil {
		return err
	}

	// Open filter headers import source.
	if err := s.filterHeadersImportSource.Open(); err != nil {
		return err
	}

	return nil
}

// closeSources safely closes all open header sources and logs any warnings
// encountered during cleanup.
func (s *headersImport) closeSources() {
	// Close block headers source.
	if err := s.blockHeadersImportSource.Close(); err != nil {
		log.Warnf("Failed to close block headers source: %v", err)
	}

	// Close filter headers source.
	if err := s.filterHeadersImportSource.Close(); err != nil {
		log.Warnf("Failed to close filter headers source: %v", err)
	}
}

// validateSourcesCompatibility ensures that block and filter header sources
// are compatible with each other and with the target chain.
func (s *headersImport) validateSourcesCompatibility() error {
	// Get metadata from both header and filter sources.
	blockMetadata, err := s.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return err
	}
	filterMetadata, err := s.filterHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return err
	}

	// Validate that block headers are of the correct type.
	if blockMetadata.headerType != headerfs.Block {
		return fmt.Errorf("incorrect block header type: expected %s, "+
			"got %s", headerfs.Block, blockMetadata.headerType)
	}

	// Validate that filter headers are of the correct type.
	if filterMetadata.headerType != headerfs.RegularFilter {
		return fmt.Errorf("incorrect filter header type: expected %v, "+
			"got %v", headerfs.RegularFilter,
			filterMetadata.headerType)
	}

	// Validate that network types match.
	if blockMetadata.bitcoinChainType != filterMetadata.bitcoinChainType {
		return fmt.Errorf("network type mismatch: block headers "+
			"from %s (%v), filter headers from "+
			"%s (%v)", s.blockHeadersImportSource.GetURI(),
			blockMetadata.bitcoinChainType,
			s.filterHeadersImportSource.GetURI(),
			filterMetadata.bitcoinChainType)
	}

	// Validate that the source network matches the target network.
	if blockMetadata.bitcoinChainType != s.options.TargetChainParams.Net {
		return fmt.Errorf("network mismatch: headers from import "+
			"sources are for %v, but target is %v",
			blockMetadata.bitcoinChainType,
			s.options.TargetChainParams.Net)
	}

	// Validate starting heights match.
	if blockMetadata.startHeight != filterMetadata.startHeight {
		return fmt.Errorf("start height mismatch: block headers start "+
			"at %d, filter headers start at %d",
			blockMetadata.startHeight, filterMetadata.startHeight)
	}

	// Validate header counts match exactly. This also implicitly validates
	// the end height. The end height is constructed from the start height
	// and headers count.
	if filterMetadata.headersCount != blockMetadata.headersCount {
		return fmt.Errorf("headers count mismatch: block headers "+
			"import source %s (%d), filter headers import source "+
			"%s (%d)", s.blockHeadersImportSource.GetURI(),
			blockMetadata.headersCount,
			s.filterHeadersImportSource.GetURI(),
			filterMetadata.headersCount)
	}

	return nil
}

// validateChainContinuity ensures that headers from import sources can be
// properly connected to the existing headers in the target stores.
func (s *headersImport) validateChainContinuity() error {
	// Get metadata from block header source. We can safely use this count
	// for both block headers and filter headers since we've already
	// validated that the counts match across all import sources.
	sourceMetadata, err := s.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	// Get the chain tip from both target stores.
	_, blockTipHeight, err := s.options.TargetBlockHeaderStore.ChainTip()
	if err != nil {
		return fmt.Errorf("failed to get target block header chain "+
			"tip: %w", err)
	}

	_, filterTipHeight, err := s.options.TargetFilterHeaderStore.ChainTip()
	if err != nil {
		return fmt.Errorf("failed to get target filter header chain "+
			"tip: %w", err)
	}

	// Ensure that both target header stores have the same tip height.
	// A mismatch indicates a divergence region that has not yet been
	// processed. Once a resolution strategy is implemented, this check
	// will no longer return an error, and the effective tip height
	// will be defined as the minimum of the two.
	if blockTipHeight != filterTipHeight {
		return fmt.Errorf("divergence detected between target header "+
			"store tip heights (block=%d, filter=%d)",
			blockTipHeight, filterTipHeight)
	}

	// Extract import height range.
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
		err := s.verifyHeadersAtTargetHeight(importStartHeight)
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
		if err := s.validateHeaderConnection(
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

// validateHeaderConnection verifies that a block header from the import source
// properly connects with the previous header in the target block store.
func (s *headersImport) validateHeaderConnection(targetStartHeight,
	prevTargetBlockHeight uint32, headerMetadata *headerMetadata) error {

	// Get the previous block header from target store.
	prevBlkHdr, err := s.options.TargetBlockHeaderStore.FetchHeaderByHeight(
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
	currHeader, err := s.blockHeadersImportSource.GetHeader(
		importSourceIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from "+
			"import source at height %d: %w",
			targetStartHeight, err)
	}

	// Type assert the header retrieved from import source to be block
	// header.
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
func (s *headersImport) determineProcessingRegions() (*processingRegions, error) {
	// Get header metadata for import sources. The start and end height
	// constraint range validated upstream.
	metadata, err := s.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get header metadata: %v", err)
	}
	importStartHeight := metadata.startHeight
	importEndHeight := metadata.endHeight

	// Get chain tips for target stores.
	_, bTipHeight, err := s.options.TargetBlockHeaderStore.ChainTip()
	if err != nil {
		return nil, fmt.Errorf("failed to get target block header "+
			"store chain tip: %v", err)
	}
	_, fTipHeight, err := s.options.TargetFilterHeaderStore.ChainTip()
	if err != nil {
		return nil, fmt.Errorf("failed to get target filter header "+
			"store chain tip: %v", err)
	}
	effectiveTipHeight := min(bTipHeight, fTipHeight)

	// Create regions construct.
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

// verifyHeadersAtTargetHeight ensures headers at the specified height match
// exactly between import and target sources by performing a byte-level
// comparison.
//
// The function retrieves the header from both sources at the given height and
// verifies they are identical, returning an error if any discrepancy is found.
func (s *headersImport) verifyHeadersAtTargetHeight(height uint32) error {
	// Get header metadata from import souces. We can safely use this header
	// metadata for both block headers and filter headers since we've
	// already validated that those header metadata are compatible with each
	// other.
	headerMetadata, err := s.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	// Convert target height to the equivalent index for import sources.
	importSourceIndex := targetHeightToImportSourceIndex(
		height, headerMetadata.startHeight,
	)

	// Get block header at that index from the import source.
	importSourceHeader, err := s.blockHeadersImportSource.GetHeader(
		importSourceIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from import "+
			"source at height %d: %w", height, err)
	}

	// Type assert the header retrieved from import source to be block header.
	importSourceBlkHeader, err := assertBlockHeader(importSourceHeader)
	if err != nil {
		return err
	}

	// Get the equivalent block header from target store.
	targetBlkHeaderStore := s.options.TargetBlockHeaderStore
	targetBlkHeader, err := targetBlkHeaderStore.FetchHeaderByHeight(
		height,
	)
	if err != nil {
		return fmt.Errorf("failed to get block header from target at "+
			"height %d: %w", height, err)
	}

	// Compare block headers from import and target sources.
	sourceBlkHeaderHash := importSourceBlkHeader.BlockHash()
	targetBlkHeaderHash := targetBlkHeader.BlockHash()
	if !sourceBlkHeaderHash.IsEqual(&targetBlkHeaderHash) {
		return fmt.Errorf("block header mismatch at height %d: source "+
			"has %v but target has %v", height,
			sourceBlkHeaderHash, targetBlkHeaderHash)
	}

	// Get and verify filter headers. We can safely use the same import
	// source index used for import source block header store since we've
	// validated earlier their start height and headers count match exactly.
	importSourceHeader, err = s.filterHeadersImportSource.GetHeader(
		importSourceIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get filter header from import "+
			"source at height %d: %w", height, err)
	}

	// Type assert the header retrieved from import source to be filter header.
	importSourceFilterHeader, err := assertFilterHeader(importSourceHeader)
	if err != nil {
		return err
	}

	// Get the equivalent filter header from target store.s
	filterHeaderStore := s.options.TargetFilterHeaderStore
	targetFilterHeader, err := filterHeaderStore.FetchHeaderByHeight(
		height,
	)
	if err != nil {
		return fmt.Errorf("failed to get filter header from target at "+
			"height %d: %w", height, err)
	}

	// Compare filter headers from import and target sources.
	sourceFilterHeaderHash := importSourceFilterHeader.FilterHash
	targetFilterHeaderHash := *targetFilterHeader
	if !sourceFilterHeaderHash.IsEqual(&targetFilterHeaderHash) {
		return fmt.Errorf("filter header mismatch at height %d: "+
			"source has %v but target has %v", height,
			sourceFilterHeaderHash, targetFilterHeaderHash)
	}

	log.Debugf("Headers from %s (block) and %s (filter) verified at "+
		"height %d", s.blockHeadersImportSource.GetURI(),
		s.filterHeadersImportSource.GetURI(), height)

	return nil
}

// processNewHeadersRegion imports headers from the specified region into the
// target stores. This method handles the case where headers exist in the import
// source but not in the target stores.
func (s *headersImport) processNewHeadersRegion(ctx context.Context,
	region headerRegion, result *ImportResult) error {

	if !region.exists {
		return nil
	}

	log.Infof("Adding %d new headers (block and filter) from heights "+
		"%d to %d", region.end-region.start+1, region.start, region.end)

	err := s.appendNewHeaders(ctx, region.start, region.end)
	if err != nil {
		return fmt.Errorf("failed to append new headers: %w", err)
	}

	result.ProcessedCount += int(region.end - region.start + 1)
	result.AddedCount += int(region.end - region.start + 1)

	return nil
}

// appendNewHeaders adds new headers from import source.
func (s *headersImport) appendNewHeaders(ctx context.Context, startHeight,
	endHeight uint32) error {

	// Get metadata from sources.
	metadata, err := s.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get block header "+
			"metadata: %w", err)
	}

	totalHeaders := endHeight - startHeight + 1
	log.Infof("Appending %d new headers in batches of %d", totalHeaders,
		s.options.WriteBatchSizePerRegion)

	// Calculate indices in the source based on the metadata start height.
	sourceStartIdx := targetHeightToImportSourceIndex(
		startHeight, metadata.startHeight,
	)
	sourceEndIdx := targetHeightToImportSourceIndex(
		endHeight, metadata.startHeight,
	)

	// Create iterators with the configured batch size.
	blockIter := s.blockHeadersImportSource.Iterator(
		sourceStartIdx, sourceEndIdx,
		uint32(s.options.WriteBatchSizePerRegion),
	)
	defer blockIter.Close()

	filterIter := s.filterHeadersImportSource.Iterator(
		sourceStartIdx, sourceEndIdx,
		uint32(s.options.WriteBatchSizePerRegion),
	)
	defer filterIter.Close()

	// Reset iterators for stateful iteration.
	blockIter.Reset(sourceStartIdx, sourceEndIdx)
	filterIter.Reset(sourceStartIdx, sourceEndIdx)

	batchStart := startHeight
	for {
		// Check for context cancellation before processing each batch.
		if err := ctxCancelled(ctx); err != nil {
			return err
		}

		batchEnd, err := s.processBatch(
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
func (s *headersImport) processBatch(blockIter, filterIter HeaderIterator,
	batchStart uint32) (uint32, error) {

	// Get next block headers batch.
	blockBatch, blockErr := blockIter.NextBatch()
	if blockErr == io.EOF {
		return 0, io.EOF
	}
	if blockErr != nil {
		return 0, fmt.Errorf("failed to read block headers "+
			"batch: %w", blockErr)
	}

	// Get corresponding filter headers batch.
	filterBatch, filterErr := filterIter.NextBatch()
	if filterErr == io.EOF {
		return 0, errors.New("filter headers ended before block " +
			"headers")
	}
	if filterErr != nil {
		return 0, fmt.Errorf("failed to read filter headers "+
			"batch: %w", filterErr)
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

	// Calculate batch end based on actual batch size.
	batchEnd := batchStart + uint32(len(blockBatch)) - 1

	// The length check conditions should never be triggered during
	// normal import operations as validation occurs earlier. They
	// serve as sanity checks to catch unexpected inconsistencies.
	if len(blockHeaders) != len(filterHeaders) {
		return 0, fmt.Errorf("mismatch between block headers "+
			"(%d) and filter headers (%d) for batch %d-%d",
			len(blockHeaders), len(filterHeaders),
			batchStart, batchEnd)
	}
	if len(blockHeaders) == 0 {
		return 0, fmt.Errorf("no headers read for "+
			"batch %d-%d", batchStart, batchEnd)
	}

	setLastFilterHeaderHash(filterHeaders, blockHeaders)

	err := s.writeHeadersToTargetStores(
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
func (s *headersImport) writeHeadersToTargetStores(
	blockHeaders []headerfs.BlockHeader,
	filterHeaders []headerfs.FilterHeader,
	batchStart, batchEnd uint32) error {

	// Write block headers batch to target store.
	if err := s.options.TargetBlockHeaderStore.WriteHeaders(
		blockHeaders...,
	); err != nil {
		return fmt.Errorf("failed to write block headers "+
			"batch %d-%d: %w", batchStart, batchEnd, err)
	}

	// Write filter headers batch to target store.
	if err := s.options.TargetFilterHeaderStore.WriteHeaders(
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
		blkStore := s.options.TargetBlockHeaderStore
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

// ImportOptions defines parameters for the import process.
type ImportOptions struct {
	// TargetChainParams specifies the blockchain network parameters
	// for the chain into which headers will be imported.
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
}

// createBlockHeaderImportSrc creates the appropriate import source for block
// headers.
func (options *ImportOptions) createBlockHeaderImportSrc() HeaderImportSource {
	// Check if the block headers source is a HTTP(s) URI.
	if strings.HasPrefix(options.BlockHeadersSource, "http") {
		// The empty string ("") URI passed to newFileheaderImportSource
		// will be replaced by the temporary file name once the file has
		// been downloaded from the HTTP source.
		return newHTTPheaderImportSource(
			options.BlockHeadersSource, newHTTPClient(),
			newFileheaderImportSource("", newBlockHeader),
		)
	}

	// Otherwise, fallback to file headers import source.
	return newFileheaderImportSource(
		options.BlockHeadersSource, newBlockHeader,
	)
}

// createBlockHeaderValidator creates the appropriate validator for block
// headers.
func (options *ImportOptions) createBlockHeaderValidator() HeadersValidator {
	return newBlockHeadersImportSourceValidator()
}

// createFilterHeaderImportSrc creates the appropriate import source for
// filter headers.
func (options *ImportOptions) createFilterHeaderImportSrc() HeaderImportSource {
	// Check if the filter headers source is a HTTP(s) URI.
	if strings.HasPrefix(options.FilterHeadersSource, "http") {
		return newHTTPheaderImportSource(
			options.FilterHeadersSource, newHTTPClient(),
			newFileheaderImportSource("", newFilterHeader),
		)
	}

	// Otherwise, fallback to file headers import source.
	return newFileheaderImportSource(
		options.FilterHeadersSource, newFilterHeader,
	)
}

// createFilterHeaderValidator creates the appropriate validator for filter
// headers.
func (options *ImportOptions) createFilterHeaderValidator() HeadersValidator {
	return newFilterHeadersImportSourceValidator()
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
