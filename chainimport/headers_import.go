package chainimport

import (
	"errors"
	"fmt"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightninglabs/neutrino/headerfs"
)

const (
	// defaultWriteBatchSizePerRegion defines the default number of headers
	// to process in a single batch when no specific batch size is provided.
	defaultWriteBatchSizePerRegion = 16384
)

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

	// options contains configuration parameters for the import process.
	options *ImportOptions
}

// NewHeadersImport creates a new headersImport instance with the given options.
func NewHeadersImport(options *ImportOptions) (*headersImport, error) {
	// First validate import options.
	if err := options.validate(); err != nil {
		return nil, err
	}

	// Set default batch size if not specified.
	if options.WriteBatchSizePerRegion <= 0 {
		options.WriteBatchSizePerRegion = defaultWriteBatchSizePerRegion
		log.Infof("Using default write batch size of %d "+
			"headers per region", options.WriteBatchSizePerRegion)
	}

	blockHeadersSource := options.createBlockHeaderImportSrc()
	filterHeadersSource := options.createFilterHeaderImportSrc()

	importer := &headersImport{
		blockHeadersImportSource:  blockHeadersSource,
		filterHeadersImportSource: filterHeadersSource,
		options:                   options,
	}

	return importer, nil
}

// Import import headers data in target header stores. The Import process is
// currently performed only if the target stores are completely empty except for
// gensis block/filter header otherwise it is entirely skipped. On first
// development iteration, it is designed to serve new users who don't yet have
// headers data, or existing users who are willing to reset their headers data.
func (h *headersImport) Import() (*ImportResult, error) {
	// Check first if the target header stores are fresh.
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

	// Initialize and open header sources before proceeding with validation
	// and import operations.
	if err := h.openSources(); err != nil {
		return nil, fmt.Errorf("failed to open sources: %w", err)
	}
	defer h.closeSources()

	// Check if the sources are compatible with each other.
	if err := h.validateSourcesCompatibility(); err != nil {
		return nil, fmt.Errorf("failed to validate compatibility of "+
			"header import sources with each other: %w", err)
	}

	// Validate chain continuity with the target chain.
	if err := h.validateChainContinuity(); err != nil {
		return nil, fmt.Errorf("failed to validate continuity of "+
			"import headers chain with target chain: %v", err)
	}

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

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

	// Get the chain tip from both target stores.
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
// comparison.
//
// The function retrieves the header from both sources at the given height and
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

	// Convert target height to the equivalent index for import sources.
	importSourceIndex := targetHeightToImportSourceIndex(
		height, headerMetadata.startHeight,
	)

	// Get block header at that index from the import source.
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

	// Get the equivalent block header from target store.
	targetBlkHeaderStore := h.options.TargetBlockHeaderStore
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

	// Get the equivalent filter header from target store.s
	filterHeaderStore := h.options.TargetFilterHeaderStore
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
		"height %d", h.blockHeadersImportSource.GetURI(),
		h.filterHeadersImportSource.GetURI(), height)

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
	// Check if required import sources are provided.
	if h.blockHeadersImportSource == nil ||
		h.filterHeadersImportSource == nil {

		return fmt.Errorf("missing required header sources - block "+
			"headers source: %v, filter headers source: %v",
			h.blockHeadersImportSource != nil,
			h.filterHeadersImportSource != nil)
	}

	// Open block headers import source.
	if err := h.blockHeadersImportSource.Open(); err != nil {
		return err
	}

	// Open filter headers import source.
	if err := h.filterHeadersImportSource.Open(); err != nil {
		return err
	}

	return nil
}

// closeSources safely closes all open header sources and logs any warnings
// encountered during cleanup.
func (h *headersImport) closeSources() {
	// Close block headers source.
	if err := h.blockHeadersImportSource.Close(); err != nil {
		log.Warnf("Failed to close block headers source: %v", err)
	}

	// Close filter headers source.
	if err := h.filterHeadersImportSource.Close(); err != nil {
		log.Warnf("Failed to close filter headers source: %v", err)
	}
}

// validateSourcesCompatibility ensures that block and filter header sources
// are compatible with each other and with the target chain.
func (h *headersImport) validateSourcesCompatibility() error {
	// Get metadata from both header and filter sources.
	blockMetadata, err := h.blockHeadersImportSource.GetHeaderMetadata()
	if err != nil {
		return err
	}
	filterMetadata, err := h.filterHeadersImportSource.GetHeaderMetadata()
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
			"%s (%v)", h.blockHeadersImportSource.GetURI(),
			blockMetadata.bitcoinChainType,
			h.filterHeadersImportSource.GetURI(),
			filterMetadata.bitcoinChainType)
	}

	// Validate that the source network matches the target network.
	if blockMetadata.bitcoinChainType != h.options.TargetChainParams.Net {
		return fmt.Errorf("network mismatch: headers from import "+
			"sources are for %v, but target is %v",
			blockMetadata.bitcoinChainType,
			h.options.TargetChainParams.Net)
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
			"%s (%d)", h.blockHeadersImportSource.GetURI(),
			blockMetadata.headersCount,
			h.filterHeadersImportSource.GetURI(),
			filterMetadata.headersCount)
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

// validate checks that all required fields in ImportOptions are properly set
// and returns an error if any validation fails.
func (options *ImportOptions) validate() error {
	// Validate required sources.
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
	return newFileHeaderImportSource(
		options.BlockHeadersSource, newBlockHeader,
	)
}

// createFilterHeaderImportSrc creates the appropriate import source for
// filter headers.
func (options *ImportOptions) createFilterHeaderImportSrc() HeaderImportSource {
	return newFileHeaderImportSource(
		options.FilterHeadersSource, newFilterHeader,
	)
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
