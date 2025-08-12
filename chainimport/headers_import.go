package chainimport

import (
	"errors"
	"fmt"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightninglabs/neutrino/headerfs"
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
	if err := options.validate(); err != nil {
		return nil, err
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

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result, nil
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
