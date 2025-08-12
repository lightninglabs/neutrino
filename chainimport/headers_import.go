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
	options *ImportOptions
}

// NewHeadersImport creates a new headersImport instance with the given options.
func NewHeadersImport(options *ImportOptions) (*headersImport, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}

	importer := &headersImport{
		options: options,
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

	return &ImportResult{}, nil
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
