package chainimport

import (
	"errors"

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

// Import import headers data in target header stores.
func (h *headersImport) Import() error {
	return nil
}

// ImportOptions defines options for the import process.
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
