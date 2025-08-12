package chainimport

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/headerfs"
)

// AddHeadersImportMetadata prepares a header file for import by adding the
// necessary metadata to the file. This function takes an existing header file
// and prepends metadata required by Neutrino's header import feature.
func AddHeadersImportMetadata(sourceFilePath string, chainType wire.BitcoinNet,
	headerType headerfs.HeaderType, startHeight uint32) error {

	// Open the existing file for reading.
	sourceFile, err := os.Open(sourceFilePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	// Create a temporary file pattern using the original filename.
	tmpPattern := fmt.Sprintf("%s_with_metadata_*.tmp",
		filepath.Base(sourceFilePath))

	// Create a temporary file for writing.
	tempFile, err := os.CreateTemp(filepath.Dir(sourceFilePath), tmpPattern)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tempFilePath := tempFile.Name()

	// Set up cleanup for the temporary file.
	defer func() {
		tempFile.Close()
		// Only attempt to remove if the file still exists at the path.
		if _, statErr := os.Stat(tempFilePath); statErr == nil {
			os.Remove(tempFilePath)
		}
	}()

	// Create import metadata struct and encode it.
	metadata := &importMetadata{
		bitcoinChainType: chainType,
		headerType:       headerType,
		startHeight:      startHeight,
	}

	// Encode metadata and write to temp file.
	if err = metadata.encode(tempFile); err != nil {
		return fmt.Errorf("failed to encode and write "+
			"metadata: %w", err)
	}

	// Copy the original file content to the temp file.
	if _, err = io.Copy(tempFile, sourceFile); err != nil {
		return fmt.Errorf("failed to copy original file content: %w",
			err)
	}

	// Sync the file to ensure all data is written to disk.
	if err = tempFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temporary file: %w", err)
	}

	// Replace the original file with our new file.
	if err = os.Rename(tempFilePath, sourceFilePath); err != nil {
		return fmt.Errorf("failed to replace original file: %w", err)
	}

	return nil
}

// targetHeightToImportSourceIndex converts the absolute blockchain target
// height to the equivalent import source height based on the start height
// input.
func targetHeightToImportSourceIndex(targetH, importStartH uint32) uint32 {
	return targetH - importStartH
}

// assertBlockHeader type asserts header to *blockHeader or returns error.
func assertBlockHeader(header Header) (*blockHeader, error) {
	blkHeader, ok := header.(*blockHeader)
	if !ok {
		return nil, fmt.Errorf("expected blockHeader type, got %T",
			header)
	}
	return blkHeader, nil
}

// assertFilterHeader type asserts header to *filterHeader or returns error.
func assertFilterHeader(header Header) (*filterHeader, error) {
	fHeader, ok := header.(*filterHeader)
	if !ok {
		return nil, fmt.Errorf("expected filterHeader type, got %T",
			header)
	}
	return fHeader, nil
}
