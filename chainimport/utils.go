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
func AddHeadersImportMetadata(sourceFilePath string,
	networkMagic wire.BitcoinNet, version uint8,
	headerType headerfs.HeaderType, startHeight uint32) error {

	sourceFile, err := os.Open(sourceFilePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	tmpPattern := fmt.Sprintf("%s_with_metadata_*.tmp",
		filepath.Base(sourceFilePath))

	tempFile, err := os.CreateTemp(filepath.Dir(sourceFilePath), tmpPattern)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tempFilePath := tempFile.Name()

	defer func() {
		tempFile.Close()
		if _, statErr := os.Stat(tempFilePath); statErr == nil {
			os.Remove(tempFilePath)
		}
	}()

	metadata := &importMetadata{
		networkMagic: networkMagic,
		version:      version,
		headerType:   headerType,
		startHeight:  startHeight,
	}

	if err = metadata.encode(tempFile); err != nil {
		return fmt.Errorf("failed to encode and write "+
			"metadata: %w", err)
	}

	if _, err = io.Copy(tempFile, sourceFile); err != nil {
		return fmt.Errorf("failed to copy original file content: %w",
			err)
	}

	if err = tempFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temporary file: %w", err)
	}

	if err = os.Rename(tempFilePath, sourceFilePath); err != nil {
		return fmt.Errorf("failed to replace original file: %w", err)
	}

	return nil
}

// setLastFilterHeaderHash updates the HeaderHash of the last filter header to
// match the block hash of the corresponding block header. This maintains chain
// tip consistency for the regular tip.
func setLastFilterHeaderHash(filterHeaders []headerfs.FilterHeader,
	blockHeaders []headerfs.BlockHeader) {

	// We only need to set the block header hash of the last filter header
	// to maintain chain tip consistency for regular tip.
	lastIdx := len(filterHeaders) - 1
	chainTipHash := blockHeaders[lastIdx].BlockHeader.BlockHash()
	filterHeaders[lastIdx].HeaderHash = chainTipHash
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
