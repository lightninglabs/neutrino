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
