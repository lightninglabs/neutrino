package chainimport

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/headerfs"
)

const (
	// bitcoinChainTypeSize is the Bitcoin Chain Type size: 4 bytes.
	bitcoinChainTypeSize = 4

	// headerTypeSize is the Header Type size: 1 byte.
	headerTypeSize = 1

	// startHeaderHeightSize is the Start Header Height size: 4 bytes.
	startHeaderHeightSize = 4

	// importMetadataSize is the ImportMetadata size in bytes
	// (sum of all above): 9 bytes.
	//
	//nolint:lll
	importMetadataSize = bitcoinChainTypeSize + headerTypeSize + startHeaderHeightSize
)

// blockHeader represents a block header that can be imported into the chain
// store. It wraps a headerfs.BlockHeader with additional functionality needed
// for the import process.
type blockHeader struct {
	headerfs.BlockHeader
}

// Compile-time assertion to ensure blockHeader implements HeaderBase interface.
var _ Header = (*blockHeader)(nil)

// Deserialize reconstructs a block header from binary data at the specified
// height.
func (b *blockHeader) Deserialize(r io.Reader, height uint32) error {
	// Deserialize the wire.BlockHeader portion.
	if err := b.BlockHeader.BlockHeader.Deserialize(r); err != nil {
		return fmt.Errorf("failed to deserialize wire.BlockHeader: "+
			"%w", err)
	}

	// Set block header height.
	b.BlockHeader.Height = height

	return nil
}

// newBlockHeader creates and returns a new empty blockHeader instance.
func newBlockHeader() Header {
	return &blockHeader{
		BlockHeader: headerfs.BlockHeader{
			BlockHeader: &wire.BlockHeader{},
		},
	}
}

// filterHeader represents a filter header that can be imported into
// the chain store. It wraps a headerfs.FilterHeader with additional
// functionality needed for the import process.
type filterHeader struct {
	headerfs.FilterHeader
}

// Compile-time assertion to ensure filterHeader implements
// HeaderBase interface.
var _ Header = (*filterHeader)(nil)

// Deserialize reconstructs a filter header from binary data at the specified
// height.
func (f *filterHeader) Deserialize(r io.Reader, height uint32) error {
	// Read the filter hash (32 bytes).
	if _, err := io.ReadFull(r, f.FilterHash[:]); err != nil {
		return fmt.Errorf("failed to read filter hash: %w", err)
	}

	// Set filter header height.
	f.FilterHeader.Height = height

	return nil
}

// newFilterHeader creates and returns a new empty filterHeader instance. It is
// part of HeaderFactory interface.
func newFilterHeader() Header {
	return &filterHeader{
		FilterHeader: headerfs.FilterHeader{},
	}
}

// headerMetadata contains metadata for a headers import operation, including
// the range and size information for the headers being imported.
type headerMetadata struct {
	*importMetadata
	endHeight    uint32
	headerSize   int
	headersCount uint32
}

// importMetadata contains the basic metadata required for importing headers,
// including the Bitcoin chain type, header type, and starting height.
type importMetadata struct {
	bitcoinChainType wire.BitcoinNet
	headerType       headerfs.HeaderType
	startHeight      uint32
}

// encode writes the import metadata to the provided writer in binary format.
//
// NOTE: The writer's position is supposed to be at the beginning of the file.
func (m *importMetadata) encode(w io.Writer) error {
	// Write chainType (4 bytes).
	err := binary.Write(w, binary.LittleEndian, m.bitcoinChainType)
	if err != nil {
		return fmt.Errorf("failed to write chain type: %w", err)
	}

	// Write headerType (1 byte).
	err = binary.Write(w, binary.LittleEndian, byte(m.headerType))
	if err != nil {
		return fmt.Errorf("failed to write header type: %w", err)
	}

	// Write startHeight (4 bytes).
	err = binary.Write(w, binary.LittleEndian, m.startHeight)
	if err != nil {
		return fmt.Errorf("failed to write start height: %w", err)
	}

	return nil
}

// decode reads the import metadata from the provided reader.
//
// NOTE: The reader's position is supposed to be at the beginning of the file.
func (m *importMetadata) decode(r io.Reader) error {
	// Read chainType (4 bytes).
	err := binary.Read(r, binary.LittleEndian, &m.bitcoinChainType)
	if err != nil {
		return fmt.Errorf("failed to read chain type: %w", err)
	}

	// Read headerType (1 byte).
	var headerTypeByte byte
	err = binary.Read(r, binary.LittleEndian, &headerTypeByte)
	if err != nil {
		return fmt.Errorf("failed to read header type: %w", err)
	}
	m.headerType = headerfs.HeaderType(headerTypeByte)

	// Read startHeight (4 bytes).
	err = binary.Read(r, binary.LittleEndian, &m.startHeight)
	if err != nil {
		return fmt.Errorf("failed to read start height: %w", err)
	}

	return nil
}

// size returns the binary size of the import metadata in bytes.
func (m *importMetadata) size() int {
	return importMetadataSize
}
