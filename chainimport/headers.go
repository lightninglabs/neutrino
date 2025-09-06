package chainimport

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/headerfs"
)

const (
	// networkMagicSize is the Network Magic size: 4 bytes.
	networkMagicSize = 4

	// versionSize is the Version size: 1 byte.
	versionSize = 1

	// headerTypeSize is the Header Type size: 1 byte.
	headerTypeSize = 1

	// startHeaderHeightSize is the Start Header Height size: 4 bytes.
	startHeaderHeightSize = 4

	// importMetadataSize is the ImportMetadata size in bytes
	// (sum of all above): 10 bytes.
	ImportMetadataSize = networkMagicSize + versionSize + headerTypeSize +
		startHeaderHeightSize
)

// blockHeader represents a block header that can be imported into the chain
// store. It wraps a headerfs.BlockHeader with additional functionality needed
// for the import process.
type blockHeader struct {
	headerfs.BlockHeader
}

// Compile-time assertion to ensure blockHeader implements HeaderBase interface.
var _ Header = (*blockHeader)(nil)

// newBlockHeader creates and returns a new block header instance.
func newBlockHeader() Header {
	return &blockHeader{
		BlockHeader: headerfs.BlockHeader{
			BlockHeader: &wire.BlockHeader{},
		},
	}
}

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

// filterHeader represents a filter header that can be imported into the chain
// store. It wraps a headerfs.FilterHeader with additional functionality needed
// for the import process.
type filterHeader struct {
	headerfs.FilterHeader
}

// Compile-time assertion to ensure filterHeader implements HeaderBase
// interface.
var _ Header = (*filterHeader)(nil)

// newFilterHeader creates and returns a new filter header instance.
func newFilterHeader() Header {
	return &filterHeader{
		FilterHeader: headerfs.FilterHeader{},
	}
}

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

// headerMetadata contains metadata for a headers import operation, including
// the range and size information for the headers being imported.
type headerMetadata struct {
	// importMetadata contains the basic import metadata
	// (network, version, etc.).
	*importMetadata

	// endHeight is the block height where the header sequence ends.
	endHeight uint32

	// headerSize is the size in bytes of each individual header.
	headerSize int

	// headersCount is the total number of headers in the sequence.
	headersCount uint32
}

// importMetadata contains the basic metadata required for importing headers,
// including network magic, version, header type, and starting height.
type importMetadata struct {
	// networkMagic identifies the Bitcoin network (mainnet, testnet, etc.).
	networkMagic wire.BitcoinNet

	// version indicates the data format version, currently 0 for
	// uncompressed headers. Future versions may support compressed header
	// formats.
	version uint8

	// headerType specifies whether these are block headers or filter
	// headers.
	headerType headerfs.HeaderType

	// startHeight is the block height where the header sequence begins.
	startHeight uint32
}

// encode writes the import metadata to the provided writer in binary format.
//
// NOTE: The writer's position is supposed to be at the beginning of the file.
func (m *importMetadata) encode(w io.Writer) error {
	err := binary.Write(w, binary.LittleEndian, m.networkMagic)
	if err != nil {
		return fmt.Errorf("failed to write chain type: %w", err)
	}

	if err = binary.Write(w, binary.LittleEndian, m.version); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	err = binary.Write(w, binary.LittleEndian, byte(m.headerType))
	if err != nil {
		return fmt.Errorf("failed to write header type: %w", err)
	}

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
	err := binary.Read(r, binary.LittleEndian, &m.networkMagic)
	if err != nil {
		return fmt.Errorf("failed to read network magic: %w", err)
	}

	if err = binary.Read(r, binary.LittleEndian, &m.version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}

	if m.version != 0 {
		return fmt.Errorf("unsupported header format version %d, "+
			"only version 0 (uncompressed) is currently supported",
			m.version)
	}

	var headerTypeByte byte
	err = binary.Read(r, binary.LittleEndian, &headerTypeByte)
	if err != nil {
		return fmt.Errorf("failed to read header type: %w", err)
	}
	m.headerType = headerfs.HeaderType(headerTypeByte)

	err = binary.Read(r, binary.LittleEndian, &m.startHeight)
	if err != nil {
		return fmt.Errorf("failed to read start height: %w", err)
	}

	return nil
}

// size returns the binary size of the import metadata in bytes.
func (m *importMetadata) size() int {
	return ImportMetadataSize
}
