package sideload

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/tlv"
)

const (
	// blockHeaderSize is the size one block header occupies in bytes.
	blockHeaderSize dataSize = 80
)

// binReader is an internal struct that holds all data the binReader needs
// to fetch headers.
// Each file has a header of varint size consisting of,
//
// dataType || extra metadata (e.g. filterType) || startHeight || endHeight
// || chain
// in that order.
type binReader struct {
	// reader represents the source to be read.
	reader io.ReadSeeker

	// startHeight represents the height of the first header in the file.
	startHeight uint32

	// endHeight represents the height of the last header in the file.
	endHeight uint32

	// offset represents the distance required to read the first header from
	// the file.
	initialOffset int64

	// chain represents the bitcoin network the headers in the file belong
	// to.
	chain wire.BitcoinNet

	// dataSize is the size, one header occupies in the file, in bytes.
	dataSize dataSize
}

// blkHdrBinReader is specialized for reading and decoding binary block headers.
// It embeds a general binary reader (binReader) and includes a specific decoder
// for block headers.
type blkHdrBinReader struct {
	// binReader is a general binary data reader configured for blockchain data.
	*binReader

	// headerDecoder is a function tailored to decode binary block headers.
	headerDecoder[*wire.BlockHeader]
}

// newBinaryBlkHdrLoader initializes a blkHdrBinReader with the given
// configuration. It sets up the binary loader for block headers specifically,
// using a provided configuration that includes the data source and
// pre-configures it to read block header data.
func newBinaryBlkHdrLoader(r io.ReadSeeker) (*blkHdrBinReader, error) {
	b, err := newBinaryLoader(r, BlockHeaders)
	if err != nil {
		return nil, err
	}

	return &blkHdrBinReader{
		binReader:     b,
		headerDecoder: blkHdrDecoder,
	}, nil
}

// newBinaryChainDataLoader initializes a Binary Loader.
func newBinaryLoader(reader io.ReadSeeker,
	typeData dataType) (*binReader, error) {

	// Ensure we start reading from the beginning.
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("error seeking reader from start")
	}

	// Create scratch buffer.
	scratch := [8]byte{}

	typeOfData, err := tlv.ReadVarInt(reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining data type "+
			"of file %w", err)
	}

	if dataType(typeOfData) != typeData {
		return nil, fmt.Errorf("data type mismatch: got %v but "+
			"expected %v", dataType(typeOfData), typeData)
	}

	var headerSize dataSize
	switch typeData {
	case BlockHeaders:
		headerSize = blockHeaderSize
	default:
		return nil, errors.New("unsupported header type")
	}

	// Read start height of block header file.
	start, err := tlv.ReadVarInt(reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining start height "+
			"of file %w", err)
	}

	// Read end height of block header file.
	end, err := tlv.ReadVarInt(reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining end height of "+
			"file %w", err)
	}

	// Read the bitcoin network, the headers in the header file belong to.
	chainChar, err := tlv.ReadVarInt(reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining chain of "+
			"file %w", err)
	}

	// obtain space occupied by metadata as initial offset
	initialOffset, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("unable to determine initial "+
			"offset: %v", err)
	}

	return &binReader{
		reader:        reader,
		startHeight:   uint32(start),
		endHeight:     uint32(end),
		initialOffset: initialOffset,
		chain:         wire.BitcoinNet(chainChar),
		dataSize:      headerSize,
	}, nil
}

// headerDecoder type serializes the passed byte to the required header type.
type headerDecoder[T header] func([]byte) (T, error)

// blkHdrDecoder serializes the passed data in bytes to  *wire.BlockHeader type.
func blkHdrDecoder(data []byte) (*wire.BlockHeader, error) {
	var blockHeader wire.BlockHeader

	headerReader := bytes.NewReader(data)

	// Finally, decode the raw bytes into a proper bitcoin header.
	if err := blockHeader.Deserialize(headerReader); err != nil {
		return nil, fmt.Errorf("error deserializing block "+
			"header: %w", err)
	}

	return &blockHeader, nil
}

// readHeaders fetches headers from the binary file.
func readHeaders[T header](numHeaders uint32, reader io.ReadSeeker,
	dataTypeSize dataSize,
	decoder headerDecoder[T]) ([]T, error) {

	hdrs := make([]T, 0, numHeaders)
	for i := uint32(0); i < numHeaders; i++ {
		rawData := make([]byte, dataTypeSize)

		if _, err := reader.Read(rawData); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		hdr, err := decoder(rawData)

		if err != nil {
			return nil, err
		}
		hdrs = append(hdrs, hdr)
	}
	return hdrs, nil
}

// FetchHeaders retrieves a specified number of block headers from the
// blkHdrBinReader's underlying data source. It utilizes a generic function,
// readHeaders, to perform the actual reading and decoding based on the
// blkHdrBinReader's configuration.
func (b *blkHdrBinReader) FetchHeaders(numHeaders uint32) ([]*wire.BlockHeader,
	error) {

	return readHeaders[*wire.BlockHeader](
		numHeaders, b.reader, b.dataSize, b.headerDecoder,
	)
}

// EndHeight function returns the height of the last header in the file.
func (b *binReader) EndHeight() uint32 {
	return b.endHeight
}

// StartHeight function returns the height of the first header in the file.
func (b *binReader) StartHeight() uint32 {
	return b.startHeight
}

// HeadersChain function returns the network the headers in the file belong to.
func (b *binReader) HeadersChain() wire.BitcoinNet {
	return b.chain
}

// SetHeight sets the reader's position for fetching headers from a specific
// block height. If height is -1, it resets to the start. Validates height
// within available range and adjusts the reader to the correct offset.
// Returns an error if the height is out of range or on seek failure.
func (b *binReader) SetHeight(height int32) error {
	if height == -1 {
		_, err := b.reader.Seek(b.initialOffset, io.SeekStart)
		if err != nil {
			return ErrSetSeek(err)
		}

		return nil
	}

	if height < int32(b.startHeight) || height >= int32(b.endHeight) {
		return errors.New("height out of range")
	}

	offset := int64(height-int32(b.startHeight))*int64(b.dataSize) +
		b.initialOffset

	_, err := b.reader.Seek(offset, io.SeekStart)
	if err != nil {
		return ErrSetSeek(err)
	}

	return nil
}
