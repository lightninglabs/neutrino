package chaindataloader

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightningnetwork/lnd/tlv"
)

/*

================================================================================

ENCODING FORMAT
================================================================================

Each file has a header of varint size consisting of,

dataType || startHeight || endHeight || chain

in that order.
*/

// binReader is an internal struct that holds all data the binReader needs
// to fetch headers.
type binReader struct {
	// reader represents the source to be read.
	reader io.ReadSeeker

	// startHeight represents the height of the first header in the file.
	startHeight uint32

	// endHeight represents the height of the last header in the file.
	endHeight uint32

	// offset represents the distance required to read the first header from
	// the file.
	initialOffset uint32

	// chain represents the bitcoin network the headers in the file belong to.
	chain        wire.BitcoinNet
	dataTypeSize uint32
}

// newBinaryChainDataLoader initializes a Binary Reader.
func newBinaryChainDataLoader(reader io.ReadSeeker,
	typeData dataType) (*binReader, error) {

	// Read data type.
	dataTypeBytes := [8]byte{}
	typeOfData, err := tlv.ReadVarInt(reader, &dataTypeBytes)
	if err != nil {
		return nil, fmt.Errorf("error obtaining data type "+
			"of file %w", err)
	}

	if dataType(typeOfData) != typeData {
		return nil, fmt.Errorf("data type mismatch: got %v but expected %v",
			dataType(typeOfData), typeData)
	}

	var dataTypeSize uint32
	switch {
	case typeData == blockHeaders:
		dataTypeSize = headerfs.BlockHeaderSize
	default:
		return nil, fmt.Errorf("got unsupported dataType: %v", typeData)
	}

	// Read start height of block header file.
	startBytes := [8]byte{}
	start, err := tlv.ReadVarInt(reader, &startBytes)
	if err != nil {
		return nil, fmt.Errorf("error obtaining start height "+
			"of file %w", err)
	}

	// Read end height of block header file.
	endBytes := [8]byte{}
	end, err := tlv.ReadVarInt(reader, &endBytes)
	if err != nil {
		return nil, fmt.Errorf("error obtaining end height "+
			"of file %w", err)
	}

	// Read the bitcoin network, the headers in the header file belong to.
	netBytes := [8]byte{}
	chainChar, err := tlv.ReadVarInt(reader, &netBytes)
	if err != nil {
		return nil, fmt.Errorf("error obtaining chain "+
			"of file %w", err)
	}

	var chain wire.BitcoinNet
	switch {
	case chainChar == 1:
		chain = wire.TestNet3
	case chainChar == 2:
		chain = wire.MainNet
	case chainChar == 3:
		chain = wire.SimNet
	case chainChar == 4:
		chain = wire.TestNet
	default:
		return nil, fmt.Errorf("read unsupported character (%d) for "+
			"network of side-load source file", chainChar)
	}

	// obtain space occupied by metadata as initial offset
	initialOffset, err := reader.Seek(0, io.SeekCurrent)

	if err != nil {
		return nil, fmt.Errorf("unable to determine initial offset: "+
			"%v", err)
	}

	return &binReader{
		reader:        reader,
		startHeight:   uint32(start),
		endHeight:     uint32(end),
		chain:         chain,
		initialOffset: uint32(initialOffset),
		dataTypeSize:  dataTypeSize,
	}, nil
}

// NextBlockHeaders fetches headers from the reader. An empty slice is returned
// if there are no headers left to fetch.
func (b *binReader) NextBlockHeaders(n uint32) ([]*wire.BlockHeader, error) {
	headers := make([]*wire.BlockHeader, n)

	createData := func(headers []*wire.BlockHeader) func(data []byte,
		i uint32) error {

		return func(data []byte, i uint32) error {
			var blockHeader wire.BlockHeader

			headerReader := bytes.NewReader(data)

			// Finally, decode the raw bytes into a proper bitcoin header.
			if err := blockHeader.Deserialize(headerReader); err != nil {
				return fmt.Errorf("error deserializing block header: %w",
					err)
			}

			headers[i] = &blockHeader
			return nil
		}
	}

	err := b.readData(n, createData(headers))

	if err != nil {
		return nil, err
	}
	return headers, nil
}

func (b *binReader) readData(n uint32, createData func(data []byte,
	i uint32) error) error {

	for i := uint32(0); i < n; i++ {
		rawData := make([]byte, b.dataTypeSize)

		if _, err := b.reader.Read(rawData); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		err := createData(rawData, i)

		if err != nil {
			return err
		}
	}

	log.Infof("Loaded %v headers", n)
	return nil
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

// SetHeight function receives a height which should be the height of the last
// header the caller has. It uses this to set the appropriate offest for
// fetching Headers.
func (b *binReader) SetHeight(height uint32) error {
	if b.startHeight > height || height > b.endHeight {
		return errors.New("unable to set height as file does not " +
			"contain requested height")
	}

	// Compute offset to fetch header at `height` relative to the binReader's
	// header's start height.
	offset := (height - b.startHeight) * b.dataTypeSize

	// Compute total offset to fetch next header *after* header at `height`
	// relative to the reader's start point which contains the reader's
	// metadata as well.
	totalOffset := (offset + b.initialOffset) + b.dataTypeSize

	_, err := b.reader.Seek(int64(totalOffset), io.SeekStart)

	if err != nil {
		return fmt.Errorf("unable to set seek for Reader: %v", err)
	}

	return nil
}
