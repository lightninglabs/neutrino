package chaindataloader

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"io"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/tlv"
)

// headerChainType is the type that indicates the header's bitcoin network.
type headerChainType uint8

const (
	// blockHeaderSize is the size one block header occupies in bytes.
	blockHeaderSize dataSize = 80

	// filterHeaderSize is the size one filter header occupies in bytes.
	filterHeaderSize dataSize = 32

	testnet3 headerChainType = 1

	mainnet headerChainType = 2

	simnet headerChainType = 3

	testnet headerChainType = 4
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
	initialOffset uint32

	// chain represents the bitcoin network the headers in the file belong to.
	chain wire.BitcoinNet

	// dataTypeSize is the size of a header in bytes.
	dataTypeSize uint32
}

// processHeader is a function type that handles the headers read from the file.
type processHeader[T neutrinoHeader] func([]T) (*ProcessHdrResp, error)

// blkHdrBinReader is the struct that contains metadata required to fetch and
// process block headers from the binary file.
type blkHdrBinReader struct {
	*binReader
	processBlkHdr processHeader[*wire.BlockHeader]
}

// filterHdrBinReader is the struct that contains metadata required to fetch and
// process filter headers from the binary file.
type filterHdrBinReader struct {
	*binReader
	processCfHdr processHeader[*chainhash.Hash]
}

// Load fetches blockheaders from the file and processes it.
func (b *blkHdrBinReader) Load(n uint32) (*ProcessHdrResp, error) {
	hdrs, err := readHeaders(n, blkHdrDecoder, b.dataTypeSize, b.reader)

	if err != nil {
		return nil, err
	}

	resp, err := b.processBlkHdr(hdrs)

	if err != nil {
		return nil, err
	}

	return resp, nil
}

// Load fetches filter headers from the file and processes it.
func (b *filterHdrBinReader) Load(n uint32) (*ProcessHdrResp, error) {
	hdrs, err := readHeaders(n, filterHdrDecoder, b.dataTypeSize, b.reader)

	if err != nil {
		return nil, err
	}

	resp, err := b.processCfHdr(hdrs)

	if err != nil {
		return nil, err
	}

	return resp, nil
}

// newBinaryBlkHdrChainDataLoader initializes the struct required to fetch
// process block headers from the binary file.
func newBinaryBlkHdrChainDataLoader(c *BlkHdrReaderConfig) (*blkHdrBinReader,
	error) {

	scratch := [8]byte{}
	typeOfData, err := tlv.ReadVarInt(c.Reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining data type "+
			"of file %w", err)
	}

	if dataType(typeOfData) != blockHeaders {
		return nil, fmt.Errorf("data type mismatch: got %v but expected %v",
			dataType(typeOfData), blockHeaders)
	}

	bin, err := newBinaryChainDataLoader(c.Reader)
	if err != nil {
		return nil, err
	}

	bin.dataTypeSize = uint32(blockHeaderSize)

	return &blkHdrBinReader{
		binReader:     bin,
		processBlkHdr: c.ProcessBlkHeader,
	}, nil

}

// newBinaryFilterHdrChainDataLoader initializes the struct required to fetch
// process filter headers from the binary file.
func newBinaryFilterHdrChainDataLoader(c *FilterHdrReaderConfig) (
	*filterHdrBinReader,
	error) {

	scratch := [8]byte{}
	typeOfData, err := tlv.ReadVarInt(c.Reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining data type "+
			"of file %w", err)
	}

	if dataType(typeOfData) != filterHeaders {
		return nil, fmt.Errorf("data type mismatch: got %v but expected %v",
			dataType(typeOfData), filterHeaders)
	}

	filterType, err := tlv.ReadVarInt(c.Reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining data type "+
			"of file %w", err)
	}

	if wire.FilterType(filterType) != c.FilterType {
		return nil, fmt.Errorf("filter type mismatch: got %v but "+
			"expected %v", filterType, c.FilterType)
	}
	bin, err := newBinaryChainDataLoader(c.Reader)
	if err != nil {
		return nil, err
	}
	bin.dataTypeSize = uint32(filterHeaderSize)

	return &filterHdrBinReader{
		binReader:    bin,
		processCfHdr: c.ProcessCfHeader,
	}, nil

}

// newBinaryChainDataLoader initializes a Binary Reader.
func newBinaryChainDataLoader(reader io.ReadSeeker) (
	*binReader, error) {

	// Create scratch buffer.
	scratch := [8]byte{}

	// Read start height of block header file.
	start, err := tlv.ReadVarInt(reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining start height "+
			"of file %w", err)
	}

	// Read end height of block header file.
	end, err := tlv.ReadVarInt(reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining end height of file "+
			"%w", err)
	}

	// Read the bitcoin network, the headers in the header file belong to.
	chainChar, err := tlv.ReadVarInt(reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining chain of file %w", err)
	}

	headerChain := headerChainType(chainChar)
	var chain wire.BitcoinNet
	switch {
	case headerChain == testnet3:
		chain = wire.TestNet3
	case headerChain == mainnet:
		chain = wire.MainNet
	case headerChain == simnet:
		chain = wire.SimNet
	case headerChain == testnet:
		chain = wire.TestNet
	default:
		return nil, fmt.Errorf("read unsupported character (%d) for "+
			"network of side-load source file", headerChain)
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
	}, nil
}

// neutrinoHeader is a type parameter for block header and filter hash.
type neutrinoHeader interface {
	*wire.BlockHeader | *chainhash.Hash
}

// headerDecoder type serializes the passed byte to the required header type.
type headerDecoder[T neutrinoHeader] func([]byte) (T, error)

// blkHdrDecoder serializes the passed data in bytes to  *wire.BlockHeader type.
func blkHdrDecoder(data []byte) (*wire.BlockHeader, error) {
	var blockHeader wire.BlockHeader

	headerReader := bytes.NewReader(data)

	// Finally, decode the raw bytes into a proper bitcoin header.
	if err := blockHeader.Deserialize(headerReader); err != nil {
		return nil, fmt.Errorf("error deserializing block header: %w",
			err)
	}

	return &blockHeader, nil
}

// filterHdrDecoder serializes the passed data in bytes to filter header hash.
func filterHdrDecoder(data []byte) (*chainhash.Hash, error) {

	return chainhash.NewHash(data)
}

// readHeaders fetches headers from the binary file.
func readHeaders[T neutrinoHeader](numHeaders uint32,
	decoder headerDecoder[T], dataTypeSize uint32, reader io.ReadSeeker) (
	[]T, error) {

	hdrs := make([]T, numHeaders)
	for i := uint32(0); i < numHeaders; i++ {
		rawData := make([]byte, dataTypeSize)

		if _, err := reader.Read(rawData); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		T, err := decoder(rawData)

		if err != nil {
			return nil, err
		}
		hdrs[i] = T
	}
	return hdrs, nil
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
