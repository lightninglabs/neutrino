package chaindataloader

import (
	"errors"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/wire"
)

var (
	ErrUnsupportedSourceType = errors.New("source type is not supported")
)

// Reader is the interface a caller fetches data from.
type Reader interface {
	// EndHeight indicates the height of the last Header in the chaindataloader
	// source.
	EndHeight() uint32

	// StartHeight indicates the height of the first Header in the
	// chaindataloader source.
	StartHeight() uint32

	// SetHeight sets the required offset to fetch the next header after the
	// header at "height".
	SetHeight(height uint32) error

	// HeadersChain returns the bitcoin network the headers in the
	// chaindataloader source belong to.
	HeadersChain() wire.BitcoinNet
}

type BlockHeaderReader interface {
	Reader
	// NextBlockHeaders fetches header from a chaindataloader source.
	NextBlockHeaders(uint32) ([]*wire.BlockHeader, error)
}

// SourceType is a type that indicates the encoding format of the
// sideload source.
type SourceType uint8

const (
	// Binary is a SourceType that uses binary number system to encode
	// information.
	Binary SourceType = 0
)

// dataType indicates the type of data stored by the side load source.
type dataType uint8

const (
	// blockHeaders is a data type that indicates the data stored is
	// *wire.BlockHeaders.
	blockHeaders dataType = 0
)

// ReaderConfig is a struct that contains configuration details required to
// fetch a Reader.
type ReaderConfig struct {

	// SourceType is the format type of the chaindataloader source.
	SourceType SourceType

	// Reader is the chaindataloader's source.
	Reader io.ReadSeeker
}

// NewBlockHeaderReader initializes a block header Reader based on the source
// type of the reader config.
func NewBlockHeaderReader(cfg *ReaderConfig) (BlockHeaderReader, error) {
	reader, err := newChainDataLoader(cfg, blockHeaders)
	if err != nil {
		return nil, err
	}

	blockHeaderReader, ok := reader.(BlockHeaderReader)

	if !ok {
		return nil, errors.New("reader not of expected type")
	}

	return blockHeaderReader, nil
}

// newChainDataLoader returns the chain data loader's reader.
func newChainDataLoader(cfg *ReaderConfig, typeOfData dataType) (Reader,
	error) {

	var (
		reader Reader
		err    error
	)

	switch {
	case cfg.SourceType == Binary:
		reader, err = newBinaryChainDataLoader(cfg.Reader, typeOfData)
	default:
		return nil, ErrUnsupportedSourceType
	}

	if err != nil {
		return nil, fmt.Errorf("error initializing %v chain data "+
			"loader: %w", cfg.SourceType, err)
	}

	return reader, nil
}
