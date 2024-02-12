package chaindataloader

import (
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"io"

	"github.com/btcsuite/btcd/wire"
)

// ProcessHdrResp is a struct return after headers are processed indicating
// current height of the caller's store and the last height the next call
// should stop fetching at.
type ProcessHdrResp struct {
	CurHeight         int32
	NextCheckptHeight int32
}

// Reader is the interface a caller fetches data from.
type Reader interface {
	// EndHeight indicates the height of the last Header in the chaindataloader
	// source.
	EndHeight() uint32

	// StartHeight indicates the height of the first Header in the
	// chaindataloader source.
	StartHeight() uint32

	// HeadersChain returns the bitcoin network the headers in the
	// chaindataloader source belong to.
	HeadersChain() wire.BitcoinNet

	// Load fetches and processes the passed amount of headers returning an
	// error and dteails about the current call that would affect the next.
	Load(uint32) (*ProcessHdrResp, error)

	// SetHeight tells the chaindataloader, the point the caller would want to
	// start reading from.
	SetHeight(uint32) error
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

	// filterHeaders is a data type that indicates the data stored is
	// filter hash.
	filterHeaders dataType = 1
)

// dataSize is a type indicating the size in bytes a single unit of data
// handled by the chaindataloader occupies.
type dataSize uint32

// ReaderConfig is a generic struct that contains configuration details
// required to etch a Reader.
type ReaderConfig struct {

	// SourceType is the format type of the chaindataloader source.
	SourceType SourceType

	// Reader is the chaindataloader's source.
	Reader io.ReadSeeker
}

// BlkHdrReaderConfig is a struct that contains configuration details
// required to etch a Reader for block headers.
type BlkHdrReaderConfig struct {
	ReaderConfig
	ProcessBlkHeader func([]*wire.BlockHeader) (*ProcessHdrResp, error)
}

// FilterHdrReaderConfig is a struct that contains configuration details
// required to etch a Reader for filter headers.
type FilterHdrReaderConfig struct {
	ReaderConfig
	ProcessCfHeader func([]*chainhash.Hash) (*ProcessHdrResp, error)
	FilterType      wire.FilterType
}

// NewBlockHeaderReader initializes a block header Reader based on the source
// type of the reader config.
func NewBlockHeaderReader(cfg *BlkHdrReaderConfig) (Reader, error) {

	var (
		reader Reader
		err    error
	)

	switch {
	case cfg.SourceType == Binary:
		reader, err = newBinaryBlkHdrChainDataLoader(cfg)
	default:
	}

	return reader, err
}

// NewFilterHeaderReader initializes a filter header Reader based on the source
// type of the reader config.
func NewFilterHeaderReader(cfg *FilterHdrReaderConfig) (Reader, error) {

	var (
		reader Reader
		err    error
	)

	switch {
	case cfg.SourceType == Binary:
		reader, err = newBinaryFilterHdrChainDataLoader(cfg)
	default:
	}

	return reader, err
}
