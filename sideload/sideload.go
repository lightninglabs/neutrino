package sideload

import (
	"errors"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

var (
	// ErrUnsupportedSourceType is an error message returned if a sideload
	// source is encountered that is not recognized or cannot be handled.
	ErrUnsupportedSourceType = errors.New("unsupported source type")

	// ErrBitcoinNetworkMismatchFmt is a function that formats an error
	// message for a mismatch between the bitcoin network of the chain and
	// the sideload's network.
	ErrBitcoinNetworkMismatchFmt = func(
		cfgChain wire.BitcoinNet, headerChain wire.BitcoinNet) error {
		return fmt.Errorf("bitcoin network mismatch, chain's "+
			"network: %v, sideload's network: %v",
			cfgChain, headerChain.String())
	}

	// CompletedSideloadMsg is message that indicates that sideloading
	// has beeen completed.
	CompletedSideloadMsg = func(tipHeight uint32) string {
		return fmt.Sprintf("Completed sideloading at height %v", tipHeight)
	}

	ErrSetSeek = func(err error) error {
		return fmt.Errorf("unable to set seek for Loader: %w", err)
	}
)

// SourceType is a type that indicates the encoding format of the
// sideload source.
type SourceType uint8

const (
	// Binary indicates that the sideload source is in binary encoding
	// format.
	Binary SourceType = 0
)

// dataType indicates the type of data stored by the sideload source.
type dataType uint8

// dataSize is a type indicating the size in bytes a single unit of data
// handled by the sideload source occupies.
type dataSize uint32

const (
	// BlockHeaders is a data type that indicates the data stored is
	// *wire.BlockHeaders.
	BlockHeaders dataType = 0
)

// header represents the headers handled by the package.
type header interface {
	*wire.BlockHeader
}

// HeaderValidator defines an interface for validating blockchain headers,
// parameterized over a type T, which represents a header. The type T must
// satisfy the 'header' constraint.
type HeaderValidator[T header] interface {
	// VerifyCheckpoint checks the validity of headers between two
	// checkpoints. It accepts a pointer to a chainhash.Hash for the
	// previous checkpoint, a slice of headers of type T, and a pointer to a
	// chainhash.Hash for the next checkpoint. It returns true if the
	// headers are valid, false otherwise.
	VerifyCheckpoint(prevCheckpoint *chainhash.Hash, headers []T,
		nextCheckpoint *chainhash.Hash) bool

	// Verify assesses the validity of a slice of headers of type T.
	// It returns true if the headers pass the validation rules, false
	// otherwise.
	Verify([]T) bool
}

// HeaderWriter defines an interface for persisting blockchain headers to a
// storage layer. It is parameterized over a type T, which represents a
// blockchain header. The type T must satisfy the 'header' constraint,
// indicating it is a header type.
type HeaderWriter[T header] interface {
	// ChainTip returns the hash and height of the most recently known
	// header in the storage.
	ChainTip() (*chainhash.Hash, uint32, error)

	// Write takes a slice of headers of type T and persists them to the
	// storage layer. It returns an error if any.
	Write([]T) error
}

// Checkpoints provides methods for accessing header checkpoints.
type Checkpoints interface {
	// FetchCheckpoint returns the checkpoint height and hash at a given
	// index.
	FetchCheckpoint(idx int32) (height uint32, hash *chainhash.Hash)

	// FindNextHeaderCheckpoint locates the next checkpoint after a given
	// height, returning its index, header height and hash.
	FindNextHeaderCheckpoint(curHeight uint32) (index int32, height uint32,
		hash *chainhash.Hash)

	// FindPreviousHeaderCheckpoint locates the previous checkpoint before a
	// given height, returning its index, header height and hash.
	FindPreviousHeaderCheckpoint(curHeight uint32) (index int32,
		height uint32, hash *chainhash.Hash)

	// Len returns the total number of checkpoints.
	Len() int32
}

// LoaderSource defines methods for retrieving data from a source.
type LoaderSource[T header] interface {
	// EndHeight returns the height of the last header in the sideload
	// source.
	EndHeight() uint32

	// StartHeight returns the height of the first header in the sideload
	// source.
	StartHeight() uint32

	// HeadersChain identifies the Bitcoin network associated with the
	// headers in the sideload source.
	HeadersChain() wire.BitcoinNet

	// FetchHeaders retrieves a specified number of headers starting from
	// the current set height.
	FetchHeaders(numHeaders uint32) ([]T, error)

	// SetHeight specifies the height from which the caller intends to start
	// reading headers. It returns an error, if any.
	SetHeight(int32) error
}

// LoaderInternal integrates header writing and validation functionalities,
// tailored for processing blockchain headers. It employs generics to work
// with a specific header type that meets the 'header' interface constraints.
type LoaderInternal[T header] struct {
	// HeaderWriter provides methods to persist headers into the storage layer.
	HeaderWriter[T]

	// HeaderValidator offers validation capabilities for blockchain headers,
	// ensuring they adhere to specific rules and checkpoints.
	HeaderValidator[T]

	// SkipVerify indicates whether header validation should be bypassed.
	// True means validation is skipped; false means validation is performed.
	SkipVerify bool

	// Chkpt holds methods for accessing header checkpoints used during the
	// header validation process to ensure headers align with known valid points
	// in the blockchain.
	Chkpt Checkpoints

	// SideloadRange the number of headers that are fetched from the sideload
	// source at a time.
	SideloadRange uint32
}

// LoaderConfig holds configuration details necessary for initializing
// a Loader. It is generic over a type T, which represents a blockchain
// header and must satisfy the 'header' constraint, indicating its
// suitability for header-related operations.
type LoaderConfig[T header] struct {
	// LoaderInternal embeds functionalities for writing and validating
	// headers, as well as additional settings related to the validation
	// process and sideloading capabilities.
	LoaderInternal[T]

	// SourceType identifies the encoding format of the sideload source,
	// indicating how sideloaded data should be interpreted.
	SourceType SourceType

	// Reader provides an io.ReadSeeker interface to the sideload source.
	Reader io.ReadSeeker

	// Chain specifies the Bitcoin network for which the loader's caller is
	// configured, distinguishing between different network types
	// (e.g., mainnet, testnet).
	Chain wire.BitcoinNet
}

// SideLoader manages the sideloading of headers.  It is generic over a type T,
// which represents a blockchain header and must satisfy the 'header'
// constraint.
type SideLoader[T header] struct {
	// LoaderInternal contains basic loader functionalities such as
	// writing and validating headers, along with configurations for
	// sideloading and validation processes.
	LoaderInternal[T]

	// source represents the data source from which headers are sideloaded.
	// It is specific to the sideloading process, encapsulating the logic
	// for fetching and decoding sideloaded headers.
	source LoaderSource[T]

	// chkptTracker keeps track of the current checkpoint index during the
	// sideloading process.
	chkptTracker int32

	// haltHeight specifies a blockchain height at which the sideloading
	// process should be halted.
	haltHeight uint32

	// haltHeightFunc is a function that dynamically determines the halt height
	// for the sideloading process.
	haltHeightFunc func() uint32
}

// NewBlockHeaderLoader initializes a block header Loader based on the source
// type of the reader config.
func NewBlockHeaderLoader(
	cfg *LoaderConfig[*wire.BlockHeader]) (*SideLoader[*wire.BlockHeader],
	error) {

	var (
		source LoaderSource[*wire.BlockHeader]
		err    error
	)

	switch {
	case cfg.SourceType == Binary:
		source, err = newBinaryBlkHdrLoader(cfg.Reader)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrUnsupportedSourceType
	}

	headerChain := source.HeadersChain()

	if headerChain != cfg.Chain {
		return nil, ErrBitcoinNetworkMismatchFmt(
			cfg.Chain, headerChain,
		)
	}

	return &SideLoader[*wire.BlockHeader]{
		LoaderInternal: cfg.LoaderInternal,
		source:         source,
		haltHeightFunc: func() uint32 {
			return source.EndHeight()
		},
	}, nil
}

// obtainRangeFetchFunc returns the next number of headers to be fetched and
// the next checkpoint header and checkpoint height, if skipVerify is false.
func (s *SideLoader[T]) obtainRangeFetchFunc() func(
	curHeight uint32) (uint32, *chainhash.Hash) {

	// If we are to verify headers and the length of checkpoints is
	// greater than zero, we want to fetch headers from our current height to
	// our next checkpoint.
	if !s.SkipVerify && s.Chkpt.Len() > 0 {
		return func(curHeight uint32) (uint32, *chainhash.Hash) {
			chkpt := s.Chkpt

			// Check if idx is out of range
			if s.chkptTracker >= chkpt.Len() {
				return 0, nil
			}
			checkpointHeight, checkpointHash := chkpt.
				FetchCheckpoint(s.chkptTracker)

			if checkpointHeight > s.source.EndHeight() {
				return curHeight, nil
			}

			s.chkptTracker++

			return checkpointHeight, checkpointHash
		}
	}

	// If we do not want to Verify headers we use whatever
	//  `sideloadRange` was given to us by the caller.
	return func(curHeight uint32) (uint32, *chainhash.Hash) {
		if curHeight >= s.haltHeight {
			return curHeight, nil
		}
		if curHeight+s.SideloadRange > s.haltHeight {
			return s.haltHeight - curHeight, nil
		}

		return curHeight + s.SideloadRange, nil
	}
}

// Load sideloads headers into the neutrino system.
func (s *SideLoader[T]) Load() error {
	// Fetch the height at which we stop sideloading.
	haltHeight := s.haltHeightFunc()
	s.haltHeight = haltHeight

	_, tipHeight, err := s.HeaderWriter.ChainTip()
	if err != nil {
		return err
	}

	source := s.source

	err = source.SetHeight(int32(tipHeight))
	if err != nil {
		log.Infof("Nothing to load, chain height: %v, "+
			"source end height: %v", tipHeight, source.EndHeight())

		return nil
	}

	var prevCheckpt *chainhash.Hash
	if s.Chkpt.Len() > 0 {
		idx, _, _ := s.Chkpt.FindNextHeaderCheckpoint(tipHeight)
		s.chkptTracker = idx

		_, _, prevCheckpt = s.Chkpt.FindPreviousHeaderCheckpoint(tipHeight)

	}

	fetchRange := s.obtainRangeFetchFunc()

	log.Infof("Sideloading headers from height %v", tipHeight)
	for tipHeight < haltHeight {
		chkpointHeight, chkpointHash := fetchRange(tipHeight)
		fetchSize := chkpointHeight - tipHeight

		log.Infof("chkpoint height %v", chkpointHeight)
		if fetchSize == 0 {
			log.Infof(CompletedSideloadMsg(tipHeight))

			return nil
		}

		headers, err := s.source.FetchHeaders(fetchSize)
		if err != nil {
			return err
		}

		if len(headers) == 0 {
			log.Infof(CompletedSideloadMsg(tipHeight))

			return nil
		}

		if !s.SkipVerify {
			if !s.Verify(headers) {
				log.Warnf("headers failed verification at "+
					"height: %v", tipHeight)

				return nil
			}

			if s.Chkpt.Len() > 0 {
				if !s.VerifyCheckpoint(
					prevCheckpt, headers[:len(headers)-1],
					chkpointHash,
				) {

					log.Warnf("headers failed checkpoint "+
						"verification at height: "+
						"%v", tipHeight)

					return nil
				}
			}
		}

		err = s.Write(headers)
		if err != nil {
			return err
		}

		// Update tip.
		_, tipHeight, err = s.ChainTip()
		if err != nil {
			return err
		}

		// Update previous checkpoint hash.
		prevCheckpt = chkpointHash
	}

	log.Infof(CompletedSideloadMsg(tipHeight))

	return nil
}
