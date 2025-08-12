package chainimport

import (
	"io"
	"iter"
	"net/http"

	"github.com/btcsuite/btcd/chaincfg"
)

// ImportHeadersFile defines the interface for file-like objects used in header
// import operations. It combines standard I/O interfaces with length
// information, typically implemented by memory-mapped files for efficient
// random access to header data.
type ImportHeadersFile interface {
	io.Reader
	io.ReaderAt
	io.Closer
	Len() int
}

// Header is the base interface for header operations.
type Header interface {
	// Deserialize reconstructs the header from binary data at the specified
	// height.
	Deserialize(io.Reader, uint32) error
}

// StatelessIterator provides functional iteration over headers using Go's iter
// package. Each method call creates a fresh, independent iterator.
type StatelessIterator interface {
	// Iterator returns an iter.Seq2[header, error] for the specified range.
	// Each call creates a fresh iterator that independently traverses the
	// range.
	Iterator(start, end uint32) iter.Seq2[Header, error]

	// BatchIterator returns an iterator that yields batches of headers for
	// the specified range, where each batch respects the configured batch
	// size limit. Each call creates a fresh iterator that independently
	// traverses the range.
	BatchIterator(start, end uint32) iter.Seq2[[]Header, error]

	// ReadBatch collects all headers from the given range into a slice.
	ReadBatch(start, end uint32) ([]Header, error)

	// Close releases any resources associated with the iterator.
	Close() error
}

// StatefulIterator provides position-based iteration over headers with internal
// state.
type StatefulIterator interface {
	// Next returns the next header in the current iteration sequence.
	// Maintains internal position state. Returns nil, io.EOF when no more
	// headers are available.
	Next() (Header, error)

	// NextBatch returns the next batch of headers in the current iteration
	// sequence. Maintains internal position state. Returns nil, io.EOF when
	// no more batches are available.
	NextBatch() ([]Header, error)

	// Close releases any resources associated with the iterator.
	Close() error
}

// HeaderIterator combines both stateless and stateful iteration capabilities.
type HeaderIterator interface {
	// StatelessIterator provides functional iteration with fresh iterators.
	StatelessIterator

	// StatefulIterator provides sequential iteration with internal state.
	StatefulIterator
}

// HeaderImportSource is an interface for loading headers from various sources
// (files, HTTP endpoints) with support for both sequential iteration and
// efficient random access using memory-mapped files.
type HeaderImportSource interface {
	// Open initializes the header source and prepares it for reading.
	Open() error

	// Close releases any resources associated with the header source.
	Close() error

	// GetHeaderMetadata retrieves metadata information about the headers
	// collection.
	GetHeaderMetadata() (*headerMetadata, error)

	// Iterator returns a headerIterator that can traverse headers between
	// start and end indices.
	Iterator(start, end uint32, batchSize uint32) HeaderIterator

	// GetHeader retrieves a single header at the specified index using
	// efficient random access (memory-mapped files) without loading the
	// entire file into memory.
	GetHeader(index uint32) (Header, error)

	// GetURI returns the location identifier for this header source
	// (file path for file sources, URL for HTTP sources).
	GetURI() string

	// SetURI sets the location identifier for this header source
	// (file path for file sources, URL for HTTP sources).
	SetURI(uri string)
}

// HeadersValidator defines methods for validating blockchain headers.
type HeadersValidator interface {
	// Validate performs comprehensive validation on a sequence of headers
	// provided by the iterator. It checks that the entire sequence forms
	// a valid chain according to the given chain parameters.
	Validate(HeaderIterator, chaincfg.Params) error

	// ValidateBatch performs validation on a batch of headers, checking
	// that they form a valid chain segment according to the given chain
	// parameters.
	ValidateBatch([]Header, chaincfg.Params) error

	// ValidatePair verifies that two consecutive headers (prev and current)
	// form a valid chain link according to the specified chain parameters.
	ValidatePair(prev, current Header, targetCh chaincfg.Params) error

	// ValidateSingle validates a single header for basic sanity checks.
	ValidateSingle(header Header, targetChainParams chaincfg.Params) error
}

// HttpClient defines the interface for making HTTP GET requests.
type HttpClient interface {
	Get(url string) (*http.Response, error)
}
