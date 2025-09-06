package chainimport

import (
	"context"
	"io"
	"iter"
	"net/http"
)

// ImportHeadersFile defines the interface for file-like objects used in header
// import operations. It combines standard I/O interfaces with length info,
// typically implemented by memory-mapped files for efficient random access to
// header data.
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

// HeaderIterator provides stateless iteration capabilities over headers. Each
// call to Iterator/BatchIterator creates a fresh, independent sequence for the
// specified range.
type HeaderIterator interface {
	// Iterator returns an iter.Seq2[header, error] for the specified range.
	// Each call creates a fresh iterator that independently traverses the
	// range from start to end index.
	Iterator(start, end uint32) iter.Seq2[Header, error]

	// BatchIterator returns an iterator that yields batches of headers for
	// the specified range, where each batch respects the configured batch
	// size limit. Each call creates a fresh iterator that independently
	// traverses the range from start to end index.
	BatchIterator(start, end, batchSize uint32) iter.Seq2[[]Header, error]

	// ReadBatch collects all headers from the specified range into a slice,
	// respecting the configured batch size limit.
	ReadBatch(start, end, batchSize uint32) ([]Header, error)

	// GetStartIndex returns the configured start index for this iterator.
	// This is useful for accessing the intended range, validation, and
	// as a fallback when you want to use the configured range.
	GetStartIndex() uint32

	// GetEndIndex returns the configured end index for this iterator.
	// This is useful for accessing the intended range, validation, and
	// as a fallback when you want to use the configured range.
	GetEndIndex() uint32

	// GetBatchSize returns the configured batch size for this iterator.
	GetBatchSize() uint32
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
	// a valid chain.
	Validate(context.Context, HeaderIterator) error

	// ValidateBatch performs validation on a batch of headers, checking
	// that they form a valid chain segment.
	ValidateBatch([]Header) error

	// ValidatePair verifies that two consecutive headers (prev and current)
	// form a valid chain link.
	ValidatePair(prev, current Header) error

	// ValidateSingle validates a single header for basic sanity checks.
	ValidateSingle(header Header) error
}

// HttpClient defines the interface for making HTTP GET requests.
type HttpClient interface {
	Get(url string) (*http.Response, error)
}
