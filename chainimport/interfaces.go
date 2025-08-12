package chainimport

import "io"

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

// HeaderImportSource is an interface for loading headers from various sources
// (files, HTTP endpoints) with support for both sequential iteration and
// efficient random access using memory-mapped files.
type HeaderImportSource interface {
	// Open initializes the header source and prepares it for reading.
	Open() error

	// Close releases any resources associated with the header source.
	Close() error

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
