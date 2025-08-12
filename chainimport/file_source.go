package chainimport

import (
	"bytes"
	"errors"
	"fmt"

	"golang.org/x/exp/mmap"
)

// fileHeaderImportSource implements headerImportSource for header files.
//
// Expected file format:
//   - ImportMetadata (10 bytes): Network magic (4), version (1),
//     header type (1), start height (4)
//   - Header data: Consecutive raw headers starting from the specified height
//
// The file contains a fixed-size metadata header followed by a sequence of
// blockchain headers. Each header's size depends on its type – 80 bytes for
// block headers and 32 bytes for filter headers. Headers must be stored
// consecutively without gaps or padding.
type fileHeaderImportSource struct {
	// uri is the file path or location identifier for the header source.
	uri string

	// file provides the underlying file interface for reading header data.
	file ImportHeadersFile

	// fileSize stores the total size of the header file in bytes.
	fileSize int

	// metadata contains parsed header metadata.
	metadata *headerMetadata

	// headerFactory creates new Header instances for deserialization.
	headerFactory func() Header

	// headerBuffer is a pre-allocated buffer for reading header data.
	headerBuffer []byte
}

// Compile-time assertion to ensure fileHeaderImportSource implements
// headerImportSource interface.
var _ HeaderImportSource = (*fileHeaderImportSource)(nil)

// newFileHeaderImportSource creates a new file header import source with the
// given URI and header factory.
func newFileHeaderImportSource(uri string,
	headerFactory func() Header) *fileHeaderImportSource {

	return &fileHeaderImportSource{
		uri:           uri,
		headerFactory: headerFactory,
	}
}

// Open opens the file and initializes the reader.
func (f *fileHeaderImportSource) Open() error {
	r, err := mmap.Open(f.GetURI())
	if err != nil {
		return fmt.Errorf("failed to mmap file: %w", err)
	}

	f.file = newMmapFile(r)
	f.fileSize = f.file.Len()

	mData, err := f.GetHeaderMetadata()
	if err != nil {
		return fmt.Errorf("failed to get header metadata: %w", err)
	}
	f.metadata = mData
	f.metadata.endHeight = mData.startHeight + mData.headersCount - 1

	f.headerBuffer = make([]byte, mData.headerSize)
	return nil
}

// Close closes the file and releases the mmap reader.
func (f *fileHeaderImportSource) Close() error {
	return f.file.Close()
}

// GetHeaderMetadata reads the metadata from the file. The metadata is memoized
// after the first call, with subsequent calls returning the cached result
// without re-reading the file.
func (f *fileHeaderImportSource) GetHeaderMetadata() (*headerMetadata, error) {
	if f.metadata != nil {
		return f.metadata, nil
	}

	if f.file == nil {
		return nil, errors.New("file reader not initialized")
	}

	importMetadata := &importMetadata{}
	if err := importMetadata.decode(f.file); err != nil {
		return nil, err
	}

	headerMetadata := &headerMetadata{
		importMetadata: importMetadata,
	}

	headerSize, err := importMetadata.headerType.Size()
	if err != nil {
		return nil, fmt.Errorf("failed to get header size: %v", err)
	}
	headerMetadata.headerSize = headerSize

	usableFileSize := f.fileSize - importMetadata.size()

	if usableFileSize == 0 {
		return nil, errors.New("no headers available in import source")
	}

	if usableFileSize%headerSize != 0 {
		return nil, fmt.Errorf("file size (%d) is not a multiple of "+
			"header size (%d); possible data corruption",
			usableFileSize, headerSize)
	}

	headerMetadata.headersCount = uint32(usableFileSize / headerSize)

	return headerMetadata, err
}

// GetHeader retrieves a single header at the specified index.
func (f *fileHeaderImportSource) GetHeader(index uint32) (Header, error) {
	var empty Header

	if f.file == nil {
		return empty, errors.New("file reader not initialized")
	}
	if f.metadata == nil {
		return empty, errors.New("header metadata not initialized")
	}

	offset := ImportMetadataSize + (index * uint32(f.metadata.headerSize))

	_, err := f.file.ReadAt(f.headerBuffer, int64(offset))
	if err != nil {
		return empty, fmt.Errorf("failed to read header at "+
			"index %d: %w", index, err)
	}
	reader := bytes.NewReader(f.headerBuffer)

	height := index + f.metadata.startHeight

	header := f.headerFactory()
	if err := header.Deserialize(reader, height); err != nil {
		return empty, err
	}

	return header, nil
}

// SetURI sets the file path for this import source. This method is primarily
// used by HTTP import sources to dynamically update the file path after
// downloading headers to a temporary file.
func (f *fileHeaderImportSource) SetURI(uri string) {
	f.uri = uri
}

// GetURI returns the file path for this import source.
func (f *fileHeaderImportSource) GetURI() string {
	return f.uri
}

// mmapFile wraps mmap.ReaderAt to provide ImportHeadersFile interface.
type mmapFile struct {
	readerAt *mmap.ReaderAt
	offset   int64
}

// Compile-time assertion to ensure mmapFile implements ImportHeadersFile
// interface.
var _ ImportHeadersFile = (*mmapFile)(nil)

// newMmapFile creates a new memory-mapped file adapter for mmap.ReaderAt.
func newMmapFile(readerAt *mmap.ReaderAt) *mmapFile {
	return &mmapFile{
		readerAt: readerAt,
		offset:   0,
	}
}

// Read implements io.Reader interface.
func (m *mmapFile) Read(p []byte) (int, error) {
	n, err := m.readerAt.ReadAt(p, m.offset)
	m.offset += int64(n)
	return n, err
}

// ReadAt implements io.ReaderAt interface.
func (m *mmapFile) ReadAt(p []byte, off int64) (int, error) {
	return m.readerAt.ReadAt(p, off)
}

// Close implements io.Closer interface.
func (m *mmapFile) Close() error {
	return m.readerAt.Close()
}

// Len returns the length of the underlying reader.
func (m *mmapFile) Len() int {
	return m.readerAt.Len()
}
