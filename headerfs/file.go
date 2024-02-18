//go:build !windows && !js && !wasm

package headerfs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/walletdb"
)

// headerBufPool is a pool of bytes.Buffer that will be re-used by the various
// headerStore implementations to batch their header writes to disk. By
// utilizing this variable we can minimize the total number of allocations when
// writing headers to disk.
var headerBufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

// headerStore combines a on-disk set of headers within a flat file in addition
// to a database which indexes that flat file. Together, these two abstractions
// can be used in order to build an indexed header store for any type of
// "header" as it deals only with raw bytes, and leaves it to a higher layer to
// interpret those raw bytes accordingly.
//
// TODO(roasbeef): quickcheck coverage.
type headerStore struct {
	mtx sync.RWMutex // nolint:structcheck // false positive because used as embedded struct only

	fileName string

	file *os.File

	hType HeaderType

	*headerIndex
}

// newHeaderStore creates a new headerStore given an already open database, a
// target file path for the flat-file and a particular header type. The target
// file will be created as necessary.
func newHeaderStore(db walletdb.DB, filePath string,
	hType HeaderType) (*headerStore, error) {

	var flatFileName string
	switch hType {
	case Block:
		flatFileName = "block_headers.bin"
	case RegularFilter:
		flatFileName = "reg_filter_headers.bin"
	default:
		return nil, fmt.Errorf("unrecognized filter type: %v", hType)
	}

	flatFileName = filepath.Join(filePath, flatFileName)

	// We'll open the file, creating it if necessary and ensuring that all
	// writes are actually appends to the end of the file.
	fileFlags := os.O_RDWR | os.O_APPEND | os.O_CREATE
	headerFile, err := os.OpenFile(flatFileName, fileFlags, 0644)
	if err != nil {
		return nil, err
	}

	// With the file open, we'll then create the header index so we can
	// have random access into the flat files.
	index, err := newHeaderIndex(db, hType)
	if err != nil {
		return nil, err
	}

	return &headerStore{
		fileName:    flatFileName,
		file:        headerFile,
		hType:       hType,
		headerIndex: index,
	}, nil
}

// WriteHeaders writes a set of headers to disk and updates the index in a
// single atomic transaction.
//
// NOTE: Part of the BlockHeaderStore interface.
func (h *blockHeaderStore) WriteHeaders(hdrs ...BlockHeader) error {
	// Lock store for write.
	h.mtx.Lock()
	defer h.mtx.Unlock()

	// First, we'll grab a buffer from the write buffer pool so we an
	// reduce our total number of allocations, and also write the headers
	// in a single swoop.
	headerBuf := headerBufPool.Get().(*bytes.Buffer)
	headerBuf.Reset()
	defer headerBufPool.Put(headerBuf)

	// Next, we'll write out all the passed headers in series into the
	// buffer we just extracted from the pool.
	for _, header := range hdrs {
		if err := header.Serialize(headerBuf); err != nil {
			return err
		}
	}

	// With all the headers written to the buffer, we'll now write out the
	// entire batch in a single write call.
	if err := h.appendRaw(headerBuf.Bytes()); err != nil {
		return err
	}

	// Once those are written, we'll then collate all the headers into
	// headerEntry instances so we can write them all into the index in a
	// single atomic batch.
	headerLocs := make([]headerEntry, len(hdrs))
	for i, header := range hdrs {
		headerLocs[i] = header.toIndexEntry()
	}

	return h.addHeaders(headerLocs)
}

// WriteHeaders writes a batch of filter headers to persistent storage. The
// headers themselves are appended to the flat file, and then the index updated
// to reflect the new entires.
func (f *FilterHeaderStore) WriteHeaders(hdrs ...FilterHeader) error {
	// Lock store for write.
	f.mtx.Lock()
	defer f.mtx.Unlock()

	// If there are 0 headers to be written, return immediately. This
	// prevents the newTip assignment from panicking because of an index
	// of -1.
	if len(hdrs) == 0 {
		return nil
	}

	// First, we'll grab a buffer from the write buffer pool so we an
	// reduce our total number of allocations, and also write the headers
	// in a single swoop.
	headerBuf := headerBufPool.Get().(*bytes.Buffer)
	headerBuf.Reset()
	defer headerBufPool.Put(headerBuf)

	// Next, we'll write out all the passed headers in series into the
	// buffer we just extracted from the pool.
	for _, header := range hdrs {
		if _, err := headerBuf.Write(header.FilterHash[:]); err != nil {
			return err
		}
	}

	// With all the headers written to the buffer, we'll now write out the
	// entire batch in a single write call.
	if err := f.appendRaw(headerBuf.Bytes()); err != nil {
		return err
	}

	// As the block headers should already be written, we only need to
	// update the tip pointer for this particular header type.
	newTip := hdrs[len(hdrs)-1].toIndexEntry().hash
	return f.truncateIndex(&newTip, false)
}

// Remove the file.
func (h *headerStore) Remove() error {
	// Close the file before removing it. This is required by some
	// OS, e.g., Windows.
	if err := h.file.Close(); err != nil {
		return err
	}
	if err := os.Remove(h.fileName); err != nil {
		return err
	}

	return nil
}

// Calculate the current height.
func (h *headerStore) height() (uint32, bool, error) {
	fileInfo, err := h.file.Stat()
	if err != nil {
		return 0, false, err
	}

	size := fileInfo.Size()

	// Check if the file is empty. Fallback to a height of zero.
	if size == 0 {
		return 0, true, nil
	}

	var fileHeight uint32

	// Compute the size of the current file so we can
	// calculate the latest header written to disk.
	switch h.hType {
	case Block:
		fileHeight = uint32(size/80) - 1

	case RegularFilter:
		fileHeight = uint32(size/32) - 1
	}

	return fileHeight, false, nil
}

// appendRaw appends a new raw header to the end of the flat file.
func (h *headerStore) appendRaw(header []byte) error {
	if _, err := h.file.Write(header); err != nil {
		return err
	}

	return nil
}

// readRaw reads a raw header from disk from a particular seek distance. The
// amount of bytes read past the seek distance is determined by the specified
// header type.
func (h *headerStore) readRaw(seekDist uint64) ([]byte, error) {
	var headerSize uint32

	// Based on the defined header type, we'll determine the number of
	// bytes that we need to read past the sync point.
	switch h.indexType {
	case Block:
		headerSize = 80

	case RegularFilter:
		headerSize = 32

	default:
		return nil, fmt.Errorf("unknown index type: %v", h.indexType)
	}

	// TODO(roasbeef): add buffer pool

	// With the number of bytes to read determined, we'll create a slice
	// for that number of bytes, and read directly from the file into the
	// buffer.
	rawHeader := make([]byte, headerSize)
	if _, err := h.file.ReadAt(rawHeader, int64(seekDist)); err != nil {
		return nil, &ErrHeaderNotFound{err}
	}

	return rawHeader, nil
}

// readHeaderRange will attempt to fetch a series of block headers within the
// target height range. This method batches a set of reads into a single system
// call thereby increasing performance when reading a set of contiguous
// headers.
//
// NOTE: The end height is _inclusive_ so we'll fetch all headers from the
// startHeight up to the end height, including the final header.
func (h *blockHeaderStore) readHeaderRange(startHeight uint32,
	endHeight uint32) ([]wire.BlockHeader, error) {

	// Based on the defined header type, we'll determine the number of
	// bytes that we need to read from the file.
	headerReader, err := readHeadersFromFile(
		h.file, BlockHeaderSize, startHeight, endHeight,
	)
	if err != nil {
		return nil, err
	}

	// We'll now incrementally parse out the set of individual headers from
	// our set of serialized contiguous raw headers.
	numHeaders := endHeight - startHeight + 1
	headers := make([]wire.BlockHeader, 0, numHeaders)
	for headerReader.Len() != 0 {
		var nextHeader wire.BlockHeader
		if err := nextHeader.Deserialize(headerReader); err != nil {
			return nil, err
		}

		headers = append(headers, nextHeader)
	}

	return headers, nil
}

// readHeader reads a full block header from the flat-file. The header read is
// determined by the height value.
func (h *blockHeaderStore) readHeader(height uint32) (wire.BlockHeader, error) {
	var header wire.BlockHeader

	// Each header is 80 bytes, so using this information, we'll seek a
	// distance to cover that height based on the size of block headers.
	seekDistance := uint64(height) * 80

	// With the distance calculated, we'll raw a raw header start from that
	// offset.
	rawHeader, err := h.readRaw(seekDistance)
	if err != nil {
		return header, err
	}
	headerReader := bytes.NewReader(rawHeader)

	// Finally, decode the raw bytes into a proper bitcoin header.
	if err := header.Deserialize(headerReader); err != nil {
		return header, err
	}

	return header, nil
}

// readHeader reads a single filter header at the specified height from the
// flat files on disk.
func (f *FilterHeaderStore) readHeader(height uint32) (*chainhash.Hash, error) {
	seekDistance := uint64(height) * 32

	rawHeader, err := f.readRaw(seekDistance)
	if err != nil {
		return nil, err
	}

	return chainhash.NewHash(rawHeader)
}

// readHeaderRange will attempt to fetch a series of filter headers within the
// target height range. This method batches a set of reads into a single system
// call thereby increasing performance when reading a set of contiguous
// headers.
//
// NOTE: The end height is _inclusive_ so we'll fetch all headers from the
// startHeight up to the end height, including the final header.
func (f *FilterHeaderStore) readHeaderRange(startHeight uint32,
	endHeight uint32) ([]chainhash.Hash, error) {

	// Based on the defined header type, we'll determine the number of
	// bytes that we need to read from the file.
	headerReader, err := readHeadersFromFile(
		f.file, RegularFilterHeaderSize, startHeight, endHeight,
	)
	if err != nil {
		return nil, err
	}

	// We'll now incrementally parse out the set of individual headers from
	// our set of serialized contiguous raw headers.
	numHeaders := endHeight - startHeight + 1
	headers := make([]chainhash.Hash, 0, numHeaders)
	for headerReader.Len() != 0 {
		var nextHeader chainhash.Hash
		if _, err := headerReader.Read(nextHeader[:]); err != nil {
			return nil, err
		}

		headers = append(headers, nextHeader)
	}

	return headers, nil
}

// readHeadersFromFile reads a chunk of headers, each of size headerSize, from
// the given file, from startHeight to endHeight.
func readHeadersFromFile(f *os.File, headerSize, startHeight,
	endHeight uint32) (*bytes.Reader, error) {

	// Each header is headerSize bytes, so using this information, we'll
	// seek a distance to cover that height based on the size the headers.
	seekDistance := uint64(startHeight) * uint64(headerSize)

	// Based on the number of headers in the range, we'll allocate a single
	// slice that's able to hold the entire range of headers.
	numHeaders := endHeight - startHeight + 1
	rawHeaderBytes := make([]byte, headerSize*numHeaders)

	// Now that we have our slice allocated, we'll read out the entire
	// range of headers with a single system call.
	_, err := f.ReadAt(rawHeaderBytes, int64(seekDistance))
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(rawHeaderBytes), nil
}
