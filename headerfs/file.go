package headerfs

import (
	"bytes"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

// ErrHeaderNotFound is returned when a target header on disk (flat file) can't
// be found.
type ErrHeaderNotFound struct {
	error
}

// appendRaw appends a new raw header to the end of the flat file.
func (h *headerStore) appendRaw(header []byte) error {
	// Get current file position before writing. We'll use this position to
	// revert to if the write fails partially.
	currentPos, err := h.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := h.file.Sync(); err != nil {
		return err
	}

	n, err := h.file.Write(header)
	if err != nil {
		// If we wrote some bytes but not all (partial write),
		// truncate the file back to its original size to maintain
		// consistency. This removes the partial/corrupt header.
		if n > 0 {
			truncErr := h.file.Truncate(currentPos)
			if truncErr != nil {
				return fmt.Errorf("failed to write header "+
					"type %s: partial write (%d bytes), "+
					"write error: %w, truncate error: %v",
					h.indexType, n, err, truncErr)
			}
		}

		return fmt.Errorf("failed to write header type %s: write "+
			"error: %w", h.indexType, err)
	}

	return nil
}

// readRaw reads a raw header from disk from a particular seek distance. The
// amount of bytes read past the seek distance is determined by the specified
// header type.
func (h *headerStore) readRaw(seekDist uint64) ([]byte, error) {
	// Based on the defined header type, we'll determine the number of bytes
	// that we need to read past the sync point.
	headerSize, err := h.indexType.Size()
	if err != nil {
		return nil, err
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
func (f *filterHeaderStore) readHeader(height uint32) (*chainhash.Hash, error) {
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
func (f *filterHeaderStore) readHeaderRange(startHeight uint32,
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
func readHeadersFromFile(f File, headerSize, startHeight,
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
