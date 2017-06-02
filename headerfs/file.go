package headerfs

import (
	"bytes"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

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
func (h *headerStore) readRaw(seekDist int64) ([]byte, error) {
	var headerSize uint32

	// Based on the defined header type, we'll determine the number of
	// bytes that we need to read past the sync point.
	switch h.indexType {
	case Block:
		headerSize = 80
	case RegularFilter:
		fallthrough
	case ExtendedFilter:
		headerSize = 32
	}

	// TODO(roasbeef): add buffer pool

	// With the number of bytes to read determined, we'll create a slice
	// for that number of bytes, and read directly from the file into the
	// buffer.
	rawHeader := make([]byte, headerSize)
	if _, err := h.file.ReadAt(rawHeader, seekDist); err != nil {
		return nil, err
	}

	return rawHeader, nil
}

// singleTruncate truncates a single header from the end of the header file.
// This can be used in the case of a re-org to remove the last header from the
// end of the main chain.
//
// TODO(roasbeef): define this and the two methods above on a headerFile
// struct?
func (h *headerStore) singleTruncate() error {
	// In order to truncate the file, we'll need to grab the absolute size
	// of the file as it stands currently.
	fileInfo, err := h.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// Next, we'll determine the number of bytes we need to truncate from
	// the end of the file.
	var truncateLength int64
	switch h.indexType {
	case Block:
		truncateLength = 80
	case RegularFilter:
		fallthrough
	case ExtendedFilter:
		truncateLength = 32
	}

	// Finally, we'll use both of these values to calculate the new size of
	// the file and truncate it accordingly.
	newSize := fileSize - truncateLength
	return h.file.Truncate(newSize)
}

// readHeader reads a full block header from the flat-file. The header read is
// determined by the hight value.
func (h *BlockHeaderStore) readHeader(height int64) (*wire.BlockHeader, error) {
	// Each header is 80 bytes, so using this information, we'll seek a
	// distance to cover that height based on the size of block headers.
	seekDistance := height * 80

	// With the distance calculated, we'll raw a raw header start from that
	// offset.
	rawHeader, err := h.readRaw(seekDistance)
	if err != nil {
		return nil, err
	}
	headerReader := bytes.NewReader(rawHeader)

	// Finally, decode the raw bytes into a proper bitcoin header.
	var header wire.BlockHeader
	if err := header.Deserialize(headerReader); err != nil {
		return nil, err
	}

	return &header, nil
}

// readHeader reads a single filter header at the specified height from the
// flat files on disk.
func (f *FilterHeaderStore) readHeader(height int64) (*chainhash.Hash, error) {
	seekDistance := height * 32

	rawHeader, err := f.readRaw(seekDistance)
	if err != nil {
		return nil, err
	}

	return chainhash.NewHash(rawHeader)
}
