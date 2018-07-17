package headerfs

import (
	"bytes"
	"fmt"

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
		return nil, err
	}

	return rawHeader, nil
}

// readHeader reads a full block header from the flat-file. The header read is
// determined by the hight value.
func (h *BlockHeaderStore) readHeader(height uint32) (*wire.BlockHeader, error) {
	// Each header is 80 bytes, so using this information, we'll seek a
	// distance to cover that height based on the size of block headers.
	seekDistance := uint64(height) * 80

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
func (f *FilterHeaderStore) readHeader(height uint32) (*chainhash.Hash, error) {
	seekDistance := uint64(height) * 32

	rawHeader, err := f.readRaw(seekDistance)
	if err != nil {
		return nil, err
	}

	return chainhash.NewHash(rawHeader)
}
