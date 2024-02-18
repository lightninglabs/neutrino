//go:build js && wasm

package headerfs

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"syscall/js"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/linden/indexeddb"
)

const headersStore = "headers"

// headerStore combines a IndexedDB store of headers within a flat file in addition
// to a database which indexes that flat file. Together, these two abstractions
// can be used in order to build an indexed header store for any type of
// "header" as it deals only with raw bytes, and leaves it to a higher layer to
// interpret those raw bytes accordingly.
//
// TODO(roasbeef): quickcheck coverage.
type headerStore struct {
	mtx sync.RWMutex // nolint:structcheck // false positive because used as embedded struct only

	idb *indexeddb.DB

	*headerIndex
}

// readHeader reads a full block header from the flat-file. The header read is
// determined by the hight value.
func (h *headerStore) readHeader(height uint32) (wire.BlockHeader, error) {
	// Create a new read/write transaction, scoped to headers.
	tx, err := h.idb.NewTransaction([]string{headersStore}, indexeddb.ReadWriteMode)
	if err != nil {
		return wire.BlockHeader{}, err
	}

	// Get the headers store.
	hdrs := tx.Store(headersStore)

	// Get the raw header.
	val, err := hdrs.Get(height)

	// check if the value was not found as that case is handled differently.
	if errors.Is(err, indexeddb.ErrValueNotFound) {
		return wire.BlockHeader{}, &ErrHeaderNotFound{err}
	}

	if err != nil {
		return wire.BlockHeader{}, err
	}

	// Ensure the value is a string.
	if val.Type() != js.TypeString {
		return wire.BlockHeader{}, fmt.Errorf("unexpected type: %s", val.Type())
	}

	var header wire.BlockHeader

	// Unquote the value.
	unquote, err := strconv.Unquote(val.String())
	if err != nil {
		return wire.BlockHeader{}, err
	}

	// Create a new reader from the raw header.
	reader := strings.NewReader(unquote)

	// Finally, decode the  bytes into a proper bitcoin header.
	err = header.Deserialize(reader)

	return header, err
}

func (h *headerStore) height() (uint32, bool, error) {
	// Create a new read-only transaction. Scoped to headers.
	tx, err := h.idb.NewTransaction([]string{headersStore}, indexeddb.ReadMode)
	if err != nil {
		return 0, false, err
	}

	// Get the headers store.
	hdrs := tx.Store(headersStore)

	// Count the amount of headers.
	count, err := hdrs.Count()

	// Fallback to the zero value if no value is found.
	if errors.Is(err, indexeddb.ErrValueNotFound) || count == 0 {
		return 0, true, nil
	}

	if err != nil {
		return 0, false, err
	}

	// Subtract one as the block height does not include the genesis block.
	return uint32(count - 1), false, nil
}

func (h *headerStore) singleTruncate() error {
	// Get the current height.
	height, genesis, err := h.height()
	if err != nil {
		return err
	}

	// Create a write transaction. Scoped to height.
	tx, err := h.idb.NewTransaction([]string{headersStore}, indexeddb.ReadWriteMode)
	if err != nil {
		return err
	}

	// Get the height store.
	hdrs := tx.Store(headersStore)

	// Delete the genesis block.
	if genesis {
		return hdrs.Delete("genesis")
	}

	// Delete the header.
	return hdrs.Delete(height + 1)
}

// Remove every key/value and set the height to 0.
func (h *headerStore) Remove() error {
	// Create a write transaction. Scoped to headers.
	tx, err := h.idb.NewTransaction([]string{headersStore}, indexeddb.ReadWriteMode)
	if err != nil {
		return err
	}

	// Get the headers store.
	hdrs := tx.Store(headersStore)

	// Clear all the headers.
	return hdrs.Clear()
}

// newHeaderStore creates a new headerStore given an already open database, a
// target file path for the flat-file and a particular header type. The target
// file will be created as necessary.
func newHeaderStore(db walletdb.DB, filePath string, hType HeaderType) (*headerStore, error) {
	var prefix string
	switch hType {
	case Block:
		prefix = "blocks"
	case RegularFilter:
		prefix = "filters"
	default:
		return nil, fmt.Errorf("unrecognized filter type: %v", hType)
	}

	// Prefix with the file path to prevent collisions.
	prefix = filePath + "-" + prefix

	// Create the database.
	idb, err := indexeddb.New(prefix, 1, func(up *indexeddb.Upgrade) error {
		// Create the headers store.
		up.CreateStore(headersStore)

		return nil
	})
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
		idb:         idb,
		headerIndex: index,
	}, nil
}

// readHeaderRange will attempt to fetch a series of block headers within the
// target height range.
//
// NOTE: The end height is _inclusive_ so we'll fetch all headers from the
// startHeight up to the end height, including the final header.
func (h *blockHeaderStore) readHeaderRange(startHeight uint32,
	endHeight uint32) ([]wire.BlockHeader, error) {
	var headers []wire.BlockHeader

	// Add headers from start height to end height.
	for height := startHeight; height <= endHeight; height++ {
		// Read the header.
		header, err := h.readHeader(height)
		if err != nil {
			return nil, err
		}

		// Append the header to the slice.
		headers = append(headers, header)
	}

	return headers, nil
}

// WriteHeaders writes a set of headers to disk and updates the index in a
// single atomic transaction.
//
// NOTE: Part of the BlockHeaderStore interface.
func (h *blockHeaderStore) WriteHeaders(hdrs ...BlockHeader) error {
	// Lock store for write.
	h.mtx.Lock()
	defer h.mtx.Unlock()

	height, genesis, err := h.height()
	if err != nil {
		return err
	}

	tx, err := h.idb.NewTransaction([]string{headersStore}, indexeddb.ReadWriteMode)
	if err != nil {
		return err
	}

	str := tx.Store(headersStore)

	// Next, we'll write out all the passed headers in series.
	for _, header := range hdrs {
		buf := new(bytes.Buffer)

		// Serialize the header.
		if err := header.Serialize(buf); err != nil {
			return err
		}

		var key any

		if genesis {
			key = height
			genesis = false
		} else {
			// Add space for the genesis block.
			key = height + 1
		}

		// Put the block header.
		err = str.Put(
			key,
			// Quote the string so it is UTF-8 safe.
			strconv.Quote(buf.String()),
		)
		if err != nil {
			return err
		}

		height++

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

// readHeader reads a single filter header at the specified height from the
// flat files on disk.
func (f *FilterHeaderStore) readHeader(height uint32) (*chainhash.Hash, error) {
	// Create a new read-only transaction.
	tx, err := f.idb.NewTransaction([]string{headersStore}, indexeddb.ReadMode)
	if err != nil {
		return nil, err
	}

	// Get the headers store.
	hdrs := tx.Store(headersStore)

	// Get the header.
	val, err := hdrs.Get(height)

	// check if the value was not found as that case is handled differently.
	if errors.Is(err, indexeddb.ErrValueNotFound) {
		return nil, &ErrHeaderNotFound{err}
	}

	if err != nil {
		return nil, err
	}

	// Ensure the value is a string.
	if val.Type() != js.TypeString {
		return nil, fmt.Errorf("unexpected type: %s", val.Type())
	}

	// Unquote the value.
	unquote, err := strconv.Unquote(val.String())
	if err != nil {
		return nil, err
	}

	// Cast the hash to a chainhash.
	return (*chainhash.Hash)([]byte(unquote)), nil
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
	var headers []chainhash.Hash

	// Add headers from start height to end height.
	for height := startHeight; height <= endHeight; height++ {
		// Read the header.
		header, err := f.readHeader(height)
		if err != nil {
			return nil, err
		}

		// Append the header to the slice.
		headers = append(headers, *header)
	}

	return headers, nil
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

	height, genesis, err := f.height()
	if err != nil {
		return err
	}

	// Create a new transaction.
	tx, err := f.idb.NewTransaction([]string{headersStore}, indexeddb.ReadWriteMode)
	if err != nil {
		return err
	}

	str := tx.Store(headersStore)

	// Next, we'll write out all the passed headers in series into the
	// buffer we just extracted from the pool.
	for _, header := range hdrs {
		var key any

		if genesis {
			key = height
			genesis = false
		} else {
			// Add space for the genesis block.
			key = height + 1
		}

		// Put the filter header.
		err = str.Put(
			key,
			// Encode the filter hash.
			strconv.Quote(string(header.FilterHash[:])),
		)
		if err != nil {
			return err
		}

		height++
	}

	// As the block headers should already be written, we only need to
	// update the tip pointer for this particular header type.
	newTip := hdrs[len(hdrs)-1].toIndexEntry().hash
	return f.truncateIndex(&newTip, false)
}
