package headerfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/walletdb"
)

// BlockStamp represents a block, identified by its height and time stamp in
// the chain. We also lift the timestamp from the block header itself into this
// struct as well.
type BlockStamp struct {
	// Height is the height of the target block.
	Height int32

	// Hash is the hash that uniquely identifies this block.
	Hash chainhash.Hash

	// Timestamp is the timestamp of the block in the chain.
	Timestamp time.Time
}

// BlockHeaderStore is an interface that provides an abstraction for a generic
// store for block headers.
type BlockHeaderStore interface {
	// ChainTip returns the best known block header and height for the
	// BlockHeaderStore.
	ChainTip() (*wire.BlockHeader, uint32, error)

	// LatestBlockLocator returns the latest block locator object based on
	// the tip of the current main chain from the PoV of the
	// BlockHeaderStore.
	LatestBlockLocator() (blockchain.BlockLocator, error)

	// FetchHeaderByHeight attempts to retrieve a target block header based
	// on a block height.
	FetchHeaderByHeight(height uint32) (*wire.BlockHeader, error)

	// FetchHeaderAncestors fetches the numHeaders block headers that are
	// the ancestors of the target stop hash. A total of numHeaders+1
	// headers will be returned, as we'll walk back numHeaders distance to
	// collect each header, then return the final header specified by the
	// stop hash. We'll also return the starting height of the header range
	// as well so callers can compute the height of each header without
	// knowing the height of the stop hash.
	FetchHeaderAncestors(uint32, *chainhash.Hash) ([]wire.BlockHeader,
		uint32, error)

	// HeightFromHash returns the height of a particular block header given
	// its hash.
	HeightFromHash(*chainhash.Hash) (uint32, error)

	// FetchHeader attempts to retrieve a block header determined by the
	// passed block height.
	FetchHeader(*chainhash.Hash) (*wire.BlockHeader, uint32, error)

	// WriteHeaders adds a set of headers to the BlockHeaderStore in a
	// single atomic transaction.
	WriteHeaders(...BlockHeader) error

	// RollbackBlockHeaders rolls back a specified number of headers from
	// the tip of the chain. It removes the most recent 'numHeaders' from
	// the block headers file and updates the corresponding indices. This
	// method is used during blockchain reorganizations to remove headers
	// that are no longer part of the main chain. The function will return
	// an error if the rollback would reach or go before the genesis block
	// (height 0). The information about the new header tip after truncation
	// is returned.
	RollbackBlockHeaders(numHeaders uint32) (*BlockStamp, error)

	// RollbackLastBlock rolls back the BlockHeaderStore by a _single_
	// header. This method is meant to be used in the case of re-org which
	// disconnects the latest block header from the end of the main chain.
	// The information about the new header tip after truncation is
	// returned.
	RollbackLastBlock() (*BlockStamp, error)
}

// headerBufPool is a pool of bytes.Buffer that will be re-used by the various
// headerStore implementations to batch their header writes to disk. By
// utilizing this variable we can minimize the total number of allocations when
// writing headers to disk.
var headerBufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

// File defines the minimum file operations needed by headerStore.
type File interface {
	// Basic I/O operations.
	io.Reader
	io.Writer
	io.Closer

	// Extended I/O positioning.
	io.Seeker
	io.ReaderAt

	// File-specific operations.
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error

	// Returns the name of the file.
	Name() string
}

type headerFile struct {
	file File
}

// truncateHeaders truncates one or more headers from the end of the header
// file. This can be used in the case of a re-org to remove headers from the
// end of the main chain.
//
// The numHeaders parameter specifies how many headers to truncate.
// If numHeaders is 1, this is equivalent to the old singleTruncate behavior.
//
// This function handles platform-specific differences in file truncation.
// On Windows, the file is closed, truncated, and reopened due to Windows
// limitations on truncating open files. On other platforms, the file is
// truncated directly without closing.
func (h *headerFile) truncateHeaders(numHeaders uint32,
	headerType HeaderType) error {

	// If numHeaders is 0, treat it as a no-op and return no error.
	if numHeaders == 0 {
		return nil
	}

	// In order to truncate the file, we'll need to grab the absolute size
	// of the file as it stands currently.
	fileInfo, err := h.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// Calculate the total bytes to truncate based on number of headers.
	headerTypeSize, err := headerType.Size()
	if err != nil {
		return err
	}
	truncateLength := int64(numHeaders) * int64(headerTypeSize)

	// Finally, we'll use both of these values to calculate the new size of
	// the file and truncate it accordingly.
	newSize := fileSize - truncateLength

	// On Windows, we need to close, truncate, and reopen the file.
	if runtime.GOOS == "windows" {
		fileName := h.file.Name()
		if err = h.file.Close(); err != nil {
			return err
		}

		if err = os.Truncate(fileName, newSize); err != nil {
			return err
		}

		fileFlags := os.O_RDWR | os.O_APPEND | os.O_CREATE
		h.file, err = os.OpenFile(fileName, fileFlags, 0644)
		return err
	}

	return h.file.Truncate(newSize)
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

	*headerFile

	*headerIndex
}

// newHeaderStore creates a new headerStore given an already open database, a
// target file path for the flat-file and a particular header type. The target
// file will be created as necessary.
func newHeaderStore(db walletdb.DB, filePath string,
	hType HeaderType) (*headerStore, error) {

	var headerFile = &headerFile{}

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
	file, err := os.OpenFile(flatFileName, fileFlags, 0644)
	if err != nil {
		return nil, err
	}
	headerFile.file = file

	// With the file open, we'll then create the header index so we can
	// have random access into the flat files.
	index, err := newHeaderIndex(db, hType)
	if err != nil {
		return nil, err
	}

	return &headerStore{
		headerFile:  headerFile,
		headerIndex: index,
	}, nil
}

// blockHeaderStore is an implementation of the BlockHeaderStore interface, a
// fully fledged database for Bitcoin block headers. The blockHeaderStore
// combines a flat file to store the block headers with a database instance for
// managing the index into the set of flat files.
type blockHeaderStore struct {
	*headerStore
}

// A compile-time check to ensure the blockHeaderStore adheres to the
// BlockHeaderStore interface.
var _ BlockHeaderStore = (*blockHeaderStore)(nil)

// NewBlockHeaderStore creates a new instance of the blockHeaderStore based on
// a target file path, an open database instance, and finally a set of
// parameters for the target chain. These parameters are required as if this is
// the initial start up of the blockHeaderStore, then the initial genesis
// header will need to be inserted.
func NewBlockHeaderStore(filePath string, db walletdb.DB,
	netParams *chaincfg.Params) (BlockHeaderStore, error) {

	hStore, err := newHeaderStore(db, filePath, Block)
	if err != nil {
		return nil, err
	}

	// With the header store created, we'll fetch the file size to see if
	// we need to initialize it with the first header or not.
	fileInfo, err := hStore.file.Stat()
	if err != nil {
		return nil, err
	}

	bhs := &blockHeaderStore{
		headerStore: hStore,
	}

	// If the size of the file is zero, then this means that we haven't yet
	// written the initial genesis header to disk, so we'll do so now.
	if fileInfo.Size() == 0 {
		genesisHeader := BlockHeader{
			BlockHeader: &netParams.GenesisBlock.Header,
			Height:      0,
		}
		if err := bhs.WriteHeaders(genesisHeader); err != nil {
			return nil, err
		}

		return bhs, nil
	}

	// As a final initialization step (if this isn't the first time), we'll
	// ensure that the header tip within the flat files, is in sync with
	// out database index.
	tipHash, tipHeight, err := bhs.chainTip()
	if err != nil {
		return nil, err
	}

	// First, we'll compute the size of the current file so we can
	// calculate the latest header written to disk.
	fileHeight := uint32(fileInfo.Size()/80) - 1

	// Using the file's current height, fetch the latest on-disk header.
	latestFileHeader, err := bhs.readHeader(fileHeight)
	if err != nil {
		return nil, err
	}

	// If the index's tip hash, and the file on-disk match, then we're
	// done here.
	latestBlockHash := latestFileHeader.BlockHash()
	if tipHash.IsEqual(&latestBlockHash) {
		return bhs, nil
	}

	// TODO(roasbeef): below assumes index can never get ahead?
	//  * we always update files _then_ indexes
	//  * need to dual pointer walk back for max safety

	// Otherwise, we'll need to truncate the file until it matches the
	// current index tip.
	err = bhs.truncateHeaders(fileHeight-tipHeight, Block)
	if err != nil {
		return nil, err
	}

	return bhs, nil
}

// FetchHeader attempts to retrieve a block header determined by the passed
// block height.
//
// NOTE: Part of the BlockHeaderStore interface.
func (h *blockHeaderStore) FetchHeader(hash *chainhash.Hash) (*wire.BlockHeader, uint32, error) {
	// Lock store for read.
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	// First, we'll query the index to obtain the block height of the
	// passed block hash.
	height, err := h.heightFromHash(hash)
	if err != nil {
		return nil, 0, err
	}

	// With the height known, we can now read the header from disk.
	header, err := h.readHeader(height)
	if err != nil {
		return nil, 0, err
	}

	return &header, height, nil
}

// FetchHeaderByHeight attempts to retrieve a target block header based on a
// block height.
//
// NOTE: Part of the BlockHeaderStore interface.
func (h *blockHeaderStore) FetchHeaderByHeight(height uint32) (*wire.BlockHeader, error) {
	// Lock store for read.
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	// For this query, we don't need to consult the index, and can instead
	// just seek into the flat file based on the target height and return
	// the full header.
	header, err := h.readHeader(height)
	if err != nil {
		return nil, err
	}

	return &header, nil
}

// FetchHeaderAncestors fetches the numHeaders block headers that are the
// ancestors of the target stop hash. A total of numHeaders+1 headers will be
// returned, as we'll walk back numHeaders distance to collect each header,
// then return the final header specified by the stop hash. We'll also return
// the starting height of the header range as well so callers can compute the
// height of each header without knowing the height of the stop hash.
//
// NOTE: Part of the BlockHeaderStore interface.
func (h *blockHeaderStore) FetchHeaderAncestors(numHeaders uint32,
	stopHash *chainhash.Hash) ([]wire.BlockHeader, uint32, error) {

	// First, we'll find the final header in the range, this will be the
	// ending height of our scan.
	endHeight, err := h.heightFromHash(stopHash)
	if err != nil {
		return nil, 0, err
	}
	startHeight := endHeight - numHeaders

	headers, err := h.readHeaderRange(startHeight, endHeight)
	if err != nil {
		return nil, 0, err
	}

	return headers, startHeight, nil
}

// HeightFromHash returns the height of a particular block header given its
// hash.
//
// NOTE: Part of the BlockHeaderStore interface.
func (h *blockHeaderStore) HeightFromHash(hash *chainhash.Hash) (uint32, error) {
	return h.heightFromHash(hash)
}

// RollbackBlockHeaders removes the specified number of block headers from the
// end of the chain. It returns a BlockStamp representing the new chain tip. If
// numHeaders is 0, it returns an empty BlockStamp without performing any
// operations.
//
// The function ensures rollback doesn't precede genesis block
// (height 0), reads the header range to be removed plus the new tip header,
// truncates the headers file to remove the specified number of headers, and
// updates the header indices to reflect the new chain tip.
func (h *blockHeaderStore) RollbackBlockHeaders(
	numHeaders uint32) (*BlockStamp, error) {

	if numHeaders == 0 {
		return &BlockStamp{}, nil
	}

	// Lock store for rollback.
	h.mtx.Lock()
	defer h.mtx.Unlock()

	// First, we'll obtain the latest height that the index knows of.
	_, chainTipHeight, err := h.chainTip()
	if err != nil {
		return nil, err
	}

	// Check if this rollback would go beyond the genesis block.
	if numHeaders > chainTipHeight+1 {
		return nil, fmt.Errorf("cannot roll back %d headers when "+
			"chain height is %d", numHeaders, chainTipHeight)
	}

	// With this height obtained, we'll use it to read the previous header
	// from disk, so we can populate our return value which requires the
	// prev header hash.
	headers, err := h.readHeaderRange(
		chainTipHeight-numHeaders, chainTipHeight,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to read headers range: %v", err)
	}
	prevHeader := headers[0]
	prevHeaderHash := prevHeader.BlockHash()

	// Transform to blockhashes for downstream operations, starting at
	// headers + 1 skipping the previous header.
	headersToTruncate := make([]*chainhash.Hash, len(headers)-1)
	for i, header := range headers[1:] {
		blkHash := header.BlockHash()
		headersToTruncate[i] = &blkHash
	}

	err = h.truncateHeaders(numHeaders, h.indexType)
	if err != nil {
		return nil, err
	}

	err = h.truncateIndices(&prevHeaderHash, headersToTruncate, true)
	if err != nil {
		return nil, err
	}

	return &BlockStamp{
		Height:    int32(chainTipHeight) - 1,
		Hash:      prevHeaderHash,
		Timestamp: prevHeader.Timestamp,
	}, nil
}

// RollbackLastBlock rollsback both the index, and on-disk header file by a
// _single_ header. This method is meant to be used in the case of re-org which
// disconnects the latest block header from the end of the main chain. The
// information about the new header tip after truncation is returned.
//
// NOTE: Part of the BlockHeaderStore interface.
func (h *blockHeaderStore) RollbackLastBlock() (*BlockStamp, error) {
	return h.RollbackBlockHeaders(1)
}

// BlockHeader is a Bitcoin block header that also has its height included.
type BlockHeader struct {
	*wire.BlockHeader

	// Height is the height of this block header within the current main
	// chain.
	Height uint32
}

// toIndexEntry converts the BlockHeader into a matching headerEntry. This
// method is used when a header is to be written to disk.
func (b *BlockHeader) toIndexEntry() headerEntry {
	return headerEntry{
		hash:   b.BlockHash(),
		height: b.Height,
	}
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

	// Attempt to add the headers to the database. If this fails, we'll need
	// to roll back any changes to the header file to maintain consistency.
	// The rollback process bases on the number of header serialized. If
	// both the initial operation and the rollback fail, we return
	// a detailed error explaining both failures to aid in debugging.
	if err := h.addHeaders(headerLocs); err != nil {
		// Since the probability of failing to write to the database is
		// very low, it is mostly worth the cost of file sync operation
		// to make sure truncate headers does it correctly.
		if err := h.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync block headers "+
				"file: %v", err)
		}

		headersToRollback := len(hdrs)
		truncateErr := h.truncateHeaders(
			uint32(headersToRollback), h.indexType,
		)
		if truncateErr != nil {
			return fmt.Errorf("failed to rollback block headers "+
				"binary file to previous valid state: %v, "+
				"error writing to database: %v", truncateErr,
				err)
		}
		return fmt.Errorf("failed to add block headers to db: %v", err)
	}

	return nil
}

// blockLocatorFromHash takes a given block hash and then creates a block
// locator using it as the root of the locator. We'll start by taking a single
// step backwards, then keep doubling the distance until genesis after we get
// 10 locators.
//
// TODO(roasbeef): make into single transaction.
func (h *blockHeaderStore) blockLocatorFromHash(hash *chainhash.Hash) (
	blockchain.BlockLocator, error) {

	var locator blockchain.BlockLocator

	// Append the initial hash
	locator = append(locator, hash)

	// If hash isn't found in DB or this is the genesis block, return the
	// locator as is.
	height, err := h.heightFromHash(hash)
	if height == 0 || err != nil {
		return locator, nil
	}

	decrement := uint32(1)
	for height > 0 && len(locator) < wire.MaxBlockLocatorsPerMsg {
		// Decrement by 1 for the first 10 blocks, then double the jump
		// until we get to the genesis hash
		if len(locator) > 10 {
			decrement *= 2
		}

		if decrement > height {
			height = 0
		} else {
			height -= decrement
		}

		blockHeader, err := h.FetchHeaderByHeight(height)
		if err != nil {
			return locator, err
		}
		headerHash := blockHeader.BlockHash()

		locator = append(locator, &headerHash)
	}

	return locator, nil
}

// LatestBlockLocator returns the latest block locator object based on the tip
// of the current main chain from the PoV of the database and flat files.
//
// NOTE: Part of the BlockHeaderStore interface.
func (h *blockHeaderStore) LatestBlockLocator() (blockchain.BlockLocator, error) {
	// Lock store for read.
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	var locator blockchain.BlockLocator

	chainTipHash, _, err := h.chainTip()
	if err != nil {
		return locator, err
	}

	return h.blockLocatorFromHash(chainTipHash)
}

// BlockLocatorFromHash computes a block locator given a particular hash. The
// standard Bitcoin algorithm to compute block locators are employed.
func (h *blockHeaderStore) BlockLocatorFromHash(hash *chainhash.Hash) (
	blockchain.BlockLocator, error) {

	// Lock store for read.
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	return h.blockLocatorFromHash(hash)
}

// CheckConnectivity cycles through all of the block headers on disk, from last
// to first, and makes sure they all connect to each other. Additionally, at
// each block header, we also ensure that the index entry for that height and
// hash also match up properly.
func (h *blockHeaderStore) CheckConnectivity() error {
	// Lock store for read.
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	return walletdb.View(h.db, func(tx walletdb.ReadTx) error {
		// First, we'll fetch the chain tip so we can start our
		// backwards scan.
		_, tipHeight, err := h.chainTipWithTx(tx)
		if err != nil {
			return err
		}

		// With the height extracted, we'll now read the _last_ block
		// header within the file before we kick off our connectivity
		// loop.
		header, err := h.readHeader(tipHeight)
		if err != nil {
			return err
		}

		// We'll now cycle backwards, seeking backwards along the
		// header file to ensure each header connects properly and the
		// index entries are also accurate. To do this, we start from a
		// height of one before our current tip.
		var newHeader wire.BlockHeader
		for height := tipHeight - 1; height > 0; height-- {
			// First, read the block header for this block height,
			// and also compute the block hash for it.
			newHeader, err = h.readHeader(height)
			if err != nil {
				return fmt.Errorf("couldn't retrieve header "+
					"%s: %s", header.PrevBlock, err)
			}
			newHeaderHash := newHeader.BlockHash()

			// With the header retrieved, we'll now fetch the
			// height for this current header hash to ensure the
			// on-disk state and the index matches up properly.
			indexHeight, err := h.heightFromHashWithTx(
				tx, &newHeaderHash,
			)
			if err != nil {
				return fmt.Errorf("index and on-disk file "+
					"out of sync at height: %v", height)
			}

			// With the index entry retrieved, we'll now assert
			// that the height matches up with our current height
			// in this backwards walk.
			if indexHeight != height {
				return fmt.Errorf("index height isn't " +
					"monotonically increasing")
			}

			// Finally, we'll assert that this new header is
			// actually the prev header of the target header from
			// the last loop. This ensures connectivity.
			if newHeader.BlockHash() != header.PrevBlock {
				return fmt.Errorf("block %s doesn't match "+
					"block %s's PrevBlock (%s)",
					newHeader.BlockHash(),
					header.BlockHash(), header.PrevBlock)
			}

			// As all the checks have passed, we'll now reset our
			// header pointer to this current location, and
			// continue our backwards walk.
			header = newHeader
		}

		return nil
	})
}

// ChainTip returns the best known block header and height for the
// blockHeaderStore.
//
// NOTE: Part of the BlockHeaderStore interface.
func (h *blockHeaderStore) ChainTip() (*wire.BlockHeader, uint32, error) {
	// Lock store for read.
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	_, tipHeight, err := h.chainTip()
	if err != nil {
		return nil, 0, fmt.Errorf("unable to fetch chain tip: %v", err)
	}

	latestHeader, err := h.readHeader(tipHeight)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to read block header: %v", err)
	}

	return &latestHeader, tipHeight, nil
}

// FilterHeaderStore defines the interface for storing and retrieving filter
// headers.
type FilterHeaderStore interface {
	// ChainTip returns the hash and height of the latest filter header.
	ChainTip() (*chainhash.Hash, uint32, error)

	// FetchHeader fetches the filter header for a specific block hash.
	FetchHeader(hash *chainhash.Hash) (*chainhash.Hash, error)

	// FetchHeaderAncestors fetches the given number of headers starting
	// from the specified stop hash and working backwards.
	FetchHeaderAncestors(numHeaders uint32,
		stopHash *chainhash.Hash) ([]chainhash.Hash, uint32, error)

	// FetchHeaderByHeight fetches the filter header for a specific block
	// height.
	FetchHeaderByHeight(height uint32) (*chainhash.Hash, error)

	// WriteHeaders writes a set of filter headers to the store.
	WriteHeaders(hdrs ...FilterHeader) error

	// RollbackLastBlock rolls back the last block, returning the new tip
	// after rollback.
	RollbackLastBlock(newTip *chainhash.Hash) (*BlockStamp, error)
}

// filterHeaderStore is an implementation of a fully fledged database for any
// variant of filter headers. The filterHeaderStore combines a flat file to
// store the block headers with a database instance for managing the index into
// the set of flat files.
type filterHeaderStore struct {
	*headerStore
}

// Compile-time assertion to ensure filterHeaderStore implements
// FilterHeaderStore interface.
var _ FilterHeaderStore = (*filterHeaderStore)(nil)

// NewFilterHeaderStore returns a new instance of the FilterHeaderStore based
// on a target file path, filter type, and target net parameters. These
// parameters are required as if this is the initial start up of the
// FilterHeaderStore, then the initial genesis filter header will need to be
// inserted.
func NewFilterHeaderStore(filePath string, db walletdb.DB,
	filterType HeaderType, netParams *chaincfg.Params,
	headerStateAssertion *FilterHeader) (FilterHeaderStore, error) {

	fStore, err := newHeaderStore(db, filePath, filterType)
	if err != nil {
		return nil, err
	}

	// With the header store created, we'll fetch the fiie size to see if
	// we need to initialize it with the first header or not.
	fileInfo, err := fStore.file.Stat()
	if err != nil {
		return nil, err
	}

	fhs := &filterHeaderStore{
		fStore,
	}

	// TODO(roasbeef): also reconsile with block header state due to way
	// roll back works atm

	// If the size of the file is zero, then this means that we haven't yet
	// written the initial genesis header to disk, so we'll do so now.
	if fileInfo.Size() == 0 {
		var genesisFilterHash chainhash.Hash
		switch filterType {
		case RegularFilter:
			basicFilter, err := builder.BuildBasicFilter(
				netParams.GenesisBlock, nil,
			)
			if err != nil {
				return nil, err
			}

			genesisFilterHash, err = builder.MakeHeaderForFilter(
				basicFilter,
				netParams.GenesisBlock.Header.PrevBlock,
			)
			if err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("unknown filter type: %v", filterType)
		}

		genesisHeader := FilterHeader{
			HeaderHash: *netParams.GenesisHash,
			FilterHash: genesisFilterHash,
			Height:     0,
		}
		if err := fhs.WriteHeaders(genesisHeader); err != nil {
			return nil, err
		}

		return fhs, nil
	}

	// If we have a state assertion then we'll check it now to see if we
	// need to modify our filter header files before we proceed.
	if headerStateAssertion != nil {
		reset, err := fhs.maybeResetHeaderState(
			headerStateAssertion,
		)
		if err != nil {
			return nil, err
		}

		// If the filter header store was reset, we'll re-initialize it
		// to recreate our on-disk state.
		if reset {
			return NewFilterHeaderStore(
				filePath, db, filterType, netParams, nil,
			)
		}
	}

	// As a final initialization step, we'll ensure that the header tip
	// within the flat files, is in sync with out database index.
	tipHash, tipHeight, err := fhs.chainTip()
	if err != nil {
		return nil, err
	}

	// First, we'll compute the size of the current file so we can
	// calculate the latest header written to disk.
	fileHeight := uint32(fileInfo.Size()/32) - 1

	// Using the file's current height, fetch the latest on-disk header.
	latestFileHeader, err := fhs.readHeader(fileHeight)
	if err != nil {
		return nil, err
	}

	// If the index's tip hash, and the file on-disk match, then we're
	// doing here.
	if tipHash.IsEqual(latestFileHeader) {
		return fhs, nil
	}

	// Otherwise, we'll need to truncate the file until it matches the
	// current index tip.
	if err := fhs.truncateHeaders(
		fileHeight-tipHeight, RegularFilter,
	); err != nil {
		return nil, err
	}

	return fhs, nil
}

// maybeResetHeaderState will reset the header state if the header assertion
// fails, but only if the target height is found. The boolean returned indicates
// that header state was reset.
func (f *filterHeaderStore) maybeResetHeaderState(
	headerStateAssertion *FilterHeader) (bool, error) {

	// First, we'll attempt to locate the header at this height. If no such
	// header is found, then we'll exit early.
	assertedHeader, err := f.FetchHeaderByHeight(
		headerStateAssertion.Height,
	)
	if _, ok := err.(*ErrHeaderNotFound); ok {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// If our on disk state and the provided header assertion don't match,
	// then we'll purge this state so we can sync it anew once we fully
	// start up.
	if *assertedHeader != headerStateAssertion.FilterHash {
		// Close the file before removing it. This is required by some
		// OS, e.g., Windows.
		if err := f.file.Close(); err != nil {
			return true, err
		}
		if err := os.Remove(f.file.Name()); err != nil {
			return true, err
		}
		return true, nil
	}

	return false, nil
}

// FetchHeader returns the filter header that corresponds to the passed block
// height.
func (f *filterHeaderStore) FetchHeader(hash *chainhash.Hash) (*chainhash.Hash, error) {
	// Lock store for read.
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	height, err := f.heightFromHash(hash)
	if err != nil {
		return nil, err
	}

	return f.readHeader(height)
}

// FetchHeaderByHeight returns the filter header for a particular block height.
func (f *filterHeaderStore) FetchHeaderByHeight(height uint32) (*chainhash.Hash, error) {
	// Lock store for read.
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	return f.readHeader(height)
}

// FetchHeaderAncestors fetches the numHeaders filter headers that are the
// ancestors of the target stop block hash. A total of numHeaders+1 headers will be
// returned, as we'll walk back numHeaders distance to collect each header,
// then return the final header specified by the stop hash. We'll also return
// the starting height of the header range as well so callers can compute the
// height of each header without knowing the height of the stop hash.
func (f *filterHeaderStore) FetchHeaderAncestors(numHeaders uint32,
	stopHash *chainhash.Hash) ([]chainhash.Hash, uint32, error) {

	// First, we'll find the final header in the range, this will be the
	// ending height of our scan.
	endHeight, err := f.heightFromHash(stopHash)
	if err != nil {
		return nil, 0, err
	}
	startHeight := endHeight - numHeaders

	headers, err := f.readHeaderRange(startHeight, endHeight)
	if err != nil {
		return nil, 0, err
	}

	return headers, startHeight, nil
}

// FilterHeader represents a filter header (basic or extended). The filter
// header itself is coupled with the block height and hash of the filter's
// block.
type FilterHeader struct {
	// HeaderHash is the hash of the block header that this filter header
	// corresponds to.
	HeaderHash chainhash.Hash

	// FilterHash is the filter header itself.
	FilterHash chainhash.Hash

	// Height is the block height of the filter header in the main chain.
	Height uint32
}

// toIndexEntry converts the filter header into a index entry to be stored
// within the database.
func (f *FilterHeader) toIndexEntry() headerEntry {
	return headerEntry{
		hash:   f.HeaderHash,
		height: f.Height,
	}
}

// WriteHeaders writes a batch of filter headers to persistent storage. The
// headers themselves are appended to the flat file, and then the index updated
// to reflect the new entries.
func (f *filterHeaderStore) WriteHeaders(hdrs ...FilterHeader) error {
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

	// Attempt to add the headers to the database. If this fails, we'll need
	// to roll back any changes to the header file to maintain consistency.
	// The rollback process bases on the number of header serialized. If
	// both the initial operation and the rollback fail, we return
	// a detailed error explaining both failures to aid in debugging.
	if err := f.truncateIndices(&newTip, nil, false); err != nil {
		// Since the probability of failing to write to the database is
		// very low, it is mostly worth the cost of file sync operation
		// to make sure truncate headers does it correctly.
		if err := f.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync filter headers "+
				"file: %v", err)
		}

		headersToRollback := len(hdrs)
		truncateErr := f.truncateHeaders(
			uint32(headersToRollback), f.indexType,
		)
		if truncateErr != nil {
			return fmt.Errorf("failed to rollback filter headers "+
				"binary file to previous valid state: %v, "+
				"error writing to database: %v", truncateErr,
				err)
		}
		return fmt.Errorf("failed to add filter headers to db: %v", err)
	}

	return nil
}

// ChainTip returns the latest filter header and height known to the
// FilterHeaderStore.
func (f *filterHeaderStore) ChainTip() (*chainhash.Hash, uint32, error) {
	// Lock store for read.
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	_, tipHeight, err := f.chainTip()
	if err != nil {
		return nil, 0, fmt.Errorf("unable to fetch chain tip: %v", err)
	}

	latestHeader, err := f.readHeader(tipHeight)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to read filter header: %v", err)
	}

	return latestHeader, tipHeight, nil
}

// RollbackLastBlock rollsback both the index, and on-disk header file by a
// _single_ filter header. This method is meant to be used in the case of
// re-org which disconnects the latest filter header from the end of the main
// chain. The information about the latest header tip after truncation is
// returned.
func (f *filterHeaderStore) RollbackLastBlock(newTip *chainhash.Hash) (*BlockStamp, error) {
	// Lock store for write.
	f.mtx.Lock()
	defer f.mtx.Unlock()

	// First, we'll obtain the latest height that the index knows of.
	_, chainTipHeight, err := f.chainTip()
	if err != nil {
		return nil, err
	}

	// With this height obtained, we'll use it to read what will be the new
	// chain tip from disk.
	newHeightTip := chainTipHeight - 1
	newHeaderTip, err := f.readHeader(newHeightTip)
	if err != nil {
		return nil, err
	}

	// Now that we have the information we need to return from this
	// function, we can now truncate both the header file and the index.
	if err := f.truncateHeaders(1, f.indexType); err != nil {
		return nil, err
	}
	if err := f.truncateIndices(newTip, nil, false); err != nil {
		return nil, err
	}

	// TODO(roasbeef): return chain hash also?
	return &BlockStamp{
		Height: int32(newHeightTip),
		Hash:   *newHeaderTip,
	}, nil
}
