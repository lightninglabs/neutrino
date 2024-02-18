package headerfs

import (
	"fmt"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/walletdb"
)

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

	// RollbackLastBlock rolls back the BlockHeaderStore by a _single_
	// header. This method is meant to be used in the case of re-org which
	// disconnects the latest block header from the end of the main chain.
	// The information about the new header tip after truncation is
	// returned.
	RollbackLastBlock() (*BlockStamp, error)

	// Close and delete the BlockHeaderStore.
	Remove() error
}

// BlockHeader is a Bitcoin block header that also has its height included.
type BlockHeader struct {
	*wire.BlockHeader

	// Height is the height of this block header within the current main
	// chain.
	Height uint32
}

// toIndexEntry converts the BlockHeader into a matching headerEntry. This
// method is used when a header is to be written to persistent storage.
func (b *BlockHeader) toIndexEntry() headerEntry {
	return headerEntry{
		hash:   b.BlockHash(),
		height: b.Height,
	}
}

// ErrHeaderNotFound is returned when a target header on persistent storage (flat file) can't
// be found.
type ErrHeaderNotFound struct {
	error
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

	// With the height known, we can now read the header from persistent storage.
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

// RollbackLastBlock rollsback both the index, and on-persistent storage header file by a
// _single_ header. This method is meant to be used in the case of re-org which
// disconnects the latest block header from the end of the main chain. The
// information about the new header tip after truncation is returned.
//
// NOTE: Part of the BlockHeaderStore interface.
func (h *blockHeaderStore) RollbackLastBlock() (*BlockStamp, error) {
	// Lock store for write.
	h.mtx.Lock()
	defer h.mtx.Unlock()

	// First, we'll obtain the latest height that the index knows of.
	_, chainTipHeight, err := h.chainTip()
	if err != nil {
		return nil, err
	}

	// With this height obtained, we'll use it to read the previous header
	// from persistent storage, so we can populate our return value which requires the
	// prev header hash.
	prevHeader, err := h.readHeader(chainTipHeight - 1)
	if err != nil {
		return nil, err
	}

	prevHeaderHash := prevHeader.BlockHash()

	// Now that we have the information we need to return from this
	// function, we can now truncate the header file, and then use the hash
	// of the prevHeader to set the proper index chain tip.
	if err := h.singleTruncate(); err != nil {
		return nil, err
	}
	if err := h.truncateIndex(&prevHeaderHash, true); err != nil {
		return nil, err
	}

	return &BlockStamp{
		Height:    int32(chainTipHeight) - 1,
		Hash:      prevHeaderHash,
		Timestamp: prevHeader.Timestamp,
	}, nil
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

// CheckConnectivity cycles through all of the block headers on persistent storage, from last
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
			// on-persistent storage state and the index matches up properly.
			indexHeight, err := h.heightFromHashWithTx(
				tx, &newHeaderHash,
			)
			if err != nil {
				return fmt.Errorf("index and on-persistent storage file "+
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
		return nil, 0, err
	}

	latestHeader, err := h.readHeader(tipHeight)
	if err != nil {
		return nil, 0, err
	}

	return &latestHeader, tipHeight, nil
}

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
	height, genesis, err := hStore.height()
	if err != nil {
		return nil, err
	}

	bhs := &blockHeaderStore{
		headerStore: hStore,
	}

	// If the size of the file is zero, then this means that we haven't yet
	// written the initial genesis header to persistent storage, so we'll do so now.
	if genesis {
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

	// Move back to the last header written.
	height--

	// Using the current height, fetch the latest on-persistent storage header.
	latestFileHeader, err := bhs.readHeader(height)
	if err != nil {
		return nil, err
	}

	// If the index's tip hash, and the file on-persistent storage match, then we're
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
	for height > tipHeight {
		if err := bhs.singleTruncate(); err != nil {
			return nil, err
		}

		height--
	}

	return bhs, nil
}

// FilterHeaderStore is an implementation of a fully fledged database for any
// variant of filter headers.  The FilterHeaderStore combines a flat file to
// store the block headers with a database instance for managing the index into
// the set of flat files.
type FilterHeaderStore struct {
	*headerStore
}

// FetchHeader returns the filter header that corresponds to the passed block
// height.
func (f *FilterHeaderStore) FetchHeader(hash *chainhash.Hash) (*chainhash.Hash, error) {
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
func (f *FilterHeaderStore) FetchHeaderByHeight(height uint32) (*chainhash.Hash, error) {
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
func (f *FilterHeaderStore) FetchHeaderAncestors(numHeaders uint32,
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

// maybeResetHeaderState will reset the header state if the header assertion
// fails, but only if the target height is found. The boolean returned indicates
// that header state was reset.
func (f *FilterHeaderStore) maybeResetHeaderState(
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

	// If our persistent state and the provided header assertion don't match,
	// then we'll purge this state so we can sync it anew once we fully
	// start up.
	if *assertedHeader != headerStateAssertion.FilterHash {
		err = f.Remove()
		return true, err
	}

	return false, nil
}

// ChainTip returns the latest filter header and height known to the
// FilterHeaderStore.
func (f *FilterHeaderStore) ChainTip() (*chainhash.Hash, uint32, error) {
	// Lock store for read.
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	_, tipHeight, err := f.chainTip()
	if err != nil {
		return nil, 0, fmt.Errorf("unable to fetch chain tip: %v", err)
	}

	latestHeader, err := f.readHeader(tipHeight)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to read header: %v", err)
	}

	return latestHeader, tipHeight, nil
}

// RollbackLastBlock rollsback both the index, and on-persistent storage header file by a
// _single_ filter header. This method is meant to be used in the case of
// re-org which disconnects the latest filter header from the end of the main
// chain. The information about the latest header tip after truncation is
// returned.
func (f *FilterHeaderStore) RollbackLastBlock(newTip *chainhash.Hash) (*BlockStamp, error) {
	// Lock store for write.
	f.mtx.Lock()
	defer f.mtx.Unlock()

	// First, we'll obtain the latest height that the index knows of.
	_, chainTipHeight, err := f.chainTip()
	if err != nil {
		return nil, err
	}

	// With this height obtained, we'll use it to read what will be the new
	// chain tip from persistent storage.
	newHeightTip := chainTipHeight - 1
	newHeaderTip, err := f.readHeader(newHeightTip)
	if err != nil {
		return nil, err
	}

	// Now that we have the information we need to return from this
	// function, we can now truncate both the header file and the index.
	if err := f.singleTruncate(); err != nil {
		return nil, err
	}
	if err := f.truncateIndex(newTip, false); err != nil {
		return nil, err
	}

	// TODO(roasbeef): return chain hash also?
	return &BlockStamp{
		Height: int32(newHeightTip),
		Hash:   *newHeaderTip,
	}, nil
}

// NewFilterHeaderStore returns a new instance of the FilterHeaderStore based
// on a target file path, filter type, and target net parameters. These
// parameters are required as if this is the initial start up of the
// FilterHeaderStore, then the initial genesis filter header will need to be
// inserted.
func NewFilterHeaderStore(filePath string, db walletdb.DB,
	filterType HeaderType, netParams *chaincfg.Params,
	headerStateAssertion *FilterHeader) (*FilterHeaderStore, error) {

	fStore, err := newHeaderStore(db, filePath, filterType)
	if err != nil {
		return nil, err
	}

	// With the header store created, we'll fetch the fiie size to see if
	// we need to initialize it with the first header or not.
	height, genesis, err := fStore.height()
	if err != nil {
		return nil, err
	}

	fhs := &FilterHeaderStore{
		fStore,
	}

	// TODO(roasbeef): also reconsile with block header state due to way
	// roll back works atm

	// If the size of the file is zero, then this means that we haven't yet
	// written the initial genesis header to disk, so we'll do so now.
	if genesis {
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

	// Using the file's current height, fetch the latest on-disk header.
	latestFileHeader, err := fhs.readHeader(height)
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
	for height > tipHeight {
		if err := fhs.singleTruncate(); err != nil {
			return nil, err
		}

		height--
	}

	// TODO(roasbeef): make above into func

	return fhs, nil
}
