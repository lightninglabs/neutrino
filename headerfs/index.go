package headerfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcwallet/walletdb"
)

var (
	// indexBucket is the main top-level bucket for the header index.
	// Nothing is stored in this bucket other than the sub-buckets which
	// contains the indexes for the various header types.
	indexBucket = []byte("header-index")

	// bitcoinTip is the key which tracks the "tip" of the block header
	// chain. The value of this key will be the current block hash of the
	// best known chain that we're synced to.
	bitcoinTip = []byte("bitcoin")

	// regFilterTip is the key which tracks the "tip" of the regular
	// compact filter header chain. The value of this key will be the
	// current block hash of the best known chain that the headers for
	// regular filter are synced to.
	regFilterTip = []byte("regular")
)

var (
	// ErrHeightNotFound is returned when a specified height isn't found in
	// a target index.
	ErrHeightNotFound = fmt.Errorf("target height not found in index")

	// ErrHashNotFound is returned when a specified block hash isn't found
	// in a target index.
	ErrHashNotFound = fmt.Errorf("target hash not found in index")
)

// HeaderType is an enum-like type which defines the various header types that
// are stored within the index.
type HeaderType uint8

const (
	// Block is the header type that represents regular Bitcoin block
	// headers.
	Block HeaderType = iota

	// RegularFilter is a header type that represents the basic filter
	// header type for the filter header chain.
	RegularFilter
)

const (
	// BlockHeaderSize is the size in bytes of the Block header type.
	BlockHeaderSize = 80

	// RegularFilterHeaderSize is the size in bytes of the RegularFilter
	// header type.
	RegularFilterHeaderSize = 32

	// numSubBucketBytes is the number of bytes of a hash that's used as a
	// sub bucket to store the index keys (hash->height) in. Storing a large
	// number of keys in the same bucket has a large impact on memory usage
	// in bbolt if small-ish batch sizes are used (the b+ tree needs to be
	// copied with every resize operation). Using sub buckets is a
	// compromise between memory usage and access time. 2 bytes (=max 65535
	// sub buckets) seems to be the sweet spot (-50% memory usage,
	// +30% access time). We take the bytes from the beginning of the byte-
	// serialized hash since all Bitcoin hashes are reverse-serialized when
	// displayed as strings. That means the leading zeroes of a block hash
	// are actually at the end of the byte slice.
	numSubBucketBytes = 2
)

// headerIndex is an index stored within the database that allows for random
// access into the on-disk header file. This, in conjunction with a flat file
// of headers consists of header database. The keys have been specifically
// crafted in order to ensure maximum write performance during IBD, and also to
// provide the necessary indexing properties required.
type headerIndex struct {
	db walletdb.DB

	indexType HeaderType
}

// newHeaderIndex creates a new headerIndex given an already open database, and
// a particular header type.
func newHeaderIndex(db walletdb.DB, indexType HeaderType) (*headerIndex, error) {
	// As an initially step, we'll attempt to create all the buckets
	// necessary for functioning of the index. If these buckets has already
	// been created, then we can exit early.
	err := walletdb.Update(db, func(tx walletdb.ReadWriteTx) error {
		_, err := tx.CreateTopLevelBucket(indexBucket)
		return err
	})
	if err != nil && err != walletdb.ErrBucketExists {
		return nil, err
	}

	return &headerIndex{
		db:        db,
		indexType: indexType,
	}, nil
}

// headerEntry is an internal type that's used to quickly map a (height, hash)
// pair into the proper key that'll be stored within the database.
type headerEntry struct {
	hash   chainhash.Hash
	height uint32
}

// headerBatch is a batch of header entries to be written to disk.
//
// NOTE: The entries within a batch SHOULD be properly sorted by hash in
// order to ensure the batch is written in a sequential write.
type headerBatch []headerEntry

// Len returns the number of routes in the collection.
//
// NOTE: This is part of the sort.Interface implementation.
func (h headerBatch) Len() int {
	return len(h)
}

// Less reports where the entry with index i should sort before the entry with
// index j. As we want to ensure the items are written in sequential order,
// items with the "first" hash.
//
// NOTE: This is part of the sort.Interface implementation.
func (h headerBatch) Less(i, j int) bool {
	return bytes.Compare(h[i].hash[:], h[j].hash[:]) < 0
}

// Swap swaps the elements with indexes i and j.
//
// NOTE: This is part of the sort.Interface implementation.
func (h headerBatch) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// addHeaders writes a batch of header entries in a single atomic batch.
func (h *headerIndex) addHeaders(batch headerBatch) error {
	// If we're writing a 0-length batch, make no changes and return.
	if len(batch) == 0 {
		return nil
	}

	// In order to ensure optimal write performance, we'll ensure that the
	// items are sorted by their hash before insertion into the database.
	sort.Sort(batch)

	return walletdb.Update(h.db, func(tx walletdb.ReadWriteTx) error {
		rootBucket := tx.ReadWriteBucket(indexBucket)

		var tipKey []byte

		// Based on the specified index type of this instance of the
		// index, we'll grab the key that tracks the tip of the chain
		// so we can update the index once all the header entries have
		// been updated.
		// TODO(roasbeef): only need block tip?
		switch h.indexType {
		case Block:
			tipKey = bitcoinTip
		case RegularFilter:
			tipKey = regFilterTip
		default:
			return fmt.Errorf("unknown index type: %v", h.indexType)
		}

		var (
			chainTipHash   chainhash.Hash
			chainTipHeight uint32
		)

		for _, header := range batch {
			if err := putHeaderEntry(rootBucket, header); err != nil {
				return err
			}

			// TODO(roasbeef): need to remedy if side-chain
			// tracking added
			if header.height >= chainTipHeight {
				chainTipHash = header.hash
				chainTipHeight = header.height
			}
		}

		return rootBucket.Put(tipKey, chainTipHash[:])
	})
}

// heightFromHash returns the height of the entry that matches the specified
// height. With this height, the caller is then able to seek to the appropriate
// spot in the flat files in order to extract the true header.
func (h *headerIndex) heightFromHash(hash *chainhash.Hash) (uint32, error) {
	var height uint32
	err := walletdb.View(h.db, func(tx walletdb.ReadTx) error {
		var err error
		height, err = h.heightFromHashWithTx(tx, hash)
		return err
	})
	if err != nil {
		return 0, err
	}

	return height, nil
}

// heightFromHashWithTx returns the height of the entry that matches the
// specified hash by using the given DB transaction. With this height, the
// caller is then able to seek to the appropriate spot in the flat files in
// order to extract the true header.
func (h *headerIndex) heightFromHashWithTx(tx walletdb.ReadTx,
	hash *chainhash.Hash) (uint32, error) {

	rootBucket := tx.ReadBucket(indexBucket)

	return getHeaderEntry(rootBucket, hash[:])
}

// chainTip returns the best hash and height that the index knows of.
func (h *headerIndex) chainTip() (*chainhash.Hash, uint32, error) {
	var (
		tipHeight uint32
		tipHash   *chainhash.Hash
	)

	err := walletdb.View(h.db, func(tx walletdb.ReadTx) error {
		var err error
		tipHash, tipHeight, err = h.chainTipWithTx(tx)
		return err
	})
	if err != nil {
		return nil, 0, err
	}

	return tipHash, tipHeight, nil
}

// chainTipWithTx returns the best hash and height that the index knows of by
// using the given DB transaction.
func (h *headerIndex) chainTipWithTx(tx walletdb.ReadTx) (*chainhash.Hash,
	uint32, error) {

	rootBucket := tx.ReadBucket(indexBucket)

	var tipKey []byte

	// Based on the specified index type of this instance of the index,
	// we'll grab the particular key that tracks the chain tip.
	switch h.indexType {
	case Block:
		tipKey = bitcoinTip
	case RegularFilter:
		tipKey = regFilterTip
	default:
		return nil, 0, fmt.Errorf("unknown chain tip index type: %v",
			h.indexType)
	}

	// Now that we have the particular tip key for this header type, we'll
	// fetch the hash for this tip, then using that we'll fetch the height
	// that corresponds to that hash.
	tipHashBytes := rootBucket.Get(tipKey)
	tipHeight, err := getHeaderEntry(rootBucket, tipHashBytes)
	if err != nil {
		return nil, 0, ErrHeightNotFound
	}

	// With the height fetched, we can now populate our return
	// parameters.
	tipHash, err := chainhash.NewHash(tipHashBytes)
	if err != nil {
		return nil, 0, err
	}

	return tipHash, tipHeight, nil
}

// truncateIndex truncates the index for a particular header type by a single
// header entry. The passed newTip pointer should point to the hash of the new
// chain tip. Optionally, if the entry is to be deleted as well, then the
// remove flag should be set to true.
func (h *headerIndex) truncateIndex(newTip *chainhash.Hash, remove bool) error {
	return walletdb.Update(h.db, func(tx walletdb.ReadWriteTx) error {
		rootBucket := tx.ReadWriteBucket(indexBucket)

		var tipKey []byte

		// Based on the specified index type of this instance of the
		// index, we'll grab the key that tracks the tip of the chain
		// we need to update.
		switch h.indexType {
		case Block:
			tipKey = bitcoinTip
		case RegularFilter:
			tipKey = regFilterTip
		default:
			return fmt.Errorf("unknown index type: %v", h.indexType)
		}

		// If the remove flag is set, then we'll also delete this entry
		// from the database as the primary index (block headers) is
		// being rolled back.
		if remove {
			prevTipHash := rootBucket.Get(tipKey)
			err := delHeaderEntry(rootBucket, prevTipHash)
			if err != nil {
				return err
			}
		}

		// With the now stale entry deleted, we'll update the chain tip
		// to point to the new hash.
		return rootBucket.Put(tipKey, newTip[:])
	})
}

// putHeaderEntry stores a headerEntry into the bbolt database. The entry is
// always placed below a sub bucket with the first few bytes of the hash as its
// name to improve write performance.
func putHeaderEntry(rootBucket walletdb.ReadWriteBucket,
	header headerEntry) error {

	// Place key in a sub bucket to improve bbolt memory behavior at the
	// expense of a slight increase in access latency.
	subBucket, err := rootBucket.CreateBucketIfNotExists(
		header.hash[0:numSubBucketBytes],
	)
	if err != nil {
		return err
	}

	var heightBytes [4]byte
	binary.BigEndian.PutUint32(heightBytes[:], header.height)
	return subBucket.Put(header.hash[:], heightBytes[:])
}

// getHeaderEntry tries to look up the height of a header by its hash. It first
// looks in the "new" prefixed sub bucket that we use to store new headers. If
// the header isn't found there, we fall back to the old place where we used to
// store the keys (the root bucket). If no key for the given hash is found in
// either places, ErrHashNotFound is returned.
func getHeaderEntry(rootBucket walletdb.ReadBucket, hashBytes []byte) (uint32,
	error) {

	subBucket := rootBucket.NestedReadBucket(hashBytes[0:numSubBucketBytes])
	if subBucket == nil {
		// Fall back to the old method which will return the
		// ErrHashNotFound error if there's no key in the root bucket.
		return getHeaderEntryFallback(rootBucket, hashBytes)
	}

	heightBytes := subBucket.Get(hashBytes)
	if heightBytes == nil {
		// Fall back to the old method which will return the
		// ErrHashNotFound error if there's no key in the root bucket.
		return getHeaderEntryFallback(rootBucket, hashBytes)
	}

	return binary.BigEndian.Uint32(heightBytes), nil
}

// getHeaderEntryFallback tries to look up the height of a header by its hash by
// looking at the root bucket directly, which is the old place we used to store
// header entries. If no key for the given hash is found, ErrHashNotFound is
// returned.
func getHeaderEntryFallback(rootBucket walletdb.ReadBucket,
	hashBytes []byte) (uint32, error) {

	heightBytes := rootBucket.Get(hashBytes)
	if heightBytes == nil {
		// If the hash wasn't found, then we don't know of this hash
		// within the index.
		return 0, ErrHashNotFound
	}

	return binary.BigEndian.Uint32(heightBytes), nil
}

// delHeaderEntry tries to remove a header entry from the bbolt database. It
// first looks if a key for it exists in the old place, the root bucket. If it
// does, it's deleted from there. If not, it's attempted to be deleted from the
// sub bucket instead.
func delHeaderEntry(rootBucket walletdb.ReadWriteBucket, hashBytes []byte) error {
	// In case this header was stored in the old place (the root bucket
	// directly), let's remove it from there.
	if len(rootBucket.Get(hashBytes)) == 4 {
		return rootBucket.Delete(hashBytes)
	}

	// The hash wasn't stored in the root bucket. So we try the sub bucket
	// now. If that doesn't exist, something is wrong and we want to return
	// an error here.
	subBucket := rootBucket.NestedReadWriteBucket(
		hashBytes[0:numSubBucketBytes],
	)
	if subBucket == nil {
		return ErrHashNotFound
	}

	return subBucket.Delete(hashBytes)
}
