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

	// bitcoinHeader is the bucket in which the index for regular bitcoin
	// headers are stored.
	bitcoinBucket = []byte("bitcoin")

	// regFitler is the bucket where the index for regular (basic) filter
	// headers are stored.
	regFitlerBucket = []byte("regular")

	// extFilter is the bucket where the index for extended filter headers
	// are stored.
	extFilterBucket = []byte("ext")
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

	// RegularFiler is a header type that represents the basic filter
	// header type for the filter header chain.
	RegularFilter

	// ExtendedFilter is a header type that represents the extended filter
	// header type for the filter header chain.
	ExtendedFilter
)

// headerLocator is 36-byte identifier used within the index solely as the key,
// meaning no actual values are stored within the index. The format of a
// serialized headerLocator is: height || hash. The height is serialized in a
// big-endian format. With this details, we ensure that all writes during
// normal initial block sync are simply _sequential_ writes, speeding up write
// performance, as this entails a pure appends to a leaf of a B+Tree.
// Additionally, this format serves as the only key needed to create an index,
// as it's possible to map hash -> height, and height -> hash with this index
// with proper use of database cursors.
//
// TODO(roasbeef): instead do hash -> {blockIndex, regFilterIndex, extFilterIndex}
type headerLocator [4 + 32]byte

// height returns the concrete height specified by the headerLocator.
func (h *headerLocator) height() uint32 {
	return binary.BigEndian.Uint32(h[:4])
}

// hash returns the deserialized height specified by the headerLocator.
func (h *headerLocator) hash() (*chainhash.Hash, error) {
	return chainhash.NewHash(h[4:])
}

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
		indexBucket, err := tx.CreateTopLevelBucket(indexBucket)
		if err != nil {
			return err
		}

		_, err = indexBucket.CreateBucketIfNotExists(bitcoinBucket)
		if err != nil {
			return err
		}

		_, err = indexBucket.CreateBucketIfNotExists(regFitlerBucket)
		if err != nil {
			return err
		}

		_, err = indexBucket.CreateBucketIfNotExists(regFitlerBucket)
		if err != nil {
			return err
		}

		return nil
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

// toLocator returns the serialized headerLocator which maps to this particular
// header entry.
func (h *headerEntry) toLocator() headerLocator {
	var loc headerLocator

	// The on-disk format of a header locator: is height || hash, this
	// ensures that all writes using headerLocator sequentially ordered.
	binary.BigEndian.PutUint32(loc[:], h.height)
	copy(loc[4:], h.hash[:])

	return loc
}

// headerBatch is a batch of header entries to be written to disk.
//
// NOTE: The entries within a batch SHOULD be properly sorted by height in
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
// items with the lowest height should come first in the batch.
//
// NOTE: This is part of the sort.Interface implementation.
func (h headerBatch) Less(i, j int) bool {
	return h[i].height < h[j].height
}

// Swap swaps the elements with indexes i and j.
//
// NOTE: This is part of the sort.Interface implementation.
func (h headerBatch) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// addHeaders writes a batch of header entries in a single atomic batch
func (h *headerIndex) addHeaders(batch headerBatch) error {
	// In order to ensure optimal write performance, we'll ensure that the
	// items are sorted before insertion into the database.
	sort.Sort(batch)

	return walletdb.Update(h.db, func(tx walletdb.ReadWriteTx) error {
		rootBucket := tx.ReadWriteBucket(indexBucket)

		var bucket walletdb.ReadWriteBucket

		// Based on the specified index type of this instance of the
		// index, we'll grab the appropriate bucket.
		switch h.indexType {
		case Block:
			bucket = rootBucket.NestedReadWriteBucket(bitcoinBucket)
		case RegularFilter:
			bucket = rootBucket.NestedReadWriteBucket(regFitlerBucket)
		case ExtendedFilter:
			bucket = rootBucket.NestedReadWriteBucket(extFilterBucket)
		}

		// Each each item within the batch, we'll convert the entry
		// into a locator, and insert it into the target bucket.
		for _, header := range batch {
			headerloc := header.toLocator()
			err := bucket.Put(headerloc[:], nil)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// truncateIndex deletes the _last_ index entry within the index. This is to be
// used in the case of a reorganization that ends up disconnected the tip of
// the chain from the best chain.
func (h *headerIndex) truncateIndex() error {
	return walletdb.Update(h.db, func(tx walletdb.ReadWriteTx) error {
		rootBucket := tx.ReadWriteBucket(indexBucket)

		var bucket walletdb.ReadWriteBucket

		// Based on the specified index type of this instance of the
		// index, we'll grab the appropriate bucket.
		switch h.indexType {
		case Block:
			bucket = rootBucket.NestedReadWriteBucket(bitcoinBucket)
		case RegularFilter:
			bucket = rootBucket.NestedReadWriteBucket(regFitlerBucket)
		case ExtendedFilter:
			bucket = rootBucket.NestedReadWriteBucket(extFilterBucket)
		}

		// Using the bucket, we'll obtain a cursor so we can seek to
		// the end of the key space.
		cursor := bucket.ReadWriteCursor()

		// With the cursor obtained, we'll seek to the _last_ entry,
		// then delete it, thereby truncating the index chain by a
		// single block.
		_, _ = cursor.Last()
		return cursor.Delete()
	})
}

// hashFromHeight returns the hash of the header at the specified height.
func (h *headerIndex) hashFromHeight(height uint32) (*chainhash.Hash, error) {
	var hash *chainhash.Hash
	err := walletdb.View(h.db, func(tx walletdb.ReadTx) error {
		rootBucket := tx.ReadBucket(indexBucket)

		var bucket walletdb.ReadBucket

		// Based on the specified index type of this instance of the
		// index, we'll grab the appropriate bucket.
		switch h.indexType {
		case Block:
			bucket = rootBucket.NestedReadBucket(bitcoinBucket)
		case RegularFilter:
			bucket = rootBucket.NestedReadBucket(regFitlerBucket)
		case ExtendedFilter:
			bucket = rootBucket.NestedReadBucket(extFilterBucket)
		}

		// With the proper bucket grabbed, we'll obtain a cursor so we
		// can jump around in the key space.
		cursor := bucket.ReadCursor()

		// Next, we'll construct the key prefix for our desired entry:
		// height || zeroes
		var heightBytes [4]byte
		binary.BigEndian.PutUint32(heightBytes[:], height)

		// We'll then convert that into the full header locator leaving
		// the latter bytes blank.
		var targetKey headerLocator
		copy(targetKey[:], heightBytes[:])

		// WIth the key construted, we can now seek to the target key.
		indexKey, _ := cursor.Seek(targetKey[:])

		// As a sanity check, if the entry is within the database, it
		// should share the same prefix.
		if !bytes.HasPrefix(indexKey[:], heightBytes[:]) {
			return ErrHeightNotFound
		}

		// If the check above passes, then we have the proper hash that
		// matches this height, so we'll go ahead an extract the hash
		// object from it.
		var loc headerLocator
		copy(loc[:], indexKey[:])
		headerHash, err := loc.hash()
		if err != nil {
			return err
		}

		hash = headerHash

		return nil
	})
	if err != nil {
		return nil, err
	}

	return hash, nil
}

// heightFromHash returns the height of the entry that matches the specified
// height. With this height, the caller is then able to seek to the appropriate
// spot in the flat files in order to extract the true header.
func (h *headerIndex) heightFromHash(hash *chainhash.Hash) (uint32, error) {
	var height uint32
	err := walletdb.View(h.db, func(tx walletdb.ReadTx) error {
		rootBucket := tx.ReadBucket(indexBucket)

		var bucket walletdb.ReadBucket

		// Based on the specified index type of this instance of the
		// index, we'll grab the appropriate bucket.
		//
		// TODO(roasbeef): factor out
		switch h.indexType {
		case Block:
			bucket = rootBucket.NestedReadBucket(bitcoinBucket)
		case RegularFilter:
			bucket = rootBucket.NestedReadBucket(regFitlerBucket)
		case ExtendedFilter:
			bucket = rootBucket.NestedReadBucket(extFilterBucket)
		}

		// TODO(roasbeef): replace with a manual binary search
		// implementation
		//  * atm is O(N)

		// As boltdb is unable to do suffix searches with the exposed
		// API, we'll do a backwards scan for the target header hash.
		cursor := bucket.ReadCursor()
		for indexKey, _ := cursor.Last(); indexKey != nil; indexKey, _ = cursor.Prev() {
			// With the cursor seeked, if this key has a matching
			// suffix, then we can exit our search here as we've
			// found the key.
			if !bytes.HasSuffix(indexKey[:], hash[:]) {
				continue
			}

			// If the above check passes, then we've found the
			// proper index entry and can extract the height from
			// it.
			var loc headerLocator
			copy(loc[:], indexKey[:])
			height = loc.height()

			return nil
		}

		// If the hash wasn't found, then we don't know of this hash
		// within the index.
		return ErrHashNotFound
	})
	if err != nil {
		return 0, err
	}

	return height, nil
}

// chainTip returns the best hash and height that the index knows of.
func (h *headerIndex) chainTip() (*chainhash.Hash, uint32, error) {
	var (
		tipHeight uint32
		tipHash   *chainhash.Hash
	)

	err := walletdb.View(h.db, func(tx walletdb.ReadTx) error {
		rootBucket := tx.ReadBucket(indexBucket)

		var bucket walletdb.ReadBucket

		// Based on the specified index type of this instance of the
		// index, we'll grab the appropriate bucket.
		switch h.indexType {
		case Block:
			bucket = rootBucket.NestedReadBucket(bitcoinBucket)
		case RegularFilter:
			bucket = rootBucket.NestedReadBucket(regFitlerBucket)
		case ExtendedFilter:
			bucket = rootBucket.NestedReadBucket(extFilterBucket)
		}

		// Obtain the cursor, and seek it to the _last_ entry. This
		// will be the entry with the largest height
		cursor := bucket.ReadCursor()
		indexKey, _ := cursor.Last()

		// Once we have the full key, then we can tract both the height
		// and the hash to return to the caller.
		var loc headerLocator
		copy(loc[:], indexKey[:])

		tipHeight = loc.height()

		hash, err := loc.hash()
		if err != nil {
			return err
		}
		tipHash = hash

		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return tipHash, tipHeight, nil
}
