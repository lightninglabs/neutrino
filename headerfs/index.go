package headerfs

import (
	"bytes"
	"encoding/binary"
	"errors"
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

const (
	// BlockHeaderSize is the size in bytes of the Block header type.
	BlockHeaderSize = 80

	// RegularFilterHeaderSize is the size in bytes of the RegularFilter
	// header type.
	RegularFilterHeaderSize = 32

	UnknownHeaderSize = 0

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

// HeaderType is an enum-like type which defines the various header types that
// are stored within the index.
type HeaderType uint8

const (
	// Block is the header type that represents regular Bitcoin block
	// headers.
	Block HeaderType = 0

	// RegularFilter is a header type that represents the basic filter
	// header type for the filter header chain.
	RegularFilter HeaderType = 1

	// UnknownHeader represents an unrecognized header type.
	UnknownHeader HeaderType = 255
)

// String returns the string representation of the HeaderType.
func (h HeaderType) String() string {
	switch h {
	case Block:
		return "BlockHeader"
	case RegularFilter:
		return "RegularFilterHeader"
	default:
		return fmt.Sprintf("UnknownHeaderType(%d)", h)
	}
}

// Size returns the size in bytes for a given header type.
func (h HeaderType) Size() (int, error) {
	switch h {
	case Block:
		return BlockHeaderSize, nil
	case RegularFilter:
		return RegularFilterHeaderSize, nil
	default:
		return 0, fmt.Errorf("unknown header type: %d", h)
	}
}

// TipKey returns the current tip key for the given header type.
// Returns an error if the header type is unknown.
func (h HeaderType) TipKey() ([]byte, error) {
	switch h {
	case Block:
		return bitcoinTip, nil
	case RegularFilter:
		return regFilterTip, nil
	default:
		return nil, fmt.Errorf("unknown header type: %d", h)
	}
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
		tipKey, err := h.indexType.TipKey()
		if err != nil {
			return err
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

	// Based on the specified index type of this instance of the index,
	// we'll grab the particular key that tracks the chain tip.
	tipKey, err := h.indexType.TipKey()
	if err != nil {
		return nil, 0, err
	}

	// Now that we have the particular tip key for this header type, we'll
	// fetch the hash for this tip, then using that we'll fetch the height
	// that corresponds to that hash.
	tipHashBytes := rootBucket.Get(tipKey)
	if tipHashBytes == nil {
		return nil, 0, fmt.Errorf(
			"the key %s does not exist in bucket %s",
			tipKey, indexBucket,
		)
	}
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

// truncateIndices truncates the index for a particular header type by removing
// a set of header entries. The passed newTip pointer should point to the hash
// of the new chain tip. The blockHeadersToTruncate contains the hashes of all
// block headers that should be removed from the index, which are the block
// headers after the new tip. Optionally, if the entries are to be deleted as
// well, then the remove flag should be set to true.
func (h *headerIndex) truncateIndices(newTip *chainhash.Hash,
	blockHeadersToTruncate []*chainhash.Hash, remove bool) error {

	if remove && len(blockHeadersToTruncate) == 0 {
		return errors.New("remove flag set but headers to truncate " +
			"beyond new tip not provided")
	}

	if !remove && len(blockHeadersToTruncate) != 0 {
		return errors.New("headers to truncate beyond new tip " +
			"provided but remove flag not set")
	}

	return walletdb.Update(h.db, func(tx walletdb.ReadWriteTx) error {
		rootBucket := tx.ReadWriteBucket(indexBucket)

		var tipKey []byte

		// Based on the specified index type of this instance of the
		// index, we'll grab the key that tracks the tip of the chain
		// we need to update.
		tipKey, err := h.indexType.TipKey()
		if err != nil {
			return err
		}

		// If the remove flag is set, then we'll also delete those
		// entries from the database as the primary index
		// (block headers) is being rolled back.
		if remove {
			if err := deleteHeaderEntries(
				rootBucket, blockHeadersToTruncate,
			); err != nil {
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

// deleteHeaderEntries tries to remove multiple header entries from the bbolt
// database. For each header hash, it first looks if a key exists in the old
// place, the root bucket. If it does, it's deleted from there. If not, it's
// attempted to be deleted from the appropriate sub-bucket instead.
func deleteHeaderEntries(rootBucket walletdb.ReadWriteBucket,
	headerHashes []*chainhash.Hash) error {

	if len(headerHashes) == 0 {
		return nil
	}

	// Group hashes by their sub-bucket for more efficient deletion.
	bySubBucket := make(map[string][]*chainhash.Hash)
	rootBucketHashes := make([]*chainhash.Hash, 0, len(headerHashes))

	// Check which hashes are in the root bucket and group the rest by their
	// sub-bucket prefix.
	for _, hash := range headerHashes {
		// Convert hash to bytes for DB operations.
		hashBytes := hash.CloneBytes()

		// In case this header was stored in the old place
		// (the root bucket directly), let's check and mark it for
		// removal from there.
		if len(rootBucket.Get(hashBytes)) == 4 {
			rootBucketHashes = append(rootBucketHashes, hash)
		} else {
			// The hash wasn't stored in the root bucket. So we need
			// to use the sub-bucket. We extract the prefix to
			// determine which sub-bucket to use.
			prefix := string(hashBytes[0:numSubBucketBytes])
			bySubBucket[prefix] = append(bySubBucket[prefix], hash)
		}
	}

	// Delete entries from root bucket.
	for _, hash := range rootBucketHashes {
		hashBytes := hash.CloneBytes()
		if err := rootBucket.Delete(hashBytes); err != nil {
			return err
		}
	}

	// Delete enties from sub-buckets.
	for prefix, hashes := range bySubBucket {
		// Try to get the sub-bucket for this prefix. If it doesn't
		// exist, something is wrong and we want to return an error.
		subBucket := rootBucket.NestedReadWriteBucket([]byte(prefix))
		if subBucket == nil {
			return fmt.Errorf("%w: sub-bucket for prefix %x not "+
				"found", ErrHashNotFound, prefix)
		}

		for _, hash := range hashes {
			hashBytes := hash.CloneBytes()
			if err := subBucket.Delete(hashBytes); err != nil {
				return err
			}
		}
	}

	return nil
}
