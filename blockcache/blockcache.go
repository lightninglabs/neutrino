package blockcache

import (
	"bytes"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcd/wire"
	"os"
	"path/filepath"
	"strconv"
)

var (
	// ErrBlockNotFound is returned when the requested block is not persisted.
	ErrBlockNotFound = fmt.Errorf("unable to find block")

	// blockCacheBucket is the name of the database bucket we'll use to
	// persist the blocks.
	blockCacheBucket = []byte("block-cache")

	// defaultCapacity is the default maximum capacity of the
	// MostRecentBlockCache, in blocks.
	DefaultCapacity = 500
)

const (
	dbName           = "blockcache.db"
	dbFilePermission = 0600
)

// BlockCache is an interface which represents an object that is capable of
// storing and retrieving blocks according to their corresponding block hash.
//
type BlockCache interface {
	// PutBlock stores a block to persistent storage.
	PutBlock(block *wire.MsgBlock) error

	// FetchBlock attempts to fetch a block with the given hash from the
	// backing store. If the backing store does not contain the block then
	// an ErrBlockNotFound error is returned.
	FetchBlock(*chainhash.Hash) (*wire.MsgBlock, error)
}

// MostRecentBlockCache is an implementation of the BlockCache interface
// which attempts to cache a fixed number of the most recent blocks.
// If a newer block (by height) is inserted and the cache is already full,
// then the oldest block will be evicted to make room.
type MostRecentBlockCache struct {
	db *bolt.DB

	// A BlockHeaderStore allows translation between block heights and hashes.
	blockHeaders *headerfs.BlockHeaderStore

	// The maximum number of blocks to store in the cache.
	capacity int
}

// A compile-time check to ensure the MostRecentBlockCache adheres to the
// BlockCache interface.
var _ BlockCache = (*MostRecentBlockCache)(nil)

// fileExists returns true if the file exists, and false otherwise.
func fileExists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}

	return true
}

// New creates a new instance of a MostRecentBlockCache given a path to store
// the data.
func New(dbPath string, capacity int, blockHeaders *headerfs.BlockHeaderStore) (*MostRecentBlockCache, error) {
	if !fileExists(dbPath) {
		if err := os.MkdirAll(dbPath, 0700); err != nil {
			return nil, err
		}
	}

	path := filepath.Join(dbPath, dbName)
	bdb, err := bolt.Open(path, dbFilePermission, nil)
	if err != nil {
		return nil, err
	}

	// Create the bucket if necessary.
	err = bdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(blockCacheBucket)

		if err != nil && err != bolt.ErrBucketExists {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("unable to create cache db")
	}

	return &MostRecentBlockCache{
		db:           bdb,
		capacity:     capacity,
		blockHeaders: blockHeaders,
	}, nil
}

func (c *MostRecentBlockCache) PutBlock(block *wire.MsgBlock) error {
	// The key for this block is its height in the blockchain.
	// This allows us to evict the oldest entry easily.
	hash := block.BlockHash()
	height, err := c.blockHeaders.HeightFromHash(&hash)
	if err != nil {
		return err
	}

	key := []byte(strconv.FormatUint(uint64(height), 10))

	// Serialize the block.
	w := bytes.NewBuffer(make([]byte, 0, block.SerializeSize()))
	err = block.Serialize(w)
	if err != nil {
		return err
	}

	// Insert the block, evicting the oldest block if the capacity has been
	// reached.
	err = c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(blockCacheBucket)

		isFull := bucket.Stats().KeyN >= c.capacity

		if isFull {
			// Find the oldest block we have in the cache.
			oldestKey, _ := bucket.Cursor().First()
			isNewer := bytes.Compare(key, oldestKey) == 1

			// Only insert this block if it wouldn't evict a block that's newer
			// than itself.
			if isNewer {
				bucket.Delete(oldestKey)
				bucket.Put(key, w.Bytes())
			}
		} else {
			bucket.Put(key, w.Bytes())
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *MostRecentBlockCache) FetchBlock(hash *chainhash.Hash) (*wire.MsgBlock,
	error) {
	// Look up the height from the hash, then query by height and double
	// check we got the right block.
	height, err := c.blockHeaders.HeightFromHash(hash)
	if err != nil {
		return nil, err
	}

	key := []byte(strconv.FormatUint(uint64(height), 10))

	var block wire.MsgBlock
	err = c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(blockCacheBucket)

		// Fetch the block, if it exists.
		value := bucket.Get(key)
		if value == nil {
			return nil
		}

		err := block.Deserialize(bytes.NewReader(value))
		if err != nil {
			return err
		}

		return nil
	})

	// If we didn't find the block in the cache, or if we found a different
	// block at that height, then return not found.
	if block.BlockHash() != *hash {
		return nil, ErrBlockNotFound
	}

	return &block, nil
}
