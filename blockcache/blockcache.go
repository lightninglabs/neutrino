package blockcache

import (
	"bytes"
	"fmt"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcd/wire"
	"os"
	"sync/atomic"
)

var (
	// ErrBlockNotFound is returned when the requested block is not persisted.
	ErrBlockNotFound = fmt.Errorf("unable to find block")

	// blockCacheBucket is the name of the database bucket we'll use to
	// persist the blocks.
	blockCacheBucket = []byte("block-cache")

	// DefaultCapacity is the default maximum capacity of the
	// MostRecentBlockCache, in bytes.
	DefaultCapacity = 1000 * 1024 * 1024
)

const (
	dbName           = "blockcache.db"
	dbFilePermission = 0600
)

// BlockCache is an interface which represents an object that is capable of
// storing and retrieving blocks according to their corresponding block hash.
type BlockCache interface {
	// PutBlock stores a block to persistent storage.
	PutBlock(block *wire.MsgBlock) error

	// FetchBlock attempts to fetch a block with the given hash from the
	// backing store. If the backing store does not contain the block then
	// an ErrBlockNotFound error is returned.
	FetchBlock(*chainhash.Hash) (*wire.MsgBlock, error)

	// Close releases any resources currently in use by the cache.
	Close()
}

// MostRecentBlockCache is an implementation of the BlockCache interface
// which attempts to cache a fixed number of the most recent blocks.
// If a newer block (by height) is inserted and the cache is already full,
// then the oldest block will be evicted to make room.
type MostRecentBlockCache struct {
	// The number of cache hits and misses. Must be accessed atomically.
	hits   uint64
	misses uint64

	cache *Cache
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
func New(dbPath string, capacity int) (*MostRecentBlockCache, error) {
	// Create the backing boltdb database.
	store, err := NewStore(dbPath)
	if err != nil {
		return nil, err
	}

	cache, err := NewCache(store, int32(capacity))
	if err != nil {
		return nil, err
	}

	return &MostRecentBlockCache{
		cache: cache,
	}, nil
}

// Close instructs the underlying database to stop cleanly.
func (c *MostRecentBlockCache) Close() {
	//c.cache.Close()
}

// PutBlock attempts to insert the block into the cache. If the cache is at
// capacity it will first attempt to evict the oldest block, if the given block
// is older than all blocks currently in the cache, then no action happens.
func (c *MostRecentBlockCache) PutBlock(block *wire.MsgBlock) error {
	// Serialize the block.
	w := bytes.NewBuffer(make([]byte, 0, block.SerializeSize()))
	err := block.Serialize(w)
	if err != nil {
		return err
	}

	hash := block.BlockHash()

	return c.cache.Put(hash.CloneBytes(), w.Bytes())
}

// GetStats returns the number of cache hits and misses.
func (c *MostRecentBlockCache) GetStats() (uint64, uint64) {
	hits := atomic.LoadUint64(&c.hits)
	misses := atomic.LoadUint64(&c.misses)
	return hits, misses
}

// FetchBlock queries the cache for a block matching the given hash, returning
// the block or ErrBlockNotFound if it couldn't be retrieved.
func (c *MostRecentBlockCache) FetchBlock(hash *chainhash.Hash) (*wire.MsgBlock,
	error) {
	var block wire.MsgBlock

	value, err := c.cache.Get(hash.CloneBytes())
	if err != nil && err != ErrBlockNotFound {
		return nil, err
	}

	if value == nil {
		atomic.AddUint64(&c.misses, 1)
		return nil, ErrBlockNotFound
	}

	err = block.Deserialize(bytes.NewReader(value))
	if err != nil {
		return nil, err
	}

	atomic.AddUint64(&c.hits, 1)
	return &block, nil
}
