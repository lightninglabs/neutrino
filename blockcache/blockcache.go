package blockcache

import (
	"fmt"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcd/wire"
	"github.com/roasbeef/btcwallet/walletdb"
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
	db walletdb.DB

	// A BlockHeaderStore allows translation between block heights and hashes.
	blockHeaders *headerfs.BlockHeaderStore

	// The maximum number of blocks to store in the cache.
	capacity int
}

// A compile-time check to ensure the MostRecentBlockCache adheres to the
// BlockCache interface.
var _ BlockCache = (*MostRecentBlockCache)(nil)

// New creates a new instance of a MostRecentBlockCache given an already open
// database.
func New(db walletdb.DB, capacity int, blockHeaders *headerfs.BlockHeaderStore) (*MostRecentBlockCache, error) {
	err := walletdb.Update(db, func(tx walletdb.ReadWriteTx) error {
		// Create the cache bucket, if it doesn't already exist.
		_, err := tx.CreateTopLevelBucket(blockCacheBucket)
		return err
	})
	if err != nil && err != walletdb.ErrBucketExists {
		return nil, err
	}

	return &MostRecentBlockCache{
		db:           db,
		capacity:     capacity,
		blockHeaders: blockHeaders,
	}, nil
}

func (c *MostRecentBlockCache) PutBlock(block *wire.MsgBlock) error {
	// TODO(simon): Key by height to maintain order.

	/*
		err := db.Update(func(tx *bolt.Tx) error {
		...
		return nil
		})
	*/

	return fmt.Errorf("not implemented")
}

func (c *MostRecentBlockCache) FetchBlock(*chainhash.Hash) (*wire.MsgBlock,
	error) {
	// TODO(simon): Look up height from hash, then query by height and double
	// check.

	/*
		err := db.View(func(tx *bolt.Tx) error {
			...
			return nil
		})
	*/
	return nil, ErrBlockNotFound
}
