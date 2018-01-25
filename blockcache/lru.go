package blockcache

import (
	"bytes"
	"container/list"
	"fmt"
	"github.com/boltdb/bolt"
	"os"
	"path/filepath"
	"sync"
)

// Store is an interface to a persistent key-value store.
type Store interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) ([]byte, error)
	Size() int
	Keys() ([][]byte, error)
}

// BoltStore uses boltdb to implement the Store interface.
type BoltStore struct {
	db *bolt.DB
}

// NewStore opens an existing boltdb store, or if it doesn't exist creates a
// new one.
func NewStore(dbPath string) (*BoltStore, error) {
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

	return &BoltStore{db: bdb}, nil
}

// Close instructs the underlying database to stop cleanly.
func (s *BoltStore) Close() {
	s.db.Close()
}

// Size calculates the size of all values stored in the db.
func (s *BoltStore) Size() int {
	var usage int
	s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(blockCacheBucket).ForEach(func(k, v []byte) error {
			usage += len(v)
			return nil
		})
	})

	return usage
}

// Keys returns a slice of all keys in the db.
func (s *BoltStore) Keys() ([][]byte, error) {
	var keys [][]byte
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(blockCacheBucket).ForEach(func(k, v []byte) error {
			keys = append(keys, k)
			return nil
		})
	})

	return keys, err
}

// Put stores a key-value pair in the db.
func (s *BoltStore) Put(key []byte, value []byte) error {
	return s.db.Batch(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(blockCacheBucket)

		if err := bucket.Put(key, value); err != nil {
			return err
		}

		return nil
	})
}

// Get returns the value for this key, or nil if it doesn't exist.
func (s *BoltStore) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(blockCacheBucket)

		// Fetch the block, if it exists.
		value = bucket.Get(key)

		return nil
	})

	return value, err
}

// Delete removes the given key from the db and returns its value.
func (s *BoltStore) Delete(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.Batch(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(blockCacheBucket)
		value = bucket.Get(key)
		return bucket.Delete(key)
	})

	return value, err
}

// Cache implements an LRU cache with a fixed capacity. If an element is added
// to the cache that causes it to be over capacity, then the least recently used
// elements will be evicted until the cache satisfies its capacity constraint.
type Cache struct {
	// The maximum number of bytes to store in the cache.
	capacity int32
	usage    int32

	mu sync.Mutex // protects access

	access *list.List
	store  Store
}

// NewCache creates a new LRU cache backed by the given store and constrained by
// the given capacity, in bytes.
func NewCache(store Store, capacity int32) (*Cache, error) {
	// Read which keys are currently in the cache.
	access := list.New()
	keys, err := store.Keys()
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		access.PushBack(key)
	}

	return &Cache{
		capacity: capacity,
		usage:    int32(store.Size()),
		access:   access,
		store:    store,
	}, nil
}

func (c *Cache) find(key []byte) *list.Element {
	for e := c.access.Front(); e != nil; e = e.Next() {
		if bytes.Equal(e.Value.([]byte), key) {
			return e
		}
	}
	return nil
}

func (c *Cache) isFull() bool {
	return c.usage > c.capacity
}

// evict deletes the least recently used entry from the cache. Callers must hold
// the c.mu mutex.
func (c *Cache) evict() error {
	e := c.access.Back()
	if e == nil {
		return fmt.Errorf("cache empty")
	}

	key := c.access.Remove(e).([]byte)
	value, err := c.store.Delete(key)
	if err != nil {
		return err
	}

	c.usage -= int32(len(value))

	return nil
}

// Put inserts a new element into the cache, evicting the oldest elements to
// make room.
// This function is safe for concurrent access.
func (c *Cache) Put(key []byte, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the key already exists, if so there's nothing to do.
	e := c.find(key)
	if e != nil {
		return nil
	}

	err := c.store.Put(key, value)
	if err != nil {
		return err
	}
	c.access.PushFront(key)
	c.usage += int32(len(value))

	// Check if we need to evict anything.
	for c.isFull() {
		if err := c.evict(); err != nil {
			return err
		}
	}

	return nil
}

// Get returns the value for the given key, or ErrBlockNotFound if the element
// does not exist.
// This function is safe for concurrent access.
func (c *Cache) Get(key []byte) ([]byte, error) {
	// Check if this key exists in the cache.
	value, err := c.store.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, ErrBlockNotFound
	}

	// If this element was already cached then bump it to the front of the list.
	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.find(key)
	// It's possible that the value was removed from the cache between the call
	// to Get and now, in that case we can't mark it as recently used, but we
	// can still return the value.
	if e != nil {
		c.access.MoveToFront(e)
	}

	return value, nil
}
