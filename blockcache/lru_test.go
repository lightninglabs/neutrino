package blockcache

import (
	"bytes"
	"os"
	"testing"
)

func TestCacheHit(t *testing.T) {
	store, _ := NewStore(os.TempDir())
	cache, _ := NewCache(store, 5)
	defer store.Close()

	cache.Put([]byte("key"), []byte("value"))

	value, _ := cache.Get([]byte("key"))
	if !bytes.Equal(value, []byte("value")) {
		t.Errorf("Value was incorrect")
	}
}

func TestCacheMiss(t *testing.T) {
	store, _ := NewStore(os.TempDir())
	cache, _ := NewCache(store, 5)
	defer store.Close()

	cache.Put([]byte("key"), []byte("value"))

	_, err := cache.Get([]byte("key2"))
	if err != ErrBlockNotFound {
		t.Errorf("Expected not to find entry")
	}
}

func TestEvictionOfOldest(t *testing.T) {
	store, _ := NewStore(os.TempDir())
	cache, _ := NewCache(store, 5)
	defer store.Close()

	cache.Put([]byte("key"), []byte("value"))
	cache.Put([]byte("key2"), []byte("hello"))

	_, err := cache.Get([]byte("key"))
	if err != ErrBlockNotFound {
		t.Errorf("Expected not to find entry")
	}

	value, err := cache.Get([]byte("key2"))
	if err != nil || !bytes.Equal(value, []byte("hello")) {
		t.Errorf("Expected to find most recent entry, got %v", value)
	}
}

func TestEvictionOfMultipleEntries(t *testing.T) {
	store, _ := NewStore(os.TempDir())
	cache, _ := NewCache(store, 5)
	defer store.Close()

	// The following values should fit in the cache.
	cache.Put([]byte("key1"), []byte("1"))
	cache.Put([]byte("key2"), []byte("2"))
	cache.Put([]byte("key3"), []byte("3"))

	// This should cause the eviction of everything we previously added.
	cache.Put([]byte("key4"), []byte("hello"))

	_, err := cache.Get([]byte("key1"))
	if err != ErrBlockNotFound {
		t.Errorf("Expected not to find entry")
	}
	_, err = cache.Get([]byte("key2"))
	if err != ErrBlockNotFound {
		t.Errorf("Expected not to find entry")
	}
	_, err = cache.Get([]byte("key3"))
	if err != ErrBlockNotFound {
		t.Errorf("Expected not to find entry")
	}

	value, err := cache.Get([]byte("key4"))
	if err != nil || !bytes.Equal(value, []byte("hello")) {
		t.Errorf("Expected to find most recent entry, got %v", value)
	}
}
