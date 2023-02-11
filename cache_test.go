package neutrino

import (
	"crypto/rand"
	"testing"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/gcs"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/cache"
	"github.com/lightninglabs/neutrino/cache/lru"
	"github.com/lightninglabs/neutrino/filterdb"
)

// TestBlockFilterCaches tests that we can put and retrieve elements from all
// implementations of the filter and block caches.
func TestBlockFilterCaches(t *testing.T) {
	t.Parallel()

	const filterType = filterdb.RegularFilter

	// Create a cache large enough to not evict any item. We do this so we
	// don't have to worry about the eviction strategy of the tested
	// caches.
	const numElements = 10
	const cacheSize = 100000

	// Initialize all types of caches we want to test, for both filters and
	// blocks. Currently the LRU cache is the only implementation.
	filterCaches := []cache.Cache[FilterCacheKey, *CacheableFilter]{
		lru.NewCache[FilterCacheKey, *CacheableFilter](cacheSize),
	}
	blockCaches := []cache.Cache[wire.InvVect, *CacheableBlock]{
		lru.NewCache[wire.InvVect, *CacheableBlock](cacheSize),
	}

	// Generate a list of hashes, filters and blocks that we will use as
	// cache keys an values.
	var (
		blockHashes []chainhash.Hash
		filters     []*gcs.Filter
		blocks      []*btcutil.Block
	)
	for i := 0; i < numElements; i++ {
		var blockHash chainhash.Hash
		if _, err := rand.Read(blockHash[:]); err != nil {
			t.Fatalf("unable to read rand: %v", err)
		}

		blockHashes = append(blockHashes, blockHash)

		filter, err := gcs.FromBytes(
			uint32(i), uint8(i), uint64(i), []byte{byte(i)},
		)
		if err != nil {
			t.Fatalf("unable to create filter: %v", err)
		}
		filters = append(filters, filter)

		// Put the generated filter in the filter caches.
		cacheKey := FilterCacheKey{blockHash, filterType}
		for _, c := range filterCaches {
			_, _ = c.Put(cacheKey, &CacheableFilter{Filter: filter})
		}

		msgBlock := &wire.MsgBlock{}
		block := btcutil.NewBlock(msgBlock)
		blocks = append(blocks, block)

		// Add the block to the block caches, using the block INV
		// vector as key.
		blockKey := wire.NewInvVect(
			wire.InvTypeWitnessBlock, &blockHash,
		)
		for _, c := range blockCaches {
			_, _ = c.Put(*blockKey, &CacheableBlock{block})
		}
	}

	// Now go through the list of block hashes, and make sure we can
	// retrieve all elements from the caches.
	for i, blockHash := range blockHashes {
		blockHash := blockHash

		// Check filter caches.
		cacheKey := FilterCacheKey{blockHash, filterType}
		for _, c := range filterCaches {
			e, err := c.Get(cacheKey)
			if err != nil {
				t.Fatalf("Unable to get filter: %v", err)
			}

			// Ensure we got the correct filter.
			filter := e.Filter
			if filter != filters[i] {
				t.Fatalf("Filters not equal: %v vs %v ",
					filter, filters[i])
			}
		}

		// Check block caches.
		blockKey := wire.NewInvVect(
			wire.InvTypeWitnessBlock, &blockHash,
		)
		for _, c := range blockCaches {
			b, err := c.Get(*blockKey)
			if err != nil {
				t.Fatalf("Unable to get block: %v", err)
			}

			// Ensure it is the same block.
			block := b.Block
			if block != blocks[i] {
				t.Fatalf("Not equal: %v vs %v ",
					block, blocks[i])
			}
		}
	}
}
