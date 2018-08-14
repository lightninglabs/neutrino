package neutrino

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcutil/gcs"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/lightninglabs/neutrino/cache"
	"github.com/lightninglabs/neutrino/cache/lru"
	"github.com/lightninglabs/neutrino/filterdb"
)

// genRandomBlockHash generates a random block hash using math/rand.
func genRandomBlockHash() *chainhash.Hash {
	var seed [32]byte
	rand.Read(seed[:])
	hash := chainhash.Hash(seed)
	return &hash
}

// getRandFilter generates a random GCS filter that contains numElements. It
// will then convert that filter into CacheableFilter to compute it's size for
// convenience. It will return the filter along with it's size and randomly
// generated block hash. testing.T is passed in as a convenience to deal with
// errors in this method and making the test code more straigthforward. Method
// originally taken from filterdb/db_test.go.
func genRandFilter(numElements uint32, t *testing.T) (
	*chainhash.Hash, *gcs.Filter, uint64) {
	elements := make([][]byte, numElements)
	for i := uint32(0); i < numElements; i++ {
		var elem [20]byte
		if _, err := rand.Read(elem[:]); err != nil {
			t.Fatalf("unable to create random filter: %v", err)
			return nil, nil, 0
		}

		elements[i] = elem[:]
	}

	var key [16]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("unable to create random filter: %v", err)
		return nil, nil, 0
	}

	filter, err := gcs.BuildGCSFilter(
		builder.DefaultP, builder.DefaultM, key, elements)
	if err != nil {
		t.Fatalf("unable to create random filter: %v", err)
		return nil, nil, 0
	}

	// Convert into CacheableFilter and compute Size.
	c := &cache.CacheableFilter{Filter: filter}
	s, err := c.Size()
	if err != nil {
		t.Fatalf("unable to create random filter: %v", err)
		return nil, nil, 0
	}

	return genRandomBlockHash(), filter, s
}

// getFilter is a convenience method which will extract a value from the cache
// and handle errors, it makes the test code easier to follow.
func getFilter(cs *ChainService, b *chainhash.Hash, t *testing.T) *gcs.Filter {
	val, err := cs.getFilterFromCache(b, filterdb.RegularFilter)
	if err != nil {
		t.Fatal(err)
	}
	return val
}

func assertEqual(t *testing.T, a interface{}, b interface{}, message string) {
	if a == b {
		return
	}
	if len(message) == 0 {
		message = fmt.Sprintf("%v != %v", a, b)
	}
	t.Fatal(message)
}

// TestCacheBigEnoughHoldsAllFilter creates a cache big enough to hold all
// filters, then gets them in random order and makes sure they are always there.
func TestCacheBigEnoughHoldsAllFilter(t *testing.T) {
	// Create different sized filters.
	b1, f1, s1 := genRandFilter(1, t)
	b2, f2, s2 := genRandFilter(10, t)
	b3, f3, s3 := genRandFilter(100, t)

	cs := &ChainService{
		FilterCache: lru.NewCache(s1 + s2 + s3),
	}

	// Insert those filters into the cache making sure nothing gets evicted.
	assertEqual(t, cs.FilterCache.Len(), 0, "")
	cs.putFilterToCache(b1, filterdb.RegularFilter, f1)
	assertEqual(t, cs.FilterCache.Len(), 1, "")
	cs.putFilterToCache(b2, filterdb.RegularFilter, f2)
	assertEqual(t, cs.FilterCache.Len(), 2, "")
	cs.putFilterToCache(b3, filterdb.RegularFilter, f3)
	assertEqual(t, cs.FilterCache.Len(), 3, "")

	// Check that we can get those filters back independent of Get order.
	assertEqual(t, getFilter(cs, b1, t), f1, "")
	assertEqual(t, getFilter(cs, b2, t), f2, "")
	assertEqual(t, getFilter(cs, b3, t), f3, "")
	assertEqual(t, getFilter(cs, b2, t), f2, "")
	assertEqual(t, getFilter(cs, b3, t), f3, "")
	assertEqual(t, getFilter(cs, b1, t), f1, "")
	assertEqual(t, getFilter(cs, b3, t), f3, "")

	assertEqual(t, cs.FilterCache.Len(), 3, "")
}

// TestBigFilterEvictsEverything creates a cache big enough to hold a large
// filter and inserts many smaller filters into. Then it inserts the big filter
// and verifies that it's the only one remaining.
func TestBigFilterEvictsEverything(t *testing.T) {
	// Create different sized filters.
	b1, f1, _ := genRandFilter(1, t)
	b2, f2, _ := genRandFilter(3, t)
	b3, f3, s3 := genRandFilter(10, t)

	cs := &ChainService{
		FilterCache: lru.NewCache(s3),
	}

	// Insert the smaller filters.
	assertEqual(t, cs.FilterCache.Len(), 0, "")
	cs.putFilterToCache(b1, filterdb.RegularFilter, f1)
	assertEqual(t, cs.FilterCache.Len(), 1, "")
	cs.putFilterToCache(b2, filterdb.RegularFilter, f2)
	assertEqual(t, cs.FilterCache.Len(), 2, "")

	// Insert the big filter and check all previous filters are evicted.
	cs.putFilterToCache(b3, filterdb.RegularFilter, f3)
	assertEqual(t, cs.FilterCache.Len(), 1, "")
	assertEqual(t, getFilter(cs, b3, t), f3, "")
}
