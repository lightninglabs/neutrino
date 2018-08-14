package lru

import (
	"fmt"
	"sync"
	"testing"

	"github.com/lightninglabs/neutrino/cache"
)

func assertEqual(t *testing.T, a interface{}, b interface{}, message string) {
	if a == b {
		return
	}
	if len(message) == 0 {
		message = fmt.Sprintf("%v != %v", a, b)
	}
	t.Fatal(message)
}

// sizeable is a simple struct that represents an element of arbitrary size
// which holds a simple integer.
type sizeable struct {
	value int
	size  uint64
}

// Size implements the CacheEntry interface on sizeable struct.
func (s *sizeable) Size() (uint64, error) {
	return s.size, nil
}

// getSizeableValue is a helper method used for converting the cache.Value
// interface to sizeable struct and extracting the value from it.
func getSizeableValue(generic cache.Value, _ error) int {
	return generic.(*sizeable).value
}

// TestEmptyCacheSizeZero will check that an empty cache has a size of 0.
func TestEmptyCacheSizeZero(t *testing.T) {
	t.Parallel()
	c := NewCache(10)
	assertEqual(t, c.Len(), 0, "")
}

// TestCacheNeverExceedsSize inserts many filters into the cache and verifies
// at each step that the cache never exceeds it's initial size.
func TestCacheNeverExceedsSize(t *testing.T) {
	t.Parallel()
	c := NewCache(2)
	c.Put(1, &sizeable{value: 1, size: 1})
	c.Put(2, &sizeable{value: 2, size: 1})
	assertEqual(t, c.Len(), 2, "")
	for i := 0; i < 10; i++ {
		c.Put(i, &sizeable{value: i, size: 1})
		assertEqual(t, c.Len(), 2, "")
	}
}

// TestCacheAlwaysHasLastAccessedItems will check that the last items that
// were put in the cache are always available, it will also check the eviction
// behavior when items put in the cache exceeds cache capacity.
func TestCacheAlwaysHasLastAccessedItems(t *testing.T) {
	t.Parallel()
	c := NewCache(2)
	c.Put(1, &sizeable{value: 1, size: 1})
	c.Put(2, &sizeable{value: 2, size: 1})
	two := getSizeableValue(c.Get(2))
	one := getSizeableValue(c.Get(1))
	assertEqual(t, two, 2, "")
	assertEqual(t, one, 1, "")

	c = NewCache(2)
	c.Put(1, &sizeable{value: 1, size: 1})
	c.Put(2, &sizeable{value: 2, size: 1})
	c.Put(3, &sizeable{value: 3, size: 1})
	oneEntry, _ := c.Get(1)
	two = getSizeableValue(c.Get(2))
	three := getSizeableValue(c.Get(3))
	assertEqual(t, oneEntry, nil, "")
	assertEqual(t, two, 2, "")
	assertEqual(t, three, 3, "")

	c = NewCache(2)
	c.Put(1, &sizeable{value: 1, size: 1})
	c.Put(2, &sizeable{value: 2, size: 1})
	c.Get(1)
	c.Put(3, &sizeable{value: 3, size: 1})
	one = getSizeableValue(c.Get(1))
	twoEntry, _ := c.Get(2)
	three = getSizeableValue(c.Get(3))
	assertEqual(t, one, 1, "")
	assertEqual(t, twoEntry, nil, "")
	assertEqual(t, three, 3, "")
}

// TestElementSizeCapacityEvictsEverything tests that Cache evicts everything
// from cache when an element with size=capacity is inserted.
func TestElementSizeCapacityEvictsEverything(t *testing.T) {
	t.Parallel()
	c := NewCache(3)

	c.Put(1, &sizeable{value: 1, size: 1})
	c.Put(2, &sizeable{value: 2, size: 1})
	c.Put(3, &sizeable{value: 3, size: 1})

	// Insert element with size=capacity of cache, should evict everything.
	c.Put(4, &sizeable{value: 4, size: 3})
	assertEqual(t, c.Len(), 1, "")
	assertEqual(t, len(c.cache), 1, "")
	four := getSizeableValue(c.Get(4))
	assertEqual(t, four, 4, "")

	c = NewCache(6)
	c.Put(1, &sizeable{value: 1, size: 1})
	c.Put(2, &sizeable{value: 2, size: 2})
	c.Put(3, &sizeable{value: 3, size: 3})
	assertEqual(t, c.size, uint64(6), "")

	// Insert element with size=capacity of cache.
	c.Put(4, &sizeable{value: 4, size: 6})
	assertEqual(t, c.Len(), 1, "")
	assertEqual(t, len(c.cache), 1, "")
	four = getSizeableValue(c.Get(4))
	assertEqual(t, four, 4, "")
}

// TestCacheFailsInsertionSizeBiggerCapacity tests that the cache fails the
// put operation when the element's size is bigger than it's capacity.
func TestCacheFailsInsertionSizeBiggerCapacity(t *testing.T) {
	t.Parallel()
	c := NewCache(2)

	err := c.Put(1, &sizeable{value: 1, size: 3})
	if err == nil {
		t.Fatal("shouldn't be able to put elements larger than cache")
	}
	assertEqual(t, c.Len(), 0, "")
}

// TestManySmallElementCanInsertAfterBigEviction tests that when a big element
// is evicted from the Cache, multiple smaller ones can be inserted without an
// eviction taking place.
func TestManySmallElementCanInsertAfterBigEviction(t *testing.T) {
	t.Parallel()
	c := NewCache(3)

	err := c.Put(1, &sizeable{value: 1, size: 3})
	if err != nil {
		t.Fatal("couldn't insert element")
	}

	assertEqual(t, c.Len(), 1, "")

	c.Put(2, &sizeable{value: 2, size: 1})
	two := getSizeableValue(c.Get(2))
	oneEntry, _ := c.Get(1)
	assertEqual(t, c.Len(), 1, "")
	assertEqual(t, two, 2, "")
	assertEqual(t, oneEntry, nil, "")

	c.Put(3, &sizeable{value: 3, size: 1})
	assertEqual(t, c.Len(), 2, "")

	c.Put(4, &sizeable{value: 4, size: 1})
	assertEqual(t, c.Len(), 3, "")

	two = getSizeableValue(c.Get(2))
	three := getSizeableValue(c.Get(3))
	four := getSizeableValue(c.Get(4))
	assertEqual(t, two, 2, "")
	assertEqual(t, three, 3, "")
	assertEqual(t, four, 4, "")
}

// TestReplacingElementValueSmallerSize tests that if an existing element is
// replaced with a value of size smaller, that the size shrinks and we can
// insert without an eviction taking place.
func TestReplacingElementValueSmallerSize(t *testing.T) {
	t.Parallel()
	c := NewCache(2)

	c.Put(1, &sizeable{value: 1, size: 2})

	c.Put(1, &sizeable{value: 1, size: 1})
	c.Put(2, &sizeable{value: 2, size: 1})
	one := getSizeableValue(c.Get(1))
	two := getSizeableValue(c.Get(2))
	assertEqual(t, one, 1, "")
	assertEqual(t, two, 2, "")
	assertEqual(t, c.Len(), 2, "")
}

// TestReplacingElementValueBiggerSize tests that if an existing element is
// replaced with a value of size bigger, that it evicts accordingly.
func TestReplacingElementValueBiggerSize(t *testing.T) {
	t.Parallel()
	c := NewCache(2)

	c.Put(1, &sizeable{value: 1, size: 1})
	c.Put(2, &sizeable{value: 2, size: 1})

	c.Put(1, &sizeable{value: 3, size: 2})
	assertEqual(t, c.Len(), 1, "")
	one := getSizeableValue(c.Get(1))
	assertEqual(t, one, 3, "")
}

// TestConcurrencySimple is a very simple test that checks concurrent access to
// the lru cache. When running the test, "-race" option should be passed to
// "go test" command.
func TestConcurrencySimple(t *testing.T) {
	t.Parallel()
	c := NewCache(5)
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := c.Put(i, &sizeable{value: i, size: 1})
			if err != nil {
				t.Fatal(err)
			}
		}(i)
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.Get(i)
			if err != nil && err != cache.ErrElementNotFound {
				t.Fatal(err)
			}
		}(i)
	}

	wg.Wait()
}

// TestConcurrencySmallCache is a test that checks concurrent access to the
// lru cache when the cache is smaller than the number of elements we want to
// put and retrieve. When running the test, "-race" option should be passed to
// "go test" command.
func TestConcurrencySmallCache(t *testing.T) {
	t.Parallel()
	c := NewCache(5)
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := c.Put(i, &sizeable{value: i, size: 1})
			if err != nil {
				t.Fatal(err)
			}
		}(i)
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.Get(i)
			if err != nil && err != cache.ErrElementNotFound {
				t.Fatal(err)
			}
		}(i)
	}

	wg.Wait()
}

// TestConcurrencyBigCache is a test that checks concurrent access to the
// lru cache when the cache is bigger than the number of elements we want to
// put and retrieve. When running the test, "-race" option should be passed to
// "go test" command.
func TestConcurrencyBigCache(t *testing.T) {
	t.Parallel()
	c := NewCache(100)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := c.Put(i, &sizeable{value: i, size: 1})
			if err != nil {
				t.Fatal(err)
			}
		}(i)
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.Get(i)
			if err != nil && err != cache.ErrElementNotFound {
				t.Fatal(err)
			}
		}(i)
	}

	wg.Wait()
}
