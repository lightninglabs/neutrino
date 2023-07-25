package chanutils

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const waitTime = time.Second * 5

// TestBatchWriter tests that the BatchWriter behaves as expected.
func TestBatchWriter(t *testing.T) {
	t.Parallel()
	rand.Seed(time.Now().UnixNano())

	// waitForItems is a helper function that will wait for a given set of
	// items to appear in the db.
	waitForItems := func(db *mockItemsDB, items ...*item) {
		err := waitFor(func() bool {
			return db.hasItems(items...)
		}, waitTime)
		require.NoError(t, err)
	}

	t.Run("filters persisted after ticker", func(t *testing.T) {
		t.Parallel()

		// Create a mock filters DB.
		db := newMockItemsDB()

		// Construct a new BatchWriter backed by the mock db.
		b := NewBatchWriter[*item](&BatchWriterConfig[*item]{
			QueueBufferSize:        10,
			MaxBatch:               20,
			DBWritesTickerDuration: time.Millisecond * 500,
			PutItems:               db.PutItems,
		})
		b.Start()
		t.Cleanup(b.Stop)

		fs := genFilterSet(5)
		for _, f := range fs {
			b.AddItem(f)
		}
		waitForItems(db, fs...)
	})

	t.Run("write once threshold is reached", func(t *testing.T) {
		t.Parallel()

		// Create a mock filters DB.
		db := newMockItemsDB()

		// Construct a new BatchWriter backed by the mock db.
		// Make the DB writes ticker duration extra long so that we
		// can explicitly test that the batch gets persisted if the
		// MaxBatch threshold is reached.
		b := NewBatchWriter[*item](&BatchWriterConfig[*item]{
			QueueBufferSize:        10,
			MaxBatch:               20,
			DBWritesTickerDuration: time.Hour,
			PutItems:               db.PutItems,
		})
		b.Start()
		t.Cleanup(b.Stop)

		// Generate 30 filters and add each one to the batch writer.
		fs := genFilterSet(30)
		for _, f := range fs {
			b.AddItem(f)
		}

		// Since the MaxBatch threshold has been reached, we expect the
		// first 20 filters to be persisted.
		waitForItems(db, fs[:20]...)

		// Since the last 10 filters don't reach the threshold and since
		// the ticker has definitely not ticked yet, we don't expect the
		// last 10 filters to be in the db yet.
		require.False(t, db.hasItems(fs[21:]...))
	})

	t.Run("stress test", func(t *testing.T) {
		t.Parallel()

		// Create a mock filters DB.
		db := newMockItemsDB()

		// Construct a new BatchWriter backed by the mock db.
		// Make the DB writes ticker duration extra long so that we
		// can explicitly test that the batch gets persisted if the
		// MaxBatch threshold is reached.
		b := NewBatchWriter[*item](&BatchWriterConfig[*item]{
			QueueBufferSize:        5,
			MaxBatch:               5,
			DBWritesTickerDuration: time.Millisecond * 2,
			PutItems:               db.PutItems,
		})
		b.Start()
		t.Cleanup(b.Stop)

		// Generate lots of filters and add each to the batch writer.
		// Sleep for a bit between each filter to ensure that we
		// sometimes hit the timeout write and sometimes the threshold
		// write.
		fs := genFilterSet(1000)
		for _, f := range fs {
			b.AddItem(f)

			n := rand.Intn(3)
			time.Sleep(time.Duration(n) * time.Millisecond)
		}

		// Since the MaxBatch threshold has been reached, we expect the
		// first 20 filters to be persisted.
		waitForItems(db, fs...)
	})
}

type item struct {
	i int
}

// mockItemsDB is a mock DB that holds a set of items.
type mockItemsDB struct {
	items map[int]bool
	mu    sync.Mutex
}

// newMockItemsDB constructs a new mockItemsDB.
func newMockItemsDB() *mockItemsDB {
	return &mockItemsDB{
		items: make(map[int]bool),
	}
}

// hasItems returns true if the db contains all the given items.
func (m *mockItemsDB) hasItems(items ...*item) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, i := range items {
		_, ok := m.items[i.i]
		if !ok {
			return false
		}
	}

	return true
}

// PutItems adds a set of items to the db.
func (m *mockItemsDB) PutItems(items ...*item) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, i := range items {
		m.items[i.i] = true
	}

	return nil
}

// genItemSet generates a set of numFilters items.
func genFilterSet(numFilters int) []*item {
	res := make([]*item, numFilters)
	for i := 0; i < numFilters; i++ {
		res[i] = &item{i: i}
	}

	return res
}

// pollInterval is a constant specifying a 200 ms interval.
const pollInterval = 200 * time.Millisecond

// waitFor is a helper test function that will wait for a timeout period of
// time until the passed predicate returns true. This function is helpful as
// timing doesn't always line up well when running integration tests with
// several running lnd nodes. This function gives callers a way to assert that
// some property is upheld within a particular time frame.
func waitFor(pred func() bool, timeout time.Duration) error {
	exitTimer := time.After(timeout)
	result := make(chan bool, 1)

	for {
		<-time.After(pollInterval)

		go func() {
			result <- pred()
		}()

		// Each time we call the pred(), we expect a result to be
		// returned otherwise it will timeout.
		select {
		case <-exitTimer:
			return fmt.Errorf("predicate not satisfied after " +
				"time out")

		case succeed := <-result:
			if succeed {
				return nil
			}
		}
	}
}
