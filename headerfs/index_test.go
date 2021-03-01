package headerfs

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	mathRand "math/rand"
	"os"
	"testing"
	"time"

	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
)

func createTestIndex() (func(), *headerIndex, error) {
	tempDir, err := ioutil.TempDir("", "neutrino")
	if err != nil {
		return nil, nil, err
	}

	db, err := walletdb.Create(
		"bdb", tempDir+"/test.db", true, time.Second*10,
	)
	if err != nil {
		return nil, nil, err
	}

	cleanUp := func() {
		_ = db.Close()
		fi, _ := os.Stat(tempDir + "/test.db")
		fmt.Printf("DB file size at cleanup: %d bytes\n", fi.Size())
		_ = os.RemoveAll(tempDir)
	}

	filterDB, err := newHeaderIndex(db, Block)
	if err != nil {
		return nil, nil, err
	}

	return cleanUp, filterDB, nil
}

func TestAddHeadersIndexRetrieve(t *testing.T) {
	cleanUp, hIndex, err := createTestIndex()
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

	// First, we'll create a a series of random headers that we'll use to
	// write into the database.
	const numHeaders = 100
	headerEntries, headerIndex, err := writeRandomBatch(hIndex, numHeaders)
	if err != nil {
		t.Fatalf("error writing random batch: %v", err)
	}

	// Next, verify that the database tip matches the _final_ header
	// inserted.
	dbTip, dbHeight, err := hIndex.chainTip()
	if err != nil {
		t.Fatalf("unable to obtain chain tip: %v", err)
	}
	lastEntry := headerIndex[numHeaders-1]
	if dbHeight != lastEntry.height {
		t.Fatalf("height doesn't match: expected %v, got %v",
			lastEntry.height, dbHeight)
	}
	if !bytes.Equal(dbTip[:], lastEntry.hash[:]) {
		t.Fatalf("tip doesn't match: expected %x, got %x",
			lastEntry.hash[:], dbTip[:])
	}

	// For each header written, check that we're able to retrieve the entry
	// both by hash and height.
	for i, headerEntry := range headerEntries {
		height, err := hIndex.heightFromHash(&headerEntry.hash)
		if err != nil {
			t.Fatalf("unable to retreive height(%v): %v", i, err)
		}
		if height != headerEntry.height {
			t.Fatalf("height doesn't match: expected %v, got %v",
				headerEntry.height, height)
		}
	}

	// Next if we truncate the index by one, then we should end up at the
	// second to last entry for the tip.
	newTip := headerIndex[numHeaders-2]
	if err := hIndex.truncateIndex(&newTip.hash, true); err != nil {
		t.Fatalf("unable to truncate index: %v", err)
	}

	// This time the database tip should be the _second_ to last entry
	// inserted.
	dbTip, dbHeight, err = hIndex.chainTip()
	if err != nil {
		t.Fatalf("unable to obtain chain tip: %v", err)
	}
	lastEntry = headerIndex[numHeaders-2]
	if dbHeight != lastEntry.height {
		t.Fatalf("height doesn't match: expected %v, got %v",
			lastEntry.height, dbHeight)
	}
	if !bytes.Equal(dbTip[:], lastEntry.hash[:]) {
		t.Fatalf("tip doesn't match: expected %x, got %x",
			lastEntry.hash[:], dbTip[:])
	}
}

// BenchmarkWriteHeadersSmallBatch measures the performance of writing 500k
// headers to the database in small size batches (100 headers per batch).
func BenchmarkWriteHeadersSmallBatch(b *testing.B) {
	const (
		batchSize  = 100
		numBatches = 5000
	)
	for n := 0; n < b.N; n++ {
		cleanUp, hIndex, err := createTestIndex()
		if err != nil {
			b.Fatalf("unable to create test db: %v", err)
		}

		for j := 0; j < numBatches; j++ {
			_, _, err := writeRandomBatch(hIndex, batchSize)
			if err != nil {
				b.Fatalf("error writing random batch: %v", err)
			}
		}

		cleanUp()
	}
}

// BenchmarkWriteHeadersMediumBatch measures the performance of writing 500k
// headers to the database in medium size batches (2000 headers per batch).
func BenchmarkWriteHeadersMediumBatch(b *testing.B) {
	const (
		batchSize  = 2000
		numBatches = 250
	)
	for n := 0; n < b.N; n++ {
		cleanUp, hIndex, err := createTestIndex()
		if err != nil {
			b.Fatalf("unable to create test db: %v", err)
		}

		for j := 0; j < numBatches; j++ {
			_, _, err := writeRandomBatch(hIndex, batchSize)
			if err != nil {
				b.Fatalf("error writing random batch: %v", err)
			}
		}

		cleanUp()
	}
}

// BenchmarkWriteHeadersLargeBatch measures the performance of writing 500k
// headers to the database in large size batches (10000 headers per batch).
func BenchmarkWriteHeadersLargeBatch(b *testing.B) {
	const (
		batchSize  = 10000
		numBatches = 50
	)
	for n := 0; n < b.N; n++ {
		cleanUp, hIndex, err := createTestIndex()
		if err != nil {
			b.Fatalf("unable to create test db: %v", err)
		}

		for j := 0; j < numBatches; j++ {
			_, _, err := writeRandomBatch(hIndex, batchSize)
			if err != nil {
				b.Fatalf("error writing random batch: %v", err)
			}
		}

		cleanUp()
	}
}

// BenchmarkHeightLookupLatency benchmarks the speed for randomly accessing the
// index with a hash.
func BenchmarkHeightLookupLatency(b *testing.B) {
	// Start by creating an index with 10k headers.
	cleanUp, hIndex, err := createTestIndex()
	if err != nil {
		b.Fatalf("unable to create test db: %v", err)
	}

	const batchSize = 50000
	_, headerIndex, err := writeRandomBatch(hIndex, batchSize)
	if err != nil {
		b.Fatalf("error writing random batch: %v", err)
	}

	// Only start the benchmark counter now, for this test we don't want to
	// know how fast we can write the index but how fast we can read it.
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		index := uint32(mathRand.Int31n(batchSize))
		hash := headerIndex[index].hash
		_, err := hIndex.heightFromHash(&hash)
		if err != nil {
			b.Fatalf("error fetching height: %v", err)
		}
	}

	cleanUp()
}

// writeRandomBatch creates a random batch with numHeaders headers and writes it
// to the given header index in one update transaction.
func writeRandomBatch(hIndex *headerIndex, numHeaders uint32) (headerBatch,
	map[uint32]headerEntry, error) {

	headerEntries := make(headerBatch, numHeaders)
	headerIndex := make(map[uint32]headerEntry)
	for i := uint32(0); i < numHeaders; i++ {
		var header headerEntry
		if _, err := rand.Read(header.hash[:]); err != nil {
			return nil, nil, fmt.Errorf("unable to read header: %v",
				err)
		}
		header.height = i

		headerEntries[i] = header
		headerIndex[i] = header
	}

	// With the headers constructed, we'll write them to disk in a single
	// batch.
	if err := hIndex.addHeaders(headerEntries); err != nil {
		return nil, nil, fmt.Errorf("unable to add headers: %v", err)
	}

	return headerEntries, headerIndex, nil
}
