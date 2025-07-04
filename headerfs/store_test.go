package headerfs

import (
	"bytes"
	"crypto/sha256"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/davecgh/go-spew/spew"
)

func createTestBlockHeaderStore() (func(), walletdb.DB, string,
	*blockHeaderStore, error) {

	tempDir, err := ioutil.TempDir("", "store_test")
	if err != nil {
		return nil, nil, "", nil, err
	}

	dbPath := filepath.Join(tempDir, "test.db")
	db, err := walletdb.Create(
		"bdb", dbPath, true, time.Second*10,
	)
	if err != nil {
		return nil, nil, "", nil, err
	}

	hStore, err := NewBlockHeaderStore(tempDir, db, &chaincfg.SimNetParams)
	if err != nil {
		return nil, nil, "", nil, err
	}

	cleanUp := func() {
		os.RemoveAll(tempDir)
		db.Close()
	}

	return cleanUp, db, tempDir, hStore.(*blockHeaderStore), nil
}

func createTestBlockHeaderChain(numHeaders uint32) []BlockHeader {
	blockHeaders := make([]BlockHeader, numHeaders)
	prevHeader := &chaincfg.SimNetParams.GenesisBlock.Header
	for i := uint32(1); i <= numHeaders; i++ {
		bitcoinHeader := &wire.BlockHeader{
			Bits:      uint32(rand.Int31()),
			Nonce:     uint32(rand.Int31()),
			Timestamp: prevHeader.Timestamp.Add(time.Minute * 1),
			PrevBlock: prevHeader.BlockHash(),
		}

		blockHeaders[i-1] = BlockHeader{
			BlockHeader: bitcoinHeader,
			Height:      i,
		}

		prevHeader = bitcoinHeader
	}

	return blockHeaders
}

func TestBlockHeaderStoreOperations(t *testing.T) {
	cleanUp, _, _, bhs, err := createTestBlockHeaderStore()
	if cleanUp != nil {
		defer cleanUp()
	}
	if err != nil {
		t.Fatalf("unable to create new block header store: %v", err)
	}

	rand.Seed(time.Now().Unix())

	// With our test instance created, we'll now generate a series of
	// "fake" block headers to insert into the database.
	const numHeaders = 100
	blockHeaders := createTestBlockHeaderChain(numHeaders)

	// With all the headers inserted, we'll now insert them into the
	// database in a single batch.
	if err := bhs.WriteHeaders(blockHeaders...); err != nil {
		t.Fatalf("unable to write block headers: %v", err)
	}

	// At this point, the _tip_ of the chain from the PoV of the database
	// should be the very last header we inserted.
	lastHeader := blockHeaders[len(blockHeaders)-1]
	tipHeader, tipHeight, err := bhs.ChainTip()
	if err != nil {
		t.Fatalf("unable to fetch chain tip")
	}
	if !reflect.DeepEqual(lastHeader.BlockHeader, tipHeader) {
		t.Fatalf("tip height headers don't match up: "+
			"expected %v, got %v", spew.Sdump(lastHeader),
			spew.Sdump(tipHeader))
	}
	if tipHeight != lastHeader.Height {
		t.Fatalf("chain tip doesn't match: expected %v, got %v",
			lastHeader.Height, tipHeight)
	}

	// Ensure that from the PoV of the database, the headers perfectly
	// connect.
	if err := bhs.CheckConnectivity(); err != nil {
		t.Fatalf("bhs detects that headers don't connect: %v", err)
	}

	// With all the headers written, we should be able to retrieve each
	// header according to its hash _and_ height.
	for _, header := range blockHeaders {
		dbHeader, err := bhs.FetchHeaderByHeight(header.Height)
		if err != nil {
			t.Fatalf("unable to fetch header by height: %v", err)
		}
		if !reflect.DeepEqual(*header.BlockHeader, *dbHeader) {
			t.Fatalf("retrieved by height headers don't match up: "+
				"expected %v, got %v", spew.Sdump(*header.BlockHeader),
				spew.Sdump(*dbHeader))
		}

		blockHash := header.BlockHash()
		dbHeader, _, err = bhs.FetchHeader(&blockHash)
		if err != nil {
			t.Fatalf("unable to fetch header by hash: %v", err)
		}
		if !reflect.DeepEqual(*dbHeader, *header.BlockHeader) {
			t.Fatalf("retrieved by hash headers don't match up: "+
				"expected %v, got %v", spew.Sdump(header),
				spew.Sdump(dbHeader))
		}
	}

	// Finally, we'll test the roll back scenario. Roll back the chain by a
	// single block, the returned block stamp should exactly match the last
	// header inserted, and the current chain tip should be the second to
	// last header inserted.
	secondToLastHeader := blockHeaders[len(blockHeaders)-2]
	blockStamp, err := bhs.RollbackLastBlock()
	if err != nil {
		t.Fatalf("unable to rollback chain: %v", err)
	}
	if secondToLastHeader.Height != uint32(blockStamp.Height) {
		t.Fatalf("chain tip doesn't match: expected %v, got %v",
			secondToLastHeader.Height, blockStamp.Height)
	}
	headerHash := secondToLastHeader.BlockHash()
	if !bytes.Equal(headerHash[:], blockStamp.Hash[:]) {
		t.Fatalf("header hashes don't match: expected %v, got %v",
			headerHash, blockStamp.Hash)
	}
	tipHeader, tipHeight, err = bhs.ChainTip()
	if err != nil {
		t.Fatalf("unable to fetch chain tip")
	}
	if !reflect.DeepEqual(secondToLastHeader.BlockHeader, tipHeader) {
		t.Fatalf("tip height headers don't match up: "+
			"expected %v, got %v", spew.Sdump(secondToLastHeader),
			spew.Sdump(tipHeader))
	}
	if tipHeight != secondToLastHeader.Height {
		t.Fatalf("chain tip doesn't match: expected %v, got %v",
			secondToLastHeader.Height, tipHeight)
	}
}

// TestBlockHeaderStoreRecovery tests the block header store's ability to
// recover from a partial write scenario. It simulates a situation where headers
// were written to the database but the index wasn't fully updated
// (which could happen if the system crashes during an update). The test writes
// 10 headers, then intentionally corrupts the database by rolling back the
// index by 5 blocks all at once. It then recreates the header store and
// verifies that the recovery logic correctly detects the inconsistency and
// restores the index to match the last properly indexed header.
func TestBlockHeaderStoreRecovery(t *testing.T) {
	cleanUp, db, tempDir, bhs, err := createTestBlockHeaderStore()
	if cleanUp != nil {
		defer cleanUp()
	}
	if err != nil {
		t.Fatalf("unable to create new block header store: %v", err)
	}

	// First we'll generate a test header chain of length 10, inserting it
	// into the header store.
	blockHeaders := createTestBlockHeaderChain(10)
	if err := bhs.WriteHeaders(blockHeaders...); err != nil {
		t.Fatalf("unable to write block headers: %v", err)
	}

	// Next, in order to simulate a partial write, we'll roll back the
	// internal index by 5 blocks.
	// Set new tip to be block 4 (height 4).
	newTip := blockHeaders[4].BlockHash()
	headersToTruncate := make([]*chainhash.Hash, 5)
	for i := 0; i < 5; i++ {
		// Get headers 5, 6, 7, 8, 9 to truncate.
		headerIdx := len(blockHeaders) - i - 1
		headerHash := blockHeaders[headerIdx].BlockHash()
		headersToTruncate[i] = &headerHash
	}

	// Truncate all 5 headers at once.
	err = bhs.truncateIndices(&newTip, headersToTruncate, true)
	if err != nil {
		t.Fatalf("unable to truncate indices: %v", err)
	}

	// Next, we'll re-create the block header store in order to trigger the
	// recovery logic.
	hs, err := NewBlockHeaderStore(tempDir, db, &chaincfg.SimNetParams)
	if err != nil {
		t.Fatalf("unable to re-create bhs: %v", err)
	}
	bhs = hs.(*blockHeaderStore)

	// The chain tip of this new instance should be of height 5, and match
	// the 5th to last block header.
	tipHash, tipHeight, err := bhs.ChainTip()
	if err != nil {
		t.Fatalf("unable to get chain tip: %v", err)
	}
	if tipHeight != 5 {
		t.Fatalf("tip height mismatch: expected %v, got %v", 5, tipHeight)
	}
	prevHeaderHash := blockHeaders[5].BlockHash()
	tipBlockHash := tipHash.BlockHash()
	if bytes.Equal(prevHeaderHash[:], tipBlockHash[:]) {
		t.Fatalf("block hash mismatch: expected %v, got %v",
			prevHeaderHash, tipBlockHash)
	}
}

func createTestFilterHeaderStore() (func(), walletdb.DB, string, FilterHeaderStore, error) {
	tempDir, err := ioutil.TempDir("", "store_test")
	if err != nil {
		return nil, nil, "", nil, err
	}

	dbPath := filepath.Join(tempDir, "test.db")
	db, err := walletdb.Create("bdb", dbPath, true, time.Second*10)
	if err != nil {
		return nil, nil, "", nil, err
	}

	hStore, err := NewFilterHeaderStore(
		tempDir, db, RegularFilter, &chaincfg.SimNetParams, nil,
	)
	if err != nil {
		return nil, nil, "", nil, err
	}

	cleanUp := func() {
		os.RemoveAll(tempDir)
		db.Close()
	}

	return cleanUp, db, tempDir, hStore, nil
}

func createTestFilterHeaderChain(numHeaders uint32) []FilterHeader {
	filterHeaders := make([]FilterHeader, numHeaders)
	for i := uint32(1); i <= numHeaders; i++ {
		filterHeaders[i-1] = FilterHeader{
			HeaderHash: chainhash.DoubleHashH([]byte{byte(i)}),
			FilterHash: sha256.Sum256([]byte{byte(i)}),
			Height:     i,
		}
	}

	return filterHeaders
}

func TestFilterHeaderStoreOperations(t *testing.T) {
	cleanUp, _, _, fhsVal, err := createTestFilterHeaderStore()
	if cleanUp != nil {
		defer cleanUp()
	}
	if err != nil {
		t.Fatalf("unable to create new block header store: %v", err)
	}

	// Type assertion to convert back to concrete *filterHeaderStore type.
	fhs, ok := fhsVal.(*filterHeaderStore)
	if !ok {
		t.Fatal("filter header interface value is not of type " +
			"*filterHeaderStore")
	}

	rand.Seed(time.Now().Unix())

	// With our test instance created, we'll now generate a series of
	// "fake" filter headers to insert into the database.
	const numHeaders = 100
	blockHeaders := createTestFilterHeaderChain(numHeaders)

	// We simulate the expected behavior of the block headers being written
	// to disk before the filter headers are.
	if err := walletdb.Update(fhs.db, func(tx walletdb.ReadWriteTx) error {
		rootBucket := tx.ReadWriteBucket(indexBucket)

		for _, header := range blockHeaders {
			entry := headerEntry{
				hash:   header.HeaderHash,
				height: header.Height,
			}
			if err := putHeaderEntry(rootBucket, entry); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("unable to pre-load block index: %v", err)
	}

	// With all the headers inserted, we'll now insert them into the
	// database in a single batch.
	if err := fhs.WriteHeaders(blockHeaders...); err != nil {
		t.Fatalf("unable to write block headers: %v", err)
	}

	// At this point, the _tip_ of the chain from the PoV of the database
	// should be the very last header we inserted.
	lastHeader := blockHeaders[len(blockHeaders)-1]
	tipHeader, tipHeight, err := fhs.ChainTip()
	if err != nil {
		t.Fatalf("unable to fetch chain tip")
	}
	if !bytes.Equal(lastHeader.FilterHash[:], tipHeader[:]) {
		t.Fatalf("tip height headers don't match up: "+
			"expected %v, got %v", lastHeader, tipHeader)
	}
	if tipHeight != lastHeader.Height {
		t.Fatalf("chain tip doesn't match: expected %v, got %v",
			lastHeader.Height, tipHeight)
	}

	// With all the headers written, we should be able to retrieve each
	// header according to its hash _and_ height.
	for _, header := range blockHeaders {
		dbHeader, err := fhs.FetchHeaderByHeight(header.Height)
		if err != nil {
			t.Fatalf("unable to fetch header by height: %v", err)
		}
		if !bytes.Equal(header.FilterHash[:], dbHeader[:]) {
			t.Fatalf("retrieved by height headers don't match up: "+
				"expected %v, got %v", header.FilterHash,
				dbHeader)
		}

		blockHash := header.HeaderHash
		dbHeader, err = fhs.FetchHeader(&blockHash)
		if err != nil {
			t.Fatalf("unable to fetch header by hash: %v", err)
		}
		if !bytes.Equal(dbHeader[:], header.FilterHash[:]) {
			t.Fatalf("retrieved by hash headers don't match up: "+
				"expected %v, got %v", spew.Sdump(header),
				spew.Sdump(dbHeader))
		}
	}

	// Finally, we'll test the roll back scenario. Roll back the chain by a
	// single block, the returned block stamp should exactly match the last
	// header inserted, and the current chain tip should be the second to
	// last header inserted.
	secondToLastHeader := blockHeaders[len(blockHeaders)-2]
	blockStamp, err := fhs.RollbackLastBlock(&secondToLastHeader.HeaderHash)
	if err != nil {
		t.Fatalf("unable to rollback chain: %v", err)
	}
	if secondToLastHeader.Height != uint32(blockStamp.Height) {
		t.Fatalf("chain tip doesn't match: expected %v, got %v",
			secondToLastHeader.Height, blockStamp.Height)
	}
	if !bytes.Equal(secondToLastHeader.FilterHash[:], blockStamp.Hash[:]) {
		t.Fatalf("header hashes don't match: expected %v, got %v",
			secondToLastHeader.FilterHash, blockStamp.Hash)
	}
	tipHeader, tipHeight, err = fhs.ChainTip()
	if err != nil {
		t.Fatalf("unable to fetch chain tip")
	}
	if !bytes.Equal(secondToLastHeader.FilterHash[:], tipHeader[:]) {
		t.Fatalf("tip height headers don't match up: "+
			"expected %v, got %v", spew.Sdump(secondToLastHeader),
			spew.Sdump(tipHeader))
	}
	if tipHeight != secondToLastHeader.Height {
		t.Fatalf("chain tip doesn't match: expected %v, got %v",
			secondToLastHeader.Height, tipHeight)
	}
}

// TestFilterHeaderStoreRecovery tests the filter header store's ability to
// recover from a partial write scenario. It simulates a situation where headers
// were written to the database but the index wasn't fully updated
// (which could happen if the system crashes during an update). The test writes
// 10 headers, then intentionally corrupts the database by rolling back the
// index by 5 blocks all at once. It then recreates the header store and
// verifies that the recovery logic correctly detects the inconsistency and
// restores the index to match the last properly indexed header.
func TestFilterHeaderStoreRecovery(t *testing.T) {
	// In this test we want to exercise the ability of the filter header
	// store to recover in the face of a partial batch write (the headers
	// were written, but the index wasn't updated).
	cleanUp, db, tempDir, fhsVal, err := createTestFilterHeaderStore()
	if cleanUp != nil {
		defer cleanUp()
	}
	if err != nil {
		t.Fatalf("unable to create new block header store: %v", err)
	}

	// Type assertion to convert back to concrete *filterHeaderStore type.
	fhs, ok := fhsVal.(*filterHeaderStore)
	if !ok {
		t.Fatal("filter header interface value is not of type " +
			"*filterHeaderStore")
	}

	blockHeaders := createTestFilterHeaderChain(10)

	// We simulate the expected behavior of the block headers being written
	// to disk before the filter headers are.
	if err := walletdb.Update(fhs.db, func(tx walletdb.ReadWriteTx) error {
		rootBucket := tx.ReadWriteBucket(indexBucket)

		for _, header := range blockHeaders {
			entry := headerEntry{
				hash:   header.HeaderHash,
				height: header.Height,
			}
			if err := putHeaderEntry(rootBucket, entry); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("unable to pre-load block index: %v", err)
	}

	// Next, we'll insert the filter header chain itself in to the
	// database.
	if err := fhs.WriteHeaders(blockHeaders...); err != nil {
		t.Fatalf("unable to write block headers: %v", err)
	}

	// Next, in order to simulate a partial write, we'll roll back the
	// internal index by 5 blocks all at once.
	newTip := blockHeaders[4].HeaderHash // Set new tip to be block 4

	// Create a slice of headers to truncate (headers 5-9).
	headersToTruncate := make([]*chainhash.Hash, 5)
	for i := 0; i < 5; i++ {
		headerIdx := 5 + i // This gives us indices 5, 6, 7, 8, 9
		headerHash := blockHeaders[headerIdx].HeaderHash
		headersToTruncate[i] = &headerHash
	}

	// Truncate all 5 headers at once.
	err = fhs.truncateIndices(&newTip, headersToTruncate, true)
	if err != nil {
		t.Fatalf("unable to truncate indices: %v", err)
	}

	// Next, we'll re-create the block header store in order to trigger the
	// recovery logic.
	fhsVal, err = NewFilterHeaderStore(
		tempDir, db, RegularFilter, &chaincfg.SimNetParams, nil,
	)
	if err != nil {
		t.Fatalf("unable to re-create bhs: %v", err)
	}

	// The chain tip of this new instance should be of height 5, and match
	// the 5th to last filter header.
	tipHash, tipHeight, err := fhs.ChainTip()
	if err != nil {
		t.Fatalf("unable to get chain tip: %v", err)
	}
	if tipHeight != 5 {
		t.Fatalf("tip height mismatch: expected %v, got %v", 5, tipHeight)
	}
	prevHeaderHash := blockHeaders[5].FilterHash
	if bytes.Equal(prevHeaderHash[:], tipHash[:]) {
		t.Fatalf("block hash mismatch: expected %v, got %v",
			prevHeaderHash, tipHash[:])
	}
}

// TestBlockHeadersFetchHeaderAncestors tests that we're able to properly fetch
// the ancestors of a particular block, going from a set distance back to the
// target block.
func TestBlockHeadersFetchHeaderAncestors(t *testing.T) {
	t.Parallel()

	cleanUp, _, _, bhs, err := createTestBlockHeaderStore()
	if cleanUp != nil {
		defer cleanUp()
	}
	if err != nil {
		t.Fatalf("unable to create new block header store: %v", err)
	}

	rand.Seed(time.Now().Unix())

	// With our test instance created, we'll now generate a series of
	// "fake" block headers to insert into the database.
	const numHeaders = 100
	blockHeaders := createTestBlockHeaderChain(numHeaders)

	// With all the headers inserted, we'll now insert them into the
	// database in a single batch.
	if err := bhs.WriteHeaders(blockHeaders...); err != nil {
		t.Fatalf("unable to write block headers: %v", err)
	}

	// Now that the headers have been written to disk, we'll attempt to
	// query for all the ancestors of the final header written, to query
	// the entire range.
	lastHeader := blockHeaders[numHeaders-1]
	lastHash := lastHeader.BlockHash()
	diskHeaders, startHeight, err := bhs.FetchHeaderAncestors(
		numHeaders-1, &lastHash,
	)
	if err != nil {
		t.Fatalf("unable to fetch headers: %v", err)
	}

	// Ensure that the first height of the block is height 1, and not the
	// genesis block.
	if startHeight != 1 {
		t.Fatalf("expected start height of %v got %v", 1, startHeight)
	}

	// Ensure that we retrieve the correct number of headers.
	if len(diskHeaders) != numHeaders {
		t.Fatalf("expected %v headers got %v headers",
			numHeaders, len(diskHeaders))
	}

	// We should get back the exact same set of headers that we inserted in
	// the first place.
	for i := 0; i < len(diskHeaders); i++ {
		diskHeader := diskHeaders[i]
		blockHeader := blockHeaders[i].BlockHeader
		if !reflect.DeepEqual(diskHeader, *blockHeader) {
			t.Fatalf("header mismatch, expected %v got %v",
				spew.Sdump(blockHeader), spew.Sdump(diskHeader))
		}
	}
}

// TestFilterHeaderStateAssertion tests that we'll properly delete or not
// delete the current on disk filter header state if a headerStateAssertion is
// passed in during initialization.
func TestFilterHeaderStateAssertion(t *testing.T) {
	t.Parallel()

	const chainTip = 10
	filterHeaderChain := createTestFilterHeaderChain(chainTip)

	setup := func(t *testing.T) (func(), string, walletdb.DB) {
		cleanUp, db, tempDir, fhsVal, err := createTestFilterHeaderStore()
		if err != nil {
			t.Fatalf("unable to create new filter header store: %v",
				err)
		}

		// Type assertion to convert back to concrete *filterHeaderStore
		// type.
		fhs, ok := fhsVal.(*filterHeaderStore)
		if !ok {
			t.Fatal("filter header interface value is not of " +
				"type *filterHeaderStore")
		}

		// We simulate the expected behavior of the block headers being
		// written to disk before the filter headers are.
		if err := walletdb.Update(fhs.db, func(tx walletdb.ReadWriteTx) error {
			rootBucket := tx.ReadWriteBucket(indexBucket)

			for _, header := range filterHeaderChain {
				entry := headerEntry{
					hash:   header.HeaderHash,
					height: header.Height,
				}
				err := putHeaderEntry(rootBucket, entry)
				if err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			cleanUp()
			t.Fatalf("unable to pre-load block index: %v", err)
		}

		// Next we'll also write the chain to the flat file we'll make
		// our assertions against it.
		if err := fhs.WriteHeaders(filterHeaderChain...); err != nil {
			cleanUp()
			t.Fatalf("unable to write filter headers: %v", err)
		}

		return cleanUp, tempDir, db
	}

	testCases := []struct {
		name            string
		headerAssertion *FilterHeader
		shouldRemove    bool
	}{
		// A header that we know already to be in our local chain. It
		// shouldn't remove the state.
		{
			name:            "correct assertion",
			headerAssertion: &filterHeaderChain[3],
			shouldRemove:    false,
		},

		// A made up header that isn't in the chain. It should remove
		// all state.
		{
			name: "incorrect assertion",
			headerAssertion: &FilterHeader{
				Height: 5,
			},
			shouldRemove: true,
		},

		// A header that's well beyond the chain height, it should be a
		// noop.
		{
			name: "assertion not found",
			headerAssertion: &FilterHeader{
				Height: 500,
			},
			shouldRemove: false,
		},
	}

	for _, testCase := range testCases {
		success := t.Run(testCase.name, func(t *testing.T) {
			// We'll start the test by setting up our required
			// dependencies.
			cleanUp, tempDir, db := setup(t)
			defer cleanUp()

			// We'll then re-initialize the filter header store with
			// its expected assertion.
			fhs, err := NewFilterHeaderStore(
				tempDir, db, RegularFilter, &chaincfg.SimNetParams,
				testCase.headerAssertion,
			)
			if err != nil {
				t.Fatalf("unable to make new fhs: %v", err)
			}

			// If the assertion failed, we should expect the tip of
			// the chain to no longer exist as the state should've
			// been removed.
			_, err = fhs.FetchHeaderByHeight(chainTip)
			if testCase.shouldRemove {
				if _, ok := err.(*ErrHeaderNotFound); !ok {
					t.Fatal("expected file to be removed")
				}
			}
			if !testCase.shouldRemove && err != nil {
				t.Fatal("expected file to not be removed")
			}
		})
		if !success {
			break
		}
	}
}

// TODO(roasbeef): combined re-org scenarios
