package headerfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	"github.com/stretchr/testify/require"
)

// Block headers for testing captured from simnet network.
var blockHdrs = []string{
	"010000000000000000000000000000000000000000000000000000000000" +
		"0000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3" +
		"888a51323a9fb8aa4b1e5e4a45068653ffff7f2002000000",
	"00000020f67ad7695d9b662a72ff3d8edbbb2de0bfa67b13974bb9910d11" +
		"6d5cbd863e68c552826d121f12fcb288895d9488d189891ce0a6" +
		"5a56193ea2ff3d4b99eabb875fac5a68ffff7f2003000000",
	"000000200582f786cda8187a3bb13c044a70f11a5f299cbdb55dd43744a2" +
		"de24cef76a72964688cc27da9f45261b8c35b00edea462f26469" +
		"67fcb6052063d0140a1275de60ac5a68ffff7f2001000000",
	"00000020f83e8ae2309315ff0a36646e2d43e7aa777b7aaa1eadb4876073" +
		"e7a8dac11c1dc3a5e71065b6ab83ed8972d277de2670ceed1fc4" +
		"3fd03f066cc84047d95eeaa360ac5a68ffff7f2002000000",
	"000000203513820c27ba7b218bb6732e851ef404986f299f44b4275334d5" +
		"eab0db09710835f6fc14632ebb23e141f680ae6aec6bdf76557b" +
		"46daf1b4c0160631d89e1ac461ac5a68ffff7f2000000000",
}

// Filter headers for testing captured from simnet network.
//
// nolint:unused
var filterHdrs = []string{
	"b2ef0f5c5d790832d79fc9c9a7b3cef02dd94f143c63feba9d836248cad6" +
		"24cf",
	"b14a448b043b12401327695318318bbb53ec955e1e7963e3fd569a450448" +
		"9177",
	"75ae9eebc6e956fcb4fa00853aec5f252cf0046ed03587feece580386a6c" +
		"d113",
	"f99cbb96ca78c36c741b3765d78b22f0c1039add8afa6d2f6284b5cd6ab9" +
		"d8d6",
	"33e95706f9580a84e2cb167faf2239079805113cc7d3aaefff194b1ce6e6" +
		"a26c",
}

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
	// In this test we want to exercise the ability of the block header
	// store to recover in the face of a partial batch write (the headers
	// were written, but the index wasn't updated).
	cleanUp, db, tempDir, bhs, err := createTestBlockHeaderStore()
	t.Cleanup(cleanUp)
	require.NoError(t, err)

	// First we'll generate a test header chain of length 10, inserting it
	// into the header store.
	blockHeaders := createTestBlockHeaderChain(10)
	err = bhs.WriteHeaders(blockHeaders...)
	require.NoError(t, err)

	// Next, in order to simulate a partial write, we'll roll back the
	// internal index by 5 blocks all at once.

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
	require.NoError(t, err)

	// Next, we'll re-create the block header store in order to trigger the
	// recovery logic.
	hs, err := NewBlockHeaderStore(tempDir, db, &chaincfg.SimNetParams)
	require.NoError(t, err)

	bhs, ok := hs.(*blockHeaderStore)
	require.True(t, ok)

	// The chain tip of this new instance should be of height 5, and match
	// the 5th to last block header.
	tipHash, tipHeight, err := bhs.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(5), tipHeight)
	prevHeaderHash := blockHeaders[5].BlockHash()
	tipBlockHash := tipHash.BlockHash()
	require.NotEqual(t, tipBlockHash, prevHeaderHash)
}

//nolint:lll
func createTestFilterHeaderStore() (func(), walletdb.DB, string, FilterHeaderStore, error) {
	tempDir, err := os.MkdirTemp("", "store_test")
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
	cleanUp, _, _, fS, err := createTestFilterHeaderStore()
	t.Cleanup(cleanUp)
	require.NoError(t, err)

	fhs, ok := fS.(*filterHeaderStore)
	require.True(t, ok)

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
	cleanUp, db, tempDir, fS, err := createTestFilterHeaderStore()
	t.Cleanup(cleanUp)
	require.NoError(t, err)

	fhs, ok := fS.(*filterHeaderStore)
	require.True(t, ok)

	blockHeaders := createTestFilterHeaderChain(10)

	// We simulate the expected behavior of the block headers being written
	// to disk before the filter headers are.
	err = walletdb.Update(fhs.db, func(tx walletdb.ReadWriteTx) error {
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
	})
	require.NoError(t, err, "unable to pre-load block index")

	// Next, we'll insert the filter header chain itself in to the database.
	err = fhs.WriteHeaders(blockHeaders...)
	require.NoError(t, err)

	// Next, in order to simulate a partial write, we'll roll back the
	// internal index by 5 blocks all at once.

	// Set new tip to be block 4.
	newTip := blockHeaders[4].HeaderHash

	// Create a slice of headers to truncate (headers 5-9).
	headersToTruncate := make([]*chainhash.Hash, 5)
	for i := 0; i < 5; i++ {
		// This gives us indices 5, 6, 7, 8, 9.
		headerIdx := 5 + i
		headerHash := blockHeaders[headerIdx].HeaderHash
		headersToTruncate[i] = &headerHash
	}

	// Truncate all 5 headers at once.
	err = fhs.truncateIndices(&newTip, headersToTruncate, true)
	require.NoError(t, err)

	// Next, we'll re-create the block header store in order to trigger the
	// recovery logic.
	fS, err = NewFilterHeaderStore(
		tempDir, db, RegularFilter, &chaincfg.SimNetParams, nil,
	)
	require.NoError(t, err)

	fhs, ok = fS.(*filterHeaderStore)
	require.True(t, ok)

	// The chain tip of this new instance should be of height 5, and match
	// the 5th to last filter header.
	tipHash, tipHeight, err := fhs.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(5), tipHeight)
	prevHeaderHash := blockHeaders[5].FilterHash
	require.NotEqual(t, tipHash, prevHeaderHash)
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
		cleanUp, db, tempDir, fS, err := createTestFilterHeaderStore()
		require.NoError(t, err)

		fhs, ok := fS.(*filterHeaderStore)
		require.True(t, ok)

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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// We'll start the test by setting up our required
			// dependencies.
			cleanUp, tempDir, db := setup(t)
			defer cleanUp()

			// We'll then re-initialize the filter header store with
			// its expected assertion.
			fhs, err := NewFilterHeaderStore(
				tempDir, db, RegularFilter,
				&chaincfg.SimNetParams, tc.headerAssertion,
			)
			require.NoError(t, err)

			// If the assertion failed, we should expect the tip of
			// the chain to no longer exist as the state should've
			// been removed.
			_, err = fhs.FetchHeaderByHeight(chainTip)
			if tc.shouldRemove {
				_, ok := err.(*ErrHeaderNotFound)
				require.True(
					t, ok, "expected file to be removed",
				)
			} else {
				// When file shouldn't be removed, we expect
				// no error.
				msg := "expected no error when file should " +
					"not be removed"
				require.NoError(t, err, msg)
			}
		})
	}
}

// TODO(roasbeef): combined re-org scenarios

// TestRollbackBlockHeaders tests that we're able to rollback block headers
// successfully from the block header store.
func TestRollbackBlockHeaders(t *testing.T) {
	t.Parallel()
	type Prep struct {
		blockHeaderStore BlockHeaderStore
		cleanup          func()
		err              error
	}
	type Verify struct {
		tc               *testing.T
		blockHeaderStore BlockHeaderStore
		blockStamp       *BlockStamp
	}
	rollbackPrep := func() Prep {
		// Prep target header stores.
		tempDir := t.TempDir()
		c1 := func() {
			os.RemoveAll(tempDir)
		}

		dbPath := filepath.Join(tempDir, "test.db")
		db, err := walletdb.Create(
			"bdb", dbPath, true, time.Second*10,
		)
		cleanup := func() {
			db.Close()
			c1()
		}
		if err != nil {
			return Prep{
				cleanup: cleanup,
				err:     err,
			}
		}

		bHS, err := NewBlockHeaderStore(
			tempDir, db, &chaincfg.SimNetParams,
		)
		if err != nil {
			return Prep{
				cleanup: cleanup,
				err:     err,
			}
		}

		// Prep block headers to write to the target headers store.
		// Ignore the genesis block header since NewBlockHeaderStore
		// already wrote it.
		nBHs := len(blockHdrs)
		blkHdrsToWrite := make([]BlockHeader, nBHs-1)
		for i := 1; i < nBHs; i++ {
			blockHdr := blockHdrs[i]
			h, err := constructBlkHdr(
				blockHdr, uint32(i),
			)
			if err != nil {
				return Prep{
					cleanup: cleanup,
					err:     err,
				}
			}
			blkHdrsToWrite[i-1] = *h
		}

		// Write block headers to the store.
		err = bHS.WriteHeaders(blkHdrsToWrite...)
		if err != nil {
			return Prep{
				cleanup: cleanup,
				err:     err,
			}
		}

		return Prep{
			blockHeaderStore: bHS,
			cleanup:          cleanup,
		}
	}
	testCases := []struct {
		name         string
		nHeaders     uint32
		prep         func() Prep
		verify       func(Verify)
		expectErr    bool
		expectErrMsg string
	}{
		{
			name:      "ErrorOnRollingbackGenesisHeader",
			nHeaders:  uint32(len(blockHdrs)),
			prep:      rollbackPrep,
			verify:    func(Verify) {},
			expectErr: true,
			expectErrMsg: fmt.Sprintf("cannot roll back %d "+
				"headers when chain height is %d",
				len(blockHdrs), len(blockHdrs)-1),
		},
		{
			name:      "ErrorOnRollingbackBeyondGenesisHeader",
			nHeaders:  uint32(len(blockHdrs) + 1),
			prep:      rollbackPrep,
			verify:    func(Verify) {},
			expectErr: true,
			expectErrMsg: fmt.Sprintf("cannot roll back %d "+
				"headers when chain height is %d",
				len(blockHdrs)+1, len(blockHdrs)-1),
		},
		{
			name:     "NoErrorOnRollingbackNoHeaders",
			nHeaders: 0,
			prep:     rollbackPrep,
			verify:   func(Verify) {},
		},
		{
			name:     "RollbackHeadersSuccessfully",
			nHeaders: uint32(len(blockHdrs) - 1),
			prep:     rollbackPrep,
			verify: func(v Verify) {
				// Verify chain tip of the block header store.
				bHS := v.blockHeaderStore
				chainTipB, height, err := bHS.ChainTip()
				require.NoError(t, err)

				// Since we have wrote 4 headers and those
				// rolledback on filter headers write failure,
				// we can expect the chain tip height to be 0.
				require.Equal(v.tc, uint32(0), height)

				// Assert that the known block header at this
				// index matches the retrieved one.
				chainTipBEx, err := constructBlkHdr(
					blockHdrs[0], uint32(0),
				)
				require.NoError(v.tc, err)
				b := chainTipBEx.BlockHeader
				require.Equal(v.tc, b, chainTipB)

				// Assert that the known blockstamp height
				// currently equals the genesis header height.
				require.Equal(t, int32(0), v.blockStamp.Height)

				// Assert that the known blockstamp hash equals
				// the genesis header hash.
				require.Equal(
					t, chainTipBEx.BlockHash(),
					v.blockStamp.Hash,
				)

				// Assert that the know blockstamp timestamp
				// equals the genesis header timestamp.
				require.Equal(
					t, chainTipBEx.Timestamp,
					v.blockStamp.Timestamp,
				)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prep := tc.prep()
			t.Cleanup(prep.cleanup)
			require.NoError(t, prep.err)

			store := prep.blockHeaderStore
			blockStamp, err := store.RollbackBlockHeaders(
				tc.nHeaders,
			)
			verify := Verify{
				tc:               t,
				blockHeaderStore: prep.blockHeaderStore,
				blockStamp:       blockStamp,
			}
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				tc.verify(verify)
				return
			}
			require.NoError(t, err)
			tc.verify(verify)
		})
	}
}

// constructBlkHdr constructs a block header from a hex string and height.
func constructBlkHdr(blockHeaderHex string,
	height uint32) (*BlockHeader, error) {

	buff, err := hex.DecodeString(blockHeaderHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode block header hex: %v",
			err)
	}
	reader := bytes.NewReader(buff)

	// Deserialize block header.
	bH := &BlockHeader{
		BlockHeader: &wire.BlockHeader{},
		Height:      height,
	}
	if err := bH.Deserialize(reader); err != nil {
		return nil, fmt.Errorf("failed to deserialize block "+
			"header: %v", err)
	}

	return bH, nil
}
