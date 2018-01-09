package blockcache

import (
	"github.com/roasbeef/btcwallet/walletdb"
	"io/ioutil"

	"os"
	"reflect"
	"testing"

	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/roasbeef/btcd/chaincfg"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcd/wire"
	_ "github.com/roasbeef/btcwallet/walletdb/bdb"
	"time"
)

// Create two test blocks with only the necessary information for these tests.
func createTestBlocks() (wire.MsgBlock, wire.MsgBlock) {
	// Block 100000.
	prevBlock, _ := chainhash.NewHashFromStr("000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250")
	merkleRoot, _ := chainhash.NewHashFromStr("f3e94742aca4b5ef85488dc37c06c3282295ffec960994b2c0d5ac2a25a95766")

	// Block 100001.
	prevBlock2, _ := chainhash.NewHashFromStr("000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506")
	merkleRoot2, _ := chainhash.NewHashFromStr("7fe79307aeb300d910d9c4bec5bacb4c7e114c7dfd6789e19f3a733debb3bb6a")

	return wire.MsgBlock{
			Header: wire.BlockHeader{
				Version:    1,
				PrevBlock:  *prevBlock,
				MerkleRoot: *merkleRoot,
				Timestamp:  time.Unix(1293623863, 0),
				Bits:       0x1b04864c,
				Nonce:      0x10572b0f,
			},
			Transactions: []*wire.MsgTx{},
		}, wire.MsgBlock{
			Header: wire.BlockHeader{
				Version:    1,
				PrevBlock:  *prevBlock2,
				MerkleRoot: *merkleRoot2,
				Timestamp:  time.Unix(1293653204, 0),
				Bits:       0x1b04864c,
				Nonce:      0x9bcc8940,
			},
			Transactions: []*wire.MsgTx{},
		}
}

func createTestDatabase(capacity int) (func(), BlockCache,
	*headerfs.BlockHeaderStore, error) {
	tempDir, err := ioutil.TempDir("", "neutrino")
	if err != nil {
		return nil, nil, nil, err
	}

	db, err := walletdb.Create("bdb", tempDir+"/test.db")
	if err != nil {
		return nil, nil, nil, err
	}

	headers, err := headerfs.NewBlockHeaderStore(tempDir, db,
		&chaincfg.MainNetParams)

	cleanUp := func() {
		os.RemoveAll(tempDir)
		db.Close()
	}

	blockDB, err := New(tempDir, capacity, headers)
	if err != nil {
		return nil, nil, nil, err
	}

	return cleanUp, blockDB, headers, nil
}

// Test that any block that's persisted is retrieved intact and exactly the
// same.
func TestRetrievesSameBlockData(t *testing.T) {
	cleanUp, database, headers, err := createTestDatabase(DefaultCapacity)
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

	blockA, _ := createTestBlocks()

	block := &blockA
	hash := blockA.BlockHash()

	// Ensure we can look up the height from the hash.
	headers.WriteHeaders(headerfs.BlockHeader{
		BlockHeader: &blockA.Header,
		Height:      100000,
	})

	err = database.PutBlock(block)
	if err != nil {
		t.Fatalf("unable to store block: %v", err)
	}

	// Ensure that the block we just persisted can be fetched,
	// and that it's content is exactly the same.
	retrievedBlock, err := database.FetchBlock(&hash)
	if err != nil {
		t.Fatalf("unable to retrieve block: %v", err)
	}
	if !reflect.DeepEqual(block, retrievedBlock) {
		t.Fatalf("block content doesn't match!")
	}
}

// Test that if the block does not exist in the cache then an
// ErrBlockNotFound is returned.
func TestNotFoundIfBlockDoesntExist(t *testing.T) {
	cleanUp, database, headers, err := createTestDatabase(DefaultCapacity)
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

	blockA, _ := createTestBlocks()

	// Ensure we can look up the height from the hash.
	headers.WriteHeaders(headerfs.BlockHeader{
		BlockHeader: &blockA.Header,
		Height:      100000,
	})

	hash := blockA.BlockHash()

	_, err = database.FetchBlock(&hash)
	if err != ErrBlockNotFound {
		t.Fatalf("expected not found")
	}
}

// Test that once the cache is at capacity then the oldest block is evicted.
func TestEviction(t *testing.T) {
	// Set the cache to have a capacity of a single element.
	cleanUp, database, headers, err := createTestDatabase(1)
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

	blockA, blockB := createTestBlocks()

	// Ensure we can look up the height from the hash.
	headers.WriteHeaders(headerfs.BlockHeader{
		BlockHeader: &blockA.Header,
		Height:      100000,
	}, headerfs.BlockHeader{
		BlockHeader: &blockB.Header,
		Height:      100001,
	})

	// Persist the first block.
	err = database.PutBlock(&blockA)
	if err != nil {
		t.Fatalf("unable to store block: %v", err)
	}

	// Expect that this insertion causes the eviction of the first block.
	err = database.PutBlock(&blockB)
	if err != nil {
		t.Fatalf("unable to store block: %v", err)
	}

	hash := blockA.BlockHash()
	_, err = database.FetchBlock(&hash)
	if err == nil {
		t.Fatal("older block not evicted")
	}

	block2Hash := blockB.BlockHash()
	_, err = database.FetchBlock(&block2Hash)
	if err != nil {
		t.Fatalf("could not retrieve newer block: %v", err)
	}
}
