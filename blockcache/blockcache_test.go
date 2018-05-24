package blockcache

import (
	"github.com/roasbeef/btcwallet/walletdb"
	"io/ioutil"

	"os"
	"reflect"
	"testing"

	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcd/wire"
	_ "github.com/roasbeef/btcwallet/walletdb/bdb"
	"time"
)

// Create a test block with only the necessary information for these tests.
func createTestBlock() wire.MsgBlock {
	prevBlock, _ := chainhash.NewHashFromStr("000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250")
	merkleRoot, _ := chainhash.NewHashFromStr("f3e94742aca4b5ef85488dc37c06c3282295ffec960994b2c0d5ac2a25a95766")

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
	}
}

func createTestDatabase(capacity int) (func(), BlockCache, error) {
	tempDir, err := ioutil.TempDir("", "neutrino")
	if err != nil {
		return nil, nil, err
	}

	db, err := walletdb.Create("bdb", tempDir+"/test.db")
	if err != nil {
		return nil, nil, err
	}

	blockDB, err := New(tempDir, capacity)
	if err != nil {
		return nil, nil, err
	}

	cleanUp := func() {
		db.Close()
		blockDB.Close()
		os.RemoveAll(tempDir)
	}

	return cleanUp, blockDB, nil
}

// Test that any block that's persisted is retrieved intact and exactly the
// same.
func TestRetrievesSameBlockData(t *testing.T) {
	cleanUp, database, err := createTestDatabase(DefaultCapacity)
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

	blockA := createTestBlock()

	block := &blockA
	hash := blockA.BlockHash()

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
	cleanUp, database, err := createTestDatabase(DefaultCapacity)
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

	blockA := createTestBlock()

	hash := blockA.BlockHash()

	_, err = database.FetchBlock(&hash)
	if err != ErrBlockNotFound {
		t.Fatalf("expected not found")
	}
}
