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
func createTestBlocks() (wire.MsgBlock, wire.MsgBlock, wire.MsgBlock) {
	// Block 100000.
	prevBlock, _ := chainhash.NewHashFromStr("000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250")
	merkleRoot, _ := chainhash.NewHashFromStr("f3e94742aca4b5ef85488dc37c06c3282295ffec960994b2c0d5ac2a25a95766")

	// Block 100001.
	prevBlock2, _ := chainhash.NewHashFromStr("000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506")
	merkleRoot2, _ := chainhash.NewHashFromStr("7fe79307aeb300d910d9c4bec5bacb4c7e114c7dfd6789e19f3a733debb3bb6a")

	// Block 100002.
	prevBlock3, _ := chainhash.NewHashFromStr("00000000000080b66c911bd5ba14a74260057311eaeb1982802f7010f1a9f090")
	merkleRoot3, _ := chainhash.NewHashFromStr("2fda58e5959b0ee53c5253da9b9f3c0c739422ae04946966991cf55895287552")

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
				Timestamp:  time.Unix(1293623863, 0),
				Bits:       0x1b04864c,
				Nonce:      0x10572b0f,
			},
			Transactions: []*wire.MsgTx{},
		}, wire.MsgBlock{
			Header: wire.BlockHeader{
				Version:    1,
				PrevBlock:  *prevBlock3,
				MerkleRoot: *merkleRoot3,
				Timestamp:  time.Unix(1293653204, 0),
				Bits:       0x1b04864c,
				Nonce:      0x9bcc8940,
			},
			Transactions: []*wire.MsgTx{
				{
					Version: 1,
					TxIn: []*wire.TxIn{
						{
							PreviousOutPoint: wire.OutPoint{
								Hash:  chainhash.Hash{},
								Index: 0xffffffff,
							},
							SignatureScript: []byte{
								0x04, 0xff, 0xff, 0x00, 0x1d, 0x01, 0x04,
							},
							Sequence: 0xffffffff,
						},
					},
					TxOut: []*wire.TxOut{
						{
							Value: 0x12a05f200,
							PkScript: []byte{
								0x41, // OP_DATA_65
								0x04, 0x96, 0xb5, 0x38, 0xe8, 0x53, 0x51, 0x9c,
								0x72, 0x6a, 0x2c, 0x91, 0xe6, 0x1e, 0xc1, 0x16,
								0x00, 0xae, 0x13, 0x90, 0x81, 0x3a, 0x62, 0x7c,
								0x66, 0xfb, 0x8b, 0xe7, 0x94, 0x7b, 0xe6, 0x3c,
								0x52, 0xda, 0x75, 0x89, 0x37, 0x95, 0x15, 0xd4,
								0xe0, 0xa6, 0x04, 0xf8, 0x14, 0x17, 0x81, 0xe6,
								0x22, 0x94, 0x72, 0x11, 0x66, 0xbf, 0x62, 0x1e,
								0x73, 0xa8, 0x2c, 0xbf, 0x23, 0x42, 0xc8, 0x58,
								0xee, // 65-byte signature
								0xac, // OP_CHECKSIG
							},
						},
					},
					LockTime: 0,
				},
			},
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

	blockDB, err := New(tempDir, capacity, headers)
	if err != nil {
		return nil, nil, nil, err
	}

	cleanUp := func() {
		db.Close()
		blockDB.Close()
		os.RemoveAll(tempDir)
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

	blockA, _, _ := createTestBlocks()

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

	blockA, _, _ := createTestBlocks()

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
	blockA, blockB, _ := createTestBlocks()

	// Set the cache to have a capacity the size of blockA.
	cleanUp, database, headers, err := createTestDatabase(blockA.SerializeSize())
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

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

// Test that once the cache is at capacity then the oldest blocks are evicted.
func TestEvictionOfMultipleBlocks(t *testing.T) {
	blockA, blockB, blockC := createTestBlocks()

	// Set the cache to have a capacity the size of blockC, this will cause the
	// eviction of any other blocks.
	cleanUp, database, headers, err := createTestDatabase(blockC.SerializeSize())
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

	// Ensure we can look up the height from the hash.
	headers.WriteHeaders(headerfs.BlockHeader{
		BlockHeader: &blockA.Header,
		Height:      100000,
	}, headerfs.BlockHeader{
		BlockHeader: &blockB.Header,
		Height:      100001,
	}, headerfs.BlockHeader{
		BlockHeader: &blockC.Header,
		Height:      100002,
	})

	// Persist the first blocks.
	err = database.PutBlock(&blockA)
	if err != nil {
		t.Fatalf("unable to store block: %v", err)
	}
	err = database.PutBlock(&blockB)
	if err != nil {
		t.Fatalf("unable to store block: %v", err)
	}

	// Expect that this insertion causes the eviction of the first blocks.
	err = database.PutBlock(&blockC)
	if err != nil {
		t.Fatalf("unable to store block: %v", err)
	}

	hash := blockA.BlockHash()
	_, err = database.FetchBlock(&hash)
	if err == nil {
		t.Fatal("older block not evicted")
	}
	hash = blockB.BlockHash()
	_, err = database.FetchBlock(&hash)
	if err == nil {
		t.Fatal("older block not evicted")
	}

	hash = blockC.BlockHash()
	_, err = database.FetchBlock(&hash)
	if err != nil {
		t.Fatalf("could not retrieve newer block: %v", err)
	}
}
