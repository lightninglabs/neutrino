package filterdb

import (
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/roasbeef/btcd/chaincfg"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcutil/gcs"
	"github.com/roasbeef/btcwallet/walletdb"
	_ "github.com/roasbeef/btcwallet/walletdb/bdb"
)

func createTestDatabase() (func(), FilterDatabase, error) {
	tempDir, err := ioutil.TempDir("", "neutrino")
	if err != nil {
		return nil, nil, err
	}

	db, err := walletdb.Create("bdb", tempDir+"/test.db")
	if err != nil {
		return nil, nil, err
	}

	cleanUp := func() {
		os.RemoveAll(tempDir)
		db.Close()
	}

	filterDB, err := New(db, chaincfg.SimNetParams)
	if err != nil {
		return nil, nil, err
	}

	return cleanUp, filterDB, nil
}

func TestGenesisFilterCreation(t *testing.T) {
	cleanUp, database, err := createTestDatabase()
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

	genesisHash := chaincfg.SimNetParams.GenesisHash

	// With the database initialized, we should be able to fetch both the
	// regular and extended filter for the genesis block.
	regGenesisFilter, err := database.FetchFilter(genesisHash, RegularFilter)
	if err != nil {
		t.Fatalf("unable to fetch regular genesis filter: %v", err)
	}

	// The regular filter should be non-nil as the gensis block's output
	// and the coinbase txid should be indexed.
	if regGenesisFilter == nil {
		t.Fatalf("regular genesis filter is nil")
	}

	// We don't test the extended genesis filter for nullness because it
	// only has a coinbase tx, which has no inputs to be indexed.
	// Therefore, we only check if there's an error on retrieval.
	_, err = database.FetchFilter(genesisHash, ExtendedFilter)
	if err != nil {
		t.Fatalf("unable to fetch ext genesis filter: %v", err)
	}
}

func genRandFilter(numElements uint32) (*gcs.Filter, error) {
	elements := make([][]byte, numElements)
	for i := uint32(0); i < numElements; i++ {
		var elem [20]byte
		if _, err := rand.Read(elem[:]); err != nil {
			return nil, err
		}

		elements[i] = elem[:]
	}

	var key [16]byte
	if _, err := rand.Read(key[:]); err != nil {
		return nil, err
	}

	filter, err := gcs.BuildGCSFilter(20, key, elements)
	if err != nil {
		return nil, err
	}

	return filter, nil
}

func TestFilterStorage(t *testing.T) {
	// TODO(roasbeef): use testing.Quick
	cleanUp, database, err := createTestDatabase()
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

	// We'll generate a random block hash to create our test filters
	// against.
	var randHash chainhash.Hash
	if _, err := rand.Read(randHash[:]); err != nil {
		t.Fatalf("unable to generate random hash: %v", err)
	}

	// First, we'll create and store a random fitler for the regular filter
	// type for the block hash generate above.
	regFilter, err := genRandFilter(100)
	if err != nil {
		t.Fatalf("unable to create random filter: %v", err)
	}
	err = database.PutFilter(&randHash, regFilter, RegularFilter)
	if err != nil {
		t.Fatalf("unable to store regular filter: %v", err)
	}

	// Next, we'll do the same for the exteended fitler type.
	extFilter, err := genRandFilter(100)
	if err != nil {
		t.Fatalf("unable to create random filter: %v", err)
	}
	err = database.PutFilter(&randHash, extFilter, ExtendedFilter)
	if err != nil {
		t.Fatalf("unable to store extended filter: %v", err)
	}

	// With the filter stored, we should be able to retrieve the filter
	// without any issue, and it should match the stored filter exactly.
	regFilterDB, err := database.FetchFilter(&randHash, RegularFilter)
	if err != nil {
		t.Fatalf("unable to retrieve reg filter: %v", err)
	}
	if !reflect.DeepEqual(regFilter, regFilterDB) {
		t.Fatalf("regular filter doesn't match!")
	}

	// Similarly, we should also be able to retrive the extended fitler,
	// and it should match what we initially put in.
	extFitlerDB, err := database.FetchFilter(&randHash, ExtendedFilter)
	if err != nil {
		t.Fatalf("unable to retrieve ext filter: %v", err)
	}
	if !reflect.DeepEqual(extFilter, extFitlerDB) {
		t.Fatalf("extended filter doesn't match!")
	}
}
