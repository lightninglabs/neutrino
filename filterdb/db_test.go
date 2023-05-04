package filterdb

import (
	"math/rand"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil/gcs"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	"github.com/stretchr/testify/require"
)

func createTestDatabase(t *testing.T) FilterDatabase {
	tempDir := t.TempDir()

	db, err := walletdb.Create(
		"bdb", tempDir+"/test.db", true, time.Second*10,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	filterDB, err := New(db, chaincfg.SimNetParams)
	require.NoError(t, err)

	return filterDB
}

// TestGenesisFilterCreation tests the fetching of the genesis block filter.
func TestGenesisFilterCreation(t *testing.T) {
	var (
		database    = createTestDatabase(t)
		genesisHash = chaincfg.SimNetParams.GenesisHash
	)

	// With the database initialized, we should be able to fetch the
	// regular filter for the genesis block.
	regGenesisFilter, err := database.FetchFilter(
		genesisHash, RegularFilter,
	)
	require.NoError(t, err)

	// The regular filter should be non-nil as the gensis block's output
	// and the coinbase txid should be indexed.
	require.NotNil(t, regGenesisFilter)
}

func genRandFilter(t *testing.T, numElements uint32) *gcs.Filter {
	elements := make([][]byte, numElements)
	for i := uint32(0); i < numElements; i++ {
		var elem [20]byte
		_, err := rand.Read(elem[:])
		require.NoError(t, err)

		elements[i] = elem[:]
	}

	var key [16]byte
	_, err := rand.Read(key[:])
	require.NoError(t, err)

	filter, err := gcs.BuildGCSFilter(
		builder.DefaultP, builder.DefaultM, key, elements,
	)
	require.NoError(t, err)

	return filter
}

// TestFilterStorage test writing to and reading from the filter DB.
func TestFilterStorage(t *testing.T) {
	database := createTestDatabase(t)

	// We'll generate a random block hash to create our test filters
	// against.
	var randHash chainhash.Hash
	_, err := rand.Read(randHash[:])
	require.NoError(t, err)

	// First, we'll create and store a random filter for the regular filter
	// type for the block hash generate above.
	regFilter := genRandFilter(t, 100)

	err = database.PutFilter(&randHash, regFilter, RegularFilter)
	require.NoError(t, err)

	// With the filter stored, we should be able to retrieve the filter
	// without any issue, and it should match the stored filter exactly.
	regFilterDB, err := database.FetchFilter(&randHash, RegularFilter)
	require.NoError(t, err)
	require.Equal(t, regFilter, regFilterDB)
}
