package filterdb

import (
	"context"
	"math/rand"
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/lightninglabs/neutrino/sqldb"
)

// newSQLFilterStore returns a SQLFilterStore backed by a fresh in-memory
// SQLite database.
func newSQLFilterStore(t *testing.T) *SQLFilterStore {
	t.Helper()

	backend := sqldb.NewTestBackend(t)
	store, err := NewSQLFilterStore(
		context.Background(), backend.FilterTxer,
		chaincfg.SimNetParams,
	)
	require.NoError(t, err)
	return store
}

// filterStoreFactory wires a FilterDatabase implementation up for
// parameterized tests. The kvdb factory reuses createTestDatabase from
// db_test.go.
type filterStoreFactory func(t *testing.T) FilterDatabase

func filterStoreFactories() map[string]filterStoreFactory {
	return map[string]filterStoreFactory{
		"kvdb":   func(t *testing.T) FilterDatabase { return createTestDatabase(t) },
		"sqlite": func(t *testing.T) FilterDatabase { return newSQLFilterStore(t) },
	}
}

// TestFilterStoreBackends covers the genesis filter creation and the basic
// store-and-fetch round-trip across both backends.
func TestFilterStoreBackends(t *testing.T) {
	t.Parallel()

	for name, makeStore := range filterStoreFactories() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := makeStore(t)
			testGenesisFilter(t, store)
			testFilterRoundTrip(t, store)
		})
	}
}

func testGenesisFilter(t *testing.T, store FilterDatabase) {
	t.Helper()

	genesisHash := chaincfg.SimNetParams.GenesisHash
	regGenesisFilter, err := store.FetchFilter(genesisHash, RegularFilter)
	require.NoError(t, err)
	require.NotNil(t, regGenesisFilter)
}

func testFilterRoundTrip(t *testing.T, store FilterDatabase) {
	t.Helper()

	var randHash chainhash.Hash
	_, err := rand.Read(randHash[:])
	require.NoError(t, err)

	regFilter := genRandFilter(t, 100)
	require.NoError(t, store.PutFilters(&FilterData{
		Filter:    regFilter,
		BlockHash: &randHash,
		Type:      RegularFilter,
	}))

	got, err := store.FetchFilter(&randHash, RegularFilter)
	require.NoError(t, err)
	require.Equal(t, regFilter, got)
}

// TestSQLFilterStoreNilSentinel verifies that storing a nil filter and then
// fetching it returns (nil, nil), matching the legacy "deleted filter"
// sentinel semantics. ErrFilterNotFound must NOT be returned for a row that
// is present but holds a zero-length value.
func TestSQLFilterStoreNilSentinel(t *testing.T) {
	t.Parallel()

	store := newSQLFilterStore(t)

	var randHash chainhash.Hash
	_, err := rand.Read(randHash[:])
	require.NoError(t, err)

	require.NoError(t, store.PutFilters(&FilterData{
		Filter:    nil,
		BlockHash: &randHash,
		Type:      RegularFilter,
	}))

	got, err := store.FetchFilter(&randHash, RegularFilter)
	require.NoError(t, err)
	require.Nil(t, got)
}

// TestSQLFilterStoreNotFound verifies that an absent row maps to
// ErrFilterNotFound.
func TestSQLFilterStoreNotFound(t *testing.T) {
	t.Parallel()

	store := newSQLFilterStore(t)

	var randHash chainhash.Hash
	_, err := rand.Read(randHash[:])
	require.NoError(t, err)

	_, err = store.FetchFilter(&randHash, RegularFilter)
	require.ErrorIs(t, err, ErrFilterNotFound)
}

// TestSQLFilterStorePurge inserts several filters, purges the table, and
// verifies that all entries (including the genesis filter) become absent —
// matching the legacy bucket-recreate semantics.
func TestSQLFilterStorePurge(t *testing.T) {
	t.Parallel()

	store := newSQLFilterStore(t)

	hashes := make([]chainhash.Hash, 5)
	filters := make([]*FilterData, 5)
	for i := range hashes {
		_, err := rand.Read(hashes[i][:])
		require.NoError(t, err)

		filters[i] = &FilterData{
			Filter:    genRandFilter(t, 50),
			BlockHash: &hashes[i],
			Type:      RegularFilter,
		}
	}
	require.NoError(t, store.PutFilters(filters...))

	require.NoError(t, store.PurgeFilters(RegularFilter))

	for i := range hashes {
		_, err := store.FetchFilter(&hashes[i], RegularFilter)
		require.ErrorIs(t, err, ErrFilterNotFound)
	}

	// Even the genesis row, which the constructor seeded, must be gone:
	// the legacy walletdb impl drops and recreates the per-type bucket
	// without re-seeding genesis, and we faithfully match that.
	genesisHash := chaincfg.SimNetParams.GenesisHash
	_, err := store.FetchFilter(genesisHash, RegularFilter)
	require.ErrorIs(t, err, ErrFilterNotFound)
}

// TestSQLFilterStoreUnknownType ensures that unsupported FilterType values
// produce an error rather than silently ignoring the row.
func TestSQLFilterStoreUnknownType(t *testing.T) {
	t.Parallel()

	store := newSQLFilterStore(t)

	var randHash chainhash.Hash
	_, err := rand.Read(randHash[:])
	require.NoError(t, err)

	bogus := FilterType(99)
	_, err = store.FetchFilter(&randHash, bogus)
	require.Error(t, err)

	err = store.PurgeFilters(bogus)
	require.Error(t, err)
}

// TestSQLFilterStoreGenesisIdempotent verifies that constructing the SQL
// filter store twice against the same database leaves a single genesis row.
func TestSQLFilterStoreGenesisIdempotent(t *testing.T) {
	t.Parallel()

	backend := sqldb.NewTestBackend(t)

	_, err := NewSQLFilterStore(
		context.Background(), backend.FilterTxer,
		chaincfg.SimNetParams,
	)
	require.NoError(t, err)

	store, err := NewSQLFilterStore(
		context.Background(), backend.FilterTxer,
		chaincfg.SimNetParams,
	)
	require.NoError(t, err)

	got, err := store.FetchFilter(
		chaincfg.SimNetParams.GenesisHash, RegularFilter,
	)
	require.NoError(t, err)
	require.NotNil(t, got)
}
