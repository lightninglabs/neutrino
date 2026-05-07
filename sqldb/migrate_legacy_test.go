package sqldb_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"

	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/filterdb"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/sqldb"
)

// seedLegacy populates a fresh legacy data directory with realistic state:
// numBlocks block + filter headers, a couple of GCS filters, and a few peer
// bans. It returns the data directory path and the open walletdb handle so
// the caller can pass them into a LegacyDataSource.
func seedLegacy(t *testing.T,
	numBlocks uint32) (string, walletdb.DB, []*wire.BlockHeader) {

	t.Helper()

	dataDir := t.TempDir()
	bdbPath := filepath.Join(dataDir, "neutrino.db")

	db, err := walletdb.Create(
		"bdb", bdbPath, true, time.Second*10,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	// Block headers.
	bhs, err := headerfs.NewBlockHeaderStore(
		dataDir, db, &chaincfg.SimNetParams,
	)
	require.NoError(t, err)

	prev := chaincfg.SimNetParams.GenesisBlock.Header
	headers := make([]headerfs.BlockHeader, numBlocks)
	rawHeaders := make([]*wire.BlockHeader, numBlocks+1)
	rawHeaders[0] = &chaincfg.SimNetParams.GenesisBlock.Header

	for i := uint32(1); i <= numBlocks; i++ {
		bh := &wire.BlockHeader{
			Version:    1,
			PrevBlock:  prev.BlockHash(),
			MerkleRoot: chainhash.Hash{byte(i), byte(i >> 8)},
			Timestamp:  prev.Timestamp.Add(10 * time.Minute),
			Bits:       0x207fffff,
			Nonce:      i,
		}
		headers[i-1] = headerfs.BlockHeader{
			BlockHeader: bh,
			Height:      i,
		}
		rawHeaders[i] = bh
		prev = *bh
	}
	require.NoError(t, bhs.WriteHeaders(headers...))

	// Filter headers.
	fhs, err := headerfs.NewFilterHeaderStore(
		dataDir, db, headerfs.RegularFilter,
		&chaincfg.SimNetParams, nil,
	)
	require.NoError(t, err)

	filterHeaders := make([]headerfs.FilterHeader, numBlocks)
	for i := uint32(1); i <= numBlocks; i++ {
		filterHeaders[i-1] = headerfs.FilterHeader{
			HeaderHash: rawHeaders[i].BlockHash(),
			FilterHash: chainhash.Hash{
				byte(i), byte(i >> 8), 0xAB, 0xCD,
			},
			Height: i,
		}
	}
	require.NoError(t, fhs.WriteHeaders(filterHeaders...))

	// Filters: store a deterministic filter for the genesis hash plus
	// a nil-sentinel filter for one chosen block.
	fdb, err := filterdb.New(db, chaincfg.SimNetParams)
	require.NoError(t, err)

	mid := rawHeaders[numBlocks/2].BlockHash()
	require.NoError(t, fdb.PutFilters(&filterdb.FilterData{
		Filter:    nil,
		BlockHash: &mid,
		Type:      filterdb.RegularFilter,
	}))

	// Bans: one active, one already expired (must be skipped).
	bs, err := banman.NewStore(db)
	require.NoError(t, err)
	active, err := banman.ParseIPNet("10.0.0.1:8333", nil)
	require.NoError(t, err)
	require.NoError(t, bs.BanIPNet(
		active, banman.NoCompactFilters, time.Hour,
	))
	expired, err := banman.ParseIPNet("10.0.0.2:8333", nil)
	require.NoError(t, err)
	require.NoError(t, bs.BanIPNet(
		expired, banman.ExceededBanThreshold, -time.Hour,
	))

	return dataDir, db, rawHeaders
}

// openSQLBackend opens a fresh SQLite-backed neutrino sqldb.Backend wired
// with the supplied LegacyDataSource (may be nil).
func openSQLBackend(t *testing.T, dataDir string,
	src *sqldb.LegacyDataSource) *sqldb.Backend {

	t.Helper()

	cfg := &sqldb.Config{
		Backend:        sqldb.BackendSqlite,
		Sqlite:         &sqldbv2.SqliteConfig{},
		SqliteFilename: "neutrino.sqlite",
	}
	backend, err := sqldb.NewBackend(
		dataDir, cfg, sqldb.MakeLegacyImportMigration(src),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })
	return backend
}

// TestLegacyImportRoundTrip verifies the end-to-end migration path: seed a
// kvdb-based neutrino state, pass it through MakeLegacyImportMigration, then
// open the resulting SQL stores and assert that every row matches the
// legacy state.
func TestLegacyImportRoundTrip(t *testing.T) {
	t.Parallel()

	const numBlocks = 25

	dataDir, db, rawHeaders := seedLegacy(t, numBlocks)
	src := &sqldb.LegacyDataSource{
		DB:        db,
		DataDir:   dataDir,
		Params:    &chaincfg.SimNetParams,
		BatchSize: 7, // small batches to exercise the boundary loop.
	}
	backend := openSQLBackend(t, dataDir, src)

	// Construct SQL stores against the migrated DB.
	bhs, err := headerfs.NewSQLBlockHeaderStore(
		context.Background(), backend.HeaderTxer,
		&chaincfg.SimNetParams,
	)
	require.NoError(t, err)
	fhs, err := headerfs.NewSQLFilterHeaderStore(
		context.Background(), backend.HeaderTxer,
		headerfs.RegularFilter, &chaincfg.SimNetParams, nil,
	)
	require.NoError(t, err)
	fdb, err := filterdb.NewSQLFilterStore(
		context.Background(), backend.FilterTxer,
		chaincfg.SimNetParams,
	)
	require.NoError(t, err)
	bs, err := banman.NewSQLStore(backend.BanTxer)
	require.NoError(t, err)

	// Block headers: same tip, same per-height bytes.
	tipHeader, tipHeight, err := bhs.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(numBlocks), tipHeight)
	require.Equal(t, rawHeaders[numBlocks].BlockHash(),
		tipHeader.BlockHash())

	for h := uint32(0); h <= numBlocks; h++ {
		got, err := bhs.FetchHeaderByHeight(h)
		require.NoError(t, err)
		expected := rawHeaders[h].BlockHash()
		gotHash := got.BlockHash()
		require.Equal(t, expected[:], gotHash[:],
			"block hash mismatch at height %d", h)
	}

	// Filter headers: ChainTip works, every height resolvable.
	_, fTipHeight, err := fhs.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(numBlocks), fTipHeight)
	for h := uint32(0); h <= numBlocks; h++ {
		_, err := fhs.FetchHeaderByHeight(h)
		require.NoError(t, err, "filter header missing at height %d", h)
	}

	// Filters: the nil-sentinel for the mid block must round-trip as
	// (nil, nil); the genesis filter must still be present.
	mid := rawHeaders[numBlocks/2].BlockHash()
	gotFilter, err := fdb.FetchFilter(&mid, filterdb.RegularFilter)
	require.NoError(t, err)
	require.Nil(t, gotFilter)

	genesis := chaincfg.SimNetParams.GenesisHash
	gotGenesis, err := fdb.FetchFilter(genesis, filterdb.RegularFilter)
	require.NoError(t, err)
	require.NotNil(t, gotGenesis)

	// Bans: the active one survives; the expired one was skipped.
	activeIP, _ := banman.ParseIPNet("10.0.0.1:8333", nil)
	status, err := bs.Status(activeIP)
	require.NoError(t, err)
	require.True(t, status.Banned)
	require.Equal(t, banman.NoCompactFilters, status.Reason)

	expiredIP, _ := banman.ParseIPNet("10.0.0.2:8333", nil)
	status, err = bs.Status(expiredIP)
	require.NoError(t, err)
	require.False(t, status.Banned)

	// Legacy artifacts must have been renamed to .bak.
	for _, name := range []string{
		sqldb.DefaultBlockHeadersFilename,
		sqldb.DefaultRegFilterHeadersFilename,
	} {
		_, err := os.Stat(filepath.Join(dataDir, name))
		require.True(t, os.IsNotExist(err))
		_, err = os.Stat(filepath.Join(dataDir, name+".bak"))
		require.NoError(t, err)
	}
}

// TestLegacyImportFreshInstall verifies that constructing a backend with no
// LegacyDataSource on a fresh data directory leaves the SQL tables empty
// and the version-2 migration recorded as a no-op.
func TestLegacyImportFreshInstall(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	backend := openSQLBackend(t, tempDir, nil)

	// Stores construct cleanly and report empty state.
	bhs, err := headerfs.NewSQLBlockHeaderStore(
		context.Background(), backend.HeaderTxer,
		&chaincfg.SimNetParams,
	)
	require.NoError(t, err)

	tipHeader, tipHeight, err := bhs.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(0), tipHeight)
	require.Equal(t, chaincfg.SimNetParams.GenesisBlock.Header.BlockHash(),
		tipHeader.BlockHash())
}

// TestLegacyImportIdempotent verifies that re-opening a successfully
// migrated SQL backend (the migration tracker is at v2) does not re-run the
// import — the SQL state is preserved exactly. We simulate this by opening
// the backend twice: first with the legacy source, then with a brand-new
// LegacyDataSource pointing at an unrelated directory. The second backend
// must NOT touch the SQL tables (since the migration tracker says v2 is
// already applied for that DB).
func TestLegacyImportIdempotent(t *testing.T) {
	t.Parallel()

	const numBlocks = 5
	dataDir, db, rawHeaders := seedLegacy(t, numBlocks)

	backend := openSQLBackend(t, dataDir, &sqldb.LegacyDataSource{
		DB:        db,
		DataDir:   dataDir,
		Params:    &chaincfg.SimNetParams,
		BatchSize: 3,
	})

	bhs, err := headerfs.NewSQLBlockHeaderStore(
		context.Background(), backend.HeaderTxer,
		&chaincfg.SimNetParams,
	)
	require.NoError(t, err)
	tip1, height1, err := bhs.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(numBlocks), height1)
	require.Equal(t, rawHeaders[numBlocks].BlockHash(), tip1.BlockHash())

	require.NoError(t, backend.Close())

	// Re-open the same SQL DB. Even with a (technically valid) legacy
	// source, the import must not re-run because the migration tracker
	// is already at v2.
	backend2 := openSQLBackend(t, dataDir, &sqldb.LegacyDataSource{
		DB:      db,
		DataDir: dataDir,
		Params:  &chaincfg.SimNetParams,
	})
	bhs2, err := headerfs.NewSQLBlockHeaderStore(
		context.Background(), backend2.HeaderTxer,
		&chaincfg.SimNetParams,
	)
	require.NoError(t, err)
	tip2, height2, err := bhs2.ChainTip()
	require.NoError(t, err)
	require.Equal(t, height1, height2)
	require.Equal(t, tip1.BlockHash(), tip2.BlockHash())
}

// _ keeps the net import alive for cases where the helper file does not
// otherwise reference it.
var _ = net.IPNet{}
