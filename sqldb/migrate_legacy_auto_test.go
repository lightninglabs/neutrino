package sqldb_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/stretchr/testify/require"

	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/filterdb"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/sqldb"
)

// openAuto opens a SQLite backend over dataDir through NewBackendWithLegacy.
// The backend is closed at the end of the test if the open succeeded.
func openAuto(t *testing.T, dataDir string, params *chaincfg.Params,
	skip bool, explicit *sqldb.LegacyDataSource) (*sqldb.Backend, error) {

	t.Helper()

	cfg := configFor(t, dataDir)
	cfg.SkipLegacyMigration = skip
	backend, err := sqldb.NewBackendWithLegacy(
		dataDir, cfg, params, explicit,
	)
	if err == nil {
		t.Cleanup(func() { _ = backend.Close() })
	}

	return backend, err
}

// requireTips asserts the block and filter header chain tips stored in the SQL
// backend.
func requireTips(t *testing.T, backend *sqldb.Backend,
	params *chaincfg.Params, blockHeight, filterHeight uint32) {

	t.Helper()

	bhs, err := headerfs.NewSQLBlockHeaderStore(
		t.Context(), backend.HeaderTxer, params,
	)
	require.NoError(t, err)
	_, height, err := bhs.ChainTip()
	require.NoError(t, err)
	require.Equal(t, blockHeight, height)

	fhs, err := headerfs.NewSQLFilterHeaderStore(
		t.Context(), backend.HeaderTxer, headerfs.RegularFilter,
		params, nil,
	)
	require.NoError(t, err)
	_, height, err = fhs.ChainTip()
	require.NoError(t, err)
	require.Equal(t, filterHeight, height)
}

// requireRetired asserts the flat files were renamed to *.bak.
func requireRetired(t *testing.T, dataDir string) {
	t.Helper()

	for _, name := range []string{
		sqldb.DefaultBlockHeadersFilename,
		sqldb.DefaultRegFilterHeadersFilename,
	} {
		require.NoFileExists(t, filepath.Join(dataDir, name))
		require.FileExists(t, filepath.Join(dataDir, name+".bak"))
	}
}

// TestAutoImportDetectsLegacyData verifies that no LegacyDataSource is needed:
// the flat files and the walletdb in the data directory are found and
// imported, and the flat files are retired afterwards.
func TestAutoImportDetectsLegacyData(t *testing.T) {
	t.Parallel()

	const numBlocks = 25
	params := &chaincfg.SimNetParams

	dataDir, db, rawHeaders := seedLegacy(t, numBlocks)
	require.NoError(t, db.Close())

	backend, err := openAuto(t, dataDir, params, false, nil)
	require.NoError(t, err)
	requireTips(t, backend, params, numBlocks, numBlocks)
	requireRetired(t, dataDir)

	// Spot check a header, a filter and the bans from the walletdb.
	bhs, err := headerfs.NewSQLBlockHeaderStore(
		t.Context(), backend.HeaderTxer, params,
	)
	require.NoError(t, err)
	got, err := bhs.FetchHeaderByHeight(numBlocks / 2)
	require.NoError(t, err)
	require.Equal(t, rawHeaders[numBlocks/2].BlockHash(), got.BlockHash())

	fdb, err := filterdb.NewSQLFilterStore(
		t.Context(), backend.FilterTxer, *params,
	)
	require.NoError(t, err)
	genesisFilter, err := fdb.FetchFilter(
		params.GenesisHash, filterdb.RegularFilter,
	)
	require.NoError(t, err)
	require.NotNil(t, genesisFilter)

	bs, err := banman.NewSQLStore(backend.BanTxer)
	require.NoError(t, err)
	active, err := banman.ParseIPNet("10.0.0.1:8333", nil)
	require.NoError(t, err)
	status, err := bs.Status(active)
	require.NoError(t, err)
	require.True(t, status.Banned)

	// The walletdb is only read, never removed.
	require.FileExists(t, filepath.Join(dataDir, "neutrino.db"))
}

// TestAutoImportNoLegacyData verifies a fresh install, which has nothing to
// import, opens cleanly and does not conjure up any files.
func TestAutoImportNoLegacyData(t *testing.T) {
	t.Parallel()

	params := &chaincfg.SimNetParams
	dataDir := t.TempDir()

	require.Nil(t, sqldb.DetectLegacyDataSource(dataDir, params))

	backend, err := openAuto(t, dataDir, params, false, nil)
	require.NoError(t, err)
	requireTips(t, backend, params, 0, 0)

	for _, name := range []string{
		sqldb.DefaultBlockHeadersFilename + ".bak",
		sqldb.DefaultRegFilterHeadersFilename + ".bak",
	} {
		require.NoFileExists(t, filepath.Join(dataDir, name))
	}
}

// TestAutoImportPartialState covers the combinations of legacy files that are
// legitimately partial: nothing but flat files, nothing but a walletdb and
// filter headers that trail the block headers.
func TestAutoImportPartialState(t *testing.T) {
	t.Parallel()

	const numBlocks = 12
	params := &chaincfg.SimNetParams

	tests := []struct {
		name   string
		mutate func(t *testing.T, dir string)

		blockHeight  uint32
		filterHeight uint32
		filters      bool
	}{{
		name: "flat files without walletdb",
		mutate: func(t *testing.T, dir string) {
			require.NoError(t, os.Remove(
				filepath.Join(dir, "neutrino.db"),
			))
		},
		blockHeight:  numBlocks,
		filterHeight: numBlocks,
	}, {
		name: "walletdb without flat files",
		mutate: func(t *testing.T, dir string) {
			for _, name := range []string{
				sqldb.DefaultBlockHeadersFilename,
				sqldb.DefaultRegFilterHeadersFilename,
			} {
				require.NoError(t, os.Remove(
					filepath.Join(dir, name),
				))
			}
		},
		filters: true,
	}, {
		name: "filter headers trail block headers",
		mutate: func(t *testing.T, dir string) {
			path := filepath.Join(
				dir, sqldb.DefaultRegFilterHeadersFilename,
			)
			require.NoError(t, os.Truncate(path, 5*32))
		},
		blockHeight:  numBlocks,
		filterHeight: 4,
		filters:      true,
	}, {
		name: "block headers without filter headers",
		mutate: func(t *testing.T, dir string) {
			require.NoError(t, os.Remove(filepath.Join(
				dir, sqldb.DefaultRegFilterHeadersFilename,
			)))
		},
		blockHeight: numBlocks,
		filters:     true,
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dataDir, db, _ := seedLegacy(t, numBlocks)
			require.NoError(t, db.Close())
			test.mutate(t, dataDir)

			backend, err := openAuto(t, dataDir, params, false, nil)
			require.NoError(t, err)

			if test.blockHeight > 0 || test.filterHeight > 0 {
				requireTips(
					t, backend, params, test.blockHeight,
					test.filterHeight,
				)
			}

			// The walletdb is the only source of bans.
			bs, err := banman.NewSQLStore(backend.BanTxer)
			require.NoError(t, err)
			active, err := banman.ParseIPNet("10.0.0.1:8333", nil)
			require.NoError(t, err)
			status, err := bs.Status(active)
			require.NoError(t, err)
			require.Equal(t, test.filters, status.Banned)
		})
	}
}

// TestAutoImportRejectsBadState verifies that inconsistent or corrupt legacy
// files fail the open with a descriptive error instead of being skipped.
func TestAutoImportRejectsBadState(t *testing.T) {
	t.Parallel()

	const numBlocks = 10

	tests := []struct {
		name   string
		params *chaincfg.Params
		mutate func(t *testing.T, dir string)
		errMsg string
	}{{
		name:   "wrong network",
		params: &chaincfg.MainNetParams,
		mutate: func(t *testing.T, dir string) {},
		errMsg: "not the genesis block",
	}, {
		name:   "truncated block headers",
		params: &chaincfg.SimNetParams,
		mutate: func(t *testing.T, dir string) {
			path := filepath.Join(
				dir, sqldb.DefaultBlockHeadersFilename,
			)
			require.NoError(t, os.Truncate(path, 5*80+7))
		},
		errMsg: "truncated or corrupt",
	}, {
		name:   "truncated filter headers",
		params: &chaincfg.SimNetParams,
		mutate: func(t *testing.T, dir string) {
			path := filepath.Join(
				dir, sqldb.DefaultRegFilterHeadersFilename,
			)
			require.NoError(t, os.Truncate(path, 5*32+1))
		},
		errMsg: "truncated or corrupt",
	}, {
		name:   "filter headers without block headers",
		params: &chaincfg.SimNetParams,
		mutate: func(t *testing.T, dir string) {
			require.NoError(t, os.Remove(filepath.Join(
				dir, sqldb.DefaultBlockHeadersFilename,
			)))
		},
		errMsg: "missing or empty",
	}, {
		name:   "filter headers beyond block headers",
		params: &chaincfg.SimNetParams,
		mutate: func(t *testing.T, dir string) {
			path := filepath.Join(
				dir, sqldb.DefaultBlockHeadersFilename,
			)
			require.NoError(t, os.Truncate(path, 4*80))
		},
		errMsg: "more than",
	}, {
		name:   "broken header chain",
		params: &chaincfg.SimNetParams,
		mutate: func(t *testing.T, dir string) {
			path := filepath.Join(
				dir, sqldb.DefaultBlockHeadersFilename,
			)
			raw, err := os.ReadFile(path)
			require.NoError(t, err)

			// Flip a bit in the previous-block hash of height 6.
			raw[6*80+10] ^= 1
			require.NoError(t, os.WriteFile(path, raw, 0o600))
		},
		errMsg: "does not connect",
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dataDir, db, _ := seedLegacy(t, numBlocks)
			require.NoError(t, db.Close())
			test.mutate(t, dataDir)

			_, err := openAuto(t, dataDir, test.params, false, nil)
			require.ErrorContains(t, err, test.errMsg)

			// A failed import must not retire the flat files.
			for _, name := range []string{
				sqldb.DefaultBlockHeadersFilename,
				sqldb.DefaultRegFilterHeadersFilename,
			} {
				require.NoFileExists(
					t, filepath.Join(dataDir, name+".bak"),
				)
			}
		})
	}
}

// TestAutoImportRetryAfterCrash simulates an import that dies after some
// batches were already committed. The next start must wipe the partial rows
// and import the repaired state in full, rather than treating the failed
// migration as done.
func TestAutoImportRetryAfterCrash(t *testing.T) {
	t.Parallel()

	const (
		numHeaders = 70_000
		badHeight  = 50_000
	)
	params := &chaincfg.MainNetParams

	dataDir := t.TempDir()
	writeSyntheticLegacy(t, dataDir, params, numHeaders, 10)

	// Corrupt a header in the second pipeline chunk. The first chunk has
	// been committed by then, so the failed run leaves partial rows behind.
	path := filepath.Join(dataDir, sqldb.DefaultBlockHeadersFilename)
	good, err := os.ReadFile(path)
	require.NoError(t, err)
	bad := append([]byte(nil), good...)
	bad[badHeight*80+10] ^= 1
	require.NoError(t, os.WriteFile(path, bad, 0o600))

	explicit := &sqldb.LegacyDataSource{BatchSize: 20_000}
	_, err = openAuto(t, dataDir, params, false, explicit)
	require.ErrorContains(t, err, "does not connect")
	require.FileExists(t, path)
	require.NoFileExists(t, path+".bak")

	// "Repair" the file and restart. The import runs again from scratch.
	require.NoError(t, os.WriteFile(path, good, 0o600))
	backend, err := openAuto(t, dataDir, params, false, explicit)
	require.NoError(t, err)
	requireTips(t, backend, params, numHeaders-1, numHeaders-1)
	requireRetired(t, dataDir)
}

// TestAutoImportNotSilentlySkipped is the regression test for the footgun
// where starting the SQL backend without declaring a LegacyDataSource recorded
// the import as done and orphaned the legacy data forever. Without any opt-in
// the data must be imported on that very first start.
func TestAutoImportNotSilentlySkipped(t *testing.T) {
	t.Parallel()

	const numBlocks = 8
	params := &chaincfg.SimNetParams

	dataDir, db, _ := seedLegacy(t, numBlocks)
	require.NoError(t, db.Close())

	backend, err := openAuto(t, dataDir, params, false, nil)
	require.NoError(t, err)
	requireTips(t, backend, params, numBlocks, numBlocks)
	require.NoError(t, backend.Close())

	// Restarting is a no-op: the migration is recorded, nothing is
	// re-imported over the live state.
	backend, err = openAuto(t, dataDir, params, false, nil)
	require.NoError(t, err)
	requireTips(t, backend, params, numBlocks, numBlocks)
}

// TestSkipLegacyMigrationLeavesFiles verifies the explicit opt-out leaves the
// legacy files untouched and imports nothing.
func TestSkipLegacyMigrationLeavesFiles(t *testing.T) {
	t.Parallel()

	params := &chaincfg.SimNetParams
	dataDir, db, _ := seedLegacy(t, 5)
	require.NoError(t, db.Close())

	backend, err := openAuto(t, dataDir, params, true, nil)
	require.NoError(t, err)
	requireTips(t, backend, params, 0, 0)

	for _, name := range []string{
		sqldb.DefaultBlockHeadersFilename,
		sqldb.DefaultRegFilterHeadersFilename,
	} {
		require.FileExists(t, filepath.Join(dataDir, name))
	}
}

// TestAutoImportMultiChunk imports a chain spanning several pipeline chunks
// and write transactions and compares every sampled row to its source.
func TestAutoImportMultiChunk(t *testing.T) {
	t.Parallel()

	const (
		numHeaders = 70_000
		numFilters = 150
	)
	params := &chaincfg.MainNetParams

	dataDir := t.TempDir()
	writeSyntheticLegacy(t, dataDir, params, numHeaders, numFilters)

	explicit := &sqldb.LegacyDataSource{BatchSize: 25_000}
	backend, err := openAuto(t, dataDir, params, false, explicit)
	require.NoError(t, err)
	requireTips(t, backend, params, numHeaders-1, numHeaders-1)

	raw, err := os.ReadFile(filepath.Join(
		dataDir, sqldb.DefaultBlockHeadersFilename+".bak",
	))
	require.NoError(t, err)
	filterRaw, err := os.ReadFile(filepath.Join(
		dataDir, sqldb.DefaultRegFilterHeadersFilename+".bak",
	))
	require.NoError(t, err)

	bhs, err := headerfs.NewSQLBlockHeaderStore(
		t.Context(), backend.HeaderTxer, params,
	)
	require.NoError(t, err)
	fhs, err := headerfs.NewSQLFilterHeaderStore(
		t.Context(), backend.HeaderTxer, headerfs.RegularFilter,
		params, nil,
	)
	require.NoError(t, err)

	// Sample around every chunk and transaction boundary.
	for _, h := range []uint32{
		0, 1, 32_766, 32_767, 32_768, 32_769, 49_999, 50_000, 65_535,
		65_536, numHeaders - 1,
	} {
		hash := chainhash.DoubleHashH(raw[h*80 : (h+1)*80])
		got, err := bhs.FetchHeaderByHeight(h)
		require.NoError(t, err)
		require.Equal(t, hash, got.BlockHash(), "height %d", h)

		gotFilter, err := fhs.FetchHeaderByHeight(h)
		require.NoError(t, err)
		require.Equal(t, filterRaw[h*32:(h+1)*32], gotFilter[:])

		gotHeight, err := bhs.HeightFromHash(&hash)
		require.NoError(t, err)
		require.Equal(t, h, gotHeight)
	}
}

// TestAutoImportFollowsLegacyIndexTip verifies the walletdb header index is
// authoritative for the chain tips: records past it are ignored, while an
// index that is ahead of its flat file is rejected.
func TestAutoImportFollowsLegacyIndexTip(t *testing.T) {
	t.Parallel()

	const numBlocks = 10
	params := &chaincfg.SimNetParams

	t.Run("records past the index tip", func(t *testing.T) {
		t.Parallel()

		dataDir, db, _ := seedLegacy(t, numBlocks)
		require.NoError(t, db.Close())

		// Append junk records to both files, as an interrupted write
		// would leave behind.
		for name, size := range map[string]int{
			sqldb.DefaultBlockHeadersFilename:     2 * 80,
			sqldb.DefaultRegFilterHeadersFilename: 3 * 32,
		} {
			f, err := os.OpenFile(
				filepath.Join(dataDir, name),
				os.O_APPEND|os.O_WRONLY, 0o600,
			)
			require.NoError(t, err)
			_, err = f.Write(make([]byte, size))
			require.NoError(t, err)
			require.NoError(t, f.Close())
		}

		backend, err := openAuto(t, dataDir, params, false, nil)
		require.NoError(t, err)
		requireTips(t, backend, params, numBlocks, numBlocks)
	})

	t.Run("index ahead of the file", func(t *testing.T) {
		t.Parallel()

		dataDir, db, _ := seedLegacy(t, numBlocks)
		require.NoError(t, db.Close())
		path := filepath.Join(
			dataDir, sqldb.DefaultBlockHeadersFilename,
		)
		require.NoError(t, os.Truncate(path, numBlocks*80))

		// The valid prefix is imported; filter headers are cut to the
		// block headers that survived.
		fPath := filepath.Join(
			dataDir, sqldb.DefaultRegFilterHeadersFilename,
		)
		require.NoError(t, os.Truncate(fPath, numBlocks*32))

		backend, err := openAuto(t, dataDir, params, false, nil)
		require.NoError(t, err)
		requireTips(t, backend, params, numBlocks-1, numBlocks-1)
	})
}

// TestSqliteFileGuard verifies neutrino refuses to run its migrations in a
// SQLite file that belongs to something else, such as lnd's key-value
// neutrino database.
func TestSqliteFileGuard(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	cfg := configFor(t, dataDir)
	if cfg.Backend != sqldb.BackendSqlite {
		t.Skip("sqlite only")
	}

	path := filepath.Join(dataDir, sqldb.DefaultSqliteFilename)
	other, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	_, err = other.Exec("CREATE TABLE kv (key BLOB, value BLOB)")
	require.NoError(t, err)
	require.NoError(t, other.Close())

	_, err = openAuto(
		t, dataDir, &chaincfg.SimNetParams, false, nil,
	)
	require.ErrorContains(t, err, "does not belong to neutrino")
}

// TestDirtyLegacyImportIsRetried verifies that a process killed in the middle
// of the import, which leaves migration version 2 marked dirty, can start
// again.
func TestDirtyLegacyImportIsRetried(t *testing.T) {
	t.Parallel()

	const numBlocks = 6
	params := &chaincfg.SimNetParams

	dataDir, db, _ := seedLegacy(t, numBlocks)
	require.NoError(t, db.Close())

	cfg := configFor(t, dataDir)
	if cfg.Backend != sqldb.BackendSqlite {
		t.Skip("sqlite only")
	}

	// Run a full import, then put the tracker where a kill during the
	// import would have left it.
	backend, err := openAuto(t, dataDir, params, false, nil)
	require.NoError(t, err)
	require.NoError(t, backend.Close())

	raw, err := sql.Open(
		"sqlite", filepath.Join(dataDir, sqldb.DefaultSqliteFilename),
	)
	require.NoError(t, err)
	_, err = raw.Exec("UPDATE neutrino_migrations SET version = 2, " +
		"dirty = 1")
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	// A killed import never got to retire the flat files.
	for _, name := range []string{
		sqldb.DefaultBlockHeadersFilename,
		sqldb.DefaultRegFilterHeadersFilename,
	} {
		p := filepath.Join(dataDir, name)
		require.NoError(t, os.Rename(p+".bak", p))
	}

	// The restart must come up instead of refusing the dirty state, and
	// import everything again.
	backend, err = openAuto(t, dataDir, params, false, nil)
	require.NoError(t, err)
	requireTips(t, backend, params, numBlocks, numBlocks)
}
