package sqldb

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"
)

// TestNewBackendSqlite is a smoke test that exercises Phase 1's scaffolding:
// it opens a SQLite-backed Backend in a temp directory, verifies that
// migrations were applied, and runs a trivial query through each
// per-domain TransactionExecutor.
func TestNewBackendSqlite(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	cfg := &Config{
		Backend: BackendSqlite,
		Sqlite:  &sqldbv2.SqliteConfig{},
	}

	backend, err := NewBackend(tempDir, cfg, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, backend.Close())
	})

	// The SQLite database file should exist at the default location.
	require.FileExists(
		t, filepath.Join(tempDir, DefaultSqliteFilename),
	)

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	// Each TransactionExecutor should be able to run a read tx and a
	// trivial query against the freshly-migrated schema. Empty tables
	// are the expected baseline immediately after init.
	require.NoError(t, backend.HeaderTxer.ExecTx(
		ctx, sqldbv2.ReadTxOpt(),
		func(q HeaderQueries) error {
			gotBlocks, err := q.CountBlockHeaders(ctx)
			if err != nil {
				return err
			}
			require.Equal(t, int64(0), gotBlocks)

			gotFilters, err := q.CountFilterHeaders(ctx)
			if err != nil {
				return err
			}
			require.Equal(t, int64(0), gotFilters)

			return nil
		}, sqldbv2.NoOpReset,
	))

	require.NoError(t, backend.FilterTxer.ExecTx(
		ctx, sqldbv2.ReadTxOpt(),
		func(q FilterQueries) error {
			// GetFilter on a missing key should propagate
			// sql.ErrNoRows, not panic.
			_, err := q.GetFilter(ctx, []byte{0x00, 0x01, 0x02})
			require.Error(t, err)
			return nil
		}, sqldbv2.NoOpReset,
	))

	require.NoError(t, backend.BanTxer.ExecTx(
		ctx, sqldbv2.WriteTxOpt(),
		func(q BanQueries) error {
			// DeleteExpiredBans on an empty table must report
			// zero affected rows.
			n, err := q.DeleteExpiredBans(ctx, time.Now())
			if err != nil {
				return err
			}
			require.Equal(t, int64(0), n)
			return nil
		}, sqldbv2.NoOpReset,
	))

	assertNoDuplicateHeaderHashIndex(t, backend, "block_headers")
	assertNoDuplicateHeaderHashIndex(t, backend, "filter_headers")
	assertSqliteChainPragmas(t, backend)
}

func assertNoDuplicateHeaderHashIndex(t *testing.T, backend *Backend,
	tableName string) {

	t.Helper()

	db := backend.store.GetBaseDB().DB
	rows, err := db.Query("PRAGMA index_list('" + tableName + "')")
	require.NoError(t, err)
	defer rows.Close()

	var indexNames []string
	for rows.Next() {
		var (
			seq     int
			name    string
			unique  int
			origin  string
			partial int
		)
		require.NoError(
			t, rows.Scan(&seq, &name, &unique, &origin, &partial),
		)
		indexNames = append(indexNames, name)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	var hashIndexes []string
	for _, name := range indexNames {
		cols, err := db.Query("PRAGMA index_info('" + name + "')")
		require.NoError(t, err)

		var columns []string
		for cols.Next() {
			var (
				cid     int
				seqno   int
				colName string
			)
			require.NoError(
				t, cols.Scan(&seqno, &cid, &colName),
			)
			columns = append(columns, colName)
		}
		require.NoError(t, cols.Err())
		require.NoError(t, cols.Close())

		if len(columns) == 1 && columns[0] == "block_hash" {
			hashIndexes = append(hashIndexes, name)
		}
	}

	require.Len(t, hashIndexes, 1)
	require.NotContains(t, hashIndexes, tableName+"_hash_idx")
}

func assertSqliteChainPragmas(t *testing.T, backend *Backend) {
	t.Helper()

	db := backend.store.GetBaseDB().DB
	for pragma, expected := range map[string]int64{
		"synchronous":          1,
		"checkpoint_fullfsync": 0,
		"wal_autocheckpoint":   50000,
		"journal_size_limit":   268435456,
		"temp_store":           2,
		"cache_size":           -131072,
		"mmap_size":            268435456,
	} {
		var got int64
		require.NoError(
			t, db.QueryRow("PRAGMA "+pragma).Scan(&got),
			"pragma=%v", pragma,
		)
		require.Equal(t, expected, got, "pragma=%v", pragma)
	}
}
