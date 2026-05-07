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
}
