//go:build !test_db_postgres

package sqldb_test

import (
	"database/sql"
	"os"
	"testing"

	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"

	"github.com/lightninglabs/neutrino/sqldb"
)

// pgAdminDSNEnv names an environment variable holding the DSN of a running
// PostgreSQL server. When set, the legacy import tests and benchmark run
// against a fresh database on that server instead of SQLite.
const pgAdminDSNEnv = "NEUTRINO_LEGACY_TEST_PG_DSN"

// newTestConfig returns the SQL config the legacy import tests run against:
// SQLite by default, or PostgreSQL when pgAdminDSNEnv is set.
func newTestConfig(t testing.TB) *sqldb.Config {
	t.Helper()

	adminDSN := os.Getenv(pgAdminDSNEnv)
	if adminDSN == "" {
		return &sqldb.Config{
			Backend: sqldb.BackendSqlite,
			Sqlite:  &sqldbv2.SqliteConfig{},
		}
	}

	admin, err := sql.Open("pgx", adminDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })

	dbName := sqldbv2.RandomDBName(t)
	_, err = admin.ExecContext(
		t.Context(), "CREATE DATABASE "+dbName,
	)
	require.NoError(t, err)

	// Swap the database name in the DSN path.
	cfg := &sqldbv2.PostgresConfig{
		Dsn:         replaceDSNDatabase(adminDSN, dbName),
		QueryConfig: *sqldbv2.DefaultPostgresConfig(),
	}

	return &sqldb.Config{Backend: sqldb.BackendPostgres, Postgres: cfg}
}
