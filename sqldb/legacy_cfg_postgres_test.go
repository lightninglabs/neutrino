//go:build test_db_postgres

package sqldb_test

import (
	"sync"
	"testing"
	"time"

	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"

	"github.com/lightninglabs/neutrino/sqldb"
)

var (
	pgFixture     *sqldbv2.TestPgFixture
	pgFixtureOnce sync.Once
)

// newTestConfig returns a config for a fresh database on a PostgreSQL docker
// fixture shared by all tests of the package.
func newTestConfig(t testing.TB) *sqldb.Config {
	t.Helper()

	pgFixtureOnce.Do(func() {
		pgFixture = sqldbv2.NewTestPgFixture(t, 10*time.Minute)
	})

	dbName := sqldbv2.RandomDBName(t)
	_, err := pgFixture.DB().ExecContext(
		t.Context(), "CREATE DATABASE "+dbName,
	)
	require.NoError(t, err)

	pgCfg := pgFixture.GetConfig(dbName)
	pgCfg.QueryConfig = *sqldbv2.DefaultPostgresConfig()

	return &sqldb.Config{
		Backend:  sqldb.BackendPostgres,
		Postgres: pgCfg,
	}
}
