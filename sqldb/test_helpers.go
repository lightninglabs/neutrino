package sqldb

import (
	"database/sql"
	"testing"

	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"

	"github.com/lightninglabs/neutrino/sqldb/sqlc"
)

// NewTestBackend constructs a fully-migrated, in-process Backend for use in
// unit tests. Every call returns an independent backend with its own
// underlying SQL store; the t.Cleanup hook closes the store at the end of the
// test.
//
// The build tag selection in lnd/sqldb/v2 means this helper transparently
// selects between sqlite (default) and postgres (test_db_postgres tag).
func NewTestBackend(t *testing.T) *Backend {
	t.Helper()

	store := sqldbv2.NewTestDB(t, []sqldbv2.MigrationSet{
		MigrationSet(nil),
	})

	baseDB := store.GetBaseDB()

	headerExec := sqldbv2.NewTransactionExecutor[HeaderQueries](
		baseDB,
		func(tx *sql.Tx) HeaderQueries { return sqlc.New(tx) },
	)
	filterExec := sqldbv2.NewTransactionExecutor[FilterQueries](
		baseDB,
		func(tx *sql.Tx) FilterQueries { return sqlc.New(tx) },
	)
	banExec := sqldbv2.NewTransactionExecutor[BanQueries](
		baseDB,
		func(tx *sql.Tx) BanQueries { return sqlc.New(tx) },
	)

	// sqldbv2.NewTestDB already registers a t.Cleanup that closes the
	// connection. We don't add another close here.
	require.NotNil(t, headerExec)
	require.NotNil(t, filterExec)
	require.NotNil(t, banExec)

	// We hold the *SqliteStore (or *PostgresStore via build tag) in the
	// returned Backend so callers that need to access the raw DB for
	// fixture seeding can reach it via GetBaseDB().
	var typedStore sqldbv2.DB = store

	return &Backend{
		store:      typedStore,
		HeaderTxer: headerExec,
		FilterTxer: filterExec,
		BanTxer:    banExec,
	}
}
