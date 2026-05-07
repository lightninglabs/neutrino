package sqldb

import (
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"

	"github.com/lightninglabs/neutrino/sqldb/sqlc"
)

// Backend is the assembled SQL backend used by neutrino's persistent stores.
// A single Backend wraps one underlying SQL connection (sqlite or postgres)
// plus three per-domain TransactionExecutors that the store implementations
// inject as their dependency.
type Backend struct {
	// store is the concrete sqldb/v2 store (SqliteStore or PostgresStore).
	// Holding the typed interface lets us call Close on shutdown.
	store sqldbv2.DB

	// HeaderTxer is the executor injected into headerfs.SQLBlockHeaderStore
	// and headerfs.SQLFilterHeaderStore.
	HeaderTxer HeaderTx

	// FilterTxer is the executor injected into filterdb.SQLFilterStore.
	FilterTxer FilterTx

	// BanTxer is the executor injected into banman.SQLBanStore.
	BanTxer BanTx
}

// NewBackend opens (and migrates) the SQL backend selected by cfg, returning
// a fully wired Backend ready to be passed into the per-domain store
// constructors.
//
// dataDir is the neutrino data directory; the SQLite database file is created
// there using cfg.SqliteFilename (or DefaultSqliteFilename when unset).
//
// makeProgrammatic, when non-nil, is wired into the underlying MigrationSet
// so callers can register in-code migrations such as the legacy walletdb
// import. Pass nil for a pure-SQL setup with no data import step.
func NewBackend(dataDir string, cfg *Config,
	makeProgrammatic MakeProgrammaticMigrations) (*Backend, error) {

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	migSet := MigrationSet(makeProgrammatic)

	var store sqldbv2.DB
	switch cfg.Backend {
	case BackendSqlite:
		dbPath := filepath.Join(dataDir, cfg.sqliteFilename())
		s, err := sqldbv2.NewSqliteStore(cfg.Sqlite, dbPath)
		if err != nil {
			return nil, fmt.Errorf("sqldb: open sqlite: %w", err)
		}
		store = s

	case BackendPostgres:
		s, err := sqldbv2.NewPostgresStore(cfg.Postgres)
		if err != nil {
			return nil, fmt.Errorf("sqldb: open postgres: %w", err)
		}
		store = s

	default:
		return nil, fmt.Errorf("sqldb: unknown backend %d", cfg.Backend)
	}

	if err := store.ExecuteMigrations(migSet); err != nil {
		_ = closeStore(store)
		return nil, fmt.Errorf("sqldb: run migrations: %w", err)
	}

	baseDB := store.GetBaseDB()

	headerExec := sqldbv2.NewTransactionExecutor[HeaderQueries](
		baseDB,
		func(tx *sql.Tx) HeaderQueries {
			return sqlc.New(tx)
		},
	)

	filterExec := sqldbv2.NewTransactionExecutor[FilterQueries](
		baseDB,
		func(tx *sql.Tx) FilterQueries {
			return sqlc.New(tx)
		},
	)

	banExec := sqldbv2.NewTransactionExecutor[BanQueries](
		baseDB,
		func(tx *sql.Tx) BanQueries {
			return sqlc.New(tx)
		},
	)

	return &Backend{
		store:      store,
		HeaderTxer: headerExec,
		FilterTxer: filterExec,
		BanTxer:    banExec,
	}, nil
}

// Close releases the underlying SQL resources. It is safe to call once at
// neutrino shutdown.
func (b *Backend) Close() error {
	if b == nil {
		return nil
	}

	return closeStore(b.store)
}

// closeStore closes the underlying *sql.DB held by a sqldb/v2 store. The
// sqldbv2.DB interface itself does not expose Close, but every concrete store
// embeds *BaseDB which embeds *sql.DB.
func closeStore(store sqldbv2.DB) error {
	if store == nil {
		return nil
	}

	base := store.GetBaseDB()
	if base == nil || base.DB == nil {
		return nil
	}

	return base.DB.Close()
}

// MakeProgrammaticMigrations is the type passed to NewBackend to register
// in-code migrations alongside the schema migrations under sqlc/migrations.
// It mirrors the sqldb/v2 MigrationSet field so the legacy importer can be
// implemented without leaking sqldb/v2 types into the higher-level neutrino
// API.
type MakeProgrammaticMigrations = func(*sqldbv2.BaseDB) (
	map[uint]migrate.ProgrammaticMigrEntry, error)
