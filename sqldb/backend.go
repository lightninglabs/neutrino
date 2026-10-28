package sqldb

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
		if err := checkSqliteFileIsOurs(dbPath); err != nil {
			return nil, err
		}

		s, err := sqldbv2.NewSqliteStore(cfg.sqliteConfig(), dbPath)
		if err != nil {
			return nil, fmt.Errorf("sqldb: open sqlite: %w", err)
		}
		store = s
		if !cfg.SkipSqliteChainPragmas {
			// lnd sqldb/v2 currently adds fullfsync=true to the DSN
			// before appending caller-supplied pragmas, and modernc
			// does not let the later duplicate fullfsync=false
			// override it.
			// Keep the rest of the chain profile in SqliteConfig
			// PragmaOptions, but flip this one post-open until
			// sqldb/v2
			// grows a typed knob for the chain-store profile.
			if _, err := s.GetBaseDB().DB.Exec(
				"PRAGMA fullfsync=false",
			); err != nil {
				_ = closeStore(store)
				return nil, fmt.Errorf("sqldb: disable sqlite "+
					"fullfsync: %w", err)
			}

			// The pragma above only applies to the connection it
			// ran on, and sqldb/v2 recycles connections after
			// ten minutes, handing out fresh ones that default
			// to fullfsync=true. Keep the pooled connection for
			// the life of the store.
			db := s.GetBaseDB().DB
			db.SetConnMaxLifetime(0)
			db.SetConnMaxIdleTime(0)
		}

	case BackendPostgres:
		s, err := sqldbv2.NewPostgresStore(cfg.Postgres)
		if err != nil {
			return nil, fmt.Errorf("sqldb: open postgres: %w", err)
		}
		store = s

	default:
		return nil, fmt.Errorf("sqldb: unknown backend %d", cfg.Backend)
	}

	// A hard kill during the legacy import leaves migration version 2
	// marked dirty, which would refuse every later start. The import is
	// idempotent, so rewind it and let it run again.
	if err := resetDirtyLegacyImport(store.GetBaseDB()); err != nil {
		_ = closeStore(store)
		return nil, err
	}

	if err := store.ExecuteMigrations(migSet); err != nil {
		_ = closeStore(store)
		return nil, fmt.Errorf("sqldb: run migrations: %w", err)
	}

	log.Debugf("Opened neutrino SQL backend (backend=%d)", cfg.Backend)

	baseDB := store.GetBaseDB()
	headerQueryFactory := func(tx *sql.Tx) HeaderQueries {
		return sqlc.New(tx)
	}
	if cfg.Backend == BackendSqlite {
		headerQueryFactory = func(tx *sql.Tx) HeaderQueries {
			return NewSQLiteHeaderQueries(tx)
		}
	}

	headerExec := sqldbv2.NewTransactionExecutor[HeaderQueries](
		baseDB, headerQueryFactory,
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

// neutrinoSqliteTables lists every table a neutrino SQL database may contain.
// migration_tracker and schema_migrations are created by sqldb/v2 and
// golang-migrate respectively.
var neutrinoSqliteTables = map[string]struct{}{
	"migration_tracker": {},
	"schema_migrations": {},
	trackingTable:       {},
	"chain_tips":        {},
	"block_headers":     {},
	"filter_headers":    {},
	"regular_filters":   {},
	"banned_peers":      {},
}

// checkSqliteFileIsOurs refuses to run neutrino's migrations against an
// existing SQLite file that holds anything but neutrino tables, such as the
// key-value database lnd keeps for its own neutrino backend. A missing or
// empty file is fine.
func checkSqliteFileIsOurs(path string) error {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("sqldb: stat %s: %w", path, err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("sqldb: inspect %s: %w", path, err)
	}
	defer db.Close()

	rows, err := db.Query(
		"SELECT name FROM sqlite_master WHERE type = 'table' " +
			"AND name NOT LIKE 'sqlite_%'",
	)
	if err != nil {
		return fmt.Errorf("sqldb: inspect %s: %w", path, err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if _, ok := neutrinoSqliteTables[name]; !ok {
			return fmt.Errorf("sqldb: %s already holds a table %q "+
				"that does not belong to neutrino's SQL "+
				"schema; it is probably another database, set "+
				"Config.SqliteFilename to use a different file",
				path, name)
		}
	}

	return rows.Err()
}

// resetDirtyLegacyImport rewinds a dirty migration version 2 to version 1.
// The legacy import is the only migration that can be interrupted mid-way
// without returning an error, and it starts over from a clean slate.
func resetDirtyLegacyImport(base *sqldbv2.BaseDB) error {
	_, err := base.Exec(fmt.Sprintf(
		"UPDATE %s SET version = 1, dirty = %s "+
			"WHERE version = 2 AND dirty = %s",
		trackingTable, dirtyLiteral(base, false),
		dirtyLiteral(base, true),
	))
	if err == nil {
		return nil
	}

	// A brand new database has no tracking table yet.
	msg := err.Error()
	if strings.Contains(msg, "no such table") ||
		strings.Contains(msg, "does not exist") {

		return nil
	}

	return fmt.Errorf("sqldb: reset dirty legacy import: %w", err)
}

// dirtyLiteral returns the SQL literal for the dirty flag on base's dialect.
func dirtyLiteral(base *sqldbv2.BaseDB, v bool) string {
	if base.BackendType == sqldbv2.BackendTypePostgres {
		return strconv.FormatBool(v)
	}
	if v {
		return "1"
	}

	return "0"
}
