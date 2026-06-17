package sqldb

import (
	"errors"
	"fmt"

	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
)

// BackendType identifies which SQL engine the SQL config selects.
type BackendType uint8

const (
	// BackendSqlite selects the embedded SQLite engine. It does not
	// require any external service to be running.
	BackendSqlite BackendType = 0

	// BackendPostgres selects an external PostgreSQL server. The Postgres
	// field must be populated with a valid DSN.
	BackendPostgres BackendType = 1
)

// DefaultSqliteFilename is the default file name used when the caller does
// not override Config.SqliteFilename. It lives under the neutrino DataDir.
const DefaultSqliteFilename = "neutrino.sqlite"

// Config selects and configures the SQL backend to use for neutrino's
// persistent state. It is intended to be embedded inside neutrino.Config as
// a pointer; a nil value selects the legacy walletdb path.
type Config struct {
	// Backend selects which SQL engine to instantiate.
	Backend BackendType

	// Sqlite must be populated when Backend == BackendSqlite. Neutrino
	// appends chain-store pragma defaults before passing it to the lnd
	// sqldb/v2 SQLite store factory unless SkipSqliteChainPragmas is set.
	Sqlite *sqldbv2.SqliteConfig

	// Postgres must be populated when Backend == BackendPostgres. It is
	// passed verbatim to the lnd sqldb/v2 PostgreSQL store factory.
	Postgres *sqldbv2.PostgresConfig

	// SqliteFilename overrides the default SQLite database file name.
	// The file is created under the neutrino DataDir. Ignored for the
	// PostgreSQL backend.
	SqliteFilename string

	// SkipLegacyMigration disables the one-shot programmatic migration
	// from the legacy walletdb + flat-file state into the SQL backend.
	// Useful for tests and for callers that have already migrated.
	SkipLegacyMigration bool

	// SkipSqliteChainPragmas leaves the supplied SQLite pragma options
	// untouched. By default, neutrino applies a chain-data profile that
	// favors fast, consistent rebuildable sync state over wallet-grade
	// power-loss durability.
	SkipSqliteChainPragmas bool
}

// Validate sanity-checks the SQL configuration. It returns a non-nil error if
// the configuration cannot be used to construct a Backend.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("nil sqldb.Config")
	}

	switch c.Backend {
	case BackendSqlite:
		if c.Sqlite == nil {
			return errors.New("sqldb: sqlite backend selected " +
				"but Sqlite config is nil")
		}

	case BackendPostgres:
		if c.Postgres == nil {
			return errors.New("sqldb: postgres backend selected " +
				"but Postgres config is nil")
		}

		if err := c.Postgres.Validate(); err != nil {
			return fmt.Errorf("sqldb: invalid postgres "+
				"config: %w", err)
		}

	default:
		return fmt.Errorf("sqldb: unknown backend type %d", c.Backend)
	}

	return nil
}

// sqliteFilename returns the configured SQLite filename, falling back to the
// package default when unset.
func (c *Config) sqliteFilename() string {
	if c.SqliteFilename != "" {
		return c.SqliteFilename
	}

	return DefaultSqliteFilename
}

func (c *Config) sqliteConfig() *sqldbv2.SqliteConfig {
	if c.SkipSqliteChainPragmas {
		return c.Sqlite
	}

	cfg := *c.Sqlite
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 1
	}
	if cfg.MaxIdleConnections == 0 {
		cfg.MaxIdleConnections = 1
	}
	cfg.PragmaOptions = append(
		append([]string(nil), c.Sqlite.PragmaOptions...),
		defaultSqliteChainPragmas()...,
	)

	return &cfg
}

func defaultSqliteChainPragmas() []string {
	return []string{
		// WAL+NORMAL remains atomic and consistent and is safe across
		// application crashes. A power loss may lose recent commits, but
		// header/filter state can be redownloaded from peers.
		"synchronous=NORMAL",

		// sqldb/v2 currently hardcodes fullfsync=true in the SQLite DSN.
		// Keep checkpoint_fullfsync disabled here so the chain profile is
		// ready once sqldb/v2 grows a typed no-fullfsync knob.
		"checkpoint_fullfsync=false",

		// Keep WAL growth bounded without forcing tiny 1,000-page
		// checkpoints during header/cfheader ingest.
		"wal_autocheckpoint=50000",
		"journal_size_limit=268435456",

		// Initial sync is an append-heavy workload with frequent header
		// lookups. Keep sqlite's temporary structures in memory and give
		// the connection enough cache/mmap room to avoid unnecessary pager
		// churn while preserving the on-disk format.
		"temp_store=MEMORY",
		"cache_size=-131072",
		"mmap_size=268435456",
	}
}
