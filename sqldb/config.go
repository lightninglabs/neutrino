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

	// Sqlite must be populated when Backend == BackendSqlite. It is
	// passed verbatim to the lnd sqldb/v2 SQLite store factory.
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
