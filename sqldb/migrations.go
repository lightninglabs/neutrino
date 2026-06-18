package sqldb

import (
	"embed"

	"github.com/golang-migrate/migrate/v4"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
)

// migrationsFS embeds every SQL migration file used by the neutrino SQL
// backend. The directory layout matches the SQLFileDirectory passed into the
// MigrationSet so golang-migrate can walk it via the httpfs source.
//
//go:embed sqlc/migrations/*.sql
var migrationsFS embed.FS

// trackingTable is the table name golang-migrate uses to record the current
// schema version of the neutrino SQL backend.
const trackingTable = "neutrino_migrations"

// LatestSchemaVersion is the highest schema_version applied by an SQL
// migration file under sqlc/migrations.
const LatestSchemaVersion = 1

// LatestMigrationVersion is the highest global migration version (the union
// of SQL and programmatic migrations). Each entry in MigrationSet.Descriptors
// counts toward this bound; downgrade protection refuses to start when the
// on-disk version exceeds this constant.
const LatestMigrationVersion = 1

// MigrationSet returns the canonical migration set for the neutrino SQL
// backend. The optional makeProgrammatic argument lets callers register
// in-code migrations alongside the embedded schema migrations. Pass nil
// when no programmatic migrations are wired.
func MigrationSet(makeProgrammatic func(*sqldbv2.BaseDB) (
	map[uint]migrate.ProgrammaticMigrEntry, error)) sqldbv2.MigrationSet {

	return sqldbv2.MigrationSet{
		TrackingTableName:          trackingTable,
		SQLFiles:                   migrationsFS,
		SQLFileDirectory:           "sqlc/migrations",
		MakeProgrammaticMigrations: makeProgrammatic,
		LatestMigrationVersion:     LatestMigrationVersion,
		Descriptors: []sqldbv2.MigrationDescriptor{
			{
				Name:          "init",
				Version:       1,
				SchemaVersion: 1,
			},
		},
	}
}
