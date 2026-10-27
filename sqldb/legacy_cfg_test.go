package sqldb_test

import (
	"net/url"
	"sync"
	"testing"

	"github.com/lightninglabs/neutrino/sqldb"
)

// testConfigs remembers the SQL config handed out per data directory so that
// reopening a data directory within a test reuses the same database.
var testConfigs sync.Map

// configFor returns the SQL config for dataDir, creating it on first use.
func configFor(t testing.TB, dataDir string) *sqldb.Config {
	t.Helper()

	if cfg, ok := testConfigs.Load(dataDir); ok {
		c := *cfg.(*sqldb.Config)
		return &c
	}

	cfg := newTestConfig(t)
	testConfigs.Store(dataDir, cfg)

	c := *cfg

	return &c
}

// replaceDSNDatabase returns dsn with its database name replaced.
func replaceDSNDatabase(dsn, dbName string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + dbName

	return u.String()
}
