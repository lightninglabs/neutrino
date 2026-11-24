package neutrino

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/lightninglabs/neutrino/sqldb"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"
)

// TestConnectOnlySkipsDNSSeeds checks that a service restricted to its
// ConnectPeers never looks up the network's DNS seeds. Nothing would use the
// addresses, and the lookups reveal the node to the seeders.
func TestConnectOnlySkipsDNSSeeds(t *testing.T) {
	t.Parallel()

	params := chaincfg.TestNet3Params
	require.NotEmpty(t, params.DNSSeeds)
	seeds := make(map[string]bool)
	for _, seed := range params.DNSSeeds {
		seeds[seed.Host] = true
	}

	var (
		mu      sync.Mutex
		lookups []string
	)
	cs, err := NewChainService(Config{
		DataDir:      t.TempDir(),
		ChainParams:  params,
		ConnectPeers: []string{"127.0.0.1:18333"},
		SQLConfig: &sqldb.Config{
			Backend: sqldb.BackendSqlite,
			Sqlite:  &sqldbv2.SqliteConfig{},
		},
		NameResolver: func(host string) ([]net.IP, error) {
			mu.Lock()
			lookups = append(lookups, host)
			mu.Unlock()

			return nil, errors.New("no lookups in this test")
		},
		Dialer: func(net.Addr) (net.Conn, error) {
			return nil, errors.New("no dials in this test")
		},
	})
	require.NoError(t, err)
	require.NoError(t, cs.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, cs.Stop()) })

	// Seeding starts as the peer handler comes up. Give it time to have
	// issued its lookups.
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for _, host := range lookups {
		require.False(t, seeds[host], "looked up DNS seed %v", host)
	}
}
