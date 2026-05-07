package banman_test

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/sqldb"
)

// newSQLBanStore constructs a SQLBanStore backed by a fresh in-memory SQLite
// database. The Backend is closed automatically at the end of the test.
func newSQLBanStore(t *testing.T) *banman.SQLBanStore {
	t.Helper()

	backend := sqldb.NewTestBackend(t)
	store, err := banman.NewSQLStore(backend.BanTxer)
	require.NoError(t, err)
	return store
}

// banStoreFactory wires a banman.Store implementation up for parameterized
// tests so the same assertion bodies cover both backends. The kvdb factory
// is the existing createTestBanStore from store_test.go.
type banStoreFactory func(t *testing.T) banman.Store

func banStoreFactories() map[string]banStoreFactory {
	return map[string]banStoreFactory{
		"kvdb": func(t *testing.T) banman.Store {
			store, cleanup := createTestBanStore(t)
			t.Cleanup(cleanup)
			return store
		},
		"sqlite": func(t *testing.T) banman.Store {
			return newSQLBanStore(t)
		},
	}
}

// TestBanStoreBackends runs the canonical BanStore lifecycle against every
// supported backend so behaviour parity is verified at the test level.
func TestBanStoreBackends(t *testing.T) {
	t.Parallel()

	for name, makeStore := range banStoreFactories() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testBanStoreLifecycle(t, makeStore(t))
		})
	}
}

// testBanStoreLifecycle is the shared body of the lifecycle test. It exercises
// banning, expiration sweep on read, repeated bans (idempotency), and
// explicit unban.
func testBanStoreLifecycle(t *testing.T, store banman.Store) {
	t.Helper()

	check := func(ipNet *net.IPNet, banned bool, reason banman.Reason,
		within time.Duration) {

		t.Helper()
		status, err := store.Status(ipNet)
		require.NoError(t, err)
		require.Equal(t, banned, status.Banned)

		if !banned {
			return
		}

		require.Equal(t, reason, status.Reason)
		remaining := time.Until(status.Expiration)
		require.LessOrEqual(t, remaining, within)
	}

	ipNet1, err := banman.ParseIPNet("127.0.0.1:8333", nil)
	require.NoError(t, err)
	require.NoError(
		t,
		store.BanIPNet(ipNet1, banman.NoCompactFilters, time.Hour),
	)

	ipNet2, err := banman.ParseIPNet("192.168.1.1:8333", nil)
	require.NoError(t, err)
	require.NoError(
		t, store.BanIPNet(
			ipNet2, banman.ExceededBanThreshold, time.Second,
		),
	)

	check(ipNet1, true, banman.NoCompactFilters, time.Hour)
	check(ipNet2, true, banman.ExceededBanThreshold, time.Second)

	// Wait for the second ban to elapse, then verify it has been swept.
	time.Sleep(1100 * time.Millisecond)
	check(ipNet1, true, banman.NoCompactFilters, time.Hour)
	check(ipNet2, false, 0, 0)

	// Repeating Status on a swept entry must continue to report unbanned.
	check(ipNet2, false, 0, 0)

	// Explicit unban removes the active entry.
	require.NoError(t, store.UnbanIPNet(ipNet1))
	check(ipNet1, false, 0, 0)
}

// TestSQLBanIdempotent verifies that repeated BanIPNet calls for the same
// network refresh the existing row instead of inserting a duplicate.
func TestSQLBanIdempotent(t *testing.T) {
	t.Parallel()

	store := newSQLBanStore(t)
	ipNet, err := banman.ParseIPNet("10.0.0.1:8333", nil)
	require.NoError(t, err)

	require.NoError(t,
		store.BanIPNet(ipNet, banman.ExceededBanThreshold, time.Hour),
	)
	first, err := store.Status(ipNet)
	require.NoError(t, err)
	require.True(t, first.Banned)

	// Re-ban with a different reason and a longer duration. The row must
	// be updated in place.
	require.NoError(t, store.BanIPNet(
		ipNet, banman.InvalidBlock, 2*time.Hour,
	))
	second, err := store.Status(ipNet)
	require.NoError(t, err)
	require.True(t, second.Banned)
	require.Equal(t, banman.InvalidBlock, second.Reason)
	require.True(t, second.Expiration.After(first.Expiration))
}

// TestSQLBanIPv6 verifies that IPv6 addresses round-trip through the SQL
// backend with the binary IPNet encoding intact.
func TestSQLBanIPv6(t *testing.T) {
	t.Parallel()

	store := newSQLBanStore(t)
	ipNet, err := banman.ParseIPNet("[2001:db8::1]:8333", nil)
	require.NoError(t, err)

	require.NoError(
		t,
		store.BanIPNet(ipNet, banman.NoCompactFilters, time.Minute),
	)
	status, err := store.Status(ipNet)
	require.NoError(t, err)
	require.True(t, status.Banned)
	require.Equal(t, banman.NoCompactFilters, status.Reason)
}

// TestSQLBanExpirationSweep verifies that a ban whose duration is already
// non-positive is treated as expired the first time Status is read.
func TestSQLBanExpirationSweep(t *testing.T) {
	t.Parallel()

	store := newSQLBanStore(t)
	ipNet, err := banman.ParseIPNet("10.0.0.2:8333", nil)
	require.NoError(t, err)

	// Ban for a tiny duration so expiration is effectively immediate.
	require.NoError(
		t,
		store.BanIPNet(ipNet, banman.ExceededBanThreshold, 0),
	)

	status, err := store.Status(ipNet)
	require.NoError(t, err)
	require.False(t, status.Banned)

	// A second Status should also report unbanned (the row was removed).
	status, err = store.Status(ipNet)
	require.NoError(t, err)
	require.False(t, status.Banned)
}

// TestSQLBanPurgeExpired verifies the bulk-sweep helper.
func TestSQLBanPurgeExpired(t *testing.T) {
	t.Parallel()

	store := newSQLBanStore(t)

	for i, addr := range []string{
		"10.0.0.10:8333", "10.0.0.11:8333", "10.0.0.12:8333",
	} {
		ipNet, err := banman.ParseIPNet(addr, nil)
		require.NoError(t, err)

		// Half are already expired, half are not.
		duration := -time.Hour
		if i%2 == 0 {
			duration = time.Hour
		}
		require.NoError(t, store.BanIPNet(
			ipNet, banman.NoCompactFilters, duration,
		))
	}

	purged, err := store.PurgeExpired()
	require.NoError(t, err)
	require.Equal(t, int64(1), purged)
}
