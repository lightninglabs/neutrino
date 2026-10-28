package banman_test

import (
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/stretchr/testify/require"
)

// createTestBanStore creates a test Store backed by a boltdb instance.
func createTestBanStore(t *testing.T) (banman.Store, func()) {
	t.Helper()

	dbDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("unable to create db dir: %v", err)
	}
	dbPath := filepath.Join(dbDir, "test.db")

	db, err := walletdb.Create("bdb", dbPath, true, time.Second*10, false)
	if err != nil {
		os.RemoveAll(dbDir)
		t.Fatalf("unable to create db: %v", err)
	}

	cleanUp := func() {
		db.Close()
		os.RemoveAll(dbDir)
	}

	banStore, err := banman.NewStore(db)
	if err != nil {
		cleanUp()
		t.Fatalf("unable to create ban store: %v", err)
	}

	return banStore, cleanUp
}

// TestBanStore ensures that the BanStore's state correctly reflects the
// BanStatus of IP networks.
func TestBanStore(t *testing.T) {
	t.Parallel()

	synctest.Test(t, testBanStore)
}

// testBanStore uses virtual time so whole-second expiration is deterministic.
func testBanStore(t *testing.T) {
	t.Helper()

	// We'll start by creating our test BanStore backed by a boltdb
	// instance.
	banStore, cleanUp := createTestBanStore(t)
	defer cleanUp()

	// checkBanStore is a helper closure to ensure to the IP network's ban
	// status is correctly reflected within the BanStore.
	checkBanStore := func(ipNet *net.IPNet, banned bool,
		reason banman.Reason, duration time.Duration) {

		t.Helper()

		banStatus, err := banStore.Status(ipNet)
		if err != nil {
			t.Fatalf("unable to determine %v's ban status: %v",
				ipNet, err)
		}
		if banned && !banStatus.Banned {
			t.Fatalf("expected %v to be banned", ipNet)
		}
		if !banned && banStatus.Banned {
			t.Fatalf("expected %v to not be banned", ipNet)
		}

		if !banned {
			return
		}

		if banStatus.Reason != reason {
			t.Fatalf("expected ban reason \"%v\", got \"%v\"",
				reason, banStatus.Reason)
		}
		banDuration := time.Until(banStatus.Expiration)
		if banDuration > duration {
			t.Fatalf("expected ban duration within %v, got %v",
				duration, banDuration)
		}
	}

	// We'll create two IP networks, the first banned for an hour and the
	// second for a second.
	addr1 := "127.0.0.1:8333"
	ipNet1, err := banman.ParseIPNet(addr1, nil)
	if err != nil {
		t.Fatalf("unable to parse IP network from %v: %v", addr1, err)
	}
	err = banStore.BanIPNet(ipNet1, banman.NoCompactFilters, time.Hour)
	if err != nil {
		t.Fatalf("unable to ban IP network: %v", err)
	}
	addr2 := "192.168.1.1:8333"
	ipNet2, err := banman.ParseIPNet(addr2, nil)
	if err != nil {
		t.Fatalf("unable to parse IP network from %v: %v", addr2, err)
	}
	err = banStore.BanIPNet(
		ipNet2, banman.ExceededBanThreshold, time.Second,
	)
	if err != nil {
		t.Fatalf("unable to ban IP network: %v", err)
	}

	// Both IP networks should be found within the BanStore with their
	// expected reason since their ban has yet to expire.
	checkBanStore(ipNet1, true, banman.NoCompactFilters, time.Hour)
	checkBanStore(ipNet2, true, banman.ExceededBanThreshold, time.Second)

	// Wait long enough for the second IP network's ban to expire.
	<-time.After(time.Second)

	// We should only find the first IP network within the BanStore.
	checkBanStore(ipNet1, true, banman.NoCompactFilters, time.Hour)
	checkBanStore(ipNet2, false, 0, 0)

	// We'll query for second IP network again as it should now be unknown
	// to the BanStore. We should expect not to find anything regarding it.
	checkBanStore(ipNet2, false, 0, 0)

	// Test UnbanIPNet.
	require.NoError(t, banStore.UnbanIPNet(ipNet1))

	// We would now check that ipNet1 is indeed unbanned.
	checkBanStore(ipNet1, false, 0, 0)
}

// TestBanStoreKey ensures that the BanStore's key-based APIs correctly
// persist IP address bans.
func TestBanStoreKey(t *testing.T) {
	t.Parallel()

	// Bans have whole-second expiration, so virtual time is what keeps the
	// expiry boundary deterministic: a real one-second sleep can be
	// starved past the deadline by the rest of the suite and then the ban
	// is already gone when it is read back.
	synctest.Test(t, testBanStoreKey)
}

// testBanStoreKey is the body of TestBanStoreKey.
func testBanStoreKey(t *testing.T) {
	t.Helper()

	banStore, cleanUp := createTestBanStore(t)
	defer cleanUp()

	key := &banman.Key{
		Net:  banman.NetworkIPv4,
		Addr: []byte{0x7f, 0x00, 0x00, 0x01},
		Mask: []byte{0xff, 0xff, 0xff, 0xff},
	}

	err := banStore.BanKey(
		key, banman.InvalidFilterHeader, time.Second,
	)
	require.NoError(t, err)

	status, err := banStore.StatusKey(key)
	require.NoError(t, err)
	require.True(t, status.Banned)
	require.Equal(t, banman.InvalidFilterHeader, status.Reason)
	require.LessOrEqual(t, time.Until(status.Expiration), time.Second)

	<-time.After(time.Second)

	status, err = banStore.StatusKey(key)
	require.NoError(t, err)
	require.False(t, status.Banned)

	err = banStore.BanKey(
		key, banman.InvalidFilterHeaderCheckpoint, time.Hour,
	)
	require.NoError(t, err)
	require.NoError(t, banStore.UnbanKey(key))

	status, err = banStore.StatusKey(key)
	require.NoError(t, err)
	require.False(t, status.Banned)
}

// TestBanStoreTorV3Key ensures that the BanStore's key-based APIs correctly
// persist overlay address bans.
func TestBanStoreTorV3Key(t *testing.T) {
	t.Parallel()

	synctest.Test(t, testBanStoreTorV3Key)
}

// testBanStoreTorV3Key is the body of TestBanStoreTorV3Key.
func testBanStoreTorV3Key(t *testing.T) {
	t.Helper()

	banStore, cleanUp := createTestBanStore(t)
	defer cleanUp()

	key := &banman.Key{
		Net: banman.NetworkTorV3,
		Addr: []byte{
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
			0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
			0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
			0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
		},
	}

	err := banStore.BanKey(
		key, banman.InvalidFilterHeader, time.Second,
	)
	require.NoError(t, err)

	status, err := banStore.StatusKey(key)
	require.NoError(t, err)
	require.True(t, status.Banned)
	require.Equal(t, banman.InvalidFilterHeader, status.Reason)
	require.LessOrEqual(t, time.Until(status.Expiration), time.Second)

	<-time.After(time.Second)

	status, err = banStore.StatusKey(key)
	require.NoError(t, err)
	require.False(t, status.Banned)

	err = banStore.BanKey(
		key, banman.InvalidFilterHeaderCheckpoint, time.Hour,
	)
	require.NoError(t, err)
	require.NoError(t, banStore.UnbanKey(key))

	status, err = banStore.StatusKey(key)
	require.NoError(t, err)
	require.False(t, status.Banned)
}

// TestIPv4OneBanCoversEverySpelling is the regression test for the reason the
// record width had to change, and it goes through the store rather than the
// codec so it asserts what a caller actually observes.
//
// Before ban keys, a ban recorded through the 16-byte-mask spelling of a
// v4 address was invisible to Status through the plain spelling of the same
// peer: the two spellings were two separate records. Each record round-tripped
// on its own, which is why this went unnoticed, but a peer could step out of a
// ban by reconnecting under the other spelling. One peer must have one record.
func TestIPv4OneBanCoversEverySpelling(t *testing.T) {
	t.Parallel()

	for name, makeStore := range banStoreFactories() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := makeStore(t)

			// Ban through the wide-mask spelling, which is what
			// net.ParseCIDR hands out for a v4-mapped address.
			_, mapped, err := net.ParseCIDR("::ffff:1.2.3.4/128")
			require.NoError(t, err)

			wide, err := banman.ParseIPNet(
				mapped.IP.String(), mapped.Mask,
			)
			require.NoError(t, err)
			require.NoError(t, store.BanIPNet(
				wide, banman.NoCompactFilters, time.Hour,
			))

			// Every other spelling of the same peer must observe
			// that ban.
			for _, addr := range []string{
				"1.2.3.4:8333",
				"1.2.3.4",
				"[::ffff:1.2.3.4]:8333",
				"::ffff:1.2.3.4",
			} {
				plain, err := banman.ParseIPNet(addr, nil)
				require.NoError(t, err)

				status, err := store.Status(plain)
				require.NoError(t, err)
				require.True(t, status.Banned,
					"ban must cover %v", addr)
			}

			// And clearing it must clear it for all of them.
			require.NoError(t, store.UnbanIPNet(wide))

			plain, err := banman.ParseIPNet("1.2.3.4:8333", nil)
			require.NoError(t, err)

			status, err := store.Status(plain)
			require.NoError(t, err)
			require.False(t, status.Banned)
		})
	}
}
