package banman_test

import (
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ltcsuite/ltcwallet/walletdb"
	_ "github.com/ltcsuite/ltcwallet/walletdb/bdb"
	"github.com/ltcsuite/neutrino/banman"
)

// createTestBanStore creates a test Store backed by a boltdb instance.
func createTestBanStore(t *testing.T) (banman.Store, func()) {
	t.Helper()

	dbDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("unable to create db dir: %v", err)
	}
	dbPath := filepath.Join(dbDir, "test.db")

	db, err := walletdb.Create("bdb", dbPath)
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

		if banned {
			return
		}

		if banStatus.Reason != reason {
			t.Fatalf("expected ban reason \"%v\", got \"%v\"",
				reason, banStatus.Reason)
		}
		banDuration := banStatus.Expiration.Sub(time.Now())
		if banStatus.Expiration.Sub(time.Now()) > duration {
			t.Fatalf("expected ban duration to be within %v, got %v",
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
	err = banStore.BanIPNet(ipNet2, banman.ExceededBanThreshold, time.Second)
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
}
