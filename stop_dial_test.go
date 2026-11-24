package neutrino

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	"github.com/stretchr/testify/require"
)

// TestStopDoesNotWaitForPendingDial checks that Stop returns while a dial to
// a permanent peer is still pending, rather than waiting for the dialer to
// give up (lnd#7349).
func TestStopDoesNotWaitForPendingDial(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(dir, "neutrino.db"), true,
		10*time.Second, false,
	)
	require.NoError(t, err)
	defer db.Close()

	// The dialer blocks until the test is over, the way a dial through
	// a proxy to an unreachable host blocks until its timeout.
	dialing := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	dialer := func(net.Addr) (net.Conn, error) {
		select {
		case dialing <- struct{}{}:
		default:
		}
		<-release

		return nil, net.ErrClosed
	}

	service, err := NewChainService(Config{
		DataDir:      dir,
		Database:     db,
		ChainParams:  chaincfg.SimNetParams,
		ConnectPeers: []string{"127.0.0.1:18555"},
		Dialer:       dialer,
	})
	require.NoError(t, err)

	// The connection manager only accepts the permanent peer's request
	// once it's running.
	require.NoError(t, service.Start(t.Context()))

	select {
	case <-dialing:
	case <-time.After(10 * time.Second):
		t.Fatal("permanent peer was never dialed")
	}

	stopped := make(chan error, 1)
	go func() {
		stopped <- service.Stop()
	}()

	select {
	case err := <-stopped:
		require.NoError(t, err)

	case <-time.After(10 * time.Second):
		t.Fatal("Stop is blocked on a pending dial")
	}
}

// TestStopDoesNotWaitForPendingLookup checks that Stop returns while the name
// lookup of a permanent peer given by hostname is still pending, the same as
// for a pending dial.
func TestStopDoesNotWaitForPendingLookup(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(dir, "neutrino.db"), true,
		10*time.Second, false,
	)
	require.NoError(t, err)
	defer db.Close()

	// The resolver blocks until the test is over, the way a lookup
	// through a slow proxy does.
	looking := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	resolver := func(string) ([]net.IP, error) {
		select {
		case looking <- struct{}{}:
		default:
		}
		<-release

		return nil, net.ErrClosed
	}

	service, err := NewChainService(Config{
		DataDir:      dir,
		Database:     db,
		ChainParams:  chaincfg.SimNetParams,
		ConnectPeers: []string{"peer.example:18555"},
		NameResolver: resolver,
	})
	require.NoError(t, err)
	require.NoError(t, service.Start(t.Context()))

	select {
	case <-looking:
	case <-time.After(10 * time.Second):
		t.Fatal("permanent peer was never looked up")
	}

	stopped := make(chan error, 1)
	go func() {
		stopped <- service.Stop()
	}()

	select {
	case err := <-stopped:
		require.NoError(t, err)

	case <-time.After(10 * time.Second):
		t.Fatal("Stop is blocked on a pending lookup")
	}
}
