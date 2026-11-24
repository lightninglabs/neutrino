package neutrino

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRemovePermanentPeerNotConnected checks that a permanent peer can be
// removed while it isn't connected, whether its dials fail, its connections
// drop right away, or its handshake never finishes, and that it isn't dialed
// again once removed.
func TestRemovePermanentPeerNotConnected(t *testing.T) {
	modes := map[string]dialMode{
		"dial fails":       dialRefuse,
		"connection drops": dialHangUp,
		"handshake stalls": dialSilent,
	}
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			n := newTestPeerNet(t)
			n.setMode(testPeerAddr, mode)
			s := newTestService(t, n)

			waitForDial(t, n, testPeerAddr)

			require.NoError(t, s.RemoveNodeByAddr(testPeerAddr))
			n.requireNoMoreDials(testPeerAddr)
			n.requireNoOpenConns()
			require.Empty(t, s.Peers())

			// The peer is gone, so removing it again fails.
			require.Error(t, s.RemoveNodeByAddr(testPeerAddr))
		})
	}
}

// TestRemovePermanentPeerDuringDial checks that removing a permanent peer
// whose dial is in flight ends its request: once the dial fails, the peer
// isn't dialed again.
func TestRemovePermanentPeerDuringDial(t *testing.T) {
	n := newTestPeerNet(t)
	release := n.hold(testPeerAddr)
	t.Cleanup(release)
	s := newTestService(t, n)

	waitForDial(t, n, testPeerAddr)
	require.NoError(t, s.RemoveNodeByAddr(testPeerAddr))
	release()

	n.requireNoMoreDials(testPeerAddr)
	require.Equal(t, 1, n.dialCount(testPeerAddr))
}

// TestRemovePermanentPeerJustAdded checks that a permanent peer removed right
// after it's added, possibly before the connection manager has registered its
// request, is dialed at most once, by a dial already under way.
func TestRemovePermanentPeerJustAdded(t *testing.T) {
	const addr = "127.0.0.2:18555"

	n := newTestPeerNet(t)
	s := newTestService(t, n)

	for range 20 {
		require.NoError(t, s.ConnectNode(addr, true))
		require.NoError(t, s.RemoveNodeByAddr(addr))
	}

	// Each request is dialed at most once. One that kept its retries
	// would be dialed every retry interval.
	require.Never(t, func() bool {
		return n.dialCount(addr) > 20
	}, quietPeriod, testRetryInterval/2)
}

// TestRemovePermanentPeerConnected checks that removing a connected
// permanent peer disconnects it for good.
func TestRemovePermanentPeerConnected(t *testing.T) {
	n := newTestPeerNet(t)
	n.setMode(testPeerAddr, dialServe)
	s := newTestService(t, n)

	waitForPeer(t, s, testPeerAddr)
	require.NoError(t, s.RemoveNodeByAddr(testPeerAddr))

	n.requireNoMoreDials(testPeerAddr)
	n.requireNoOpenConns()
	require.Empty(t, s.Peers())
	require.Equal(t, 1, n.dialCount(testPeerAddr))
}

// TestRemovePermanentPeerByID checks that a connected permanent peer can be
// removed by its ID.
func TestRemovePermanentPeerByID(t *testing.T) {
	n := newTestPeerNet(t)
	n.setMode(testPeerAddr, dialServe)
	s := newTestService(t, n)

	sp := waitForPeer(t, s, testPeerAddr)
	require.NoError(t, s.RemoveNodeByID(sp.ID()))

	n.requireNoMoreDials(testPeerAddr)
	n.requireNoOpenConns()
	require.Error(t, s.RemoveNodeByAddr(testPeerAddr))
}

// TestConnectNodeRejectsDuplicatePermanentPeer checks that while a permanent
// peer's dial is in flight, ConnectNode refuses to add the address again, so
// there's only ever one dial to it.
func TestConnectNodeRejectsDuplicatePermanentPeer(t *testing.T) {
	n := newTestPeerNet(t)
	release := n.hold(testPeerAddr)
	t.Cleanup(release)
	s := newTestService(t, n)

	waitForDial(t, n, testPeerAddr)
	require.Error(t, s.ConnectNode(testPeerAddr, true))
	require.Error(t, s.ConnectNode(testPeerAddr, false))

	// Give a second dial time to start, were there one.
	require.Never(t, func() bool {
		return n.maxConcurrentDials(testPeerAddr) > 1
	}, quietPeriod, testRetryInterval/2)
}

// TestPermanentPeerAddressForms checks that a permanent peer is matched by
// its resolved address, so a hostname, its IP, and an address with or
// without the default port all name the same peer.
func TestPermanentPeerAddressForms(t *testing.T) {
	// peer.test resolves to 127.0.0.3, and simnet's default port is
	// 18555.
	const resolved = "127.0.0.3:18555"

	n := newTestPeerNet(t)
	n.setMode(resolved, dialServe)
	s := newTestService(t, n)

	require.NoError(t, s.ConnectNode("peer.test", true))
	waitForPeer(t, s, resolved)

	for _, addr := range []string{
		"peer.test", "peer.test:18555", "127.0.0.3", resolved,
	} {
		require.Error(t, s.ConnectNode(addr, true), addr)
	}

	require.NoError(t, s.RemoveNodeByAddr("peer.test:18555"))
	n.requireNoMoreDials(resolved)
	require.Equal(t, 1, n.dialCount(resolved))

	// An onion address gets the default port too, and its name is
	// case insensitive.
	const onion = "duckduckgogg42xjoc72x3sjasowoarfbgcmvfimaftt6twa" +
		"gswzczad.onion"
	require.NoError(t, s.ConnectNode(onion, true))
	require.Error(t, s.ConnectNode(
		"DUCKDUCKGOGG42XJOC72X3SJASOWOARFBGCMVFIMAFTT6TWAGSWZ"+
			"CZAD.onion:18555", true,
	))
	require.NoError(t, s.RemoveNodeByAddr(onion+":18555"))
}

// TestRemovePermanentPeerDuringHandshake checks that a permanent peer removed
// while its handshake is in flight doesn't join once the handshake finishes.
func TestRemovePermanentPeerDuringHandshake(t *testing.T) {
	n := newTestPeerNet(t)
	n.setMode(testPeerAddr, dialServe)
	answer := n.gate(testPeerAddr)
	t.Cleanup(answer)
	s := newTestService(t, n)

	require.Eventually(t, func() bool {
		return n.open.Load() == 1
	}, 5*time.Second, time.Millisecond)

	require.NoError(t, s.RemoveNodeByAddr(testPeerAddr))
	answer()

	n.requireNoOpenConns()
	n.requireNoMoreDials(testPeerAddr)
	require.Empty(t, s.Peers())
}
