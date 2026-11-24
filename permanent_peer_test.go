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

// TestConnectNodeMakesOneOffPeerPermanent checks that ConnectNode(addr, true)
// for a connected one-off peer makes it permanent, keeping its connection,
// rather than dialing the peer again and dropping the duplicate connection
// over and over.
func TestConnectNodeMakesOneOffPeerPermanent(t *testing.T) {
	const addr = "127.0.0.2:18555"

	n := newTestPeerNet(t)
	n.setMode(addr, dialServe)
	s := newTestService(t, n)

	require.NoError(t, s.ConnectNode(addr, false))
	sp := waitForPeer(t, s, addr)

	require.NoError(t, s.ConnectNode(addr, true))
	require.Error(t, s.ConnectNode(addr, true))
	n.requireNoMoreDials(addr)
	require.Equal(t, 1, n.dialCount(addr))
	require.Same(t, sp, s.PeerByAddr(addr))

	// Once its connection is lost, the peer is reconnected as a
	// permanent one.
	sp.Disconnect()
	require.Eventually(t, func() bool {
		next := s.PeerByAddr(addr)
		return next != nil && next != sp
	}, 10*time.Second, 5*time.Millisecond)
	require.Equal(t, 2, n.dialCount(addr))

	require.NoError(t, s.RemoveNodeByAddr(addr))
	n.requireNoMoreDials(addr)
	require.Empty(t, s.Peers())
}

// TestPermanentPeerTakesPlaceOfOneOffPeer checks that a permanent peer that
// connects while we're at MaxPeers takes the place of a one-off peer, rather
// than being refused and redialed over and over.
func TestPermanentPeerTakesPlaceOfOneOffPeer(t *testing.T) {
	const oneOff = "127.0.0.2:18555"

	oldMax, oldTarget := MaxPeers, TargetOutbound
	MaxPeers = 1
	t.Cleanup(func() { MaxPeers, TargetOutbound = oldMax, oldTarget })

	n := newTestPeerNet(t)
	n.setMode(oneOff, dialServe)
	s := newTestService(t, n)

	// The permanent peer is unreachable until the one-off peer fills
	// the only place.
	require.NoError(t, s.ConnectNode(oneOff, false))
	waitForPeer(t, s, oneOff)
	n.setMode(testPeerAddr, dialServe)

	waitForPeer(t, s, testPeerAddr)
	n.requireNoMoreDials(testPeerAddr)
	require.Nil(t, s.PeerByAddr(oneOff))
	require.Len(t, s.Peers(), 1)

	// With no one-off peer left, another permanent peer goes above
	// MaxPeers.
	require.NoError(t, s.ConnectNode(oneOff, true))
	waitForPeer(t, s, oneOff)
	n.requireNoMoreDials(oneOff)
	require.Len(t, s.Peers(), 2)
}

// TestPermanentPeersAboveMaxPeers checks that every configured peer is
// connected to and kept, even when there are more of them than MaxPeers.
func TestPermanentPeersAboveMaxPeers(t *testing.T) {
	others := []string{"127.0.0.2:18555", "127.0.0.3:18555"}
	all := append([]string{testPeerAddr}, others...)

	oldMax, oldTarget := MaxPeers, TargetOutbound
	MaxPeers = 1
	t.Cleanup(func() { MaxPeers, TargetOutbound = oldMax, oldTarget })

	n := newTestPeerNet(t)
	for _, addr := range all {
		n.setMode(addr, dialServe)
	}
	s := newTestService(t, n, others...)

	for _, addr := range all {
		waitForPeer(t, s, addr)
	}
	for _, addr := range all {
		n.requireNoMoreDials(addr)
		require.Equal(t, 1, n.dialCount(addr), addr)
	}
	require.Len(t, s.Peers(), len(all))
}

// TestReAddPermanentPeerWhileItShutsDown checks that a permanent peer added
// again right after its removal, while its old connection may still be
// shutting down, connects once the old peer is gone.
func TestReAddPermanentPeerWhileItShutsDown(t *testing.T) {
	n := newTestPeerNet(t)
	n.setMode(testPeerAddr, dialServe)
	s := newTestService(t, n)

	for range 5 {
		sp := waitForPeer(t, s, testPeerAddr)
		require.NoError(t, s.RemoveNodeByAddr(testPeerAddr))
		require.NoError(t, s.ConnectNode(testPeerAddr, true))

		require.Eventually(t, func() bool {
			next := s.PeerByAddr(testPeerAddr)
			return next != nil && next != sp
		}, 10*time.Second, 5*time.Millisecond)
	}
}
