package neutrino

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStopClosesHandshakingPeers checks that Stop closes the connections of
// peers that are still in their handshake, as well as of connected peers.
func TestStopClosesHandshakingPeers(t *testing.T) {
	const silent = "127.0.0.2:18555"

	n := newTestPeerNet(t)
	n.setMode(testPeerAddr, dialServe)
	n.setMode(silent, dialSilent)
	s := newTestService(t, n)

	waitForPeer(t, s, testPeerAddr)
	require.NoError(t, s.ConnectNode(silent, true))
	require.Eventually(t, func() bool {
		return n.open.Load() == 2
	}, 5*time.Second, time.Millisecond)

	require.NoError(t, s.Stop())
	n.requireNoOpenConns()
}

// TestStopClosesConnectionDroppedByConnManager checks that Stop closes a
// connection whose dial finishes once the connection manager has stopped,
// which the manager drops without closing it.
func TestStopClosesConnectionDroppedByConnManager(t *testing.T) {
	n := newTestPeerNet(t)
	n.setMode(testPeerAddr, dialSilent)
	release := n.hold(testPeerAddr)
	t.Cleanup(release)
	s := newTestService(t, n)

	waitForDial(t, n, testPeerAddr)

	// Stop starts by stopping the connection manager. The dial then
	// finishes, and the manager, having stopped, drops the connection.
	s.connManager.Stop()
	release()
	require.Eventually(t, func() bool {
		return n.open.Load() == 1
	}, 5*time.Second, time.Millisecond)

	// There's nothing to wait on for the manager dropping it. A
	// connection still in flight when Stop closes the quit channel is
	// closed either way, so this only makes the test sure to see the
	// dropped one.
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, s.Stop())
	n.requireNoOpenConns()
}
