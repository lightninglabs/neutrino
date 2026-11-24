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
