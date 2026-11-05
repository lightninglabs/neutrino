package neutrino

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSwarmConnectedPeerLifecycle(t *testing.T) {
	for i := 0; i < 20; i++ {
		var connected *ServerPeer
		t.Run(fmt.Sprintf("peer_%d", i), func(t *testing.T) {
			connected = swarmConnectedPeer(t, int32(i+1))
			require.True(t, connected.Connected())
		})
		require.False(t, connected.Connected())
	}
}

func TestSwarmRemoteHandshakeClosedConnection(t *testing.T) {
	local, remote := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- swarmRemoteHandshake(remote, 1) }()
	require.NoError(t, local.Close())
	defer remote.Close()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("remote handshake did not exit after disconnect")
	}
}
