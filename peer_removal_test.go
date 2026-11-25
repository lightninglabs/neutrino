package neutrino

import (
	"net"
	"testing"
	"time"

	"github.com/btcsuite/btcd/connmgr"
	"github.com/btcsuite/btcd/peer"
	"github.com/stretchr/testify/require"
)

// TestRemovePersistentPeerEndsItsRequest checks that removing a persistent
// peer also ends its permanent connection request, so the connection manager
// doesn't dial it again once it disconnects.
func TestRemovePersistentPeerEndsItsRequest(t *testing.T) {
	t.Parallel()

	connected := make(chan *connmgr.ConnReq, 8)
	cm, err := connmgr.New(&connmgr.Config{
		RetryDuration: 10 * time.Millisecond,
		Dial: func(net.Addr) (net.Conn, error) {
			local, remote := net.Pipe()
			_ = remote.Close()

			return local, nil
		},
		OnConnection: func(req *connmgr.ConnReq, _ net.Conn) {
			connected <- req
		},
	})
	require.NoError(t, err)
	cm.Start()
	t.Cleanup(cm.Stop)

	req := &connmgr.ConnReq{
		Addr:      &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8333},
		Permanent: true,
	}
	cm.Connect(req)
	select {
	case got := <-connected:
		require.Equal(t, req, got)
	case <-time.After(5 * time.Second):
		t.Fatal("initial dial did not complete")
	}

	p, err := peer.NewOutboundPeer(&peer.Config{}, "127.0.0.1:8333")
	require.NoError(t, err)
	s := &ChainService{connManager: cm}
	sp := &ServerPeer{Peer: p, persistent: true, connReq: req}
	state := newPeerState()
	state.persistentPeers[sp.ID()] = sp
	state.peers[sp.key()] = sp
	state.permanent[sp.key()] = &permanentReq{Addr: req.Addr, req: req}

	reply := make(chan error, 1)
	s.handleQuery(state, removeNodeMsg{
		cmp:   func(got *ServerPeer) bool { return got == sp },
		reply: reply,
	})
	require.NoError(t, <-reply)
	require.Empty(t, state.persistentPeers)

	// The peer's disconnect then reaches the done handler, as it does
	// from peerDoneHandler. Its request must not be retried.
	s.handleDonePeerMsg(state, sp)
	select {
	case <-connected:
		t.Fatal("removed persistent peer was dialed again")
	case <-time.After(200 * time.Millisecond):
	}
}
