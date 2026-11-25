package neutrino

import (
	"net"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/stretchr/testify/require"
)

// TestOutboundGroupCountsDialsAndHandshakes checks that a peer counts towards
// its outbound group from the start of its dial until its connection is
// closed, not only once it has sent its verack.
func TestOutboundGroupCountsDialsAndHandshakes(t *testing.T) {
	// Every test address is a local one.
	const group = "local"

	n := newTestPeerNet(t)
	n.setMode(testPeerAddr, dialSilent)
	release := n.hold(testPeerAddr)
	t.Cleanup(release)
	s := newTestService(t, n)

	waitForDial(t, n)
	require.Equal(t, 1, s.OutboundGroupCount(group))

	// The node never answers, so the handshake stays in flight.
	release()
	require.Eventually(t, func() bool {
		return n.open.Load() == 1
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, 1, s.OutboundGroupCount(group))

	require.NoError(t, s.RemoveNodeByAddr(testPeerAddr))
	require.Eventually(t, func() bool {
		return s.OutboundGroupCount(group) == 0
	}, 5*time.Second, time.Millisecond)
}

// TestDiscoveredPeersOnePerGroup checks that we dial at most one peer we
// found ourselves in each outbound group at a time, even while the first
// dial is still in flight.
func TestDiscoveredPeersOnePerGroup(t *testing.T) {
	// Both addresses are in the 1.2.0.0/16 group, and use testnet's
	// default port.
	addrs := []string{"1.2.3.4:18333", "1.2.3.5:18333"}

	oldSeed := DisableDNSSeed
	DisableDNSSeed = true
	t.Cleanup(func() { DisableDNSSeed = oldSeed })

	n := newTestPeerNet(t)
	for _, addr := range addrs {
		t.Cleanup(n.hold(addr))
	}

	useTestRetryInterval(t)
	s := startTestService(t, t.TempDir(), n, func(cfg *Config) {
		// Without peers to connect to only, the service looks for
		// peers in its address manager, except on a development
		// network such as simnet.
		cfg.ChainParams = chaincfg.TestNet3Params
		cfg.AddPeers = []string{testPeerAddr}
	})

	known := make([]*wire.NetAddressV2, 0, len(addrs))
	for _, addr := range addrs {
		tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
		require.NoError(t, err)
		known = append(known, wire.NetAddressV2FromBytes(
			time.Now(), RequiredServices, tcpAddr.IP,
			uint16(tcpAddr.Port),
		))
	}
	s.addrManager.AddAddresses(known, known[0])

	require.Eventually(t, func() bool {
		return n.dialCount(addrs[0])+n.dialCount(addrs[1]) > 0
	}, 10*time.Second, time.Millisecond)
	require.Never(t, func() bool {
		return n.dialCount(addrs[0])+n.dialCount(addrs[1]) > 1
	}, quietPeriod, testRetryInterval/2)
}
