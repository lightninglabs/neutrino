package neutrino

import (
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/stretchr/testify/require"
)

// testPeerAddr is the permanent peer every test service starts with. Without
// a peer in Config.ConnectPeers the service would look for peers on its own.
const testPeerAddr = "127.0.0.1:18555"

// testRetryInterval is the retry interval of a test service's connection
// manager.
const testRetryInterval = 20 * time.Millisecond

// quietPeriod is how long a test waits to see that something doesn't happen,
// such as a removed peer being dialed again. It spans many retries.
const quietPeriod = 25 * testRetryInterval

// dialMode is how the test network answers a dial to an address.
type dialMode int

const (
	// dialRefuse fails the dial.
	dialRefuse dialMode = iota

	// dialServe connects to a node that completes the handshake.
	dialServe

	// dialSilent connects to a node that never answers, so the
	// handshake stays in flight.
	dialSilent

	// dialHangUp connects to a node that closes the connection right
	// away.
	dialHangUp
)

// testPeerNet is the network of a test service. Each dial gets one end of a
// pipe, with a fake node on the other end, and the network counts dials and
// the connections still open.
type testPeerNet struct {
	t *testing.T

	mu          sync.Mutex
	modes       map[string]dialMode
	holds       map[string]chan struct{}
	gates       map[string]chan struct{}
	dials       map[string]int
	inFlight    map[string]int
	maxInFlight map[string]int

	// open counts the connections whose node end hasn't seen the
	// service close them.
	open atomic.Int64

	// dialed receives the address of each dial as it starts.
	dialed chan string
}

// newTestPeerNet returns a network that refuses every dial until told
// otherwise.
func newTestPeerNet(t *testing.T) *testPeerNet {
	return &testPeerNet{
		t:           t,
		modes:       make(map[string]dialMode),
		holds:       make(map[string]chan struct{}),
		gates:       make(map[string]chan struct{}),
		dials:       make(map[string]int),
		inFlight:    make(map[string]int),
		maxInFlight: make(map[string]int),
		dialed:      make(chan string, 1000),
	}
}

// setMode sets how dials to addr are answered from now on.
func (n *testPeerNet) setMode(addr string, mode dialMode) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.modes[addr] = mode
}

// hold makes dials to addr wait until the returned function is called.
func (n *testPeerNet) hold(addr string) func() {
	n.mu.Lock()
	defer n.mu.Unlock()

	release := make(chan struct{})
	n.holds[addr] = release

	var once sync.Once
	return func() {
		once.Do(func() { close(release) })
	}
}

// gate makes the nodes of connections to addr made from now on wait until
// the returned function is called before they answer the handshake.
func (n *testPeerNet) gate(addr string) func() {
	n.mu.Lock()
	defer n.mu.Unlock()

	open := make(chan struct{})
	n.gates[addr] = open

	var once sync.Once
	return func() {
		once.Do(func() { close(open) })
	}
}

// dialCount returns how many times addr has been dialed.
func (n *testPeerNet) dialCount(addr string) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.dials[addr]
}

// maxConcurrentDials returns the most dials to addr that were in flight at
// once.
func (n *testPeerNet) maxConcurrentDials(addr string) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.maxInFlight[addr]
}

// dial is the service's dialer.
func (n *testPeerNet) dial(addr net.Addr) (net.Conn, error) {
	key := addr.String()

	n.mu.Lock()
	n.dials[key]++
	n.inFlight[key]++
	n.maxInFlight[key] = max(n.maxInFlight[key], n.inFlight[key])
	hold := n.holds[key]
	n.mu.Unlock()

	select {
	case n.dialed <- key:
	default:
	}

	if hold != nil {
		<-hold
	}

	n.mu.Lock()
	n.inFlight[key]--
	mode := n.modes[key]
	gate := n.gates[key]
	n.mu.Unlock()

	if mode == dialRefuse {
		return nil, net.ErrClosed
	}

	local, remote := net.Pipe()
	n.open.Add(1)
	go n.runNode(remote, mode, gate)

	return local, nil
}

// runNode plays the node at the far end of a connection until the service
// closes it. If gate is set, the node waits for it before answering.
func (n *testPeerNet) runNode(conn net.Conn, mode dialMode,
	gate <-chan struct{}) {

	defer n.open.Add(-1)
	defer conn.Close()

	if mode == dialHangUp {
		return
	}

	params := chaincfg.SimNetParams.Net
	pver := uint32(wire.AddrV2Version)

	if mode == dialServe {
		// Take the service's version, then answer with ours and a
		// verack. Writes to a pipe block until the other end reads,
		// so they run alongside the reads below.
		_, _, err := wire.ReadMessage(conn, pver, params)
		if err != nil {
			return
		}
		go func() {
			if gate != nil {
				<-gate
			}

			services := wire.SFNodeNetwork | wire.SFNodeWitness |
				wire.SFNodeCF
			me := wire.NewNetAddressIPPort(
				net.IPv4(127, 0, 0, 9), 18555, services,
			)
			you := wire.NewNetAddressIPPort(
				net.IPv4(127, 0, 0, 1), 40000, 0,
			)
			version := wire.NewMsgVersion(me, you, 12345, 0)
			version.Services = services
			version.ProtocolVersion = int32(pver)

			err := wire.WriteMessage(conn, version, pver, params)
			if err != nil {
				return
			}
			_ = wire.WriteMessage(
				conn, wire.NewMsgVerAck(), pver, params,
			)
		}()
	}

	for {
		_, _, err := wire.ReadMessage(conn, pver, params)
		if err != nil {
			return
		}
	}
}

// requireNoOpenConns checks that the service has closed every connection it
// was given.
func (n *testPeerNet) requireNoOpenConns() {
	require.Eventually(n.t, func() bool {
		return n.open.Load() == 0
	}, 5*time.Second, 5*time.Millisecond, "%d connections left open",
		n.open.Load())
}

// requireNoMoreDials checks that addr isn't dialed again for a while.
func (n *testPeerNet) requireNoMoreDials(addr string) {
	before := n.dialCount(addr)
	require.Never(n.t, func() bool {
		return n.dialCount(addr) != before
	}, quietPeriod, testRetryInterval/2, "%v dialed again", addr)
}

// testResolver resolves the test hostnames, and IP addresses to themselves.
func testResolver(host string) ([]net.IP, error) {
	switch host {
	case "peer.test":
		return []net.IP{net.IPv4(127, 0, 0, 3)}, nil
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return nil, &net.DNSError{Err: "no such host", Name: host}
	}

	return []net.IP{ip}, nil
}

// newTestService starts a service on the test network, with testPeerAddr as
// its permanent peer, and stops it when the test is over.
func newTestService(t *testing.T, n *testPeerNet) *ChainService {
	oldRetry := ConnectionRetryInterval
	ConnectionRetryInterval = testRetryInterval
	t.Cleanup(func() { ConnectionRetryInterval = oldRetry })

	dir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(dir, "neutrino.db"), true,
		10*time.Second, false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	s, err := NewChainService(Config{
		DataDir:      dir,
		Database:     db,
		ChainParams:  chaincfg.SimNetParams,
		ConnectPeers: []string{testPeerAddr},
		Dialer:       n.dial,
		NameResolver: testResolver,
	})
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))
	t.Cleanup(func() { _ = s.Stop() })

	return s
}

// waitForDial waits until addr is dialed.
func waitForDial(t *testing.T, n *testPeerNet, addr string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return n.dialCount(addr) > 0
	}, 5*time.Second, time.Millisecond, "%v never dialed", addr)
}

// waitForPeer waits until the service has a connected peer at addr.
func waitForPeer(t *testing.T, s *ChainService, addr string) *ServerPeer {
	t.Helper()

	var sp *ServerPeer
	require.Eventually(t, func() bool {
		sp = s.PeerByAddr(addr)
		return sp != nil
	}, 10*time.Second, 5*time.Millisecond, "no peer at %v", addr)

	return sp
}
