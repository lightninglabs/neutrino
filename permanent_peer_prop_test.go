package neutrino

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// peerAddrForm is one way of writing a peer's address, and the address it
// resolves to.
type peerAddrForm struct {
	form     string
	resolved string
}

// peerAddrForms are the addresses the property test uses, written each way
// the service should treat as the same peer.
var peerAddrForms = []peerAddrForm{
	{testPeerAddr, testPeerAddr},
	{"127.0.0.1", testPeerAddr},
	{"127.0.0.2:18555", "127.0.0.2:18555"},
	{"peer.test", "127.0.0.3:18555"},
	{"peer.test:18555", "127.0.0.3:18555"},
	{"127.0.0.3", "127.0.0.3:18555"},
}

// peerAddrs are the distinct addresses of peerAddrForms.
var peerAddrs = []string{testPeerAddr, "127.0.0.2:18555", "127.0.0.3:18555"}

// peerModel is the model of the property test: the addresses that should be
// permanent peers, and how many dials each address may see while it isn't
// one.
type peerModel struct {
	s *ChainService
	n *testPeerNet

	permanent map[string]bool

	// oneOffs counts the one-off connections made to each address.
	oneOffs map[string]int

	// baseline is an address's dial count when it last stopped being a
	// permanent peer, and budget is the dials it may see after that:
	// one already under way when it was removed, and one for each
	// one-off connection to it.
	baseline map[string]int
	budget   map[string]int
}

// leave records that addr is no longer a permanent peer.
func (m *peerModel) leave(addr string) {
	m.permanent[addr] = false
	m.baseline[addr] = m.n.dialCount(addr)
	m.budget[addr] = 1 + m.oneOffs[addr]
}

// check verifies the invariants that hold at every step: no address that
// isn't a permanent peer is dialed beyond its budget, and there is at most
// one peer per address.
func (m *peerModel) check(t require.TestingT) {
	for _, addr := range peerAddrs {
		if m.permanent[addr] {
			continue
		}

		dials := m.n.dialCount(addr) - m.baseline[addr]
		require.LessOrEqual(t, dials, m.budget[addr],
			"%v dialed while not a permanent peer", addr)
	}

	seen := make(map[string]bool)
	for _, sp := range m.s.Peers() {
		require.False(t, seen[sp.Addr()], "two peers at %v",
			sp.Addr())
		seen[sp.Addr()] = true
	}
}

// TestPermanentPeerProperties runs random sequences of adding and removing
// peers, under every form of their address, while their dials succeed, fail
// and drop, and checks that a removed peer isn't dialed again, that there's
// at most one peer per address, that every permanent peer ends up connected
// and stays so, and that Stop leaves no connection open.
func TestPermanentPeerProperties(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a service for each check")
	}
	useTestRetryInterval(t)

	rapid.Check(t, func(rt *rapid.T) {
		dir, err := os.MkdirTemp(t.TempDir(), "")
		require.NoError(rt, err)

		n := newTestPeerNet(rt)
		s := startTestService(rt, dir, n)
		m := &peerModel{
			s:         s,
			n:         n,
			permanent: map[string]bool{testPeerAddr: true},
			oneOffs:   make(map[string]int),
			baseline:  make(map[string]int),
			budget:    make(map[string]int),
		}

		// The service adds its configured peer once it has started,
		// and dials it as soon as it has.
		waitForDial(rt, n, testPeerAddr)

		drawForm := func(t *rapid.T) peerAddrForm {
			return rapid.SampledFrom(peerAddrForms).Draw(t, "addr")
		}
		drawAddr := func(t *rapid.T) string {
			return rapid.SampledFrom(peerAddrs).Draw(t, "addr")
		}

		rt.Repeat(map[string]func(*rapid.T){
			"": func(t *rapid.T) { m.check(t) },

			"connectPermanent": func(t *rapid.T) {
				a := drawForm(t)
				err := s.ConnectNode(a.form, true)
				if m.permanent[a.resolved] {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				m.permanent[a.resolved] = true
			},

			"connectOneOff": func(t *rapid.T) {
				a := drawForm(t)
				err := s.ConnectNode(a.form, false)
				if m.permanent[a.resolved] {
					require.Error(t, err)
					return
				}
				if err == nil {
					m.oneOffs[a.resolved]++
					m.budget[a.resolved]++
				}
			},

			"removeByAddr": func(t *rapid.T) {
				a := drawForm(t)
				err := s.RemoveNodeByAddr(a.form)
				if !m.permanent[a.resolved] {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				m.leave(a.resolved)
			},

			"removeByID": func(t *rapid.T) {
				addr := drawAddr(t)
				sp := s.PeerByAddr(addr)
				if sp == nil {
					t.Skip("no peer")
				}

				// The peer may have gone since we looked it
				// up, in which case it isn't found.
				err := s.RemoveNodeByID(sp.ID())
				if err == nil {
					require.True(t, m.permanent[addr])
					m.leave(addr)
				}
			},

			"setMode": func(t *rapid.T) {
				mode := rapid.SampledFrom([]dialMode{
					dialRefuse, dialServe, dialSilent,
					dialHangUp,
				}).Draw(t, "mode")
				n.setMode(drawAddr(t), mode)
			},

			"setDelay": func(t *rapid.T) {
				delay := rapid.SampledFrom([]time.Duration{
					0, 2 * testRetryInterval,
				}).Draw(t, "delay")
				n.setDelay(drawAddr(t), delay)
			},

			"drop": func(t *rapid.T) {
				if sp := s.PeerByAddr(drawAddr(t)); sp != nil {
					sp.Disconnect()
				}
			},

			"wait": func(t *rapid.T) {
				retries := rapid.IntRange(0, 3).Draw(
					t, "retries",
				)
				time.Sleep(time.Duration(retries) *
					testRetryInterval)
			},
		})

		// Stopping at any point closes every connection, those still
		// being dialed included.
		if rapid.Bool().Draw(rt, "stopNow") {
			require.NoError(rt, s.Stop())
			n.requireNoOpenConns()

			return
		}

		// With the nodes of permanent peers answering, and every
		// other dial refused, every permanent peer connects once any
		// handshakes in flight are cut short. Then no address is
		// dialed again: a request left behind for a removed peer
		// would keep retrying its refused dials.
		for _, addr := range peerAddrs {
			mode := dialRefuse
			if m.permanent[addr] {
				mode = dialServe
			}
			n.setMode(addr, mode)
			n.setDelay(addr, 0)
		}
		n.hangUpAll()
		for _, addr := range peerAddrs {
			if m.permanent[addr] {
				waitForPeer(rt, s, addr)
			}
		}
		for _, addr := range peerAddrs {
			if m.permanent[addr] {
				require.NotNil(rt, s.PeerByAddr(addr))
			}
		}
		// A one-off dial from the last steps may still be landing,
		// so we wait for the dials to settle before we look.
		totalDials := func() int {
			total := 0
			for _, addr := range peerAddrs {
				total += n.dialCount(addr)
			}

			return total
		}
		require.Eventually(rt, func() bool {
			before := totalDials()
			time.Sleep(2 * testRetryInterval)

			return totalDials() == before
		}, 5*time.Second, time.Millisecond)

		before := make(map[string]int)
		for _, addr := range peerAddrs {
			before[addr] = n.dialCount(addr)
		}
		time.Sleep(5 * testRetryInterval)
		for _, addr := range peerAddrs {
			require.Equal(rt, before[addr], n.dialCount(addr),
				addr)
		}
		m.check(rt)

		require.NoError(rt, s.Stop())
		n.requireNoOpenConns()
	})
}
