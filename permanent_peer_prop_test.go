package neutrino

import (
	"os"
	"path/filepath"
	"strconv"
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

	// oneOffs counts the one-off connections made to each address, and
	// removals the permanent peers removed from it.
	oneOffs  map[string]int
	removals map[string]int

	// baseline is an address's dial count when it last stopped being a
	// permanent peer, and budget is the dials it may see after that:
	// one for each removal, which may have had a dial under way that
	// lands later, and one for each one-off connection to it. A request
	// left behind keeps retrying, which this and the end of the test
	// see.
	baseline map[string]int
	budget   map[string]int

	// quarantines are the quarantines applied so far.
	quarantines []peerQuarantineSpan
}

// peerQuarantineSpan is a quarantine of an address, from when it was asked
// for until it ends.
type peerQuarantineSpan struct {
	addr       string
	start, end time.Time
}

// quarantineSettle is how long after a quarantine applies a dial may still
// start, one that had passed the dial function's checks just before.
const quarantineSettle = 5 * testRetryInterval

// mayBeQuarantined returns whether a quarantine of addr may be refusing its
// dials now.
func (m *peerModel) mayBeQuarantined(addr string) bool {
	now := time.Now()
	for _, q := range m.quarantines {
		if q.addr == addr && now.Before(q.end.Add(quarantineSettle)) {
			return true
		}
	}

	return false
}

// lastQuarantineEnd returns when the last quarantine ends.
func (m *peerModel) lastQuarantineEnd() time.Time {
	var last time.Time
	for _, q := range m.quarantines {
		if q.end.After(last) {
			last = q.end
		}
	}

	return last
}

// leave records that addr is no longer a permanent peer.
func (m *peerModel) leave(addr string) {
	m.permanent[addr] = false
	m.removals[addr]++
	m.baseline[addr] = m.n.dialCount(addr)
	m.budget[addr] = m.removals[addr] + m.oneOffs[addr]
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

	// No dial starts while an address is quarantined.
	for _, q := range m.quarantines {
		dials := m.n.dialsBetween(
			q.addr, q.start.Add(quarantineSettle), q.end,
		)
		require.Zero(t, dials, "%v dialed while quarantined", q.addr)
	}

	seen := make(map[string]bool)
	for _, sp := range m.s.Peers() {
		require.False(t, seen[sp.Addr()], "two peers at %v",
			sp.Addr())
		seen[sp.Addr()] = true
	}
}

// TestPermanentPeerProperties runs random sequences of adding, removing and
// quarantining peers, under every form of their address, while their dials
// succeed, fail and drop, and checks that a removed peer isn't dialed again,
// that a quarantined one isn't dialed while that lasts, that there's at most
// one peer per address, that the outbound group count matches the
// connections we hold, that every permanent peer ends up connected
// and stays so, and that Stop leaves no connection open.
func TestPermanentPeerProperties(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a service for each check")
	}
	useTestRetryInterval(t)

	base := t.TempDir()
	checks := 0
	rapid.Check(t, func(rt *rapid.T) {
		checks++
		dir := filepath.Join(base, strconv.Itoa(checks))
		require.NoError(rt, os.Mkdir(dir, 0o700))

		n := newTestPeerNet(rt)
		s := startTestService(rt, dir, n, connectTo())
		m := &peerModel{
			s:         s,
			n:         n,
			permanent: map[string]bool{testPeerAddr: true},
			oneOffs:   make(map[string]int),
			removals:  make(map[string]int),
			baseline:  make(map[string]int),
			budget:    make(map[string]int),
		}

		// The service adds its configured peer once it has started,
		// and dials it as soon as it has.
		waitForDial(rt, n)

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
				dials := n.dialCount(a.resolved)
				err := s.ConnectNode(a.form, false)
				if m.permanent[a.resolved] {
					require.Error(t, err)
					return
				}
				if err != nil {
					return
				}
				m.oneOffs[a.resolved]++
				m.budget[a.resolved]++

				// The one-off connection is dialed once, so we
				// wait for its dial rather than have it land
				// in a later step. A quarantine may refuse the
				// dial instead.
				if m.mayBeQuarantined(a.resolved) {
					return
				}
				require.Eventually(t, func() bool {
					return n.dialCount(a.resolved) > dials
				}, 10*time.Second, time.Millisecond)
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

			"quarantine": func(t *rapid.T) {
				addr := drawAddr(t)
				retries := rapid.IntRange(10, 20).Draw(
					t, "retries",
				)
				length := time.Duration(retries) *
					testRetryInterval

				// The quarantine reaches the peer handler
				// without QuarantinePeer waiting for it, so
				// we wait for it to apply.
				end := time.Now().Add(length)
				s.QuarantinePeer(addr, length)
				require.Eventually(t, func() bool {
					now := time.Now()
					return s.quarantine.active(addr, now) ||
						now.After(end)
				}, 10*time.Second, time.Millisecond)
				start := time.Now()
				if start.After(end) {
					return
				}

				m.quarantines = append(
					m.quarantines, peerQuarantineSpan{
						addr:  addr,
						start: start,
						end:   end,
					},
				)
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
		// A peer on a connection we hung up may still be listed for
		// a moment, but no longer reports itself connected.
		n.hangUpAll()
		require.Eventually(rt, func() bool {
			for _, addr := range peerAddrs {
				if !m.permanent[addr] {
					continue
				}

				sp := s.PeerByAddr(addr)
				if sp == nil || !sp.Connected() {
					return false
				}
			}

			return true
		}, 20*time.Second, 5*time.Millisecond)
		// A quarantine refuses a dial only while it lasts, so we wait
		// for the last to end, and then for the dials of the last
		// steps to settle, before we look.
		time.Sleep(time.Until(m.lastQuarantineEnd().Add(
			quarantineSettle,
		)))
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

		// With no dial in flight, the outbound group of the test
		// addresses counts just the connections still open.
		require.EqualValues(rt, n.open.Load(),
			s.OutboundGroupCount("local"))

		require.NoError(rt, s.Stop())
		n.requireNoOpenConns()
	})
}
