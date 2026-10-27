package workmanager

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/stretchr/testify/require"
)

const (
	// broadcastTestWait bounds how long a test waits for something that is
	// expected to happen.
	broadcastTestWait = 5 * time.Second

	// broadcastTestShort is a per-attempt timeout short enough to keep the
	// tests fast, yet long enough not to fire spuriously.
	broadcastTestShort = 50 * time.Millisecond

	// broadcastTestLong is a per-attempt timeout that must never fire
	// within a test run.
	broadcastTestLong = time.Minute
)

// broadcastPeer is a Peer that records what is sent to it and lets a test
// inject responses and disconnects.
type broadcastPeer struct {
	addr string

	// recv carries the messages the test makes the peer "receive".
	recv chan wire.Message

	// disconnect is closed to simulate the peer going away.
	disconnect chan struct{}

	mu   sync.Mutex
	sent []wire.Message
	subs int
}

var _ Peer = (*broadcastPeer)(nil)

func newBroadcastPeer(addr string) *broadcastPeer {
	return &broadcastPeer{
		addr:       addr,
		recv:       make(chan wire.Message),
		disconnect: make(chan struct{}),
	}
}

func (p *broadcastPeer) QueueMessageWithEncoding(msg wire.Message,
	_ chan<- struct{}, _ wire.MessageEncoding) {

	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, msg)
}

func (p *broadcastPeer) SubscribeRecvMsg() (<-chan wire.Message, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subs++

	return p.recv, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.subs--
	}
}

func (p *broadcastPeer) Addr() string { return p.addr }

func (p *broadcastPeer) LastPingMicros() int64 { return 0 }

func (p *broadcastPeer) OnDisconnect() <-chan struct{} {
	return p.disconnect
}

// numSent returns how many times the broadcast message reached the peer.
func (p *broadcastPeer) numSent() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.sent)
}

// numSubs returns the number of live receive subscriptions.
func (p *broadcastPeer) numSubs() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.subs
}

// respond delivers msg to the broadcast as if the peer had sent it.
func (p *broadcastPeer) respond(t *testing.T, msg wire.Message) {
	t.Helper()

	select {
	case p.recv <- msg:
	case <-time.After(broadcastTestWait):
		t.Errorf("peer %v: response not consumed", p.addr)
	}
}

// newBroadcastManager returns a work manager whose broadcast recipients are
// the given peers.
func newBroadcastManager(peers ...*broadcastPeer) *peerWorkManager {
	wm := NewWorkManager(&Config{
		Peers: func() []Peer {
			out := make([]Peer, len(peers))
			for i, p := range peers {
				out[i] = p
			}

			return out
		},
	})

	return wm.(*peerWorkManager)
}

// runBroadcast starts a broadcast in the background and returns a channel that
// yields its result.
func runBroadcast(wm *peerWorkManager, b *Broadcast,
	opts ...QueryOption) <-chan error {

	errChan := make(chan error, 1)
	go func() {
		errChan <- wm.Broadcast(b, opts...)
	}()

	return errChan
}

// waitBroadcast waits for the broadcast result.
func waitBroadcast(t *testing.T, errChan <-chan error) error {
	t.Helper()

	select {
	case err := <-errChan:
		return err
	case <-time.After(broadcastTestWait):
		t.Fatal("broadcast did not return")
		return nil
	}
}

// waitSent blocks until the peer has been sent the broadcast message. It only
// reports failures through t.Errorf so it can run outside the test goroutine.
func waitSent(t *testing.T, p *broadcastPeer) {
	t.Helper()

	deadline := time.Now().Add(broadcastTestWait)
	for p.numSent() < 1 {
		if time.Now().After(deadline) {
			t.Errorf("peer %v: broadcast message not sent", p.addr)
			return
		}

		time.Sleep(time.Millisecond)
	}
}

// TestBroadcastAllPeersRespond checks that every peer is sent the message once
// and that the broadcast returns as soon as all of them are marked done.
func TestBroadcastAllPeersRespond(t *testing.T) {
	t.Parallel()

	peers := []*broadcastPeer{
		newBroadcastPeer("a"), newBroadcastPeer("b"),
		newBroadcastPeer("c"),
	}
	wm := newBroadcastManager(peers...)

	// The handler state is deliberately unsynchronized: the broadcast
	// guarantees serial handler calls, which the race detector verifies.
	got := make(map[string]wire.Message)
	req := &Broadcast{
		Msg: wire.NewMsgPing(1),
		HandleResp: func(p Peer, resp wire.Message,
			ctl *BroadcastControl) {

			got[p.Addr()] = resp
			ctl.PeerDone()
		},
	}

	errChan := runBroadcast(wm, req, Timeout(broadcastTestLong))
	for _, p := range peers {
		waitSent(t, p)
		p.respond(t, wire.NewMsgPong(7))
	}

	require.NoError(t, waitBroadcast(t, errChan))
	require.Len(t, got, len(peers))
	for _, p := range peers {
		require.Equal(t, 1, p.numSent())
		require.Eventually(t, func() bool {
			return p.numSubs() == 0
		}, broadcastTestWait, time.Millisecond)
	}
}

// TestBroadcastSomePeersTimeOut checks that peers that never answer are given
// up on after their attempts are exhausted, without failing the broadcast.
func TestBroadcastSomePeersTimeOut(t *testing.T) {
	t.Parallel()

	good, slow := newBroadcastPeer("good"), newBroadcastPeer("slow")
	wm := newBroadcastManager(good, slow)

	var responders []string
	req := &Broadcast{
		Msg: wire.NewMsgPing(1),
		HandleResp: func(p Peer, _ wire.Message,
			ctl *BroadcastControl) {

			responders = append(responders, p.Addr())
			ctl.PeerDone()
		},
	}

	// Two attempts per peer: the silent peer must be asked twice.
	errChan := runBroadcast(
		wm, req, Timeout(broadcastTestShort), NumRetries(2),
	)
	waitSent(t, good)
	good.respond(t, wire.NewMsgPong(1))

	require.NoError(t, waitBroadcast(t, errChan))
	require.Equal(t, []string{"good"}, responders)
	require.Equal(t, 1, good.numSent())
	require.Equal(t, 2, slow.numSent())
}

// TestBroadcastRejectGraceWindow checks that PeerDoneAfter keeps listening to a
// peer for the grace period, so a follow up message is still handled, and that
// the broadcast ends once the window closes.
func TestBroadcastRejectGraceWindow(t *testing.T) {
	t.Parallel()

	accepting, rejecting := newBroadcastPeer("ok"),
		newBroadcastPeer("bad")
	wm := newBroadcastManager(accepting, rejecting)

	var (
		msgs     = make(map[string][]string)
		grace    = 100 * time.Millisecond
		finished = make(chan struct{})
	)
	req := &Broadcast{
		Msg: wire.NewMsgPing(1),
		HandleResp: func(p Peer, resp wire.Message,
			ctl *BroadcastControl) {

			msgs[p.Addr()] = append(msgs[p.Addr()], resp.Command())

			// Any first answer starts the grace window, a reject
			// ends it right away.
			if _, ok := resp.(*wire.MsgReject); ok {
				ctl.PeerDone()
				return
			}
			ctl.PeerDoneAfter(grace)
		},
	}

	start := time.Now()
	errChan := runBroadcast(wm, req, Timeout(broadcastTestLong))
	go func() {
		defer close(finished)

		waitSent(t, accepting)
		accepting.respond(t, wire.NewMsgGetData())
		waitSent(t, rejecting)
		rejecting.respond(t, wire.NewMsgGetData())
		rejecting.respond(t, wire.NewMsgReject("tx", 0x10, "bad"))
	}()

	require.NoError(t, waitBroadcast(t, errChan))
	<-finished

	// The accepting peer stayed silent, so the broadcast could only end
	// after its grace window elapsed.
	require.GreaterOrEqual(t, time.Since(start), grace)
	require.Equal(t, []string{"getdata"}, msgs["ok"])
	require.Equal(t, []string{"getdata", "reject"}, msgs["bad"])
}

// TestBroadcastIgnoresMessagesAfterPeerDone checks that once a peer is marked
// done the handler is no longer invoked for it.
func TestBroadcastIgnoresMessagesAfterPeerDone(t *testing.T) {
	t.Parallel()

	chatty, quiet := newBroadcastPeer("chatty"), newBroadcastPeer("quiet")
	wm := newBroadcastManager(chatty, quiet)

	calls := make(map[string]int)
	req := &Broadcast{
		Msg: wire.NewMsgPing(1),
		HandleResp: func(p Peer, _ wire.Message,
			ctl *BroadcastControl) {

			calls[p.Addr()]++
			ctl.PeerDone()
		},
	}

	errChan := runBroadcast(wm, req, Timeout(broadcastTestLong))
	waitSent(t, chatty)
	waitSent(t, quiet)

	chatty.respond(t, wire.NewMsgPong(1))

	// A second message may or may not be read before the peer goroutine
	// notices it is done, but it must never reach the handler.
	select {
	case chatty.recv <- wire.NewMsgPong(2):
	case <-time.After(broadcastTestShort):
	}

	quiet.respond(t, wire.NewMsgPong(3))

	require.NoError(t, waitBroadcast(t, errChan))
	require.Equal(t, map[string]int{"chatty": 1, "quiet": 1}, calls)
}

// TestBroadcastDoneEndsEarly checks that the handler can end the whole
// broadcast without waiting on the other peers.
func TestBroadcastDoneEndsEarly(t *testing.T) {
	t.Parallel()

	fast, hung := newBroadcastPeer("fast"), newBroadcastPeer("hung")
	wm := newBroadcastManager(fast, hung)

	req := &Broadcast{
		Msg: wire.NewMsgPing(1),
		HandleResp: func(_ Peer, _ wire.Message,
			ctl *BroadcastControl) {

			ctl.Done()
		},
	}

	errChan := runBroadcast(wm, req, Timeout(broadcastTestLong))
	waitSent(t, fast)
	waitSent(t, hung)
	fast.respond(t, wire.NewMsgPong(1))

	require.NoError(t, waitBroadcast(t, errChan))
	require.Eventually(t, func() bool {
		return hung.numSubs() == 0
	}, broadcastTestWait, time.Millisecond)
}

// TestBroadcastPeerDisconnects checks that a peer going away mid-broadcast is
// dropped immediately instead of being waited on until its timeout.
func TestBroadcastPeerDisconnects(t *testing.T) {
	t.Parallel()

	stays, leaves := newBroadcastPeer("stays"), newBroadcastPeer("leaves")
	wm := newBroadcastManager(stays, leaves)

	req := &Broadcast{
		Msg: wire.NewMsgPing(1),
		HandleResp: func(_ Peer, _ wire.Message,
			ctl *BroadcastControl) {

			ctl.PeerDone()
		},
	}

	errChan := runBroadcast(wm, req, Timeout(broadcastTestLong))
	waitSent(t, stays)
	waitSent(t, leaves)

	// The disconnect alone must not end the broadcast: the other peer is
	// still outstanding.
	close(leaves.disconnect)
	select {
	case err := <-errChan:
		t.Fatalf("broadcast ended early: %v", err)
	case <-time.After(broadcastTestShort):
	}

	stays.respond(t, wire.NewMsgPong(1))
	require.NoError(t, waitBroadcast(t, errChan))
}

// TestBroadcastShutdown checks that stopping the manager mid-broadcast aborts
// it with ErrWorkManagerShuttingDown and tears down all peer subscriptions.
func TestBroadcastShutdown(t *testing.T) {
	t.Parallel()

	peers := []*broadcastPeer{newBroadcastPeer("a"), newBroadcastPeer("b")}
	wm := newBroadcastManager(peers...)

	req := &Broadcast{
		Msg: wire.NewMsgPing(1),
		HandleResp: func(Peer, wire.Message, *BroadcastControl) {
			t.Error("unexpected response")
		},
	}

	errChan := runBroadcast(wm, req, Timeout(broadcastTestLong))
	for _, p := range peers {
		waitSent(t, p)
	}

	close(wm.quit)

	err := waitBroadcast(t, errChan)
	require.ErrorIs(t, err, ErrWorkManagerShuttingDown)
	for _, p := range peers {
		require.Eventually(t, func() bool {
			return p.numSubs() == 0
		}, broadcastTestWait, time.Millisecond)
	}
}

// TestBroadcastCancel checks that closing the cancel channel aborts the
// broadcast with ErrJobCanceled.
func TestBroadcastCancel(t *testing.T) {
	t.Parallel()

	p := newBroadcastPeer("a")
	wm := newBroadcastManager(p)

	cancel := make(chan struct{})
	req := &Broadcast{
		Msg: wire.NewMsgPing(1),
		HandleResp: func(Peer, wire.Message, *BroadcastControl) {
			t.Error("unexpected response")
		},
	}

	errChan := runBroadcast(
		wm, req, Timeout(broadcastTestLong), Cancel(cancel),
	)
	waitSent(t, p)
	close(cancel)

	require.ErrorIs(t, waitBroadcast(t, errChan), ErrJobCanceled)
}

// TestBroadcastNoPeers checks that broadcasting without any recipients is a
// no-op rather than a hang.
func TestBroadcastNoPeers(t *testing.T) {
	t.Parallel()

	req := &Broadcast{
		Msg: wire.NewMsgPing(1),
		HandleResp: func(Peer, wire.Message, *BroadcastControl) {
			t.Error("unexpected response")
		},
	}

	// Both an empty peer set and an unwired manager must return at once.
	for _, wm := range []*peerWorkManager{
		newBroadcastManager(),
		NewWorkManager(nil).(*peerWorkManager),
	} {
		require.NoError(t, waitBroadcast(t, runBroadcast(wm, req)))
	}
}

// TestBroadcastSerialHandler hammers the broadcast from many peers at once
// to let the race detector confirm handler calls never overlap.
func TestBroadcastSerialHandler(t *testing.T) {
	t.Parallel()

	const (
		numPeers    = 16
		msgsPerPeer = 8
	)

	peers := make([]*broadcastPeer, numPeers)
	for i := range peers {
		peers[i] = newBroadcastPeer(fmt.Sprintf("peer%d", i))
	}
	wm := newBroadcastManager(peers...)

	// Plain, unguarded counters: any concurrent handler call is a race.
	counts := make(map[string]int)
	total := 0
	req := &Broadcast{
		Msg: wire.NewMsgPing(1),
		HandleResp: func(p Peer, _ wire.Message,
			ctl *BroadcastControl) {

			counts[p.Addr()]++
			total++
			if counts[p.Addr()] == msgsPerPeer {
				ctl.PeerDone()
			}
		},
	}

	errChan := runBroadcast(wm, req, Timeout(broadcastTestLong))

	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func(p *broadcastPeer) {
			defer wg.Done()

			waitSent(t, p)
			for i := 0; i < msgsPerPeer; i++ {
				p.respond(t, wire.NewMsgPong(uint64(i)))
			}
		}(p)
	}
	wg.Wait()

	require.NoError(t, waitBroadcast(t, errChan))
	require.Equal(t, numPeers*msgsPerPeer, total)
}
