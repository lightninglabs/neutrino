package neutrino

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/stretchr/testify/require"
)

// TestOnReadBackpressure checks that a subscriber that stops reading holds up
// the peer's reads, instead of every message the peer sends leaving a blocked
// goroutine behind, and that the reads go on once it cancels.
func TestOnReadBackpressure(t *testing.T) {
	sp := NewServerPeer(&ChainService{quit: make(chan struct{})}, false)
	_, cancel := sp.SubscribeRecvMsg()

	const flood = 1000

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < flood; i++ {
			sp.OnRead(nil, 0, wire.NewMsgPing(uint64(i)), nil)
		}
	}()

	// The reader is held at the first message, so the flood can't finish.
	// Handing each message to its own goroutine would let it finish at
	// once.
	select {
	case <-done:
		t.Fatal("flood finished against a subscriber that never reads")
	case <-time.After(100 * time.Millisecond):
	}

	// Cancelling the subscription lets the reads go on.
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("flood still blocked after the subscription ended")
	}

	sp.mtxSubscribers.RLock()
	require.Empty(t, sp.recvSubscribers)
	sp.mtxSubscribers.RUnlock()
}

// TestOnReadDeliversInOrder checks that a subscriber that keeps reading gets
// every message, in the order the peer sent them.
func TestOnReadDeliversInOrder(t *testing.T) {
	t.Parallel()

	sp := NewServerPeer(&ChainService{quit: make(chan struct{})}, false)
	msgs, cancel := sp.SubscribeRecvMsg()
	defer cancel()

	const n = 200
	go func() {
		for i := 0; i < n; i++ {
			sp.OnRead(nil, 0, wire.NewMsgPing(uint64(i)), nil)
		}
	}()

	for i := 0; i < n; i++ {
		select {
		case msg := <-msgs:
			ping, ok := msg.(*wire.MsgPing)
			require.True(t, ok)
			require.Equal(t, uint64(i), ping.Nonce)
		case <-time.After(5 * time.Second):
			t.Fatalf("message %d never arrived", i)
		}
	}
}

// TestOnReadReturnsOnDisconnect checks that a read held by a subscriber that
// never reads ends once the peer disconnects.
func TestOnReadReturnsOnDisconnect(t *testing.T) {
	t.Parallel()

	sp := NewServerPeer(&ChainService{quit: make(chan struct{})}, false)
	_, cancel := sp.SubscribeRecvMsg()
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		sp.OnRead(nil, 0, wire.NewMsgPing(1), nil)
	}()

	close(sp.quit)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("read still blocked after the peer disconnected")
	}
}
