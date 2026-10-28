package neutrino

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// TestQuarantinePeerDoesNotBlock is the regression for a deadlock between the
// block handler and peerHandler. The block handler quarantines peers, while
// peerHandler, the only reader of the query channel, can be blocked handing a
// new peer to the block handler. QuarantinePeer must therefore return without
// waiting for the query channel, and still deliver once it is read.
func TestQuarantinePeerDoesNotBlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &ChainService{
			query: make(chan interface{}),
			quit:  make(chan struct{}),
		}

		// Nobody reads the query channel, as when peerHandler is
		// stuck in NewPeer. The call must still return.
		s.QuarantinePeer("127.0.0.1:8333", time.Minute)

		msg := <-s.query
		quarantine, ok := msg.(quarantinePeerMsg)
		require.True(t, ok)
		require.Equal(t, "127.0.0.1:8333", quarantine.addr)

		// A pending handoff exits on shutdown.
		s.QuarantinePeer("127.0.0.2:8333", time.Minute)
		close(s.quit)
		synctest.Wait()
	})
}
