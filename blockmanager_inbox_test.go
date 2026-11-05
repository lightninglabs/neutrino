package neutrino

import (
	"sync"
	"testing"

	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/stretchr/testify/require"
)

// testInbox collects the messages posted to a block manager whose handler a
// test drives itself, sorted into the queues the handler's mailbox replaced.
type testInbox struct {
	peer    chan any
	timer   chan any
	timeout chan *headerSyncTimeoutMsg
	done    chan *ServerPeer
	result  chan chainstore.Result
}

// inboxes maps a block manager to its collected messages.
var inboxes sync.Map

// collectInbox makes the block manager hand its messages to the returned
// inbox instead of the handler's mailbox. A test that starts the handler must
// not call it.
func collectInbox(bm *blockManager) *testInbox {
	in := &testInbox{
		peer:    make(chan any, MaxPeers*3),
		timer:   make(chan any, MaxPeers*3),
		timeout: make(chan *headerSyncTimeoutMsg, MaxPeers*3),
		done:    make(chan *ServerPeer, MaxPeers),
		result:  make(chan chainstore.Result, 1),
	}
	inboxes.Store(bm, in)

	bm.setDeliver(func(message any) {
		switch msg := message.(type) {
		case *newPeerMsg, *invMsg, *headersMsg:
			select {
			case in.peer <- msg:
			case <-bm.quit:
			}

		case *headerSyncTimeoutMsg:
			select {
			case in.timeout <- msg:
			case <-bm.quit:
			}

		case *donePeerMsg:
			select {
			case in.done <- msg.peer:
			case <-bm.quit:
			}

		case chainstore.Result:
			select {
			case in.result <- msg:
			case <-bm.quit:
			}

		default:
			select {
			case in.timer <- msg:
			case <-bm.quit:
			}
		}
	})

	return in
}

// inboxOf returns the inbox collectInbox made for the block manager.
func inboxOf(t *testing.T, bm *blockManager) *testInbox {
	t.Helper()

	in, ok := inboxes.Load(bm)
	require.True(t, ok, "the block manager does not collect its inbox")

	return in.(*testInbox)
}
