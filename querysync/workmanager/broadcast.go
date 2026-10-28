package workmanager

import (
	"sync"
	"time"

	"github.com/btcsuite/btcd/wire/v2"
)

const (
	// defaultBroadcastRetries is the number of times a broadcast message is
	// sent to each peer when the caller doesn't specify NumRetries. Unlike
	// ranged queries, a broadcast is not retried by default as the same
	// message would only be sent to the same peer again.
	defaultBroadcastRetries = 1
)

// BroadcastRespHandler is called for every message a peer sends while a
// broadcast is in flight. Calls are serialized across all peers, so a handler
// may freely mutate state captured by the closure without further locking. The
// handler should be cheap, as it stalls delivery of messages from every other
// peer while it runs.
type BroadcastRespHandler func(peer Peer, resp wire.Message,
	ctl *BroadcastControl)

// Broadcast describes a single message that is sent to every eligible
// connected peer, together with the handler used to interpret what the peers
// send back.
type Broadcast struct {
	// Msg is the message that is sent to each peer.
	Msg wire.Message

	// HandleResp is invoked for each message received from a peer until
	// that peer, or the whole broadcast, is marked as done through the
	// supplied BroadcastControl. It must not be nil.
	HandleResp BroadcastRespHandler
}

// Broadcaster is an interface defining the API for sending a single message to
// all connected bitcoin peers and collecting their responses.
type Broadcaster interface {
	// Broadcast sends b.Msg to every eligible connected peer and feeds
	// all responses to b.HandleResp. It blocks until every peer has either
	// been marked done, timed out on its final attempt, or disconnected,
	// the handler marks the entire broadcast as done, the cancel option
	// fires, or the work manager shuts down. The timeout option bounds how
	// long each attempt waits on a peer, not the broadcast as a whole.
	//
	// ErrJobCanceled is returned if the cancel channel was closed and
	// ErrWorkManagerShuttingDown if the manager stopped before the
	// broadcast completed. Peers failing to respond is not an error.
	Broadcast(b *Broadcast, options ...QueryOption) error
}

// BroadcastControl lets a BroadcastRespHandler tell the broadcast when it no
// longer needs to wait on a single peer, or on any peer. It is safe for use by
// multiple goroutines and every method is idempotent.
type BroadcastControl struct {
	// run is the broadcast this control belongs to.
	run *broadcastRun

	// peerQuit is closed once the owning peer is done.
	peerQuit chan struct{}

	// peerOnce guards the close of peerQuit.
	peerOnce sync.Once
}

// PeerDone marks the peer that sent the current message as finished. Its
// pending retries are abandoned and any further messages it sends are
// ignored, while the broadcast keeps waiting on the remaining peers.
func (c *BroadcastControl) PeerDone() {
	c.peerOnce.Do(func() {
		close(c.peerQuit)
	})
}

// PeerDoneAfter marks the peer as finished once the delay has elapsed, or
// immediately if the broadcast ends or the work manager shuts down first. It
// is used when a peer has answered but may still follow up, for example with a
// reject message.
func (c *BroadcastControl) PeerDoneAfter(delay time.Duration) {
	c.run.wg.Add(1)
	go func() {
		defer c.run.wg.Done()
		defer c.PeerDone()

		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
		case <-c.run.done:
		case <-c.run.finished:
		case <-c.run.quit:
		}
	}()
}

// Done marks the entire broadcast as finished, causing Broadcast to return
// without waiting on any remaining peer.
func (c *BroadcastControl) Done() {
	c.run.doneOnce.Do(func() {
		close(c.run.done)
	})
}

// broadcastMsg is a message received from a peer, tagged with its origin.
type broadcastMsg struct {
	peer Peer
	msg  wire.Message
	ctl  *BroadcastControl
}

// broadcastRun holds the state shared by the goroutines serving a single
// Broadcast call.
type broadcastRun struct {
	// req is the broadcast being served.
	req *Broadcast

	// opts are the resolved query options of the broadcast.
	opts Options

	// quit is closed when the work manager shuts down.
	quit <-chan struct{}

	// done is closed when the handler declares the whole broadcast
	// finished, and doneOnce guards that close.
	done     chan struct{}
	doneOnce sync.Once

	// finished is closed by the collector once it stops reading messages,
	// releasing any peer goroutine blocked on a hand-off.
	finished chan struct{}

	// msgs carries peer messages to the collector.
	msgs chan broadcastMsg

	// peerWG tracks the per-peer goroutines.
	peerWG sync.WaitGroup

	// wg tracks the delayed peer completions.
	wg sync.WaitGroup
}

// A compile time check to ensure peerWorkManager satisfies the Broadcaster
// interface.
var _ Broadcaster = (*peerWorkManager)(nil)

// Broadcast sends b.Msg to all eligible connected peers and feeds their
// responses to b.HandleResp.
//
// NOTE: This is part of the Broadcaster interface.
func (w *peerWorkManager) Broadcast(b *Broadcast,
	options ...QueryOption) error {

	// Apply the caller's options on top of the broadcast specific
	// defaults.
	opts := ResolveOptions(append(
		[]QueryOption{NumRetries(defaultBroadcastRetries)}, options...,
	)...)

	// Snapshot the eligible peers. If the manager is not wired to a peer
	// source, there is nobody to broadcast to.
	var peers []Peer
	if w.cfg.Peers != nil {
		peers = w.cfg.Peers()
	}

	run := &broadcastRun{
		req:      b,
		opts:     opts,
		quit:     w.quit,
		done:     make(chan struct{}),
		finished: make(chan struct{}),
		msgs:     make(chan broadcastMsg),
	}

	// Start a goroutine per peer which sends the message, retries it on
	// timeout, and relays everything the peer says back to us.
	for _, peer := range peers {
		ctl := &BroadcastControl{
			run:      run,
			peerQuit: make(chan struct{}),
		}

		run.peerWG.Add(1)
		go run.servePeer(peer, ctl)
	}

	// Once all peer goroutines are gone nothing more can arrive, which is
	// what wakes the collector below. Delayed completions are tracked by
	// wg instead, as they only end once the collector is done.
	allPeersGone := make(chan struct{})
	go func() {
		run.peerWG.Wait()
		close(allPeersGone)
	}()

	// Collect messages and run the handler serially until the broadcast
	// is complete.
	err := run.collect(allPeersGone, w.quit, opts.CancelChan)

	// Release every goroutine still parked on us and wait for them, so
	// that no subscription outlives the call.
	close(run.finished)
	run.peerWG.Wait()
	run.wg.Wait()

	return err
}

// collect feeds the messages received from the peers to the response handler
// until the broadcast is over, and returns the reason it ended.
func (r *broadcastRun) collect(allPeersGone <-chan struct{},
	quit, cancel <-chan struct{}) error {

	for {
		select {
		// Every peer is done, either because the handler said so, it
		// timed out on all its attempts, or it disconnected.
		case <-allPeersGone:
			return nil

		// The handler found what it was looking for.
		case <-r.done:
			return nil

		case <-cancel:
			return ErrJobCanceled

		case <-quit:
			return ErrWorkManagerShuttingDown

		case m := <-r.msgs:
			// A peer that was already marked done may still have a
			// message in flight, which we no longer care about.
			select {
			case <-m.ctl.peerQuit:
				continue
			default:
			}

			r.req.HandleResp(m.peer, m.msg, m.ctl)
		}
	}
}

// servePeer sends the broadcast message to a single peer, retrying after each
// timeout, and relays the messages the peer sends back to the collector. It
// returns once the peer is done, has disconnected, has exhausted its attempts,
// or the broadcast is over.
func (r *broadcastRun) servePeer(peer Peer, ctl *BroadcastControl) {
	defer r.peerWG.Done()

	// Subscribe before the first send so that a fast response can't be
	// missed.
	recv, cancelSub := peer.SubscribeRecvMsg()
	defer cancelSub()

	disconnected := peer.OnDisconnect()

	for try := uint8(0); try < r.opts.NumRetries; try++ {
		timer := time.NewTimer(r.opts.Timeout)
		peer.QueueMessageWithEncoding(r.req.Msg, nil, r.opts.Encoding)

		// Wait for this attempt to either time out or end for good.
		ended, retry := r.awaitAttempt(
			peer, ctl, recv, timer.C, disconnected,
		)
		timer.Stop()
		if ended || !retry {
			return
		}
	}
}

// awaitAttempt relays peer messages until the attempt timer fires. The first
// return value is true if the peer goroutine must stop, in which case retry is
// meaningless.
func (r *broadcastRun) awaitAttempt(peer Peer, ctl *BroadcastControl,
	recv <-chan wire.Message, timeout <-chan time.Time,
	disconnected <-chan struct{}) (bool, bool) {

	for {
		select {
		case msg := <-recv:
			// Hand the message to the collector, unless the
			// broadcast ends in the meantime.
			select {
			case r.msgs <- broadcastMsg{
				peer: peer, msg: msg, ctl: ctl,
			}:
			case <-r.finished:
				return true, false
			case <-ctl.peerQuit:
				return true, false
			}

		case <-timeout:
			return false, true

		case <-ctl.peerQuit:
			return true, false

		case <-disconnected:
			return true, false

		case <-r.done:
			return true, false

		case <-r.finished:
			return true, false

		case <-r.quit:
			return true, false

		case <-r.opts.CancelChan:
			return true, false
		}
	}
}
