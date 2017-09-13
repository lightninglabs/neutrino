// NOTE: THIS API IS UNSTABLE AND WILL BE MOVED TO ITS OWN PACKAGE OR REFACTORED
// OUT.

package neutrino

import "github.com/roasbeef/btcd/wire"

// messageType describes the type of blockMessage.
type messageType int

const (
	connectBasic messageType = iota
	connectExt
	disconnect
)

// blockMessage is a notification from the block manager to a block
// subscription's goroutine to be forwarded on via the appropriate channel.
type blockMessage struct {
	header  *wire.BlockHeader
	msgType messageType
}

// blockSubscription allows a client to subscribe to and unsubscribe from block
// connect and disconnect notifications.
// TODO(aakselrod): Move this to its own package so that the subscriber can't
// access internals, in particular the notifyBlock and intQuit members.
type blockSubscription struct {
	onConnectBasic chan<- wire.BlockHeader
	onConnectExt   chan<- wire.BlockHeader
	onDisconnect   chan<- wire.BlockHeader
	quit           <-chan struct{}

	notifyBlock chan *blockMessage
	intQuit     chan struct{}
}

// sendSubscribedMsg sends all block subscribers a message if they request this
// type.
// TODO(aakselrod): Refactor so we're able to handle more message types in new
// package.
func (s *ChainService) sendSubscribedMsg(bm *blockMessage) {
	var subChan chan<- wire.BlockHeader
	s.mtxSubscribers.RLock()
	for sub := range s.blockSubscribers {
		switch bm.msgType {
		case connectBasic:
			subChan = sub.onConnectBasic
		case connectExt:
			subChan = sub.onConnectExt
		case disconnect:
			subChan = sub.onDisconnect
		default:
			// TODO: Return a useful error when factored out into
			// its own package.
			panic("invalid message type")
		}
		if subChan != nil {
			select {
			case sub.notifyBlock <- bm:
			case <-sub.quit:
			case <-sub.intQuit:
			}
		}
	}
	s.mtxSubscribers.RUnlock()
}

// subscribeBlockMsg handles adding block subscriptions to the ChainService.
// TODO(aakselrod): move this to its own package and refactor so that we're
// not modifying an object held by the caller.
func (s *ChainService) subscribeBlockMsg(onConnectBasic, onConnectExt,
	onDisconnect chan<- wire.BlockHeader,
	quit <-chan struct{}) *blockSubscription {
	s.mtxSubscribers.Lock()
	defer s.mtxSubscribers.Unlock()
	subscription := blockSubscription{
		onConnectBasic: onConnectBasic,
		onConnectExt:   onConnectExt,
		onDisconnect:   onDisconnect,
		quit:           quit,
		notifyBlock:    make(chan *blockMessage),
		intQuit:        make(chan struct{}),
	}
	s.blockSubscribers[&subscription] = struct{}{}
	go subscription.subscriptionHandler()
	return &subscription
}

// unsubscribeBlockMsgs handles removing block subscriptions from the
// ChainService.
// TODO(aakselrod): move this to its own package and refactor so that we're
// not depending on the caller to not modify the argument between subscribe and
// unsubscribe.
func (s *ChainService) unsubscribeBlockMsgs(subscription *blockSubscription) {
	s.mtxSubscribers.Lock()
	delete(s.blockSubscribers, subscription)
	s.mtxSubscribers.Unlock()
	close(subscription.intQuit)

	// Drain the inbound notification channel
cleanup:
	for {
		select {
		case <-subscription.notifyBlock:
		default:
			break cleanup
		}
	}
}

// subscriptionHandler must be run as a goroutine and queues notification
// messages from the chain service to the subscriber.
func (s *blockSubscription) subscriptionHandler() {
	// Start with a small queue; it will grow if needed.
	ntfns := make([]*blockMessage, 0, 5)
	var next *blockMessage

	// Try to send on the specified channel. If a new message arrives while
	// we try to send, queue it and continue with the loop. If a quit signal
	// is sent, let the loop know.
	selectChan := func(notify chan<- wire.BlockHeader) bool {
		if notify == nil {
			select {
			case <-s.quit:
				return false
			case <-s.intQuit:
				return false
			default:
				return true
			}
		}
		select {
		case notify <- *next.header:
			next = nil
			return true
		case queueMsg := <-s.notifyBlock:
			ntfns = append(ntfns, queueMsg)
			return true
		case <-s.quit:
			return false
		case <-s.intQuit:
			return false
		}
	}

	// Loop until we get a signal on s.quit or s.intQuit.
	for {
		if next != nil {
			// If selectChan returns false, we were signalled on
			// s.quit or s.intQuit.
			switch next.msgType {
			case connectBasic:
				if !selectChan(s.onConnectBasic) {
					return
				}
			case connectExt:
				if !selectChan(s.onConnectExt) {
					return
				}
			case disconnect:
				if !selectChan(s.onDisconnect) {
					return
				}
			}
		} else {
			// Next notification is nil, so see if we can get a
			// notification from the queue. If not, we wait for a
			// notification on s.notifyBlock or quit if signalled.
			if len(ntfns) > 0 {
				next = ntfns[0]
				ntfns = ntfns[1:]
			} else {
				select {
				case next = <-s.notifyBlock:
				case <-s.quit:
					return
				case <-s.intQuit:
					return
				}
			}
		}
	}
}
