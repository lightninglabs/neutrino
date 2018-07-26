// NOTE: THIS API IS UNSTABLE AND WILL BE MOVED TO ITS OWN PACKAGE OR REFACTORED
// OUT.

package neutrino

import (
	"fmt"

	"github.com/btcsuite/btcd/wire"
)

// messageType describes the type of blockMessage.
type messageType int

const (
	// connectBasic is a type of notification sent whenever we connect a
	// new set of basic filter headers to the end of the main chain.
	connectBasic messageType = iota

	// disconnect is a type of filter notification that is sent whenever a
	// block is disconnected from the end of the main chain.
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
	onDisconnect   chan<- wire.BlockHeader

	quit <-chan struct{}

	notifyBlock chan *blockMessage
	intQuit     chan struct{}
}

// sendSubscribedMsg sends all block subscribers a message if they request this
// type.
//
// TODO(aakselrod): Refactor so we're able to handle more message types in new
// package.
func (s *ChainService) sendSubscribedMsg(bm *blockMessage) {

	s.mtxSubscribers.RLock()
	for sub := range s.blockSubscribers {
		sendMsgToSubscriber(sub, bm)
	}
	s.mtxSubscribers.RUnlock()
}

// sendMsgToSubscriber is a helper function that sends the target message to
// the subscription client over the proper channel based on the type of the new
// block notification.
func sendMsgToSubscriber(sub *blockSubscription, bm *blockMessage) {

	var subChan chan<- wire.BlockHeader

	switch bm.msgType {
	case connectBasic:
		subChan = sub.onConnectBasic
	case disconnect:
		subChan = sub.onDisconnect
	default:
		// TODO: Return a useful error when factored out into its own
		// package.
		panic("invalid message type")
	}

	// If the subscription channel was found for this subscription based on
	// the new update, then we'll wait to either send this notification, or
	// quit from either signal.
	if subChan != nil {
		select {
		case sub.notifyBlock <- bm:

		case <-sub.quit:

		case <-sub.intQuit:
		}
	}
}

// subscribeBlockMsg handles adding block subscriptions to the ChainService.
// The best known height to the caller should be passed in, such that we can
// send a backlog of notifications to the caller if they're behind the current
// best tip.
//
// TODO(aakselrod): move this to its own package and refactor so that we're not
// modifying an object held by the caller.
func (s *ChainService) subscribeBlockMsg(bestHeight uint32,
	onConnectBasic, onDisconnect chan<- wire.BlockHeader,
	quit <-chan struct{}) (*blockSubscription, error) {

	subscription := blockSubscription{
		onConnectBasic: onConnectBasic,
		onDisconnect:   onDisconnect,
		quit:           quit,
		notifyBlock:    make(chan *blockMessage),
		intQuit:        make(chan struct{}),
	}

	// At this point, we'll now check to see if we need to deliver any
	// backlog notifications as its possible that while the caller is
	// requesting right after a new set of blocks has been connected.
	err := s.blockManager.SynchronizeFilterHeaders(func(filterHeaderTip uint32) error {
		s.mtxSubscribers.Lock()
		defer s.mtxSubscribers.Unlock()

		s.blockSubscribers[&subscription] = struct{}{}
		go subscription.subscriptionHandler()

		// If the best height matches the filter header tip, then we're
		// done and don't need to proceed any further.
		if filterHeaderTip == bestHeight {
			return nil
		}

		log.Debugf("Delivering backlog block notifications from "+
			"height=%v, to height=%v", bestHeight, filterHeaderTip)

		// Otherwise, we need to read block headers from disk to
		// deliver a backlog to the caller before we proceed. We'll use
		// this synchronization method to ensure the filter header
		// state doesn't change until we're finished catching up the
		// caller.
		for currentHeight := bestHeight + 1; currentHeight <= filterHeaderTip; currentHeight++ {
			blockHeader, err := s.BlockHeaders.FetchHeaderByHeight(
				currentHeight,
			)
			if err != nil {
				return fmt.Errorf("unable to read header at "+
					"height: %v: %v", currentHeight, err)
			}

			sendMsgToSubscriber(&subscription, &blockMessage{
				msgType: connectBasic,
				header:  blockHeader,
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &subscription, nil
}

// unsubscribeBlockMsgs handles removing block subscriptions from the
// ChainService.
//
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
	// we try to send, queue it and continue with the loop. If a quit
	// signal is sent, let the loop know.
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
				ntfns[0] = nil // Set to nil to avoid GC leak.
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
