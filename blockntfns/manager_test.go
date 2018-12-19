package blockntfns_test

import (
	"errors"
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/blockntfns"
)

var emptyHeader wire.BlockHeader

type mockNtfnSource struct {
	blockChan         chan blockntfns.BlockNtfn
	blocksSinceHeight func(uint32) ([]blockntfns.BlockNtfn, uint32, error)
}

func newMockBlockSource() *mockNtfnSource {
	return &mockNtfnSource{
		blockChan: make(chan blockntfns.BlockNtfn),
	}
}

func (s *mockNtfnSource) Notifications() <-chan blockntfns.BlockNtfn {
	return s.blockChan
}

func (s *mockNtfnSource) NotificationsSinceHeight(
	height uint32) ([]blockntfns.BlockNtfn, uint32, error) {

	if s.blocksSinceHeight != nil {
		return s.blocksSinceHeight(height)
	}

	return nil, 0, nil
}

// TestManagerNewSubscription ensures that a client properly receives new
// block notifications once it successfully registers for a subscription.
func TestManagerNewSubscription(t *testing.T) {
	t.Parallel()

	// We'll start by creating a subscription manager backed by our mocked
	// block source.
	blockSource := newMockBlockSource()
	subMgr := blockntfns.NewSubscriptionManager(blockSource)
	subMgr.Start()
	defer subMgr.Stop()

	// We'll create some notifications that will be delivered to a
	// registered client.
	tipHeight := uint32(0)
	newNotifications := make([]blockntfns.BlockNtfn, 0, 20)
	for i := uint32(0); i < 20; i++ {
		newNotifications = append(newNotifications, blockntfns.NewBlockConnected(
			emptyHeader, i+1,
		))
	}
	staleNotifications := make([]blockntfns.BlockNtfn, 0, 10)
	for i := len(newNotifications) - 1; i >= 10; i-- {
		staleNotifications = append(staleNotifications, blockntfns.NewBlockDisconnected(
			emptyHeader, newNotifications[i].Height(), emptyHeader,
		))
	}

	// We'll register a client and proceed to deliver the notifications to
	// the SubscriptionManager. This should act as if the client is being
	// told of notifications following the tip of the chain.
	sub, err := subMgr.NewSubscription(0)
	if err != nil {
		t.Fatalf("unable to register new subscription: %v", err)
	}

	go func() {
		for _, block := range newNotifications {
			blockSource.blockChan <- block
		}
		for _, block := range staleNotifications {
			blockSource.blockChan <- block
		}
	}()

	// Then, we'll attempt to process these notifications in order from the
	// client's point of view. 20 successive block connected notifications
	// should be received, followed by 10 block disconnected notifications.
	for i := 0; i < len(newNotifications)+len(staleNotifications); i++ {
		select {
		case ntfn := <-sub.Notifications:
			switch ntfn := ntfn.(type) {
			case *blockntfns.Connected:
				if ntfn.Height() != tipHeight+1 {
					t.Fatalf("expected new block with "+
						"height %d, got %d",
						tipHeight+1, ntfn.Height())
				}
				tipHeight++

			case *blockntfns.Disconnected:
				if ntfn.Height() != tipHeight {
					t.Fatalf("expected stale block with "+
						"height %d, got %d", tipHeight,
						ntfn.Height())
				}
				tipHeight--
			}

		case <-time.After(time.Second):
			t.Fatal("expected to receive block notification")
		}
	}

	// Finally, the client's height should match as expected after
	// processing all the notifications in order.
	if tipHeight != 10 {
		t.Fatalf("expected chain tip with height %d, got %d", 10,
			tipHeight)
	}
}

// TestManagerCancelSubscription ensures that when a client desires to cancel
// their subscription, that they are no longer delivered any new notifications
// after the fact.
func TestManagerCancelSubscription(t *testing.T) {
	t.Parallel()

	// We'll start by creating a subscription manager backed by our mocked
	// block source.
	blockSource := newMockBlockSource()
	subMgr := blockntfns.NewSubscriptionManager(blockSource)
	subMgr.Start()
	defer subMgr.Stop()

	// We'll create two client subscriptions to ensure subscription
	// cancellation works as intended. We'll be canceling the second
	// subscription only.
	sub1, err := subMgr.NewSubscription(0)
	if err != nil {
		t.Fatalf("unable to register new subscription: %v", err)
	}
	sub2, err := subMgr.NewSubscription(0)
	if err != nil {
		t.Fatalf("unable to register new subscription: %v", err)
	}

	// We'll send a single block connected notification to both clients.
	go func() {
		blockSource.blockChan <- blockntfns.NewBlockConnected(
			emptyHeader, 1,
		)
	}()

	// Both of them should receive it.
	subs := []*blockntfns.Subscription{sub1, sub2}
	for _, sub := range subs {
		select {
		case _, ok := <-sub.Notifications:
			if !ok {
				t.Fatal("expected to continue receiving " +
					"notifications")
			}
		case <-time.After(time.Second):
			t.Fatalf("expected block connected notification")
		}
	}

	// Now, we'll attempt to deliver another block connected notification,
	// but this time we'll cancel the second subscription.
	sub2.Cancel()

	go func() {
		blockSource.blockChan <- blockntfns.NewBlockConnected(
			emptyHeader, 2,
		)
	}()

	// The first subscription should still see the new notification come
	// through.
	select {
	case _, ok := <-sub1.Notifications:
		if !ok {
			t.Fatalf("expected to continue receiving notifications")
		}
	case <-time.After(time.Second):
		t.Fatalf("expected block connected notification")
	}

	// However, the second subscription shouldn't.
	select {
	case _, ok := <-sub2.Notifications:
		if ok {
			t.Fatalf("expected closed NotificationsConnected channel")
		}
	case <-time.After(time.Second):
		t.Fatalf("expected closed NotificationsConnected channel")
	}
}

// TestManagerHistoricalBacklog ensures that when a client registers for a
// subscription with a best known height lower than the current tip of the
// chain, that a historical backlog of notifications is delivered from that
// point forwards.
func TestManagerHistoricalBacklog(t *testing.T) {
	t.Parallel()

	// We'll start by creating a subscription manager backed by our mocked
	// block source.
	blockSource := newMockBlockSource()
	subMgr := blockntfns.NewSubscriptionManager(blockSource)
	subMgr.Start()
	defer subMgr.Stop()

	// We'll make NotificationsSinceHeight return an error to ensure that a
	// client registration fails if it returns an error.
	blockSource.blocksSinceHeight = func(uint32) ([]blockntfns.BlockNtfn,
		uint32, error) {

		return nil, 0, errors.New("")
	}
	sub, err := subMgr.NewSubscription(0)
	if err == nil {
		t.Fatal("expected registration to fail due to not delivering " +
			"backlog")
	}

	// We'll go with the assumption that the tip of the chain is at height
	// 20, while the client's best known height is 10.
	// NotificationsSinceHeight should then return notifications for blocks
	// 11-20.
	const chainTip uint32 = 20
	subCurrentHeight := uint32(chainTip / 2)
	numBacklog := chainTip - subCurrentHeight
	blockSource.blocksSinceHeight = func(uint32) ([]blockntfns.BlockNtfn,
		uint32, error) {

		blocks := make([]blockntfns.BlockNtfn, 0, numBacklog)
		for i := uint32(subCurrentHeight + 1); i <= chainTip; i++ {
			blocks = append(blocks, blockntfns.NewBlockConnected(
				emptyHeader, i,
			))
		}

		return blocks, chainTip, nil
	}

	// Register a new client with the expected current height.
	sub, err = subMgr.NewSubscription(subCurrentHeight)
	if err != nil {
		t.Fatalf("unable to register new subscription: %v", err)
	}

	// Then, we'll attempt to retrieve all of the expected notifications
	// from the historical backlog delivered.
	for i := uint32(0); i < numBacklog; i++ {
		select {
		case ntfn := <-sub.Notifications:
			if ntfn, ok := ntfn.(*blockntfns.Connected); !ok {
				t.Fatalf("expected *blockntfns.Connected "+
					"notification, got %T", ntfn)
			}
			if ntfn.Height() != subCurrentHeight+1 {
				t.Fatalf("expected new block with height %d, "+
					"got %d", subCurrentHeight+1,
					ntfn.Height())
			}
			subCurrentHeight++

		case <-time.After(time.Second):
			t.Fatal("expected to receive historical block " +
				"notification")
		}
	}

	// Finally, the client should now be caught up with the chain.
	if subCurrentHeight != chainTip {
		t.Fatalf("expected client to be caught up to height %d, got %d",
			chainTip, subCurrentHeight)
	}
}
