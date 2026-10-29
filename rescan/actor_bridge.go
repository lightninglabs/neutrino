package rescan

import (
	"context"
	"fmt"

	"github.com/lightninglabs/neutrino/blockntfns"
)

// startBridge subscribes to block notifications with no backlog and starts
// the bridge goroutine that turns them into mailbox messages.
func (a *RescanActor) startBridge() error {
	a.bridgeMu.Lock()
	defer a.bridgeMu.Unlock()

	// Stop cancels the context before it cancels the bridge under this
	// lock, so checking it here means a Start racing Stop either leaves
	// the subscription for Stop to cancel or never creates it.
	if err := a.ctx.Err(); err != nil {
		return err
	}

	sub, err := a.cfg.Chain.Subscribe(0)
	if err != nil {
		return fmt.Errorf("unable to register block subscription: %w",
			err)
	}

	bridgeCtx, bridgeCancel := context.WithCancel(a.ctx)
	a.bridgeCancel = bridgeCancel
	a.sub = sub

	a.asyncWG.Add(1)
	go func() {
		defer a.asyncWG.Done()
		a.bridgeSubscription(bridgeCtx, sub)
	}()

	return nil
}

// cancelBridge cancels the current subscription bridge goroutine.
func (a *RescanActor) cancelBridge() {
	a.bridgeMu.Lock()
	defer a.bridgeMu.Unlock()

	if a.bridgeCancel != nil {
		a.bridgeCancel()
		a.bridgeCancel = nil
	}
	if a.sub != nil {
		a.sub.Cancel()
		a.sub = nil
	}
}

// bridgeSubscription reads from a block subscription and converts
// notifications into actor mailbox messages.
func (a *RescanActor) bridgeSubscription(ctx context.Context,
	sub *blockntfns.Subscription) {

	for {
		select {
		case <-ctx.Done():
			return

		case ntfn, ok := <-sub.Notifications:
			if !ok {
				if ctx.Err() != nil {
					return
				}

				log.Info(
					"RescanActor: subscription closed",
				)

				a.ref.Tell(ctx, &subscriptionFailedMsg{
					Err: ErrSubscriptionClosed,
				})

				return
			}

			var msg RescanActorMsg

			switch n := ntfn.(type) {
			case *blockntfns.Connected:
				msg = &blockConnectedMsg{
					Header: n.Header(),
					Height: n.Height(),
				}

			case *blockntfns.Disconnected:
				msg = &blockDisconnectedMsg{
					Header:   n.Header(),
					Height:   n.Height(),
					ChainTip: n.ChainTip(),
				}

			default:
				log.Warnf("RescanActor: unknown notification "+
					"type %T", ntfn)
				continue
			}

			a.ref.Tell(ctx, msg)
		}
	}
}
