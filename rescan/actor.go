package rescan

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/address/v2"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	neutrino "github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	lndactor "github.com/lightningnetwork/lnd/actor"
)

const (
	// retryInterval is how long the actor waits before it retries a
	// Syncing or Rewinding batch that failed.
	retryInterval = 3 * time.Second

	// requestTimeout bounds how long AddWatchAddrs and CurrentState wait
	// for the mailbox to answer.
	requestTimeout = 5 * time.Second

	// stopTimeout bounds how long Stop waits for the turn in flight.
	stopTimeout = time.Second
)

// errSubscriptionClosed is reported when the block subscription closes
// while the rescan is still running, which happens once the ChainService
// stops.
var errSubscriptionClosed = errors.New(
	"rescan block subscription was canceled",
)

// Callbacks defines the notification callbacks from the rescan actor to the
// wallet layer. These correspond to the existing rpcclient.NotificationHandlers
// but are simplified for the actor model.
//
// Every callback, and ActorConfig.OnError, runs on the actor goroutine inside
// a mailbox turn. A callback must therefore not call back into the actor: a
// call to AddWatchAddrs or CurrentState from a callback waits behind its own
// turn until it times out, and a call to Stop stalls for its stop timeout.
type Callbacks struct {
	// OnFilteredBlockConnected is called for every scanned block, matched
	// or not, before OnBlockConnected. The relevantTxs slice holds the
	// block's transactions that pay a watched script or spend a watched
	// outpoint, and is empty for a block with none.
	OnFilteredBlockConnected func(height int32, header *wire.BlockHeader,
		relevantTxs []*btcutil.Tx)

	// OnBlockConnected is called for every block that is connected,
	// regardless of whether it matches the watch list. Used to advance
	// the wallet's sync state.
	OnBlockConnected func(height int32, header *wire.BlockHeader,
		timestamp time.Time)

	// OnBlockDisconnected is called when a block is disconnected during
	// a reorg or rewind.
	OnBlockDisconnected func(height int32, header *wire.BlockHeader)

	// OnFilteredBlockDisconnected is called when a block is disconnected,
	// just before OnBlockDisconnected. Matches the old rescan's behavior
	// of calling both OnFilteredBlockDisconnected and OnBlockDisconnected.
	OnFilteredBlockDisconnected func(height int32, header *wire.BlockHeader)

	// OnRescanProgress is called periodically during historical sync to
	// report progress.
	OnRescanProgress func(height int32)

	// OnRescanFinished is called when the rescan catches up to the chain
	// tip. It is called again only after a rewind.
	OnRescanFinished func(height int32)

	// OnTxConfirmed is called for each relevant transaction found in a
	// block. This is used to mark transactions as confirmed on the
	// broadcaster, preventing unnecessary rebroadcasts.
	OnTxConfirmed func(txHash chainhash.Hash)
}

// ActorConfig configures a new RescanActor.
type ActorConfig struct {
	// Chain provides access to headers, filters, blocks, and
	// subscriptions.
	Chain neutrino.ChainSource

	// StartStamp is the block stamp to begin scanning from. It counts as
	// already scanned, so the first block scanned is StartStamp.Height+1.
	// It must be on the active chain.
	StartStamp headerfs.BlockStamp

	// StartHeader is the header at StartStamp.
	StartHeader wire.BlockHeader

	// StartTime is the wallet birthday. Blocks before this time are
	// skipped during filter matching.
	StartTime time.Time

	// WatchAddrs is the initial set of addresses to watch.
	WatchAddrs []address.Address

	// WatchInputs is the initial set of outpoints to watch for spends.
	WatchInputs []neutrino.InputWithScript

	// QueryOpts are passed through to chain queries.
	QueryOpts []neutrino.QueryOption

	// Callbacks provides notification hooks back to the wallet layer.
	Callbacks Callbacks

	// BatchSize controls how many blocks are processed per batch during
	// historical sync. Defaults to DefaultBatchSize if zero.
	BatchSize int

	// OnError reports errors for observation only. It is called when a
	// Syncing or Rewinding batch fails, which the actor retries on its
	// own after retryInterval, and once when the block subscription
	// fails, after which the rescan has stopped in StateTerminal. Like the
	// callbacks, it runs on the actor goroutine.
	OnError func(err error)
}

// RescanActor wraps the rescan FSM in the lnd actor runtime. All external API
// calls, continuations, retries, and subscription bridge notifications are
// routed through one mailbox and handled by Receive, one at a time on the
// actor goroutine.
type RescanActor struct {
	// machine holds the FSM state. Only the actor goroutine drives it.
	machine *stateMachine
	cfg     ActorConfig

	// ctx is cancelled by Stop. It cancels in-flight chain queries, the
	// bridge, and the retry and continuation goroutines.
	ctx    context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	stopOnce  sync.Once

	// actor hosts the mailbox, and ref is its reference. Both are fixed
	// at construction, so every method can use them without a lock.
	actor *lndactor.Actor[RescanActorMsg, RescanActorResp]
	ref   lndactor.ActorRef[RescanActorMsg, RescanActorResp]

	// bridgeMu guards the subscription and its bridge, which Start
	// creates and both the actor goroutine and Stop cancel.
	bridgeMu     sync.Mutex
	bridgeCancel context.CancelFunc
	sub          *blockntfns.Subscription

	// asyncWG tracks the bridge, retry and continuation goroutines. It
	// does not cover the actor goroutine itself.
	asyncWG sync.WaitGroup

	// continuationPending is set while a continuation is on its way to
	// the mailbox, so at most one is in flight. Only the actor goroutine
	// touches it.
	continuationPending bool
}

// NewRescanActor creates a new rescan actor with the given configuration.
// Call Start to begin processing. A request sent before Start waits in the
// mailbox until Start runs. It fails if a watch address has no payment
// script.
func NewRescanActor(cfg ActorConfig) (*RescanActor, error) {
	// Build the initial watch list the same way a later update extends
	// it, so a bad address fails here instead of staying in the list and
	// failing every later update.
	watch, err := (&WatchState{}).ApplyUpdate(&AddWatchAddrsEvent{
		Addrs:  cfg.WatchAddrs,
		Inputs: copyInputsWithScripts(cfg.WatchInputs),
	})
	if err != nil {
		return nil, fmt.Errorf("invalid watch address: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	queryOpts := append([]neutrino.QueryOption(nil), cfg.QueryOpts...)
	queryOpts = append(queryOpts, neutrino.CancelQuery(ctx.Done()))

	// Create the FSM with initial state and immutable environment.
	machine := newStateMachine(
		&StateInitializing{Cursor: Cursor{
			CurStamp:  cfg.StartStamp,
			CurHeader: cfg.StartHeader,
			Watch:     watch,
		}},
		&Environment{
			Chain:     cfg.Chain,
			StartTime: cfg.StartTime,
			QueryOpts: queryOpts,
			BatchSize: cfg.BatchSize,
		},
	)

	a := &RescanActor{
		machine: machine,
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
	}

	act, err := lndactor.NewActor(lndactor.ActorConfig[
		RescanActorMsg, RescanActorResp,
	]{
		ID:          rescanActorID,
		Behavior:    a,
		MailboxSize: mailboxSize,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	a.actor = act
	a.ref = act.Ref()

	return a, nil
}

// Start begins the rescan actor. It starts the FSM and the mailbox,
// subscribes to block notifications, and enqueues the first batch. Start does
// nothing once Stop has run.
func (a *RescanActor) Start() {
	a.startOnce.Do(func() {
		if a.ctx.Err() != nil {
			return
		}

		a.machine.Start(a.ctx)
		a.actor.Start()

		if err := a.startBridge(); err != nil {
			a.ref.Tell(a.ctx, &subscriptionFailedMsg{Err: err})
			return
		}
		a.ref.Tell(a.ctx, &processNextBlockMsg{})
	})
}

// Stop signals the rescan actor to shut down. It cancels in-flight chain
// queries, waits briefly for the mailbox to finish its current turn and
// record the stop, then stops the mailbox, the subscription and the FSM. Stop
// may run before, during or after Start.
func (a *RescanActor) Stop() {
	a.stopOnce.Do(func() {
		// Cancel a network query before waiting for the actor mailbox.
		a.cancel()

		stopCtx, cancel := context.WithTimeout(
			context.Background(), stopTimeout,
		)
		result := a.ref.Ask(stopCtx, &stopRescanMsg{}).Await(stopCtx)
		cancel()
		if result.IsErr() {
			log.Debugf("RescanActor: graceful stop failed: %v",
				result.Err())
		}

		a.actor.Stop()
		a.cancelBridge()
		a.machine.Stop()
	})
}

// WaitForShutdown blocks until the bridge, retry and continuation goroutines
// have stopped. It does not wait for the actor goroutine.
func (a *RescanActor) WaitForShutdown() {
	a.asyncWG.Wait()
}

// AddWatchAddrs sends an AddWatchAddrs mailbox request so watch-list changes
// are serialized with all other actor work and any FSM failure is surfaced to
// the caller. Without rewindTo the update applies to blocks scanned after it.
// With rewindTo, the rescan disconnects every block above that height and
// scans them again, so the block at rewindTo itself is kept. A timeout leaves
// the outcome unknown: the update may still apply after the call returns.
func (a *RescanActor) AddWatchAddrs(
	addrs []address.Address,
	inputs []neutrino.InputWithScript,
	rewindTo *uint32,
) error {

	req := newAddWatchAddrsMsg(addrs, inputs, rewindTo)

	reqCtx, cancel := context.WithTimeout(
		context.Background(), requestTimeout,
	)
	defer cancel()

	result := a.ref.Ask(reqCtx, req).Await(reqCtx)
	if result.IsErr() {
		return result.Err()
	}

	return nil
}

// CurrentState returns the current FSM state after all prior mailbox messages
// have been processed.
func (a *RescanActor) CurrentState() (RescanState, error) {
	queryCtx, cancel := context.WithTimeout(
		context.Background(), requestTimeout,
	)
	defer cancel()

	result := a.ref.Ask(queryCtx, &currentStateReq{}).
		Await(queryCtx)
	if result.IsErr() {
		return nil, result.Err()
	}

	resp, ok := result.UnwrapOr(nil).(*currentStateResp)
	if !ok || resp == nil {
		return nil, fmt.Errorf(
			"unexpected current state response",
		)
	}

	return resp.State, nil
}
