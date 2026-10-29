package rescan

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcjson"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/filtermatch"
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
// wallet layer. The neutrino package maps its rpcclient.NotificationHandlers
// onto them one for one, so each has the signature and the place in the
// callback order that the legacy rescan loop gave it. Every field is
// optional.
//
// For each scanned block, the actor calls, in order: OnRedeemingTx and then
// OnRecvTx for each relevant transaction, in block order, then
// OnFilteredBlockConnected, then OnBlockConnected. A disconnect notification
// calls OnFilteredBlockDisconnected and then OnBlockDisconnected. A rewind
// calls them the other way around, OnBlockDisconnected first, and not at all
// if its update disabled them.
//
// Every callback, and ActorConfig.OnError, runs on the actor goroutine inside
// a mailbox turn. A callback must therefore not call back into the actor: a
// call to AddWatchAddrs or CurrentState from a callback waits behind its own
// turn until it times out, and a call to Stop stalls for its stop timeout.
type Callbacks struct {
	// OnFilteredBlockConnected is called for every scanned block, matched
	// or not. The relevantTxs slice holds the block's transactions that
	// pay a watched script or spend a watched outpoint. It is nil when
	// the block was not fetched, and empty when it was fetched and
	// nothing in it matched.
	OnFilteredBlockConnected func(height int32, header *wire.BlockHeader,
		relevantTxs []*btcutil.Tx)

	// OnBlockConnected is called for every scanned block, right after
	// OnFilteredBlockConnected, with the block's header timestamp.
	OnBlockConnected func(hash *chainhash.Hash, height int32,
		t time.Time)

	// OnFilteredBlockDisconnected is called for every block the rescan
	// undoes.
	OnFilteredBlockDisconnected func(height int32, header *wire.BlockHeader)

	// OnBlockDisconnected is called for every block the rescan undoes,
	// with the block's header timestamp.
	OnBlockDisconnected func(hash *chainhash.Hash, height int32,
		t time.Time)

	// OnRedeemingTx is called for each relevant transaction that spends
	// a watched outpoint.
	OnRedeemingTx func(tx *btcutil.Tx, details *btcjson.BlockDetails)

	// OnRecvTx is called for each relevant transaction that pays a
	// watched address. A transaction that does both gets OnRedeemingTx
	// first.
	OnRecvTx func(tx *btcutil.Tx, details *btcjson.BlockDetails)

	// OnRescanProgress is called every ProgressInterval blocks during
	// historical sync. The legacy rescan has no such callback, so the
	// neutrino package leaves it unset.
	OnRescanProgress func(height int32)

	// OnRescanFinished is called when the rescan catches up to the chain
	// tip. It is called again only after a rewind. The neutrino package
	// leaves it unset, as the legacy rescan never reported it.
	OnRescanFinished func(height int32)
}

// ActorConfig configures a new RescanActor.
type ActorConfig struct {
	// Chain provides access to headers, filters, blocks, and
	// subscriptions.
	Chain ChainSource

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
	WatchInputs []filtermatch.InputWithScript

	// Callbacks provides notification hooks back to the wallet layer.
	Callbacks Callbacks

	// BatchSize controls how many blocks are processed per batch during
	// historical sync. Defaults to DefaultBatchSize if zero.
	BatchSize int

	// EndBlock is the block the rescan stops at, once it has scanned it.
	// The zero value never matches, so the rescan follows the chain until
	// it is stopped. A non-zero height matches by height, and a non-zero
	// hash by hash.
	EndBlock headerfs.BlockStamp

	// RetryInterval is how long the actor waits before it retries a
	// batch that failed. Defaults to three seconds if zero.
	RetryInterval time.Duration

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

	// started is set once Start has started the mailbox, so Stop knows
	// whether a barrier request can be answered.
	started atomic.Bool

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

	// turnMu is held for each mailbox turn, so WaitForShutdown can wait
	// for the turn in flight, and with it the last callback.
	turnMu sync.Mutex

	// done is closed, and err set, when the rescan ends: it reached its
	// end block, its subscription failed, or Stop ran.
	doneOnce sync.Once
	done     chan struct{}
	err      error
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

	// Create the FSM with initial state and immutable environment.
	machine := newStateMachine(
		&StateInitializing{Cursor: Cursor{
			CurStamp:  cfg.StartStamp,
			CurHeader: cfg.StartHeader,
			Watch:     watch,

			// The start block itself decides whether the first
			// scanned block is checked, as in the legacy rescan,
			// since a block's timestamp may precede its parent's.
			Scanning: cfg.StartHeader.Timestamp.After(
				cfg.StartTime,
			),
		}},
		&Environment{
			Chain:     cfg.Chain,
			StartTime: cfg.StartTime,
			BatchSize: cfg.BatchSize,
			EndBlock:  cfg.EndBlock,
		},
	)

	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = retryInterval
	}

	a := &RescanActor{
		machine: machine,
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
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
		a.started.Store(true)
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

		// Wait for the turn in flight, if the mailbox ever started.
		// Before Start nothing would answer, so the barrier would only
		// cost the full stopTimeout.
		if a.started.Load() {
			stopCtx, cancel := context.WithTimeout(
				context.Background(), stopTimeout,
			)
			result := a.ref.Ask(stopCtx, &stopRescanMsg{}).
				Await(stopCtx)
			cancel()
			if result.IsErr() {
				log.Debugf("RescanActor: graceful stop "+
					"failed: %v", result.Err())
			}
		}

		a.actor.Stop()
		a.cancelBridge()
		a.machine.Stop()
		a.finish(ErrStopped)
	})
}

// WaitForShutdown blocks until the bridge, retry and continuation goroutines
// have stopped and no mailbox turn is in flight. Once Stop has run, no
// callback starts after it returns.
func (a *RescanActor) WaitForShutdown() {
	a.asyncWG.Wait()

	a.turnMu.Lock()
	defer a.turnMu.Unlock()
}

// Done returns a channel that is closed once the rescan has ended: it
// reached its end block, its block subscription failed, or Stop ran. Err
// then says which.
func (a *RescanActor) Done() <-chan struct{} {
	return a.done
}

// Err returns nil if the rescan ended at its end block, the subscription
// error if its subscription failed, and an error if Stop ended it. It
// returns nil until Done is closed.
func (a *RescanActor) Err() error {
	select {
	case <-a.done:
		return a.err
	default:
		return nil
	}
}

// finish records why the rescan ended and closes done. Only the first call
// counts.
func (a *RescanActor) finish(err error) {
	a.doneOnce.Do(func() {
		a.err = err
		close(a.done)
	})
}

// WatchUpdate is a watch-list update for AddWatchAddrs.
type WatchUpdate struct {
	// Addrs are addresses to watch for payments.
	Addrs []address.Address

	// Inputs are outpoints to watch for spends.
	Inputs []filtermatch.InputWithScript

	// RewindTo, if set below the scanned tip, rewinds the rescan to that
	// height: every block above it is disconnected and scanned again, and
	// the block at RewindTo itself is kept.
	RewindTo *uint32

	// DisableDisconnectedNtfns suppresses the disconnect callbacks of
	// the blocks the rewind undoes.
	DisableDisconnectedNtfns bool
}

// AddWatchAddrs sends an AddWatchAddrs mailbox request so watch-list changes
// are serialized with all other actor work and any FSM failure is surfaced to
// the caller. Without a rewind the update applies to blocks scanned after it.
// With one, the rescan disconnects every block above the rewind height and
// scans them again, so the block at the rewind height itself is kept. A
// timeout leaves the outcome unknown: the update may still apply after the
// call returns.
func (a *RescanActor) AddWatchAddrs(update WatchUpdate) error {
	req := newAddWatchAddrsMsg(update)

	reqCtx, cancel := context.WithTimeout(
		context.Background(), requestTimeout,
	)
	defer cancel()

	result := a.ref.Ask(reqCtx, req).Await(reqCtx)
	if result.IsErr() {
		return stoppedErr(result.Err())
	}

	return nil
}

// stoppedErr maps the mailbox's error for a request to a stopped actor to
// ErrStopped, and returns any other error unchanged.
func stoppedErr(err error) error {
	if errors.Is(err, lndactor.ErrActorTerminated) {
		return ErrStopped
	}

	return err
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
