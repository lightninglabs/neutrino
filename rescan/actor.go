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
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
	neutrino "github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	lndactor "github.com/lightningnetwork/lnd/actor"
	fn "github.com/lightningnetwork/lnd/fn/v2"
)

const (
	// filterRetryInterval is how long to wait before retrying a failed
	// filter fetch.
	filterRetryInterval = 3 * time.Second
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
type Callbacks struct {
	// OnFilteredBlockConnected is called when a block matching the watch
	// list is connected. The relevantTxs slice contains the matching
	// transactions.
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
	// providing the height and header. Matches the old rescan's behavior
	// of calling both OnFilteredBlockDisconnected and OnBlockDisconnected.
	OnFilteredBlockDisconnected func(height int32, header *wire.BlockHeader)

	// OnRescanProgress is called periodically during historical sync to
	// report progress.
	OnRescanProgress func(height int32)

	// OnRescanFinished is called when the rescan catches up to the chain
	// tip.
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

	// StartStamp is the block stamp to begin scanning from.
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

	// OnError is called when the FSM encounters an error processing an
	// event. The caller (e.g., NeutrinoClient) can use this to
	// implement retry logic for transient failures like filter fetch
	// errors.
	OnError func(err error)
}

// RescanActor wraps the rescan FSM in the lnd actor runtime. All external API
// calls, self-tells, retries, and subscription bridge notifications are routed
// through one mailbox and handled by Receive.
type RescanActor struct {
	machine *stateMachine
	cfg     ActorConfig

	ctx    context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	stopOnce  sync.Once

	// actor hosts the mailbox, and ref is its reference. Both are fixed
	// at construction, so every method can use them without a lock.
	actor *lndactor.Actor[RescanActorMsg, RescanActorResp]
	ref   lndactor.ActorRef[RescanActorMsg, RescanActorResp]

	bridgeMu     sync.Mutex
	bridgeCancel context.CancelFunc
	sub          *blockntfns.Subscription

	asyncWG sync.WaitGroup

	// continuationPending is owned by the actor mailbox.
	continuationPending bool
}

// NewRescanActor creates a new rescan actor with the given configuration.
// Call Start() to begin processing.
func NewRescanActor(cfg ActorConfig) *RescanActor {
	ctx, cancel := context.WithCancel(context.Background())

	// Build the initial watch state from config.
	watch := WatchState{
		Addrs: make(
			[]address.Address,
			len(cfg.WatchAddrs),
		),
		Inputs: make(
			[]neutrino.InputWithScript,
			len(cfg.WatchInputs),
		),
	}
	copy(watch.Addrs, cfg.WatchAddrs)
	watch.Inputs = copyInputsWithScripts(cfg.WatchInputs)

	// Convert addresses to pkScripts for the watch list.
	for _, addr := range watch.Addrs {
		script, err := txscript.PayToAddrScript(addr)
		if err != nil {
			log.Errorf(
				"Failed to convert addr to script: %v",
				err,
			)
			continue
		}

		watch.List = append(watch.List, script)
	}
	for _, input := range watch.Inputs {
		watch.List = append(watch.List, input.PkScript)
	}

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
		// NewActor fails only for an empty ID or a nil behavior, and
		// both are fixed here.
		panic(err)
	}
	a.actor = act
	a.ref = act.Ref()

	return a
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
			context.Background(), time.Second,
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

// WaitForShutdown blocks until the bridge and retry goroutines have stopped.
func (a *RescanActor) WaitForShutdown() {
	a.asyncWG.Wait()
}

// AddWatchAddrs sends an AddWatchAddrs mailbox request so watch-list changes
// are serialized with all other actor work and any FSM failure is surfaced to
// the caller.
func (a *RescanActor) AddWatchAddrs(
	addrs []address.Address,
	inputs []neutrino.InputWithScript,
	rewindTo *uint32,
) error {

	req := newAddWatchAddrsMsg(addrs, inputs, rewindTo)

	reqCtx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
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
		context.Background(), 5*time.Second,
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

// Receive processes mailbox messages one at a time in the lnd actor runtime.
func (a *RescanActor) Receive(actorCtx context.Context,
	msg RescanActorMsg) fn.Result[RescanActorResp] {

	switch m := msg.(type) {
	case *processNextBlockMsg:
		a.continuationPending = false
		return a.handleContinuation(actorCtx)

	case *blockConnectedMsg:
		return a.handleFSMEvent(actorCtx, BlockConnectedEvent{
			Header: m.Header,
			Height: m.Height,
		})

	case *blockDisconnectedMsg:
		return a.handleFSMEvent(actorCtx, BlockDisconnectedEvent{
			Header:   m.Header,
			Height:   m.Height,
			ChainTip: m.ChainTip,
		})

	case *addWatchAddrsMsg:
		return a.handleFSMEvent(
			actorCtx,
			AddWatchAddrsEvent{
				Addrs:    m.Addrs,
				Inputs:   m.Inputs,
				RewindTo: m.RewindTo,
			},
		)

	case *currentStateReq:
		return fn.Ok[RescanActorResp](&currentStateResp{
			State: snapshotState(a.machine.CurrentState()),
		})

	case *subscriptionFailedMsg:
		return a.handleSubscriptionFailed(actorCtx, m)

	case *stopRescanMsg:
		return a.handleStop(actorCtx)

	default:
		return fn.Err[RescanActorResp](fmt.Errorf(
			"unknown message type: %T", msg,
		))
	}
}

// handleContinuation runs the next batch. A failed batch leaves the state
// unchanged. It is reported through OnError and retried after
// filterRetryInterval.
func (a *RescanActor) handleContinuation(
	actorCtx context.Context) fn.Result[RescanActorResp] {

	result := a.handleFSMEvent(actorCtx, ProcessNextBlockEvent{})
	if result.IsErr() {
		err := result.Err()
		log.Warnf("RescanActor: error processing next block: %v, "+
			"retrying in %v", err, filterRetryInterval)

		if a.cfg.OnError != nil {
			a.cfg.OnError(err)
		}

		a.scheduleRetry()
	}

	return result
}

func (a *RescanActor) handleFSMEvent(actorCtx context.Context,
	event RescanEvent) fn.Result[RescanActorResp] {

	outbox, err := a.machine.ProcessEvent(actorCtx, event)
	if err != nil {
		return fn.Err[RescanActorResp](err)
	}

	a.dispatchOutbox(outbox)

	return fn.Ok[RescanActorResp](&rescanActorAck{})
}

// scheduleRetry sends a continuation after filterRetryInterval, unless the
// actor stops first.
func (a *RescanActor) scheduleRetry() {
	a.asyncWG.Add(1)
	go func() {
		defer a.asyncWG.Done()

		timer := time.NewTimer(filterRetryInterval)
		defer timer.Stop()

		select {
		case <-a.ctx.Done():
			return

		case <-timer.C:
		}

		a.ref.Tell(a.ctx, &processNextBlockMsg{})
	}()
}

func (a *RescanActor) handleStop(
	actorCtx context.Context) fn.Result[RescanActorResp] {

	return a.handleFSMEvent(actorCtx, StopEvent{})
}

// handleSubscriptionFailed ends the rescan when its block subscription could
// not be created or closed underneath it. Both happen only once the
// ChainService has stopped, so the rescan reports the error once and moves
// to Terminal, as the legacy rescan loop does, rather than retrying.
func (a *RescanActor) handleSubscriptionFailed(actorCtx context.Context,
	msg *subscriptionFailedMsg) fn.Result[RescanActorResp] {

	if a.ctx.Err() != nil || a.machine.CurrentState().IsTerminal() {
		return fn.Ok[RescanActorResp](&rescanActorAck{})
	}

	log.Errorf("RescanActor: %v, stopping", msg.Err)
	if a.cfg.OnError != nil {
		a.cfg.OnError(msg.Err)
	}
	a.cancelBridge()

	return a.handleFSMEvent(actorCtx, StopEvent{})
}

// dispatchOutbox processes outbox events emitted by FSM state transitions.
func (a *RescanActor) dispatchOutbox(outbox []OutboxEvent) {
	for _, event := range outbox {
		select {
		case <-a.ctx.Done():
			return
		default:
		}

		switch e := event.(type) {
		case FilteredBlockOutbox:
			if a.cfg.Callbacks.OnFilteredBlockConnected != nil {
				a.cfg.Callbacks.OnFilteredBlockConnected(
					e.Height, e.Header, e.RelevantTxs,
				)
			}

			if a.cfg.Callbacks.OnTxConfirmed != nil {
				for _, tx := range e.RelevantTxs {
					confirm := a.cfg.Callbacks.OnTxConfirmed
					confirm(*tx.Hash())
				}
			}

		case BlockConnectedOutbox:
			if a.cfg.Callbacks.OnBlockConnected != nil {
				a.cfg.Callbacks.OnBlockConnected(
					e.Height, e.Header,
					time.Unix(e.Timestamp, 0),
				)
			}

		case BlockDisconnectedOutbox:
			if a.cfg.Callbacks.OnFilteredBlockDisconnected != nil {
				a.cfg.Callbacks.OnFilteredBlockDisconnected(
					e.Height, e.Header,
				)
			}
			if a.cfg.Callbacks.OnBlockDisconnected != nil {
				a.cfg.Callbacks.OnBlockDisconnected(
					e.Height, e.Header,
				)
			}

		case RescanProgressOutbox:
			if a.cfg.Callbacks.OnRescanProgress != nil {
				a.cfg.Callbacks.OnRescanProgress(e.Height)
			}

		case RescanFinishedOutbox:
			if a.cfg.Callbacks.OnRescanFinished != nil {
				a.cfg.Callbacks.OnRescanFinished(e.Height)
			}

		case ContinueOutbox:
			a.enqueueContinuation()
		}
	}
}

// enqueueContinuation avoids blocking the mailbox on its own bounded queue.
func (a *RescanActor) enqueueContinuation() {
	if a.continuationPending {
		return
	}
	a.continuationPending = true
	a.asyncWG.Add(1)
	go func() {
		defer a.asyncWG.Done()
		a.ref.Tell(a.ctx, &processNextBlockMsg{})
	}()
}

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
					Err: errSubscriptionClosed,
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

// snapshotState returns a copy of state whose watch list the caller may
// mutate without touching the state the machine holds.
func snapshotState(state RescanState) RescanState {
	switch s := state.(type) {
	case *StateInitializing:
		snapshot := *s
		snapshot.Watch = s.Watch.Clone()
		return &snapshot
	case *StateSyncing:
		snapshot := *s
		snapshot.Watch = s.Watch.Clone()
		return &snapshot
	case *StateCurrent:
		snapshot := *s
		snapshot.Watch = s.Watch.Clone()
		return &snapshot
	case *StateRewinding:
		snapshot := *s
		snapshot.Watch = s.Watch.Clone()
		return &snapshot
	default:
		return state
	}
}

// Ensure the mailbox behavior implements the lnd actor behavior interface.
var _ lndactor.ActorBehavior[RescanActorMsg, RescanActorResp] = (*RescanActor)(
	nil,
)

// Ensure all states implement the RescanState interface at compile time.
var _ RescanState = (*StateInitializing)(nil)
var _ RescanState = (*StateSyncing)(nil)
var _ RescanState = (*StateCurrent)(nil)
var _ RescanState = (*StateRewinding)(nil)
var _ RescanState = (*StateTerminal)(nil)

// Ensure all outbox events implement the OutboxEvent interface at compile time.
var _ OutboxEvent = (*FilteredBlockOutbox)(nil)
var _ OutboxEvent = (*BlockConnectedOutbox)(nil)
var _ OutboxEvent = (*BlockDisconnectedOutbox)(nil)
var _ OutboxEvent = (*RescanProgressOutbox)(nil)
var _ OutboxEvent = (*RescanFinishedOutbox)(nil)
var _ OutboxEvent = (*ContinueOutbox)(nil)

// Ensure all events implement the RescanEvent interface at compile time.
var _ RescanEvent = (*BlockConnectedEvent)(nil)
var _ RescanEvent = (*BlockDisconnectedEvent)(nil)
var _ RescanEvent = (*AddWatchAddrsEvent)(nil)
var _ RescanEvent = (*ProcessNextBlockEvent)(nil)
var _ RescanEvent = (*StopEvent)(nil)
