package rescan

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
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

var errSubscriptionClosed = errors.New(
	"rescan block subscription closed unexpectedly",
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
	WatchAddrs []btcutil.Address

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
	fsm *StateMachine
	cfg ActorConfig

	ctx    context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	stopOnce  sync.Once

	mu         sync.RWMutex
	startErr   error
	actorID    string
	serviceKey lndactor.ServiceKey[RescanActorMsg, RescanActorResp]
	actorRef   lndactor.ActorRef[RescanActorMsg, RescanActorResp]
	system     *lndactor.ActorSystem

	bridgeMu     sync.Mutex
	bridgeCancel context.CancelFunc
	sub          *blockntfns.Subscription

	asyncWG sync.WaitGroup
}

// NewRescanActor creates a new rescan actor with the given configuration.
// Call Start() to begin processing.
func NewRescanActor(cfg ActorConfig) *RescanActor {
	ctx, cancel := context.WithCancel(context.Background())

	// Build the initial watch state from config.
	watch := WatchState{
		Addrs:  make([]btcutil.Address, len(cfg.WatchAddrs)),
		Inputs: make([]neutrino.InputWithScript, len(cfg.WatchInputs)),
	}
	copy(watch.Addrs, cfg.WatchAddrs)
	copy(watch.Inputs, cfg.WatchInputs)

	// Convert addresses to pkScripts for the watch list.
	for _, addr := range watch.Addrs {
		script, err := txscript.PayToAddrScript(addr)
		if err != nil {
			log.Errorf("Failed to convert addr to script: %v", err)
			continue
		}

		watch.List = append(watch.List, script)
	}
	for _, input := range watch.Inputs {
		watch.List = append(watch.List, input.PkScript)
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	// Create the FSM with initial state and immutable environment.
	fsm := NewStateMachine(StateMachineCfg{
		ErrorReporter: &logErrorReporter{},
		InitialState: &StateInitializing{
			StartStamp:  cfg.StartStamp,
			StartHeader: cfg.StartHeader,
			Watch:       watch,
		},
		Env: &Environment{
			Chain:     cfg.Chain,
			StartTime: cfg.StartTime,
			QueryOpts: cfg.QueryOpts,
			BatchSize: batchSize,
		},
	})

	return &RescanActor{
		fsm:        fsm,
		cfg:        cfg,
		ctx:        ctx,
		cancel:     cancel,
		actorID:    RescanActorServiceKeyName,
		serviceKey: RescanActorServiceKey(),
	}
}

// Start begins the rescan actor. It starts the FSM, spawns the actor runtime,
// and enqueues the initial block-processing message through the mailbox.
func (a *RescanActor) Start() {
	a.startOnce.Do(func() {
		a.fsm.Start()

		system := lndactor.NewActorSystem()
		ref, err := a.serviceKey.Spawn(system, a.actorID, a)
		if err != nil {
			log.Errorf("RescanActor: failed to spawn actor runtime: %v",
				err)

			a.mu.Lock()
			a.startErr = err
			a.system = system
			a.mu.Unlock()

			a.fsm.Stop()
			_ = system.Shutdown()

			return
		}

		a.mu.Lock()
		a.actorRef = ref
		a.system = system
		a.mu.Unlock()

		ref.Tell(context.Background(), &processNextBlockMsg{})
	})
}

// Stop signals the rescan actor to shut down. It asks the mailbox to process a
// stop message so shutdown side effects are serialized with the rest of the
// actor's work, then tears down the runtime and FSM.
func (a *RescanActor) Stop() {
	a.stopOnce.Do(func() {
		ref, system, startErr := a.runtimeSnapshot()
		if startErr == nil && ref != nil {
			stopCtx, cancel := context.WithTimeout(
				context.Background(), time.Second,
			)
			result := ref.Ask(stopCtx, &stopRescanMsg{}).Await(stopCtx)
			cancel()

			if result.IsErr() {
				log.Debugf("RescanActor: graceful stop via mailbox "+
					"failed: %v", result.Err())
			}
		}

		a.cancel()
		a.cancelBridge()

		if system != nil {
			if ref != nil {
				a.serviceKey.Unregister(system, ref)
			}

			if err := system.Shutdown(); err != nil {
				log.Debugf("RescanActor: actor system shutdown "+
					"failed: %v", err)
			}
		}

		a.fsm.Stop()

		a.mu.Lock()
		a.actorRef = nil
		a.system = nil
		a.mu.Unlock()
	})
}

// WaitForShutdown blocks until the bridge and retry goroutines have stopped.
func (a *RescanActor) WaitForShutdown() {
	a.asyncWG.Wait()
}

// AddWatchAddrs sends an AddWatchAddrs mailbox request so watch-list changes
// are serialized with all other actor work and any FSM failure is surfaced to
// the caller.
func (a *RescanActor) AddWatchAddrs(addrs []btcutil.Address,
	inputs []neutrino.InputWithScript, rewindTo *uint32) error {

	ref, err := a.requireActorRef()
	if err != nil {
		return err
	}

	req := newAddWatchAddrsMsg(addrs, inputs, rewindTo)

	reqCtx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	result := ref.Ask(reqCtx, req).Await(reqCtx)
	if result.IsErr() {
		return result.Err()
	}

	return nil
}

// CurrentState returns the current FSM state after all prior mailbox messages
// have been processed.
func (a *RescanActor) CurrentState() (RescanState, error) {
	ref, err := a.requireActorRef()
	if err != nil {
		return nil, err
	}

	queryCtx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	result := ref.Ask(queryCtx, &currentStateReq{}).Await(queryCtx)
	if result.IsErr() {
		return nil, result.Err()
	}

	resp, ok := result.UnwrapOr(nil).(*currentStateResp)
	if !ok || resp == nil {
		return nil, fmt.Errorf("unexpected current state response")
	}

	return resp.State, nil
}

func (a *RescanActor) runtimeSnapshot() (
	lndactor.ActorRef[RescanActorMsg, RescanActorResp],
	*lndactor.ActorSystem, error) {

	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.actorRef, a.system, a.startErr
}

func (a *RescanActor) requireActorRef() (
	lndactor.ActorRef[RescanActorMsg, RescanActorResp], error) {

	ref, _, startErr := a.runtimeSnapshot()
	if startErr != nil {
		return nil, startErr
	}
	if ref == nil {
		return nil, fmt.Errorf("rescan actor not started")
	}

	return ref, nil
}

// Receive processes mailbox messages one at a time in the lnd actor runtime.
func (a *RescanActor) Receive(actorCtx context.Context,
	msg RescanActorMsg) fn.Result[RescanActorResp] {

	switch m := msg.(type) {
	case *processNextBlockMsg:
		return a.handleRetriableEvent(
			actorCtx, m, ProcessNextBlockEvent{},
		)

	case *blockConnectedMsg:
		return a.handleRetriableEvent(
			actorCtx, m, BlockConnectedEvent{
				Header: m.Header,
				Height: m.Height,
			},
		)

	case *blockDisconnectedMsg:
		return a.handleRetriableEvent(
			actorCtx, m, BlockDisconnectedEvent{
				Header:   m.Header,
				Height:   m.Height,
				ChainTip: m.ChainTip,
			},
		)

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
		state, err := a.fsm.CurrentState()
		if err != nil {
			return fn.Err[RescanActorResp](err)
		}

		return fn.Ok[RescanActorResp](&currentStateResp{
			State: state,
		})

	case *subscriptionClosedMsg:
		return a.handleSubscriptionClosed()

	case *stopRescanMsg:
		return a.handleStop(actorCtx)

	default:
		return fn.Err[RescanActorResp](fmt.Errorf(
			"unknown message type: %T", msg,
		))
	}
}

func (a *RescanActor) handleRetriableEvent(actorCtx context.Context,
	msg RescanActorMsg, event RescanEvent) fn.Result[RescanActorResp] {

	result := a.handleFSMEvent(actorCtx, event)
	if result.IsErr() {
		err := result.Err()

		log.Warnf("RescanActor: error processing %s: %v, "+
			"retrying in %v", msg.MessageType(), err,
			filterRetryInterval)

		if a.cfg.OnError != nil {
			a.cfg.OnError(err)
		}

		a.scheduleRetry(msg)
	}

	return result
}

func (a *RescanActor) handleFSMEvent(actorCtx context.Context,
	event RescanEvent) fn.Result[RescanActorResp] {

	outbox, err := a.fsm.AskEvent(actorCtx, event)
	if err != nil {
		return fn.Err[RescanActorResp](err)
	}

	a.dispatchOutbox(actorCtx, outbox)

	return fn.Ok[RescanActorResp](&rescanActorAck{})
}

func (a *RescanActor) scheduleRetry(msg RescanActorMsg) {
	retryMsg, err := cloneRetryMsg(msg)
	if err != nil {
		log.Errorf("RescanActor: unable to clone retry message %T: %v",
			msg, err)

		return
	}

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

		ref, err := a.requireActorRef()
		if err != nil {
			return
		}

		ref.Tell(context.Background(), retryMsg)
	}()
}

func (a *RescanActor) handleStop(
	actorCtx context.Context) fn.Result[RescanActorResp] {

	if a.fsm.IsRunning() {
		outbox, err := a.fsm.AskEvent(actorCtx, StopEvent{})
		if err != nil {
			return fn.Err[RescanActorResp](err)
		}

		a.dispatchOutbox(actorCtx, outbox)
	}

	a.cancelBridge()

	return fn.Ok[RescanActorResp](&rescanActorAck{})
}

func (a *RescanActor) handleSubscriptionClosed() fn.Result[RescanActorResp] {
	select {
	case <-a.ctx.Done():
		return fn.Ok[RescanActorResp](&rescanActorAck{})
	default:
	}

	state, err := a.fsm.CurrentState()
	if err != nil {
		return fn.Err[RescanActorResp](err)
	}

	cur, ok := state.(*StateCurrent)
	if !ok {
		return fn.Ok[RescanActorResp](&rescanActorAck{})
	}

	log.Warnf("RescanActor: restarting closed subscription from height %d",
		cur.CurStamp.Height)

	if a.cfg.OnError != nil {
		a.cfg.OnError(errSubscriptionClosed)
	}

	a.startBridge(uint32(cur.CurStamp.Height))

	return fn.Ok[RescanActorResp](&rescanActorAck{})
}

// dispatchOutbox processes outbox events emitted by FSM state transitions.
func (a *RescanActor) dispatchOutbox(actorCtx context.Context,
	outbox []OutboxEvent) {

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
					a.cfg.Callbacks.OnTxConfirmed(*tx.Hash())
				}
			}

		case BlockConnectedOutbox:
			if a.cfg.Callbacks.OnBlockConnected != nil {
				header := e.Header
				if header == nil {
					var err error
					header, err = a.cfg.Chain.GetBlockHeaderByHeight(
						uint32(e.Height),
					)
					if err != nil {
						log.Errorf("Failed to get header for "+
							"height %d: %v", e.Height, err)
						continue
					}
				}

				a.cfg.Callbacks.OnBlockConnected(
					e.Height, header,
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

		case StartSubscriptionOutbox:
			a.startBridge(e.BestHeight)

		case CancelSubscriptionOutbox:
			a.cancelBridge()

		case SelfTellOutbox:
			selfMsg, err := actorMsgFromFSMEvent(e.Event)
			if err != nil {
				log.Errorf("RescanActor: unable to map self-tell "+
					"event %T: %v", e.Event, err)

				if a.cfg.OnError != nil {
					a.cfg.OnError(err)
				}

				continue
			}

			ref, err := a.requireActorRef()
			if err != nil {
				continue
			}

			ref.Tell(actorCtx, selfMsg)
		}
	}
}

// startBridge starts the subscription bridge goroutine that converts block
// subscription notifications into mailbox messages.
func (a *RescanActor) startBridge(bestHeight uint32) {
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

	bridgeCtx, bridgeCancel := context.WithCancel(a.ctx)
	sub, err := a.cfg.Chain.Subscribe(bestHeight)
	if err != nil {
		bridgeCancel()

		log.Errorf("RescanActor: failed to subscribe at height %d: %v",
			bestHeight, err)

		return
	}

	a.bridgeCancel = bridgeCancel
	a.sub = sub

	a.asyncWG.Add(1)
	go func() {
		defer a.asyncWG.Done()
		a.bridgeSubscription(bridgeCtx, sub)
	}()
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

				log.Info("RescanActor: subscription channel closed")

				ref, err := a.requireActorRef()
				if err == nil {
					ref.Tell(
						context.Background(),
						&subscriptionClosedMsg{},
					)
				}

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

			ref, err := a.requireActorRef()
			if err != nil {
				return
			}

			ref.Tell(context.Background(), msg)
		}
	}
}

// logErrorReporter logs FSM errors.
type logErrorReporter struct{}

func (r *logErrorReporter) ReportError(err error) {
	log.Errorf("RescanActor FSM error: %v", err)
}

// Ensure the mailbox behavior implements the lnd actor behavior interface.
var _ lndactor.ActorBehavior[RescanActorMsg, RescanActorResp] = (*RescanActor)(nil)

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
var _ OutboxEvent = (*StartSubscriptionOutbox)(nil)
var _ OutboxEvent = (*CancelSubscriptionOutbox)(nil)
var _ OutboxEvent = (*SelfTellOutbox)(nil)

// Ensure all events implement the RescanEvent interface at compile time.
var _ RescanEvent = (*BlockConnectedEvent)(nil)
var _ RescanEvent = (*BlockDisconnectedEvent)(nil)
var _ RescanEvent = (*AddWatchAddrsEvent)(nil)
var _ RescanEvent = (*ProcessNextBlockEvent)(nil)
var _ RescanEvent = (*StopEvent)(nil)

// Ensure ErrorReporter is implemented.
var _ ErrorReporter = (*logErrorReporter)(nil)
