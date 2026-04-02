package rescan

import (
	"context"
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
)

const (
	// filterRetryInterval is how long to wait before retrying a failed
	// filter fetch.
	filterRetryInterval = 3 * time.Second
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

// RescanActor wraps a rescan FSM StateMachine with subscription bridge
// management and outbox event dispatching. It provides the public API for
// interacting with the rescan pipeline.
type RescanActor struct {
	fsm *StateMachine
	cfg ActorConfig

	// Bridge goroutine management.
	bridgeCtx    context.Context
	bridgeCancel context.CancelFunc
	bridgeMu     sync.Mutex
	sub          *blockntfns.Subscription

	// Lifecycle.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
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
		fsm:    fsm,
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins the rescan actor. It starts the FSM and sends the initial
// ProcessNextBlockEvent to kick off scanning.
func (a *RescanActor) Start() {
	a.fsm.Start()

	// Start the outbox dispatcher goroutine.
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.dispatchLoop()
	}()
}

// dispatchLoop continuously sends events to the FSM using the Ask pattern
// and dispatches the resulting outbox events. The self-Tell pattern causes
// ProcessNextBlockEvents to arrive back through this loop.
func (a *RescanActor) dispatchLoop() {
	// Kick off the initial event.
	a.sendAndDispatch(ProcessNextBlockEvent{})
}

// sendAndDispatch sends an event to the FSM via AskEvent and dispatches all
// resulting outbox events. Self-Tell outbox events are re-sent through this
// same path, creating a processing loop.
//
// If the FSM returns a transient error (e.g., filter fetch failure), the
// event is retried after filterRetryInterval. This matches the old
// blockRetryQueue behavior. The retry is bounded by context cancellation
// (actor shutdown).
func (a *RescanActor) sendAndDispatch(event RescanEvent) {
	outbox, err := a.fsm.AskEvent(a.ctx, event)
	if err != nil {
		log.Warnf("RescanActor: error processing event %T: %v, "+
			"retrying in %v", event, err, filterRetryInterval)

		// Notify the caller if configured.
		if a.cfg.OnError != nil {
			a.cfg.OnError(err)
		}

		// Schedule a retry after a delay. The retry is bounded
		// by the actor's context — when Stop() is called, the
		// context is canceled and no more retries happen.
		time.AfterFunc(filterRetryInterval, func() {
			select {
			case <-a.ctx.Done():
				return
			default:
				a.sendAndDispatch(event)
			}
		})

		return
	}

	a.dispatchOutbox(outbox)
}

// dispatchOutbox processes outbox events emitted by FSM state transitions.
func (a *RescanActor) dispatchOutbox(outbox []OutboxEvent) {
	for _, event := range outbox {
		// Check for shutdown between dispatching events.
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

			// Mark relevant transactions as confirmed on the
			// broadcaster to prevent rebroadcasting.
			if a.cfg.Callbacks.OnTxConfirmed != nil {
				for _, tx := range e.RelevantTxs {
					a.cfg.Callbacks.OnTxConfirmed(
						*tx.Hash(),
					)
				}
			}

		case BlockConnectedOutbox:
			if a.cfg.Callbacks.OnBlockConnected != nil {
				header, err := a.cfg.Chain.GetBlockHeaderByHeight(
					uint32(e.Height),
				)
				if err != nil {
					log.Errorf("Failed to get header for "+
						"height %d: %v", e.Height, err)
					continue
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
			// Re-send the enclosed event through the FSM. This is
			// the self-Tell pattern: instead of using an internal
			// event (which would be drained immediately by
			// applyEvents), we route through the actor's dispatch
			// loop so the FSM's event channel can interleave
			// AddWatchAddrs from external callers.
			a.sendAndDispatch(e.Event)
		}
	}
}

// startBridge starts the subscription bridge goroutine that converts block
// subscription notifications into FSM events.
func (a *RescanActor) startBridge(bestHeight uint32) {
	a.bridgeMu.Lock()
	defer a.bridgeMu.Unlock()

	// Cancel any existing bridge.
	if a.bridgeCancel != nil {
		a.bridgeCancel()
	}

	bridgeCtx, bridgeCancel := context.WithCancel(a.ctx)
	a.bridgeCtx = bridgeCtx
	a.bridgeCancel = bridgeCancel

	sub, err := a.cfg.Chain.Subscribe(bestHeight)
	if err != nil {
		log.Errorf("RescanActor: failed to subscribe at height %d: %v",
			bestHeight, err)
		return
	}
	a.sub = sub

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
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
// notifications into FSM events sent through the actor's dispatch path.
func (a *RescanActor) bridgeSubscription(ctx context.Context,
	sub *blockntfns.Subscription) {

	for {
		select {
		case <-ctx.Done():
			return

		case ntfn, ok := <-sub.Notifications:
			if !ok {
				log.Info("RescanActor: subscription channel " +
					"closed")
				return
			}

			var event RescanEvent

			switch n := ntfn.(type) {
			case *blockntfns.Connected:
				event = BlockConnectedEvent{
					Header: n.Header(),
					Height: n.Height(),
				}

			case *blockntfns.Disconnected:
				event = BlockDisconnectedEvent{
					Header:   n.Header(),
					Height:   n.Height(),
					ChainTip: n.ChainTip(),
				}

			default:
				log.Warnf("RescanActor: unknown notification "+
					"type %T", ntfn)
				continue
			}

			// Send through AskEvent so outbox events are
			// dispatched.
			a.sendAndDispatch(event)
		}
	}
}

// Stop signals the rescan actor to shut down. It sends a StopEvent to the
// FSM and cancels all goroutines.
func (a *RescanActor) Stop() {
	// Send stop event to FSM first (best-effort).
	a.fsm.SendEvent(StopEvent{})

	// Cancel all goroutines.
	a.cancel()

	// Stop the FSM.
	a.fsm.Stop()
}

// WaitForShutdown blocks until all goroutines have finished.
func (a *RescanActor) WaitForShutdown() {
	a.wg.Wait()
}

// AddWatchAddrs sends an AddWatchAddrsEvent to the FSM to add new addresses
// and/or inputs to the watch list. If rewindTo is non-nil, the FSM will
// rewind to that height and re-scan.
func (a *RescanActor) AddWatchAddrs(addrs []btcutil.Address,
	inputs []neutrino.InputWithScript, rewindTo *uint32) error {

	event := AddWatchAddrsEvent{
		Addrs:    addrs,
		Inputs:   inputs,
		RewindTo: rewindTo,
	}

	// Use sendAndDispatch (not SendEvent) so that outbox events from
	// the transition are dispatched. This is critical for rewinds
	// which emit CancelSubscriptionOutbox and SelfTellOutbox.
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.sendAndDispatch(event)
	}()

	return nil
}

// CurrentState returns the current state of the FSM.
func (a *RescanActor) CurrentState() (RescanState, error) {
	return a.fsm.CurrentState()
}

// logErrorReporter logs FSM errors.
type logErrorReporter struct{}

func (r *logErrorReporter) ReportError(err error) {
	log.Errorf("RescanActor FSM error: %v", err)
}


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

// Unused import guard.
var _ = fmt.Errorf
