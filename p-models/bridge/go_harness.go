package bridge

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	neutrino "github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/rescan"
)

// ObservedEventKind enumerates the concrete events emitted by the Go actor.
type ObservedEventKind string

const (
	ObservedFilteredBlockConnected    ObservedEventKind = "filtered_block_connected"
	ObservedBlockConnected            ObservedEventKind = "block_connected"
	ObservedBlockDisconnected         ObservedEventKind = "block_disconnected"
	ObservedFilteredBlockDisconnected ObservedEventKind = "filtered_block_disconnected"
	ObservedRescanFinished            ObservedEventKind = "rescan_finished"
)

// ObservedEvent is a normalized event emitted by the real Go actor.
type ObservedEvent struct {
	Kind            ObservedEventKind
	Height          int32
	Hash            chainhash.Hash
	RelevantTxCount int
}

// EventRecorder captures the real actor callbacks so the bridge can run
// model-style invariant checks against actual code.
type EventRecorder struct {
	mu sync.Mutex

	events        []ObservedEvent
	finishedCount atomic.Int32
	onObserved    func(ObservedEvent)
}

// Callbacks returns rescan callbacks that record every emitted event.
func (r *EventRecorder) Callbacks() rescan.Callbacks {
	return rescan.Callbacks{
		OnFilteredBlockConnected: func(height int32,
			header *wire.BlockHeader, relevantTxs []*btcutil.Tx) {

			r.record(ObservedEvent{
				Kind:            ObservedFilteredBlockConnected,
				Height:          height,
				Hash:            header.BlockHash(),
				RelevantTxCount: len(relevantTxs),
			})
		},
		OnBlockConnected: func(height int32,
			header *wire.BlockHeader, _ time.Time) {

			r.record(ObservedEvent{
				Kind:   ObservedBlockConnected,
				Height: height,
				Hash:   header.BlockHash(),
			})
		},
		OnFilteredBlockDisconnected: func(height int32,
			header *wire.BlockHeader) {

			r.record(ObservedEvent{
				Kind:   ObservedFilteredBlockDisconnected,
				Height: height,
				Hash:   header.BlockHash(),
			})
		},
		OnBlockDisconnected: func(height int32, header *wire.BlockHeader) {
			r.record(ObservedEvent{
				Kind:   ObservedBlockDisconnected,
				Height: height,
				Hash:   header.BlockHash(),
			})
		},
		OnRescanFinished: func(height int32) {
			r.finishedCount.Add(1)
			r.record(ObservedEvent{
				Kind:   ObservedRescanFinished,
				Height: height,
			})
		},
	}
}

// Events returns a stable snapshot of the recorded events.
func (r *EventRecorder) Events() []ObservedEvent {
	r.mu.Lock()
	defer r.mu.Unlock()

	events := make([]ObservedEvent, len(r.events))
	copy(events, r.events)

	return events
}

// FinishedCount returns how many rescan-finished callbacks were observed.
func (r *EventRecorder) FinishedCount() int32 {
	return r.finishedCount.Load()
}

func (r *EventRecorder) record(event ObservedEvent) {
	r.mu.Lock()
	r.events = append(r.events, event)
	onObserved := r.onObserved
	r.mu.Unlock()

	if onObserved != nil {
		onObserved(event)
	}
}

// SetOnObserved installs a hook invoked after each event is recorded.
func (r *EventRecorder) SetOnObserved(fn func(ObservedEvent)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.onObserved = fn
}

// CheckFilteredBlockForEveryBlock enforces the same invariant as the P monitor:
// every block-connected callback must have a matching filtered-block-connected
// callback at the same height.
func CheckFilteredBlockForEveryBlock(events []ObservedEvent) []string {
	filteredByHeight := make(map[int32]struct{})
	var errs []string

	for _, event := range events {
		switch event.Kind {
		case ObservedFilteredBlockConnected:
			filteredByHeight[event.Height] = struct{}{}

		case ObservedBlockConnected:
			if _, ok := filteredByHeight[event.Height]; !ok {
				errs = append(errs, fmt.Sprintf(
					"block connected at height %d without prior filtered block",
					event.Height,
				))
			}
		}
	}

	return errs
}

// CheckBlockCallbackConsistency verifies that a filtered-block callback is
// always paired with a block-connected callback for the same block.
func CheckBlockCallbackConsistency(events []ObservedEvent) []string {
	var (
		errs         []string
		lastFiltered *ObservedEvent
	)

	for i := range events {
		event := events[i]

		switch event.Kind {
		case ObservedFilteredBlockConnected:
			lastFiltered = &events[i]

		case ObservedBlockConnected:
			if lastFiltered == nil {
				errs = append(errs, fmt.Sprintf(
					"block connected at height %d without a preceding filtered-block callback",
					event.Height,
				))
				continue
			}

			if lastFiltered.Height != event.Height ||
				lastFiltered.Hash != event.Hash {

				errs = append(errs, fmt.Sprintf(
					"callback mismatch: filtered block (%d, %s) followed by block connected (%d, %s)",
					lastFiltered.Height, lastFiltered.Hash,
					event.Height, event.Hash,
				))
			}

			lastFiltered = nil
		}
	}

	return errs
}

// CheckDisconnectCallbackConsistency verifies that a filtered-block-disconnect
// callback is always paired with a block-disconnect callback for the same
// block.
func CheckDisconnectCallbackConsistency(events []ObservedEvent) []string {
	var (
		errs             []string
		lastFilteredDisc *ObservedEvent
	)

	for i := range events {
		event := events[i]

		switch event.Kind {
		case ObservedFilteredBlockDisconnected:
			lastFilteredDisc = &events[i]

		case ObservedBlockDisconnected:
			if lastFilteredDisc == nil {
				errs = append(errs, fmt.Sprintf(
					"block disconnected at height %d without a preceding filtered-block-disconnect callback",
					event.Height,
				))
				continue
			}

			if lastFilteredDisc.Height != event.Height ||
				lastFilteredDisc.Hash != event.Hash {

				errs = append(errs, fmt.Sprintf(
					"disconnect callback mismatch: filtered block disconnected (%d, %s) followed by block disconnected (%d, %s)",
					lastFilteredDisc.Height, lastFilteredDisc.Hash,
					event.Height, event.Hash,
				))
			}

			lastFilteredDisc = nil
		}
	}

	return errs
}

// CheckMonotonicFilteredProgress mirrors the P MonotonicProgress monitor for
// the Go bridge: forward filtered-block callbacks must increase strictly until
// a disconnect resets the forward progress window.
func CheckMonotonicFilteredProgress(events []ObservedEvent) []string {
	var (
		errs              []string
		lastForwardHeight int32 = -1
	)

	for _, event := range events {
		switch event.Kind {
		case ObservedFilteredBlockDisconnected, ObservedBlockDisconnected:
			lastForwardHeight = -1

		case ObservedFilteredBlockConnected:
			if event.Height <= lastForwardHeight {
				errs = append(errs, fmt.Sprintf(
					"non-monotonic filtered progress: height %d after %d",
					event.Height, lastForwardHeight,
				))
			}

			lastForwardHeight = event.Height
		}
	}

	return errs
}

// CheckSubscriptionAlternation mirrors the P SubscriptionLifecycle monitor for
// the Go bridge: subscribes and cancels must strictly alternate.
func CheckSubscriptionAlternation(events []SubscriptionEvent) []string {
	var (
		errs   []string
		active bool
	)

	for _, event := range events {
		switch event.Kind {
		case SubscriptionStarted:
			if active {
				errs = append(errs, fmt.Sprintf(
					"double subscription start at height %d without cancel",
					event.Height,
				))
			}

			active = true

		case SubscriptionCanceled:
			if !active {
				errs = append(errs,
					"subscription cancel without active subscription")
			}

			active = false
		}
	}

	return errs
}

// ActorHarness runs the real Go rescan actor against the bridge MockChain.
type ActorHarness struct {
	Chain    *MockChain
	Recorder *EventRecorder
	Actor    *rescan.RescanActor
}

// ActorHarnessConfig configures a real Go actor scenario.
type ActorHarnessConfig struct {
	StartTime   time.Time
	BatchSize   int
	WatchAddrs  []btcutil.Address
	WatchInputs []neutrino.InputWithScript
	OnError     func(error)
}

// NewActorHarness creates a harness with a fresh chain of the requested size.
func NewActorHarness(numBlocks int, cfg ActorHarnessConfig) (*ActorHarness, error) {
	return NewActorHarnessWithChain(NewMockChain(numBlocks), cfg)
}

// NewActorHarnessWithChain creates a harness using the provided chain.
func NewActorHarnessWithChain(chain *MockChain,
	cfg ActorHarnessConfig) (*ActorHarness, error) {

	bestBlock, err := chain.BestBlock()
	if err != nil {
		return nil, err
	}

	genesisHash := chain.ChainParams().GenesisHash
	genesisHeader := &chain.ChainParams().GenesisBlock.Header
	startStamp := headerfs.BlockStamp{
		Hash:   *genesisHash,
		Height: 0,
	}
	if bestBlock.Height == 0 {
		startStamp.Timestamp = genesisHeader.Timestamp
	}

	recorder := &EventRecorder{}
	actor := rescan.NewRescanActor(rescan.ActorConfig{
		Chain:       chain,
		StartStamp:  startStamp,
		StartHeader: *genesisHeader,
		StartTime:   cfg.StartTime,
		WatchAddrs:  cfg.WatchAddrs,
		WatchInputs: cfg.WatchInputs,
		Callbacks:   recorder.Callbacks(),
		BatchSize:   cfg.BatchSize,
		OnError:     cfg.OnError,
	})

	return &ActorHarness{
		Chain:    chain,
		Recorder: recorder,
		Actor:    actor,
	}, nil
}

// Start begins the actor.
func (h *ActorHarness) Start() {
	h.Actor.Start()
}

// StopAndWait shuts the actor down completely.
func (h *ActorHarness) StopAndWait() {
	h.Actor.Stop()
	h.Actor.WaitForShutdown()
}

// WaitForCurrent waits until the actor reports StateCurrent.
func (h *ActorHarness) WaitForCurrent(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := h.Actor.CurrentState()
		if err == nil {
			if _, ok := state.(*rescan.StateCurrent); ok {
				return nil
			}
		}

		time.Sleep(10 * time.Millisecond)
	}

	return fmt.Errorf("actor did not reach current state within %v", timeout)
}
