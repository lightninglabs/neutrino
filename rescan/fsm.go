package rescan

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// RescanState represents a single state in the rescan FSM. Each state
// processes events and returns a state transition describing the next state
// and any emitted events. State structs carry their own mutable data
// (CurStamp, WatchList, etc.) — the environment is immutable.
type RescanState interface {
	// ProcessEvent handles an incoming event given the immutable
	// environment and returns the resulting state transition.
	ProcessEvent(ctx context.Context, event RescanEvent,
		env *Environment) (*StateTransition, error)

	// IsTerminal returns true if this is a terminal state from which no
	// further transitions are possible.
	IsTerminal() bool

	// String returns a human-readable name for this state.
	String() string
}

// StateTransition describes the result of processing an event: the next
// state and any events to emit.
type StateTransition struct {
	// NextState is the state the FSM should transition to. This is a
	// new struct with updated fields — states are never mutated.
	NextState RescanState

	// Events holds the emitted events from this transition, if any.
	Events *EmittedEvent
}

// EmittedEvent holds the outbox events produced by a state transition. The
// actor dispatches them after the transition commits.
type EmittedEvent struct {
	// Outbox contains events that should be dispatched externally by the
	// actor wrapper: wallet notifications and continuations.
	Outbox []OutboxEvent
}

// StateMachineCfg is the configuration for creating a new StateMachine.
type StateMachineCfg struct {
	// InitialState is the starting state of the state machine.
	InitialState RescanState

	// Env is the immutable environment providing dependencies to states.
	Env *Environment
}

// StateMachine drives the rescan FSM. It accepts events via AskEvent,
// processes them through the current state's ProcessEvent method, and returns
// the outbox events. The state machine runs in its own goroutine started by
// Start().
//
// This follows the same pattern as darepo-client's baselib/protofsm
// StateMachine, simplified for neutrino's needs (no fn/v2 dependency).
type StateMachine struct {
	cfg StateMachineCfg

	// syncEvents is used for synchronous event processing (Ask pattern)
	// where the caller wants the accumulated outbox events back.
	syncEvents chan syncEventRequest

	// stateQuery is used by outside callers to query the current state.
	stateQuery chan stateQueryReq

	quit chan struct{}

	startOnce sync.Once
	stopOnce  sync.Once
	running   atomic.Bool
	wg        sync.WaitGroup
}

// syncEventRequest represents a synchronous event request that returns
// accumulated outbox events via the result channel.
type syncEventRequest struct {
	event  RescanEvent
	result chan syncEventResult
}

// syncEventResult holds the result of a synchronous event request.
type syncEventResult struct {
	outbox []OutboxEvent
	err    error
}

// stateQueryReq is used by callers to query the current state.
type stateQueryReq struct {
	result chan RescanState
}

// NewStateMachine creates a new state machine with the given configuration.
func NewStateMachine(cfg StateMachineCfg) *StateMachine {
	return &StateMachine{
		cfg:        cfg,
		syncEvents: make(chan syncEventRequest, 1),
		stateQuery: make(chan stateQueryReq),
		quit:       make(chan struct{}),
	}
}

// Start spawns the goroutine that drives the state machine.
func (s *StateMachine) Start() {
	s.startOnce.Do(func() {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.driveMachine()
		}()
		s.running.Store(true)
	})
}

// Stop signals the state machine to shut down and waits for it to finish.
func (s *StateMachine) Stop() {
	s.stopOnce.Do(func() {
		close(s.quit)
		s.wg.Wait()
		s.running.Store(false)
	})
}

// AskEvent sends an event to the state machine and waits for processing to
// complete, returning the accumulated outbox events. This enables the actor
// wrapper to collect outbox events for dispatch.
func (s *StateMachine) AskEvent(ctx context.Context,
	event RescanEvent) ([]OutboxEvent, error) {

	req := syncEventRequest{
		event:  event,
		result: make(chan syncEventResult, 1),
	}

	select {
	case s.syncEvents <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.quit:
		return nil, fmt.Errorf("state machine shutting down")
	}

	select {
	case res := <-req.result:
		return res.outbox, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.quit:
		return nil, fmt.Errorf("state machine shutting down")
	}
}

// CurrentState returns the current state of the state machine.
func (s *StateMachine) CurrentState() (RescanState, error) {
	query := stateQueryReq{
		result: make(chan RescanState, 1),
	}

	select {
	case s.stateQuery <- query:
	case <-s.quit:
		return nil, fmt.Errorf("state machine shutting down")
	}

	select {
	case state := <-query.result:
		return state, nil
	case <-s.quit:
		return nil, fmt.Errorf("state machine shutting down")
	}
}

// IsRunning returns true if the state machine is currently running.
func (s *StateMachine) IsRunning() bool {
	return s.running.Load()
}

// applyEvent processes an event through the current state and returns the
// next state and its outbox events.
func (s *StateMachine) applyEvent(currentState RescanState,
	event RescanEvent) (RescanState, []OutboxEvent, error) {

	transition, err := currentState.ProcessEvent(
		context.TODO(), event, s.cfg.Env,
	)
	if err != nil {
		return currentState, nil, err
	}

	log.Debugf("FSM: %v -> %v (event: %T)",
		currentState, transition.NextState, event)

	var outbox []OutboxEvent
	if transition.Events != nil {
		outbox = transition.Events.Outbox
	}

	return transition.NextState, outbox, nil
}

// driveMachine is the main event loop. It processes external events, drives
// state transitions, and responds to state queries.
func (s *StateMachine) driveMachine() {
	currentState := s.cfg.InitialState

	for {
		select {
		// Synchronous event (Ask pattern) — return outbox events.
		// On error, we return the error to the caller but do NOT
		// stop the machine. The actor wrapper handles retries for
		// transient errors (filter fetch failures, etc.).
		case syncReq := <-s.syncEvents:
			newState, outbox, err := s.applyEvent(
				currentState, syncReq.event,
			)
			if err != nil {
				log.Warnf("FSM error processing sync "+
					"event: %v", err)

				syncReq.result <- syncEventResult{err: err}
				continue
			}

			currentState = newState
			syncReq.result <- syncEventResult{outbox: outbox}

		// State query from external caller.
		case query := <-s.stateQuery:
			query.result <- snapshotState(currentState)

		case <-s.quit:
			return
		}
	}
}

// snapshotState keeps callers from mutating the state owned by the driver.
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
