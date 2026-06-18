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

// EmittedEvent holds events produced by a state transition. Internal events
// are fed back into the FSM immediately (drained before returning to the
// actor mailbox). Outbox events are accumulated and returned to the caller
// for external dispatch.
type EmittedEvent struct {
	// InternalEvent contains events that should be processed by the FSM
	// before returning control to the actor mailbox. These are drained
	// in a loop by the state machine driver.
	InternalEvent []RescanEvent

	// Outbox contains events that should be dispatched externally by the
	// actor wrapper (notifications to wallet, self-tells, subscription
	// management). These are accumulated across all internal event
	// processing and returned as a batch.
	Outbox []OutboxEvent
}

// ErrorReporter is used to report errors that occur during state machine
// execution.
type ErrorReporter interface {
	// ReportError reports an error that occurred during state machine
	// execution.
	ReportError(err error)
}

// StateMachineCfg is the configuration for creating a new StateMachine.
type StateMachineCfg struct {
	// ErrorReporter is used to report errors during state transitions.
	ErrorReporter ErrorReporter

	// InitialState is the starting state of the state machine.
	InitialState RescanState

	// Env is the immutable environment providing dependencies to states.
	Env *Environment
}

// StateMachine drives the rescan FSM. It accepts external events via
// SendEvent, processes them through the current state's ProcessEvent method,
// drains any internal events, and accumulates outbox events. The state
// machine runs in its own goroutine started by Start().
//
// This follows the same pattern as darepo-client's baselib/protofsm
// StateMachine, simplified for neutrino's needs (no fn/v2 dependency).
type StateMachine struct {
	cfg StateMachineCfg

	// events is the channel used to send new events to the FSM.
	events chan RescanEvent

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
		events:     make(chan RescanEvent, 1),
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

// SendEvent sends a new event to the state machine asynchronously (fire and
// forget).
func (s *StateMachine) SendEvent(event RescanEvent) {
	select {
	case s.events <- event:
	case <-s.quit:
	}
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

// applyEvents processes an event through the FSM, draining all internal
// events and accumulating outbox events. Returns the final state, outbox
// events, and any error.
func (s *StateMachine) applyEvents(currentState RescanState,
	newEvent RescanEvent) (RescanState, []OutboxEvent, error) {

	var allOutbox []OutboxEvent

	// Process events in a queue. Internal events are added to the queue
	// and processed before returning.
	queue := []RescanEvent{newEvent}

	for len(queue) > 0 {
		event := queue[0]
		queue = queue[1:]

		transition, err := currentState.ProcessEvent(
			context.TODO(), event, s.cfg.Env,
		)
		if err != nil {
			return currentState, allOutbox, err
		}

		log.Debugf("FSM: %v -> %v (event: %T)",
			currentState, transition.NextState, event)

		// Update state.
		currentState = transition.NextState

		// Collect emitted events.
		if transition.Events != nil {
			queue = append(
				queue, transition.Events.InternalEvent...,
			)
			allOutbox = append(
				allOutbox, transition.Events.Outbox...,
			)
		}

		if currentState.IsTerminal() {
			break
		}
	}

	return currentState, allOutbox, nil
}

// driveMachine is the main event loop. It processes external events, drives
// state transitions, and responds to state queries.
func (s *StateMachine) driveMachine() {
	currentState := s.cfg.InitialState

	for {
		select {
		// Fire-and-forget event.
		case newEvent := <-s.events:
			newState, _, err := s.applyEvents(
				currentState, newEvent,
			)
			if err != nil {
				s.cfg.ErrorReporter.ReportError(err)
				log.Errorf("FSM error processing event: %v",
					err)

				go s.Stop()
				return
			}

			currentState = newState

		// Synchronous event (Ask pattern) — return outbox events.
		// On error, we return the error to the caller but do NOT
		// stop the machine. The actor wrapper handles retries for
		// transient errors (filter fetch failures, etc.).
		case syncReq := <-s.syncEvents:
			newState, outbox, err := s.applyEvents(
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
			query.result <- currentState

		case <-s.quit:
			return
		}
	}
}
