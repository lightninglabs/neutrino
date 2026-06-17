package protofsm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/lightningnetwork/lnd/fn/v2"
)

var (
	// ErrStateMachineShutdown occurs when trying to feed an event to a
	// StateMachine that has been asked to stop.
	ErrStateMachineShutdown = errors.New("state machine is shutting down")

	// ErrNilInitialState occurs when a StateMachine is configured without an
	// initial state.
	ErrNilInitialState = errors.New("initial state must not be nil")
)

// EmittedEvent is a special type that can be emitted by a state transition.
// Internal events are routed back into the same FSM. Outbox events are returned
// to the caller so the actor that owns the FSM can dispatch them to other
// actors, persistence sinks, trace recorders, or test harnesses.
type EmittedEvent[Event any, Outbox any] struct {
	// InternalEvent is an optional set of internal events that should be
	// routed back into the state machine. This lets one external input
	// drive several deterministic state transitions.
	InternalEvent []Event

	// Outbox is an optional set of commands emitted by the transition. The
	// StateMachine never dispatches these directly; its owning actor does.
	Outbox []Outbox
}

// StateTransition denotes the next state and the set of events emitted while
// processing an input event.
type StateTransition[Event any, Outbox any, Env any] struct {
	// NextState is the next state to transition to.
	NextState State[Event, Outbox, Env]

	// NewEvents is the set of events to emit.
	NewEvents fn.Option[EmittedEvent[Event, Outbox]]
}

// State defines a state transition function that takes an event and an
// environment and returns the next state plus any emitted events.
type State[Event any, Outbox any, Env any] interface {
	// ProcessEvent takes an event and an environment, then returns a state
	// transition. The state machine will keep processing emitted internal
	// events until it reaches a terminal state or runs out of work.
	ProcessEvent(ctx context.Context, event Event, env Env) (
		*StateTransition[Event, Outbox, Env], error)

	// IsTerminal returns true if this state is terminal.
	IsTerminal() bool

	// String returns a human-readable representation of the state.
	String() string
}

// ErrorReporter is used to report errors that occur during state machine
// execution.
type ErrorReporter interface {
	// ReportError reports an error that occurred during state execution.
	ReportError(err error)
}

// ErrorReporterFunc adapts a function into an ErrorReporter.
type ErrorReporterFunc func(error)

// ReportError implements ErrorReporter.
func (e ErrorReporterFunc) ReportError(err error) {
	e(err)
}

type noopErrorReporter struct{}

func (noopErrorReporter) ReportError(error) {}

// StateMachineCfg configures a StateMachine.
type StateMachineCfg[Event any, Outbox any, Env any] struct {
	// ErrorReporter is used to report errors that occur during state
	// transitions. If nil, errors are ignored after stopping the machine.
	ErrorReporter ErrorReporter

	// InitialState is the initial state of the state machine.
	InitialState State[Event, Outbox, Env]

	// Env is the environment that the state machine will use to execute.
	Env Env
}

// StateMachine is an abstract FSM that processes incoming events and drives
// state transitions until it reaches a terminal state or is stopped. The
// machine owns no actor-system dependencies; its caller is responsible for
// dispatching returned outbox messages.
type StateMachine[Event any, Outbox any, Env any] struct {
	cfg StateMachineCfg[Event, Outbox, Env]

	// newStateEvents notifies subscribers of state transitions.
	newStateEvents *fn.EventDistributor[State[Event, Outbox, Env]]

	currentState State[Event, Outbox, Env]

	startOnce sync.Once
	stopOnce  sync.Once

	mu      sync.Mutex
	running atomic.Bool
}

// NewStateMachine creates a new state machine.
func NewStateMachine[Event any, Outbox any, Env any](
	cfg StateMachineCfg[Event, Outbox, Env]) (*StateMachine[
	Event, Outbox, Env], error) {

	if cfg.InitialState == nil {
		return nil, ErrNilInitialState
	}

	if cfg.ErrorReporter == nil {
		cfg.ErrorReporter = noopErrorReporter{}
	}

	return &StateMachine[Event, Outbox, Env]{
		cfg:            cfg,
		currentState:   cfg.InitialState,
		newStateEvents: fn.NewEventDistributor[State[Event, Outbox, Env]](),
	}, nil
}

// Start marks the state machine as live and emits the initial state to
// subscribers. The owning actor still drives the machine synchronously through
// ProcessEvent.
func (s *StateMachine[Event, Outbox, Env]) Start(context.Context) {
	s.startOnce.Do(func() {
		s.running.Store(true)
		s.newStateEvents.NotifySubscribers(s.currentState)
	})
}

// Stop marks the state machine as stopped.
func (s *StateMachine[Event, Outbox, Env]) Stop() {
	s.stopOnce.Do(func() {
		s.running.Store(false)
	})
}

// ProcessEvent applies an external event and returns all outbox messages
// emitted while processing the event and any internal follow-up events.
func (s *StateMachine[Event, Outbox, Env]) ProcessEvent(ctx context.Context,
	event Event) ([]Outbox, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running.Load() {
		return nil, ErrStateMachineShutdown
	}

	newState, outbox, err := s.applyEvents(ctx, s.currentState, event)
	if err != nil {
		s.cfg.ErrorReporter.ReportError(err)

		return nil, err
	}

	s.currentState = newState

	return outbox, nil
}

// CurrentState returns the current state of the state machine.
func (s *StateMachine[Event, Outbox, Env]) CurrentState() State[
	Event, Outbox, Env] {

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.currentState
}

// StateSubscriber represents an active state transition subscription.
type StateSubscriber[Event any, Outbox any, Env any] *fn.EventReceiver[State[Event, Outbox, Env]]

// RegisterStateEvents registers a new event listener that will be notified of
// new state transitions.
func (s *StateMachine[Event, Outbox, Env]) RegisterStateEvents() StateSubscriber[
	Event, Outbox, Env] {

	subscriber := fn.NewEventReceiver[State[Event, Outbox, Env]](10)
	s.newStateEvents.RegisterSubscriber(subscriber)

	return subscriber
}

// RemoveStateSub removes the target state subscriber.
func (s *StateMachine[Event, Outbox, Env]) RemoveStateSub(sub StateSubscriber[
	Event, Outbox, Env]) {

	_ = s.newStateEvents.RemoveSubscriber(sub)
}

// IsRunning returns true if the state machine is currently running.
func (s *StateMachine[Event, Outbox, Env]) IsRunning() bool {
	return s.running.Load()
}

func (s *StateMachine[Event, Outbox, Env]) applyEvents(ctx context.Context,
	currentState State[Event, Outbox, Env], newEvent Event) (
	State[Event, Outbox, Env], []Outbox, error) {

	eventQueue := fn.NewQueue(newEvent)
	var outbox []Outbox

	for nextEvent := eventQueue.Dequeue(); nextEvent.IsSome(); nextEvent = eventQueue.Dequeue() {
		err := fn.MapOptionZ(nextEvent, func(event Event) error {
			transition, err := currentState.ProcessEvent(
				ctx, event, s.cfg.Env,
			)
			if err != nil {
				return err
			}

			if transition == nil || transition.NextState == nil {
				return fmt.Errorf("state %v returned nil transition",
					currentState)
			}

			err = fn.MapOptionZ(
				transition.NewEvents,
				func(events EmittedEvent[Event, Outbox]) error {
					for _, internalEvent := range events.InternalEvent {
						eventQueue.Enqueue(internalEvent)
					}

					outbox = append(outbox, events.Outbox...)

					return nil
				},
			)
			if err != nil {
				return err
			}

			currentState = transition.NextState
			s.newStateEvents.NotifySubscribers(currentState)

			return nil
		})
		if err != nil {
			return currentState, nil, err
		}

		if currentState.IsTerminal() {
			return currentState, outbox, nil
		}
	}

	return currentState, outbox, nil
}
