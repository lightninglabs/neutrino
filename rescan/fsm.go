package rescan

import (
	"github.com/lightninglabs/neutrino/protofsm"
	fn "github.com/lightningnetwork/lnd/fn/v2"
)

// RescanState is a single state of the rescan FSM. Each state processes an
// event against the immutable Environment and returns the next state. States
// are values: a transition builds a new state struct rather than mutating the
// current one.
type RescanState = protofsm.State[RescanEvent, OutboxEvent, *Environment]

// StateTransition is the result of processing one event: the next state and
// the outbox events the actor dispatches once the transition commits.
type StateTransition = protofsm.StateTransition[
	RescanEvent, OutboxEvent, *Environment,
]

// stateMachine is the protofsm machine that holds the current rescan state.
// The actor drives it synchronously from its Receive turn.
type stateMachine = protofsm.StateMachine[
	RescanEvent, OutboxEvent, *Environment,
]

// newStateMachine creates the rescan state machine in its initial state.
func newStateMachine(initial RescanState, env *Environment) *stateMachine {
	machine, err := protofsm.NewStateMachine(protofsm.StateMachineCfg[
		RescanEvent, OutboxEvent, *Environment,
	]{
		InitialState: initial,
		Env:          env,
	})
	if err != nil {
		// NewStateMachine fails only for a nil initial state, which
		// NewRescanActor never passes.
		panic(err)
	}

	return machine
}

// transition returns a transition to next that emits the given outbox
// events.
func transition(next RescanState, outbox ...OutboxEvent) *StateTransition {
	t := &StateTransition{NextState: next}
	if len(outbox) > 0 {
		t.NewEvents = fn.Some(protofsm.EmittedEvent[
			RescanEvent, OutboxEvent,
		]{
			Outbox: outbox,
		})
	}

	return t
}

// outboxOf returns the outbox events a transition emits.
func outboxOf(t *StateTransition) []OutboxEvent {
	var outbox []OutboxEvent
	t.NewEvents.WhenSome(func(e protofsm.EmittedEvent[
		RescanEvent, OutboxEvent]) {

		outbox = e.Outbox
	})

	return outbox
}

// Ensure all states implement the RescanState interface at compile time.
var _ RescanState = (*StateInitializing)(nil)
var _ RescanState = (*StateSyncing)(nil)
var _ RescanState = (*StateCurrent)(nil)
var _ RescanState = (*StateRewinding)(nil)
var _ RescanState = (*StateTerminal)(nil)
