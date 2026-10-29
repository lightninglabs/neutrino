package rescan

import "context"

// StateTerminal is the terminal state of the rescan FSM. Every other state
// moves here on StopEvent, which the actor sends when Stop runs and when the
// block subscription fails. Once entered, no further transitions are
// possible.
type StateTerminal struct{}

// ProcessEvent leaves the state unchanged. It is still called for messages
// that were already in the mailbox, such as a retry or a notification that
// arrived just before the stop.
//
// NOTE: This is part of the RescanState interface.
func (s *StateTerminal) ProcessEvent(_ context.Context, _ RescanEvent,
	_ *Environment) (*StateTransition, error) {

	return transition(s), nil
}

// IsTerminal returns true, since this is the terminal state.
//
// NOTE: This is part of the RescanState interface.
func (s *StateTerminal) IsTerminal() bool { return true }

// String returns the state name.
//
// NOTE: This is part of the RescanState interface.
func (s *StateTerminal) String() string { return "Terminal" }
