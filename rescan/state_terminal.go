package rescan

import "context"

// StateTerminal is the terminal state of the rescan FSM. Once entered, no
// further transitions are possible.
type StateTerminal struct{}

// ProcessEvent is a no-op for terminal state. It should never be called
// since the state machine stops processing when IsTerminal() returns true.
//
// NOTE: This is part of the RescanState interface.
func (s *StateTerminal) ProcessEvent(_ context.Context, _ RescanEvent,
	_ *Environment) (*StateTransition, error) {

	return &StateTransition{NextState: s}, nil
}

// IsTerminal returns true — this is the terminal state.
//
// NOTE: This is part of the RescanState interface.
func (s *StateTerminal) IsTerminal() bool { return true }

// String returns the state name.
//
// NOTE: This is part of the RescanState interface.
func (s *StateTerminal) String() string { return "Terminal" }
