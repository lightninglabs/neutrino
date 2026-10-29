package rescan

import (
	"context"
)

// StateInitializing is the initial state of the rescan FSM. It waits for a
// ProcessNextBlockEvent to begin syncing. Any AddWatchAddrsEvents received
// in this state are buffered into the watch state that will be carried
// forward when transitioning to StateSyncing.
type StateInitializing struct {
	// Cursor is the start block, which counts as already scanned.
	Cursor

	// rewindTo records the earliest rewind before scanning.
	rewindTo *uint32
}

// ProcessEvent handles events in the Initializing state.
//
// NOTE: This is part of the RescanState interface.
func (s *StateInitializing) ProcessEvent(
	_ context.Context,
	event RescanEvent,
	_ *Environment,
) (*StateTransition, error) {

	switch e := event.(type) {
	case ProcessNextBlockEvent:
		// Apply a rewind accepted during subscription setup.
		var next RescanState = &StateSyncing{Cursor: s.Cursor}
		if s.rewindTo != nil {
			next = &StateRewinding{
				Cursor:       s.Cursor,
				TargetHeight: *s.rewindTo,
			}
		}
		return transition(next, ContinueOutbox{}), nil

	case AddWatchAddrsEvent:
		// Buffer addresses into the watch state.
		cursor, rewind, err := s.addWatch(&e)
		if err != nil {
			return nil, err
		}

		rewindTo := s.rewindTo
		if rewind != nil && (rewindTo == nil || *rewind < *rewindTo) {
			rewindTo = rewind
		}

		return transition(&StateInitializing{
			Cursor:   cursor,
			rewindTo: rewindTo,
		}), nil

	case StopEvent:
		return transition(&StateTerminal{}), nil

	default:
		log.Warnf("StateInitializing: ignoring unexpected event %T",
			event)

		return transition(s), nil
	}
}

// IsTerminal returns false since Initializing is not terminal.
//
// NOTE: This is part of the RescanState interface.
func (s *StateInitializing) IsTerminal() bool { return false }

// String returns the state name.
//
// NOTE: This is part of the RescanState interface.
func (s *StateInitializing) String() string { return "Initializing" }
