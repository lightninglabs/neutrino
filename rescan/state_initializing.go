package rescan

import (
	"context"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/headerfs"
)

// StateInitializing is the initial state of the rescan FSM. It waits for a
// ProcessNextBlockEvent to begin syncing. Any AddWatchAddrsEvents received
// in this state are buffered into the watch state that will be carried
// forward when transitioning to StateSyncing.
type StateInitializing struct {
	// StartStamp is the block stamp to begin scanning from.
	StartStamp headerfs.BlockStamp

	// StartHeader is the header at StartStamp.
	StartHeader wire.BlockHeader

	// Watch holds the current watch list state.
	Watch WatchState
}

// ProcessEvent handles events in the Initializing state.
//
// NOTE: This is part of the RescanState interface.
func (s *StateInitializing) ProcessEvent(_ context.Context, event RescanEvent,
	_ *Environment) (*StateTransition, error) {

	switch e := event.(type) {
	case ProcessNextBlockEvent:
		// Transition to syncing and kick off the first batch.
		return &StateTransition{
			NextState: &StateSyncing{
				CurStamp:  s.StartStamp,
				CurHeader: s.StartHeader,
				Watch:     s.Watch,
				Scanning:  false,
			},
			Events: &EmittedEvent{
				Outbox: []OutboxEvent{
					SelfTellOutbox{
						Event: ProcessNextBlockEvent{},
					},
				},
			},
		}, nil

	case AddWatchAddrsEvent:
		// Buffer addresses into the watch state.
		newWatch, err := s.Watch.ApplyUpdate(&e)
		if err != nil {
			return nil, err
		}

		return &StateTransition{
			NextState: &StateInitializing{
				StartStamp:  s.StartStamp,
				StartHeader: s.StartHeader,
				Watch:       newWatch,
			},
		}, nil

	case StopEvent:
		return &StateTransition{
			NextState: &StateTerminal{},
		}, nil

	default:
		log.Warnf("StateInitializing: ignoring unexpected event %T",
			event)

		return &StateTransition{NextState: s}, nil
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
