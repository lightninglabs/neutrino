package rescan

import (
	"context"
)

// StateInitializing is the initial state of the rescan FSM, before the first
// scan. The actor subscribes to block notifications before it sends the
// first ProcessNextBlockEvent, and this state exists to hold AddWatchAddrs
// updates until then: scanning without a subscription could miss a
// disconnect. It buffers each update into the watch list, and remembers the
// rewinds below the start.
//
// It also waits for the chain, as the legacy rescan did before its first
// scan: until the best block has reached the start block, and the chain is
// current or has reached the end block. Until then each
// ProcessNextBlockEvent asks to be sent again after the retry interval, with
// no error, so filter fetches are batched against a settled header chain and
// the scan does not report finished at a tip that is still moving.
//
// A disconnect of the start block moves the start back to its parent. It is
// reported when the first scan starts, as nothing is reported before then.
//
// It leaves on ProcessNextBlockEvent once the chain is ready, to Syncing, or
// to Rewinding when a buffered update asked to rewind, and on StopEvent to
// Terminal.
type StateInitializing struct {
	// Cursor is the start block, which counts as already scanned.
	Cursor

	// rewinds records the rewinds below the start that updates asked
	// for before scanning began.
	rewinds rewindPlan

	// undone holds the disconnects of start blocks that a reorg undid
	// before scanning began. Nothing is reported before the chain is
	// ready, so they're reported when the first scan starts.
	undone []OutboxEvent
}

// ProcessEvent handles events in the Initializing state.
//
// NOTE: This is part of the RescanState interface.
func (s *StateInitializing) ProcessEvent(
	_ context.Context,
	event RescanEvent,
	env *Environment,
) (*StateTransition, error) {

	switch e := event.(type) {
	case ProcessNextBlockEvent:
		ready, err := s.chainReady(env)
		if err != nil {
			return nil, err
		}
		if !ready {
			log.Debugf("StateInitializing: waiting for the chain "+
				"to reach height %d and be current",
				s.CurStamp.Height)

			return transition(s, WaitOutbox{}), nil
		}

		// Apply a rewind accepted during subscription setup.
		var next RescanState = &StateSyncing{Cursor: s.Cursor}
		if len(s.rewinds) > 0 {
			next = newRewinding(s.Cursor, s.rewinds)
		}
		outbox := append(
			append([]OutboxEvent(nil), s.undone...),
			ContinueOutbox{},
		)

		return transition(next, outbox...), nil

	case AddWatchAddrsEvent:
		// Buffer addresses into the watch state.
		cursor, rewind, err := s.addWatch(&e)
		if err != nil {
			return nil, err
		}

		rewinds := s.rewinds
		if rewind != nil {
			rewinds = rewinds.lower(*rewind)
		}

		return transition(&StateInitializing{
			Cursor:  cursor,
			rewinds: rewinds,
			undone:  s.undone,
		}), nil

	case BlockDisconnectedEvent:
		// The start block counts as scanned, so a disconnect of it
		// moves the start back, and is reported once the first scan
		// starts. Dropping it would leave that scan to start from an
		// orphan.
		cursor, undone, ok := s.disconnect(&e)
		if !ok {
			return transition(s), nil
		}

		return transition(&StateInitializing{
			Cursor:  cursor,
			rewinds: s.rewinds,
			undone: append(
				append([]OutboxEvent(nil), s.undone...),
				undone,
			),
		}), nil

	case StopEvent:
		return transition(&StateTerminal{}), nil

	default:
		log.Warnf("StateInitializing: ignoring unexpected event %T",
			event)

		return transition(s), nil
	}
}

// chainReady reports whether the chain is ready for the first scan: its best
// block has reached the start block, and it is current or has reached the end
// block.
func (s *StateInitializing) chainReady(env *Environment) (bool, error) {
	best, err := env.bestBlock()
	if err != nil {
		return false, err
	}
	if best.Height < s.CurStamp.Height {
		return false, nil
	}

	end := env.EndBlock
	return env.Chain.IsCurrent() ||
		(end.Height > 0 && best.Height >= end.Height) ||
		best.Hash == end.Hash, nil
}

// IsTerminal returns false since Initializing is not terminal.
//
// NOTE: This is part of the RescanState interface.
func (s *StateInitializing) IsTerminal() bool { return false }

// String returns the state name.
//
// NOTE: This is part of the RescanState interface.
func (s *StateInitializing) String() string { return "Initializing" }
