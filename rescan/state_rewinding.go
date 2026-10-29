package rescan

import (
	"context"

	"github.com/lightninglabs/neutrino/headerfs"
)

// StateRewinding walks backward through the chain from the cursor to the
// target height, one block per ProcessNextBlockEvent, and emits a
// BlockDisconnectedOutbox for each block above the target. The target block
// itself is kept.
//
// It is entered from Initializing, Syncing or Current on an update that
// rewinds below the cursor. A later update can lower the target. Once the
// cursor reaches the target, it leaves to Syncing, which scans the undone
// blocks again with the cursor's Scanning flag as it was, as the legacy
// rescan does. It leaves to Terminal on StopEvent.
type StateRewinding struct {
	// Cursor is the position, walking backwards.
	Cursor

	// TargetHeight is the height to rewind to, the lowest target of plan.
	TargetHeight uint32

	// plan holds each update's rewind, so a block is undone silently
	// exactly when the update whose rewind covers it asked for that. A
	// nil plan reports every disconnect.
	plan rewindPlan
}

// newRewinding returns a Rewinding state that walks cursor back through plan.
func newRewinding(cursor Cursor, plan rewindPlan) *StateRewinding {
	return &StateRewinding{
		Cursor:       cursor,
		TargetHeight: plan.target(),
		plan:         plan,
	}
}

// ProcessEvent handles events in the Rewinding state.
//
// NOTE: This is part of the RescanState interface.
func (s *StateRewinding) ProcessEvent(
	_ context.Context,
	event RescanEvent,
	env *Environment,
) (*StateTransition, error) {

	switch e := event.(type) {
	case ProcessNextBlockEvent:
		return s.processRewind(env)

	case AddWatchAddrsEvent:
		return s.handleAddWatch(&e)

	case BlockDisconnectedEvent:
		cursor, undone, ok := s.disconnect(&e)
		if !ok {
			return transition(s), nil
		}

		return transition(&StateRewinding{
			Cursor:       cursor,
			TargetHeight: s.TargetHeight,
			plan:         s.plan,
		}, undone, ContinueOutbox{}), nil

	case StopEvent:
		return transition(&StateTerminal{}), nil

	default:
		log.Warnf("StateRewinding: ignoring unexpected event %T",
			event)

		return transition(s), nil
	}
}

// processRewind undoes the cursor block, or moves to Syncing once the cursor
// has reached the target. A failed step leaves the state unchanged.
func (s *StateRewinding) processRewind(
	env *Environment) (*StateTransition, error) {

	cur := s.Cursor

	// Check if we've reached the target.
	if cur.CurStamp.Height <= int32(s.TargetHeight) {
		// Transition to syncing from this point.
		return transition(
			&StateSyncing{Cursor: cur}, ContinueOutbox{},
		), nil
	}

	// Emit disconnection notification for the current block, unless the
	// update that asked for this part of the rewind disabled it.
	var outbox []OutboxEvent
	headerCopy := cur.CurHeader
	if !s.plan.silentAt(cur.CurStamp.Height) {
		outbox = append(outbox, BlockDisconnectedOutbox{
			Hash:   cur.CurStamp.Hash,
			Height: cur.CurStamp.Height,
			Header: &headerCopy,
			Rewind: true,
		})
	}

	// Walk back to the previous block.
	header, height, err := env.Chain.GetBlockHeader(
		&cur.CurHeader.PrevBlock,
	)
	if err != nil {
		return nil, err
	}

	cur.CurHeader = *header
	cur.CurStamp = headerfs.BlockStamp{
		Hash:   header.BlockHash(),
		Height: int32(height),
	}

	return transition(&StateRewinding{
		Cursor:       cur,
		TargetHeight: s.TargetHeight,
		plan:         s.plan,
	}, append(outbox, ContinueOutbox{})...), nil
}

// handleAddWatch handles an AddWatchAddrsEvent during rewinding. If the
// new rewind target is lower, update the target.
func (s *StateRewinding) handleAddWatch(
	event *AddWatchAddrsEvent) (*StateTransition, error) {

	cursor, rewind, err := s.addWatch(event)
	if err != nil {
		return nil, err
	}

	plan := s.plan
	if plan == nil {
		plan = rewindPlan{{Target: s.TargetHeight}}
	}
	if rewind != nil {
		plan = plan.lower(*rewind)
	}

	return transition(newRewinding(cursor, plan)), nil
}

// IsTerminal returns false since Rewinding is not terminal.
//
// NOTE: This is part of the RescanState interface.
func (s *StateRewinding) IsTerminal() bool { return false }

// String returns the state name.
//
// NOTE: This is part of the RescanState interface.
func (s *StateRewinding) String() string { return "Rewinding" }
