package rescan

import (
	"context"

	"github.com/lightninglabs/neutrino/headerfs"
)

// StateRewinding walks backward through the chain from the current position
// to the target rewind height. For each disconnected block, it emits a
// BlockDisconnectedOutbox notification. Once the target is reached, it
// transitions to StateSyncing to re-scan forward with the updated watch list.
type StateRewinding struct {
	// Cursor is the position, walking backwards.
	Cursor

	// TargetHeight is the height to rewind to.
	TargetHeight uint32
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
		}, undone, ContinueOutbox{}), nil

	case StopEvent:
		return transition(&StateTerminal{}), nil

	default:
		log.Warnf("StateRewinding: ignoring unexpected event %T",
			event)

		return transition(s), nil
	}
}

// processRewind walks back one batch of blocks toward the rewind target.
func (s *StateRewinding) processRewind(
	env *Environment) (*StateTransition, error) {

	cur := s.Cursor
	var outbox []OutboxEvent

	for i := 0; i < env.batchSize(); i++ {
		// Check if we've reached the target.
		if cur.CurStamp.Height <= int32(s.TargetHeight) {
			// Transition to syncing from this point.
			cur.Scanning = true
			return transition(
				&StateSyncing{Cursor: cur},
				append(outbox, ContinueOutbox{})...,
			), nil
		}

		// Emit disconnection notification for the current block.
		headerCopy := cur.CurHeader
		outbox = append(outbox, BlockDisconnectedOutbox{
			Hash:   cur.CurStamp.Hash,
			Height: cur.CurStamp.Height,
			Header: &headerCopy,
		})

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
	}

	// Batch complete but not yet at target. Continue through the mailbox.
	return transition(&StateRewinding{
		Cursor:       cur,
		TargetHeight: s.TargetHeight,
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

	targetHeight := s.TargetHeight
	if rewind != nil && *rewind < targetHeight {
		targetHeight = *rewind
	}

	return transition(&StateRewinding{
		Cursor:       cursor,
		TargetHeight: targetHeight,
	}), nil
}

// IsTerminal returns false since Rewinding is not terminal.
//
// NOTE: This is part of the RescanState interface.
func (s *StateRewinding) IsTerminal() bool { return false }

// String returns the state name.
//
// NOTE: This is part of the RescanState interface.
func (s *StateRewinding) String() string { return "Rewinding" }
