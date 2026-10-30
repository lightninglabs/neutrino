package rescan

import (
	"context"
	"fmt"
)

// StateSyncing is the historical catch-up state. It scans the blocks above
// its cursor from the header store, one block per ProcessNextBlockEvent, and
// asks for a continuation after each. The actor runs a batch of such steps
// per mailbox turn and ends the batch early when a request is waiting, so
// AddWatchAddrs never waits for more than one block.
//
// It is entered from Initializing, from Rewinding once the rewind reaches
// its target, and from Current when a live block cannot be scanned. It
// leaves to Current once the cursor reaches the best block, to Rewinding on
// an update that rewinds below the cursor, and to Terminal on StopEvent. A
// disconnect of the cursor block moves the cursor back and stays. Connect
// notifications are ignored: the header store already holds a block when its
// notification arrives, so a later step scans it.
type StateSyncing struct {
	// Cursor is the last scanned block.
	Cursor

	// finished records that RescanFinished was already reported for this
	// scan, so reaching the tip again does not report it twice. It is set
	// when Current falls back to Syncing, and clear after a rewind.
	finished bool
}

// ProcessEvent handles events in the Syncing state. On ProcessNextBlockEvent,
// it scans the next block and emits a ContinueOutbox to continue.
//
// NOTE: This is part of the RescanState interface.
func (s *StateSyncing) ProcessEvent(
	ctx context.Context,
	event RescanEvent,
	env *Environment,
) (*StateTransition, error) {

	switch e := event.(type) {
	case ProcessNextBlockEvent:
		return s.processNextBlock(ctx, env)

	case AddWatchAddrsEvent:
		return s.updateWatch(&e, func(c Cursor) RescanState {
			return &StateSyncing{Cursor: c, finished: s.finished}
		})

	case BlockDisconnectedEvent:
		cursor, undone, ok := s.disconnect(&e)
		if !ok {
			return transition(s), nil
		}

		return transition(&StateSyncing{
			Cursor:   cursor,
			finished: s.finished,
		}, undone, ContinueOutbox{}), nil

	case StopEvent:
		return transition(&StateTerminal{}), nil

	default:
		log.Warnf("StateSyncing: ignoring unexpected event %T", event)

		return transition(s), nil
	}
}

// processNextBlock scans the block after the cursor, or moves to Current if
// the cursor is at the best block. A failed step leaves the state unchanged.
func (s *StateSyncing) processNextBlock(ctx context.Context,
	env *Environment) (*StateTransition, error) {

	cur := s.Cursor

	// Stop once the cursor reaches the end block, before anything above
	// it is scanned.
	if env.reachedEnd(cur.CurStamp) {
		return transition(&StateTerminal{}, EndReachedOutbox{}), nil
	}

	// Check if we've caught up to the chain tip.
	bestBlock, err := env.bestBlock()
	if err != nil {
		return nil, err
	}

	if cur.CurStamp.Height > bestBlock.Height {
		return nil, fmt.Errorf("scanned height %d exceeds tip %d",
			cur.CurStamp.Height, bestBlock.Height)
	}

	// A historical fetch can race a reorg. Never advance from an orphaned
	// tip or publish a child from a different chain.
	active, err := env.headerByHeight(
		uint32(cur.CurStamp.Height),
	)
	if err != nil {
		return nil, err
	}
	if active.BlockHash() != cur.CurStamp.Hash {
		return nil, fmt.Errorf("scanned %s at height %d is no longer "+
			"active", cur.CurStamp.Hash, cur.CurStamp.Height)
	}

	if cur.CurStamp.Height == bestBlock.Height {
		// We're current. Report it unless this scan already did.
		var outbox []OutboxEvent
		if !s.finished {
			outbox = append(outbox, RescanFinishedOutbox{
				Hash:   cur.CurStamp.Hash,
				Height: cur.CurStamp.Height,
			})
		}

		return transition(&StateCurrent{Cursor: cur}, outbox...), nil
	}

	// Advance to the next block.
	nextHeight := uint32(cur.CurStamp.Height + 1)
	header, err := env.headerByHeight(nextHeight)
	if err != nil {
		return nil, err
	}

	if header.PrevBlock != cur.CurStamp.Hash {
		return nil, fmt.Errorf("block at height %d does not extend "+
			"scanned tip", nextHeight)
	}

	next, outbox, err := cur.scanBlock(ctx, env, header, true)
	if err != nil {
		return nil, err
	}

	// Emit progress periodically.
	if nextHeight%ProgressInterval == 0 {
		outbox = append(outbox, RescanProgressOutbox{
			Height: int32(nextHeight),
		})
	}

	return transition(
		&StateSyncing{Cursor: next, finished: s.finished},
		append(outbox, ContinueOutbox{})...,
	), nil
}

// IsTerminal returns false since Syncing is not terminal.
//
// NOTE: This is part of the RescanState interface.
func (s *StateSyncing) IsTerminal() bool { return false }

// String returns the state name.
//
// NOTE: This is part of the RescanState interface.
func (s *StateSyncing) String() string { return "Syncing" }
