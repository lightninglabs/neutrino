package rescan

import (
	"context"
	"fmt"
)

// StateSyncing is the historical catch-up state. It scans the blocks above
// its cursor from the header store, in batches of env.BatchSize per
// ProcessNextBlockEvent, and asks for a continuation after each batch so the
// mailbox can interleave AddWatchAddrs and notifications between batches.
//
// It is entered from Initializing, from Rewinding once the rewind reaches
// its target, and from Current when a live block cannot be scanned. It
// leaves to Current once the cursor reaches the best block, to Rewinding on
// an update that rewinds below the cursor, and to Terminal on StopEvent. A
// disconnect of the cursor block moves the cursor back and stays. Connect
// notifications are ignored: the header store already holds a block when its
// notification arrives, so the next batch scans it.
type StateSyncing struct {
	// Cursor is the last scanned block.
	Cursor

	// finished records that RescanFinished was already reported for this
	// scan, so reaching the tip again does not report it twice. It is set
	// when Current falls back to Syncing, and clear after a rewind.
	finished bool
}

// ProcessEvent handles events in the Syncing state. On ProcessNextBlockEvent,
// it processes up to env.BatchSize blocks, then emits a ContinueOutbox to
// continue. This allows AddWatchAddrs to interleave between batches.
//
// NOTE: This is part of the RescanState interface.
func (s *StateSyncing) ProcessEvent(
	_ context.Context,
	event RescanEvent,
	env *Environment,
) (*StateTransition, error) {

	switch e := event.(type) {
	case ProcessNextBlockEvent:
		return s.processNextBatch(env)

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

// processNextBatch processes up to env.BatchSize blocks during historical
// catch-up.
func (s *StateSyncing) processNextBatch(
	env *Environment) (*StateTransition, error) {

	cur := s.Cursor
	var outbox []OutboxEvent

	// Commit a completed prefix before retrying a failed block. Otherwise a
	// transient failure later in the batch discards already emitted work.
	partial := func(err error) (*StateTransition, error) {
		if len(outbox) == 0 {
			return nil, err
		}

		return transition(
			&StateSyncing{Cursor: cur, finished: s.finished},
			append(outbox, ContinueOutbox{})...,
		), nil
	}

	for i := 0; i < env.batchSize(); i++ {
		// Check if we've caught up to the chain tip.
		bestBlock, err := env.Chain.BestBlock()
		if err != nil {
			return partial(err)
		}

		if cur.CurStamp.Height > bestBlock.Height {
			return partial(
				fmt.Errorf(
					"scanned height %d exceeds tip %d",
					cur.CurStamp.Height,
					bestBlock.Height,
				),
			)
		}

		// A historical fetch can race a reorg. Never advance from an
		// orphaned tip or publish a child from a different chain.
		active, err := env.Chain.GetBlockHeaderByHeight(
			uint32(cur.CurStamp.Height),
		)
		if err != nil {
			return partial(err)
		}
		if active.BlockHash() != cur.CurStamp.Hash {
			return partial(
				fmt.Errorf(
					"scanned %s at height %d is"+
						" no longer active",
					cur.CurStamp.Hash,
					cur.CurStamp.Height,
				),
			)
		}

		if cur.CurStamp.Height == bestBlock.Height {
			// We're current. Include all accumulated outbox
			// events from blocks processed in this batch,
			// followed by the finished event unless this scan
			// already reported it.
			if !s.finished {
				outbox = append(outbox, RescanFinishedOutbox{
					Hash:   cur.CurStamp.Hash,
					Height: cur.CurStamp.Height,
				})
			}

			return transition(
				&StateCurrent{Cursor: cur}, outbox...,
			), nil
		}

		// Advance to the next block.
		nextHeight := uint32(cur.CurStamp.Height + 1)
		header, err := env.Chain.GetBlockHeaderByHeight(nextHeight)
		if err != nil {
			return partial(err)
		}

		if header.PrevBlock != cur.CurStamp.Hash {
			return partial(
				fmt.Errorf(
					"block at height %d does not "+
						"extend scanned tip",
					nextHeight,
				),
			)
		}

		next, blockOutbox, err := cur.scanBlock(env, header, true)
		if err != nil {
			return partial(err)
		}
		outbox = append(outbox, blockOutbox...)

		// Emit progress periodically.
		if nextHeight%ProgressInterval == 0 {
			outbox = append(
				outbox,
				RescanProgressOutbox{
					Height: int32(
						nextHeight,
					),
				},
			)
		}

		cur = next
	}

	// Batch complete but not yet current. Continue through the mailbox so
	// any pending AddWatchAddrs runs first.
	return transition(
		&StateSyncing{Cursor: cur, finished: s.finished},
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
