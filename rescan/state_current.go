package rescan

import (
	"context"
	"fmt"
)

// StateCurrent is the steady state when the rescan is caught up to the chain
// tip. It scans each block that a connect notification delivers, and undoes
// its tip on a disconnect notification.
//
// It is entered from Syncing at the tip. It leaves to Syncing when a
// connected block cannot be scanned, because a fetch failed or the block does
// not extend the cursor, so Syncing catches up from the header store instead
// of retrying the block. It leaves to Rewinding on an update that rewinds
// below the cursor, and to Terminal on StopEvent.
type StateCurrent struct {
	// Cursor is the last scanned block, the chain tip when last checked.
	Cursor
}

// ProcessEvent handles events in the Current state.
//
// NOTE: This is part of the RescanState interface.
func (s *StateCurrent) ProcessEvent(
	ctx context.Context,
	event RescanEvent,
	env *Environment,
) (*StateTransition, error) {

	switch e := event.(type) {
	case ProcessNextBlockEvent:
		// A continuation or retry queued before Syncing reached the
		// tip, or the actor yielding a connect notification to a
		// waiting request. If the header store holds blocks above the
		// cursor, Syncing scans them, the notified block included.
		// The subscription reports every later reorg, so there is
		// nothing else to check.
		best, err := env.Chain.BestBlock()
		if err != nil {
			return nil, err
		}
		if best.Height <= s.CurStamp.Height {
			return transition(s), nil
		}

		return transition(&StateSyncing{
			Cursor:   s.Cursor,
			finished: true,
		}, ContinueOutbox{}), nil

	case BlockConnectedEvent:
		next, err := s.handleBlockConnected(ctx, &e, env)
		if err != nil {
			// Catch up from the header store instead of retrying
			// this block. Syncing rescans every block above the
			// cursor, this one included, and so also covers any
			// successor that arrives in the meantime.
			log.Warnf("StateCurrent: block %d: %v, falling back "+
				"to Syncing", e.Height, err)

			return transition(&StateSyncing{
				Cursor:   s.Cursor,
				finished: true,
			}, ContinueOutbox{}), nil
		}

		return next, nil

	case BlockDisconnectedEvent:
		cursor, undone, ok := s.disconnect(&e)
		if !ok {
			log.Debugf("StateCurrent: ignoring disconnect for "+
				"block %v (current tip: %v)",
				e.Header.BlockHash(), s.CurStamp.Hash)

			return transition(s), nil
		}

		return current(env, cursor, undone), nil

	case AddWatchAddrsEvent:
		return s.updateWatch(&e, func(c Cursor) RescanState {
			return &StateCurrent{Cursor: c}
		})

	case StopEvent:
		return transition(&StateTerminal{}), nil

	default:
		log.Warnf("StateCurrent: ignoring unexpected event %T", event)

		return transition(s), nil
	}
}

// handleBlockConnected processes a new block from the subscription.
func (s *StateCurrent) handleBlockConnected(ctx context.Context,
	event *BlockConnectedEvent,
	env *Environment,
) (*StateTransition, error) {

	blockHash := event.Header.BlockHash()

	// Notifications queued while catching up may arrive after this block
	// has already been scanned. Keep the current tip unchanged.
	if int32(event.Height) < s.CurStamp.Height {
		return transition(s), nil
	}

	// Duplicate delivery of the block at our current tip can happen when a
	// previously failed block notification is retried after the block was
	// already recovered through another path, such as rewind+sync. Treat it
	// as an idempotent no-op rather than an ordering error.
	if event.Height == uint32(s.CurStamp.Height) &&
		blockHash == s.CurStamp.Hash {

		return transition(s), nil
	}

	// A filter notification can be delayed past a reorg. Only process the
	// block if its height still identifies the active chain header.
	active, err := env.Chain.GetBlockHeaderByHeight(event.Height)
	if err != nil {
		return nil, err
	}
	if active.BlockHash() != blockHash {
		return transition(s), nil
	}

	// Validate the block connects to our current tip.
	if event.Header.PrevBlock != s.CurStamp.Hash {
		return nil, fmt.Errorf(
			"block at height %d has prev hash "+
				"%v, expected %v",
			event.Height,
			event.Header.PrevBlock,
			s.CurStamp.Hash,
		)
	}

	next, outbox, err := s.scanBlock(ctx, env, &event.Header, false)
	if err != nil {
		return nil, err
	}

	return current(env, next, outbox...), nil
}

// current returns a transition to Current at cursor that emits outbox, or to
// Terminal if cursor has reached the end block.
func current(env *Environment, cursor Cursor,
	outbox ...OutboxEvent) *StateTransition {

	if env.reachedEnd(cursor.CurStamp) {
		return transition(&StateTerminal{},
			append(outbox, EndReachedOutbox{})...)
	}

	return transition(&StateCurrent{Cursor: cursor}, outbox...)
}

// IsTerminal returns false since Current is not terminal.
//
// NOTE: This is part of the RescanState interface.
func (s *StateCurrent) IsTerminal() bool { return false }

// String returns the state name.
//
// NOTE: This is part of the RescanState interface.
func (s *StateCurrent) String() string { return "Current" }
