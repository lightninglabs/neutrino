package rescan

import (
	"context"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/headerfs"
)

// StateRewinding walks backward through the chain from the current position
// to the target rewind height. For each disconnected block, it emits a
// BlockDisconnectedOutbox notification. Once the target is reached, it
// transitions to StateSyncing to re-scan forward with the updated watch list.
type StateRewinding struct {
	// CurStamp tracks the current position (walking backwards).
	CurStamp headerfs.BlockStamp

	// CurHeader is the full header at the current position.
	CurHeader wire.BlockHeader

	// Watch holds the current watch list state.
	Watch WatchState

	// TargetHeight is the height to rewind to.
	TargetHeight uint32
}

// ProcessEvent handles events in the Rewinding state.
//
// NOTE: This is part of the RescanState interface.
func (s *StateRewinding) ProcessEvent(_ context.Context, event RescanEvent,
	env *Environment) (*StateTransition, error) {

	switch e := event.(type) {
	case ProcessNextBlockEvent:
		return s.processRewind(env)

	case AddWatchAddrsEvent:
		return s.handleAddWatch(&e)

	case StopEvent:
		return &StateTransition{
			NextState: &StateTerminal{},
		}, nil

	default:
		log.Warnf("StateRewinding: ignoring unexpected event %T",
			event)

		return &StateTransition{NextState: s}, nil
	}
}

// processRewind walks back one batch of blocks toward the rewind target.
func (s *StateRewinding) processRewind(
	env *Environment) (*StateTransition, error) {

	curStamp := s.CurStamp
	curHeader := s.CurHeader
	var outbox []OutboxEvent

	batchSize := env.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	for i := 0; i < batchSize; i++ {
		// Check if we've reached the target.
		if curStamp.Height <= int32(s.TargetHeight) {
			// Transition to syncing from this point.
			return &StateTransition{
				NextState: &StateSyncing{
					CurStamp:  curStamp,
					CurHeader: curHeader,
					Watch:     s.Watch,
					Scanning:  true,
				},
				Events: &EmittedEvent{
					Outbox: append(outbox, SelfTellOutbox{
						Event: ProcessNextBlockEvent{},
					}),
				},
			}, nil
		}

		// Emit disconnection notification for the current block.
		outbox = append(outbox, BlockDisconnectedOutbox{
			Hash:   curStamp.Hash,
			Height: int32(curStamp.Height),
			Header: &curHeader,
		})

		// Walk back to the previous block.
		header, height, err := env.Chain.GetBlockHeader(
			&curHeader.PrevBlock,
		)
		if err != nil {
			return nil, err
		}

		curHeader = *header
		curStamp = headerfs.BlockStamp{
			Hash:   curHeader.BlockHash(),
			Height: int32(height),
		}
	}

	// Batch complete but not yet at target. Self-Tell to continue.
	return &StateTransition{
		NextState: &StateRewinding{
			CurStamp:     curStamp,
			CurHeader:    curHeader,
			Watch:        s.Watch,
			TargetHeight: s.TargetHeight,
		},
		Events: &EmittedEvent{
			Outbox: append(outbox, SelfTellOutbox{
				Event: ProcessNextBlockEvent{},
			}),
		},
	}, nil
}

// handleAddWatch handles an AddWatchAddrsEvent during rewinding. If the
// new rewind target is lower, update the target.
func (s *StateRewinding) handleAddWatch(
	event *AddWatchAddrsEvent) (*StateTransition, error) {

	newWatch, err := s.Watch.ApplyUpdate(event)
	if err != nil {
		return nil, err
	}

	targetHeight := s.TargetHeight
	if event.RewindTo != nil && *event.RewindTo < targetHeight {
		targetHeight = *event.RewindTo
	}

	return &StateTransition{
		NextState: &StateRewinding{
			CurStamp:     s.CurStamp,
			CurHeader:    s.CurHeader,
			Watch:        newWatch,
			TargetHeight: targetHeight,
		},
	}, nil
}

// IsTerminal returns false since Rewinding is not terminal.
//
// NOTE: This is part of the RescanState interface.
func (s *StateRewinding) IsTerminal() bool { return false }

// String returns the state name.
//
// NOTE: This is part of the RescanState interface.
func (s *StateRewinding) String() string { return "Rewinding" }
