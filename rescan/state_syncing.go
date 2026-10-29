package rescan

import (
	"context"
	"fmt"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// StateSyncing is the historical catch-up state. It processes blocks in
// batches (env.BatchSize per ProcessNextBlockEvent), fetching cfilters and
// checking them against the watch list. Between batches, the actor mailbox
// can interleave AddWatchAddrs events via the self-Tell pattern.
type StateSyncing struct {
	// CurStamp tracks the current position of the rescan in the chain.
	CurStamp headerfs.BlockStamp

	// CurHeader is the full header at the current position.
	CurHeader wire.BlockHeader

	// Watch holds the current watch list state.
	Watch WatchState

	// Scanning indicates whether we've passed the StartTime threshold
	// and are actively checking filters.
	Scanning bool
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
		return s.handleAddWatch(&e)

	case BlockDisconnectedEvent:
		return s.handleDisconnect(&e)

	case StopEvent:
		return &StateTransition{
			NextState: &StateTerminal{},
		}, nil

	default:
		log.Warnf("StateSyncing: ignoring unexpected event %T", event)

		return &StateTransition{NextState: s}, nil
	}
}

// processNextBatch processes up to env.BatchSize blocks during historical
// catch-up.
func (s *StateSyncing) processNextBatch(
	env *Environment) (*StateTransition, error) {

	curStamp := s.CurStamp
	curHeader := s.CurHeader
	watch := s.Watch
	scanning := s.Scanning

	var outbox []OutboxEvent

	// Commit a completed prefix before retrying a failed block. Otherwise a
	// transient failure later in the batch discards already emitted work.
	partial := func(err error) (*StateTransition, error) {
		if len(outbox) == 0 {
			return nil, err
		}
		return &StateTransition{
			NextState: &StateSyncing{
				CurStamp: curStamp, CurHeader: curHeader,
				Watch: watch, Scanning: scanning,
			},
			Events: &EmittedEvent{
				Outbox: append(
					outbox,
					ContinueOutbox{},
				),
			},
		}, nil
	}

	batchSize := env.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	for i := 0; i < batchSize; i++ {
		// Check if we've caught up to the chain tip.
		bestBlock, err := env.Chain.BestBlock()
		if err != nil {
			return partial(err)
		}

		if curStamp.Height > bestBlock.Height {
			return partial(
				fmt.Errorf(
					"scanned height %d exceeds tip %d",
					curStamp.Height,
					bestBlock.Height,
				),
			)
		}

		// A historical fetch can race a reorg. Never advance from an
		// orphaned tip or publish a child from a different chain.
		if curStamp.Height <= bestBlock.Height {
			active, err := env.Chain.GetBlockHeaderByHeight(
				uint32(curStamp.Height),
			)
			if err != nil {
				return partial(err)
			}
			if active.BlockHash() != curStamp.Hash {
				return partial(
					fmt.Errorf(
						"scanned %s at height %d is"+
							" no longer active",
						curStamp.Hash,
						curStamp.Height,
					),
				)
			}
		}

		if curStamp.Height >= bestBlock.Height {
			// We're current. Include all accumulated outbox
			// events from blocks processed in this batch,
			// followed by the finished event.
			outbox = append(outbox, RescanFinishedOutbox{
				Hash:   curStamp.Hash,
				Height: curStamp.Height,
			})

			return &StateTransition{
				NextState: &StateCurrent{
					CurStamp:  curStamp,
					CurHeader: curHeader,
					Watch:     watch,
					Scanning:  scanning,
				},
				Events: &EmittedEvent{
					Outbox: outbox,
				},
			}, nil
		}

		// Advance to the next block.
		nextHeight := uint32(curStamp.Height + 1)
		header, err := env.Chain.GetBlockHeaderByHeight(
			nextHeight,
		)
		if err != nil {
			return partial(err)
		}

		if header.PrevBlock != curStamp.Hash {
			return partial(
				fmt.Errorf(
					"block at height %d does not "+
						"extend scanned tip",
					nextHeight,
				),
			)
		}
		blockHash := header.BlockHash()
		nextStamp := headerfs.BlockStamp{
			Hash:   blockHash,
			Height: int32(nextHeight),
		}

		// Check if we've passed the birthday threshold.
		nextScanning := scanning ||
			header.Timestamp.After(env.StartTime)

		// Track relevant transactions for this block. The
		// FilteredBlockOutbox is emitted for EVERY block (with
		// nil RelevantTxs when no match) because the wallet
		// relies on it to advance its sync state.
		var relevantTxs []*btcutil.Tx

		if nextScanning && len(watch.List) > 0 {
			// Fetch filter and check for matches.
			matched, filter, err := FetchAndMatchFilter(
				env.Chain,
				&blockHash,
				watch.List,
				env.QueryOpts...,
			)
			if err != nil {
				return partial(err)
			}

			if matched {
				// Fetch full block and extract matches.
				relevantTxs, watch, err = ExtractBlockMatches(
					env.Chain,
					&nextStamp,
					filter,
					watch,
					env.QueryOpts...,
				)
				if err != nil {
					return partial(err)
				}
			}
		}

		// Always emit filtered block connected for the wallet
		// to advance its sync state. This matches the old
		// notifyBlockWithFilter behavior.
		headerCopy := *header
		outbox = append(outbox, FilteredBlockOutbox{
			Height:      int32(nextHeight),
			Header:      &headerCopy,
			RelevantTxs: relevantTxs,
		})

		// Also emit the simpler block connected notification.
		outbox = append(outbox, BlockConnectedOutbox{
			Header:    &headerCopy,
			Hash:      blockHash,
			Height:    int32(nextHeight),
			Timestamp: header.Timestamp.Unix(),
		})

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

		curStamp = nextStamp
		curHeader = *header
		scanning = nextScanning
	}

	// Batch complete but not yet current. Emit self-Tell to continue
	// after checking the mailbox for any pending AddWatchAddrs.
	return &StateTransition{
		NextState: &StateSyncing{
			CurStamp:  curStamp,
			CurHeader: curHeader,
			Watch:     watch,
			Scanning:  scanning,
		},
		Events: &EmittedEvent{
			Outbox: append(outbox, ContinueOutbox{}),
		},
	}, nil
}

// handleDisconnect applies a queued chain disconnection to the scanned tip.
func (s *StateSyncing) handleDisconnect(
	event *BlockDisconnectedEvent,
) (*StateTransition, error) {

	if event.Header.BlockHash() != s.CurStamp.Hash ||
		int32(event.Height) != s.CurStamp.Height {

		return &StateTransition{NextState: s}, nil
	}
	header := event.Header
	return &StateTransition{
		NextState: &StateSyncing{
			CurStamp: headerfs.BlockStamp{
				Hash:   event.ChainTip.BlockHash(),
				Height: s.CurStamp.Height - 1,
			},
			CurHeader: event.ChainTip,
			Watch:     s.Watch,
			Scanning:  s.Scanning,
		},
		Events: &EmittedEvent{Outbox: []OutboxEvent{
			BlockDisconnectedOutbox{
				Hash:   s.CurStamp.Hash,
				Height: s.CurStamp.Height,
				Header: &header,
			},
			ContinueOutbox{},
		}},
	}, nil
}

// handleAddWatch handles an AddWatchAddrsEvent during syncing.
func (s *StateSyncing) handleAddWatch(
	event *AddWatchAddrsEvent) (*StateTransition, error) {

	newWatch, err := s.Watch.ApplyUpdate(event)
	if err != nil {
		return nil, err
	}

	// If a rewind is requested and target is below our current height,
	// transition to rewinding.
	if event.RewindTo != nil &&
		int32(*event.RewindTo) < s.CurStamp.Height {

		return &StateTransition{
			NextState: &StateRewinding{
				CurStamp:     s.CurStamp,
				CurHeader:    s.CurHeader,
				Watch:        newWatch,
				TargetHeight: *event.RewindTo,
			},
			Events: &EmittedEvent{
				Outbox: []OutboxEvent{
					ContinueOutbox{},
				},
			},
		}, nil
	}

	// No rewind needed. Stay in syncing with updated watch list.
	return &StateTransition{
		NextState: &StateSyncing{
			CurStamp:  s.CurStamp,
			CurHeader: s.CurHeader,
			Watch:     newWatch,
			Scanning:  s.Scanning,
		},
	}, nil
}

// IsTerminal returns false since Syncing is not terminal.
//
// NOTE: This is part of the RescanState interface.
func (s *StateSyncing) IsTerminal() bool { return false }

// String returns the state name.
//
// NOTE: This is part of the RescanState interface.
func (s *StateSyncing) String() string { return "Syncing" }
