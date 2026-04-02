package rescan

import (
	"context"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/wire"
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
// it processes up to env.BatchSize blocks, then emits a SelfTellOutbox to
// continue. This allows AddWatchAddrs to interleave between batches.
//
// NOTE: This is part of the RescanState interface.
func (s *StateSyncing) ProcessEvent(_ context.Context, event RescanEvent,
	env *Environment) (*StateTransition, error) {

	switch e := event.(type) {
	case ProcessNextBlockEvent:
		return s.processNextBatch(env)

	case AddWatchAddrsEvent:
		return s.handleAddWatch(&e, env)

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

	batchSize := env.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	for i := 0; i < batchSize; i++ {
		// Check if we've hit the end block (bounded rescan).
		if env.EndBlock != nil {
			if curStamp.Hash == env.EndBlock.Hash ||
				curStamp.Height >= env.EndBlock.Height {

				return &StateTransition{
					NextState: &StateTerminal{},
					Events: &EmittedEvent{
						Outbox: append(outbox,
							RescanFinishedOutbox{
								Hash: curStamp.Hash,
								Height: int32(
									curStamp.Height,
								),
							},
						),
					},
				}, nil
			}
		}

		// Check if we've caught up to the chain tip.
		bestBlock, err := env.Chain.BestBlock()
		if err != nil {
			return nil, err
		}

		if curStamp.Height >= bestBlock.Height {
			// We're current. Include all accumulated outbox
			// events from blocks processed in this batch,
			// followed by the subscription and finished events.
			outbox = append(outbox,
				StartSubscriptionOutbox{
					BestHeight: uint32(curStamp.Height),
				},
				RescanFinishedOutbox{
					Hash:   curStamp.Hash,
					Height: int32(curStamp.Height),
				},
			)

			return &StateTransition{
				NextState: &StateCurrent{
					CurStamp:  curStamp,
					CurHeader: curHeader,
					Watch:     watch,
				},
				Events: &EmittedEvent{
					Outbox: outbox,
				},
			}, nil
		}

		// Advance to the next block.
		nextHeight := uint32(curStamp.Height + 1)
		header, err := env.Chain.GetBlockHeaderByHeight(nextHeight)
		if err != nil {
			return nil, err
		}

		blockHash := header.BlockHash()
		nextStamp := headerfs.BlockStamp{
			Hash:   blockHash,
			Height: int32(nextHeight),
		}

		// Check if we've passed the birthday threshold.
		if !scanning {
			if header.Timestamp.After(env.StartTime) {
				scanning = true
			}
		}

		// Track relevant transactions for this block. The
		// FilteredBlockOutbox is emitted for EVERY block (with
		// nil RelevantTxs when no match) because the wallet
		// relies on it to advance its sync state.
		var relevantTxs []*btcutil.Tx

		if scanning && len(watch.List) > 0 {
			// Fetch filter and check for matches.
			matched, filter, err := FetchAndMatchFilter(
				env.Chain, &blockHash, watch.List,
				env.QueryOpts...,
			)
			if err != nil {
				return nil, err
			}

			if matched {
				// Fetch full block and extract matches.
				relevantTxs, watch, err = ExtractBlockMatches(
					env.Chain, &nextStamp, filter,
					watch, env.QueryOpts...,
				)
				if err != nil {
					return nil, err
				}
			}
		}

		// Always emit filtered block connected for the wallet
		// to advance its sync state. This matches the old
		// notifyBlockWithFilter behavior.
		outbox = append(outbox, FilteredBlockOutbox{
			Height:      int32(nextHeight),
			Header:      header,
			RelevantTxs: relevantTxs,
		})

		// Also emit the simpler block connected notification.
		outbox = append(outbox, BlockConnectedOutbox{
			Hash:      blockHash,
			Height:    int32(nextHeight),
			Timestamp: header.Timestamp.Unix(),
		})

		// Emit progress periodically.
		if nextHeight%ProgressInterval == 0 {
			outbox = append(outbox, RescanProgressOutbox{
				Height: int32(nextHeight),
			})
		}

		curStamp = nextStamp
		curHeader = *header
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
			Outbox: append(outbox, SelfTellOutbox{
				Event: ProcessNextBlockEvent{},
			}),
		},
	}, nil
}

// handleAddWatch handles an AddWatchAddrsEvent during syncing.
func (s *StateSyncing) handleAddWatch(event *AddWatchAddrsEvent,
	env *Environment) (*StateTransition, error) {

	newWatch, err := s.Watch.ApplyUpdate(event)
	if err != nil {
		return nil, err
	}

	// If a rewind is requested and target is below our current height,
	// transition to rewinding.
	if event.RewindTo != nil && int32(*event.RewindTo) < s.CurStamp.Height {
		return &StateTransition{
			NextState: &StateRewinding{
				CurStamp:     s.CurStamp,
				CurHeader:    s.CurHeader,
				Watch:        newWatch,
				TargetHeight: *event.RewindTo,
			},
			Events: &EmittedEvent{
				Outbox: []OutboxEvent{
					SelfTellOutbox{
						Event: ProcessNextBlockEvent{},
					},
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
