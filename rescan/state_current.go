package rescan

import (
	"context"
	"fmt"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// StateCurrent is the steady-state when the rescan is caught up to the chain
// tip. It receives block notifications from the subscription bridge goroutine
// and checks them against the watch list. AddWatchAddrs with a rewind target
// will cause a transition to StateRewinding.
type StateCurrent struct {
	// CurStamp tracks the current position of the rescan in the chain.
	CurStamp headerfs.BlockStamp

	// CurHeader is the full header at the current position.
	CurHeader wire.BlockHeader

	// Watch holds the current watch list state.
	Watch WatchState

	// Scanning records whether the birthday threshold has been reached.
	Scanning bool
}

// ProcessEvent handles events in the Current state.
//
// NOTE: This is part of the RescanState interface.
func (s *StateCurrent) ProcessEvent(
	_ context.Context,
	event RescanEvent,
	env *Environment,
) (*StateTransition, error) {

	switch e := event.(type) {
	case ProcessNextBlockEvent:
		return s.reconcileTip(env)

	case BlockConnectedEvent:
		return s.handleBlockConnected(&e, env)

	case BlockDisconnectedEvent:
		return s.handleBlockDisconnected(&e)

	case AddWatchAddrsEvent:
		return s.handleAddWatch(&e)

	case StopEvent:
		return transition(&StateTerminal{}), nil

	default:
		log.Warnf("StateCurrent: ignoring unexpected event %T", event)

		return transition(s), nil
	}
}

// reconcileTip checks the scanned tip after the subscription is installed.
func (s *StateCurrent) reconcileTip(
	env *Environment,
) (*StateTransition, error) {

	header, err := env.Chain.GetBlockHeaderByHeight(
		uint32(s.CurStamp.Height),
	)
	if err != nil {
		return nil, err
	}
	if header.BlockHash() == s.CurStamp.Hash {
		return transition(s), nil
	}
	if s.CurStamp.Height <= 0 {
		return nil, fmt.Errorf("genesis block changed")
	}
	return transition(&StateRewinding{
		CurStamp: s.CurStamp, CurHeader: s.CurHeader,
		Watch:        s.Watch,
		TargetHeight: uint32(s.CurStamp.Height - 1),
	}, ContinueOutbox{}), nil
}

// handleBlockConnected processes a new block from the subscription.
func (s *StateCurrent) handleBlockConnected(
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

	newStamp := headerfs.BlockStamp{
		Hash:   blockHash,
		Height: int32(event.Height),
	}

	watch := s.Watch
	var (
		outbox      []OutboxEvent
		relevantTxs []*btcutil.Tx
	)

	scanning := s.Scanning ||
		event.Header.Timestamp.After(env.StartTime)
	if scanning && len(watch.List) > 0 {
		// Fetch the filter for this block.
		filter, err := env.Chain.GetCFilter(
			blockHash, wire.GCSFilterRegular, env.QueryOpts...,
		)
		if err != nil {
			return nil, err
		}

		if filter != nil && filter.N() != 0 {
			matched, err := MatchBlockFilter(
				filter, &blockHash, watch.List,
			)
			if err != nil {
				return nil, err
			}

			if matched {
				relevantTxs, watch, err = ExtractBlockMatches(
					env.Chain,
					&newStamp,
					filter,
					watch,
					env.QueryOpts...,
				)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// Always emit filtered block connected — the wallet relies on
	// this for every block to advance its sync state. This matches
	// the old notifyBlockWithFilter which calls
	// OnFilteredBlockConnected for every block.
	headerCopy := event.Header
	outbox = append(outbox, FilteredBlockOutbox{
		Height:      int32(event.Height),
		Header:      &headerCopy,
		RelevantTxs: relevantTxs,
	})

	// Also emit the simpler block connected notification.
	outbox = append(outbox, BlockConnectedOutbox{
		Header:    &headerCopy,
		Hash:      blockHash,
		Height:    int32(event.Height),
		Timestamp: event.Header.Timestamp.Unix(),
	})

	return transition(&StateCurrent{
		CurStamp:  newStamp,
		CurHeader: event.Header,
		Watch:     watch,
		Scanning:  scanning,
	}, outbox...), nil
}

// handleBlockDisconnected processes a block disconnection (reorg) from the
// subscription.
func (s *StateCurrent) handleBlockDisconnected(
	event *BlockDisconnectedEvent,
) (*StateTransition, error) {

	// Only handle if this is the block at our current tip.
	if event.Header.BlockHash() != s.CurStamp.Hash {
		log.Debugf(
			"StateCurrent: ignoring disconnect for block %v "+
				"(current tip: %v)",
			event.Header.BlockHash(),
			s.CurStamp.Hash,
		)

		return transition(s), nil
	}

	headerCopy := s.CurHeader
	return transition(&StateCurrent{
		CurStamp: headerfs.BlockStamp{
			Hash:   event.ChainTip.BlockHash(),
			Height: s.CurStamp.Height - 1,
		},
		CurHeader: event.ChainTip,
		Watch:     s.Watch,
		Scanning:  s.Scanning,
	}, BlockDisconnectedOutbox{
		Hash:   s.CurStamp.Hash,
		Height: s.CurStamp.Height,
		Header: &headerCopy,
	}), nil
}

// handleAddWatch handles an AddWatchAddrsEvent while current.
func (s *StateCurrent) handleAddWatch(
	event *AddWatchAddrsEvent) (*StateTransition, error) {

	newWatch, err := s.Watch.ApplyUpdate(event)
	if err != nil {
		return nil, err
	}

	// Only rewind if the requested target is actually below our current
	// height. A no-op or future rewind target should behave like a plain
	// watch-list update and keep the current subscription intact.
	if event.RewindTo != nil &&
		int32(*event.RewindTo) < s.CurStamp.Height {

		return transition(&StateRewinding{
			CurStamp:     s.CurStamp,
			CurHeader:    s.CurHeader,
			Watch:        newWatch,
			TargetHeight: *event.RewindTo,
		}, ContinueOutbox{}), nil
	}

	// No rewind — stay current with updated watch list.
	return transition(&StateCurrent{
		CurStamp:  s.CurStamp,
		CurHeader: s.CurHeader,
		Watch:     newWatch,
		Scanning:  s.Scanning,
	}), nil
}

// IsTerminal returns false since Current is not terminal.
//
// NOTE: This is part of the RescanState interface.
func (s *StateCurrent) IsTerminal() bool { return false }

// String returns the state name.
//
// NOTE: This is part of the RescanState interface.
func (s *StateCurrent) String() string { return "Current" }
