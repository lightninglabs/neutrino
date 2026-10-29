package rescan

import (
	"context"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// Cursor is the scan position that every non-terminal state carries. The
// block at CurStamp has already been scanned: its notifications were emitted,
// or, for the start block, the caller scanned it before the rescan began.
type Cursor struct {
	// CurStamp is the hash and height of the last scanned block.
	CurStamp headerfs.BlockStamp

	// CurHeader is the header of the last scanned block.
	CurHeader wire.BlockHeader

	// Watch holds the current watch list state.
	Watch WatchState

	// Scanning records whether a scanned block has passed the wallet
	// birthday. Once set, every later block's filter is checked.
	Scanning bool
}

// scanBlock scans header, which must extend the cursor, and returns the
// cursor advanced to it together with the block's notifications. The filter
// is fetched only once the birthday has passed and the watch list is not
// empty. batch asks the chain to fetch the filters of the following blocks
// in the same query, which pays off only during historical catch-up.
func (c Cursor) scanBlock(ctx context.Context, env *Environment,
	header *wire.BlockHeader, batch bool) (Cursor, []OutboxEvent, error) {

	hash := header.BlockHash()
	next := Cursor{
		CurStamp: headerfs.BlockStamp{
			Hash:   hash,
			Height: c.CurStamp.Height + 1,
		},
		CurHeader: *header,
		Watch:     c.Watch,
		Scanning:  c.Scanning || header.Timestamp.After(env.StartTime),
	}

	var relevantTxs []*btcutil.Tx
	if next.Scanning && len(next.Watch.List) > 0 {
		matched, filter, err := FetchAndMatchFilter(
			ctx, env.Chain, &hash, next.Watch.List, batch,
		)
		if err != nil {
			return c, nil, err
		}

		if matched {
			relevantTxs, next.Watch, err = ExtractBlockMatches(
				ctx, env.Chain, &next.CurStamp, filter,
				next.Watch,
			)
			if err != nil {
				return c, nil, err
			}
		}
	}

	// Every block gets both notifications, matched or not, because the
	// wallet advances its sync state on them.
	headerCopy := *header
	return next, []OutboxEvent{
		FilteredBlockOutbox{
			Height:      next.CurStamp.Height,
			Header:      &headerCopy,
			RelevantTxs: relevantTxs,
		},
		BlockConnectedOutbox{
			Header:    &headerCopy,
			Hash:      hash,
			Height:    next.CurStamp.Height,
			Timestamp: header.Timestamp.Unix(),
		},
	}, nil
}

// disconnect undoes the cursor's tip if event disconnects it, and returns the
// cursor moved back to its parent with the notification to emit. It returns
// false, with the cursor unchanged, for an event that names any other block:
// one that was never scanned, or one already undone by a rewind.
func (c Cursor) disconnect(event *BlockDisconnectedEvent) (Cursor,
	BlockDisconnectedOutbox, bool) {

	if event.Header.BlockHash() != c.CurStamp.Hash ||
		int32(event.Height) != c.CurStamp.Height {

		return c, BlockDisconnectedOutbox{}, false
	}

	header := c.CurHeader
	undone := BlockDisconnectedOutbox{
		Hash:   c.CurStamp.Hash,
		Height: c.CurStamp.Height,
		Header: &header,
	}

	c.CurStamp = headerfs.BlockStamp{
		Hash:   event.ChainTip.BlockHash(),
		Height: c.CurStamp.Height - 1,
	}
	c.CurHeader = event.ChainTip

	return c, undone, true
}

// addWatch applies an AddWatchAddrs update to the cursor's watch list. It
// also returns the update's rewind target when that target is below the
// cursor, and nil when the update needs no rewind. The target block itself
// is kept: a rewind to height h rescans from h+1.
func (c Cursor) addWatch(event *AddWatchAddrsEvent) (Cursor, *uint32,
	error) {

	watch, err := c.Watch.ApplyUpdate(event)
	if err != nil {
		return c, nil, err
	}
	c.Watch = watch

	if event.RewindTo == nil || int32(*event.RewindTo) >= c.CurStamp.Height {
		return c, nil, nil
	}

	target := *event.RewindTo
	return c, &target, nil
}

// updateWatch handles an AddWatchAddrs update in a state that scans forward.
// A rewind below the cursor moves to Rewinding. Any other update stays put,
// in the state that stay rebuilds around the updated cursor.
func (c Cursor) updateWatch(event *AddWatchAddrsEvent,
	stay func(Cursor) RescanState) (*StateTransition, error) {

	next, rewind, err := c.addWatch(event)
	if err != nil {
		return nil, err
	}
	if rewind == nil {
		return transition(stay(next)), nil
	}

	return transition(&StateRewinding{
		Cursor:       next,
		TargetHeight: *rewind,
	}, ContinueOutbox{}), nil
}
