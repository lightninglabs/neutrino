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

	var (
		relevantTxs []*btcutil.Tx
		matches     []TxMatch
	)
	if next.Scanning && len(next.Watch.List) > 0 {
		matched, filter, err := FetchAndMatchFilter(
			ctx, env.Chain, &hash, next.Watch.List, batch,
		)
		if err != nil {
			return c, nil, err
		}

		if matched {
			relevantTxs, matches, next.Watch, err =
				ExtractBlockMatches(
					ctx, env.Chain, &next.CurStamp,
					filter, next.Watch,
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
			Matches:     matches,
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

// rewindStep is one update's rewind: every block above Target is undone,
// with its disconnect callbacks suppressed if Silent is set.
type rewindStep struct {
	Target uint32
	Silent bool
}

// rewindPlan is the rewinds a state still owes, in arrival order, each with a
// lower target than the one before. The legacy rescan applied each update's
// rewind in full before it read the next update, so the blocks above the
// first target take the first update's Silent flag, and the blocks between
// the first and the second target take the second's.
type rewindPlan []rewindStep

// lower adds step to the plan if it rewinds below every step already in it. A
// step that does not is a no-op, since the legacy rescan would already have
// walked below its target when it read the update.
func (p rewindPlan) lower(step rewindStep) rewindPlan {
	if len(p) > 0 && step.Target >= p[len(p)-1].Target {
		return p
	}

	return append(append(rewindPlan(nil), p...), step)
}

// target returns the lowest target in the plan.
func (p rewindPlan) target() uint32 {
	return p[len(p)-1].Target
}

// silentAt reports whether undoing the block at height must not report its
// disconnect: the block belongs to the first step whose target is below it.
func (p rewindPlan) silentAt(height int32) bool {
	for _, step := range p {
		if height > int32(step.Target) {
			return step.Silent
		}
	}

	return false
}

// addWatch applies an AddWatchAddrs update to the cursor's watch list. It
// also returns the update's rewind when its target is below the cursor, and
// nil when the update needs no rewind. The target block itself is kept: a
// rewind to height h rescans from h+1.
func (c Cursor) addWatch(event *AddWatchAddrsEvent) (Cursor, *rewindStep,
	error) {

	watch, err := c.Watch.ApplyUpdate(event)
	if err != nil {
		return c, nil, err
	}
	c.Watch = watch

	if event.RewindTo == nil ||
		int32(*event.RewindTo) >= c.CurStamp.Height {

		return c, nil, nil
	}

	return c, &rewindStep{
		Target: *event.RewindTo,
		Silent: event.DisableDisconnectedNtfns,
	}, nil
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

	return transition(newRewinding(next, rewindPlan{*rewind}),
		ContinueOutbox{}), nil
}
