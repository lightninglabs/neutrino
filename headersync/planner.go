package headersync

import (
	"fmt"
	"sort"

	"github.com/btcsuite/btcd/chaincfg"
)

// PlanRangesResult describes the ranges created by one anchor-planning pass.
type PlanRangesResult struct {
	Ranges          []HeaderRange
	NextID          RangeID
	SkippedExisting int
}

// SeedCommittedAnchor makes the current committed tip available as a trusted
// start boundary for future range planning.
func (h *HeaderSyncFSM) SeedCommittedAnchor() Anchor {
	anchor, _ := h.AddOrConfirmAnchor(
		h.committedTip, h.committedHash, true,
	)

	return anchor
}

// AddCheckpointAnchors adds chain checkpoints as trusted range boundaries.
func (h *HeaderSyncFSM) AddCheckpointAnchors(
	checkpoints []chaincfg.Checkpoint) int {

	var added int
	for _, checkpoint := range checkpoints {
		if checkpoint.Height < 0 || checkpoint.Hash == nil {
			continue
		}

		if _, ok := h.anchors[uint32(checkpoint.Height)]; !ok {
			added++
		}
		h.AddOrConfirmAnchor(
			uint32(checkpoint.Height), *checkpoint.Hash, true,
		)
	}

	return added
}

// AddDiscoveredAnchor records an untrusted peer-discovered boundary. The
// returned anchor is usable only once AnchorConfirmed returns true.
func (h *HeaderSyncFSM) AddDiscoveredAnchor(height uint32,
	hash Hash) (Anchor, bool) {

	return h.AddOrConfirmAnchor(height, hash, false)
}

// PlanConfirmedAnchorRanges plans work between adjacent confirmed anchors in
// [startHeight, stopHeight]. This does not invent split points; if no confirmed
// anchor exists at a height, no range boundary is created there.
func (h *HeaderSyncFSM) PlanConfirmedAnchorRanges(nextID RangeID,
	startHeight, stopHeight uint32) (PlanRangesResult, error) {

	if nextID == NoRange {
		return PlanRangesResult{}, fmt.Errorf("%w: zero next id",
			ErrInvalidRange)
	}
	if startHeight >= stopHeight {
		return PlanRangesResult{}, fmt.Errorf("%w: start %d stop %d",
			ErrInvalidRange, startHeight, stopHeight)
	}

	anchors := h.confirmedAnchorsInRange(startHeight, stopHeight)
	if len(anchors) < 2 || anchors[0].Height != startHeight {
		return PlanRangesResult{NextID: nextID}, nil
	}

	result := PlanRangesResult{NextID: nextID}
	for i := 1; i < len(anchors); i++ {
		start := anchors[i-1]
		stop := anchors[i]
		if _, ok := h.rangeBySpan(start.Height, stop.Height); ok {
			result.SkippedExisting++
			continue
		}

		rng, ok, err := h.PlanRange(
			result.NextID, start.Height, stop.Height,
		)
		if err != nil {
			return result, err
		}
		if !ok {
			continue
		}

		result.Ranges = append(result.Ranges, rng)
		result.NextID++
		if result.NextID == NoRange {
			return result, fmt.Errorf("%w: range id overflow",
				ErrInvalidRange)
		}
	}

	return result, nil
}

func (h *HeaderSyncFSM) confirmedAnchorsInRange(startHeight,
	stopHeight uint32) []Anchor {

	anchors := make([]Anchor, 0, len(h.anchors))
	for _, anchor := range h.anchors {
		if anchor.Height < startHeight || anchor.Height > stopHeight ||
			!h.anchorConfirmed(anchor) {

			continue
		}

		anchors = append(anchors, anchor)
	}

	sort.Slice(anchors, func(i, j int) bool {
		return anchors[i].Height < anchors[j].Height
	})

	return anchors
}

func (h *HeaderSyncFSM) rangeBySpan(startHeight,
	stopHeight uint32) (HeaderRange, bool) {

	for _, rng := range h.ranges {
		if rng.StartHeight == startHeight && rng.StopHeight == stopHeight &&
			rng.State != RangeFailed && rng.State != RangeStale {

			return rng, true
		}
	}

	return HeaderRange{}, false
}
