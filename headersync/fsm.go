package headersync

import (
	"fmt"
	"sort"
	"time"
)

// HeaderSyncFSM is the pure manager state machine. It owns no goroutines and
// performs no network I/O; actors and the block manager drive it with explicit
// events.
type HeaderSyncFSM struct {
	cfg Config

	committedTip  uint32
	committedHash Hash

	anchors   map[uint32]Anchor
	ranges    map[RangeID]HeaderRange
	rangeKeys []RangeID
	peers     map[string]PeerSnapshot

	commitLog []RangeID
	metrics   Metrics
}

// NewHeaderSyncFSM creates a header sync FSM from the current committed tip.
func NewHeaderSyncFSM(committedTip uint32, committedHash Hash,
	cfg Config) *HeaderSyncFSM {

	return &HeaderSyncFSM{
		cfg:           cfg.normalize(),
		committedTip:  committedTip,
		committedHash: committedHash,
		anchors:       make(map[uint32]Anchor),
		ranges:        make(map[RangeID]HeaderRange),
		peers:         make(map[string]PeerSnapshot),
	}
}

// CommittedTip returns the visible chain tip height.
func (h *HeaderSyncFSM) CommittedTip() uint32 {
	return h.committedTip
}

// CommittedHash returns the visible chain tip hash.
func (h *HeaderSyncFSM) CommittedHash() Hash {
	return h.committedHash
}

// AdvanceCommittedTip reconciles the FSM with a block-header tip that was
// persisted outside the staged headersync commit path. Production can still
// fall back to the legacy serial writer in a few edge cases; when that writer
// advances the database tip, the scheduler must treat the new tip as committed
// before planning additional frontier work.
func (h *HeaderSyncFSM) AdvanceCommittedTip(height uint32,
	hash Hash) bool {

	if height < h.committedTip {
		return false
	}
	if height == h.committedTip {
		h.AddOrConfirmAnchor(height, hash, true)
		h.committedHash = hash

		return false
	}

	h.committedTip = height
	h.committedHash = hash
	h.AddOrConfirmAnchor(height, hash, true)

	for id, rng := range h.ranges {
		switch {
		case rng.StopHeight <= height:
			h.detachRangeFromPeers(id)
			rng.OwnerPeer = NoPeer
			rng.State = RangeCommitted
			rng.Valid = true
			rng.StagedAt = time.Time{}
			h.ranges[id] = rng

		case rng.StartHeight < height:
			h.detachRangeFromPeers(id)
			rng.OwnerPeer = NoPeer
			rng.State = RangeStale
			rng.Valid = false
			rng.StagedAt = time.Time{}
			h.ranges[id] = rng
		}
	}

	return true
}

// Metrics returns the current counters.
func (h *HeaderSyncFSM) Metrics() Metrics {
	return h.metrics
}

// CommitLog returns the ranges committed in order.
func (h *HeaderSyncFSM) CommitLog() []RangeID {
	return append([]RangeID(nil), h.commitLog...)
}

// AddOrConfirmAnchor adds a trusted checkpoint or records a matching peer
// observation for a discovered anchor.
func (h *HeaderSyncFSM) AddOrConfirmAnchor(height uint32, hash Hash,
	trusted bool) (Anchor, bool) {

	if existing, ok := h.anchors[height]; ok {
		if existing.Hash != hash {
			if trusted {
				anchor := h.newAnchor(height, hash, true)
				h.anchors[height] = anchor
				h.metrics.AnchorsConfirmed++

				return anchor, true
			}

			if !existing.Trusted {
				existing.Rejected = true
				h.anchors[height] = existing
			}
			h.metrics.AnchorsRejected++

			return existing, false
		}

		wasConfirmed := h.anchorConfirmed(existing)
		if trusted {
			existing.Trusted = true
			existing.Confirmations =
				h.cfg.AnchorConfirmationsRequired
		} else if existing.Confirmations <
			h.cfg.AnchorConfirmationsRequired {

			existing.Confirmations++
		}

		h.anchors[height] = existing
		if !wasConfirmed && h.anchorConfirmed(existing) {
			h.metrics.AnchorsConfirmed++
		}

		return existing, true
	}

	anchor := h.newAnchor(height, hash, trusted)
	h.anchors[height] = anchor
	h.metrics.AnchorsAdded++
	if h.anchorConfirmed(anchor) {
		h.metrics.AnchorsConfirmed++
	}

	return anchor, true
}

func (h *HeaderSyncFSM) newAnchor(height uint32, hash Hash,
	trusted bool) Anchor {

	confirmations := 1
	if trusted {
		confirmations = h.cfg.AnchorConfirmationsRequired
	}

	return Anchor{
		Height:        height,
		Hash:          hash,
		Trusted:       trusted,
		Confirmations: confirmations,
	}
}

// AnchorByHeight returns an anchor by height.
func (h *HeaderSyncFSM) AnchorByHeight(height uint32) (Anchor, bool) {
	anchor, ok := h.anchors[height]
	return anchor, ok
}

// Anchors returns all anchors sorted by height.
func (h *HeaderSyncFSM) Anchors() []Anchor {
	heights := make([]int, 0, len(h.anchors))
	for height := range h.anchors {
		heights = append(heights, int(height))
	}
	sort.Ints(heights)

	anchors := make([]Anchor, 0, len(heights))
	for _, height := range heights {
		anchors = append(anchors, h.anchors[uint32(height)])
	}

	return anchors
}

// AnchorConfirmed returns true if the anchor can be used as a range boundary.
func (h *HeaderSyncFSM) AnchorConfirmed(anchor Anchor) bool {
	return h.anchorConfirmed(anchor)
}

func (h *HeaderSyncFSM) anchorConfirmed(anchor Anchor) bool {
	return !anchor.Rejected && (anchor.Trusted ||
		anchor.Confirmations >= h.cfg.AnchorConfirmationsRequired)
}

// AddPeer adds or replaces a peer snapshot.
func (h *HeaderSyncFSM) AddPeer(peer PeerSnapshot) error {
	if peer.ID == "" {
		return fmt.Errorf("%w: empty peer id", ErrInvalidPeer)
	}

	if existing, ok := h.peers[peer.ID]; ok && peer.LocalRanges == nil {
		peer.LocalRanges = existing.LocalRanges
	}

	h.peers[peer.ID] = peer.clone()
	return nil
}

// RemovePeer removes a peer from the scheduler.
func (h *HeaderSyncFSM) RemovePeer(id string) {
	peer, ok := h.peers[id]
	if ok {
		for _, rangeID := range peer.LocalRanges {
			h.requeuePeerRange(id, rangeID)
		}
		if peer.ActiveRange != NoRange {
			h.requeuePeerRange(id, peer.ActiveRange)
		}
	}

	for _, rng := range h.ranges {
		if rng.OwnerPeer != id {
			continue
		}

		h.requeuePeerRange(id, rng.ID)
	}

	delete(h.peers, id)
}

func (h *HeaderSyncFSM) requeuePeerRange(peerID string, rangeID RangeID) {
	rng, ok := h.ranges[rangeID]
	if !ok || rng.OwnerPeer != peerID {
		return
	}

	switch rng.State {
	case RangeOwned, RangeActive:
	default:
		return
	}

	rng.OwnerPeer = NoPeer
	rng.LeaseEpoch++
	rng.State = RangeQueued
	rng.Valid = false
	rng.StagedAt = time.Time{}

	h.ranges[rangeID] = rng
}

// SnapshotPeer returns a copy of one peer.
func (h *HeaderSyncFSM) SnapshotPeer(id string) (PeerSnapshot, bool) {
	peer, ok := h.peers[id]
	if !ok {
		return PeerSnapshot{}, false
	}

	return peer.clone(), true
}

// Peers returns all peers sorted by ID.
func (h *HeaderSyncFSM) Peers() []PeerSnapshot {
	peers := make([]PeerSnapshot, 0, len(h.peers))
	for _, peer := range h.peers {
		peers = append(peers, peer.clone())
	}

	sort.Slice(peers, func(i, j int) bool {
		return peers[i].ID < peers[j].ID
	})

	return peers
}

// UpdatePeerState updates a peer scheduling state.
func (h *HeaderSyncFSM) UpdatePeerState(id string,
	state PeerState) error {

	peer, ok := h.peers[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownPeer, id)
	}

	peer.State = state
	h.peers[id] = peer

	return nil
}

// UpdatePeerRTT updates a peer RTT sample.
func (h *HeaderSyncFSM) UpdatePeerRTT(id string,
	rtt time.Duration) error {

	peer, ok := h.peers[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownPeer, id)
	}

	peer.RTT = rtt
	h.peers[id] = peer

	return nil
}

// RangeByID returns a copy of one range.
func (h *HeaderSyncFSM) RangeByID(id RangeID) (HeaderRange, bool) {
	rng, ok := h.ranges[id]
	return rng, ok
}

// Ranges returns all ranges sorted by start height, then ID.
func (h *HeaderSyncFSM) Ranges() []HeaderRange {
	ranges := make([]HeaderRange, 0, len(h.ranges))
	for _, rng := range h.ranges {
		ranges = append(ranges, rng)
	}

	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].StartHeight != ranges[j].StartHeight {
			return ranges[i].StartHeight < ranges[j].StartHeight
		}

		return ranges[i].ID < ranges[j].ID
	})

	return ranges
}

// CanPlanRange returns true when both boundary anchors are confirmed.
func (h *HeaderSyncFSM) CanPlanRange(startHeight,
	stopHeight uint32) bool {

	if startHeight >= stopHeight {
		return false
	}
	if stopHeight-startHeight > h.cfg.MaxRangeHeaders {
		return false
	}

	startAnchor, startOK := h.anchors[startHeight]
	stopAnchor, stopOK := h.anchors[stopHeight]

	return startOK && stopOK && h.anchorConfirmed(startAnchor) &&
		h.anchorConfirmed(stopAnchor)
}

// PlanRange creates queued work between two confirmed anchors. The range uses
// interval semantics (start_height, stop_height], so the start anchor is the
// locator boundary and the stop anchor is the final expected header hash.
func (h *HeaderSyncFSM) PlanRange(id RangeID, startHeight,
	stopHeight uint32) (HeaderRange, bool, error) {

	if id == NoRange {
		return HeaderRange{}, false, fmt.Errorf("%w: zero range id",
			ErrInvalidRange)
	}
	if _, ok := h.ranges[id]; ok {
		return HeaderRange{}, false, fmt.Errorf("%w: %d",
			ErrDuplicateRange, id)
	}
	if startHeight >= stopHeight {
		return HeaderRange{}, false, fmt.Errorf("%w: start %d stop %d",
			ErrInvalidRange, startHeight, stopHeight)
	}
	if !h.CanPlanRange(startHeight, stopHeight) {
		return HeaderRange{}, false, nil
	}
	if h.rangeOverlaps(startHeight, stopHeight) {
		return HeaderRange{}, false, fmt.Errorf(
			"%w: overlapping range %d-%d", ErrInvalidRange,
			startHeight, stopHeight,
		)
	}

	startAnchor := h.anchors[startHeight]
	stopAnchor := h.anchors[stopHeight]
	rng := HeaderRange{
		ID:          id,
		StartHeight: startHeight,
		StartHash:   startAnchor.Hash,
		StopHeight:  stopHeight,
		StopHash:    stopAnchor.Hash,
		State:       RangeQueued,
	}

	h.ranges[id] = rng
	h.rangeKeys = append(h.rangeKeys, id)
	h.metrics.RangesPlanned++

	return rng, true, nil
}

// StageDiscoveredRange creates staged work from a confirmed anchor-discovery
// response. Discovery has already fetched the headers for this span, so the
// caller can skip a second getheaders round trip and let ordered commit drain
// the range when it becomes contiguous.
func (h *HeaderSyncFSM) StageDiscoveredRange(id RangeID, peerID string,
	startHeight, stopHeight uint32, now time.Time) (HeaderRange, bool, error) {

	if existing, ok := h.rangeBySpan(startHeight, stopHeight); ok {
		switch existing.State {
		case RangeStaged, RangeCommitted:
			return existing, false, nil

		case RangeQueued, RangeOwned, RangeActive:
			return h.stageRangeFromDiscovery(existing, peerID, now),
				true, nil
		}
	}

	rng, ok, err := h.PlanRange(id, startHeight, stopHeight)
	if err != nil || !ok {
		return rng, ok, err
	}

	return h.stageRangeFromDiscovery(rng, peerID, now), true, nil
}

func (h *HeaderSyncFSM) stageRangeFromDiscovery(rng HeaderRange,
	peerID string, now time.Time) HeaderRange {

	h.detachRangeFromPeers(rng.ID)

	rng.LeaseEpoch++
	rng.OwnerPeer = peerID
	rng.State = RangeStaged
	rng.Valid = true
	rng.StagedAt = now

	h.ranges[rng.ID] = rng
	h.metrics.RangesStaged++

	return rng
}

func (h *HeaderSyncFSM) detachRangeFromPeers(rangeID RangeID) {
	for id, peer := range h.peers {
		peer.LocalRanges = removeRangeID(peer.LocalRanges, rangeID)
		if peer.ActiveRange == rangeID {
			peer.ActiveRange = NoRange
			if peer.State == PeerBusy {
				peer.State = PeerReady
			}
		}

		h.peers[id] = peer
	}
}

func (h *HeaderSyncFSM) rangeOverlaps(startHeight,
	stopHeight uint32) bool {

	for _, rng := range h.ranges {
		if rng.State == RangeFailed || rng.State == RangeStale {
			continue
		}
		if startHeight < rng.StopHeight && stopHeight > rng.StartHeight {
			return true
		}
	}

	return false
}

// AssignNextQueuedRange moves the earliest queued range into the selected
// peer's bounded local range queue.
func (h *HeaderSyncFSM) AssignNextQueuedRange() (RangeAssignment, bool) {
	owner, ok := h.selectRangeOwner()
	if !ok {
		h.metrics.FailedAssignments++
		return RangeAssignment{}, false
	}

	rangeID, ok := h.nextQueuedRange()
	if !ok {
		h.metrics.FailedAssignments++
		return RangeAssignment{}, false
	}

	assignment, ok, err := h.AssignQueuedRangeToPeer(rangeID, owner.ID)
	if err != nil || !ok {
		return RangeAssignment{}, false
	}

	return assignment, true
}

// AssignQueuedRangeToPeer moves a specific queued range into a specific peer's
// bounded local range queue. This is used when a shared dispatch actor has
// already made the peer selection.
func (h *HeaderSyncFSM) AssignQueuedRangeToPeer(rangeID RangeID,
	peerID string) (RangeAssignment, bool, error) {

	owner, ok := h.peers[peerID]
	if !ok {
		return RangeAssignment{}, false, fmt.Errorf("%w: %s",
			ErrUnknownPeer, peerID)
	}
	if !h.peerCanOwnRange(owner) {
		h.metrics.FailedAssignments++
		return RangeAssignment{}, false, nil
	}

	rng, ok := h.ranges[rangeID]
	if !ok {
		return RangeAssignment{}, false, fmt.Errorf("%w: %d",
			ErrUnknownRange, rangeID)
	}
	if rng.State != RangeQueued {
		h.metrics.FailedAssignments++
		return RangeAssignment{}, false, nil
	}

	rng.OwnerPeer = owner.ID
	rng.LeaseEpoch++
	rng.State = RangeOwned
	rng.Valid = false
	rng.StagedAt = time.Time{}

	owner.LocalRanges = append(owner.LocalRanges, rng.ID)

	h.ranges[rng.ID] = rng
	h.peers[owner.ID] = owner
	h.metrics.RangesAssigned++

	return assignmentFromRange(rng), true, nil
}

func (h *HeaderSyncFSM) nextQueuedRange() (RangeID, bool) {
	var (
		best  HeaderRange
		found bool
	)

	for _, rng := range h.ranges {
		if rng.State != RangeQueued {
			continue
		}
		if !found || rng.StartHeight < best.StartHeight ||
			(rng.StartHeight == best.StartHeight && rng.ID < best.ID) {

			best = rng
			found = true
		}
	}

	if !found {
		return NoRange, false
	}

	return best.ID, true
}

func (h *HeaderSyncFSM) selectRangeOwner() (PeerSnapshot, bool) {
	var (
		best  PeerSnapshot
		found bool
	)

	for _, peer := range h.peers {
		if !h.peerCanOwnRange(peer) {
			continue
		}

		if !found || h.localOwnerOrdersBefore(peer, best) {
			best = peer
			found = true
		}
	}

	return best.clone(), found
}

func (h *HeaderSyncFSM) peerCanOwnRange(peer PeerSnapshot) bool {
	if peer.State != PeerReady && peer.State != PeerBusy {
		return false
	}

	return h.peerOutstandingRanges(peer) < h.cfg.MaxPeerOutstandingRanges
}

func (h *HeaderSyncFSM) peerOutstandingRanges(peer PeerSnapshot) int {
	count := len(peer.LocalRanges)
	if peer.ActiveRange != NoRange {
		count++
	}

	return count
}

func (h *HeaderSyncFSM) localOwnerOrdersBefore(peer,
	candidate PeerSnapshot) bool {

	peerLoad := h.peerOutstandingRanges(peer)
	candidateLoad := h.peerOutstandingRanges(candidate)
	if peerLoad != candidateLoad {
		return peerLoad < candidateLoad
	}

	return peerOrdersBefore(peer, candidate)
}

func peerOrdersBefore(peer, candidate PeerSnapshot) bool {
	if peer.Rank != candidate.Rank {
		return peer.Rank < candidate.Rank
	}
	if peer.RTT != candidate.RTT {
		return peer.RTT < candidate.RTT
	}

	return peer.ID < candidate.ID
}

// StartPeerRange moves the next locally owned range into the peer active slot.
func (h *HeaderSyncFSM) StartPeerRange(peerID string) (
	RangeAssignment, bool, error) {

	peer, ok := h.peers[peerID]
	if !ok {
		return RangeAssignment{}, false, fmt.Errorf("%w: %s",
			ErrUnknownPeer, peerID)
	}
	if !peerCanStartRange(peer) {
		return RangeAssignment{}, false, nil
	}

	rangeID := peer.LocalRanges[0]
	rng, ok := h.ranges[rangeID]
	if !ok {
		return RangeAssignment{}, false, fmt.Errorf("%w: %d",
			ErrUnknownRange, rangeID)
	}
	if rng.State != RangeOwned || rng.OwnerPeer != peerID {
		return RangeAssignment{}, false, nil
	}

	peer.LocalRanges = dropFirstRangeID(peer.LocalRanges)
	peer.ActiveRange = rangeID
	peer.State = PeerBusy

	rng.State = RangeActive

	h.peers[peerID] = peer
	h.ranges[rangeID] = rng
	h.metrics.RangesStarted++

	return assignmentFromRange(rng), true, nil
}

func peerCanStartRange(peer PeerSnapshot) bool {
	return peer.State == PeerReady && peer.ActiveRange == NoRange &&
		len(peer.LocalRanges) > 0
}

// StealRange transfers an owned or active range lease to another peer. Timeout
// and donor-failure policy lives above this pure transition; this method
// enforces the safety rule that stealing increments the lease epoch.
func (h *HeaderSyncFSM) StealRange(thiefID string,
	rangeID RangeID) (StealResult, bool, error) {

	return h.stealRange(thiefID, rangeID, StealReasonExplicit)
}

func (h *HeaderSyncFSM) stealRange(thiefID string, rangeID RangeID,
	reason StealReason) (StealResult, bool, error) {

	thief, ok := h.peers[thiefID]
	if !ok {
		return StealResult{}, false, fmt.Errorf("%w: %s",
			ErrUnknownPeer, thiefID)
	}
	rng, ok := h.ranges[rangeID]
	if !ok {
		return StealResult{}, false, fmt.Errorf("%w: %d",
			ErrUnknownRange, rangeID)
	}
	if !h.peerCanOwnRange(thief) || rng.OwnerPeer == NoPeer ||
		rng.OwnerPeer == thiefID || (rng.State != RangeOwned &&
		rng.State != RangeActive) {

		h.metrics.FailedStealActions++
		return StealResult{}, false, nil
	}

	donor, ok := h.peers[rng.OwnerPeer]
	if !ok {
		return StealResult{}, false, fmt.Errorf("%w: %s",
			ErrUnknownPeer, rng.OwnerPeer)
	}

	oldLeaseEpoch := rng.LeaseEpoch
	donor.LocalRanges = removeRangeID(donor.LocalRanges, rangeID)
	if donor.ActiveRange == rangeID {
		donor.ActiveRange = NoRange
		if donor.State == PeerBusy {
			donor.State = PeerReady
		}
	}

	thief.LocalRanges = append(thief.LocalRanges, rangeID)
	rng.LeaseEpoch++
	rng.OwnerPeer = thiefID
	rng.State = RangeOwned
	rng.Valid = false
	rng.StagedAt = time.Time{}

	h.peers[donor.ID] = donor
	h.peers[thief.ID] = thief
	h.ranges[rangeID] = rng
	h.metrics.RangesStolen++

	return StealResult{
		ThiefID:       thief.ID,
		DonorID:       donor.ID,
		RangeID:       rangeID,
		OldLeaseEpoch: oldLeaseEpoch,
		LeaseEpoch:    rng.LeaseEpoch,
		Reason:        reason,
	}, true, nil
}

// StealNextRange selects one stealable range for the thief. It only steals
// active ranges from blocked or quarantined donors; active work from a merely
// busy peer requires an explicit timeout decision by the caller through
// StealRange.
func (h *HeaderSyncFSM) StealNextRange(thiefID string) (
	StealResult, bool, error) {

	thief, ok := h.peers[thiefID]
	if !ok {
		return StealResult{}, false, fmt.Errorf("%w: %s",
			ErrUnknownPeer, thiefID)
	}
	if !h.peerCanOwnRange(thief) {
		h.metrics.FailedStealActions++
		return StealResult{}, false, nil
	}

	rangeID, reason, ok := h.selectStealableRange(thiefID)
	if !ok {
		h.metrics.FailedStealActions++
		return StealResult{}, false, nil
	}

	return h.stealRange(thiefID, rangeID, reason)
}

func (h *HeaderSyncFSM) selectStealableRange(thiefID string) (
	RangeID, StealReason, bool) {

	var (
		best      HeaderRange
		bestDonor PeerSnapshot
		found     bool
	)

	for _, rng := range h.ranges {
		if rng.OwnerPeer == NoPeer || rng.OwnerPeer == thiefID {
			continue
		}

		donor, ok := h.peers[rng.OwnerPeer]
		if !ok || !h.rangeCanBeAutoStolen(rng, donor) {
			continue
		}

		if !found || h.stealCandidateOrdersBefore(
			rng, donor, best, bestDonor,
		) {
			best = rng
			bestDonor = donor
			found = true
		}
	}

	if !found {
		return NoRange, "", false
	}

	return best.ID, stealReasonForCandidate(best, bestDonor), true
}

func stealReasonForCandidate(rng HeaderRange,
	donor PeerSnapshot) StealReason {

	switch rng.State {
	case RangeActive:
		if donor.State == PeerQuarantined {
			return StealReasonActiveQuarantined
		}

		return StealReasonActiveBlocked

	case RangeOwned:
		switch donor.State {
		case PeerBlocked:
			return StealReasonQueuedBlocked

		case PeerQuarantined:
			return StealReasonQueuedQuarantined

		default:
			return StealReasonQueuedOverloaded
		}

	default:
		return StealReasonExplicit
	}
}

func (h *HeaderSyncFSM) rangeCanBeAutoStolen(rng HeaderRange,
	donor PeerSnapshot) bool {

	switch rng.State {
	case RangeOwned:
		switch donor.State {
		case PeerBlocked, PeerQuarantined:
			return true

		case PeerReady, PeerBusy:
			return len(donor.LocalRanges) > 1

		default:
			return false
		}

	case RangeActive:
		return donor.State == PeerBlocked ||
			donor.State == PeerQuarantined

	default:
		return false
	}
}

func (h *HeaderSyncFSM) stealCandidateOrdersBefore(rng HeaderRange,
	donor PeerSnapshot, candidate HeaderRange,
	candidateDonor PeerSnapshot) bool {

	donorPriority := stealDonorPriority(donor)
	candidatePriority := stealDonorPriority(candidateDonor)
	if donorPriority != candidatePriority {
		return donorPriority > candidatePriority
	}

	donorLoad := h.peerOutstandingRanges(donor)
	candidateLoad := h.peerOutstandingRanges(candidateDonor)
	if donorLoad != candidateLoad {
		return donorLoad > candidateLoad
	}

	if donor.Rank != candidateDonor.Rank {
		return donor.Rank > candidateDonor.Rank
	}

	if rng.StartHeight != candidate.StartHeight {
		return rng.StartHeight < candidate.StartHeight
	}

	return rng.ID < candidate.ID
}

func stealDonorPriority(peer PeerSnapshot) int {
	switch peer.State {
	case PeerBlocked, PeerQuarantined:
		return 3

	case PeerReady, PeerBusy:
		return 2

	default:
		return 0
	}
}

// CompleteRange records an active range response.
func (h *HeaderSyncFSM) CompleteRange(peerID string, rangeID RangeID,
	leaseEpoch uint64, valid bool) (CompleteResult, error) {

	return h.CompleteRangeAt(peerID, rangeID, leaseEpoch, valid, time.Now())
}

// CompleteRangeAt records an active range response with an explicit timestamp
// so deterministic tests can inspect staged-range age.
func (h *HeaderSyncFSM) CompleteRangeAt(peerID string, rangeID RangeID,
	leaseEpoch uint64, valid bool, now time.Time) (CompleteResult, error) {

	rng, ok := h.ranges[rangeID]
	if !ok {
		return CompleteResult{}, fmt.Errorf("%w: %d",
			ErrUnknownRange, rangeID)
	}

	if rng.OwnerPeer != peerID || rng.LeaseEpoch != leaseEpoch ||
		rng.State != RangeActive {

		h.metrics.StaleCompletions++
		return CompleteResult{
			RangeID:    rangeID,
			Stale:      true,
			RangeState: rng.State,
		}, nil
	}

	peer, ok := h.peers[peerID]
	if !ok {
		return CompleteResult{}, fmt.Errorf("%w: %s",
			ErrUnknownPeer, peerID)
	}

	if peer.ActiveRange == rangeID {
		peer.ActiveRange = NoRange
		if peer.State == PeerBusy {
			peer.State = PeerReady
		}
	}

	if valid {
		rng.State = RangeStaged
		rng.Valid = true
		rng.StagedAt = now
		h.metrics.RangesStaged++
	} else {
		rng.State = RangeFailed
		rng.Valid = false
		rng.StagedAt = time.Time{}
		h.metrics.RangesFailed++
	}

	h.peers[peerID] = peer
	h.ranges[rangeID] = rng

	return CompleteResult{
		RangeID:    rangeID,
		Accepted:   true,
		Valid:      valid,
		RangeState: rng.State,
	}, nil
}

// CommitReadyRanges drains all staged ranges that connect to the visible tip.
func (h *HeaderSyncFSM) CommitReadyRanges() CommitResult {
	result := CommitResult{
		TipHeight: h.committedTip,
		TipHash:   h.committedHash,
	}

	for {
		next, ok := h.nextCommittableRange()
		if !ok {
			return result
		}

		next.State = RangeCommitted
		next.Valid = true
		next.StagedAt = time.Time{}

		h.ranges[next.ID] = next
		h.committedTip = next.StopHeight
		h.committedHash = next.StopHash
		h.commitLog = append(h.commitLog, next.ID)
		h.metrics.RangesCommitted++

		result.Committed = append(result.Committed, next.ID)
		result.TipHeight = h.committedTip
		result.TipHash = h.committedHash
	}
}

// ReadyRanges returns the staged ranges that can be committed without mutating
// the FSM. Callers that need to persist side effects before advancing visible
// header state can use this as a preflight before CommitReadyRanges.
func (h *HeaderSyncFSM) ReadyRanges() []RangeID {
	var (
		tipHeight = h.committedTip
		tipHash   = h.committedHash
		ready     []RangeID
	)

	for {
		next, ok := h.nextCommittableRangeFrom(tipHeight, tipHash)
		if !ok {
			return ready
		}

		ready = append(ready, next.ID)
		tipHeight = next.StopHeight
		tipHash = next.StopHash
	}
}

func (h *HeaderSyncFSM) nextCommittableRange() (HeaderRange, bool) {
	return h.nextCommittableRangeFrom(h.committedTip, h.committedHash)
}

func (h *HeaderSyncFSM) nextCommittableRangeFrom(tipHeight uint32,
	tipHash Hash) (HeaderRange, bool) {

	var (
		best  HeaderRange
		found bool
	)

	for _, rng := range h.ranges {
		if rng.State != RangeStaged || !rng.Valid ||
			rng.StartHeight != tipHeight ||
			rng.StartHash != tipHash {

			continue
		}

		if !found || rng.StopHeight < best.StopHeight ||
			(rng.StopHeight == best.StopHeight && rng.ID < best.ID) {

			best = rng
			found = true
		}
	}

	return best, found
}

// CommitStats returns staged-range pressure relative to the current visible tip.
func (h *HeaderSyncFSM) CommitStats(now time.Time) CommitStats {
	var stats CommitStats
	var oldest time.Time

	for _, rng := range h.ranges {
		if rng.State != RangeStaged || !rng.Valid {
			continue
		}

		stats.StagedCount++
		if rng.StopHeight > h.committedTip &&
			rng.StopHeight-h.committedTip > stats.CommitLag {

			stats.CommitLag = rng.StopHeight - h.committedTip
		}
		if !rng.StagedAt.IsZero() &&
			(oldest.IsZero() || rng.StagedAt.Before(oldest)) {

			oldest = rng.StagedAt
		}
	}

	if !oldest.IsZero() && now.After(oldest) {
		stats.OldestStagedAge = now.Sub(oldest)
	}

	return stats
}

func assignmentFromRange(rng HeaderRange) RangeAssignment {
	return RangeAssignment{
		PeerID:      rng.OwnerPeer,
		RangeID:     rng.ID,
		LeaseEpoch:  rng.LeaseEpoch,
		StartHeight: rng.StartHeight,
		StopHeight:  rng.StopHeight,
		StartHash:   rng.StartHash,
		StopHash:    rng.StopHash,
	}
}

func dropFirstRangeID(ids []RangeID) []RangeID {
	if len(ids) <= 1 {
		return nil
	}

	return append([]RangeID(nil), ids[1:]...)
}

func removeRangeID(ids []RangeID, id RangeID) []RangeID {
	result := make([]RangeID, 0, len(ids))
	for _, existing := range ids {
		if existing != id {
			result = append(result, existing)
		}
	}

	return result
}
