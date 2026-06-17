package headersync

import (
	"time"

	"github.com/btcsuite/btcd/wire"
)

// RangeRequest tracks an in-flight getheaders request for a leased header
// range. The timer remains owned here so callers can stop and replace requests
// without leaking timeout callbacks.
type RangeRequest struct {
	Assignment RangeAssignment
	RequestID  uint64
	Timer      *time.Timer
}

// AnchorRequest tracks an in-flight anchor-discovery getheaders request. Anchor
// discovery is separate from range download because long checkpoint spans must
// first learn trustworthy intermediate boundary hashes.
type AnchorRequest struct {
	StartHeight uint32
	StartHash   Hash
	StopHeight  uint32
	StopHash    Hash
	RequestID   uint64
	Timer       *time.Timer
}

// StaleRequest remembers a timed-out range request long enough to classify a
// late response as stale instead of handing it to the legacy serial path.
type StaleRequest struct {
	Assignment RangeAssignment
	ExpiresAt  time.Time
}

// StagedRange stores a completed-but-not-yet-committed range. Parallel peers
// can complete ranges out of order, so completed headers are staged until the
// manager marks the next contiguous range ready to commit.
type StagedRange struct {
	Headers []*wire.BlockHeader
	PeerID  string
}

// Session owns production header-sync request bookkeeping that is not chain
// storage state: in-flight range requests, anchor discovery requests, stale
// completions, staged headers, and deterministic range ID allocation.
type Session struct {
	rangeRequests  map[string]RangeRequest
	anchorRequests map[string]AnchorRequest
	staleRequests  map[string]StaleRequest
	stagedRanges   map[RangeID]StagedRange
	cooldowns      map[string]time.Time
	nextRangeID    RangeID
}

// NewSession creates empty request state with range IDs starting at one. Range
// zero is reserved as NoRange.
func NewSession() *Session {
	s := &Session{}
	s.Reset()

	return s
}

// Reset stops outstanding timers and clears all request/staging state.
func (s *Session) Reset() {
	s.StopAll()

	s.rangeRequests = make(map[string]RangeRequest)
	s.anchorRequests = make(map[string]AnchorRequest)
	s.staleRequests = make(map[string]StaleRequest)
	s.stagedRanges = make(map[RangeID]StagedRange)
	if s.cooldowns == nil {
		s.cooldowns = make(map[string]time.Time)
	}
	s.nextRangeID = 1
}

// StopAll stops outstanding timers and clears in-flight request maps. It keeps
// the Session object reusable by Reset or future Track calls.
func (s *Session) StopAll() {
	if s == nil {
		return
	}

	for peerID, request := range s.rangeRequests {
		stopTimer(request.Timer)
		delete(s.rangeRequests, peerID)
	}
	for peerID, request := range s.anchorRequests {
		stopTimer(request.Timer)
		delete(s.anchorRequests, peerID)
	}
	for peerID := range s.staleRequests {
		delete(s.staleRequests, peerID)
	}
}

// NextRangeID returns the next deterministic range ID to pass into the FSM.
func (s *Session) NextRangeID() RangeID {
	return s.nextRangeID
}

// SetNextRangeID records the next range ID after a planning batch.
func (s *Session) SetNextRangeID(next RangeID) {
	s.nextRangeID = next
}

// AllocateRangeID reserves and returns one range ID.
func (s *Session) AllocateRangeID() RangeID {
	id := s.nextRangeID
	s.nextRangeID++

	return id
}

// TrackRange records an in-flight range request for a peer.
func (s *Session) TrackRange(peerID string, request RangeRequest) {
	if existing, ok := s.rangeRequests[peerID]; ok {
		stopTimer(existing.Timer)
	}

	delete(s.staleRequests, peerID)
	s.rangeRequests[peerID] = request
}

// FinishRange removes and stops an in-flight range request.
func (s *Session) FinishRange(peerID string) (RangeRequest, bool) {
	request, ok := s.rangeRequests[peerID]
	if !ok {
		return RangeRequest{}, false
	}

	stopTimer(request.Timer)
	delete(s.rangeRequests, peerID)

	return request, true
}

// TimeoutRange removes an in-flight range request only if the timeout still
// matches the active request identity.
func (s *Session) TimeoutRange(peerID string, requestID uint64,
	rangeID RangeID, leaseEpoch uint64) (RangeRequest, bool) {

	request, ok := s.rangeRequests[peerID]
	if !ok || request.RequestID != requestID ||
		request.Assignment.RangeID != rangeID ||
		request.Assignment.LeaseEpoch != leaseEpoch {

		return RangeRequest{}, false
	}

	delete(s.rangeRequests, peerID)

	return request, true
}

// RangeActive returns true when a peer already has an in-flight range request.
func (s *Session) RangeActive(peerID string) bool {
	_, ok := s.rangeRequests[peerID]
	return ok
}

// ActiveRangeCount returns the number of active range requests.
func (s *Session) ActiveRangeCount() int {
	return len(s.rangeRequests)
}

// MarkStale records a timed-out request so a late response can be classified.
func (s *Session) MarkStale(peerID string, request StaleRequest) {
	s.staleRequests[peerID] = request
}

// PopStale removes a stale request for a peer.
func (s *Session) PopStale(peerID string) (StaleRequest, bool) {
	request, ok := s.staleRequests[peerID]
	if !ok {
		return StaleRequest{}, false
	}

	delete(s.staleRequests, peerID)

	return request, true
}

// ClearStale removes any stale response tracking for a peer.
func (s *Session) ClearStale(peerID string) {
	delete(s.staleRequests, peerID)
}

// TrackAnchor records an in-flight anchor-discovery request for a peer.
func (s *Session) TrackAnchor(peerID string, request AnchorRequest) {
	if existing, ok := s.anchorRequests[peerID]; ok {
		stopTimer(existing.Timer)
	}

	s.anchorRequests[peerID] = request
}

// FinishAnchor removes and stops an in-flight anchor request.
func (s *Session) FinishAnchor(peerID string) (AnchorRequest, bool) {
	request, ok := s.anchorRequests[peerID]
	if !ok {
		return AnchorRequest{}, false
	}

	stopTimer(request.Timer)
	delete(s.anchorRequests, peerID)

	return request, true
}

// TimeoutAnchor removes an in-flight anchor request only if the timeout still
// matches the active request identity.
func (s *Session) TimeoutAnchor(peerID string, requestID uint64,
	startHeight uint32) (AnchorRequest, bool) {

	request, ok := s.anchorRequests[peerID]
	if !ok || request.RequestID != requestID ||
		request.StartHeight != startHeight {

		return AnchorRequest{}, false
	}

	delete(s.anchorRequests, peerID)

	return request, true
}

// AnchorActive returns true when a peer already has an in-flight anchor
// discovery request.
func (s *Session) AnchorActive(peerID string) bool {
	_, ok := s.anchorRequests[peerID]
	return ok
}

// ActiveAnchorCount returns the number of active anchor-discovery requests.
func (s *Session) ActiveAnchorCount() int {
	return len(s.anchorRequests)
}

// PruneAnchorsAtOrBefore drops in-flight anchor-discovery requests whose stop
// height is already committed. Hedged discovery can leave duplicate requests for
// an old span after the first peer commits it; those completions should not keep
// the production scheduler waiting on obsolete work.
func (s *Session) PruneAnchorsAtOrBefore(stopHeight uint32) int {
	var pruned int
	for peerID, request := range s.anchorRequests {
		if request.StopHeight > stopHeight {
			continue
		}

		stopTimer(request.Timer)
		delete(s.anchorRequests, peerID)
		pruned++
	}

	return pruned
}

// MatchingActiveAnchors returns the number of active anchor requests for the
// same start/stop span.
func (s *Session) MatchingActiveAnchors(startHeight uint32, startHash Hash,
	stopHeight uint32) int {

	var active int
	for _, request := range s.anchorRequests {
		if request.StartHeight == startHeight &&
			request.StopHeight == stopHeight &&
			request.StartHash == startHash {

			active++
		}
	}

	return active
}

// HasActiveWork returns true if there are in-flight range or anchor requests.
func (s *Session) HasActiveWork() bool {
	return len(s.rangeRequests) > 0 || len(s.anchorRequests) > 0
}

// StageRange stores a completed range until contiguous commit can drain it.
func (s *Session) StageRange(rangeID RangeID, headers []*wire.BlockHeader,
	peerID string) {

	s.stagedRanges[rangeID] = StagedRange{
		Headers: append([]*wire.BlockHeader(nil), headers...),
		PeerID:  peerID,
	}
}

// StagedRange returns a staged range by ID.
func (s *Session) StagedRange(rangeID RangeID) (StagedRange, bool) {
	staged, ok := s.stagedRanges[rangeID]
	return staged, ok
}

// DeleteStagedRange removes a committed staged range.
func (s *Session) DeleteStagedRange(rangeID RangeID) {
	delete(s.stagedRanges, rangeID)
}

// TrackPeerCooldown updates the peer cooldown table based on a scheduling state
// transition. Blocked and quarantined peers are held out of assignment until the
// cooldown expires; ready peers clear any existing cooldown.
func (s *Session) TrackPeerCooldown(peerID string, state PeerState,
	now time.Time, cooldown time.Duration) (time.Time, bool) {

	if s.cooldowns == nil {
		s.cooldowns = make(map[string]time.Time)
	}

	switch state {
	case PeerBlocked, PeerQuarantined:
		until := now.Add(cooldown)
		s.cooldowns[peerID] = until

		return until, true

	case PeerReady:
		delete(s.cooldowns, peerID)
	}

	return time.Time{}, false
}

// CooldownUntil returns a peer's cooldown expiry when it is still active.
func (s *Session) CooldownUntil(peerID string,
	now time.Time) (time.Time, bool) {

	until, ok := s.cooldowns[peerID]
	if !ok || !now.Before(until) {
		return time.Time{}, false
	}

	return until, true
}

// CooldownExpired returns true when a peer has a tracked cooldown that has
// elapsed and should be moved back to ready if it has no active request.
func (s *Session) CooldownExpired(peerID string, now time.Time) (time.Time,
	bool) {

	until, ok := s.cooldowns[peerID]
	if !ok || now.Before(until) {
		return time.Time{}, false
	}

	return until, true
}

// CooldownCount returns the number of peers with tracked cooldown state.
func (s *Session) CooldownCount() int {
	return len(s.cooldowns)
}

func stopTimer(timer *time.Timer) {
	if timer != nil {
		timer.Stop()
	}
}
