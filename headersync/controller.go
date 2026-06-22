package headersync

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/btcsuite/btcd/wire"
)

// ChainPoint identifies a known header boundary.
type ChainPoint struct {
	Height uint32
	Hash   Hash
}

// CheckpointRangePlan describes the result of one checkpoint-backed planning
// pass. Ranges may be empty because the span is already planned, the peer set
// is not high enough yet, or intermediate anchors are still needed.
type CheckpointRangePlan struct {
	Ranges            []HeaderRange
	WaitingForAnchors bool
	Span              uint32
	MaxRangeHeaders   uint32
}

// PeerCandidate is the controller's peer-selection view. Production callers
// keep concrete peer I/O outside headersync and pass this narrow snapshot in.
type PeerCandidate struct {
	ID     string
	Height int32
	Rank   int
	RTT    time.Duration
}

// AnchorRequestPlan describes which peers should receive an anchor-discovery
// getheaders request for a span.
type AnchorRequestPlan struct {
	Peers         []PeerCandidate
	ActivePeers   int
	EligiblePeers int
	RequiredPeers int
	Deferred      bool
}

// Active returns true when discovery is either already in flight or new peer
// requests should be sent.
func (a AnchorRequestPlan) Active() bool {
	return a.Deferred || len(a.Peers) > 0
}

// ReplanResult records a replacement range planned after a failed response.
type ReplanResult struct {
	Range HeaderRange
	ID    RangeID
	OK    bool
}

// CommitReadyResult reports the FSM commit plus production session cleanup
// performed after the visible header tip advances.
type CommitReadyResult struct {
	Commit        CommitResult
	PrunedAnchors int
}

// AnchorHeadersResult reports how one anchor-discovery response was handled.
type AnchorHeadersResult struct {
	Request  AnchorRequest
	Response AnchorResponse
	Anchor   Anchor
	Range    HeaderRange

	Confirmed bool
	Rejected  bool
	Staged    bool
	Reason    string
	Err       error
}

// Controller owns production header-sync orchestration policy that is
// independent of blockmanager's storage and peer I/O. It composes the actor FSM
// with request/session bookkeeping.
type Controller struct {
	manager *ManagerActor
	session *Session
	cfg     Config
}

// NewController creates a production controller around the manager actor and
// session. The caller remains responsible for starting the manager actor.
func NewController(manager *ManagerActor, session *Session,
	cfg Config) *Controller {

	if session == nil {
		session = NewSession()
	}

	return &Controller{
		manager: manager,
		session: session,
		cfg:     cfg.normalize(),
	}
}

// Session returns the request bookkeeping state owned by the controller.
func (c *Controller) Session() *Session {
	return c.session
}

// PlanCheckpointRanges seeds the current tip and stop checkpoint as trusted
// anchors, then plans all adjacent confirmed ranges between them. It does not
// invent missing boundary hashes; callers should start anchor discovery when
// WaitingForAnchors is true.
func (c *Controller) PlanCheckpointRanges(ctx context.Context,
	tip, stop ChainPoint, bestHeight uint32) (CheckpointRangePlan, error) {

	if c == nil || c.manager == nil {
		return CheckpointRangePlan{}, nil
	}
	if tip.Height >= stop.Height || bestHeight < stop.Height {
		return CheckpointRangePlan{}, nil
	}

	if _, err := c.manager.AdvanceCommittedTip(
		ctx, tip.Height, tip.Hash,
	); err != nil {
		return CheckpointRangePlan{}, err
	}

	if _, ok, err := c.manager.AddAnchor(ctx, stop.Height, stop.Hash,
		true); err != nil {

		return CheckpointRangePlan{}, err
	} else if !ok {
		return CheckpointRangePlan{}, nil
	}

	result, err := c.manager.PlanConfirmedAnchorRanges(
		ctx, c.session.NextRangeID(), tip.Height, stop.Height,
	)
	if err != nil {
		return CheckpointRangePlan{}, err
	}
	c.session.SetNextRangeID(result.NextID)

	span := stop.Height - tip.Height
	return CheckpointRangePlan{
		Ranges:            result.Ranges,
		WaitingForAnchors: len(result.Ranges) == 0 && span > c.cfg.MaxRangeHeaders,
		Span:              span,
		MaxRangeHeaders:   c.cfg.MaxRangeHeaders,
	}, nil
}

// PlanFrontierRanges plans any already-known confirmed frontier spans after
// the current persisted block-header tip. This is the recovery path for
// discovery that learned anchors out of order: missing spans can be fetched as
// normal range work while later staged ranges wait for ordered commit.
func (c *Controller) PlanFrontierRanges(ctx context.Context,
	tip ChainPoint, bestHeight uint32) (PlanRangesResult, error) {

	if c == nil || c.manager == nil {
		return PlanRangesResult{}, nil
	}
	if tip.Height >= bestHeight {
		return PlanRangesResult{}, nil
	}

	if _, err := c.manager.AdvanceCommittedTip(
		ctx, tip.Height, tip.Hash,
	); err != nil {
		return PlanRangesResult{}, err
	}

	result, err := c.manager.PlanConfirmedAnchorRanges(
		ctx, c.session.NextRangeID(), tip.Height, bestHeight,
	)
	if err != nil {
		return result, err
	}
	c.session.SetNextRangeID(result.NextID)

	return result, nil
}

// AnchorDiscoverySpan returns the next span that needs anchor discovery before
// it can be split into bounded parallel getheaders ranges. Discovery advances
// from the furthest confirmed anchor ahead of the committed tip so range
// downloads and future-anchor discovery can be pipelined.
func (c *Controller) AnchorDiscoverySpan(ctx context.Context,
	tip, stop ChainPoint, bestHeight uint32) (AnchorRequest, bool, error) {

	if c == nil || c.manager == nil {
		return AnchorRequest{}, false, nil
	}

	if tip.Height >= stop.Height || bestHeight < stop.Height {
		return AnchorRequest{}, false, nil
	}

	start := tip
	anchors, err := c.manager.Anchors(ctx)
	if err != nil {
		return AnchorRequest{}, false, err
	}
	for _, anchor := range anchors {
		if anchor.Height < tip.Height || anchor.Height >= stop.Height {
			continue
		}
		if !c.AnchorConfirmed(anchor) {
			continue
		}
		if anchor.Height > start.Height {
			start = ChainPoint{
				Height: anchor.Height,
				Hash:   anchor.Hash,
			}
		}
	}

	if stop.Height-start.Height <= c.cfg.MaxRangeHeaders {

		return AnchorRequest{}, false, nil
	}

	return AnchorRequest{
		StartHeight: start.Height,
		StartHash:   start.Hash,
		StopHeight:  stop.Height,
		StopHash:    stop.Hash,
	}, true, nil
}

// FrontierDiscoverySpan returns the next untrusted discovery span after the
// final checkpoint. There is no known stop hash in this lane, so the request
// uses a zero stop hash and bounds the expected response by height only. A
// returned boundary still needs the normal independent confirmation policy
// before it can become staged range work.
func (c *Controller) FrontierDiscoverySpan(ctx context.Context,
	tip ChainPoint, bestHeight uint32) (AnchorRequest, bool, error) {

	if c == nil || c.manager == nil {
		return AnchorRequest{}, false, nil
	}
	if tip.Height >= bestHeight {
		return AnchorRequest{}, false, nil
	}

	start := tip
	anchors, err := c.manager.Anchors(ctx)
	if err != nil {
		return AnchorRequest{}, false, err
	}
	for _, anchor := range anchors {
		if anchor.Height < tip.Height || anchor.Height >= bestHeight {
			continue
		}
		if !c.AnchorConfirmed(anchor) {
			continue
		}
		if anchor.Height > start.Height {
			start = ChainPoint{
				Height: anchor.Height,
				Hash:   anchor.Hash,
			}
		}
	}

	span, ok := c.FrontierLookaheadSpan(start, bestHeight)
	return span, ok, nil
}

// FrontierLookaheadSpan returns the next zero-stop discovery request after a
// confirmed frontier boundary. Production calls this as soon as an anchor
// response has been structurally validated, before the just-downloaded range is
// committed, so network discovery can overlap ordered local commit work.
func (c *Controller) FrontierLookaheadSpan(start ChainPoint,
	bestHeight uint32) (AnchorRequest, bool) {

	if c == nil {
		return AnchorRequest{}, false
	}
	if start.Height >= bestHeight {
		return AnchorRequest{}, false
	}

	stopHeight := bestHeight
	if stopHeight-start.Height > c.cfg.MaxRangeHeaders {
		stopHeight = start.Height + c.cfg.MaxRangeHeaders
	}
	if stopHeight <= start.Height {
		return AnchorRequest{}, false
	}

	return AnchorRequest{
		StartHeight: start.Height,
		StartHash:   start.Hash,
		StopHeight:  stopHeight,
	}, true
}

// PlanAnchorRequests selects peers for an anchor-discovery span while
// respecting active range/anchor requests and excluded peers.
func (c *Controller) PlanAnchorRequests(span AnchorRequest,
	peers []PeerCandidate, excludedPeers ...string) AnchorRequestPlan {

	requiredPeers := c.cfg.AnchorConfirmationsRequired
	requestFanout := c.cfg.AnchorRequestFanout
	excluded := make(map[string]struct{}, len(excludedPeers))
	for _, peerID := range excludedPeers {
		excluded[peerID] = struct{}{}
	}

	activePeers := c.session.MatchingActiveAnchors(
		span.StartHeight, span.StartHash, span.StopHeight,
	)
	if activePeers >= requestFanout {
		return AnchorRequestPlan{
			ActivePeers:   activePeers,
			RequiredPeers: requiredPeers,
			Deferred:      true,
		}
	}

	eligiblePeers := make([]PeerCandidate, 0, len(peers))
	for _, peer := range peers {
		if peer.Height < int32(span.StopHeight) {
			continue
		}
		if _, ok := excluded[peer.ID]; ok {
			continue
		}
		if c.session.RangeActive(peer.ID) ||
			c.session.AnchorActive(peer.ID) {

			continue
		}

		eligiblePeers = append(eligiblePeers, peer)
	}
	sort.SliceStable(eligiblePeers, func(i, j int) bool {
		return peerCandidateOrdersBefore(
			eligiblePeers[i], eligiblePeers[j],
		)
	})

	plan := AnchorRequestPlan{
		ActivePeers:   activePeers,
		EligiblePeers: len(eligiblePeers),
		RequiredPeers: requiredPeers,
	}
	need := requestFanout - activePeers
	if need > len(eligiblePeers) {
		need = len(eligiblePeers)
	}
	if activePeers+len(eligiblePeers) < requestFanout {
		plan.Deferred = activePeers > 0 || need > 0
		plan.Peers = append(plan.Peers, eligiblePeers[:need]...)
		return plan
	}

	plan.Peers = append(plan.Peers, eligiblePeers[:need]...)

	return plan
}

func peerCandidateOrdersBefore(peer, candidate PeerCandidate) bool {
	if peer.Rank != candidate.Rank {
		return peer.Rank < candidate.Rank
	}
	if peerRTTOrdersBefore(peer.RTT, candidate.RTT) {
		return true
	}
	if peerRTTOrdersBefore(candidate.RTT, peer.RTT) {
		return false
	}

	return peer.ID < candidate.ID
}

// AnchorConfirmed returns true when an anchor can be used for range planning.
func (c *Controller) AnchorConfirmed(anchor Anchor) bool {
	return !anchor.Rejected && (anchor.Trusted ||
		anchor.Confirmations >= c.cfg.AnchorConfirmationsRequired)
}

// FinishAnchorHeaders validates an in-flight anchor-discovery response and
// records the discovered boundary in the manager FSM. The caller remains
// responsible for peer I/O and any peer punishment/disconnect policy.
func (c *Controller) FinishAnchorHeaders(ctx context.Context,
	peerID string, headers []*wire.BlockHeader,
	maxHeaders uint32) (AnchorHeadersResult, bool, error) {

	if c == nil || c.manager == nil || c.session == nil {
		return AnchorHeadersResult{}, false, nil
	}

	request, ok := c.session.FinishAnchor(peerID)
	if !ok {
		return AnchorHeadersResult{}, false, nil
	}

	result := AnchorHeadersResult{
		Request: request,
	}
	response, err := ValidateAnchorResponse(
		request, headers, maxHeaders,
	)
	if err != nil {
		result.Rejected = true
		result.Reason = err.Error()
		result.Err = err

		return result, true, nil
	}
	result.Response = response

	anchor, ok, err := c.manager.AddAnchor(
		ctx, response.Height, response.Hash, response.Trusted,
	)
	if err != nil {
		return result, true, err
	}

	result.Anchor = anchor
	if !ok || anchor.Rejected {
		result.Rejected = true
		result.Reason = fmt.Sprintf(
			"conflicting anchor height=%d hash=%s",
			response.Height, response.Hash,
		)

		return result, true, nil
	}

	result.Confirmed = c.AnchorConfirmed(anchor)
	if result.Confirmed {
		rangeID := c.session.NextRangeID()
		rng, staged, err := c.manager.StageDiscoveredRange(
			ctx, rangeID, peerID, request.StartHeight,
			response.Height, time.Now(),
		)
		if err != nil {
			return result, true, err
		}
		if staged {
			result.Range = rng
			result.Staged = true
			c.session.SetNextRangeID(rangeID + 1)
		}
	}

	return result, true, nil
}

// AssignQueuedRanges mirrors queued ranges into the shared dispatch scheduler
// until either the queues are filled or no queued range remains.
func (c *Controller) AssignQueuedRanges(ctx context.Context,
	peerCount int) ([]RangeAssignment, error) {

	if c == nil || c.manager == nil || peerCount <= 0 {
		return nil, nil
	}

	maxAssignments := peerCount * c.cfg.MaxPeerOutstandingRanges
	assignments := make([]RangeAssignment, 0, maxAssignments)
	for i := 0; i < maxAssignments; i++ {
		assignment, ok, err := c.manager.AssignNextQueuedRange(ctx)
		if err != nil {
			return assignments, err
		}
		if !ok {
			break
		}

		assignments = append(assignments, assignment)
	}

	return assignments, nil
}

// CommitReadyRanges advances the manager FSM through contiguous staged ranges
// and drops obsolete in-flight anchor requests that can no longer produce new
// work. This keeps late hedged discovery from blocking the next span until its
// timeout after another peer has already committed the same stop height.
func (c *Controller) CommitReadyRanges(ctx context.Context) (
	CommitReadyResult, error) {

	if c == nil || c.manager == nil {
		return CommitReadyResult{}, nil
	}

	commit, err := c.manager.CommitReadyRanges(ctx)
	if err != nil {
		return CommitReadyResult{}, err
	}

	var pruned int
	if c.session != nil && len(commit.Committed) > 0 {
		pruned = c.session.PruneAnchorsAtOrBefore(commit.TipHeight)
	}

	return CommitReadyResult{
		Commit:        commit,
		PrunedAnchors: pruned,
	}, nil
}

// ReplanRange allocates a new range ID and requeues a failed span.
func (c *Controller) ReplanRange(ctx context.Context,
	rng HeaderRange) (ReplanResult, error) {

	if c == nil || c.manager == nil {
		return ReplanResult{}, fmt.Errorf("%w: missing controller",
			ErrInvalidRange)
	}

	retryID := c.session.AllocateRangeID()
	retry, ok, err := c.manager.PlanRange(
		ctx, retryID, rng.StartHeight, rng.StopHeight,
	)
	if err != nil {
		return ReplanResult{ID: retryID}, err
	}

	return ReplanResult{
		Range: retry,
		ID:    retryID,
		OK:    ok,
	}, nil
}
