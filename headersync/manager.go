package headersync

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lightninglabs/neutrino/protofsm"
	"github.com/lightningnetwork/lnd/actor"
	"github.com/lightningnetwork/lnd/fn/v2"
)

// ManagerMsg is accepted by the header sync manager.
type ManagerMsg interface {
	actor.Message

	managerMsg()
}

type managerMessage struct {
	actor.BaseMessage
}

func (managerMessage) managerMsg() {}

func (managerMessage) MessageType() string {
	return "headersync.ManagerMsg"
}

// ManagerResp is returned by the header sync manager.
type ManagerResp interface {
	managerResp()
}

type managerResponse struct{}

func (managerResponse) managerResp() {}

// ManagerAck confirms a mutating manager message was applied.
type ManagerAck struct {
	managerResponse
}

// AddAnchorMsg adds or confirms an anchor.
type AddAnchorMsg struct {
	managerMessage

	Height  uint32
	Hash    Hash
	Trusted bool
}

// AddAnchorResp reports the anchor after applying the observation.
type AddAnchorResp struct {
	managerResponse

	Anchor Anchor
	OK     bool
}

// AdvanceCommittedTipMsg moves the manager-visible committed tip to a header
// tip already persisted by the block manager.
type AdvanceCommittedTipMsg struct {
	managerMessage

	Height uint32
	Hash   Hash
}

// AdvanceCommittedTipResp reports whether the committed tip changed.
type AdvanceCommittedTipResp struct {
	managerResponse

	Advanced bool
}

// RequeueStagedRangeMsg returns a staged range whose headers were lost to the
// queue.
type RequeueStagedRangeMsg struct {
	managerMessage

	ID RangeID
}

// RequeueStagedRangeResp reports whether the range was requeued.
type RequeueStagedRangeResp struct {
	managerResponse

	OK bool
}

// AnchorsMsg asks the manager for the currently known anchor set.
type AnchorsMsg struct {
	managerMessage
}

// AnchorsResp reports the current anchor set sorted by height.
type AnchorsResp struct {
	managerResponse

	Anchors []Anchor
}

// PlanRangeMsg asks the manager to create queued range work.
type PlanRangeMsg struct {
	managerMessage

	ID          RangeID
	StartHeight uint32
	StopHeight  uint32
}

// PlanRangeResp reports the planned range, if anchors allowed it.
type PlanRangeResp struct {
	managerResponse

	Range HeaderRange
	OK    bool
}

// StageDiscoveredRangeMsg asks the manager to stage a range using headers
// already returned by anchor discovery.
type StageDiscoveredRangeMsg struct {
	managerMessage

	ID          RangeID
	PeerID      string
	StartHeight uint32
	StopHeight  uint32
	At          time.Time
}

// StageDiscoveredRangeResp reports the staged range, if one was created.
type StageDiscoveredRangeResp struct {
	managerResponse

	Range HeaderRange
	OK    bool
}

// InvalidateRangeMsg reports permanent contextual validation failure.
type InvalidateRangeMsg struct {
	managerMessage

	ID RangeID
}

// InvalidateRangeResp retains invalidated work if dispatch mirroring fails.
type InvalidateRangeResp struct {
	managerResponse

	Result InvalidationResult
	Err    error
}

// RejectAnchorMsg reports that peers contradict an untrusted discovered anchor.
type RejectAnchorMsg struct {
	managerMessage

	Height uint32
	Hash   Hash
}

// RejectAnchorResp retains the rejected work if dispatch mirroring fails.
type RejectAnchorResp struct {
	managerResponse

	Result InvalidationResult
	Err    error
}

// PlanAnchorRangesMsg asks the manager to plan all bounded adjacent ranges
// between confirmed anchors.
type PlanAnchorRangesMsg struct {
	managerMessage

	NextID      RangeID
	StartHeight uint32
	StopHeight  uint32
}

// PlanAnchorRangesResp reports the created anchor-backed ranges.
type PlanAnchorRangesResp struct {
	managerResponse

	Result PlanRangesResult
}

// AddPeerMsg asks the manager to add or replace a peer snapshot.
type AddPeerMsg struct {
	managerMessage

	Peer PeerSnapshot
}

// RemovePeerMsg asks the manager to remove a peer.
type RemovePeerMsg struct {
	managerMessage

	ID string
}

// UpdatePeerStateMsg asks the manager to update peer state.
type UpdatePeerStateMsg struct {
	managerMessage

	ID    string
	State PeerState
}

// UpdatePeerRTTMsg asks the manager to update peer RTT.
type UpdatePeerRTTMsg struct {
	managerMessage

	ID  string
	RTT time.Duration
}

// AssignRangeMsg asks the manager to move queued range work to a peer queue.
type AssignRangeMsg struct {
	managerMessage
}

// StartRangeMsg asks the manager to start the next range for a peer.
type StartRangeMsg struct {
	managerMessage

	PeerID string
}

// RangeAssignmentResp reports a range assignment.
type RangeAssignmentResp struct {
	managerResponse

	Assignment RangeAssignment
	OK         bool
}

// StealRangeMsg asks the manager to steal a specific range, or to select one
// if RangeID is NoRange.
type StealRangeMsg struct {
	managerMessage

	ThiefID string
	RangeID RangeID
}

// StealRangeResp reports a steal result.
type StealRangeResp struct {
	managerResponse

	Result StealResult
	OK     bool
}

// CompleteRangeMsg reports a peer range completion.
type CompleteRangeMsg struct {
	managerMessage

	PeerID     string
	RangeID    RangeID
	LeaseEpoch uint64
	Valid      bool
	At         time.Time
}

// CompleteRangeResp reports how a completion was handled.
type CompleteRangeResp struct {
	managerResponse

	Result CompleteResult
}

// CommitReadyMsg asks the manager to drain staged contiguous ranges.
type CommitReadyMsg struct {
	managerMessage
}

// CommitReadyResp reports committed ranges.
type CommitReadyResp struct {
	managerResponse

	Result CommitResult
}

// ReadyRangesMsg asks which staged ranges can be persisted before committing
// them in the FSM.
type ReadyRangesMsg struct {
	managerMessage
}

// ReadyRangesResp reports staged, contiguous range ids without mutating state.
type ReadyRangesResp struct {
	managerResponse

	RangeIDs []RangeID
}

// SnapshotPeerMsg asks the manager for one peer snapshot.
type SnapshotPeerMsg struct {
	managerMessage

	ID string
}

// SnapshotPeerResp is the response to SnapshotPeerMsg.
type SnapshotPeerResp struct {
	managerResponse

	Peer PeerSnapshot
	OK   bool
}

// SnapshotRangeMsg asks the manager for one range snapshot.
type SnapshotRangeMsg struct {
	managerMessage

	ID RangeID
}

// SnapshotRangeResp is the response to SnapshotRangeMsg.
type SnapshotRangeResp struct {
	managerResponse

	Range HeaderRange
	OK    bool
}

// MetricsMsg asks for current counters.
type MetricsMsg struct {
	managerMessage
}

// MetricsResp is the response to MetricsMsg.
type MetricsResp struct {
	managerResponse

	Metrics Metrics
}

// CommitStatsMsg asks for out-of-order staging pressure.
type CommitStatsMsg struct {
	managerMessage

	Now time.Time
}

// CommitStatsResp is the response to CommitStatsMsg.
type CommitStatsResp struct {
	managerResponse

	Stats CommitStats
}

type managerOutbox interface {
	managerOutbox()
}

type managerOutboxResponse struct {
	Response ManagerResp
}

func (managerOutboxResponse) managerOutbox() {}

type managerEnv struct {
	fsm      *HeaderSyncFSM
	dispatch DispatchController
}

// syncPeers mirrors every peer's FSM state into the dispatch scheduler and
// returns the last error, so one failing peer does not skip the rest.
func (e *managerEnv) syncPeers(ctx context.Context) error {
	if e.dispatch == nil {
		return nil
	}

	var syncErr error
	for _, peer := range e.fsm.Peers() {
		if err := e.dispatch.SyncPeer(ctx, peer); err != nil {
			syncErr = err
		}
	}

	return syncErr
}

type managerState struct{}

func (managerState) String() string {
	return "headersync-manager"
}

func (managerState) IsTerminal() bool {
	return false
}

func (managerState) ProcessEvent(ctx context.Context, msg ManagerMsg,
	env *managerEnv) (*protofsm.StateTransition[
	ManagerMsg, managerOutbox, *managerEnv], error) {

	respond := func(resp ManagerResp) (*protofsm.StateTransition[
		ManagerMsg, managerOutbox, *managerEnv], error) {

		return &protofsm.StateTransition[
			ManagerMsg, managerOutbox, *managerEnv,
		]{
			NextState: managerState{},
			NewEvents: fn.Some(protofsm.EmittedEvent[
				ManagerMsg, managerOutbox,
			]{
				Outbox: []managerOutbox{
					managerOutboxResponse{Response: resp},
				},
			}),
		}, nil
	}

	switch m := msg.(type) {
	case *AddAnchorMsg:
		anchor, ok := env.fsm.AddOrConfirmAnchor(
			m.Height, m.Hash, m.Trusted,
		)

		return respond(&AddAnchorResp{Anchor: anchor, OK: ok})

	case *RequeueStagedRangeMsg:
		ok := env.fsm.RequeueStagedRange(m.ID)

		return respond(&RequeueStagedRangeResp{OK: ok})

	case *AdvanceCommittedTipMsg:
		advanced := env.fsm.AdvanceCommittedTip(m.Height, m.Hash)

		return respond(&AdvanceCommittedTipResp{Advanced: advanced})

	case *AnchorsMsg:
		return respond(&AnchorsResp{Anchors: env.fsm.Anchors()})

	case *PlanRangeMsg:
		rng, ok, err := env.fsm.PlanRange(
			m.ID, m.StartHeight, m.StopHeight,
		)
		if err != nil {
			return nil, err
		}

		return respond(&PlanRangeResp{Range: rng, OK: ok})

	case *StageDiscoveredRangeMsg:
		now := m.At
		if now.IsZero() {
			now = time.Now()
		}
		rng, ok, err := env.fsm.StageDiscoveredRange(
			m.ID, m.PeerID, m.StartHeight, m.StopHeight, now,
		)
		if err != nil {
			return nil, err
		}

		return respond(&StageDiscoveredRangeResp{
			Range: rng,
			OK:    ok,
		})

	case *InvalidateRangeMsg:
		result, err := env.fsm.InvalidateRange(m.ID)
		if err != nil {
			return nil, err
		}
		syncErr := env.syncPeers(ctx)
		return respond(&InvalidateRangeResp{
			Result: result, Err: syncErr,
		})

	case *RejectAnchorMsg:
		result := env.fsm.RejectAnchor(m.Height, m.Hash)
		syncErr := env.syncPeers(ctx)
		return respond(&RejectAnchorResp{
			Result: result, Err: syncErr,
		})

	case *PlanAnchorRangesMsg:
		result, err := env.fsm.PlanConfirmedAnchorRanges(
			m.NextID, m.StartHeight, m.StopHeight,
		)
		if err != nil {
			return nil, err
		}

		return respond(&PlanAnchorRangesResp{Result: result})

	case *AddPeerMsg:
		if err := env.fsm.AddPeer(m.Peer); err != nil {
			return nil, err
		}
		if env.dispatch != nil {
			if err := env.dispatch.AddPeer(
				ctx,
				m.Peer,
			); err != nil {
				return nil, err
			}
		}

		return respond(&ManagerAck{})

	case *RemovePeerMsg:
		env.fsm.RemovePeer(m.ID)
		if env.dispatch != nil {
			if err := env.dispatch.RemovePeer(
				ctx,
				m.ID,
			); err != nil {
				return nil, err
			}
		}

		return respond(&ManagerAck{})

	case *UpdatePeerStateMsg:
		if err := env.fsm.UpdatePeerState(m.ID, m.State); err != nil {
			return nil, err
		}
		if env.dispatch != nil {
			if err := env.dispatch.UpdatePeerState(
				ctx, m.ID, m.State,
			); err != nil {
				return nil, err
			}
		}

		return respond(&ManagerAck{})

	case *UpdatePeerRTTMsg:
		if err := env.fsm.UpdatePeerRTT(m.ID, m.RTT); err != nil {
			return nil, err
		}
		if env.dispatch != nil {
			if err := env.dispatch.UpdatePeerRTT(
				ctx, m.ID, m.RTT,
			); err != nil {
				return nil, err
			}
		}

		return respond(&ManagerAck{})

	case *AssignRangeMsg:
		var (
			assignment RangeAssignment
			ok         bool
			err        error
		)
		if env.dispatch == nil {
			assignment, ok = env.fsm.AssignNextQueuedRange()
		} else {
			assignment, ok, err = assignRangeWithDispatch(ctx, env)
			if err != nil {
				return nil, err
			}
		}

		return respond(&RangeAssignmentResp{
			Assignment: assignment,
			OK:         ok,
		})

	case *StartRangeMsg:
		assignment, ok, err := env.fsm.StartPeerRange(m.PeerID)
		if err != nil {
			return nil, err
		}
		if ok && env.dispatch != nil {
			peer, peerOK := env.fsm.SnapshotPeer(m.PeerID)
			if !peerOK {
				return nil, fmt.Errorf("%w: %s", ErrUnknownPeer,
					m.PeerID)
			}
			if err := env.dispatch.StartRange(
				ctx,
				peer,
			); err != nil {
				return nil, err
			}
		}

		return respond(&RangeAssignmentResp{
			Assignment: assignment,
			OK:         ok,
		})

	case *StealRangeMsg:
		var (
			result StealResult
			ok     bool
			err    error
		)
		if m.RangeID == NoRange {
			if env.dispatch == nil {
				result, ok, err = env.fsm.StealNextRange(
					m.ThiefID,
				)
			} else {
				result, ok, err = stealRangeWithDispatch(
					ctx, env, m.ThiefID,
				)
			}
		} else {
			result, ok, err = env.fsm.StealRange(
				m.ThiefID, m.RangeID,
			)
			if ok && env.dispatch != nil {
				if err := syncStealPeers(
					ctx,
					env,
					result,
				); err != nil {
					return nil, err
				}
			}
		}
		if err != nil {
			return nil, err
		}

		return respond(&StealRangeResp{Result: result, OK: ok})

	case *CompleteRangeMsg:
		var (
			result CompleteResult
			err    error
		)
		if m.At.IsZero() {
			result, err = env.fsm.CompleteRange(
				m.PeerID, m.RangeID, m.LeaseEpoch, m.Valid,
			)
		} else {
			result, err = env.fsm.CompleteRangeAt(
				m.PeerID, m.RangeID, m.LeaseEpoch, m.Valid,
				m.At,
			)
		}
		if err != nil {
			return nil, err
		}
		if result.Accepted && !result.Stale && env.dispatch != nil {
			if err := env.dispatch.CompleteRange(
				ctx, m.PeerID,
			); err != nil {
				return nil, err
			}
			peer, peerOK := env.fsm.SnapshotPeer(m.PeerID)
			if peerOK {
				if err := env.dispatch.SyncPeer(
					ctx, peer,
				); err != nil {
					return nil, err
				}
			}
		}

		return respond(&CompleteRangeResp{Result: result})

	case *CommitReadyMsg:
		return respond(&CommitReadyResp{
			Result: env.fsm.CommitReadyRanges(),
		})

	case *ReadyRangesMsg:
		return respond(&ReadyRangesResp{
			RangeIDs: env.fsm.ReadyRanges(),
		})

	case *SnapshotPeerMsg:
		peer, ok := env.fsm.SnapshotPeer(m.ID)

		return respond(&SnapshotPeerResp{Peer: peer, OK: ok})

	case *SnapshotRangeMsg:
		rng, ok := env.fsm.RangeByID(m.ID)

		return respond(&SnapshotRangeResp{Range: rng, OK: ok})

	case *MetricsMsg:
		return respond(&MetricsResp{Metrics: env.fsm.Metrics()})

	case *CommitStatsMsg:
		now := m.Now
		if now.IsZero() {
			now = time.Now()
		}

		return respond(&CommitStatsResp{
			Stats: env.fsm.CommitStats(now),
		})

	default:
		return nil, fmt.Errorf("unknown headersync manager message: "+
			"%T",
			msg)
	}
}

func assignRangeWithDispatch(ctx context.Context, env *managerEnv) (
	RangeAssignment, bool, error) {

	rangeID, ok := env.fsm.nextQueuedRange()
	if !ok {
		env.fsm.metrics.FailedAssignments++
		return RangeAssignment{}, false, nil
	}

	dispatchAssignment, ok, err := env.dispatch.AssignRange(ctx, rangeID)
	if err != nil || !ok {
		if !ok {
			env.fsm.metrics.FailedAssignments++
		}

		return RangeAssignment{}, false, err
	}

	return env.fsm.AssignQueuedRangeToPeer(
		dispatchAssignment.RangeID, dispatchAssignment.PeerID,
	)
}

func stealRangeWithDispatch(ctx context.Context, env *managerEnv,
	thiefID string) (StealResult, bool, error) {

	dispatchSteal, ok, err := env.dispatch.StealRange(ctx, thiefID)
	if err != nil {
		return StealResult{}, false, err
	}
	if !ok {
		result, ok, err := env.fsm.StealNextRange(thiefID)
		if err != nil || !ok {
			return StealResult{}, ok, err
		}
		if err := syncStealPeers(ctx, env, result); err != nil {
			return StealResult{}, false, err
		}

		return result, true, nil
	}

	rangeIDs := dispatchSteal.RangeIDs
	if len(rangeIDs) == 0 && dispatchSteal.RangeID != NoRange {
		rangeIDs = []RangeID{dispatchSteal.RangeID}
	}
	if len(rangeIDs) == 0 {
		env.fsm.metrics.FailedStealActions++
		return StealResult{}, false, nil
	}

	var result StealResult
	for _, rangeID := range rangeIDs {
		steal, ok, err := env.fsm.stealRange(
			thiefID, rangeID, StealReasonDispatch,
		)
		if err != nil || !ok {
			return StealResult{}, false, err
		}
		if result.RangeID == NoRange {
			result = steal
		}
	}

	return result, true, nil
}

func syncStealPeers(ctx context.Context, env *managerEnv,
	result StealResult) error {

	thief, ok := env.fsm.SnapshotPeer(result.ThiefID)
	if ok {
		if err := env.dispatch.SyncPeer(ctx, thief); err != nil {
			return err
		}
	}

	donor, ok := env.fsm.SnapshotPeer(result.DonorID)
	if ok {
		if err := env.dispatch.SyncPeer(ctx, donor); err != nil {
			return err
		}
	}

	return nil
}

// Manager owns the header sync FSM. Every message runs synchronously on
// the caller's goroutine, under mu. There is no mailbox, so a caller's deadline
// can't expire between a message applying and its reply arriving, which is what
// used to leave callers with a wrong view of the FSM.
type Manager struct {
	fsm      *HeaderSyncFSM
	dispatch DispatchController
	events   EventSink

	// mu serializes messages. A peer holds its own lock when it calls in
	// here, and the manager never calls a peer, so the order is always
	// peer, then manager.
	mu      sync.Mutex
	machine *protofsm.StateMachine[ManagerMsg, managerOutbox, *managerEnv]
	stopped bool
}

// NewManager creates a manager around a HeaderSyncFSM.
func NewManager(fsm *HeaderSyncFSM) *Manager {
	return &Manager{fsm: fsm}
}

// SetEventSink enables structured header sync events.
func (m *Manager) SetEventSink(events EventSink) {
	m.events = events
}

// SetDispatchController routes generic range ownership and unstarted-work
// stealing through an external scheduler. It should be called before Start.
func (m *Manager) SetDispatchController(dispatch DispatchController) {
	m.dispatch = dispatch
}

// Start starts the manager.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.machine != nil {
		return
	}

	machine, err := protofsm.NewStateMachine[
		ManagerMsg, managerOutbox,
	](protofsm.StateMachineCfg[
		ManagerMsg, managerOutbox, *managerEnv,
	]{
		InitialState: managerState{},
		Env: &managerEnv{
			fsm:      m.fsm,
			dispatch: m.dispatch,
		},
	})
	if err != nil {
		panic(fmt.Sprintf("unable to create header manager FSM: %v",
			err))
	}

	m.machine = machine
	m.machine.Start(ctx)
}

// Stop stops the manager. Later messages report ErrActorStopped.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		return
	}
	m.stopped = true

	if m.machine != nil {
		m.machine.Stop()
	}
}

// ask runs one message.
func (m *Manager) ask(ctx context.Context, msg ManagerMsg) (
	ManagerResp, error) {

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.machine == nil || m.stopped {
		return nil, ErrActorStopped
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	outbox, err := m.machine.ProcessEvent(ctx, msg)
	if err != nil {
		return nil, err
	}

	var resp ManagerResp = &ManagerAck{}
	for _, out := range outbox {
		switch out := out.(type) {
		case managerOutboxResponse:
			resp = out.Response

		default:
			return nil, fmt.Errorf(
				"unknown manager outbox: %T", out,
			)
		}
	}

	if m.events != nil {
		if event, ok := eventForManagerMessage(m.fsm, msg, resp); ok {
			m.events.RecordHeaderSyncEvent(event)
		}
	}

	return resp, nil
}

// AddAnchor asks the manager to add or confirm an anchor.
func (m *Manager) AddAnchor(ctx context.Context, height uint32,
	hash Hash, trusted bool) (Anchor, bool, error) {

	resp, err := m.ask(ctx, &AddAnchorMsg{
		Height:  height,
		Hash:    hash,
		Trusted: trusted,
	})
	if err != nil {
		return Anchor{}, false, err
	}

	anchorResp, ok := resp.(*AddAnchorResp)
	if !ok {
		return Anchor{}, false, unexpectedManagerResp(resp)
	}

	return anchorResp.Anchor, anchorResp.OK, nil
}

// AdvanceCommittedTip asks the manager to reconcile its committed tip with a
// block-header tip that has already been persisted by the block manager.
func (m *Manager) AdvanceCommittedTip(ctx context.Context, height uint32,
	hash Hash) (bool, error) {

	resp, err := m.ask(ctx, &AdvanceCommittedTipMsg{
		Height: height,
		Hash:   hash,
	})
	if err != nil {
		return false, err
	}

	advanceResp, ok := resp.(*AdvanceCommittedTipResp)
	if !ok {
		return false, unexpectedManagerResp(resp)
	}

	return advanceResp.Advanced, nil
}

// RequeueStagedRange asks the manager to return a staged range whose headers
// were lost to the queue under a new lease epoch.
func (m *Manager) RequeueStagedRange(ctx context.Context,
	id RangeID) (bool, error) {

	resp, err := m.ask(ctx, &RequeueStagedRangeMsg{ID: id})
	if err != nil {
		return false, err
	}

	requeueResp, ok := resp.(*RequeueStagedRangeResp)
	if !ok {
		return false, unexpectedManagerResp(resp)
	}

	return requeueResp.OK, nil
}

// Anchors asks the manager for the current known anchors.
func (m *Manager) Anchors(ctx context.Context) ([]Anchor, error) {
	resp, err := m.ask(ctx, &AnchorsMsg{})
	if err != nil {
		return nil, err
	}

	anchorsResp, ok := resp.(*AnchorsResp)
	if !ok {
		return nil, unexpectedManagerResp(resp)
	}

	return anchorsResp.Anchors, nil
}

// PlanRange asks the manager to create queued range work.
func (m *Manager) PlanRange(ctx context.Context, id RangeID,
	startHeight, stopHeight uint32) (HeaderRange, bool, error) {

	resp, err := m.ask(ctx, &PlanRangeMsg{
		ID:          id,
		StartHeight: startHeight,
		StopHeight:  stopHeight,
	})
	if err != nil {
		return HeaderRange{}, false, err
	}

	planResp, ok := resp.(*PlanRangeResp)
	if !ok {
		return HeaderRange{}, false, unexpectedManagerResp(resp)
	}

	return planResp.Range, planResp.OK, nil
}

// StageDiscoveredRange asks the manager to stage a range using a confirmed
// anchor-discovery response.
func (m *Manager) StageDiscoveredRange(ctx context.Context,
	id RangeID, peerID string, startHeight, stopHeight uint32,
	at time.Time) (HeaderRange, bool, error) {

	resp, err := m.ask(ctx, &StageDiscoveredRangeMsg{
		ID:          id,
		PeerID:      peerID,
		StartHeight: startHeight,
		StopHeight:  stopHeight,
		At:          at,
	})
	if err != nil {
		return HeaderRange{}, false, err
	}

	stageResp, ok := resp.(*StageDiscoveredRangeResp)
	if !ok {
		return HeaderRange{}, false, unexpectedManagerResp(resp)
	}

	return stageResp.Range, stageResp.OK, nil
}

// InvalidateRange asks the FSM to reject invalid work and dependent
// boundaries.
func (m *Manager) InvalidateRange(ctx context.Context,
	id RangeID) (InvalidationResult, error) {

	resp, err := m.ask(ctx, &InvalidateRangeMsg{ID: id})
	if err != nil {
		return InvalidationResult{}, err
	}
	result, ok := resp.(*InvalidateRangeResp)
	if !ok {
		return InvalidationResult{}, unexpectedManagerResp(resp)
	}
	return result.Result, result.Err
}

// RejectAnchor asks the FSM to reject an untrusted discovered anchor and the
// unstaged work that uses it as a boundary.
func (m *Manager) RejectAnchor(ctx context.Context, height uint32,
	hash Hash) (InvalidationResult, error) {

	resp, err := m.ask(ctx, &RejectAnchorMsg{Height: height, Hash: hash})
	if err != nil {
		return InvalidationResult{}, err
	}
	result, ok := resp.(*RejectAnchorResp)
	if !ok {
		return InvalidationResult{}, unexpectedManagerResp(resp)
	}
	return result.Result, result.Err
}

// PlanConfirmedAnchorRanges asks the manager to plan all bounded adjacent
// ranges between confirmed anchors in [startHeight, stopHeight].
func (m *Manager) PlanConfirmedAnchorRanges(ctx context.Context,
	nextID RangeID, startHeight, stopHeight uint32) (
	PlanRangesResult, error) {

	resp, err := m.ask(ctx, &PlanAnchorRangesMsg{
		NextID:      nextID,
		StartHeight: startHeight,
		StopHeight:  stopHeight,
	})
	if err != nil {
		return PlanRangesResult{}, err
	}

	planResp, ok := resp.(*PlanAnchorRangesResp)
	if !ok {
		return PlanRangesResult{}, unexpectedManagerResp(resp)
	}

	return planResp.Result, nil
}

// AddPeer asks the manager to add a peer.
func (m *Manager) AddPeer(ctx context.Context,
	peer PeerSnapshot) error {

	_, err := m.ask(ctx, &AddPeerMsg{Peer: peer})

	return err
}

// RemovePeer asks the manager to remove a peer.
func (m *Manager) RemovePeer(ctx context.Context, id string) error {
	_, err := m.ask(ctx, &RemovePeerMsg{ID: id})

	return err
}

// UpdatePeerState asks the manager to update peer state.
func (m *Manager) UpdatePeerState(ctx context.Context, id string,
	state PeerState) error {

	_, err := m.ask(ctx, &UpdatePeerStateMsg{
		ID:    id,
		State: state,
	})

	return err
}

// UpdatePeerRTT asks the manager to update peer RTT.
func (m *Manager) UpdatePeerRTT(ctx context.Context, id string,
	rtt time.Duration) error {

	_, err := m.ask(ctx, &UpdatePeerRTTMsg{ID: id, RTT: rtt})

	return err
}

// AssignNextQueuedRange asks the manager to assign global queued work.
func (m *Manager) AssignNextQueuedRange(ctx context.Context) (
	RangeAssignment, bool, error) {

	resp, err := m.ask(ctx, &AssignRangeMsg{})
	if err != nil {
		return RangeAssignment{}, false, err
	}

	assignResp, ok := resp.(*RangeAssignmentResp)
	if !ok {
		return RangeAssignment{}, false, unexpectedManagerResp(resp)
	}

	return assignResp.Assignment, assignResp.OK, nil
}

// StartPeerRange asks the manager to start a peer-local range.
func (m *Manager) StartPeerRange(ctx context.Context,
	peerID string) (RangeAssignment, bool, error) {

	resp, err := m.ask(ctx, &StartRangeMsg{PeerID: peerID})
	if err != nil {
		return RangeAssignment{}, false, err
	}

	assignResp, ok := resp.(*RangeAssignmentResp)
	if !ok {
		return RangeAssignment{}, false, unexpectedManagerResp(resp)
	}

	return assignResp.Assignment, assignResp.OK, nil
}

// StealNextRange asks the manager to select stealable work for a peer.
func (m *Manager) StealNextRange(ctx context.Context,
	thiefID string) (StealResult, bool, error) {

	return m.StealRange(ctx, thiefID, NoRange)
}

// StealRange asks the manager to steal a specific range, or any stealable
// range when rangeID is NoRange.
func (m *Manager) StealRange(ctx context.Context, thiefID string,
	rangeID RangeID) (StealResult, bool, error) {

	resp, err := m.ask(ctx, &StealRangeMsg{
		ThiefID: thiefID,
		RangeID: rangeID,
	})
	if err != nil {
		return StealResult{}, false, err
	}

	stealResp, ok := resp.(*StealRangeResp)
	if !ok {
		return StealResult{}, false, unexpectedManagerResp(resp)
	}

	return stealResp.Result, stealResp.OK, nil
}

// CompleteRange asks the manager to record a range response.
func (m *Manager) CompleteRange(ctx context.Context, peerID string,
	rangeID RangeID, leaseEpoch uint64, valid bool,
	at time.Time) (CompleteResult, error) {

	resp, err := m.ask(ctx, &CompleteRangeMsg{
		PeerID:     peerID,
		RangeID:    rangeID,
		LeaseEpoch: leaseEpoch,
		Valid:      valid,
		At:         at,
	})
	if err != nil {
		return CompleteResult{}, err
	}

	completeResp, ok := resp.(*CompleteRangeResp)
	if !ok {
		return CompleteResult{}, unexpectedManagerResp(resp)
	}

	return completeResp.Result, nil
}

// CommitReadyRanges asks the manager to drain staged contiguous ranges.
func (m *Manager) CommitReadyRanges(ctx context.Context) (
	CommitResult, error) {

	resp, err := m.ask(ctx, &CommitReadyMsg{})
	if err != nil {
		return CommitResult{}, err
	}

	commitResp, ok := resp.(*CommitReadyResp)
	if !ok {
		return CommitResult{}, unexpectedManagerResp(resp)
	}

	return commitResp.Result, nil
}

// ReadyRanges asks the manager which staged ranges are ready for
// side-effectful persistence before the FSM-visible tip advances.
func (m *Manager) ReadyRanges(ctx context.Context) ([]RangeID, error) {
	resp, err := m.ask(ctx, &ReadyRangesMsg{})
	if err != nil {
		return nil, err
	}

	readyResp, ok := resp.(*ReadyRangesResp)
	if !ok {
		return nil, unexpectedManagerResp(resp)
	}

	return append([]RangeID(nil), readyResp.RangeIDs...), nil
}

// SnapshotPeer asks the manager for one peer snapshot.
func (m *Manager) SnapshotPeer(ctx context.Context,
	id string) (PeerSnapshot, bool, error) {

	resp, err := m.ask(ctx, &SnapshotPeerMsg{ID: id})
	if err != nil {
		return PeerSnapshot{}, false, err
	}

	snapshotResp, ok := resp.(*SnapshotPeerResp)
	if !ok {
		return PeerSnapshot{}, false, unexpectedManagerResp(resp)
	}

	return snapshotResp.Peer, snapshotResp.OK, nil
}

// SnapshotRange asks the manager for one range snapshot.
func (m *Manager) SnapshotRange(ctx context.Context,
	id RangeID) (HeaderRange, bool, error) {

	resp, err := m.ask(ctx, &SnapshotRangeMsg{ID: id})
	if err != nil {
		return HeaderRange{}, false, err
	}

	snapshotResp, ok := resp.(*SnapshotRangeResp)
	if !ok {
		return HeaderRange{}, false, unexpectedManagerResp(resp)
	}

	return snapshotResp.Range, snapshotResp.OK, nil
}

// Metrics asks the manager for current counters.
func (m *Manager) Metrics(ctx context.Context) (Metrics, error) {
	resp, err := m.ask(ctx, &MetricsMsg{})
	if err != nil {
		return Metrics{}, err
	}

	metricsResp, ok := resp.(*MetricsResp)
	if !ok {
		return Metrics{}, unexpectedManagerResp(resp)
	}

	return metricsResp.Metrics, nil
}

// CommitStats asks the manager for current staged-range pressure.
func (m *Manager) CommitStats(ctx context.Context,
	now time.Time) (CommitStats, error) {

	resp, err := m.ask(ctx, &CommitStatsMsg{Now: now})
	if err != nil {
		return CommitStats{}, err
	}

	statsResp, ok := resp.(*CommitStatsResp)
	if !ok {
		return CommitStats{}, unexpectedManagerResp(resp)
	}

	return statsResp.Stats, nil
}

func unexpectedManagerResp(resp ManagerResp) error {
	return fmt.Errorf("unexpected manager response: %T", resp)
}

// PeerMsg is accepted by a header peer.
type PeerMsg interface {
	actor.Message

	peerMsg()
}

type peerMessage struct {
	actor.BaseMessage
}

func (peerMessage) peerMsg() {}

func (peerMessage) MessageType() string {
	return "headersync.PeerMsg"
}

// PeerResp is returned by peer messages.
type PeerResp interface {
	peerResp()
}

type peerResponse struct{}

func (peerResponse) peerResp() {}

// PeerAck confirms a peer message was applied.
type PeerAck struct {
	peerResponse
}

// PeerRangeResp reports started range work.
type PeerRangeResp struct {
	peerResponse

	Assignment RangeAssignment
	OK         bool
}

// PeerStealResp reports stolen work.
type PeerStealResp struct {
	peerResponse

	Result StealResult
	OK     bool
}

// PeerCompleteResp reports completion handling.
type PeerCompleteResp struct {
	peerResponse

	Result CompleteResult
}

// PeerReadyEvent moves the peer into a schedulable state.
type PeerReadyEvent struct {
	peerMessage
}

// PeerBlockedEvent marks the peer temporarily unable to make progress.
type PeerBlockedEvent struct {
	peerMessage
}

// PeerQuarantinedEvent marks the peer as banned/quarantined.
type PeerQuarantinedEvent struct {
	peerMessage
}

// PeerDownEvent marks the peer disconnected.
type PeerDownEvent struct {
	peerMessage
}

// PeerRTTEvent updates the peer RTT sample.
type PeerRTTEvent struct {
	peerMessage

	RTT time.Duration
}

// PeerRankEvent updates the peer rank.
type PeerRankEvent struct {
	peerMessage

	Rank int
}

// PeerNeedRangeEvent asks the manager for the next locally owned range.
type PeerNeedRangeEvent struct {
	peerMessage
}

// PeerNeedStealEvent asks the manager for stealable range work.
type PeerNeedStealEvent struct {
	peerMessage
}

// PeerRangeAssignedEvent records range work accepted by this peer.
type PeerRangeAssignedEvent struct {
	peerMessage

	Assignment RangeAssignment
}

// PeerRangeCompletedEvent reports the active range result.
type PeerRangeCompletedEvent struct {
	peerMessage

	RangeID    RangeID
	LeaseEpoch uint64
	Valid      bool
	At         time.Time
}

// PeerRangeStolenEvent clears this peer's active range after the manager has
// transferred its lease to another peer.
type PeerRangeStolenEvent struct {
	peerMessage

	RangeID       RangeID
	OldLeaseEpoch uint64
}

type peerOutbox interface {
	peerOutbox()
}

type peerSnapshotOutbox struct {
	Peer PeerSnapshot
}

func (peerSnapshotOutbox) peerOutbox() {}

type peerStartRangeOutbox struct {
	PeerID string
}

func (peerStartRangeOutbox) peerOutbox() {}

type peerStealRangeOutbox struct {
	ThiefID string
}

func (peerStealRangeOutbox) peerOutbox() {}

type peerCompleteRangeOutbox struct {
	PeerID     string
	RangeID    RangeID
	LeaseEpoch uint64
	Valid      bool
	At         time.Time
}

func (peerCompleteRangeOutbox) peerOutbox() {}

type peerEnv struct {
	id   string
	rank int
}

type peerReadyState struct {
	rtt time.Duration
}

func (s peerReadyState) String() string {
	return "headersync-peer-ready"
}

func (s peerReadyState) IsTerminal() bool {
	return false
}

func (s peerReadyState) ProcessEvent(_ context.Context, msg PeerMsg,
	env *peerEnv) (*protofsm.StateTransition[
	PeerMsg, peerOutbox, *peerEnv], error) {

	switch m := msg.(type) {
	case PeerReadyEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerReady,
			RangeAssignment{}))

	case PeerBlockedEvent:
		next := peerBlockedState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerBlocked, RangeAssignment{}))

	case PeerQuarantinedEvent:
		next := peerQuarantinedState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerQuarantined, RangeAssignment{}))

	case PeerDownEvent:
		next := peerDownState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerDown,
			RangeAssignment{}))

	case PeerRTTEvent:
		if m.RTT < 0 {
			return nil, fmt.Errorf("negative RTT: %v", m.RTT)
		}

		next := peerReadyState{rtt: m.RTT}

		return peerTransition(next, peerSnapshot(env, m.RTT,
			PeerReady, RangeAssignment{}))

	case PeerRankEvent:
		env.rank = m.Rank

		return peerTransition(s, peerSnapshot(env, s.rtt, PeerReady,
			RangeAssignment{}))

	case PeerNeedRangeEvent:
		snapshot := peerSnapshot(
			env, s.rtt, PeerReady, RangeAssignment{},
		)
		return peerTransition(s, snapshot,
			peerStartRangeOutbox{PeerID: env.id})

	case PeerNeedStealEvent:
		snapshot := peerSnapshot(
			env, s.rtt, PeerReady, RangeAssignment{},
		)
		return peerTransition(s, snapshot,
			peerStealRangeOutbox{ThiefID: env.id})

	case PeerRangeAssignedEvent:
		if m.Assignment.RangeID == NoRange {
			return nil, fmt.Errorf("empty range assignment")
		}

		next := peerBusyState{
			rtt:    s.rtt,
			active: m.Assignment,
		}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerBusy,
			m.Assignment))

	default:
		return nil, fmt.Errorf("ready peer cannot process %T", msg)
	}
}

type peerBusyState struct {
	rtt    time.Duration
	active RangeAssignment
}

func (s peerBusyState) String() string {
	return "headersync-peer-busy"
}

func (s peerBusyState) IsTerminal() bool {
	return false
}

func (s peerBusyState) ProcessEvent(_ context.Context, msg PeerMsg,
	env *peerEnv) (*protofsm.StateTransition[
	PeerMsg, peerOutbox, *peerEnv], error) {

	switch m := msg.(type) {
	case PeerReadyEvent:
		next := peerReadyState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerReady,
			RangeAssignment{}))

	case PeerBlockedEvent:
		next := peerBlockedState(s)

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerBlocked, s.active))

	case PeerQuarantinedEvent:
		next := peerQuarantinedState(s)

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerQuarantined, s.active))

	case PeerDownEvent:
		next := peerDownState(s)

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerDown,
			s.active))

	case PeerRTTEvent:
		if m.RTT < 0 {
			return nil, fmt.Errorf("negative RTT: %v", m.RTT)
		}

		next := peerBusyState{rtt: m.RTT, active: s.active}

		return peerTransition(next, peerSnapshot(env, m.RTT, PeerBusy,
			s.active))

	case PeerRankEvent:
		env.rank = m.Rank

		return peerTransition(s, peerSnapshot(env, s.rtt, PeerBusy,
			s.active))

	case PeerRangeCompletedEvent:
		return completePeerRangeTransition(
			env,
			s.rtt,
			PeerReady,
			s.active,
			m,
		)

	case PeerRangeStolenEvent:
		return clearStolenPeerRangeTransition(
			env, s.rtt, PeerReady, s.active, m,
		)

	case PeerNeedRangeEvent, PeerNeedStealEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerBusy,
			s.active))

	case PeerRangeAssignedEvent:
		return nil, fmt.Errorf("peer %s already has active range",
			env.id)

	default:
		return nil, fmt.Errorf("busy peer cannot process %T", msg)
	}
}

type peerBlockedState struct {
	rtt    time.Duration
	active RangeAssignment
}

func (s peerBlockedState) String() string {
	return "headersync-peer-blocked"
}

func (s peerBlockedState) IsTerminal() bool {
	return false
}

func (s peerBlockedState) ProcessEvent(_ context.Context, msg PeerMsg,
	env *peerEnv) (*protofsm.StateTransition[
	PeerMsg, peerOutbox, *peerEnv], error) {

	switch m := msg.(type) {
	case PeerReadyEvent:
		return peerReadyOrBusyTransition(env, s.rtt, s.active)

	case PeerBlockedEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt,
			PeerBlocked, s.active))

	case PeerQuarantinedEvent:
		next := peerQuarantinedState(s)

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerQuarantined, s.active))

	case PeerDownEvent:
		next := peerDownState(s)

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerDown,
			s.active))

	case PeerRTTEvent:
		if m.RTT < 0 {
			return nil, fmt.Errorf("negative RTT: %v", m.RTT)
		}

		next := peerBlockedState{rtt: m.RTT, active: s.active}

		return peerTransition(next, peerSnapshot(env, m.RTT,
			PeerBlocked, s.active))

	case PeerRankEvent:
		env.rank = m.Rank

		return peerTransition(s, peerSnapshot(env, s.rtt,
			PeerBlocked, s.active))

	case PeerRangeCompletedEvent:
		return completePeerRangeTransition(
			env,
			s.rtt,
			PeerBlocked,
			s.active,
			m,
		)

	case PeerRangeStolenEvent:
		return clearStolenPeerRangeTransition(
			env, s.rtt, PeerBlocked, s.active, m,
		)

	case PeerNeedRangeEvent, PeerNeedStealEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt,
			PeerBlocked, s.active))

	case PeerRangeAssignedEvent:
		return nil, fmt.Errorf("blocked peer %s cannot accept range",
			env.id)

	default:
		return nil, fmt.Errorf("blocked peer cannot process %T", msg)
	}
}

type peerQuarantinedState struct {
	rtt    time.Duration
	active RangeAssignment
}

func (s peerQuarantinedState) String() string {
	return "headersync-peer-quarantined"
}

func (s peerQuarantinedState) IsTerminal() bool {
	return false
}

func (s peerQuarantinedState) ProcessEvent(_ context.Context, msg PeerMsg,
	env *peerEnv) (*protofsm.StateTransition[
	PeerMsg, peerOutbox, *peerEnv], error) {

	switch m := msg.(type) {
	case PeerReadyEvent:
		return peerReadyOrBusyTransition(env, s.rtt, s.active)

	case PeerBlockedEvent:
		next := peerBlockedState(s)

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerBlocked, s.active))

	case PeerQuarantinedEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt,
			PeerQuarantined, s.active))

	case PeerDownEvent:
		next := peerDownState(s)

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerDown,
			s.active))

	case PeerRTTEvent:
		if m.RTT < 0 {
			return nil, fmt.Errorf("negative RTT: %v", m.RTT)
		}

		next := peerQuarantinedState{rtt: m.RTT, active: s.active}

		return peerTransition(next, peerSnapshot(env, m.RTT,
			PeerQuarantined, s.active))

	case PeerRankEvent:
		env.rank = m.Rank

		return peerTransition(s, peerSnapshot(env, s.rtt,
			PeerQuarantined, s.active))

	case PeerRangeCompletedEvent:
		return completePeerRangeTransition(
			env,
			s.rtt,
			PeerQuarantined,
			s.active,
			m,
		)

	case PeerRangeStolenEvent:
		return clearStolenPeerRangeTransition(
			env, s.rtt, PeerQuarantined, s.active, m,
		)

	case PeerNeedRangeEvent, PeerNeedStealEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt,
			PeerQuarantined, s.active))

	case PeerRangeAssignedEvent:
		return nil, fmt.Errorf("quarantined peer %s cannot accept "+
			"range",
			env.id)

	default:
		return nil, fmt.Errorf(
			"quarantined peer cannot process %T",
			msg,
		)
	}
}

type peerDownState struct {
	rtt    time.Duration
	active RangeAssignment
}

func (s peerDownState) String() string {
	return "headersync-peer-down"
}

func (s peerDownState) IsTerminal() bool {
	return false
}

func (s peerDownState) ProcessEvent(_ context.Context, msg PeerMsg,
	env *peerEnv) (*protofsm.StateTransition[
	PeerMsg, peerOutbox, *peerEnv], error) {

	switch m := msg.(type) {
	case PeerReadyEvent:
		return peerReadyOrBusyTransition(env, s.rtt, s.active)

	case PeerBlockedEvent:
		next := peerBlockedState(s)

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerBlocked, s.active))

	case PeerQuarantinedEvent:
		next := peerQuarantinedState(s)

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerQuarantined, s.active))

	case PeerDownEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerDown,
			s.active))

	case PeerRTTEvent:
		if m.RTT < 0 {
			return nil, fmt.Errorf("negative RTT: %v", m.RTT)
		}

		next := peerDownState{rtt: m.RTT, active: s.active}

		return peerTransition(next, peerSnapshot(env, m.RTT, PeerDown,
			s.active))

	case PeerRankEvent:
		env.rank = m.Rank

		return peerTransition(s, peerSnapshot(env, s.rtt, PeerDown,
			s.active))

	case PeerRangeCompletedEvent:
		return completePeerRangeTransition(
			env,
			s.rtt,
			PeerDown,
			s.active,
			m,
		)

	case PeerRangeStolenEvent:
		return clearStolenPeerRangeTransition(
			env, s.rtt, PeerDown, s.active, m,
		)

	case PeerNeedRangeEvent, PeerNeedStealEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerDown,
			s.active))

	case PeerRangeAssignedEvent:
		return nil, fmt.Errorf("down peer %s cannot accept range",
			env.id)

	default:
		return nil, fmt.Errorf("down peer cannot process %T", msg)
	}
}

func peerReadyOrBusyTransition(env *peerEnv, rtt time.Duration,
	active RangeAssignment) (*protofsm.StateTransition[
	PeerMsg, peerOutbox, *peerEnv], error) {

	if active.RangeID == NoRange {
		next := peerReadyState{rtt: rtt}

		return peerTransition(next, peerSnapshot(env, rtt, PeerReady,
			RangeAssignment{}))
	}

	next := peerBusyState{rtt: rtt, active: active}

	return peerTransition(next, peerSnapshot(env, rtt, PeerBusy, active))
}

func completePeerRangeTransition(
	env *peerEnv,
	rtt time.Duration,
	state PeerState,
	active RangeAssignment,
	event PeerRangeCompletedEvent,
) (
	*protofsm.StateTransition[PeerMsg, peerOutbox, *peerEnv], error) {

	if active.RangeID == NoRange {
		return nil, fmt.Errorf("peer %s has no active range", env.id)
	}
	if active.RangeID != event.RangeID ||
		active.LeaseEpoch != event.LeaseEpoch {

		return nil, fmt.Errorf(
			"peer %s completion range/epoch mismatch", env.id,
		)
	}

	next := peerStateFromSnapshot(state, rtt, RangeAssignment{})

	return peerTransition(next,
		peerCompleteRangeOutbox{
			PeerID:     env.id,
			RangeID:    event.RangeID,
			LeaseEpoch: event.LeaseEpoch,
			Valid:      event.Valid,
			At:         event.At,
		},
		peerSnapshot(env, rtt, state, RangeAssignment{}),
	)
}

func clearStolenPeerRangeTransition(env *peerEnv, rtt time.Duration,
	state PeerState, active RangeAssignment, event PeerRangeStolenEvent) (
	*protofsm.StateTransition[PeerMsg, peerOutbox, *peerEnv], error) {

	if active.RangeID != event.RangeID ||
		active.LeaseEpoch != event.OldLeaseEpoch {

		return peerTransition(peerStateFromSnapshot(state, rtt, active),
			peerSnapshot(env, rtt, state, active))
	}

	return peerTransition(
		peerStateFromSnapshot(state, rtt, RangeAssignment{}),
		peerSnapshot(env, rtt, state, RangeAssignment{}),
	)
}

func peerStateFromSnapshot(state PeerState, rtt time.Duration,
	active RangeAssignment) protofsm.State[PeerMsg, peerOutbox, *peerEnv] {

	switch state {
	case PeerReady:
		if active.RangeID != NoRange {
			return peerBusyState{rtt: rtt, active: active}
		}

		return peerReadyState{rtt: rtt}

	case PeerBusy:
		return peerBusyState{rtt: rtt, active: active}

	case PeerBlocked:
		return peerBlockedState{rtt: rtt, active: active}

	case PeerQuarantined:
		return peerQuarantinedState{rtt: rtt, active: active}

	case PeerDown:
		return peerDownState{rtt: rtt, active: active}

	default:
		return peerDownState{rtt: rtt, active: active}
	}
}

func peerSnapshot(env *peerEnv, rtt time.Duration, state PeerState,
	active RangeAssignment) peerSnapshotOutbox {

	return peerSnapshotOutbox{
		Peer: PeerSnapshot{
			ID:          env.id,
			Rank:        env.rank,
			RTT:         rtt,
			State:       state,
			ActiveRange: active.RangeID,
		},
	}
}

func peerTransition(next protofsm.State[PeerMsg, peerOutbox, *peerEnv],
	outbox ...peerOutbox) (*protofsm.StateTransition[
	PeerMsg, peerOutbox, *peerEnv], error) {

	return &protofsm.StateTransition[PeerMsg, peerOutbox, *peerEnv]{
		NextState: next,
		NewEvents: fn.Some(protofsm.EmittedEvent[PeerMsg, peerOutbox]{
			Outbox: outbox,
		}),
	}, nil
}

// Peer owns peer-local header sync state. Like the manager, it runs each
// message synchronously on the caller's goroutine, under mu.
type Peer struct {
	id      string
	rank    int
	manager *Manager

	mu      sync.Mutex
	machine *protofsm.StateMachine[PeerMsg, peerOutbox, *peerEnv]
	stopped bool
}

// NewPeer creates a peer.
func NewPeer(id string, rank int, manager *Manager) *Peer {
	return &Peer{id: id, rank: rank, manager: manager}
}

// Start starts the peer and registers its initial snapshot.
func (p *Peer) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.machine == nil {
		machine, err := protofsm.NewStateMachine[PeerMsg, peerOutbox](
			protofsm.StateMachineCfg[
				PeerMsg, peerOutbox, *peerEnv,
			]{
				InitialState: peerReadyState{},
				Env: &peerEnv{
					id:   p.id,
					rank: p.rank,
				},
			},
		)
		if err != nil {
			p.mu.Unlock()

			return err
		}

		p.machine = machine
		p.machine.Start(ctx)
	}
	p.mu.Unlock()

	return p.Send(ctx, PeerReadyEvent{})
}

// Stop stops the peer. Later messages report ErrActorStopped.
func (p *Peer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return
	}
	p.stopped = true

	if p.machine != nil {
		p.machine.Stop()
	}
}

// ask runs one message.
func (p *Peer) ask(ctx context.Context, msg PeerMsg) (PeerResp, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.machine == nil || p.stopped {
		return nil, ErrActorStopped
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	outbox, err := p.machine.ProcessEvent(ctx, msg)
	if err != nil {
		return nil, err
	}

	return p.applyPeerOutbox(ctx, outbox)
}

func (p *Peer) applyPeerOutbox(ctx context.Context,
	outbox []peerOutbox) (PeerResp, error) {

	resp := PeerResp(&PeerAck{})
	for _, out := range outbox {
		switch out := out.(type) {
		case peerSnapshotOutbox:
			if err := p.manager.AddPeer(ctx, out.Peer); err != nil {
				return nil, err
			}

		case peerStartRangeOutbox:
			assignment, ok, err := p.manager.StartPeerRange(
				ctx, out.PeerID,
			)
			if err != nil {
				return nil, err
			}

			resp = &PeerRangeResp{
				Assignment: assignment,
				OK:         ok,
			}
			if ok {
				followUp, err := p.machine.ProcessEvent(
					ctx, PeerRangeAssignedEvent{
						Assignment: assignment,
					},
				)
				if err != nil {
					return nil, err
				}
				if _, err := p.applyPeerOutbox(
					ctx,
					followUp,
				); err != nil {
					return nil, err
				}
			}

		case peerStealRangeOutbox:
			result, ok, err := p.manager.StealNextRange(
				ctx, out.ThiefID,
			)
			if err != nil {
				return nil, err
			}

			resp = &PeerStealResp{Result: result, OK: ok}

		case peerCompleteRangeOutbox:
			result, err := p.manager.CompleteRange(
				ctx, out.PeerID, out.RangeID, out.LeaseEpoch,
				out.Valid, out.At,
			)
			if err != nil {
				return nil, err
			}

			resp = &PeerCompleteResp{Result: result}

		default:
			return nil, fmt.Errorf("unknown peer outbox: %T", out)
		}
	}

	return resp, nil
}

// Send applies an event to the peer.
func (p *Peer) Send(ctx context.Context, event PeerMsg) error {
	_, err := p.ask(ctx, event)

	return err
}

// RequestRange asks the peer to request its next local range from the
// manager.
func (p *Peer) RequestRange(ctx context.Context) (
	RangeAssignment, bool, error) {

	resp, err := p.ask(ctx, PeerNeedRangeEvent{})
	if err != nil {
		return RangeAssignment{}, false, err
	}

	rangeResp, ok := resp.(*PeerRangeResp)
	if !ok {
		if _, ok := resp.(*PeerAck); ok {
			return RangeAssignment{}, false, nil
		}

		return RangeAssignment{}, false, fmt.Errorf(
			"unexpected peer range response: %T", resp,
		)
	}

	return rangeResp.Assignment, rangeResp.OK, nil
}

// RequestStolenRange asks the peer to ask the manager for stealable
// work.
func (p *Peer) RequestStolenRange(ctx context.Context) (
	StealResult, bool, error) {

	resp, err := p.ask(ctx, PeerNeedStealEvent{})
	if err != nil {
		return StealResult{}, false, err
	}

	stealResp, ok := resp.(*PeerStealResp)
	if !ok {
		if _, ok := resp.(*PeerAck); ok {
			return StealResult{}, false, nil
		}

		return StealResult{}, false, fmt.Errorf(
			"unexpected peer steal response: %T", resp,
		)
	}

	return stealResp.Result, stealResp.OK, nil
}

// CompleteRange reports this peer's active range completion.
func (p *Peer) CompleteRange(ctx context.Context, rangeID RangeID,
	leaseEpoch uint64, valid bool, at time.Time) (CompleteResult, error) {

	resp, err := p.ask(ctx, PeerRangeCompletedEvent{
		RangeID:    rangeID,
		LeaseEpoch: leaseEpoch,
		Valid:      valid,
		At:         at,
	})
	if err != nil {
		return CompleteResult{}, err
	}

	completeResp, ok := resp.(*PeerCompleteResp)
	if !ok {
		return CompleteResult{}, fmt.Errorf(
			"unexpected peer complete response: %T", resp,
		)
	}

	return completeResp.Result, nil
}
