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

const (
	managerActorID = "headersync-manager"
	peerActorKey   = "headersync-peer"
)

// ManagerMsg is accepted by the header sync manager actor.
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

// ManagerResp is returned by the header sync manager actor.
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

	_ = ctx

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
			if err := env.dispatch.AddPeer(ctx, m.Peer); err != nil {
				return nil, err
			}
		}

		return respond(&ManagerAck{})

	case *RemovePeerMsg:
		env.fsm.RemovePeer(m.ID)
		if env.dispatch != nil {
			if err := env.dispatch.RemovePeer(ctx, m.ID); err != nil {
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
			if err := env.dispatch.StartRange(ctx, peer); err != nil {
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
				result, ok, err = env.fsm.StealNextRange(m.ThiefID)
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
				if err := syncStealPeers(ctx, env, result); err != nil {
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
		return nil, fmt.Errorf("unknown headersync manager message: %T",
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

// ManagerActor owns the header sync FSM and exposes it through lnd's actor
// system.
type ManagerActor struct {
	fsm         *HeaderSyncFSM
	mailboxSize int
	dispatch    DispatchController

	key     actor.ServiceKey[ManagerMsg, ManagerResp]
	system  *actor.ActorSystem
	machine *protofsm.StateMachine[ManagerMsg, managerOutbox, *managerEnv]
	ref     actor.ActorRef[ManagerMsg, ManagerResp]
	events  EventSink

	startOnce sync.Once
	stopOnce  sync.Once
}

var _ actor.ActorBehavior[ManagerMsg, ManagerResp] = (*ManagerActor)(nil)

// NewManagerActor creates an actor wrapper around a HeaderSyncFSM.
func NewManagerActor(fsm *HeaderSyncFSM, mailboxSize int) *ManagerActor {
	if mailboxSize <= 0 {
		mailboxSize = 1
	}

	return &ManagerActor{
		fsm:         fsm,
		mailboxSize: mailboxSize,
		key: actor.NewServiceKey[ManagerMsg, ManagerResp](
			managerActorID,
		),
	}
}

// SetEventSink enables structured header sync events.
func (m *ManagerActor) SetEventSink(events EventSink) {
	m.events = events
}

// SetDispatchController routes generic range ownership and unstarted-work
// stealing through an external scheduler. It should be called before Start.
func (m *ManagerActor) SetDispatchController(dispatch DispatchController) {
	m.dispatch = dispatch
}

// Start starts the manager actor.
func (m *ManagerActor) Start(ctx context.Context) {
	m.startOnce.Do(func() {
		m.system = actor.NewActorSystemWithConfig(actor.SystemConfig{
			MailboxCapacity: m.mailboxSize,
		})

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
		m.key.Spawn(m.system, managerActorID, m)

		refs := actor.FindInReceptionist(
			m.system.Receptionist(), m.key,
		)
		if len(refs) == 0 {
			panic("unable to spawn header manager actor")
		}
		m.ref = refs[0]
	})
}

// Stop stops the manager actor.
func (m *ManagerActor) Stop() {
	m.stopOnce.Do(func() {
		if m.system != nil {
			_ = m.system.Shutdown()
		}
		if m.machine != nil {
			m.machine.Stop()
		}
	})
}

// Receive processes one manager actor message.
func (m *ManagerActor) Receive(ctx context.Context,
	msg ManagerMsg) fn.Result[ManagerResp] {

	if m.machine == nil {
		return fn.Err[ManagerResp](ErrActorStopped)
	}

	outbox, err := m.machine.ProcessEvent(ctx, msg)
	if err != nil {
		return fn.Err[ManagerResp](err)
	}

	var resp ManagerResp = &ManagerAck{}
	for _, out := range outbox {
		switch out := out.(type) {
		case managerOutboxResponse:
			resp = out.Response

		default:
			return fn.Err[ManagerResp](
				fmt.Errorf("unknown manager outbox: %T", out),
			)
		}
	}

	if m.events != nil {
		if event, ok := eventForManagerMessage(m.fsm, msg, resp); ok {
			m.events.RecordHeaderSyncEvent(event)
		}
	}

	return fn.Ok(resp)
}

func (m *ManagerActor) ask(ctx context.Context, msg ManagerMsg) (
	ManagerResp, error) {

	if m.ref == nil {
		return nil, ErrActorStopped
	}

	return m.ref.Ask(ctx, msg).Await(ctx).Unpack()
}

// AddAnchor asks the manager to add or confirm an anchor.
func (m *ManagerActor) AddAnchor(ctx context.Context, height uint32,
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

// Anchors asks the manager actor for the current known anchors.
func (m *ManagerActor) Anchors(ctx context.Context) ([]Anchor, error) {
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
func (m *ManagerActor) PlanRange(ctx context.Context, id RangeID,
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
func (m *ManagerActor) StageDiscoveredRange(ctx context.Context,
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

// PlanConfirmedAnchorRanges asks the manager to plan all bounded adjacent
// ranges between confirmed anchors in [startHeight, stopHeight].
func (m *ManagerActor) PlanConfirmedAnchorRanges(ctx context.Context,
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

// AddPeer asks the manager actor to add a peer.
func (m *ManagerActor) AddPeer(ctx context.Context,
	peer PeerSnapshot) error {

	_, err := m.ask(ctx, &AddPeerMsg{Peer: peer})

	return err
}

// RemovePeer asks the manager actor to remove a peer.
func (m *ManagerActor) RemovePeer(ctx context.Context, id string) error {
	_, err := m.ask(ctx, &RemovePeerMsg{ID: id})

	return err
}

// UpdatePeerState asks the manager actor to update peer state.
func (m *ManagerActor) UpdatePeerState(ctx context.Context, id string,
	state PeerState) error {

	_, err := m.ask(ctx, &UpdatePeerStateMsg{
		ID:    id,
		State: state,
	})

	return err
}

// UpdatePeerRTT asks the manager actor to update peer RTT.
func (m *ManagerActor) UpdatePeerRTT(ctx context.Context, id string,
	rtt time.Duration) error {

	_, err := m.ask(ctx, &UpdatePeerRTTMsg{ID: id, RTT: rtt})

	return err
}

// AssignNextQueuedRange asks the manager to assign global queued work.
func (m *ManagerActor) AssignNextQueuedRange(ctx context.Context) (
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
func (m *ManagerActor) StartPeerRange(ctx context.Context,
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
func (m *ManagerActor) StealNextRange(ctx context.Context,
	thiefID string) (StealResult, bool, error) {

	return m.StealRange(ctx, thiefID, NoRange)
}

// StealRange asks the manager to steal a specific range, or any stealable
// range when rangeID is NoRange.
func (m *ManagerActor) StealRange(ctx context.Context, thiefID string,
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
func (m *ManagerActor) CompleteRange(ctx context.Context, peerID string,
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
func (m *ManagerActor) CommitReadyRanges(ctx context.Context) (
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

// ReadyRanges asks the manager which staged ranges are ready for side-effectful
// persistence before the FSM-visible tip advances.
func (m *ManagerActor) ReadyRanges(ctx context.Context) ([]RangeID, error) {
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
func (m *ManagerActor) SnapshotPeer(ctx context.Context,
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
func (m *ManagerActor) SnapshotRange(ctx context.Context,
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
func (m *ManagerActor) Metrics(ctx context.Context) (Metrics, error) {
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
func (m *ManagerActor) CommitStats(ctx context.Context,
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

// PeerMsg is accepted by a header peer actor.
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

// PeerResp is returned by peer actor messages.
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
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerReady,
			RangeAssignment{}), peerStartRangeOutbox{PeerID: env.id})

	case PeerNeedStealEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerReady,
			RangeAssignment{}), peerStealRangeOutbox{ThiefID: env.id})

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
		next := peerBlockedState{rtt: s.rtt, active: s.active}

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerBlocked, s.active))

	case PeerQuarantinedEvent:
		next := peerQuarantinedState{rtt: s.rtt, active: s.active}

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerQuarantined, s.active))

	case PeerDownEvent:
		next := peerDownState{rtt: s.rtt, active: s.active}

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
		return completePeerRangeTransition(env, s.rtt, PeerReady, s.active, m)

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
		next := peerQuarantinedState{rtt: s.rtt, active: s.active}

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerQuarantined, s.active))

	case PeerDownEvent:
		next := peerDownState{rtt: s.rtt, active: s.active}

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
		return completePeerRangeTransition(env, s.rtt, PeerBlocked, s.active, m)

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
		next := peerBlockedState{rtt: s.rtt, active: s.active}

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerBlocked, s.active))

	case PeerQuarantinedEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt,
			PeerQuarantined, s.active))

	case PeerDownEvent:
		next := peerDownState{rtt: s.rtt, active: s.active}

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
		return completePeerRangeTransition(env, s.rtt, PeerQuarantined, s.active, m)

	case PeerRangeStolenEvent:
		return clearStolenPeerRangeTransition(
			env, s.rtt, PeerQuarantined, s.active, m,
		)

	case PeerNeedRangeEvent, PeerNeedStealEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt,
			PeerQuarantined, s.active))

	case PeerRangeAssignedEvent:
		return nil, fmt.Errorf("quarantined peer %s cannot accept range",
			env.id)

	default:
		return nil, fmt.Errorf("quarantined peer cannot process %T", msg)
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
		next := peerBlockedState{rtt: s.rtt, active: s.active}

		return peerTransition(next, peerSnapshot(env, s.rtt,
			PeerBlocked, s.active))

	case PeerQuarantinedEvent:
		next := peerQuarantinedState{rtt: s.rtt, active: s.active}

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
		return completePeerRangeTransition(env, s.rtt, PeerDown, s.active, m)

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

func completePeerRangeTransition(env *peerEnv, rtt time.Duration,
	state PeerState, active RangeAssignment, event PeerRangeCompletedEvent) (
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

	return peerTransition(peerStateFromSnapshot(state, rtt, RangeAssignment{}),
		peerSnapshot(env, rtt, state, RangeAssignment{}))
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

// PeerActor owns peer-local header sync state and communicates with the manager
// only through actor messages emitted from FSM outboxes.
type PeerActor struct {
	id          string
	rank        int
	manager     *ManagerActor
	mailboxSize int

	key     actor.ServiceKey[PeerMsg, PeerResp]
	system  *actor.ActorSystem
	machine *protofsm.StateMachine[PeerMsg, peerOutbox, *peerEnv]
	ref     actor.ActorRef[PeerMsg, PeerResp]

	startOnce sync.Once
	stopOnce  sync.Once
}

var _ actor.ActorBehavior[PeerMsg, PeerResp] = (*PeerActor)(nil)

// NewPeerActor creates a peer actor.
func NewPeerActor(id string, rank int, manager *ManagerActor,
	mailboxSize int) *PeerActor {

	if mailboxSize <= 0 {
		mailboxSize = 1
	}

	return &PeerActor{
		id:          id,
		rank:        rank,
		manager:     manager,
		mailboxSize: mailboxSize,
		key: actor.NewServiceKey[PeerMsg, PeerResp](
			peerActorKey + "-" + id,
		),
	}
}

// Start starts the peer actor and registers its initial snapshot.
func (p *PeerActor) Start(ctx context.Context) error {
	var startErr error
	p.startOnce.Do(func() {
		p.system = actor.NewActorSystemWithConfig(actor.SystemConfig{
			MailboxCapacity: p.mailboxSize,
		})

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
			startErr = err
			return
		}

		p.machine = machine
		p.machine.Start(ctx)
		p.key.Spawn(p.system, p.id, p)

		refs := actor.FindInReceptionist(
			p.system.Receptionist(), p.key,
		)
		if len(refs) == 0 {
			startErr = fmt.Errorf("unable to spawn header peer %s",
				p.id)
			return
		}
		p.ref = refs[0]
	})
	if startErr != nil {
		return startErr
	}

	return p.Send(ctx, PeerReadyEvent{})
}

// Stop stops the peer actor.
func (p *PeerActor) Stop() {
	p.stopOnce.Do(func() {
		if p.system != nil {
			_ = p.system.Shutdown()
		}
		if p.machine != nil {
			p.machine.Stop()
		}
	})
}

// Receive processes one peer actor message.
func (p *PeerActor) Receive(ctx context.Context,
	msg PeerMsg) fn.Result[PeerResp] {

	if p.machine == nil {
		return fn.Err[PeerResp](ErrActorStopped)
	}

	outbox, err := p.machine.ProcessEvent(ctx, msg)
	if err != nil {
		return fn.Err[PeerResp](err)
	}

	resp, err := p.applyPeerOutbox(ctx, outbox)
	if err != nil {
		return fn.Err[PeerResp](err)
	}

	return fn.Ok(resp)
}

func (p *PeerActor) applyPeerOutbox(ctx context.Context,
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
				if _, err := p.applyPeerOutbox(ctx, followUp); err != nil {
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

// Send applies an event to the peer actor.
func (p *PeerActor) Send(ctx context.Context, event PeerMsg) error {
	if p.ref == nil {
		return ErrActorStopped
	}

	_, err := p.ref.Ask(ctx, event).Await(ctx).Unpack()

	return err
}

// RequestRange asks the peer actor to request its next local range from the
// manager.
func (p *PeerActor) RequestRange(ctx context.Context) (
	RangeAssignment, bool, error) {

	if p.ref == nil {
		return RangeAssignment{}, false, ErrActorStopped
	}

	resp, err := p.ref.Ask(
		ctx, PeerNeedRangeEvent{},
	).Await(ctx).Unpack()
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

// RequestStolenRange asks the peer actor to ask the manager for stealable work.
func (p *PeerActor) RequestStolenRange(ctx context.Context) (
	StealResult, bool, error) {

	if p.ref == nil {
		return StealResult{}, false, ErrActorStopped
	}

	resp, err := p.ref.Ask(
		ctx, PeerNeedStealEvent{},
	).Await(ctx).Unpack()
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
func (p *PeerActor) CompleteRange(ctx context.Context, rangeID RangeID,
	leaseEpoch uint64, valid bool, at time.Time) (CompleteResult, error) {

	if p.ref == nil {
		return CompleteResult{}, ErrActorStopped
	}

	resp, err := p.ref.Ask(ctx, PeerRangeCompletedEvent{
		RangeID:    rangeID,
		LeaseEpoch: leaseEpoch,
		Valid:      valid,
		At:         at,
	}).Await(ctx).Unpack()
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
