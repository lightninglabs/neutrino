package querysync

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
	managerActorID = "querysync-manager"
	peerActorKey   = "querysync-peer"
)

// ManagerMsg is the message type accepted by the scheduler manager actor.
type ManagerMsg interface {
	actor.Message

	managerMsg()
}

type managerMessage struct {
	actor.BaseMessage
}

func (managerMessage) managerMsg() {}

func (managerMessage) MessageType() string {
	return "querysync.ManagerMsg"
}

// ManagerResp is the response type returned by the scheduler manager actor.
type ManagerResp interface {
	managerResp()
}

type managerResponse struct{}

func (managerResponse) managerResp() {}

// ManagerAck confirms a mutating manager message was applied.
type ManagerAck struct {
	managerResponse
}

// AddPeerMsg asks the manager to add or replace a peer snapshot.
type AddPeerMsg struct {
	managerMessage

	Peer PeerSnapshot
}

// EnqueueMsg asks the manager to enqueue global work.
type EnqueueMsg struct {
	managerMessage

	Task Task
}

// UpdatePeerStateMsg asks the manager to update peer scheduling state.
type UpdatePeerStateMsg struct {
	managerMessage

	ID    string
	State PeerState
}

// SetPeerLocalWorkMsg asks the manager to replace a peer's local work.
type SetPeerLocalWorkMsg struct {
	managerMessage

	ID   string
	Work []TaskID
}

// RemovePeerMsg asks the manager to remove a peer.
type RemovePeerMsg struct {
	managerMessage

	ID string
}

// UpdatePeerRTTMsg asks the manager to update a peer RTT sample.
type UpdatePeerRTTMsg struct {
	managerMessage

	ID  string
	RTT time.Duration
}

// CompletePeerWorkMsg asks the manager to mark a peer ready after completion.
type CompletePeerWorkMsg struct {
	managerMessage

	ID string
}

// CancelBatchMsg asks the manager to mark queued work in a batch canceled.
type CancelBatchMsg struct {
	managerMessage

	BatchID uint64
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

// MetricsMsg asks the manager for current scheduler metrics.
type MetricsMsg struct {
	managerMessage
}

// MetricsResp is the response to MetricsMsg.
type MetricsResp struct {
	managerResponse

	Metrics Metrics
}

// DispatchMsg asks the manager for one scheduling decision.
type DispatchMsg struct {
	managerMessage
}

// DispatchTaskMsg asks the manager to dispatch one externally owned task.
type DispatchTaskMsg struct {
	managerMessage

	Task Task
}

// AssignLocalTaskMsg asks the manager to assign an externally queued task to a
// peer-local owned queue without marking the peer busy.
type AssignLocalTaskMsg struct {
	managerMessage

	Task Task
}

// DispatchResp is the response to DispatchMsg.
type DispatchResp struct {
	managerResponse

	Assignment Assignment
	OK         bool
}

// StealMsg asks the manager to move stealable work to the thief peer.
type StealMsg struct {
	managerMessage

	ThiefID string
}

// StealResp is the response to StealMsg.
type StealResp struct {
	managerResponse

	Result StealResult
	OK     bool
}

type managerOutbox interface {
	managerOutbox()
}

type managerOutboxResponse struct {
	Response ManagerResp
}

func (managerOutboxResponse) managerOutbox() {}

type managerEnv struct {
	fsm *SchedulerFSM
}

type managerState struct{}

func (managerState) String() string {
	return "manager"
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
			ManagerMsg,
			managerOutbox,
			*managerEnv,
		]{
			NextState: managerState{},
			NewEvents: fn.Some(protofsm.EmittedEvent[
				ManagerMsg,
				managerOutbox,
			]{
				Outbox: []managerOutbox{
					managerOutboxResponse{Response: resp},
				},
			}),
		}, nil
	}

	switch m := msg.(type) {
	case *AddPeerMsg:
		if err := env.fsm.AddPeer(m.Peer); err != nil {
			return nil, err
		}

		return respond(&ManagerAck{})

	case *EnqueueMsg:
		env.fsm.Enqueue(m.Task)

		return respond(&ManagerAck{})

	case *UpdatePeerStateMsg:
		if err := env.fsm.UpdatePeerState(m.ID, m.State); err != nil {
			return nil, err
		}

		return respond(&ManagerAck{})

	case *SetPeerLocalWorkMsg:
		if err := env.fsm.SetPeerLocalWork(m.ID, m.Work); err != nil {
			return nil, err
		}

		return respond(&ManagerAck{})

	case *RemovePeerMsg:
		env.fsm.RemovePeer(m.ID)

		return respond(&ManagerAck{})

	case *UpdatePeerRTTMsg:
		if err := env.fsm.UpdatePeerRTT(m.ID, m.RTT); err != nil {
			return nil, err
		}

		return respond(&ManagerAck{})

	case *CompletePeerWorkMsg:
		if err := env.fsm.CompletePeerWork(m.ID); err != nil {
			return nil, err
		}

		return respond(&ManagerAck{})

	case *CancelBatchMsg:
		env.fsm.CancelBatch(m.BatchID)

		return respond(&ManagerAck{})

	case *SnapshotPeerMsg:
		peer, ok := env.fsm.SnapshotPeer(m.ID)

		return respond(&SnapshotPeerResp{
			Peer: peer,
			OK:   ok,
		})

	case *MetricsMsg:
		return respond(&MetricsResp{Metrics: env.fsm.Metrics()})

	case *DispatchMsg:
		assignment, ok := env.fsm.DispatchNext()

		return respond(&DispatchResp{
			Assignment: assignment,
			OK:         ok,
		})

	case *DispatchTaskMsg:
		assignment, ok := env.fsm.DispatchTask(m.Task)

		return respond(&DispatchResp{
			Assignment: assignment,
			OK:         ok,
		})

	case *AssignLocalTaskMsg:
		assignment, ok := env.fsm.AssignLocalTask(m.Task)

		return respond(&DispatchResp{
			Assignment: assignment,
			OK:         ok,
		})

	case *StealMsg:
		result, ok := env.fsm.StealOne(m.ThiefID)

		return respond(&StealResp{
			Result: result,
			OK:     ok,
		})

	default:
		return nil, fmt.Errorf("unknown manager message: %T", msg)
	}
}

// ManagerActor owns the scheduler FSM and exposes it through lnd's actor
// system. Public helper methods below are thin ask wrappers used by existing
// tests and integration code; peer actors communicate with the manager through
// the same actor reference.
type ManagerActor struct {
	fsm         *SchedulerFSM
	mailboxSize int

	key     actor.ServiceKey[ManagerMsg, ManagerResp]
	system  *actor.ActorSystem
	machine *protofsm.StateMachine[ManagerMsg, managerOutbox, *managerEnv]
	ref     actor.ActorRef[ManagerMsg, ManagerResp]
	trace   *TraceRecorder
	events  SchedulerEventSink

	startOnce sync.Once
	stopOnce  sync.Once
}

var _ actor.ActorBehavior[ManagerMsg, ManagerResp] = (*ManagerActor)(nil)

// NewManagerActor creates an actor wrapper around a SchedulerFSM.
func NewManagerActor(fsm *SchedulerFSM, mailboxSize int) *ManagerActor {
	if mailboxSize <= 0 {
		mailboxSize = 1
	}

	return &ManagerActor{
		fsm:         fsm,
		mailboxSize: mailboxSize,
		key:         actor.NewServiceKey[ManagerMsg, ManagerResp](managerActorID),
	}
}

// SetTraceRecorder enables durable actor-level scheduler traces. It should be
// called before Start so all lifecycle messages are captured.
func (m *ManagerActor) SetTraceRecorder(trace *TraceRecorder) {
	m.trace = trace
}

// SetEventSink enables structured scheduler events for metrics and validation.
// It should be called before Start so lifecycle events are captured from the
// beginning of the actor run.
func (m *ManagerActor) SetEventSink(events SchedulerEventSink) {
	m.events = events
}

// Start starts the manager actor.
func (m *ManagerActor) Start(ctx context.Context) {
	m.startOnce.Do(func() {
		m.system = actor.NewActorSystemWithConfig(actor.SystemConfig{
			MailboxCapacity: m.mailboxSize,
		})

		machine, err := protofsm.NewStateMachine[
			ManagerMsg,
			managerOutbox,
		](protofsm.StateMachineCfg[
			ManagerMsg,
			managerOutbox,
			*managerEnv,
		]{
			InitialState: managerState{},
			Env: &managerEnv{
				fsm: m.fsm,
			},
		})
		if err != nil {
			panic(fmt.Sprintf("unable to create manager FSM: %v", err))
		}

		m.machine = machine
		m.machine.Start(ctx)

		m.key.Spawn(m.system, managerActorID, m)

		refs := actor.FindInReceptionist(
			m.system.Receptionist(), m.key,
		)
		if len(refs) == 0 {
			panic("unable to spawn manager actor")
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
		switch msg := out.(type) {
		case managerOutboxResponse:
			resp = msg.Response

		default:
			return fn.Err[ManagerResp](
				fmt.Errorf("unknown manager outbox: %T", out),
			)
		}
	}

	if event, ok := traceEventForManagerMessage(m.fsm, msg, resp); ok {
		m.trace.Record(event)
	}
	if m.events != nil {
		if event, ok := schedulerEventForManagerMessage(
			m.fsm, msg, resp,
		); ok {
			m.events.RecordSchedulerEvent(event)
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

// AddPeer asks the manager actor to add a peer.
func (m *ManagerActor) AddPeer(ctx context.Context,
	peer PeerSnapshot) error {

	_, err := m.ask(ctx, &AddPeerMsg{Peer: peer})

	return err
}

// Enqueue asks the manager actor to enqueue a task.
func (m *ManagerActor) Enqueue(ctx context.Context, task Task) error {
	_, err := m.ask(ctx, &EnqueueMsg{Task: task})

	return err
}

// UpdatePeerState asks the manager actor to update a peer state.
func (m *ManagerActor) UpdatePeerState(ctx context.Context, id string,
	state PeerState) error {

	_, err := m.ask(ctx, &UpdatePeerStateMsg{
		ID:    id,
		State: state,
	})

	return err
}

// SetPeerLocalWork asks the manager actor to set a peer's local work.
func (m *ManagerActor) SetPeerLocalWork(ctx context.Context, id string,
	work []TaskID) error {

	_, err := m.ask(ctx, &SetPeerLocalWorkMsg{
		ID:   id,
		Work: work,
	})

	return err
}

// RemovePeer asks the manager actor to remove a peer.
func (m *ManagerActor) RemovePeer(ctx context.Context, id string) error {
	_, err := m.ask(ctx, &RemovePeerMsg{ID: id})

	return err
}

// UpdatePeerRTT asks the manager actor to update the latest peer RTT sample.
func (m *ManagerActor) UpdatePeerRTT(ctx context.Context, id string,
	rtt time.Duration) error {

	_, err := m.ask(ctx, &UpdatePeerRTTMsg{
		ID:  id,
		RTT: rtt,
	})

	return err
}

// CompletePeerWork asks the manager actor to mark a peer ready after its
// active assignment finished.
func (m *ManagerActor) CompletePeerWork(ctx context.Context,
	id string) error {

	_, err := m.ask(ctx, &CompletePeerWorkMsg{ID: id})

	return err
}

// CancelBatch asks the manager actor to mark queued work in the batch canceled.
func (m *ManagerActor) CancelBatch(ctx context.Context,
	batchID uint64) error {

	_, err := m.ask(ctx, &CancelBatchMsg{BatchID: batchID})

	return err
}

// SnapshotPeer asks the manager actor for a stable copy of one peer.
func (m *ManagerActor) SnapshotPeer(ctx context.Context,
	id string) (PeerSnapshot, bool, error) {

	resp, err := m.ask(ctx, &SnapshotPeerMsg{ID: id})
	if err != nil {
		return PeerSnapshot{}, false, err
	}

	snapshot, ok := resp.(*SnapshotPeerResp)
	if !ok {
		return PeerSnapshot{}, false, fmt.Errorf(
			"unexpected snapshot response: %T", resp,
		)
	}

	return snapshot.Peer, snapshot.OK, nil
}

// Metrics asks the manager actor for current scheduler counters.
func (m *ManagerActor) Metrics(ctx context.Context) (Metrics, error) {
	resp, err := m.ask(ctx, &MetricsMsg{})
	if err != nil {
		return Metrics{}, err
	}

	metrics, ok := resp.(*MetricsResp)
	if !ok {
		return Metrics{}, fmt.Errorf("unexpected metrics response: %T",
			resp)
	}

	return metrics.Metrics, nil
}

// AskDispatch asks the manager actor for one scheduling decision.
func (m *ManagerActor) AskDispatch(ctx context.Context) (Assignment, bool,
	error) {

	resp, err := m.ask(ctx, &DispatchMsg{})
	if err != nil {
		return Assignment{}, false, err
	}

	dispatch, ok := resp.(*DispatchResp)
	if !ok {
		return Assignment{}, false, fmt.Errorf(
			"unexpected dispatch response: %T", resp,
		)
	}

	return dispatch.Assignment, dispatch.OK, nil
}

// AskDispatchTask asks the manager actor to dispatch one externally owned task.
func (m *ManagerActor) AskDispatchTask(ctx context.Context,
	task Task) (Assignment, bool, error) {

	resp, err := m.ask(ctx, &DispatchTaskMsg{Task: task})
	if err != nil {
		return Assignment{}, false, err
	}

	dispatch, ok := resp.(*DispatchResp)
	if !ok {
		return Assignment{}, false, fmt.Errorf(
			"unexpected dispatch response: %T", resp,
		)
	}

	return dispatch.Assignment, dispatch.OK, nil
}

// AskAssignLocalTask asks the manager actor to assign one externally queued
// task to a peer-local owned queue.
func (m *ManagerActor) AskAssignLocalTask(ctx context.Context,
	task Task) (Assignment, bool, error) {

	resp, err := m.ask(ctx, &AssignLocalTaskMsg{Task: task})
	if err != nil {
		return Assignment{}, false, err
	}

	dispatch, ok := resp.(*DispatchResp)
	if !ok {
		return Assignment{}, false, fmt.Errorf(
			"unexpected local assignment response: %T", resp,
		)
	}

	return dispatch.Assignment, dispatch.OK, nil
}

// AskSteal asks the manager actor to perform one work-stealing transition for
// the thief peer.
func (m *ManagerActor) AskSteal(ctx context.Context,
	thiefID string) (StealResult, bool, error) {

	resp, err := m.ask(ctx, &StealMsg{ThiefID: thiefID})
	if err != nil {
		return StealResult{}, false, err
	}

	steal, ok := resp.(*StealResp)
	if !ok {
		return StealResult{}, false, fmt.Errorf(
			"unexpected steal response: %T", resp,
		)
	}

	return steal.Result, steal.OK, nil
}

// PeerMsg is the message type accepted by peer actors.
type PeerMsg interface {
	actor.Message

	peerMsg()
}

type peerMessage struct {
	actor.BaseMessage
}

func (peerMessage) peerMsg() {}

func (peerMessage) MessageType() string {
	return "querysync.PeerMsg"
}

// PeerEvent is applied by PeerFSM and also sent through PeerActor.
type PeerEvent interface {
	PeerMsg

	apply(*PeerFSM) error
}

// PeerReadyEvent marks the peer as ready for more work.
type PeerReadyEvent struct {
	peerMessage
}

// PeerBlockedEvent marks a connected peer as temporarily unable to accept a
// manager assignment.
type PeerBlockedEvent struct {
	peerMessage
}

// PeerQuarantinedEvent marks a peer as intentionally excluded from scheduling
// after repeated non-responses.
type PeerQuarantinedEvent struct {
	peerMessage
}

// PeerDownEvent marks the peer as disconnected.
type PeerDownEvent struct {
	peerMessage
}

// PeerRTTEvent records the latest ping RTT sample.
type PeerRTTEvent struct {
	peerMessage

	RTT time.Duration
}

// PeerRankEvent records the latest manager rank for this peer. Production
// ranking changes as peers succeed or fail, so the peer actor carries rank as
// explicit state rather than treating the constructor value as permanent.
type PeerRankEvent struct {
	peerMessage

	Rank int
}

// PeerAssignedEvent records that the peer has been assigned work.
type PeerAssignedEvent struct {
	peerMessage

	TaskID TaskID
}

// PeerNeedWorkEvent is emitted when a ready peer has drained local work and
// wants the manager to attempt a work-steal transition.
type PeerNeedWorkEvent struct {
	peerMessage
}

// PeerResp is the response type returned by peer actors.
type PeerResp interface {
	peerResp()
}

type peerResponse struct{}

func (peerResponse) peerResp() {}

// PeerAck confirms a peer event was applied.
type PeerAck struct {
	peerResponse
}

// PeerStealResp returns the result of a peer-initiated steal request.
type PeerStealResp struct {
	peerResponse

	Result StealResult
	OK     bool
}

type peerOutbox interface {
	peerOutbox()
}

type peerSnapshotOutbox struct {
	Peer PeerSnapshot
}

func (peerSnapshotOutbox) peerOutbox() {}

type peerStealRequestOutbox struct {
	ThiefID string
}

func (peerStealRequestOutbox) peerOutbox() {}

type peerEnv struct {
	id   string
	rank int
}

type peerReadyState struct {
	rtt time.Duration
}

func (s peerReadyState) String() string {
	return "peer-ready"
}

func (s peerReadyState) IsTerminal() bool {
	return false
}

func (s peerReadyState) ProcessEvent(_ context.Context, msg PeerMsg,
	env *peerEnv) (*protofsm.StateTransition[
	PeerMsg, peerOutbox, *peerEnv], error) {

	switch m := msg.(type) {
	case PeerReadyEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerReady))

	case PeerBlockedEvent:
		next := peerBlockedState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerBlocked))

	case PeerQuarantinedEvent:
		next := peerQuarantinedState{rtt: s.rtt}

		return peerTransition(
			next, peerSnapshot(env, s.rtt, PeerQuarantined),
		)

	case PeerDownEvent:
		next := peerDownState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerDown))

	case PeerRTTEvent:
		if m.RTT < 0 {
			return nil, fmt.Errorf("negative RTT: %v", m.RTT)
		}

		next := peerReadyState{rtt: m.RTT}

		return peerTransition(next, peerSnapshot(env, m.RTT, PeerReady))

	case PeerRankEvent:
		env.rank = m.Rank

		return peerTransition(s, peerSnapshot(env, s.rtt, PeerReady))

	case PeerAssignedEvent:
		next := peerBusyState{
			rtt:    s.rtt,
			active: m.TaskID,
		}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerBusy))

	case PeerNeedWorkEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerReady),
			peerStealRequestOutbox{ThiefID: env.id})

	default:
		return nil, fmt.Errorf("ready peer cannot process %T", msg)
	}
}

type peerBusyState struct {
	rtt    time.Duration
	active TaskID
}

func (s peerBusyState) String() string {
	return "peer-busy"
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

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerReady))

	case PeerBlockedEvent:
		next := peerBlockedState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerBlocked))

	case PeerQuarantinedEvent:
		next := peerQuarantinedState{rtt: s.rtt}

		return peerTransition(
			next, peerSnapshot(env, s.rtt, PeerQuarantined),
		)

	case PeerDownEvent:
		next := peerDownState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerDown))

	case PeerRTTEvent:
		if m.RTT < 0 {
			return nil, fmt.Errorf("negative RTT: %v", m.RTT)
		}

		next := peerBusyState{
			rtt:    m.RTT,
			active: s.active,
		}

		return peerTransition(next, peerSnapshot(env, m.RTT, PeerBusy))

	case PeerRankEvent:
		env.rank = m.Rank

		return peerTransition(s, peerSnapshot(env, s.rtt, PeerBusy))

	case PeerAssignedEvent:
		return nil, fmt.Errorf("peer %s cannot accept work while busy",
			env.id)

	case PeerNeedWorkEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerBusy))

	default:
		return nil, fmt.Errorf("busy peer cannot process %T", msg)
	}
}

type peerBlockedState struct {
	rtt time.Duration
}

func (s peerBlockedState) String() string {
	return "peer-blocked"
}

func (s peerBlockedState) IsTerminal() bool {
	return false
}

func (s peerBlockedState) ProcessEvent(_ context.Context, msg PeerMsg,
	env *peerEnv) (*protofsm.StateTransition[
	PeerMsg, peerOutbox, *peerEnv], error) {

	switch m := msg.(type) {
	case PeerReadyEvent:
		next := peerReadyState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerReady))

	case PeerBlockedEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerBlocked))

	case PeerQuarantinedEvent:
		next := peerQuarantinedState{rtt: s.rtt}

		return peerTransition(
			next, peerSnapshot(env, s.rtt, PeerQuarantined),
		)

	case PeerDownEvent:
		next := peerDownState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerDown))

	case PeerRTTEvent:
		if m.RTT < 0 {
			return nil, fmt.Errorf("negative RTT: %v", m.RTT)
		}

		next := peerBlockedState{rtt: m.RTT}

		return peerTransition(next, peerSnapshot(env, m.RTT, PeerBlocked))

	case PeerRankEvent:
		env.rank = m.Rank

		return peerTransition(s, peerSnapshot(env, s.rtt, PeerBlocked))

	case PeerAssignedEvent:
		return nil, fmt.Errorf("peer %s cannot accept work while blocked",
			env.id)

	case PeerNeedWorkEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerBlocked))

	default:
		return nil, fmt.Errorf("blocked peer cannot process %T", msg)
	}
}

type peerQuarantinedState struct {
	rtt time.Duration
}

func (s peerQuarantinedState) String() string {
	return "peer-quarantined"
}

func (s peerQuarantinedState) IsTerminal() bool {
	return false
}

func (s peerQuarantinedState) ProcessEvent(_ context.Context, msg PeerMsg,
	env *peerEnv) (*protofsm.StateTransition[
	PeerMsg, peerOutbox, *peerEnv], error) {

	switch m := msg.(type) {
	case PeerReadyEvent:
		next := peerReadyState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerReady))

	case PeerBlockedEvent:
		next := peerBlockedState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerBlocked))

	case PeerQuarantinedEvent:
		return peerTransition(
			s, peerSnapshot(env, s.rtt, PeerQuarantined),
		)

	case PeerDownEvent:
		next := peerDownState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerDown))

	case PeerRTTEvent:
		if m.RTT < 0 {
			return nil, fmt.Errorf("negative RTT: %v", m.RTT)
		}

		next := peerQuarantinedState{rtt: m.RTT}

		return peerTransition(
			next, peerSnapshot(env, m.RTT, PeerQuarantined),
		)

	case PeerRankEvent:
		env.rank = m.Rank

		return peerTransition(
			s, peerSnapshot(env, s.rtt, PeerQuarantined),
		)

	case PeerAssignedEvent:
		return nil, fmt.Errorf(
			"peer %s cannot accept work while quarantined", env.id,
		)

	case PeerNeedWorkEvent:
		return peerTransition(
			s, peerSnapshot(env, s.rtt, PeerQuarantined),
		)

	default:
		return nil, fmt.Errorf(
			"quarantined peer cannot process %T", msg,
		)
	}
}

type peerDownState struct {
	rtt time.Duration
}

func (s peerDownState) String() string {
	return "peer-down"
}

func (s peerDownState) IsTerminal() bool {
	return false
}

func (s peerDownState) ProcessEvent(_ context.Context, msg PeerMsg,
	env *peerEnv) (*protofsm.StateTransition[
	PeerMsg, peerOutbox, *peerEnv], error) {

	switch m := msg.(type) {
	case PeerReadyEvent:
		next := peerReadyState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerReady))

	case PeerBlockedEvent:
		next := peerBlockedState{rtt: s.rtt}

		return peerTransition(next, peerSnapshot(env, s.rtt, PeerBlocked))

	case PeerQuarantinedEvent:
		next := peerQuarantinedState{rtt: s.rtt}

		return peerTransition(
			next, peerSnapshot(env, s.rtt, PeerQuarantined),
		)

	case PeerDownEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerDown))

	case PeerRTTEvent:
		if m.RTT < 0 {
			return nil, fmt.Errorf("negative RTT: %v", m.RTT)
		}

		next := peerDownState{rtt: m.RTT}

		return peerTransition(next, peerSnapshot(env, m.RTT, PeerDown))

	case PeerRankEvent:
		env.rank = m.Rank

		return peerTransition(s, peerSnapshot(env, s.rtt, PeerDown))

	case PeerAssignedEvent:
		return nil, fmt.Errorf("peer %s cannot accept work while down",
			env.id)

	case PeerNeedWorkEvent:
		return peerTransition(s, peerSnapshot(env, s.rtt, PeerDown))

	default:
		return nil, fmt.Errorf("down peer cannot process %T", msg)
	}
}

func peerSnapshot(env *peerEnv, rtt time.Duration,
	state PeerState) peerSnapshotOutbox {

	return peerSnapshotOutbox{
		Peer: PeerSnapshot{
			ID:    env.id,
			Rank:  env.rank,
			RTT:   rtt,
			State: state,
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

// PeerActor owns peer-local state. When peer state changes, it emits outbox
// messages that are delivered to the manager actor through the actor system.
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

// Start starts the peer actor and registers its initial snapshot with the
// manager.
func (p *PeerActor) Start(ctx context.Context) error {
	var startErr error
	p.startOnce.Do(func() {
		p.system = actor.NewActorSystemWithConfig(actor.SystemConfig{
			MailboxCapacity: p.mailboxSize,
		})

		machine, err := protofsm.NewStateMachine[PeerMsg, peerOutbox](
			protofsm.StateMachineCfg[
				PeerMsg,
				peerOutbox,
				*peerEnv,
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
			startErr = fmt.Errorf("unable to spawn peer actor %s",
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

// Receive processes one peer actor message and dispatches peer outbox messages.
func (p *PeerActor) Receive(ctx context.Context,
	msg PeerMsg) fn.Result[PeerResp] {

	if p.machine == nil {
		return fn.Err[PeerResp](ErrActorStopped)
	}

	outbox, err := p.machine.ProcessEvent(ctx, msg)
	if err != nil {
		return fn.Err[PeerResp](err)
	}

	_, needsWork := msg.(PeerNeedWorkEvent)
	stealHandled := false
	resp := PeerResp(&PeerAck{})
	for _, out := range outbox {
		switch out := out.(type) {
		case peerSnapshotOutbox:
			if _, err := p.manager.ask(ctx, &AddPeerMsg{
				Peer: out.Peer,
			}); err != nil {
				return fn.Err[PeerResp](err)
			}

		case peerStealRequestOutbox:
			managerResp, err := p.manager.ask(ctx, &StealMsg{
				ThiefID: out.ThiefID,
			})
			if err != nil {
				return fn.Err[PeerResp](err)
			}

			steal, ok := managerResp.(*StealResp)
			if !ok {
				return fn.Err[PeerResp](
					fmt.Errorf("unexpected steal response: %T",
						managerResp),
				)
			}

			stealHandled = true
			resp = &PeerStealResp{
				Result: steal.Result,
				OK:     steal.OK,
			}

		default:
			return fn.Err[PeerResp](
				fmt.Errorf("unknown peer outbox: %T", out),
			)
		}
	}

	if needsWork && !stealHandled {
		resp = &PeerStealResp{}
	}

	return fn.Ok(resp)
}

// Send applies an event to the peer actor.
func (p *PeerActor) Send(ctx context.Context, event PeerEvent) error {
	if p.ref == nil {
		return ErrActorStopped
	}

	_, err := p.ref.Ask(ctx, event).Await(ctx).Unpack()

	return err
}

// RequestStolenWork asks this peer actor to emit a work-stealing outbox
// request to the manager actor.
func (p *PeerActor) RequestStolenWork(ctx context.Context) (StealResult, bool,
	error) {

	if p.ref == nil {
		return StealResult{}, false, ErrActorStopped
	}

	resp, err := p.ref.Ask(ctx, PeerNeedWorkEvent{}).Await(ctx).Unpack()
	if err != nil {
		return StealResult{}, false, err
	}

	steal, ok := resp.(*PeerStealResp)
	if !ok {
		return StealResult{}, false, fmt.Errorf(
			"unexpected peer steal response: %T", resp,
		)
	}

	return steal.Result, steal.OK, nil
}
