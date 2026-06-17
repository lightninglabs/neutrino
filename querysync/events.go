package querysync

import (
	"sync"
	"time"
)

// SchedulerEventType identifies one observable scheduler manager transition.
type SchedulerEventType string

const (
	// SchedulerEventPeerAdded is emitted when the manager records a peer.
	SchedulerEventPeerAdded SchedulerEventType = "peer_added"

	// SchedulerEventPeerRemoved is emitted when the manager removes a peer.
	SchedulerEventPeerRemoved SchedulerEventType = "peer_removed"

	// SchedulerEventPeerStateUpdated is emitted when peer lifecycle state
	// changes.
	SchedulerEventPeerStateUpdated SchedulerEventType = "peer_state_updated"

	// SchedulerEventPeerRTTUpdated is emitted when the manager receives a
	// fresh peer RTT sample.
	SchedulerEventPeerRTTUpdated SchedulerEventType = "peer_rtt_updated"

	// SchedulerEventPeerLocalWorkSet is emitted when a peer's owned local
	// work queue is replaced.
	SchedulerEventPeerLocalWorkSet SchedulerEventType = "peer_local_work_set"

	// SchedulerEventTaskEnqueued is emitted when global work is queued.
	SchedulerEventTaskEnqueued SchedulerEventType = "task_enqueued"

	// SchedulerEventBatchCanceled is emitted when queued work for a batch is
	// marked canceled.
	SchedulerEventBatchCanceled SchedulerEventType = "batch_canceled"

	// SchedulerEventPeerCompleted is emitted when a peer completes its
	// active work slot and returns to ready.
	SchedulerEventPeerCompleted SchedulerEventType = "peer_completed"

	// SchedulerEventTaskDispatched is emitted when the manager selects an
	// active peer assignment.
	SchedulerEventTaskDispatched SchedulerEventType = "task_dispatched"

	// SchedulerEventTaskOwned is emitted when the manager assigns queued work
	// to a peer-local owned queue.
	SchedulerEventTaskOwned SchedulerEventType = "task_owned"

	// SchedulerEventWorkStolen is emitted when unstarted local work moves
	// from a donor peer to an idle thief peer.
	SchedulerEventWorkStolen SchedulerEventType = "work_stolen"

	// SchedulerEventTaskLocalQueued is emitted by the production work
	// manager when a concrete job is placed into a peer-local queue.
	SchedulerEventTaskLocalQueued SchedulerEventType = "task_local_queued"

	// SchedulerEventTaskSent is emitted by the production work manager when
	// a peer actor's local job is accepted by the worker.
	SchedulerEventTaskSent SchedulerEventType = "task_sent"

	// SchedulerEventTaskResult is emitted by the production work manager
	// when a worker returns a result for an active job.
	SchedulerEventTaskResult SchedulerEventType = "task_result"

	// SchedulerEventWorkStealApplied is emitted after production has moved
	// concrete jobs according to a model work-steal decision.
	SchedulerEventWorkStealApplied SchedulerEventType = "work_steal_applied"

	// SchedulerEventPeerCooldown is emitted when a failed peer is put into
	// temporary retry-avoidance cooldown.
	SchedulerEventPeerCooldown SchedulerEventType = "peer_cooldown"

	// SchedulerEventPeerQuarantined is emitted when a peer crosses the
	// repeated non-response threshold and is excluded from scheduling.
	SchedulerEventPeerQuarantined SchedulerEventType = "peer_quarantined"
)

// SchedulerEvent is a structured, model-owned view of scheduler behavior. It is
// separate from TraceEvent: traces are durable replay inputs, while scheduler
// events are for production telemetry, validation summaries, and metrics.
type SchedulerEvent struct {
	Type SchedulerEventType
	At   time.Time

	PeerID  string
	ThiefID string
	DonorID string

	State PeerState
	Peer  *PeerSnapshot

	TaskID     TaskID
	TaskIDs    []TaskID
	BatchID    uint64
	Assignment *Assignment
	Steal      *StealResult

	RTT       time.Duration
	LocalWork []TaskID
	Timeout   time.Duration

	Err string

	TaskAge          time.Duration
	ActiveDuration   time.Duration
	LocalQueueAge    time.Duration
	StealAge         time.Duration
	DonorLocalAge    time.Duration
	ThiefLocalAge    time.Duration
	LocalWorkLen     int
	DonorLocalLen    int
	ThiefLocalLen    int
	StolenTaskCount  int
	StolenCount      uint16
	Attempts         uint8
	FailureCount     uint8
	CooldownDuration time.Duration
	Until            time.Time

	QueueDepth int
	Metrics    Metrics
	OK         bool
}

// SchedulerEventSink receives structured scheduler events.
type SchedulerEventSink interface {
	RecordSchedulerEvent(SchedulerEvent)
}

// SchedulerEventCallback adapts a function into a SchedulerEventSink.
type SchedulerEventCallback func(SchedulerEvent)

// RecordSchedulerEvent records a scheduler event by calling the callback.
func (c SchedulerEventCallback) RecordSchedulerEvent(event SchedulerEvent) {
	if c != nil {
		c(event)
	}
}

// SchedulerEventRecorder stores scheduler events for tests and validation
// harnesses.
type SchedulerEventRecorder struct {
	mu     sync.Mutex
	events []SchedulerEvent
}

// RecordSchedulerEvent appends one scheduler event.
func (r *SchedulerEventRecorder) RecordSchedulerEvent(event SchedulerEvent) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, cloneSchedulerEvent(event))
}

// Events returns a stable copy of all recorded scheduler events.
func (r *SchedulerEventRecorder) Events() []SchedulerEvent {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	events := make([]SchedulerEvent, len(r.events))
	for i := range r.events {
		events[i] = cloneSchedulerEvent(r.events[i])
	}

	return events
}

func cloneSchedulerEvent(event SchedulerEvent) SchedulerEvent {
	if event.Peer != nil {
		peer := event.Peer.clone()
		event.Peer = &peer
	}
	if event.Assignment != nil {
		assignment := *event.Assignment
		event.Assignment = &assignment
	}
	if event.Steal != nil {
		steal := *event.Steal
		steal.TaskIDs = append([]TaskID(nil), steal.TaskIDs...)
		event.Steal = &steal
	}
	event.TaskIDs = append([]TaskID(nil), event.TaskIDs...)
	event.LocalWork = append([]TaskID(nil), event.LocalWork...)

	return event
}

func schedulerEventForManagerMessage(fsm *SchedulerFSM, msg ManagerMsg,
	resp ManagerResp) (SchedulerEvent, bool) {

	base := SchedulerEvent{
		At: time.Now(),
	}
	if fsm != nil {
		base.QueueDepth = fsm.QueueLen()
		base.Metrics = fsm.Metrics()
	}

	switch msg := msg.(type) {
	case *AddPeerMsg:
		peer := msg.Peer.clone()
		base.Type = SchedulerEventPeerAdded
		base.PeerID = msg.Peer.ID
		base.State = msg.Peer.State
		base.Peer = &peer
		base.RTT = msg.Peer.RTT
		base.LocalWork = append([]TaskID(nil), msg.Peer.LocalWork...)

		return base, true

	case *RemovePeerMsg:
		base.Type = SchedulerEventPeerRemoved
		base.PeerID = msg.ID

		return base, true

	case *UpdatePeerStateMsg:
		base.Type = SchedulerEventPeerStateUpdated
		base.PeerID = msg.ID
		base.State = msg.State
		if fsm != nil {
			if peer, ok := fsm.SnapshotPeer(msg.ID); ok {
				base.Peer = &peer
				base.RTT = peer.RTT
				base.LocalWork = append(
					[]TaskID(nil), peer.LocalWork...,
				)
			}
		}

		return base, true

	case *UpdatePeerRTTMsg:
		base.Type = SchedulerEventPeerRTTUpdated
		base.PeerID = msg.ID
		base.RTT = msg.RTT
		if fsm != nil {
			if peer, ok := fsm.SnapshotPeer(msg.ID); ok {
				base.Peer = &peer
				base.State = peer.State
				base.LocalWork = append(
					[]TaskID(nil), peer.LocalWork...,
				)
			}
		}

		return base, true

	case *SetPeerLocalWorkMsg:
		base.Type = SchedulerEventPeerLocalWorkSet
		base.PeerID = msg.ID
		base.LocalWork = append([]TaskID(nil), msg.Work...)
		if fsm != nil {
			if peer, ok := fsm.SnapshotPeer(msg.ID); ok {
				base.Peer = &peer
				base.State = peer.State
				base.RTT = peer.RTT
			}
		}

		return base, true

	case *CompletePeerWorkMsg:
		base.Type = SchedulerEventPeerCompleted
		base.PeerID = msg.ID
		base.State = PeerReady
		if fsm != nil {
			if peer, ok := fsm.SnapshotPeer(msg.ID); ok {
				base.Peer = &peer
				base.RTT = peer.RTT
				base.LocalWork = append(
					[]TaskID(nil), peer.LocalWork...,
				)
			}
		}

		return base, true

	case *CancelBatchMsg:
		base.Type = SchedulerEventBatchCanceled
		base.BatchID = msg.BatchID

		return base, true

	case *EnqueueMsg:
		base.Type = SchedulerEventTaskEnqueued
		base.TaskID = msg.Task.ID
		base.BatchID = msg.Task.BatchID

		return base, true

	case *DispatchMsg:
		return schedulerEventForDispatchResp(
			base, SchedulerEventTaskDispatched, resp,
		)

	case *DispatchTaskMsg:
		base.TaskID = msg.Task.ID
		base.BatchID = msg.Task.BatchID

		return schedulerEventForDispatchResp(
			base, SchedulerEventTaskDispatched, resp,
		)

	case *AssignLocalTaskMsg:
		base.TaskID = msg.Task.ID
		base.BatchID = msg.Task.BatchID

		return schedulerEventForDispatchResp(
			base, SchedulerEventTaskOwned, resp,
		)

	case *StealMsg:
		base.Type = SchedulerEventWorkStolen
		base.ThiefID = msg.ThiefID
		base.PeerID = msg.ThiefID

		steal, ok := resp.(*StealResp)
		if !ok {
			return base, true
		}

		base.OK = steal.OK
		base.Steal = &steal.Result
		if steal.OK {
			base.DonorID = steal.Result.DonorID
			base.TaskID = steal.Result.TaskID
			base.TaskIDs = append(
				[]TaskID(nil), steal.Result.TaskIDs...,
			)
			base.StolenTaskCount = len(steal.Result.TaskIDs)
			base.DonorLocalLen = steal.Result.DonorRemaining
		}

		return base, true

	default:
		return SchedulerEvent{}, false
	}
}

func schedulerEventForDispatchResp(base SchedulerEvent,
	eventType SchedulerEventType, resp ManagerResp) (SchedulerEvent, bool) {

	base.Type = eventType

	dispatch, ok := resp.(*DispatchResp)
	if !ok {
		return base, true
	}

	base.OK = dispatch.OK
	base.Assignment = &dispatch.Assignment
	if dispatch.OK {
		base.PeerID = dispatch.Assignment.PeerID
		base.TaskID = dispatch.Assignment.TaskID
	}

	return base, true
}
