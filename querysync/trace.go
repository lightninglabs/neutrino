package querysync

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"
)

const (
	TraceOpAddPeer         = "add_peer"
	TraceOpRemovePeer      = "remove_peer"
	TraceOpUpdatePeerState = "update_peer_state"
	TraceOpUpdatePeerRTT   = "update_peer_rtt"
	TraceOpSetPeerWork     = "set_peer_work"
	TraceOpCompletePeer    = "complete_peer"
	TraceOpCancelBatch     = "cancel_batch"
	TraceOpEnqueue         = "enqueue"
	TraceOpDispatch        = "dispatch"
	TraceOpDispatchTask    = "dispatch_task"
	TraceOpAssignLocalTask = "assign_local_task"
	TraceOpSteal           = "steal"
)

// TraceEvent is the durable, JSON-friendly form of a scheduler actor event.
// It is intentionally phrased in model terms, not Go implementation terms, so
// production traces can be replayed against both this actor-backed path and the
// P bridge invariants.
type TraceEvent struct {
	Op string `json:"op"`

	Peer  *BridgePeer `json:"peer,omitempty"`
	State string      `json:"state,omitempty"`

	PeerID string   `json:"peer_id,omitempty"`
	RTTMS  int      `json:"rtt_ms,omitempty"`
	Work   []TaskID `json:"work,omitempty"`

	Task    *BridgeTask `json:"task,omitempty"`
	BatchID uint64      `json:"batch_id,omitempty"`
	ThiefID string      `json:"thief_id,omitempty"`

	Dispatch *BridgeDispatchWant `json:"dispatch,omitempty"`
	Steal    *BridgeStealWant    `json:"steal,omitempty"`
	Error    string              `json:"error,omitempty"`
}

// TraceReplayResult is the sequence of externally visible scheduler decisions
// produced by a trace replay.
type TraceReplayResult struct {
	Dispatches []BridgeDispatchWant `json:"dispatches"`
	Steals     []BridgeStealWant    `json:"steals"`
	Metrics    Metrics              `json:"metrics"`
}

// TraceRecorder stores scheduler trace events emitted by the manager actor.
type TraceRecorder struct {
	mu     sync.Mutex
	events []TraceEvent
}

// Record appends one trace event.
func (t *TraceRecorder) Record(event TraceEvent) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.events = append(t.events, cloneTraceEvent(event))
}

// Events returns a stable copy of the recorded trace.
func (t *TraceRecorder) Events() []TraceEvent {
	if t == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	events := make([]TraceEvent, len(t.events))
	for i := range t.events {
		events[i] = cloneTraceEvent(t.events[i])
	}

	return events
}

// ReplayTrace applies a recorded scheduler trace to a fresh manager actor. If
// a dispatch or steal event includes an expected result, replay verifies that
// the actor produces the same decision.
func ReplayTrace(ctx context.Context, cfg Config,
	events []TraceEvent) (TraceReplayResult, error) {

	manager := NewManagerActor(NewSchedulerFSM(cfg), 128)
	manager.Start(ctx)
	defer manager.Stop()

	var result TraceReplayResult
	for index, event := range events {
		if err := replayTraceEvent(ctx, manager, &result, event); err != nil {
			return TraceReplayResult{}, fmt.Errorf(
				"trace event %d (%s): %w", index, event.Op, err,
			)
		}
	}

	metrics, err := manager.Metrics(ctx)
	if err != nil {
		return TraceReplayResult{}, err
	}
	result.Metrics = metrics

	return result, nil
}

func replayTraceEvent(ctx context.Context, manager *ManagerActor,
	result *TraceReplayResult, event TraceEvent) error {

	switch event.Op {
	case TraceOpAddPeer:
		if event.Peer == nil {
			return fmt.Errorf("missing peer")
		}

		peer, err := peerFromBridge(*event.Peer)
		if err != nil {
			return err
		}

		return manager.AddPeer(ctx, peer)

	case TraceOpRemovePeer:
		return manager.RemovePeer(ctx, event.PeerID)

	case TraceOpUpdatePeerState:
		state, err := ParsePeerState(event.State)
		if err != nil {
			return err
		}

		return manager.UpdatePeerState(ctx, event.PeerID, state)

	case TraceOpUpdatePeerRTT:
		return manager.UpdatePeerRTT(
			ctx, event.PeerID,
			time.Duration(event.RTTMS)*time.Millisecond,
		)

	case TraceOpSetPeerWork:
		return manager.SetPeerLocalWork(ctx, event.PeerID, event.Work)

	case TraceOpCompletePeer:
		return manager.CompletePeerWork(ctx, event.PeerID)

	case TraceOpCancelBatch:
		return manager.CancelBatch(ctx, event.BatchID)

	case TraceOpEnqueue:
		if event.Task == nil {
			return fmt.Errorf("missing task")
		}

		return manager.Enqueue(ctx, taskFromBridge(*event.Task))

	case TraceOpDispatch:
		assignment, ok, err := manager.AskDispatch(ctx)
		if err != nil {
			return err
		}

		got := bridgeDispatchWant(assignment, ok)
		result.Dispatches = append(result.Dispatches, got)
		if event.Dispatch != nil && *event.Dispatch != got {
			return fmt.Errorf("dispatch mismatch: got %+v, want %+v",
				got, *event.Dispatch)
		}

		return nil

	case TraceOpDispatchTask:
		if event.Task == nil {
			return fmt.Errorf("missing task")
		}

		assignment, ok, err := manager.AskDispatchTask(
			ctx, taskFromBridge(*event.Task),
		)
		if err != nil {
			return err
		}

		got := bridgeDispatchWant(assignment, ok)
		result.Dispatches = append(result.Dispatches, got)
		if event.Dispatch != nil && *event.Dispatch != got {
			return fmt.Errorf("dispatch_task mismatch: got %+v, "+
				"want %+v", got, *event.Dispatch)
		}

		return nil

	case TraceOpAssignLocalTask:
		if event.Task == nil {
			return fmt.Errorf("missing task")
		}

		assignment, ok, err := manager.AskAssignLocalTask(
			ctx, taskFromBridge(*event.Task),
		)
		if err != nil {
			return err
		}

		got := bridgeDispatchWant(assignment, ok)
		result.Dispatches = append(result.Dispatches, got)
		if event.Dispatch != nil && *event.Dispatch != got {
			return fmt.Errorf("assign_local_task mismatch: got "+
				"%+v, want %+v", got, *event.Dispatch)
		}

		return nil

	case TraceOpSteal:
		got, err := replayTraceSteal(ctx, manager, event.ThiefID)
		if err != nil {
			return err
		}

		result.Steals = append(result.Steals, got)
		if event.Steal != nil && !reflect.DeepEqual(*event.Steal, got) {
			return fmt.Errorf("steal mismatch: got %+v, want %+v",
				got, *event.Steal)
		}

		return nil

	default:
		return fmt.Errorf("unknown trace op")
	}
}

func replayTraceSteal(ctx context.Context, manager *ManagerActor,
	thiefID string) (BridgeStealWant, error) {

	steal, ok, err := manager.AskSteal(ctx, thiefID)
	if err != nil {
		return BridgeStealWant{}, err
	}
	if !ok {
		return BridgeStealWant{OK: false}, nil
	}

	thief, ok, err := manager.SnapshotPeer(ctx, thiefID)
	if err != nil {
		return BridgeStealWant{}, err
	}
	if !ok {
		return BridgeStealWant{}, fmt.Errorf("%w: %s",
			ErrUnknownPeer, thiefID)
	}

	return bridgeStealWant(steal, len(thief.LocalWork), true), nil
}

func traceEventForManagerMessage(fsm *SchedulerFSM, msg ManagerMsg,
	resp ManagerResp) (TraceEvent, bool) {

	switch msg := msg.(type) {
	case *AddPeerMsg:
		peer := bridgePeer(msg.Peer)

		return TraceEvent{
			Op:   TraceOpAddPeer,
			Peer: &peer,
		}, true

	case *RemovePeerMsg:
		return TraceEvent{
			Op:     TraceOpRemovePeer,
			PeerID: msg.ID,
		}, true

	case *UpdatePeerStateMsg:
		return TraceEvent{
			Op:     TraceOpUpdatePeerState,
			PeerID: msg.ID,
			State:  msg.State.String(),
		}, true

	case *UpdatePeerRTTMsg:
		return TraceEvent{
			Op:     TraceOpUpdatePeerRTT,
			PeerID: msg.ID,
			RTTMS:  int(msg.RTT / time.Millisecond),
		}, true

	case *SetPeerLocalWorkMsg:
		return TraceEvent{
			Op:     TraceOpSetPeerWork,
			PeerID: msg.ID,
			Work:   append([]TaskID(nil), msg.Work...),
		}, true

	case *CompletePeerWorkMsg:
		return TraceEvent{
			Op:     TraceOpCompletePeer,
			PeerID: msg.ID,
		}, true

	case *CancelBatchMsg:
		return TraceEvent{
			Op:      TraceOpCancelBatch,
			BatchID: msg.BatchID,
		}, true

	case *EnqueueMsg:
		task := bridgeTask(msg.Task)

		return TraceEvent{
			Op:   TraceOpEnqueue,
			Task: &task,
		}, true

	case *DispatchMsg:
		event := TraceEvent{Op: TraceOpDispatch}
		if dispatch, ok := resp.(*DispatchResp); ok {
			want := bridgeDispatchWant(
				dispatch.Assignment, dispatch.OK,
			)
			event.Dispatch = &want
		}

		return event, true

	case *DispatchTaskMsg:
		task := bridgeTask(msg.Task)
		event := TraceEvent{
			Op:   TraceOpDispatchTask,
			Task: &task,
		}
		if dispatch, ok := resp.(*DispatchResp); ok {
			want := bridgeDispatchWant(
				dispatch.Assignment, dispatch.OK,
			)
			event.Dispatch = &want
		}

		return event, true

	case *AssignLocalTaskMsg:
		task := bridgeTask(msg.Task)
		event := TraceEvent{
			Op:   TraceOpAssignLocalTask,
			Task: &task,
		}
		if dispatch, ok := resp.(*DispatchResp); ok {
			want := bridgeDispatchWant(
				dispatch.Assignment, dispatch.OK,
			)
			event.Dispatch = &want
		}

		return event, true

	case *StealMsg:
		event := TraceEvent{
			Op:      TraceOpSteal,
			ThiefID: msg.ThiefID,
		}
		if steal, ok := resp.(*StealResp); ok {
			thiefWorkLen := 0
			if steal.OK && fsm != nil {
				thief, ok := fsm.SnapshotPeer(msg.ThiefID)
				if ok {
					thiefWorkLen = len(thief.LocalWork)
				}
			}

			want := bridgeStealWant(
				steal.Result, thiefWorkLen, steal.OK,
			)
			event.Steal = &want
		}

		return event, true

	default:
		return TraceEvent{}, false
	}
}

func bridgeDispatchWant(assignment Assignment, ok bool) BridgeDispatchWant {
	if !ok {
		return BridgeDispatchWant{OK: false}
	}

	return BridgeDispatchWant{
		OK:            true,
		PeerID:        assignment.PeerID,
		TaskID:        assignment.TaskID,
		TimeoutMillis: int(assignment.Timeout / time.Millisecond),
	}
}

func bridgeStealWant(result StealResult, thiefWorkLen int,
	ok bool) BridgeStealWant {

	if !ok {
		return BridgeStealWant{OK: false}
	}

	return BridgeStealWant{
		OK:             true,
		ThiefID:        result.ThiefID,
		DonorID:        result.DonorID,
		TaskID:         result.TaskID,
		TaskIDs:        append([]TaskID(nil), result.TaskIDs...),
		DonorRemaining: result.DonorRemaining,
		ThiefWorkLen:   thiefWorkLen,
	}
}

func bridgePeer(peer PeerSnapshot) BridgePeer {
	return BridgePeer{
		ID:        peer.ID,
		Rank:      peer.Rank,
		RTTMillis: int(peer.RTT / time.Millisecond),
		State:     peer.State.String(),
		LocalWork: append([]TaskID(nil), peer.LocalWork...),
	}
}

func peerFromBridge(peer BridgePeer) (PeerSnapshot, error) {
	state, err := ParsePeerState(peer.State)
	if err != nil {
		return PeerSnapshot{}, err
	}

	return PeerSnapshot{
		ID:        peer.ID,
		Rank:      peer.Rank,
		RTT:       time.Duration(peer.RTTMillis) * time.Millisecond,
		State:     state,
		LocalWork: append([]TaskID(nil), peer.LocalWork...),
	}, nil
}

func bridgeTask(task Task) BridgeTask {
	return BridgeTask{
		ID:            task.ID,
		BatchID:       task.BatchID,
		Canceled:      task.Canceled,
		TimeoutMillis: int(task.Timeout / time.Millisecond),
		AvoidPeers:    append([]string(nil), task.AvoidPeers...),
	}
}

func taskFromBridge(task BridgeTask) Task {
	return Task{
		ID:         task.ID,
		BatchID:    task.BatchID,
		Canceled:   task.Canceled,
		Timeout:    time.Duration(task.TimeoutMillis) * time.Millisecond,
		AvoidPeers: append([]string(nil), task.AvoidPeers...),
	}
}

func cloneTraceEvent(event TraceEvent) TraceEvent {
	if event.Peer != nil {
		peer := *event.Peer
		peer.LocalWork = append([]TaskID(nil), peer.LocalWork...)
		event.Peer = &peer
	}
	if event.Task != nil {
		task := *event.Task
		task.AvoidPeers = append([]string(nil), task.AvoidPeers...)
		event.Task = &task
	}
	event.Work = append([]TaskID(nil), event.Work...)

	if event.Dispatch != nil {
		dispatch := *event.Dispatch
		event.Dispatch = &dispatch
	}
	if event.Steal != nil {
		steal := *event.Steal
		steal.TaskIDs = append([]TaskID(nil), steal.TaskIDs...)
		event.Steal = &steal
	}

	return event
}
