package querysync

// Scheduler is a synchronous wrapper around a SchedulerFSM for callers that
// already serialize access to the scheduler from a single goroutine. It applies
// the same transitions as ManagerActor and records the same trace and event
// streams, but without a mailbox round trip per call, so a trace recorded from
// a Scheduler replays identically through ReplayTrace.
//
// A Scheduler is NOT safe for concurrent use. The owning goroutine must make
// every call.
type Scheduler struct {
	fsm    *SchedulerFSM
	trace  *TraceRecorder
	events SchedulerEventSink
}

// NewScheduler creates a synchronous scheduler. The trace recorder and the
// event sink are both optional and may be nil.
func NewScheduler(cfg Config, trace *TraceRecorder,
	events SchedulerEventSink) *Scheduler {

	return &Scheduler{
		fsm:    NewSchedulerFSM(cfg),
		trace:  trace,
		events: events,
	}
}

// AddPeer adds or replaces a peer snapshot. A nil LocalWork keeps the local
// work already recorded for the peer, while an empty, non-nil slice clears it.
func (s *Scheduler) AddPeer(peer PeerSnapshot) error {
	msg := &AddPeerMsg{Peer: peer}
	if err := s.fsm.AddPeer(peer); err != nil {
		return err
	}

	s.record(msg, &ManagerAck{})

	return nil
}

// RemovePeer removes a peer from the scheduler.
func (s *Scheduler) RemovePeer(id string) {
	s.fsm.RemovePeer(id)
	s.record(&RemovePeerMsg{ID: id}, &ManagerAck{})
}

// AssignLocalTask picks the peer that should own the task's unstarted local
// queue slot. See SchedulerFSM.AssignLocalTask.
func (s *Scheduler) AssignLocalTask(task Task) (Assignment, bool) {
	assignment, ok := s.fsm.AssignLocalTask(task)
	s.record(&AssignLocalTaskMsg{Task: task}, &DispatchResp{
		Assignment: assignment,
		OK:         ok,
	})

	return assignment, ok
}

// StealOne moves unclaimed work to the idle thief peer. See
// SchedulerFSM.StealOne.
func (s *Scheduler) StealOne(thiefID string) (StealResult, bool) {
	result, ok := s.fsm.StealOne(thiefID)
	s.record(&StealMsg{ThiefID: thiefID}, &StealResp{
		Result: result,
		OK:     ok,
	})

	return result, ok
}

// record emits the trace and event records for one applied message.
func (s *Scheduler) record(msg ManagerMsg, resp ManagerResp) {
	recordManagerMessage(s.fsm, s.trace, s.events, msg, resp)
}

// recordManagerMessage emits the trace and event records for a scheduler
// message that has already been applied to the FSM. It is shared by the actor
// and synchronous scheduler front ends so both produce identical streams.
func recordManagerMessage(fsm *SchedulerFSM, trace *TraceRecorder,
	events SchedulerEventSink, msg ManagerMsg, resp ManagerResp) {

	if event, ok := traceEventForManagerMessage(fsm, msg, resp); ok {
		trace.Record(event)
	}

	if events == nil {
		return
	}
	if event, ok := schedulerEventForManagerMessage(
		fsm, msg, resp,
	); ok {
		events.RecordSchedulerEvent(event)
	}
}
