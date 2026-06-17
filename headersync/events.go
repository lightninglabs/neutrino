package headersync

import "sync"

// EventType identifies a structured header sync event.
type EventType string

const (
	EventAnchorUpdated  EventType = "anchor_updated"
	EventRangePlanned   EventType = "range_planned"
	EventRangeAssigned  EventType = "range_assigned"
	EventRangeStarted   EventType = "range_started"
	EventRangeStolen    EventType = "range_stolen"
	EventRangeCompleted EventType = "range_completed"
	EventRangeCommitted EventType = "range_committed"
)

// Event is the structured observability record emitted by the manager actor.
type Event struct {
	Type EventType
	OK   bool

	PeerID  string
	ThiefID string
	DonorID string

	AnchorHeight uint32
	RangeID      RangeID
	StartHeight  uint32
	StopHeight   uint32
	LeaseEpoch   uint64
	RangeState   RangeState

	Valid     bool
	Stale     bool
	Committed []RangeID

	Metrics Metrics
}

// EventSink receives structured header sync events.
type EventSink interface {
	RecordHeaderSyncEvent(Event)
}

// EventRecorder is an in-memory EventSink for tests and trace replay.
type EventRecorder struct {
	mu     sync.Mutex
	events []Event
}

// RecordHeaderSyncEvent records an event.
func (r *EventRecorder) RecordHeaderSyncEvent(event Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	event.Committed = append([]RangeID(nil), event.Committed...)
	r.events = append(r.events, event)
}

// Events returns a snapshot of recorded events.
func (r *EventRecorder) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	events := make([]Event, len(r.events))
	copy(events, r.events)
	for i := range events {
		events[i].Committed = append([]RangeID(nil), events[i].Committed...)
	}

	return events
}

func eventForManagerMessage(fsm *HeaderSyncFSM, msg ManagerMsg,
	resp ManagerResp) (Event, bool) {

	base := Event{Metrics: fsm.Metrics()}

	switch m := msg.(type) {
	case *AddAnchorMsg:
		r, ok := resp.(*AddAnchorResp)
		if !ok {
			return Event{}, false
		}

		base.Type = EventAnchorUpdated
		base.OK = r.OK
		base.AnchorHeight = m.Height

		return base, true

	case *PlanRangeMsg:
		r, ok := resp.(*PlanRangeResp)
		if !ok {
			return Event{}, false
		}

		base.Type = EventRangePlanned
		base.OK = r.OK
		base.RangeID = m.ID
		base.StartHeight = m.StartHeight
		base.StopHeight = m.StopHeight
		base.RangeState = r.Range.State

		return base, true

	case *AssignRangeMsg:
		r, ok := resp.(*RangeAssignmentResp)
		if !ok {
			return Event{}, false
		}

		base.Type = EventRangeAssigned
		base.OK = r.OK
		base.PeerID = r.Assignment.PeerID
		base.RangeID = r.Assignment.RangeID
		base.StartHeight = r.Assignment.StartHeight
		base.StopHeight = r.Assignment.StopHeight
		base.LeaseEpoch = r.Assignment.LeaseEpoch

		return base, true

	case *StartRangeMsg:
		r, ok := resp.(*RangeAssignmentResp)
		if !ok {
			return Event{}, false
		}

		base.Type = EventRangeStarted
		base.OK = r.OK
		base.PeerID = m.PeerID
		base.RangeID = r.Assignment.RangeID
		base.StartHeight = r.Assignment.StartHeight
		base.StopHeight = r.Assignment.StopHeight
		base.LeaseEpoch = r.Assignment.LeaseEpoch

		return base, true

	case *StealRangeMsg:
		r, ok := resp.(*StealRangeResp)
		if !ok {
			return Event{}, false
		}

		base.Type = EventRangeStolen
		base.OK = r.OK
		base.ThiefID = m.ThiefID
		base.DonorID = r.Result.DonorID
		base.RangeID = r.Result.RangeID
		base.LeaseEpoch = r.Result.LeaseEpoch

		return base, true

	case *CompleteRangeMsg:
		r, ok := resp.(*CompleteRangeResp)
		if !ok {
			return Event{}, false
		}

		base.Type = EventRangeCompleted
		base.OK = r.Result.Accepted
		base.PeerID = m.PeerID
		base.RangeID = m.RangeID
		base.LeaseEpoch = m.LeaseEpoch
		base.Valid = r.Result.Valid
		base.Stale = r.Result.Stale
		base.RangeState = r.Result.RangeState

		return base, true

	case *CommitReadyMsg:
		r, ok := resp.(*CommitReadyResp)
		if !ok {
			return Event{}, false
		}

		base.Type = EventRangeCommitted
		base.OK = len(r.Result.Committed) > 0
		base.Committed = append([]RangeID(nil), r.Result.Committed...)
		if len(r.Result.Committed) > 0 {
			base.RangeID = r.Result.Committed[len(r.Result.Committed)-1]
		}

		return base, true

	default:
		return Event{}, false
	}
}
