package querysync

import (
	"fmt"
	"time"
)

// PeerFSM is the pure per-peer state machine. It deliberately models only the
// state that the manager needs for scheduling decisions; network I/O belongs in
// the actor or adapter layer that feeds events into this FSM.
type PeerFSM struct {
	id     string
	state  PeerState
	rtt    time.Duration
	active *TaskID
}

// NewPeerFSM creates a new peer FSM.
func NewPeerFSM(id string) *PeerFSM {
	return &PeerFSM{
		id:    id,
		state: PeerReady,
	}
}

func (PeerReadyEvent) apply(f *PeerFSM) error {
	f.state = PeerReady
	f.active = nil
	return nil
}

func (PeerBlockedEvent) apply(f *PeerFSM) error {
	f.state = PeerBlocked
	return nil
}

func (PeerQuarantinedEvent) apply(f *PeerFSM) error {
	f.state = PeerQuarantined
	f.active = nil
	return nil
}

func (PeerDownEvent) apply(f *PeerFSM) error {
	f.state = PeerDown
	f.active = nil
	return nil
}

func (e PeerRTTEvent) apply(f *PeerFSM) error {
	if e.RTT < 0 {
		return fmt.Errorf("negative RTT: %v", e.RTT)
	}

	f.rtt = e.RTT
	return nil
}

func (PeerRankEvent) apply(*PeerFSM) error {
	return nil
}

func (e PeerAssignedEvent) apply(f *PeerFSM) error {
	if f.state != PeerReady {
		return fmt.Errorf("peer %s cannot accept work while %s",
			f.id, f.state)
	}

	f.state = PeerBusy
	f.active = &e.TaskID
	return nil
}

func (PeerNeedWorkEvent) apply(*PeerFSM) error {
	return nil
}

// Apply processes one peer event.
func (f *PeerFSM) Apply(event PeerEvent) error {
	return event.apply(f)
}

// Snapshot returns the manager-facing peer state.
func (f *PeerFSM) Snapshot(rank int, localWork []TaskID) PeerSnapshot {
	return PeerSnapshot{
		ID:        f.id,
		Rank:      rank,
		RTT:       f.rtt,
		State:     f.state,
		LocalWork: append([]TaskID(nil), localWork...),
	}
}
