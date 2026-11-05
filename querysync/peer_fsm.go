package querysync

import (
	"context"
	"fmt"
	"time"

	"github.com/lightninglabs/neutrino/protofsm"
)

// peerStateInfo is implemented by the states of the peer machine, so a caller
// that only holds the machine can read the scheduling state it is in.
type peerStateInfo interface {
	// peerState returns the scheduling state and the latest RTT sample.
	peerState() (PeerState, time.Duration)
}

// PeerFSM is the pure per-peer state machine. It runs the same protofsm
// machine that PeerActor does, one state per scheduling state, and models only
// the state that the manager needs for scheduling decisions; network I/O
// belongs in the actor or adapter layer that feeds events into this FSM.
type PeerFSM struct {
	id      string
	machine *protofsm.StateMachine[PeerMsg, peerOutbox, *peerEnv]
}

// NewPeerFSM creates a new peer FSM.
func NewPeerFSM(id string) *PeerFSM {
	machine, err := protofsm.NewStateMachine[PeerMsg, peerOutbox](
		protofsm.StateMachineCfg[PeerMsg, peerOutbox, *peerEnv]{
			InitialState: peerReadyState{},
			Env:          &peerEnv{id: id},
		},
	)
	if err != nil {
		// The machine only refuses a nil initial state.
		panic(fmt.Sprintf("unable to create peer FSM: %v", err))
	}

	// The machine has no lifecycle beyond the FSM's: it starts with it and
	// runs on its caller's goroutine.
	machine.Start(context.Background())

	return &PeerFSM{id: id, machine: machine}
}

// Apply processes one peer event. The outbox the machine emits for the actor
// is dropped, since the FSM has no manager to send it to.
func (f *PeerFSM) Apply(event PeerEvent) error {
	_, err := f.machine.ProcessEvent(context.Background(), event)

	return err
}

// Snapshot returns the manager-facing peer state.
func (f *PeerFSM) Snapshot(rank int, localWork []TaskID) PeerSnapshot {
	state, rtt := f.machine.CurrentState().(peerStateInfo).peerState()

	return PeerSnapshot{
		ID:        f.id,
		Rank:      rank,
		RTT:       rtt,
		State:     state,
		LocalWork: append([]TaskID(nil), localWork...),
	}
}
