package bridge

import (
	"fmt"
)

// ReplayResult holds the outcome of replaying a P trace against a Go FSM
// simulator.
type ReplayResult struct {
	// TotalEvents is the number of events processed from the trace.
	TotalEvents int

	// StateTransitions records each state transition observed.
	StateTransitions []StateTransition

	// FilteredBlocks records each filtered block outbox event.
	FilteredBlocks []FilteredBlockPayload

	// Errors collects any conformance violations found during replay.
	Errors []string
}

// StateTransition records a single FSM state transition.
type StateTransition struct {
	From FSMState
	To   FSMState
}

// RescanFSMSimulator is a minimal Go FSM simulator that tracks state
// transitions for conformance checking against P model traces. It does NOT
// replicate the full Go FSM logic (that's the job of the Go unit tests).
// Instead, it validates that the P model's announced state transitions and
// outbox events match what the Go FSM would produce.
type RescanFSMSimulator struct {
	CurrentState FSMState
	WatchList    map[int]bool
	CurHeight    int
}

// NewRescanFSMSimulator creates a new simulator in the Initializing state.
func NewRescanFSMSimulator() *RescanFSMSimulator {
	return &RescanFSMSimulator{
		CurrentState: StateInitializing,
		WatchList:    make(map[int]bool),
		CurHeight:    0,
	}
}

// ValidTransitions defines the legal FSM state transition table.
var ValidTransitions = map[FSMState][]FSMState{
	StateInitializing: {StateSyncing, StateTerminal},
	StateSyncing:      {StateCurrent, StateRewinding, StateTerminal},
	StateCurrent:      {StateRewinding, StateTerminal},
	StateRewinding:    {StateSyncing, StateTerminal},
	StateTerminal:     {},
}

// IsValidTransition checks whether a transition from→to is valid per the
// state transition table.
func IsValidTransition(from, to FSMState) bool {
	// Self-transition at init is valid.
	if from == StateInitializing && to == StateInitializing {
		return true
	}

	targets, ok := ValidTransitions[from]
	if !ok {
		return false
	}

	for _, t := range targets {
		if t == to {
			return true
		}
	}

	return false
}

// ReplayTrace replays a P model checker trace against the Go FSM simulator,
// checking for conformance violations.
func ReplayTrace(trace *Trace) (*ReplayResult, error) {
	sim := NewRescanFSMSimulator()
	result := &ReplayResult{}

	for _, entry := range trace.Entries {
		if entry.Event == "" {
			continue
		}

		result.TotalEvents++

		switch EventType(entry.Event) {
		case EvFSMStateChange:
			payload, err := ParseEventPayload[StateChangePayload](
				entry,
			)
			if err != nil {
				// Trace may use tuple format, skip silently.
				continue
			}

			transition := StateTransition{
				From: payload.From,
				To:   payload.To,
			}
			result.StateTransitions = append(
				result.StateTransitions, transition,
			)

			// Validate the transition.
			if !IsValidTransition(payload.From, payload.To) {
				result.Errors = append(result.Errors,
					fmt.Sprintf(
						"invalid transition: %s -> %s",
						payload.From, payload.To,
					),
				)
			}

			// Verify the simulator's current state matches.
			if payload.From != sim.CurrentState {
				result.Errors = append(result.Errors,
					fmt.Sprintf(
						"state mismatch: expected %s, "+
							"trace says %s",
						sim.CurrentState, payload.From,
					),
				)
			}

			sim.CurrentState = payload.To

		case EvFilteredBlockOutbox:
			payload, err := ParseEventPayload[FilteredBlockPayload](
				entry,
			)
			if err != nil {
				continue
			}
			result.FilteredBlocks = append(
				result.FilteredBlocks, payload,
			)
		}
	}

	return result, nil
}
