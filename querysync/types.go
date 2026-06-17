package querysync

import (
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultMinQueryTimeout is the minimum timeout for a single peer query
	// attempt. It matches the existing query manager floor.
	DefaultMinQueryTimeout = 2 * time.Second

	// DefaultMaxQueryTimeout is the maximum timeout for a single peer query
	// attempt.
	DefaultMaxQueryTimeout = 32 * time.Second

	// DefaultRTTMultiplier is applied to the latest peer RTT sample to
	// produce the per-peer timeout floor.
	DefaultRTTMultiplier = 8

	// DefaultMaxPeerOutstanding is the maximum amount of work a peer may
	// own at once, counting both its active query slot and its unstarted
	// local queue. This gives each peer a real bounded queue while keeping
	// enough global visibility for work stealing to correct slow owners.
	DefaultMaxPeerOutstanding = 4
)

var (
	// ErrActorStopped is returned when a caller sends a message to an actor
	// that is shutting down or already stopped.
	ErrActorStopped = errors.New("actor stopped")

	// ErrUnknownPeer is returned when an event references a peer that is not
	// present in the FSM.
	ErrUnknownPeer = errors.New("unknown peer")

	// ErrInvalidPeer is returned when a malformed peer snapshot is added to
	// the FSM.
	ErrInvalidPeer = errors.New("invalid peer")
)

// PeerState is the scheduling state that matters to the sync manager. A peer
// can be connected while still being unable to accept work, so blocked is a
// first-class state rather than an error path.
type PeerState uint8

const (
	PeerReady PeerState = iota
	PeerBusy
	PeerBlocked
	PeerQuarantined
	PeerDown
)

// String returns a stable lowercase form used by model bridge fixtures.
func (p PeerState) String() string {
	switch p {
	case PeerReady:
		return "ready"
	case PeerBusy:
		return "busy"
	case PeerBlocked:
		return "blocked"
	case PeerQuarantined:
		return "quarantined"
	case PeerDown:
		return "down"
	default:
		return fmt.Sprintf("unknown(%d)", p)
	}
}

// ParsePeerState parses the stable fixture form of a peer state.
func ParsePeerState(state string) (PeerState, error) {
	switch state {
	case "ready":
		return PeerReady, nil
	case "busy":
		return PeerBusy, nil
	case "blocked":
		return PeerBlocked, nil
	case "quarantined":
		return PeerQuarantined, nil
	case "down":
		return PeerDown, nil
	default:
		return PeerDown, fmt.Errorf("unknown peer state %q", state)
	}
}

// TaskID identifies a unit of query or sync work.
type TaskID uint64

// Task is the scheduler's global work item. The concrete Bitcoin request stays
// outside this model; here we care about identity, batch ownership, and
// cancellation.
type Task struct {
	ID       TaskID
	BatchID  uint64
	Canceled bool

	// Timeout optionally overrides the scheduler's base timeout for this
	// task. This lets the production query manager retain per-query
	// backoff while dispatching through a long-lived scheduler actor.
	Timeout time.Duration

	// AvoidPeers is the set of peer IDs that should not receive this task
	// unless the caller retries with a different task view. Production uses
	// this for immediate retry avoidance and cooldown fallback.
	AvoidPeers []string
}

// PeerSnapshot is the pure FSM view of a peer actor.
type PeerSnapshot struct {
	ID        string
	Rank      int
	RTT       time.Duration
	State     PeerState
	LocalWork []TaskID
}

func (p PeerSnapshot) clone() PeerSnapshot {
	p.LocalWork = append([]TaskID(nil), p.LocalWork...)
	return p
}

// Assignment is the command produced when the manager FSM dispatches work.
type Assignment struct {
	PeerID  string
	TaskID  TaskID
	Timeout time.Duration
}

// StealResult describes the bounded transfer performed by work stealing.
type StealResult struct {
	ThiefID        string
	DonorID        string
	TaskID         TaskID
	TaskIDs        []TaskID
	DonorRemaining int
}

// Config holds deterministic scheduler parameters. Keeping these values in a
// struct makes it straightforward for the P bridge and property tests to run
// the same transition rules with explicit bounds.
type Config struct {
	BaseTimeout   time.Duration
	MinTimeout    time.Duration
	MaxTimeout    time.Duration
	RTTMultiplier int

	// MaxPeerOutstanding caps the number of tasks a peer can own at once,
	// counting an active task as one slot. A busy peer with a cap of four
	// may therefore own at most three unstarted local tasks.
	MaxPeerOutstanding int
}

// DefaultConfig returns the production-intent scheduler bounds.
func DefaultConfig() Config {
	return Config{
		BaseTimeout:        DefaultMinQueryTimeout,
		MinTimeout:         DefaultMinQueryTimeout,
		MaxTimeout:         DefaultMaxQueryTimeout,
		RTTMultiplier:      DefaultRTTMultiplier,
		MaxPeerOutstanding: DefaultMaxPeerOutstanding,
	}
}

func (c Config) normalize() Config {
	defaults := DefaultConfig()
	if c.BaseTimeout <= 0 {
		c.BaseTimeout = defaults.BaseTimeout
	}
	if c.MinTimeout <= 0 {
		c.MinTimeout = defaults.MinTimeout
	}
	if c.MaxTimeout <= 0 {
		c.MaxTimeout = defaults.MaxTimeout
	}
	if c.RTTMultiplier <= 0 {
		c.RTTMultiplier = defaults.RTTMultiplier
	}
	if c.MaxPeerOutstanding <= 0 {
		c.MaxPeerOutstanding = defaults.MaxPeerOutstanding
	}

	return c
}

// QueryTimeoutForPeer is the Go implementation of the P model timeout rule:
//
//	timeout = clamp(max(base_timeout, latest_rtt * multiplier), min, max)
//
// A zero RTT means no sample is available, so the base timeout remains in
// force after the minimum floor is applied.
func QueryTimeoutForPeer(baseTimeout time.Duration, peer PeerSnapshot,
	cfg Config) time.Duration {

	cfg = cfg.normalize()

	timeout := baseTimeout
	if timeout < cfg.MinTimeout {
		timeout = cfg.MinTimeout
	}

	if peer.RTT > 0 {
		rttTimeout := peer.RTT * time.Duration(cfg.RTTMultiplier)
		if rttTimeout > timeout {
			timeout = rttTimeout
		}
	}

	if timeout > cfg.MaxTimeout {
		return cfg.MaxTimeout
	}

	return timeout
}
