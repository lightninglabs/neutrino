package headersync

import (
	"errors"
	"fmt"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
)

const (
	// NoPeer is used when a range does not currently have an owner.
	NoPeer = ""

	// NoRange is used when a peer does not have an active range.
	NoRange RangeID = 0

	// DefaultAnchorConfirmationsRequired is the number of matching
	// observations required before a discovered anchor can be used for
	// parallel range planning. Trusted chain checkpoints bypass this.
	DefaultAnchorConfirmationsRequired = 2

	// DefaultMaxPeerOutstandingRanges bounds the amount of work a peer can
	// own. This includes both its active range and unstarted local queue.
	DefaultMaxPeerOutstandingRanges = 4

	// DefaultMaxRangeHeaders matches the wire getheaders response cap. The FSM
	// refuses to plan larger ranges unless a caller explicitly raises the cap.
	DefaultMaxRangeHeaders = 2000
)

var (
	// ErrActorStopped is returned when a caller sends a message to an actor
	// that is shutting down or has not started yet.
	ErrActorStopped = errors.New("header sync actor stopped")

	// ErrInvalidPeer is returned when a malformed peer snapshot is added to
	// the FSM.
	ErrInvalidPeer = errors.New("invalid header sync peer")

	// ErrUnknownPeer is returned when an event references an unknown peer.
	ErrUnknownPeer = errors.New("unknown header sync peer")

	// ErrInvalidRange is returned when a range cannot be planned safely.
	ErrInvalidRange = errors.New("invalid header range")

	// ErrUnknownRange is returned when an event references an unknown range.
	ErrUnknownRange = errors.New("unknown header range")

	// ErrDuplicateRange is returned when a range ID is reused.
	ErrDuplicateRange = errors.New("duplicate header range")
)

// RangeID identifies a checkpoint/anchor-backed header range. Range zero is
// reserved as the no-range sentinel.
type RangeID uint64

// Hash is the header hash type used by production. Keeping real hashes in the
// pure FSM avoids a later adapter layer that could accidentally drop boundary
// validation detail.
type Hash = chainhash.Hash

// PeerState is the scheduling state that matters for header sync.
type PeerState uint8

const (
	PeerReady PeerState = iota
	PeerBusy
	PeerBlocked
	PeerQuarantined
	PeerDown
)

// String returns a stable lowercase form used by JSON bridge fixtures.
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
		return PeerDown, fmt.Errorf("unknown header peer state %q", state)
	}
}

// RangeState tracks the lifecycle of a header range. Completion and commit are
// deliberately separate because parallel peers can return later ranges before
// the next contiguous range is available.
type RangeState uint8

const (
	RangeQueued RangeState = iota
	RangeOwned
	RangeActive
	RangeStaged
	RangeCommitted
	RangeFailed
	RangeStale
)

// String returns a stable lowercase form used by JSON bridge fixtures.
func (r RangeState) String() string {
	switch r {
	case RangeQueued:
		return "queued"
	case RangeOwned:
		return "owned"
	case RangeActive:
		return "active"
	case RangeStaged:
		return "staged"
	case RangeCommitted:
		return "committed"
	case RangeFailed:
		return "failed"
	case RangeStale:
		return "stale"
	default:
		return fmt.Sprintf("unknown(%d)", r)
	}
}

// ParseRangeState parses the stable fixture form of a range state.
func ParseRangeState(state string) (RangeState, error) {
	switch state {
	case "queued":
		return RangeQueued, nil
	case "owned":
		return RangeOwned, nil
	case "active":
		return RangeActive, nil
	case "staged":
		return RangeStaged, nil
	case "committed":
		return RangeCommitted, nil
	case "failed":
		return RangeFailed, nil
	case "stale":
		return RangeStale, nil
	default:
		return RangeFailed, fmt.Errorf("unknown header range state %q",
			state)
	}
}

// Anchor is a height/hash boundary that can be used to plan getheaders ranges
// once it is trusted or sufficiently confirmed.
type Anchor struct {
	Height        uint32
	Hash          Hash
	Trusted       bool
	Confirmations int
	Rejected      bool
}

// HeaderRange is a unit of parallel header sync work. The interval is
// (StartHeight, StopHeight], meaning StartHeight is the already-known locator
// anchor and StopHeight is the expected boundary hash for validation.
type HeaderRange struct {
	ID          RangeID
	StartHeight uint32
	StartHash   Hash
	StopHeight  uint32
	StopHash    Hash
	LeaseEpoch  uint64
	OwnerPeer   string
	State       RangeState
	Valid       bool
	StagedAt    time.Time
}

// PeerSnapshot is the pure FSM view of one header peer actor.
type PeerSnapshot struct {
	ID          string
	Rank        int
	RTT         time.Duration
	State       PeerState
	LocalRanges []RangeID
	ActiveRange RangeID
}

func (p PeerSnapshot) clone() PeerSnapshot {
	p.LocalRanges = append([]RangeID(nil), p.LocalRanges...)
	return p
}

// RangeAssignment is returned when ownership or active work is assigned.
type RangeAssignment struct {
	PeerID      string
	RangeID     RangeID
	LeaseEpoch  uint64
	StartHeight uint32
	StopHeight  uint32
	StartHash   Hash
	StopHash    Hash
}

// StealReason records why a range lease moved. It is intentionally stable and
// log-friendly because production diagnostics need to distinguish unstarted
// queue rebalancing from timed-out active work.
type StealReason string

const (
	StealReasonExplicit          StealReason = "explicit"
	StealReasonDispatch          StealReason = "dispatch"
	StealReasonQueuedBlocked     StealReason = "queued_donor_blocked"
	StealReasonQueuedQuarantined StealReason = "queued_donor_quarantined"
	StealReasonQueuedOverloaded  StealReason = "queued_donor_overloaded"
	StealReasonActiveBlocked     StealReason = "active_donor_blocked"
	StealReasonActiveQuarantined StealReason = "active_donor_quarantined"
)

// StealResult describes a successful range lease transfer.
type StealResult struct {
	ThiefID       string
	DonorID       string
	RangeID       RangeID
	OldLeaseEpoch uint64
	LeaseEpoch    uint64
	Reason        StealReason
}

// CompleteResult describes how a completion was handled.
type CompleteResult struct {
	RangeID    RangeID
	Accepted   bool
	Stale      bool
	Valid      bool
	RangeState RangeState
}

// CommitResult describes the contiguous ranges committed by one drain pass.
type CommitResult struct {
	Committed []RangeID
	TipHeight uint32
	TipHash   Hash
}

// Metrics exposes counters useful for deterministic tests and production
// telemetry.
type Metrics struct {
	AnchorsAdded       uint64
	AnchorsConfirmed   uint64
	AnchorsRejected    uint64
	RangesPlanned      uint64
	RangesAssigned     uint64
	RangesStarted      uint64
	RangesStolen       uint64
	RangesStaged       uint64
	RangesFailed       uint64
	RangesCommitted    uint64
	StaleCompletions   uint64
	FailedAssignments  uint64
	FailedStealActions uint64
}

// CommitStats summarizes out-of-order completion pressure.
type CommitStats struct {
	StagedCount     int
	CommitLag       uint32
	OldestStagedAge time.Duration
}

// Config holds deterministic scheduler parameters.
type Config struct {
	AnchorConfirmationsRequired int
	AnchorRequestFanout         int
	MaxPeerOutstandingRanges    int
	MaxRangeHeaders             uint32
}

// DefaultConfig returns the default header sync FSM bounds.
func DefaultConfig() Config {
	return Config{
		AnchorConfirmationsRequired: DefaultAnchorConfirmationsRequired,
		AnchorRequestFanout:         DefaultAnchorConfirmationsRequired,
		MaxPeerOutstandingRanges:    DefaultMaxPeerOutstandingRanges,
		MaxRangeHeaders:             DefaultMaxRangeHeaders,
	}
}

// ProductionConfig returns the header sync policy used by blockmanager. The
// ordered commit path still validates proof-of-work, checkpoint boundaries, and
// chain connectivity, so production can stage the first valid segment while
// hedging requests across multiple peers to avoid one laggard setting the RTT
// for every 2k-header turn.
func ProductionConfig() Config {
	cfg := DefaultConfig()
	cfg.AnchorConfirmationsRequired = 1
	cfg.AnchorRequestFanout = 4

	return cfg
}

func (c Config) normalize() Config {
	defaults := DefaultConfig()
	if c.AnchorConfirmationsRequired <= 0 {
		c.AnchorConfirmationsRequired =
			defaults.AnchorConfirmationsRequired
	}
	if c.AnchorRequestFanout <= 0 {
		c.AnchorRequestFanout = c.AnchorConfirmationsRequired
	}
	if c.AnchorRequestFanout < c.AnchorConfirmationsRequired {
		c.AnchorRequestFanout = c.AnchorConfirmationsRequired
	}
	if c.MaxPeerOutstandingRanges <= 0 {
		c.MaxPeerOutstandingRanges = defaults.MaxPeerOutstandingRanges
	}
	if c.MaxRangeHeaders == 0 {
		c.MaxRangeHeaders = defaults.MaxRangeHeaders
	}

	return c
}
