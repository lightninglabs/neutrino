package headersync

import (
	"time"
)

// DispatchAssignment is the generic work ownership result returned by a shared
// dispatch scheduler.
type DispatchAssignment struct {
	PeerID  string
	RangeID RangeID
}

// DispatchStealResult is the generic unstarted-work steal result returned by a
// shared dispatch scheduler.
type DispatchStealResult struct {
	ThiefID  string
	DonorID  string
	RangeID  RangeID
	RangeIDs []RangeID
}

// DispatchController owns generic peer/work scheduling policy for headersync.
// Implementations should not know about getheaders, anchors, validation, or
// ordered commit. Those remain headersync responsibilities.
type DispatchController interface {
	AddPeer(PeerSnapshot) error
	RemovePeer(string) error
	UpdatePeerState(string, PeerState) error
	UpdatePeerRTT(string, time.Duration) error

	AssignRange(RangeID) (DispatchAssignment, bool, error)
	StartRange(PeerSnapshot) error
	CompleteRange(string) error
	SyncPeer(PeerSnapshot) error

	StealRange(string) (DispatchStealResult, bool, error)
}
