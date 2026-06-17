package headersync

import (
	"context"
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
	AddPeer(context.Context, PeerSnapshot) error
	RemovePeer(context.Context, string) error
	UpdatePeerState(context.Context, string, PeerState) error
	UpdatePeerRTT(context.Context, string, time.Duration) error

	AssignRange(context.Context, RangeID) (DispatchAssignment, bool, error)
	StartRange(context.Context, PeerSnapshot) error
	CompleteRange(context.Context, string) error
	SyncPeer(context.Context, PeerSnapshot) error

	StealRange(context.Context, string) (DispatchStealResult, bool, error)
}
