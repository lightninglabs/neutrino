package querysync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPeerFSMFollowsPeerStates checks that the FSM walks the same states as
// the peer actor, and that a sample taken in one state survives into the next.
func TestPeerFSMFollowsPeerStates(t *testing.T) {
	t.Parallel()

	peer := NewPeerFSM("peer")
	require.Equal(t, PeerReady, peer.Snapshot(0, nil).State)

	require.NoError(t, peer.Apply(PeerRTTEvent{RTT: time.Second}))
	require.NoError(t, peer.Apply(PeerAssignedEvent{TaskID: 7}))

	snapshot := peer.Snapshot(3, []TaskID{7})
	require.Equal(t, PeerBusy, snapshot.State)
	require.Equal(t, time.Second, snapshot.RTT)
	require.Equal(t, 3, snapshot.Rank)
	require.Equal(t, []TaskID{7}, snapshot.LocalWork)

	require.NoError(t, peer.Apply(PeerBlockedEvent{}))
	require.Equal(t, PeerBlocked, peer.Snapshot(0, nil).State)
	require.NoError(t, peer.Apply(PeerQuarantinedEvent{}))
	require.Equal(t, PeerQuarantined, peer.Snapshot(0, nil).State)
	require.NoError(t, peer.Apply(PeerDownEvent{}))
	require.Equal(t, PeerDown, peer.Snapshot(0, nil).State)

	// Work can only go to a ready peer, and a refused event leaves the
	// peer where it was.
	require.Error(t, peer.Apply(PeerAssignedEvent{TaskID: 8}))
	require.Equal(t, PeerDown, peer.Snapshot(0, nil).State)

	require.NoError(t, peer.Apply(PeerReadyEvent{}))
	require.Equal(t, PeerReady, peer.Snapshot(0, nil).State)
	require.Equal(t, time.Second, peer.Snapshot(0, nil).RTT)

	require.Error(t, peer.Apply(PeerRTTEvent{RTT: -time.Second}))
}
