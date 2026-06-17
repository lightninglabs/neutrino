package headersync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSessionRangeLifecycle(t *testing.T) {
	t.Parallel()

	session := NewSession()
	assignment := RangeAssignment{
		PeerID:     "peer-a",
		RangeID:    7,
		LeaseEpoch: 2,
	}

	session.TrackRange("peer-a", RangeRequest{
		Assignment: assignment,
		RequestID:  11,
	})
	require.True(t, session.RangeActive("peer-a"))
	require.Equal(t, 1, session.ActiveRangeCount())

	_, ok := session.TimeoutRange("peer-a", 10, 7, 2)
	require.False(t, ok)
	require.True(t, session.RangeActive("peer-a"))

	request, ok := session.TimeoutRange("peer-a", 11, 7, 2)
	require.True(t, ok)
	require.Equal(t, assignment, request.Assignment)
	require.False(t, session.RangeActive("peer-a"))
}

func TestSessionAnchorLifecycle(t *testing.T) {
	t.Parallel()

	session := NewSession()
	request := AnchorRequest{
		StartHeight: 1,
		StartHash:   testHash(1),
		StopHeight:  10,
		StopHash:    testHash(10),
		RequestID:   13,
	}

	session.TrackAnchor("peer-a", request)
	require.True(t, session.AnchorActive("peer-a"))
	require.Equal(t, 1, session.ActiveAnchorCount())
	require.Equal(t, 1, session.MatchingActiveAnchors(
		request.StartHeight, request.StartHash, request.StopHeight,
	))

	_, ok := session.TimeoutAnchor("peer-a", 12, request.StartHeight)
	require.False(t, ok)
	require.True(t, session.AnchorActive("peer-a"))

	finished, ok := session.FinishAnchor("peer-a")
	require.True(t, ok)
	require.Equal(t, request.StartHeight, finished.StartHeight)
	require.False(t, session.AnchorActive("peer-a"))
}

func TestSessionPruneAnchorsAtOrBefore(t *testing.T) {
	t.Parallel()

	session := NewSession()
	session.TrackAnchor("peer-a", AnchorRequest{
		StartHeight: 1,
		StopHeight:  10,
		RequestID:   13,
	})
	session.TrackAnchor("peer-b", AnchorRequest{
		StartHeight: 10,
		StopHeight:  20,
		RequestID:   14,
	})
	session.TrackAnchor("peer-c", AnchorRequest{
		StartHeight: 20,
		StopHeight:  30,
		RequestID:   15,
	})

	pruned := session.PruneAnchorsAtOrBefore(20)
	require.Equal(t, 2, pruned)
	require.False(t, session.AnchorActive("peer-a"))
	require.False(t, session.AnchorActive("peer-b"))
	require.True(t, session.AnchorActive("peer-c"))
	require.Equal(t, 1, session.ActiveAnchorCount())
}

func TestSessionStaleAndStaged(t *testing.T) {
	t.Parallel()

	session := NewSession()
	assignment := RangeAssignment{PeerID: "peer-a", RangeID: 9}
	expiresAt := time.Now().Add(time.Second)

	session.MarkStale("peer-a", StaleRequest{
		Assignment: assignment,
		ExpiresAt:  expiresAt,
	})

	stale, ok := session.PopStale("peer-a")
	require.True(t, ok)
	require.Equal(t, assignment, stale.Assignment)
	require.Equal(t, expiresAt, stale.ExpiresAt)

	session.StageRange(assignment.RangeID, nil, "peer-a")
	staged, ok := session.StagedRange(assignment.RangeID)
	require.True(t, ok)
	require.Equal(t, "peer-a", staged.PeerID)

	session.DeleteStagedRange(assignment.RangeID)
	_, ok = session.StagedRange(assignment.RangeID)
	require.False(t, ok)
}

func TestSessionRangeIDAllocation(t *testing.T) {
	t.Parallel()

	session := NewSession()
	require.Equal(t, RangeID(1), session.NextRangeID())
	require.Equal(t, RangeID(1), session.AllocateRangeID())
	require.Equal(t, RangeID(2), session.NextRangeID())

	session.SetNextRangeID(99)
	require.Equal(t, RangeID(99), session.AllocateRangeID())
	require.Equal(t, RangeID(100), session.NextRangeID())
}

func TestSessionCooldownLifecycle(t *testing.T) {
	t.Parallel()

	session := NewSession()
	now := time.Unix(100, 0)
	until, ok := session.TrackPeerCooldown(
		"peer-a", PeerQuarantined, now, time.Minute,
	)
	require.True(t, ok)
	require.Equal(t, now.Add(time.Minute), until)
	require.Equal(t, 1, session.CooldownCount())

	activeUntil, ok := session.CooldownUntil(
		"peer-a", now.Add(30*time.Second),
	)
	require.True(t, ok)
	require.Equal(t, until, activeUntil)

	expiredUntil, ok := session.CooldownExpired(
		"peer-a", now.Add(time.Minute),
	)
	require.True(t, ok)
	require.Equal(t, until, expiredUntil)

	session.TrackPeerCooldown("peer-a", PeerReady, now, time.Minute)
	require.Equal(t, 0, session.CooldownCount())
	_, ok = session.CooldownUntil("peer-a", now.Add(time.Second))
	require.False(t, ok)
}
