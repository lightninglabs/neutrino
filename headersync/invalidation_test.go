package headersync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInvalidateRangePreservesIndependentStaging(t *testing.T) {
	fsm := NewHeaderSyncFSM(0, testHash(100), ProductionConfig())
	fsm.SeedCommittedAnchor()
	fsm.AddOrConfirmAnchor(1, testHash(101), false)
	fsm.AddOrConfirmAnchor(2, testHash(102), false)
	addTrustedAnchor(t, fsm, 10, 110)
	addTrustedAnchor(t, fsm, 20, 120)
	addPeer(t, fsm, "peer")
	_, ok, err := fsm.StageDiscoveredRange(1, "peer", 0, 1, time.Now())
	require.NoError(t, err)
	require.True(t, ok)
	mustPlanRange(t, fsm, 2, 1, 2)
	mustAssignRange(t, fsm)
	active := mustStartRange(t, fsm, "peer")
	_, ok, err = fsm.StageDiscoveredRange(3, "other", 10, 20, time.Now())
	require.NoError(t, err)
	require.True(t, ok)

	result, err := fsm.InvalidateRange(1)
	require.NoError(t, err)
	require.Equal(t, []RangeID{1, 2}, result.Ranges)
	require.Equal(t, []ChainPoint{
		{Height: 1, Hash: testHash(101)},
		{Height: 2, Hash: testHash(102)},
	}, result.Anchors)
	independent, _ := fsm.RangeByID(3)
	require.Equal(t, RangeStaged, independent.State)
	require.True(t, independent.Valid)
	peer, _ := fsm.SnapshotPeer("peer")
	require.Equal(t, NoRange, peer.ActiveRange)
	require.Empty(t, peer.LocalRanges)
	late, err := fsm.CompleteRange(
		"peer", 2, active.LeaseEpoch, true,
	)
	require.NoError(t, err)
	require.True(t, late.Stale)
	require.Empty(t, fsm.ReadyRanges())
	require.Zero(t, fsm.CommittedTip())

	// Removing the bad observation, without keeping a rejected tombstone,
	// allows a valid peer to replace the same height with another hash.
	_, ok = fsm.AddOrConfirmAnchor(1, testHash(201), false)
	require.True(t, ok)
	_, ok, err = fsm.StageDiscoveredRange(4, "honest", 0, 1, time.Now())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []RangeID{4}, fsm.CommitReadyRanges().Committed)
	_, err = fsm.InvalidateRange(4)
	require.ErrorIs(t, err, ErrInvalidRange)
}

func TestInvalidationReleasesPeerActor(t *testing.T) {
	ctx := t.Context()
	runtime, _ := NewRuntime(ChainPoint{Hash: testHash(100)}, nil,
		RuntimeConfig{Config: ProductionConfig()})
	runtime.Start(ctx)
	t.Cleanup(runtime.Stop)
	manager := runtime.Manager()
	for height := uint32(1); height <= 2; height++ {
		_, _, err := manager.AddAnchor(
			ctx, height, testHash(byte(100+height)), false,
		)
		require.NoError(t, err)
	}
	_, _, err := manager.AddAnchor(ctx, 10, testHash(110), true)
	require.NoError(t, err)
	peer, _, err := runtime.RegisterPeer(ctx, PeerSnapshot{
		ID: "honest", State: PeerReady,
	})
	require.NoError(t, err)
	_, ok, err := manager.StageDiscoveredRange(
		ctx, 1, "bad", 0, 1, time.Now(),
	)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = manager.PlanRange(ctx, 2, 1, 2)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = manager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	old, ok, err := peer.RequestRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	runtime.Session().TrackRange("honest", RangeRequest{Assignment: old})
	_, err = runtime.Controller().InvalidateRange(
		ctx, 1, time.Now().Add(time.Hour),
	)
	require.NoError(t, err)
	_, ok, err = manager.PlanRange(ctx, 3, 0, 10)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = manager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	replacement, ok, err := peer.RequestRange(ctx)
	require.NoError(t, err)
	require.True(t, ok, "revoked lease kept the peer actor busy")
	require.Equal(t, RangeID(3), replacement.RangeID)
}
