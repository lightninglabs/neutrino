package headersync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRejectAnchorStalesUnstagedDependents checks that rejecting a discovered
// anchor retires the queued, owned and active ranges that use it as a
// boundary, keeps a staged range for commit-time validation, and leaves
// unrelated work alone.
func TestRejectAnchorStalesUnstagedDependents(t *testing.T) {
	cfg := ProductionConfig()
	fsm := NewHeaderSyncFSM(0, testHash(100), cfg)
	fsm.SeedCommittedAnchor()
	fsm.AddOrConfirmAnchor(4, testHash(104), false)
	fsm.AddOrConfirmAnchor(8, testHash(108), false)
	fsm.AddOrConfirmAnchor(12, testHash(112), false)
	addPeer(t, fsm, "peer")

	// Range 1 ends at the anchor and stays staged. Range 2 starts at it
	// and is active. Range 3 starts at another anchor and is untouched.
	_, ok, err := fsm.StageDiscoveredRange(1, "liar", 0, 4, time.Now())
	require.NoError(t, err)
	require.True(t, ok)
	mustPlanRange(t, fsm, 2, 4, 8)
	mustAssignRange(t, fsm)
	active := mustStartRange(t, fsm, "peer")
	require.Equal(t, RangeID(2), active.RangeID)
	mustPlanRange(t, fsm, 3, 8, 12)

	result := fsm.RejectAnchor(4, testHash(104))
	require.Equal(t, []ChainPoint{{Height: 4, Hash: testHash(104)}},
		result.Anchors)
	require.Equal(t, []RangeID{2}, result.Ranges)
	require.Len(t, result.Revoked, 1)
	require.Equal(t, RangeID(2), result.Revoked[0].RangeID)

	anchor, _ := fsm.AnchorByHeight(4)
	require.True(t, anchor.Rejected)
	require.False(t, fsm.AnchorConfirmed(anchor))

	staged, _ := fsm.RangeByID(1)
	require.Equal(t, RangeStaged, staged.State)
	stale, _ := fsm.RangeByID(2)
	require.Equal(t, RangeStale, stale.State)
	other, _ := fsm.RangeByID(3)
	require.Equal(t, RangeQueued, other.State)
	snapshot, _ := fsm.SnapshotPeer("peer")
	require.Equal(t, NoRange, snapshot.ActiveRange)

	// A late completion of the retired lease cannot stage anything.
	late, err := fsm.CompleteRange("peer", 2, active.LeaseEpoch, true)
	require.NoError(t, err)
	require.True(t, late.Stale)

	// Rejecting twice is idempotent.
	again := fsm.RejectAnchor(4, testHash(104))
	require.Empty(t, again.Ranges)
	require.EqualValues(t, 1, fsm.Metrics().AnchorsRejected)
}

// TestRejectAnchorStalesStagedRangeThatStartsAtIt checks the staged range on
// the far side of a rejected anchor. A range staged from the anchor onward
// can never connect once another hash commits at that height, and it would
// block the span as existing work, so it is retired. The staged range that
// stops at the anchor stays for commit-time validation.
func TestRejectAnchorStalesStagedRangeThatStartsAtIt(t *testing.T) {
	fsm := NewHeaderSyncFSM(0, testHash(100), ProductionConfig())
	fsm.SeedCommittedAnchor()
	fsm.AddOrConfirmAnchor(4, testHash(104), false)
	fsm.AddOrConfirmAnchor(8, testHash(108), false)

	// A persistent liar staged both sides of its own boundary.
	_, ok, err := fsm.StageDiscoveredRange(1, "liar", 0, 4, time.Now())
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = fsm.StageDiscoveredRange(2, "liar", 4, 8, time.Now())
	require.NoError(t, err)
	require.True(t, ok)

	result := fsm.RejectAnchor(4, testHash(104))
	require.Equal(t, []RangeID{2}, result.Ranges)
	before, _ := fsm.RangeByID(1)
	require.Equal(t, RangeStaged, before.State)
	after, _ := fsm.RangeByID(2)
	require.Equal(t, RangeStale, after.State)

	// The honest boundary commits, and the span above it is planned
	// again instead of being blocked by the dead staged range.
	fsm.RejectAnchor(8, testHash(108))
	fsm.AddOrConfirmAnchor(8, testHash(208), false)
	require.True(t, fsm.AdvanceCommittedTip(4, testHash(204)))
	plan, err := fsm.PlanConfirmedAnchorRanges(10, 4, 8)
	require.NoError(t, err)
	require.Len(t, plan.Ranges, 1)
	require.Zero(t, plan.SkippedExisting)
}

// TestRejectAnchorSkipsTrustedAndMismatched checks that only the recorded
// untrusted hash can be rejected. A checkpoint or committed anchor, a
// different hash at the same height, and an unknown height all report no work.
func TestRejectAnchorSkipsTrustedAndMismatched(t *testing.T) {
	fsm := NewHeaderSyncFSM(0, testHash(100), ProductionConfig())
	fsm.SeedCommittedAnchor()
	addTrustedAnchor(t, fsm, 10, 110)
	fsm.AddOrConfirmAnchor(5, testHash(105), false)

	for _, point := range []ChainPoint{
		{Height: 10, Hash: testHash(110)},
		{Height: 5, Hash: testHash(66)},
		{Height: 7, Hash: testHash(107)},
		{Height: 0, Hash: testHash(100)},
	} {
		result := fsm.RejectAnchor(point.Height, point.Hash)
		require.Empty(t, result.Anchors, "height %d", point.Height)
		require.Empty(t, result.Ranges, "height %d", point.Height)
	}

	trusted, _ := fsm.AnchorByHeight(10)
	require.False(t, trusted.Rejected)
	unknown, _ := fsm.AnchorByHeight(5)
	require.False(t, unknown.Rejected)
	require.Zero(t, fsm.Metrics().AnchorsRejected)
}

// TestRejectedAnchorIsTrustedOnceCommitted checks the bound on rejecting a
// correct anchor for free. A peer that contradicts an honest anchor can only
// reject it until the span commits: the commit makes the boundary trusted,
// which clears the rejection and cannot be rejected again.
func TestRejectedAnchorIsTrustedOnceCommitted(t *testing.T) {
	fsm := NewHeaderSyncFSM(0, testHash(100), ProductionConfig())
	fsm.SeedCommittedAnchor()
	fsm.AddOrConfirmAnchor(4, testHash(104), false)
	fsm.RejectAnchor(4, testHash(104))

	// A fresh observation of the same hash starts the anchor over and
	// confirms it again under the one-observation policy.
	anchor, ok := fsm.AddOrConfirmAnchor(4, testHash(104), false)
	require.True(t, ok)
	require.True(t, fsm.AnchorConfirmed(anchor))

	_, ok, err := fsm.StageDiscoveredRange(1, "honest", 0, 4, time.Now())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []RangeID{1}, fsm.CommitReadyRanges().Committed)

	anchor, _ = fsm.AnchorByHeight(4)
	require.True(t, anchor.Trusted)
	require.False(t, anchor.Rejected)
	result := fsm.RejectAnchor(4, testHash(104))
	require.Empty(t, result.Anchors)
	anchor, _ = fsm.AnchorByHeight(4)
	require.False(t, anchor.Rejected)
}

// TestControllerAnchorConflictFlagsConflict checks that a well formed reply
// naming another hash than the recorded anchor is reported as a conflict, not
// as an invalid response. The caller punishes the latter and neither side of
// the former.
func TestControllerAnchorConflictFlagsConflict(t *testing.T) {
	ctx := t.Context()
	cfg := ProductionConfig()
	start := testHash(100)
	manager := NewManagerActor(NewHeaderSyncFSM(0, start, cfg), 8)
	manager.Start(ctx)
	defer manager.Stop()
	_, _, err := manager.AddAnchor(ctx, 0, start, true)
	require.NoError(t, err)

	session := NewSession()
	controller := NewController(manager, session, cfg)
	request := AnchorRequest{StartHeight: 0, StartHash: start}

	first := makeTestHeaders(start, 2)
	session.TrackAnchor("liar", request)
	result, ok, err := controller.FinishAnchorHeaders(
		ctx, "liar", first, DefaultMaxRangeHeaders,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, result.Rejected)
	require.False(t, result.Conflict)

	second := makeTestHeaders(start, 2)
	second[1].Nonce++
	session.TrackAnchor("honest", request)
	result, ok, err = controller.FinishAnchorHeaders(
		ctx, "honest", second, DefaultMaxRangeHeaders,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, result.Rejected)
	require.True(t, result.Conflict)
	require.NoError(t, result.Err)
	require.Equal(t, first[1].BlockHash(), result.Anchor.Hash,
		"result names the recorded anchor to reject")
	require.True(t, result.Anchor.Rejected)

	// A structurally invalid reply is not a conflict.
	session.TrackAnchor("bad", request)
	result, ok, err = controller.FinishAnchorHeaders(
		ctx, "bad", nil, DefaultMaxRangeHeaders,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, result.Rejected)
	require.False(t, result.Conflict)
	require.ErrorIs(t, result.Err, ErrNoHeaders)
}
