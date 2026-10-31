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

// TestContradictsRangeStopEvidence checks which replies refute a range's stop
// hash. Only a connected reply from the right start that reaches the stop
// height with another header does.
func TestContradictsRangeStopEvidence(t *testing.T) {
	start := testHash(100)
	honest := makeTestHeaders(start, 6)
	rng := HeaderRange{
		StartHeight: 2, StartHash: start, StopHeight: 5,
		StopHash: honest[2].BlockHash(),
	}

	// Different chain from the same start: the stop hash is refuted.
	other := makeTestHeaders(start, 6)
	other[2].Nonce++
	require.True(t, contradictsRangeStop(rng, other))

	// An overlong reply with a different header at the stop height.
	other = makeTestHeaders(start, 3)
	other[2].Nonce++
	require.True(t, contradictsRangeStop(rng, other))

	// A reply holding the expected stop header, even overlong, is fine.
	require.False(t, contradictsRangeStop(rng, honest))
	require.False(t, contradictsRangeStop(rng, honest[:3]))

	// A short reply never reaches the stop height.
	require.False(t, contradictsRangeStop(rng, honest[:2]))
	require.False(t, contradictsRangeStop(rng, nil))

	// A reply from another start or with a gap proves nothing.
	require.False(t, contradictsRangeStop(
		rng, makeTestHeaders(testHash(101), 3),
	))
	gap := makeTestHeaders(start, 3)
	gap[1].PrevBlock = testHash(200)
	gap[2].Nonce++
	require.False(t, contradictsRangeStop(rng, gap))
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
	broken := makeTestHeaders(start, 2)
	broken[1].PrevBlock = testHash(200)
	result, ok, err = controller.FinishAnchorHeaders(
		ctx, "bad", broken, DefaultMaxRangeHeaders,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, result.Rejected)
	require.False(t, result.Conflict)
	require.ErrorIs(t, result.Err, ErrRangeDisconnected)
}

// TestControllerStopAnchorContradicted checks that a reply refuting a stop
// hash counts against the anchor only while the hash is untrusted, and that
// RejectAnchor then clears the session state that depended on it.
func TestControllerStopAnchorContradicted(t *testing.T) {
	ctx := t.Context()
	cfg := ProductionConfig()
	start := testHash(100)
	manager := NewManagerActor(NewHeaderSyncFSM(0, start, cfg), 8)
	manager.Start(ctx)
	defer manager.Stop()
	_, _, err := manager.AddAnchor(ctx, 0, start, true)
	require.NoError(t, err)

	honest := makeTestHeaders(start, 4)
	stop := honest[3].BlockHash()
	_, _, err = manager.AddAnchor(ctx, 4, stop, false)
	require.NoError(t, err)

	session := NewSession()
	controller := NewController(manager, session, cfg)
	rng := HeaderRange{
		ID: 1, StartHeight: 0, StartHash: start, StopHeight: 4,
		StopHash: stop,
	}

	// A reply from a chain that disagrees at the stop height.
	other := makeTestHeaders(start, 4)
	other[3].Nonce++
	contradicted, err := controller.StopAnchorContradicted(ctx, rng, other)
	require.NoError(t, err)
	require.True(t, contradicted)

	// A reply that agrees, or that proves nothing, does not.
	contradicted, err = controller.StopAnchorContradicted(ctx, rng, honest)
	require.NoError(t, err)
	require.False(t, contradicted)
	contradicted, err = controller.StopAnchorContradicted(
		ctx, rng, other[:2],
	)
	require.NoError(t, err)
	require.False(t, contradicted)

	// Rejecting drops the range planned on the anchor and its request.
	_, ok, err := manager.PlanRange(ctx, 1, 0, 4)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = controller.RejectAnchor(
		ctx, 4, stop, time.Now().Add(time.Hour),
	)
	require.NoError(t, err)
	planned, ok, err := manager.SnapshotRange(ctx, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, RangeStale, planned.State)
	anchors, err := manager.Anchors(ctx)
	require.NoError(t, err)
	var rejected bool
	for _, anchor := range anchors {
		if anchor.Height == 4 {
			rejected = anchor.Rejected
		}
	}
	require.True(t, rejected)

	// A range left behind by a replaced anchor has no backing either: the
	// anchor at its stop height now holds another hash.
	replaced := HeaderRange{
		ID: 3, StartHeight: 0, StartHash: start, StopHeight: 4,
		StopHash: testHash(77),
	}
	contradicted, err = controller.StopAnchorContradicted(
		ctx, replaced, honest,
	)
	require.NoError(t, err)
	require.True(t, contradicted)

	// The same contradiction against a trusted anchor is the replier's
	// fault: a checkpoint, or a boundary that already committed.
	_, _, err = manager.AddAnchor(ctx, 8, testHash(108), true)
	require.NoError(t, err)
	trustedRng := HeaderRange{
		ID: 2, StartHeight: 4, StartHash: stop, StopHeight: 8,
		StopHash: testHash(108),
	}
	contradicted, err = controller.StopAnchorContradicted(
		ctx, trustedRng, makeTestHeaders(stop, 4),
	)
	require.NoError(t, err)
	require.False(t, contradicted)
}
