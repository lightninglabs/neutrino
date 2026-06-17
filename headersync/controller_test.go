package headersync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestControllerPlansCheckpointRanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(newTestFSM(0, 100), 8)
	manager.Start(ctx)
	defer manager.Stop()

	session := NewSession()
	controller := NewController(manager, session, DefaultConfig())

	plan, err := controller.PlanCheckpointRanges(
		ctx,
		ChainPoint{Height: 0, Hash: testHash(100)},
		ChainPoint{Height: 10, Hash: testHash(110)},
		10,
	)
	require.NoError(t, err)
	require.False(t, plan.WaitingForAnchors)
	require.Len(t, plan.Ranges, 1)
	require.EqualValues(t, 1, plan.Ranges[0].ID)
	require.EqualValues(t, 2, session.NextRangeID())

	plan, err = controller.PlanCheckpointRanges(
		ctx,
		ChainPoint{Height: 0, Hash: testHash(100)},
		ChainPoint{Height: 10, Hash: testHash(110)},
		10,
	)
	require.NoError(t, err)
	require.Empty(t, plan.Ranges)
	require.False(t, plan.WaitingForAnchors)
	require.EqualValues(t, 2, session.NextRangeID())
}

func TestControllerReportsAnchorDiscoveryNeeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cfg := DefaultConfig()
	cfg.MaxRangeHeaders = 5

	manager := NewManagerActor(
		NewHeaderSyncFSM(0, testHash(100), cfg), 8,
	)
	manager.Start(ctx)
	defer manager.Stop()

	controller := NewController(manager, NewSession(), cfg)
	plan, err := controller.PlanCheckpointRanges(
		ctx,
		ChainPoint{Height: 0, Hash: testHash(100)},
		ChainPoint{Height: 10, Hash: testHash(110)},
		10,
	)
	require.NoError(t, err)
	require.Empty(t, plan.Ranges)
	require.True(t, plan.WaitingForAnchors)
	require.EqualValues(t, 10, plan.Span)
	require.EqualValues(t, 5, plan.MaxRangeHeaders)

	span, ok, err := controller.AnchorDiscoverySpan(
		ctx,
		ChainPoint{Height: 0, Hash: testHash(100)},
		ChainPoint{Height: 10, Hash: testHash(110)},
		10,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 0, span.StartHeight)
	require.EqualValues(t, 10, span.StopHeight)
}

func TestControllerAnchorDiscoveryStartsAtFurthestConfirmedAnchor(
	t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cfg := DefaultConfig()
	cfg.MaxRangeHeaders = 5

	manager := NewManagerActor(
		NewHeaderSyncFSM(0, testHash(100), cfg), 8,
	)
	manager.Start(ctx)
	defer manager.Stop()

	session := NewSession()
	session.TrackRange("busy", RangeRequest{
		Assignment: RangeAssignment{RangeID: 1},
	})
	controller := NewController(manager, session, cfg)

	_, ok, err := manager.AddAnchor(ctx, 5, testHash(105), false)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = manager.AddAnchor(ctx, 5, testHash(105), false)
	require.NoError(t, err)
	require.True(t, ok)

	span, ok, err := controller.AnchorDiscoverySpan(
		ctx,
		ChainPoint{Height: 0, Hash: testHash(100)},
		ChainPoint{Height: 20, Hash: testHash(120)},
		20,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 5, span.StartHeight)
	require.Equal(t, testHash(105), span.StartHash)
	require.EqualValues(t, 20, span.StopHeight)
}

func TestControllerFrontierDiscoverySpan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cfg := DefaultConfig()
	cfg.MaxRangeHeaders = 5

	manager := NewManagerActor(
		NewHeaderSyncFSM(0, testHash(100), cfg), 8,
	)
	manager.Start(ctx)
	defer manager.Stop()

	controller := NewController(manager, NewSession(), cfg)
	_, ok, err := manager.AddAnchor(ctx, 5, testHash(105), false)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = manager.AddAnchor(ctx, 5, testHash(105), false)
	require.NoError(t, err)
	require.True(t, ok)

	span, ok, err := controller.FrontierDiscoverySpan(
		ctx, ChainPoint{Height: 0, Hash: testHash(100)}, 20,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 5, span.StartHeight)
	require.Equal(t, testHash(105), span.StartHash)
	require.EqualValues(t, 10, span.StopHeight)
	require.Equal(t, Hash{}, span.StopHash)
}

func TestControllerFrontierLookaheadSpan(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRangeHeaders = 5

	controller := NewController(nil, NewSession(), cfg)
	span, ok := controller.FrontierLookaheadSpan(
		ChainPoint{
			Height: 10,
			Hash:   testHash(110),
		},
		20,
	)
	require.True(t, ok)
	require.EqualValues(t, 10, span.StartHeight)
	require.Equal(t, testHash(110), span.StartHash)
	require.EqualValues(t, 15, span.StopHeight)
	require.Equal(t, Hash{}, span.StopHash)

	span, ok = controller.FrontierLookaheadSpan(
		ChainPoint{
			Height: 15,
			Hash:   testHash(115),
		},
		18,
	)
	require.True(t, ok)
	require.EqualValues(t, 15, span.StartHeight)
	require.EqualValues(t, 18, span.StopHeight)

	_, ok = controller.FrontierLookaheadSpan(
		ChainPoint{
			Height: 18,
			Hash:   testHash(118),
		},
		18,
	)
	require.False(t, ok)
}

func TestControllerPlanAnchorRequestsFiltersPeers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AnchorConfirmationsRequired = 2

	session := NewSession()
	session.TrackAnchor("active", AnchorRequest{
		StartHeight: 0,
		StartHash:   testHash(100),
		StopHeight:  10,
		StopHash:    testHash(110),
	})
	session.TrackRange("busy", RangeRequest{
		Assignment: RangeAssignment{RangeID: 1},
	})

	controller := NewController(nil, session, cfg)
	plan := controller.PlanAnchorRequests(
		AnchorRequest{
			StartHeight: 0,
			StartHash:   testHash(100),
			StopHeight:  10,
			StopHash:    testHash(110),
		},
		[]PeerCandidate{
			{ID: "short", Height: 9},
			{ID: "excluded", Height: 10},
			{ID: "busy", Height: 10},
			{ID: "next", Height: 10},
			{ID: "extra", Height: 10},
		},
		"excluded",
	)

	require.Equal(t, 1, plan.ActivePeers)
	require.Equal(t, 2, plan.EligiblePeers)
	require.Equal(t, 2, plan.RequiredPeers)
	require.Len(t, plan.Peers, 1)
	require.Equal(t, "next", plan.Peers[0].ID)
	require.True(t, plan.Active())
}

func TestControllerPlanAnchorRequestsStartsPartialDiscovery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AnchorConfirmationsRequired = 2

	controller := NewController(nil, NewSession(), cfg)
	plan := controller.PlanAnchorRequests(
		AnchorRequest{
			StartHeight: 0,
			StartHash:   testHash(100),
			StopHeight:  10,
			StopHash:    testHash(110),
		},
		[]PeerCandidate{
			{ID: "only-peer", Height: 10},
		},
	)

	require.Equal(t, 1, plan.EligiblePeers)
	require.Equal(t, 2, plan.RequiredPeers)
	require.True(t, plan.Deferred)
	require.Len(t, plan.Peers, 1)
	require.Equal(t, "only-peer", plan.Peers[0].ID)
	require.True(t, plan.Active())
}

func TestControllerPlanAnchorRequestsHedgesBeyondConfirmationCount(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AnchorConfirmationsRequired = 1
	cfg.AnchorRequestFanout = 3

	controller := NewController(nil, NewSession(), cfg)
	plan := controller.PlanAnchorRequests(
		AnchorRequest{
			StartHeight: 0,
			StartHash:   testHash(100),
			StopHeight:  10,
			StopHash:    testHash(110),
		},
		[]PeerCandidate{
			{ID: "peer-1", Height: 10},
			{ID: "peer-2", Height: 10},
			{ID: "peer-3", Height: 10},
			{ID: "peer-4", Height: 10},
		},
	)

	require.Equal(t, 4, plan.EligiblePeers)
	require.Equal(t, 1, plan.RequiredPeers)
	require.Len(t, plan.Peers, 3)
	require.Equal(t, "peer-1", plan.Peers[0].ID)
	require.Equal(t, "peer-2", plan.Peers[1].ID)
	require.Equal(t, "peer-3", plan.Peers[2].ID)
}

func TestControllerFinishAnchorHeadersConfirmsDiscovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cfg := DefaultConfig()
	cfg.AnchorConfirmationsRequired = 2

	start := testHash(100)
	headers := makeTestHeaders(start, 2)
	request := AnchorRequest{
		StartHeight: 0,
		StartHash:   start,
		StopHeight:  10,
		StopHash:    testHash(110),
	}

	manager := NewManagerActor(NewHeaderSyncFSM(0, start, cfg), 8)
	manager.Start(ctx)
	defer manager.Stop()
	_, ok, err := manager.AddAnchor(ctx, 0, start, true)
	require.NoError(t, err)
	require.True(t, ok)

	session := NewSession()
	session.TrackAnchor("peer-a", request)
	controller := NewController(manager, session, cfg)

	result, ok, err := controller.FinishAnchorHeaders(
		ctx, "peer-a", headers, DefaultMaxRangeHeaders,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, result.Rejected)
	require.False(t, result.Response.Trusted)
	require.False(t, result.Confirmed)
	require.EqualValues(t, 2, result.Response.Height)
	require.Equal(t, headers[1].BlockHash(), result.Response.Hash)
	require.False(t, session.AnchorActive("peer-a"))

	session.TrackAnchor("peer-b", request)
	result, ok, err = controller.FinishAnchorHeaders(
		ctx, "peer-b", headers, DefaultMaxRangeHeaders,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, result.Rejected)
	require.True(t, result.Confirmed)
	require.Equal(t, 2, result.Anchor.Confirmations)
	require.True(t, result.Staged)
	require.EqualValues(t, 1, result.Range.ID)
	require.EqualValues(t, 0, result.Range.StartHeight)
	require.EqualValues(t, 2, result.Range.StopHeight)
	require.Equal(t, RangeStaged, result.Range.State)
}

func TestControllerFinishAnchorHeadersRejectsInvalidResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := testHash(100)
	manager := NewManagerActor(newTestFSM(0, 100), 8)
	manager.Start(ctx)
	defer manager.Stop()

	session := NewSession()
	session.TrackAnchor("peer-a", AnchorRequest{
		StartHeight: 0,
		StartHash:   start,
		StopHeight:  10,
		StopHash:    testHash(110),
	})
	controller := NewController(manager, session, DefaultConfig())

	result, ok, err := controller.FinishAnchorHeaders(
		ctx, "peer-a", makeTestHeaders(testHash(101), 1),
		DefaultMaxRangeHeaders,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, result.Rejected)
	require.Contains(t, result.Reason, ErrRangeStartMismatch.Error())
	require.False(t, session.AnchorActive("peer-a"))
}

func TestControllerAssignQueuedRangesHonorsControllerLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(newTestFSM(0, 100), 8)
	manager.Start(ctx)
	defer manager.Stop()

	addManagerAnchor(t, ctx, manager, 0, 100)
	addManagerAnchor(t, ctx, manager, 10, 110)
	addManagerAnchor(t, ctx, manager, 20, 120)
	addManagerAnchor(t, ctx, manager, 30, 130)
	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "peer",
		State: PeerReady,
	}))
	mustManagerPlan(t, ctx, manager, 1, 0, 10)
	mustManagerPlan(t, ctx, manager, 2, 10, 20)
	mustManagerPlan(t, ctx, manager, 3, 20, 30)

	cfg := DefaultConfig()
	cfg.MaxPeerOutstandingRanges = 2
	controller := NewController(manager, NewSession(), cfg)

	assignments, err := controller.AssignQueuedRanges(ctx, 1)
	require.NoError(t, err)
	require.Len(t, assignments, 2)
	require.EqualValues(t, 1, assignments[0].RangeID)
	require.EqualValues(t, 2, assignments[1].RangeID)
}

func TestControllerCommitReadyPrunesObsoleteAnchors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(newTestFSM(0, 100), 8)
	manager.Start(ctx)
	defer manager.Stop()

	addManagerAnchor(t, ctx, manager, 0, 100)
	addManagerAnchor(t, ctx, manager, 10, 110)
	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "peer",
		State: PeerReady,
	}))
	mustManagerPlan(t, ctx, manager, 1, 0, 10)
	_, ok, err := manager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	assignment, ok, err := manager.StartPeerRange(ctx, "peer")
	require.NoError(t, err)
	require.True(t, ok)
	_, err = manager.CompleteRange(
		ctx, "peer", assignment.RangeID, assignment.LeaseEpoch, true,
		time.Now(),
	)
	require.NoError(t, err)

	session := NewSession()
	session.TrackAnchor("obsolete", AnchorRequest{
		StartHeight: 0,
		StartHash:   testHash(100),
		StopHeight:  10,
		StopHash:    testHash(110),
	})
	session.TrackAnchor("future", AnchorRequest{
		StartHeight: 10,
		StartHash:   testHash(110),
		StopHeight:  20,
		StopHash:    testHash(120),
	})

	controller := NewController(manager, session, DefaultConfig())
	result, err := controller.CommitReadyRanges(ctx)
	require.NoError(t, err)
	require.Equal(t, []RangeID{1}, result.Commit.Committed)
	require.EqualValues(t, 10, result.Commit.TipHeight)
	require.Equal(t, 1, result.PrunedAnchors)
	require.False(t, session.AnchorActive("obsolete"))
	require.True(t, session.AnchorActive("future"))
}

func TestControllerReplanRangeAllocatesSessionID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(newTestFSM(0, 100), 8)
	manager.Start(ctx)
	defer manager.Stop()

	addManagerAnchor(t, ctx, manager, 0, 100)
	addManagerAnchor(t, ctx, manager, 10, 110)
	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "peer",
		State: PeerReady,
	}))
	mustManagerPlan(t, ctx, manager, 1, 0, 10)
	_, ok, err := manager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	assignment, ok, err := manager.StartPeerRange(ctx, "peer")
	require.NoError(t, err)
	require.True(t, ok)
	_, err = manager.CompleteRange(
		ctx, "peer", assignment.RangeID, assignment.LeaseEpoch, false,
		time.Now(),
	)
	require.NoError(t, err)

	failedRange, ok, err := manager.SnapshotRange(ctx, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, RangeFailed, failedRange.State)

	session := NewSession()
	session.SetNextRangeID(2)
	controller := NewController(manager, session, DefaultConfig())
	result, err := controller.ReplanRange(ctx, failedRange)
	require.NoError(t, err)
	require.True(t, result.OK)
	require.EqualValues(t, 2, result.ID)
	require.EqualValues(t, 3, session.NextRangeID())
}
