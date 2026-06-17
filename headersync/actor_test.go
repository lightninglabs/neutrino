package headersync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestManagerActorRangeLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(newTestFSM(0, 100), 8)
	manager.Start(ctx)
	defer manager.Stop()

	addManagerAnchor(t, ctx, manager, 0, 100)
	addManagerAnchor(t, ctx, manager, 10, 110)
	addManagerAnchor(t, ctx, manager, 20, 120)
	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "peer1",
		State: PeerReady,
	}))
	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "peer2",
		State: PeerReady,
	}))
	mustManagerPlan(t, ctx, manager, 1, 0, 10)
	mustManagerPlan(t, ctx, manager, 2, 10, 20)

	_, ok, err := manager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = manager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	first, ok, err := manager.StartPeerRange(ctx, "peer1")
	require.NoError(t, err)
	require.True(t, ok)
	second, ok, err := manager.StartPeerRange(ctx, "peer2")
	require.NoError(t, err)
	require.True(t, ok)

	_, err = manager.CompleteRange(
		ctx, "peer2", second.RangeID, second.LeaseEpoch, true,
		time.Unix(100, 0),
	)
	require.NoError(t, err)
	commit, err := manager.CommitReadyRanges(ctx)
	require.NoError(t, err)
	require.Empty(t, commit.Committed)

	stats, err := manager.CommitStats(ctx, time.Unix(105, 0))
	require.NoError(t, err)
	require.Equal(t, 1, stats.StagedCount)

	_, err = manager.CompleteRange(
		ctx, "peer1", first.RangeID, first.LeaseEpoch, true,
		time.Unix(101, 0),
	)
	require.NoError(t, err)
	commit, err = manager.CommitReadyRanges(ctx)
	require.NoError(t, err)
	require.Equal(t, []RangeID{1, 2}, commit.Committed)
	require.EqualValues(t, 20, commit.TipHeight)
}

func TestPeerActorRequestsAndCompletesRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(newTestFSM(0, 100), 8)
	manager.Start(ctx)
	defer manager.Stop()

	addManagerAnchor(t, ctx, manager, 0, 100)
	addManagerAnchor(t, ctx, manager, 10, 110)

	peer := NewPeerActor("peer", 0, manager, 8)
	require.NoError(t, peer.Start(ctx))
	defer peer.Stop()

	mustManagerPlan(t, ctx, manager, 1, 0, 10)
	_, ok, err := manager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	assignment, ok, err := peer.RequestRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 1, assignment.RangeID)

	snapshot, ok, err := manager.SnapshotPeer(ctx, "peer")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, PeerBusy, snapshot.State)
	require.EqualValues(t, 1, snapshot.ActiveRange)

	result, err := peer.CompleteRange(
		ctx, assignment.RangeID, assignment.LeaseEpoch, true,
		time.Unix(100, 0),
	)
	require.NoError(t, err)
	require.True(t, result.Accepted)

	snapshot, ok, err = manager.SnapshotPeer(ctx, "peer")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, PeerReady, snapshot.State)
	require.Equal(t, NoRange, snapshot.ActiveRange)

	commit, err := manager.CommitReadyRanges(ctx)
	require.NoError(t, err)
	require.Equal(t, []RangeID{1}, commit.Committed)
}

func TestPeerActorRequestsStolenRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(newTestFSM(0, 100), 8)
	manager.Start(ctx)
	defer manager.Stop()

	addManagerAnchor(t, ctx, manager, 0, 100)
	addManagerAnchor(t, ctx, manager, 10, 110)
	addManagerAnchor(t, ctx, manager, 20, 120)
	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "donor",
		State: PeerReady,
	}))
	mustManagerPlan(t, ctx, manager, 1, 0, 10)
	mustManagerPlan(t, ctx, manager, 2, 10, 20)
	_, ok, err := manager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = manager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	thief := NewPeerActor("thief", 10, manager, 8)
	require.NoError(t, thief.Start(ctx))
	defer thief.Stop()

	result, ok, err := thief.RequestStolenRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "thief", result.ThiefID)
	require.Equal(t, "donor", result.DonorID)
	require.EqualValues(t, 1, result.RangeID)

	snapshot, ok, err := manager.SnapshotPeer(ctx, "thief")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []RangeID{1}, snapshot.LocalRanges)
}

func TestPeerActorClearsStolenActiveRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(newTestFSM(0, 100), 8)
	manager.Start(ctx)
	defer manager.Stop()

	addManagerAnchor(t, ctx, manager, 0, 100)
	addManagerAnchor(t, ctx, manager, 10, 110)

	donor := NewPeerActor("donor", 0, manager, 8)
	require.NoError(t, donor.Start(ctx))
	defer donor.Stop()
	thief := NewPeerActor("thief", 10, manager, 8)
	require.NoError(t, thief.Start(ctx))
	defer thief.Stop()

	mustManagerPlan(t, ctx, manager, 1, 0, 10)
	_, ok, err := manager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	assignment, ok, err := donor.RequestRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, donor.Send(ctx, PeerQuarantinedEvent{}))

	steal, ok, err := manager.StealRange(ctx, "thief", assignment.RangeID)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, donor.Send(ctx, PeerRangeStolenEvent{
		RangeID:       steal.RangeID,
		OldLeaseEpoch: steal.OldLeaseEpoch,
	}))
	require.NoError(t, donor.Send(ctx, PeerReadyEvent{}))

	snapshot, ok, err := manager.SnapshotPeer(ctx, "donor")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, PeerReady, snapshot.State)
	require.Equal(t, NoRange, snapshot.ActiveRange)
}

func TestPeerActorBlockedDoesNotRequestRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(newTestFSM(0, 100), 8)
	manager.Start(ctx)
	defer manager.Stop()

	peer := NewPeerActor("peer", 0, manager, 8)
	require.NoError(t, peer.Start(ctx))
	defer peer.Stop()
	require.NoError(t, peer.Send(ctx, PeerBlockedEvent{}))

	_, ok, err := peer.RequestRange(ctx)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPeerActorCompletionPreservesQuarantine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(newTestFSM(0, 100), 8)
	manager.Start(ctx)
	defer manager.Stop()

	addManagerAnchor(t, ctx, manager, 0, 100)
	addManagerAnchor(t, ctx, manager, 10, 110)

	peer := NewPeerActor("peer", 0, manager, 8)
	require.NoError(t, peer.Start(ctx))
	defer peer.Stop()

	mustManagerPlan(t, ctx, manager, 1, 0, 10)
	_, ok, err := manager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	assignment, ok, err := peer.RequestRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, peer.Send(ctx, PeerQuarantinedEvent{}))
	result, err := peer.CompleteRange(
		ctx, assignment.RangeID, assignment.LeaseEpoch, false,
		time.Unix(100, 0),
	)
	require.NoError(t, err)
	require.True(t, result.Accepted)

	snapshot, ok, err := manager.SnapshotPeer(ctx, "peer")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, PeerQuarantined, snapshot.State)
	require.Equal(t, NoRange, snapshot.ActiveRange)
}

func TestManagerActorRecordsStructuredEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	events := &EventRecorder{}
	manager := NewManagerActor(newTestFSM(0, 100), 8)
	manager.SetEventSink(events)
	manager.Start(ctx)
	defer manager.Stop()

	addManagerAnchor(t, ctx, manager, 0, 100)
	addManagerAnchor(t, ctx, manager, 10, 110)
	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "peer",
		State: PeerReady,
	}))
	mustManagerPlan(t, ctx, manager, 1, 0, 10)
	assignment, ok, err := manager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	assignment, ok, err = manager.StartPeerRange(ctx, assignment.PeerID)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = manager.CompleteRange(
		ctx, assignment.PeerID, assignment.RangeID,
		assignment.LeaseEpoch, true, time.Unix(100, 0),
	)
	require.NoError(t, err)
	_, err = manager.CommitReadyRanges(ctx)
	require.NoError(t, err)

	var planned, assigned, started, completed, committed *Event
	for _, event := range events.Events() {
		event := event
		switch event.Type {
		case EventRangePlanned:
			planned = &event
		case EventRangeAssigned:
			assigned = &event
		case EventRangeStarted:
			started = &event
		case EventRangeCompleted:
			completed = &event
		case EventRangeCommitted:
			committed = &event
		}
	}

	require.NotNil(t, planned)
	require.True(t, planned.OK)
	require.EqualValues(t, 1, planned.RangeID)
	require.NotNil(t, assigned)
	require.Equal(t, "peer", assigned.PeerID)
	require.NotNil(t, started)
	require.EqualValues(t, 1, started.LeaseEpoch)
	require.NotNil(t, completed)
	require.True(t, completed.Valid)
	require.NotNil(t, committed)
	require.Equal(t, []RangeID{1}, committed.Committed)
	require.EqualValues(t, 1, committed.Metrics.RangesCommitted)
}

func addManagerAnchor(t *testing.T, ctx context.Context,
	manager *ManagerActor, height uint32, hashByte byte) {

	t.Helper()
	anchor, ok, err := manager.AddAnchor(ctx, height, testHash(hashByte),
		true)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, manager.fsm.AnchorConfirmed(anchor))
}

func mustManagerPlan(t *testing.T, ctx context.Context,
	manager *ManagerActor, id RangeID, startHeight, stopHeight uint32) {

	t.Helper()
	_, ok, err := manager.PlanRange(ctx, id, startHeight, stopHeight)
	require.NoError(t, err)
	require.True(t, ok)
}
