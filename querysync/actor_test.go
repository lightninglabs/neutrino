package querysync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestManagerActorDispatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.Start(ctx)
	defer manager.Stop()

	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "peer",
		Rank:  0,
		State: PeerReady,
	}))
	require.NoError(t, manager.Enqueue(ctx, Task{ID: 1}))

	assignment, ok, err := manager.AskDispatch(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "peer", assignment.PeerID)
	require.EqualValues(t, 1, assignment.TaskID)
}

func TestManagerActorDispatchTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.Start(ctx)
	defer manager.Stop()

	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "avoided",
		Rank:  0,
		State: PeerReady,
	}))
	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "selected",
		Rank:  10,
		State: PeerReady,
	}))

	assignment, ok, err := manager.AskDispatchTask(ctx, Task{
		ID:         7,
		Timeout:    5 * time.Second,
		AvoidPeers: []string{"avoided"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "selected", assignment.PeerID)
	require.EqualValues(t, 7, assignment.TaskID)
	require.Equal(t, 5*time.Second, assignment.Timeout)
}

func TestManagerActorAssignLocalTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.Start(ctx)
	defer manager.Stop()

	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:        "loaded",
		Rank:      0,
		State:     PeerReady,
		LocalWork: []TaskID{1, 2},
	}))
	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "empty",
		Rank:  10,
		State: PeerReady,
	}))

	assignment, ok, err := manager.AskAssignLocalTask(ctx, Task{ID: 3})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "empty", assignment.PeerID)

	peer, ok, err := manager.SnapshotPeer(ctx, "empty")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []TaskID{3}, peer.LocalWork)
}

func TestManagerActorRecordsSchedulerEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	events := &SchedulerEventRecorder{}
	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.SetEventSink(events)
	manager.Start(ctx)
	defer manager.Stop()

	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:        "donor",
		Rank:      10,
		State:     PeerReady,
		LocalWork: []TaskID{1, 2},
	}))
	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "owner",
		Rank:  0,
		State: PeerReady,
	}))
	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "thief",
		Rank:  20,
		State: PeerReady,
	}))

	assignment, ok, err := manager.AskAssignLocalTask(ctx, Task{
		ID:      3,
		BatchID: 9,
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "owner", assignment.PeerID)

	result, ok, err := manager.AskSteal(ctx, "thief")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "donor", result.DonorID)

	var ownedEvent, stealEvent *SchedulerEvent
	for _, event := range events.Events() {
		event := event
		switch event.Type {
		case SchedulerEventTaskOwned:
			ownedEvent = &event

		case SchedulerEventWorkStolen:
			stealEvent = &event
		}
	}

	require.NotNil(t, ownedEvent)
	require.True(t, ownedEvent.OK)
	require.Equal(t, "owner", ownedEvent.PeerID)
	require.EqualValues(t, 3, ownedEvent.TaskID)
	require.EqualValues(t, 9, ownedEvent.BatchID)
	require.NotNil(t, ownedEvent.Assignment)
	require.EqualValues(t, 1, ownedEvent.Metrics.Owned)

	require.NotNil(t, stealEvent)
	require.True(t, stealEvent.OK)
	require.Equal(t, "thief", stealEvent.ThiefID)
	require.Equal(t, "donor", stealEvent.DonorID)
	require.EqualValues(t, 1, stealEvent.TaskID)
	require.NotNil(t, stealEvent.Steal)
	require.EqualValues(t, 1, stealEvent.Metrics.Stolen)
}

func TestManagerActorPeerLifecycleEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.Start(ctx)
	defer manager.Stop()

	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "peer",
		Rank:  0,
		State: PeerReady,
	}))
	require.NoError(t, manager.UpdatePeerRTT(ctx, "peer",
		125*time.Millisecond))

	peer, ok, err := manager.SnapshotPeer(ctx, "peer")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 125*time.Millisecond, peer.RTT)

	require.NoError(t, manager.Enqueue(ctx, Task{
		ID:       1,
		BatchID:  99,
		Canceled: false,
	}))
	require.NoError(t, manager.CancelBatch(ctx, 99))

	_, ok, err = manager.AskDispatch(ctx)
	require.NoError(t, err)
	require.False(t, ok)

	metrics, err := manager.Metrics(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, metrics.DroppedCanceled)

	require.NoError(t, manager.Enqueue(ctx, Task{ID: 2}))
	_, ok, err = manager.AskDispatch(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	peer, ok, err = manager.SnapshotPeer(ctx, "peer")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, PeerBusy, peer.State)

	require.NoError(t, manager.CompletePeerWork(ctx, "peer"))
	peer, ok, err = manager.SnapshotPeer(ctx, "peer")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, PeerReady, peer.State)

	require.NoError(t, manager.RemovePeer(ctx, "peer"))
	_, ok, err = manager.SnapshotPeer(ctx, "peer")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPeerActorRequestsStolenWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.Start(ctx)
	defer manager.Stop()

	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:        "donor",
		Rank:      0,
		State:     PeerReady,
		LocalWork: []TaskID{10, 11, 12},
	}))

	thief := NewPeerActor("thief", 10, manager, 8)
	require.NoError(t, thief.Start(ctx))
	defer thief.Stop()

	result, ok, err := thief.RequestStolenWork(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "thief", result.ThiefID)
	require.Equal(t, "donor", result.DonorID)
	require.EqualValues(t, 10, result.TaskID)
}

func TestPeerActorBlockedNeedWorkReturnsNoSteal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.Start(ctx)
	defer manager.Stop()

	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:        "donor",
		Rank:      0,
		State:     PeerReady,
		LocalWork: []TaskID{10, 11, 12},
	}))

	thief := NewPeerActor("thief", 10, manager, 8)
	require.NoError(t, thief.Start(ctx))
	defer thief.Stop()

	require.NoError(t, thief.Send(ctx, PeerBlockedEvent{}))

	_, ok, err := thief.RequestStolenWork(ctx)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPeerActorUpdatesManagerState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	fsm := NewSchedulerFSM(DefaultConfig())
	manager := NewManagerActor(fsm, 8)
	manager.Start(ctx)
	defer manager.Stop()

	peer := NewPeerActor("peer", 0, manager, 8)
	require.NoError(t, peer.Start(ctx))
	defer peer.Stop()

	require.NoError(t, peer.Send(ctx, PeerBlockedEvent{}))
	require.NoError(t, manager.Enqueue(ctx, Task{ID: 1}))

	_, ok, err := manager.AskDispatch(ctx)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, peer.Send(ctx, PeerReadyEvent{}))
	assignment, ok, err := manager.AskDispatch(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "peer", assignment.PeerID)
}

func TestPeerActorRankUpdatesManagerState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.Start(ctx)
	defer manager.Stop()

	peer := NewPeerActor("peer", 10, manager, 8)
	require.NoError(t, peer.Start(ctx))
	defer peer.Stop()

	require.NoError(t, peer.Send(ctx, PeerRankEvent{Rank: 3}))

	snapshot, ok, err := manager.SnapshotPeer(ctx, "peer")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 3, snapshot.Rank)
}

func TestPeerActorQuarantineBlocksDispatchUntilReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.Start(ctx)
	defer manager.Stop()

	peer := NewPeerActor("peer", 0, manager, 8)
	require.NoError(t, peer.Start(ctx))
	defer peer.Stop()

	require.NoError(t, peer.Send(ctx, PeerQuarantinedEvent{}))
	require.NoError(t, manager.Enqueue(ctx, Task{ID: 1}))

	_, ok, err := manager.AskDispatch(ctx)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, peer.Send(ctx, PeerReadyEvent{}))
	assignment, ok, err := manager.AskDispatch(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "peer", assignment.PeerID)
}
