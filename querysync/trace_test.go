package querysync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTraceRecorderReplaysActorDispatchTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	trace := &TraceRecorder{}
	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.SetTraceRecorder(trace)
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

	_, ok, err := manager.AskDispatchTask(ctx, Task{
		ID:         7,
		Timeout:    5 * time.Second,
		AvoidPeers: []string{"avoided"},
	})
	require.NoError(t, err)
	require.True(t, ok)

	result, err := ReplayTrace(ctx, DefaultConfig(), trace.Events())
	require.NoError(t, err)
	require.Len(t, result.Dispatches, 1)
	require.Equal(t, BridgeDispatchWant{
		OK:            true,
		PeerID:        "selected",
		TaskID:        7,
		TimeoutMillis: 5000,
	}, result.Dispatches[0])
}

func TestTraceRecorderReplaysLocalAssignment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	trace := &TraceRecorder{}
	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.SetTraceRecorder(trace)
	manager.Start(ctx)
	defer manager.Stop()

	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID:    "peer",
		State: PeerReady,
	}))

	assignment, ok, err := manager.AskAssignLocalTask(ctx, Task{ID: 99})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "peer", assignment.PeerID)

	result, err := ReplayTrace(ctx, DefaultConfig(), trace.Events())
	require.NoError(t, err)
	require.Len(t, result.Dispatches, 1)
	require.EqualValues(t, 99, result.Dispatches[0].TaskID)
}

func TestTraceRecorderReplaysPeerActorWorkSteal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	trace := &TraceRecorder{}
	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.SetTraceRecorder(trace)
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

	steal, ok, err := thief.RequestStolenWork(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 10, steal.TaskID)

	result, err := ReplayTrace(ctx, DefaultConfig(), trace.Events())
	require.NoError(t, err)
	require.Len(t, result.Steals, 1)
	require.Equal(t, BridgeStealWant{
		OK:             true,
		ThiefID:        "thief",
		DonorID:        "donor",
		TaskID:         10,
		TaskIDs:        []TaskID{10, 11},
		DonorRemaining: 1,
		ThiefWorkLen:   2,
	}, result.Steals[0])
}
