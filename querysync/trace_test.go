package querysync

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTracePreservesFractionalDurations(t *testing.T) {
	for _, update := range []bool{false, true} {
		t.Run(fmt.Sprint(update), func(t *testing.T) {
			ctx := t.Context()
			trace := &TraceRecorder{}
			manager := NewManagerActor(
				NewSchedulerFSM(DefaultConfig()), 8,
			)
			manager.SetTraceRecorder(trace)
			manager.Start(ctx)
			defer manager.Stop()

			for i, id := range []string{"a", "b"} {
				rtt := time.Duration(100900-i*800) *
					time.Microsecond
				peer := PeerSnapshot{ID: id, State: PeerReady}
				if !update {
					peer.RTT = rtt
				}
				require.NoError(t, manager.AddPeer(ctx, peer))
				if update {
					err := manager.UpdatePeerRTT(
						ctx, id, rtt,
					)
					require.NoError(t, err)
				}
			}
			assignment, ok, err := manager.AskDispatchTask(
				ctx, Task{ID: 1},
			)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, "b", assignment.PeerID)

			data, err := json.Marshal(trace.Events())
			require.NoError(t, err)
			var events []TraceEvent
			require.NoError(t, json.Unmarshal(data, &events))
			_, err = ReplayTrace(ctx, DefaultConfig(), events)
			require.NoError(t, err)
		})
	}
}

func TestTracePreservesSubMillisecondTaskTimeout(t *testing.T) {
	ctx := t.Context()
	trace := &TraceRecorder{}
	manager := NewManagerActor(NewSchedulerFSM(DefaultConfig()), 8)
	manager.SetTraceRecorder(trace)
	manager.Start(ctx)
	defer manager.Stop()
	require.NoError(t, manager.AddPeer(ctx, PeerSnapshot{
		ID: "peer", State: PeerReady,
	}))
	_, ok, err := manager.AskDispatchTask(ctx, Task{
		ID: 1, Timeout: 900 * time.Microsecond,
	})
	require.NoError(t, err)
	require.True(t, ok)
	events := trace.Events()
	require.Equal(t, 900*time.Microsecond,
		taskFromBridge(*events[len(events)-1].Task).Timeout)
	_, err = ReplayTrace(ctx, DefaultConfig(), trace.Events())
	require.NoError(t, err)
}

func TestTraceAcceptsLegacyMilliseconds(t *testing.T) {
	data := []byte(`[
		{"op":"add_peer","peer":{
			"id":"peer","rank":0,"rtt_ms":101,"state":"ready"
		}},
		{"op":"update_peer_rtt","peer_id":"peer","rtt_ms":100},
		{"op":"dispatch_task","task":{"id":1,"timeout_ms":5000},
		 "dispatch":{"ok":true,"peer_id":"peer","task_id":1,
		 "timeout_ms":5000}}
	]`)
	var events []TraceEvent
	require.NoError(t, json.Unmarshal(data, &events))
	_, err := ReplayTrace(t.Context(), DefaultConfig(), events)
	require.NoError(t, err)
}

func TestTraceRecorderReplaysActorDispatchTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
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
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
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
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
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
