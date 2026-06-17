package querysync

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSchedulerSkipsBlockedRankedPeer(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "best-but-blocked",
		Rank:  0,
		RTT:   20 * time.Millisecond,
		State: PeerBlocked,
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "capable",
		Rank:  10,
		RTT:   50 * time.Millisecond,
		State: PeerReady,
	}))

	fsm.Enqueue(Task{ID: 100})

	assignment, ok := fsm.DispatchNext()
	require.True(t, ok)
	require.Equal(t, "capable", assignment.PeerID)
	require.EqualValues(t, 100, assignment.TaskID)
}

func TestSchedulerSkipsQuarantinedRankedPeer(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "best-but-quarantined",
		Rank:  0,
		RTT:   20 * time.Millisecond,
		State: PeerQuarantined,
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "capable",
		Rank:  10,
		RTT:   50 * time.Millisecond,
		State: PeerReady,
	}))

	fsm.Enqueue(Task{ID: 100})

	assignment, ok := fsm.DispatchNext()
	require.True(t, ok)
	require.Equal(t, "capable", assignment.PeerID)
	require.EqualValues(t, 100, assignment.TaskID)
}

func TestSchedulerDropsCanceledQueuedWork(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "peer",
		Rank:  0,
		State: PeerReady,
	}))

	fsm.Enqueue(Task{ID: 100, BatchID: 1, Canceled: true})

	_, ok := fsm.DispatchNext()
	require.False(t, ok)
	require.EqualValues(t, 1, fsm.Metrics().DroppedCanceled)
	require.Zero(t, fsm.QueueLen())
}

func TestSchedulerRTTTimeoutFloor(t *testing.T) {
	peer := PeerSnapshot{
		ID:    "peer",
		RTT:   750 * time.Millisecond,
		State: PeerReady,
	}

	timeout := QueryTimeoutForPeer(
		DefaultMinQueryTimeout, peer, DefaultConfig(),
	)
	require.Equal(t, 6*time.Second, timeout)

	peer.RTT = 9 * time.Second
	timeout = QueryTimeoutForPeer(
		DefaultMinQueryTimeout, peer, DefaultConfig(),
	)
	require.Equal(t, DefaultMaxQueryTimeout, timeout)
}

func TestSchedulerDispatchTaskUsesTaskTimeoutAndAvoidsPeers(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "avoided",
		Rank:  0,
		RTT:   500 * time.Millisecond,
		State: PeerReady,
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "selected",
		Rank:  10,
		RTT:   500 * time.Millisecond,
		State: PeerReady,
	}))

	assignment, ok := fsm.DispatchTask(Task{
		ID:         10,
		Timeout:    10 * time.Second,
		AvoidPeers: []string{"avoided"},
	})
	require.True(t, ok)
	require.Equal(t, "selected", assignment.PeerID)
	require.EqualValues(t, 10, assignment.TaskID)
	require.Equal(t, 10*time.Second, assignment.Timeout)
}

func TestSchedulerDispatchMarksPeerBusy(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "peer",
		Rank:  0,
		State: PeerReady,
	}))

	fsm.Enqueue(Task{ID: 1})
	_, ok := fsm.DispatchNext()
	require.True(t, ok)

	peer, ok := fsm.SnapshotPeer("peer")
	require.True(t, ok)
	require.Equal(t, PeerBusy, peer.State)

	require.NoError(t, fsm.CompletePeerWork("peer"))
	peer, ok = fsm.SnapshotPeer("peer")
	require.True(t, ok)
	require.Equal(t, PeerReady, peer.State)
}

func TestSchedulerAssignLocalTaskBalancesOwnedWork(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:        "loaded",
		Rank:      0,
		State:     PeerReady,
		LocalWork: []TaskID{1, 2},
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "empty",
		Rank:  10,
		State: PeerReady,
	}))

	assignment, ok := fsm.AssignLocalTask(Task{ID: 3})
	require.True(t, ok)
	require.Equal(t, "empty", assignment.PeerID)

	empty, ok := fsm.SnapshotPeer("empty")
	require.True(t, ok)
	require.Equal(t, []TaskID{3}, empty.LocalWork)
	require.EqualValues(t, 1, fsm.Metrics().Owned)
}

func TestSchedulerAssignLocalTaskBuildsBoundedLocalQueue(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:        "peer",
		State:     PeerReady,
		LocalWork: []TaskID{1},
	}))

	assignment, ok := fsm.AssignLocalTask(Task{ID: 2})
	require.True(t, ok)
	require.Equal(t, "peer", assignment.PeerID)

	peer, ok := fsm.SnapshotPeer("peer")
	require.True(t, ok)
	require.Equal(t, []TaskID{1, 2}, peer.LocalWork)
}

func TestSchedulerAssignLocalTaskDoesNotExceedPeerCapacity(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:        "peer",
		State:     PeerReady,
		LocalWork: []TaskID{1, 2, 3, 4},
	}))

	_, ok := fsm.AssignLocalTask(Task{ID: 5})
	require.False(t, ok)

	peer, ok := fsm.SnapshotPeer("peer")
	require.True(t, ok)
	require.Equal(t, []TaskID{1, 2, 3, 4}, peer.LocalWork)
}

func TestSchedulerBusyPeerActiveSlotCountsTowardCapacity(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:        "peer",
		State:     PeerBusy,
		LocalWork: []TaskID{1, 2, 3},
	}))

	_, ok := fsm.AssignLocalTask(Task{ID: 4})
	require.False(t, ok)

	peer, ok := fsm.SnapshotPeer("peer")
	require.True(t, ok)
	require.Equal(t, []TaskID{1, 2, 3}, peer.LocalWork)
}

func TestWorkStealingMovesUnclaimedWorkToIdlePeer(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:        "donor",
		Rank:      0,
		State:     PeerReady,
		LocalWork: []TaskID{10, 11, 12},
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "thief",
		Rank:  10,
		State: PeerReady,
	}))

	result, ok := fsm.StealOne("thief")
	require.True(t, ok)
	require.Equal(t, "thief", result.ThiefID)
	require.Equal(t, "donor", result.DonorID)
	require.EqualValues(t, 10, result.TaskID)
	require.Equal(t, []TaskID{10, 11}, result.TaskIDs)
	require.Equal(t, 1, result.DonorRemaining)

	donor, ok := fsm.SnapshotPeer("donor")
	require.True(t, ok)
	require.Equal(t, []TaskID{12}, donor.LocalWork)

	thief, ok := fsm.SnapshotPeer("thief")
	require.True(t, ok)
	require.Equal(t, []TaskID{10, 11}, thief.LocalWork)
}

func TestWorkStealingLeavesBusyDonorQueuedWork(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:        "donor",
		Rank:      0,
		State:     PeerBusy,
		LocalWork: []TaskID{10, 11, 12},
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "thief",
		Rank:  10,
		State: PeerReady,
	}))

	result, ok := fsm.StealOne("thief")
	require.True(t, ok)
	require.Equal(t, "thief", result.ThiefID)
	require.Equal(t, "donor", result.DonorID)
	require.Equal(t, []TaskID{10, 11}, result.TaskIDs)
	require.Equal(t, 1, result.DonorRemaining)

	donor, ok := fsm.SnapshotPeer("donor")
	require.True(t, ok)
	require.Equal(t, []TaskID{12}, donor.LocalWork)
}

func TestWorkStealingIgnoresBusySingletonDonor(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:        "busy-singleton",
		Rank:      100,
		State:     PeerBusy,
		LocalWork: []TaskID{10},
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:        "surplus-ready",
		Rank:      0,
		State:     PeerReady,
		LocalWork: []TaskID{20, 21},
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "thief",
		State: PeerReady,
	}))

	result, ok := fsm.StealOne("thief")
	require.True(t, ok)
	require.Equal(t, "surplus-ready", result.DonorID)
	require.Equal(t, []TaskID{20}, result.TaskIDs)

	busy, ok := fsm.SnapshotPeer("busy-singleton")
	require.True(t, ok)
	require.Equal(t, []TaskID{10}, busy.LocalWork)
}

func TestWorkStealingDoesNotStealSingleton(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:        "donor",
		State:     PeerReady,
		LocalWork: []TaskID{10},
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "thief",
		State: PeerReady,
	}))

	_, ok := fsm.StealOne("thief")
	require.False(t, ok)

	donor, ok := fsm.SnapshotPeer("donor")
	require.True(t, ok)
	require.Equal(t, []TaskID{10}, donor.LocalWork)
}

func TestWorkStealingCanDrainBlockedDonor(t *testing.T) {
	fsm := NewSchedulerFSM(DefaultConfig())
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:        "blocked",
		State:     PeerBlocked,
		LocalWork: []TaskID{10},
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "thief",
		State: PeerReady,
	}))

	result, ok := fsm.StealOne("thief")
	require.True(t, ok)
	require.Equal(t, "blocked", result.DonorID)
	require.EqualValues(t, 10, result.TaskID)
	require.Zero(t, result.DonorRemaining)
}

func TestPeerFSMRejectsAssignWhenBlocked(t *testing.T) {
	peer := NewPeerFSM("peer")
	require.NoError(t, peer.Apply(PeerBlockedEvent{}))
	require.Error(t, peer.Apply(PeerAssignedEvent{TaskID: 1}))
}

func TestPeerFSMRejectsAssignWhenQuarantined(t *testing.T) {
	peer := NewPeerFSM("peer")
	require.NoError(t, peer.Apply(PeerQuarantinedEvent{}))
	require.Error(t, peer.Apply(PeerAssignedEvent{TaskID: 1}))
}

func TestQuickQueryTimeoutMatchesRule(t *testing.T) {
	property := func(baseMS uint16, rttMS uint16) bool {
		cfg := DefaultConfig()
		base := time.Duration(baseMS) * time.Millisecond
		rtt := time.Duration(rttMS) * time.Millisecond

		got := QueryTimeoutForPeer(base, PeerSnapshot{RTT: rtt}, cfg)

		want := base
		if want < cfg.MinTimeout {
			want = cfg.MinTimeout
		}
		if rtt > 0 {
			rttTimeout := rtt * time.Duration(cfg.RTTMultiplier)
			if rttTimeout > want {
				want = rttTimeout
			}
		}
		if want > cfg.MaxTimeout {
			want = cfg.MaxTimeout
		}

		return got == want &&
			got >= cfg.MinTimeout &&
			got <= cfg.MaxTimeout
	}

	require.NoError(t, quick.Check(property, &quick.Config{
		MaxCount: 500,
	}))
}

func TestQuickWorkStealingPreservesTaskMultiset(t *testing.T) {
	property := func(rawDonor, rawThief uint8) bool {
		donorCount := int(rawDonor % 8)
		thiefCount := int(rawThief % 4)

		donorWork := make([]TaskID, donorCount)
		for i := range donorWork {
			donorWork[i] = TaskID(i + 1)
		}

		thiefWork := make([]TaskID, thiefCount)
		for i := range thiefWork {
			thiefWork[i] = TaskID(100 + i)
		}

		fsm := NewSchedulerFSM(DefaultConfig())
		if err := fsm.AddPeer(PeerSnapshot{
			ID:        "donor",
			State:     PeerReady,
			LocalWork: donorWork,
		}); err != nil {
			return false
		}
		if err := fsm.AddPeer(PeerSnapshot{
			ID:        "thief",
			State:     PeerReady,
			LocalWork: thiefWork,
		}); err != nil {
			return false
		}

		before := taskCounts(donorWork, thiefWork)
		_, _ = fsm.StealOne("thief")

		donor, ok := fsm.SnapshotPeer("donor")
		if !ok {
			return false
		}
		thief, ok := fsm.SnapshotPeer("thief")
		if !ok {
			return false
		}

		after := taskCounts(donor.LocalWork, thief.LocalWork)
		return reflect.DeepEqual(before, after)
	}

	require.NoError(t, quick.Check(property, &quick.Config{
		MaxCount: 500,
	}))
}

func taskCounts(workSets ...[]TaskID) map[TaskID]int {
	counts := make(map[TaskID]int)
	for _, work := range workSets {
		for _, task := range work {
			counts[task]++
		}
	}

	return counts
}
