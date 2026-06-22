package headersync

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPlanRangeRequiresConfirmedAnchors(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	fsm.AddOrConfirmAnchor(100, testHash(200), false)

	_, ok, err := fsm.PlanRange(1, 0, 100)
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, fsm.Ranges())

	fsm.AddOrConfirmAnchor(100, testHash(200), false)
	rng, ok, err := fsm.PlanRange(1, 0, 100)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 1, rng.ID)
	require.Equal(t, RangeQueued, rng.State)
}

func TestOutOfOrderCompletionStagesUntilGapFilled(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	addTrustedAnchor(t, fsm, 20, 120)
	addPeer(t, fsm, "peer1")
	addPeer(t, fsm, "peer2")
	mustPlanRange(t, fsm, 1, 0, 10)
	mustPlanRange(t, fsm, 2, 10, 20)
	mustAssignRange(t, fsm)
	mustAssignRange(t, fsm)
	first := mustStartRange(t, fsm, "peer1")
	second := mustStartRange(t, fsm, "peer2")

	now := time.Unix(100, 0)
	result, err := fsm.CompleteRangeAt(
		"peer2", second.RangeID, second.LeaseEpoch, true, now,
	)
	require.NoError(t, err)
	require.True(t, result.Accepted)
	commit := fsm.CommitReadyRanges()
	require.Empty(t, commit.Committed)
	require.EqualValues(t, 0, fsm.CommittedTip())

	stats := fsm.CommitStats(now.Add(5 * time.Second))
	require.Equal(t, 1, stats.StagedCount)
	require.EqualValues(t, 20, stats.CommitLag)
	require.Equal(t, 5*time.Second, stats.OldestStagedAge)

	_, err = fsm.CompleteRangeAt(
		"peer1", first.RangeID, first.LeaseEpoch, true,
		now.Add(time.Second),
	)
	require.NoError(t, err)
	commit = fsm.CommitReadyRanges()
	require.Equal(t, []RangeID{1, 2}, commit.Committed)
	require.EqualValues(t, 20, fsm.CommittedTip())
	require.Equal(t, []RangeID{1, 2}, fsm.CommitLog())
}

func TestReadyRangesDoesNotAdvanceCommittedTip(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	addPeer(t, fsm, "peer")
	mustPlanRange(t, fsm, 1, 0, 10)
	mustAssignRange(t, fsm)
	started := mustStartRange(t, fsm, "peer")

	_, err := fsm.CompleteRange(
		"peer", started.RangeID, started.LeaseEpoch, true,
	)
	require.NoError(t, err)

	require.Equal(t, []RangeID{1}, fsm.ReadyRanges())
	require.EqualValues(t, 0, fsm.CommittedTip())
	require.Empty(t, fsm.CommitLog())

	commit := fsm.CommitReadyRanges()
	require.Equal(t, []RangeID{1}, commit.Committed)
	require.EqualValues(t, 10, fsm.CommittedTip())
}

func TestAdvanceCommittedTipReconcilesExternalProgress(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	addTrustedAnchor(t, fsm, 20, 120)
	addTrustedAnchor(t, fsm, 30, 130)
	addPeer(t, fsm, "peer")
	mustPlanRange(t, fsm, 1, 0, 10)
	mustPlanRange(t, fsm, 2, 10, 20)
	mustPlanRange(t, fsm, 3, 20, 30)
	mustAssignRange(t, fsm)
	started := mustStartRange(t, fsm, "peer")
	require.EqualValues(t, 1, started.RangeID)

	advanced := fsm.AdvanceCommittedTip(15, testHash(115))
	require.True(t, advanced)
	require.EqualValues(t, 15, fsm.CommittedTip())
	require.Equal(t, testHash(115), fsm.CommittedHash())
	require.False(t, fsm.rangeOverlaps(15, 20))

	first, ok := fsm.ranges[1]
	require.True(t, ok)
	require.Equal(t, RangeCommitted, first.State)
	second, ok := fsm.ranges[2]
	require.True(t, ok)
	require.Equal(t, RangeStale, second.State)
}

func TestStageDiscoveredRangeUsesConfirmedAnchors(t *testing.T) {
	fsm := newTestFSM(0, 100)
	fsm.SeedCommittedAnchor()
	addTrustedAnchor(t, fsm, 10, 110)

	rng, ok, err := fsm.StageDiscoveredRange(
		1, "peer-a", 0, 10, time.Unix(1, 0),
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, RangeStaged, rng.State)
	require.True(t, rng.Valid)
	require.Equal(t, "peer-a", rng.OwnerPeer)
	require.EqualValues(t, 1, rng.LeaseEpoch)
	require.EqualValues(t, 1, fsm.Metrics().RangesStaged)

	again, ok, err := fsm.StageDiscoveredRange(
		2, "peer-b", 0, 10, time.Unix(2, 0),
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.EqualValues(t, 1, again.ID)
	require.EqualValues(t, 1, fsm.Metrics().RangesStaged)
}

func TestStageDiscoveredRangeStagesExistingQueuedRange(t *testing.T) {
	fsm := newTestFSM(0, 100)
	fsm.SeedCommittedAnchor()
	addTrustedAnchor(t, fsm, 10, 110)

	planned := mustPlanRange(t, fsm, 7, 0, 10)
	require.Equal(t, RangeQueued, planned.State)

	rng, ok, err := fsm.StageDiscoveredRange(
		99, "peer-a", 0, 10, time.Unix(1, 0),
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 7, rng.ID)
	require.Equal(t, RangeStaged, rng.State)
	require.True(t, rng.Valid)
	require.Equal(t, "peer-a", rng.OwnerPeer)
	require.EqualValues(t, 1, rng.LeaseEpoch)
	require.EqualValues(t, 1, fsm.Metrics().RangesPlanned)
	require.EqualValues(t, 1, fsm.Metrics().RangesStaged)
}

func TestStageDiscoveredRangeDetachesExistingActiveRange(t *testing.T) {
	fsm := newTestFSM(0, 100)
	fsm.SeedCommittedAnchor()
	addTrustedAnchor(t, fsm, 10, 110)
	addPeer(t, fsm, "owner")

	mustPlanRange(t, fsm, 1, 0, 10)
	mustAssignRange(t, fsm)
	started := mustStartRange(t, fsm, "owner")

	rng, ok, err := fsm.StageDiscoveredRange(
		2, "discoverer", 0, 10, time.Unix(1, 0),
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 1, rng.ID)
	require.Equal(t, RangeStaged, rng.State)
	require.Equal(t, "discoverer", rng.OwnerPeer)
	require.EqualValues(t, started.LeaseEpoch+1, rng.LeaseEpoch)

	owner, ok := fsm.SnapshotPeer("owner")
	require.True(t, ok)
	require.Equal(t, PeerReady, owner.State)
	require.Equal(t, NoRange, owner.ActiveRange)
	require.Empty(t, owner.LocalRanges)

	result, err := fsm.CompleteRangeAt(
		"owner", started.RangeID, started.LeaseEpoch, true,
		time.Unix(2, 0),
	)
	require.NoError(t, err)
	require.True(t, result.Stale)
	require.False(t, result.Accepted)
	require.EqualValues(t, 1, fsm.Metrics().StaleCompletions)
	require.EqualValues(t, 1, fsm.Metrics().RangesStaged)
}

func TestStaleCompletionCannotOverwriteStolenLease(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	addPeer(t, fsm, "peer1")
	addPeer(t, fsm, "peer2")
	mustPlanRange(t, fsm, 1, 0, 10)
	mustAssignRange(t, fsm)
	started := mustStartRange(t, fsm, "peer1")

	steal, ok, err := fsm.StealRange("peer2", 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, StealReasonExplicit, steal.Reason)
	require.EqualValues(t, started.LeaseEpoch, steal.OldLeaseEpoch)
	require.EqualValues(t, started.LeaseEpoch+1, steal.LeaseEpoch)

	result, err := fsm.CompleteRange(
		"peer1", 1, started.LeaseEpoch, false,
	)
	require.NoError(t, err)
	require.True(t, result.Stale)
	require.False(t, result.Accepted)

	rng, ok := fsm.RangeByID(1)
	require.True(t, ok)
	require.Equal(t, "peer2", rng.OwnerPeer)
	require.Equal(t, RangeOwned, rng.State)
	require.EqualValues(t, 1, fsm.Metrics().StaleCompletions)

	current := mustStartRange(t, fsm, "peer2")
	_, err = fsm.CompleteRange(
		"peer2", current.RangeID, current.LeaseEpoch, true,
	)
	require.NoError(t, err)
	commit := fsm.CommitReadyRanges()
	require.Equal(t, []RangeID{1}, commit.Committed)
	require.EqualValues(t, 10, fsm.CommittedTip())
}

func TestInvalidCompletionDoesNotCommit(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	addPeer(t, fsm, "peer")
	mustPlanRange(t, fsm, 1, 0, 10)
	mustAssignRange(t, fsm)
	started := mustStartRange(t, fsm, "peer")

	result, err := fsm.CompleteRange(
		"peer", started.RangeID, started.LeaseEpoch, false,
	)
	require.NoError(t, err)
	require.True(t, result.Accepted)
	require.False(t, result.Valid)
	require.Equal(t, RangeFailed, result.RangeState)

	commit := fsm.CommitReadyRanges()
	require.Empty(t, commit.Committed)
	require.EqualValues(t, 0, fsm.CommittedTip())
	require.EqualValues(t, 1, fsm.Metrics().RangesFailed)
}

func TestActiveRangeCanBeStolen(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	addPeer(t, fsm, "peer1")
	addPeer(t, fsm, "peer2")
	mustPlanRange(t, fsm, 1, 0, 10)
	mustAssignRange(t, fsm)
	started := mustStartRange(t, fsm, "peer1")

	steal, ok, err := fsm.StealRange("peer2", 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "peer1", steal.DonorID)
	require.Equal(t, "peer2", steal.ThiefID)
	require.Equal(t, StealReasonExplicit, steal.Reason)
	require.EqualValues(t, started.LeaseEpoch, steal.OldLeaseEpoch)
	require.EqualValues(t, started.LeaseEpoch+1, steal.LeaseEpoch)

	donor, ok := fsm.SnapshotPeer("peer1")
	require.True(t, ok)
	require.Equal(t, PeerReady, donor.State)
	require.Equal(t, NoRange, donor.ActiveRange)
	require.Empty(t, donor.LocalRanges)

	thief, ok := fsm.SnapshotPeer("peer2")
	require.True(t, ok)
	require.Equal(t, []RangeID{1}, thief.LocalRanges)
	require.EqualValues(t, 1, fsm.Metrics().RangesStolen)
}

func TestStealNextRangePrefersBlockedDonor(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	addTrustedAnchor(t, fsm, 20, 120)
	addTrustedAnchor(t, fsm, 30, 130)
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:          "ready-donor",
		Rank:        100,
		State:       PeerReady,
		LocalRanges: []RangeID{1},
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:          "blocked-donor",
		Rank:        0,
		State:       PeerBlocked,
		LocalRanges: []RangeID{2},
	}))
	addPeer(t, fsm, "thief")
	mustPlanRange(t, fsm, 1, 0, 10)
	mustPlanRange(t, fsm, 2, 10, 20)

	rng1, ok := fsm.ranges[1]
	require.True(t, ok)
	rng1.OwnerPeer = "ready-donor"
	rng1.LeaseEpoch = 1
	rng1.State = RangeOwned
	fsm.ranges[1] = rng1

	rng2, ok := fsm.ranges[2]
	require.True(t, ok)
	rng2.OwnerPeer = "blocked-donor"
	rng2.LeaseEpoch = 1
	rng2.State = RangeOwned
	fsm.ranges[2] = rng2

	steal, ok, err := fsm.StealNextRange("thief")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "blocked-donor", steal.DonorID)
	require.EqualValues(t, 2, steal.RangeID)
	require.Equal(t, StealReasonQueuedBlocked, steal.Reason)
}

func TestStealNextRangeLeavesReadySingletonDonor(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:          "donor",
		State:       PeerReady,
		LocalRanges: []RangeID{1},
	}))
	addPeer(t, fsm, "thief")
	mustPlanRange(t, fsm, 1, 0, 10)

	rng, ok := fsm.ranges[1]
	require.True(t, ok)
	rng.OwnerPeer = "donor"
	rng.LeaseEpoch = 1
	rng.State = RangeOwned
	fsm.ranges[1] = rng

	_, ok, err := fsm.StealNextRange("thief")
	require.NoError(t, err)
	require.False(t, ok)

	donor, ok := fsm.SnapshotPeer("donor")
	require.True(t, ok)
	require.Equal(t, []RangeID{1}, donor.LocalRanges)
}

func TestAssignRangeBalancesLocalQueues(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	addTrustedAnchor(t, fsm, 20, 120)
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:          "loaded",
		State:       PeerReady,
		LocalRanges: []RangeID{99, 100},
	}))
	addPeer(t, fsm, "empty")
	mustPlanRange(t, fsm, 1, 0, 10)
	mustPlanRange(t, fsm, 2, 10, 20)

	assignment := mustAssignRange(t, fsm)
	require.Equal(t, "empty", assignment.PeerID)
}

func TestAssignRangePrefersMeasuredRTTOverUnknownRTT(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "unknown-rtt",
		Rank:  0,
		State: PeerReady,
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "measured-rtt",
		Rank:  0,
		RTT:   50 * time.Millisecond,
		State: PeerReady,
	}))
	mustPlanRange(t, fsm, 1, 0, 10)

	assignment := mustAssignRange(t, fsm)
	require.Equal(t, "measured-rtt", assignment.PeerID)
}

func TestRecoveredPeerCanOwnRangeAgain(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	addPeer(t, fsm, "peer")
	mustPlanRange(t, fsm, 1, 0, 10)

	require.NoError(t, fsm.UpdatePeerState("peer", PeerBlocked))
	_, ok := fsm.AssignNextQueuedRange()
	require.False(t, ok)

	require.NoError(t, fsm.UpdatePeerState("peer", PeerReady))
	assignment := mustAssignRange(t, fsm)
	require.Equal(t, "peer", assignment.PeerID)
}

func TestRemovePeerRequeuesOwnedAndActiveRanges(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	addTrustedAnchor(t, fsm, 20, 120)
	addPeer(t, fsm, "donor")
	addPeer(t, fsm, "thief")
	mustPlanRange(t, fsm, 1, 0, 10)
	mustPlanRange(t, fsm, 2, 10, 20)

	owned, ok, err := fsm.AssignQueuedRangeToPeer(1, "donor")
	require.NoError(t, err)
	require.True(t, ok)

	active := mustStartRange(t, fsm, "donor")
	require.Equal(t, owned.RangeID, active.RangeID)

	_, ok, err = fsm.AssignQueuedRangeToPeer(2, "donor")
	require.NoError(t, err)
	require.True(t, ok)

	fsm.RemovePeer("donor")

	for _, rangeID := range []RangeID{1, 2} {
		rng, ok := fsm.RangeByID(rangeID)
		require.True(t, ok)
		require.Equal(t, RangeQueued, rng.State)
		require.Equal(t, NoPeer, rng.OwnerPeer)
		require.Equal(t, uint64(2), rng.LeaseEpoch)
	}

	assignment := mustAssignRange(t, fsm)
	require.Equal(t, "thief", assignment.PeerID)
	require.Equal(t, RangeID(1), assignment.RangeID)
}

func TestPlanRangeRejectsOverlappingWork(t *testing.T) {
	fsm := newTestFSM(0, 100)
	addTrustedAnchor(t, fsm, 0, 100)
	addTrustedAnchor(t, fsm, 10, 110)
	addTrustedAnchor(t, fsm, 20, 120)
	mustPlanRange(t, fsm, 1, 0, 20)

	_, ok, err := fsm.PlanRange(2, 0, 10)
	require.ErrorIs(t, err, ErrInvalidRange)
	require.False(t, ok)
}

func TestQuickOutOfOrderCommitPreservesContiguousTip(t *testing.T) {
	property := func(raw []uint8) bool {
		const numRanges = 5

		fsm := newTestFSM(0, 100)
		for i := uint32(0); i <= numRanges; i++ {
			addTrustedAnchorNoRequire(fsm, i*10, 100+i)
		}
		for i := 1; i <= numRanges; i++ {
			addPeerNoRequire(fsm, peerID(i))
			if _, ok, err := fsm.PlanRange(
				RangeID(i), uint32((i-1)*10), uint32(i*10),
			); err != nil || !ok {
				return false
			}
		}
		for i := 0; i < numRanges; i++ {
			if _, ok := fsm.AssignNextQueuedRange(); !ok {
				return false
			}
		}

		started := make(map[RangeID]RangeAssignment, numRanges)
		for i := 1; i <= numRanges; i++ {
			assignment, ok, err := fsm.StartPeerRange(peerID(i))
			if err != nil || !ok {
				return false
			}
			started[RangeID(i)] = assignment
		}

		order := permutedRangeOrder(numRanges, raw)
		completed := make(map[RangeID]bool, numRanges)
		for _, rangeID := range order {
			assignment := started[rangeID]
			if _, err := fsm.CompleteRangeAt(
				assignment.PeerID, rangeID, assignment.LeaseEpoch,
				true, time.Unix(int64(rangeID), 0),
			); err != nil {
				return false
			}
			completed[rangeID] = true

			fsm.CommitReadyRanges()
			expectedTip := uint32(0)
			for i := 1; i <= numRanges; i++ {
				if !completed[RangeID(i)] {
					break
				}
				expectedTip = uint32(i * 10)
			}

			if fsm.CommittedTip() != expectedTip {
				return false
			}
			if !commitLogStrictlyIncreasing(fsm.CommitLog()) {
				return false
			}
		}

		return reflect.DeepEqual(
			fsm.CommitLog(), []RangeID{1, 2, 3, 4, 5},
		)
	}

	require.NoError(t, quick.Check(property, &quick.Config{MaxCount: 100}))
}

func newTestFSM(tip uint32, hashByte byte) *HeaderSyncFSM {
	return NewHeaderSyncFSM(tip, testHash(hashByte), DefaultConfig())
}

func addTrustedAnchor(t *testing.T, fsm *HeaderSyncFSM, height uint32,
	hashByte byte) {

	t.Helper()
	anchor, ok := fsm.AddOrConfirmAnchor(height, testHash(hashByte), true)
	require.True(t, ok)
	require.True(t, fsm.AnchorConfirmed(anchor))
}

func addTrustedAnchorNoRequire(fsm *HeaderSyncFSM, height uint32,
	hashByte uint32) {

	fsm.AddOrConfirmAnchor(height, testHash(byte(hashByte)), true)
}

func addPeer(t *testing.T, fsm *HeaderSyncFSM, id string) {
	t.Helper()
	addPeerNoRequire(fsm, id)
}

func addPeerNoRequire(fsm *HeaderSyncFSM, id string) {
	_ = fsm.AddPeer(PeerSnapshot{
		ID:    id,
		State: PeerReady,
	})
}

func mustPlanRange(t *testing.T, fsm *HeaderSyncFSM, id RangeID,
	startHeight, stopHeight uint32) HeaderRange {

	t.Helper()
	rng, ok, err := fsm.PlanRange(id, startHeight, stopHeight)
	require.NoError(t, err)
	require.True(t, ok)

	return rng
}

func mustAssignRange(t *testing.T, fsm *HeaderSyncFSM) RangeAssignment {
	t.Helper()
	assignment, ok := fsm.AssignNextQueuedRange()
	require.True(t, ok)

	return assignment
}

func mustStartRange(t *testing.T, fsm *HeaderSyncFSM,
	peerID string) RangeAssignment {

	t.Helper()
	assignment, ok, err := fsm.StartPeerRange(peerID)
	require.NoError(t, err)
	require.True(t, ok)

	return assignment
}

func testHash(value byte) Hash {
	var hash Hash
	hash[0] = value

	return hash
}

func peerID(index int) string {
	return string(rune('a' + index - 1))
}

func permutedRangeOrder(numRanges int, raw []uint8) []RangeID {
	order := make([]RangeID, numRanges)
	for i := range order {
		order[i] = RangeID(i + 1)
	}

	if len(raw) == 0 {
		return order
	}

	for i, value := range raw {
		left := i % len(order)
		right := int(value) % len(order)
		order[left], order[right] = order[right], order[left]
	}

	return order
}

func commitLogStrictlyIncreasing(log []RangeID) bool {
	for i := 1; i < len(log); i++ {
		if log[i] <= log[i-1] {
			return false
		}
	}

	return true
}
