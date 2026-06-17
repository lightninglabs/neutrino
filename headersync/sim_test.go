package headersync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type simPeer struct {
	id      string
	stall   bool
	active  RangeAssignment
	started int
}

type headerSyncSim struct {
	fsm     *HeaderSyncFSM
	peers   map[string]*simPeer
	tick    int
	timeout int
	latency int
	steals  int
}

func newHeaderSyncSim(t *testing.T) *headerSyncSim {
	t.Helper()

	fsm := newTestFSM(0, 100)
	for i := uint32(0); i <= 4; i++ {
		addTrustedAnchorNoRequire(fsm, i*10, 100+i)
	}

	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "stalled",
		Rank:  0,
		State: PeerReady,
	}))
	require.NoError(t, fsm.AddPeer(PeerSnapshot{
		ID:    "fast",
		Rank:  1,
		State: PeerReady,
	}))

	for i := 1; i <= 4; i++ {
		_, ok, err := fsm.PlanRange(
			RangeID(i), uint32((i-1)*10), uint32(i*10),
		)
		require.NoError(t, err)
		require.True(t, ok)
	}

	return &headerSyncSim{
		fsm: fsm,
		peers: map[string]*simPeer{
			"stalled": {id: "stalled", stall: true},
			"fast":    {id: "fast"},
		},
		timeout: 2,
		latency: 1,
	}
}

func (s *headerSyncSim) step(t *testing.T) {
	t.Helper()

	for {
		if _, ok := s.fsm.AssignNextQueuedRange(); !ok {
			break
		}
	}

	for _, peer := range s.peers {
		if peer.active.RangeID != NoRange {
			continue
		}

		assignment, ok, err := s.fsm.StartPeerRange(peer.id)
		require.NoError(t, err)
		if !ok {
			continue
		}

		peer.active = assignment
		peer.started = s.tick
	}

	for _, peer := range s.peers {
		if peer.active.RangeID == NoRange || peer.stall {
			continue
		}
		if s.tick-peer.started < s.latency {
			continue
		}

		_, err := s.fsm.CompleteRangeAt(
			peer.id, peer.active.RangeID, peer.active.LeaseEpoch,
			true, time.Unix(int64(s.tick), 0),
		)
		require.NoError(t, err)
		peer.active = RangeAssignment{}
	}

	for _, peer := range s.peers {
		if peer.active.RangeID == NoRange || !peer.stall {
			continue
		}
		if s.tick-peer.started < s.timeout {
			continue
		}

		require.NoError(t, s.fsm.UpdatePeerState(peer.id, PeerBlocked))
		result, ok, err := s.fsm.StealRange("fast", peer.active.RangeID)
		require.NoError(t, err)
		if ok {
			require.Equal(t, "stalled", result.DonorID)
			peer.active = RangeAssignment{}
			s.steals++
		}
	}

	if s.peers["fast"].active.RangeID == NoRange {
		_, ok, err := s.fsm.StealNextRange("fast")
		require.NoError(t, err)
		if ok {
			s.steals++
		}
	}

	s.fsm.CommitReadyRanges()
	s.tick++
}

func TestSimulationStalledPeerRangeIsStolenAndCommitted(t *testing.T) {
	sim := newHeaderSyncSim(t)

	for i := 0; i < 20 && sim.fsm.CommittedTip() < 40; i++ {
		sim.step(t)
	}

	require.EqualValues(t, 40, sim.fsm.CommittedTip())
	require.GreaterOrEqual(t, sim.steals, 1)
	require.Equal(t, []RangeID{1, 2, 3, 4}, sim.fsm.CommitLog())
	require.EqualValues(t, sim.steals, sim.fsm.Metrics().RangesStolen)
}
