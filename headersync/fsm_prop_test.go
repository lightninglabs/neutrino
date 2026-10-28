package headersync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

const (
	// propPeers is the number of peer IDs the property test draws from.
	propPeers = 4

	// propHeight is the height of the anchor grid the property test plans
	// over. Anchors sit every propSpan heights.
	propHeight = 24

	// propSpan is the distance between adjacent property-test anchors.
	propSpan = 4
)

// propHash is the honest anchor hash at height in the property test.
func propHash(height uint32) Hash {
	return testHash(byte(height))
}

// fsmModel is a rapid state machine over HeaderSyncFSM. It drives the same
// op alphabet the actors use, in any order, and checks the scheduler
// invariants the header-sync DST checks end to end after every op.
type fsmModel struct {
	fsm *HeaderSyncFSM

	// nextID is the next range ID to plan.
	nextID RangeID

	// epochs is the highest lease epoch seen per range.
	epochs map[RangeID]uint64

	// tip is the committed tip seen after the previous op.
	tip uint32

	// rewound is set by an op allowed to move the tip down.
	rewound bool
}

// newFSMModel builds an FSM with trusted anchors on a grid, so ranges can be
// planned anywhere on it.
func newFSMModel(t *rapid.T) *fsmModel {
	cfg := DefaultConfig()
	cfg.AnchorConfirmationsRequired = rapid.IntRange(1, 2).Draw(t,
		"confirmations")
	cfg.MaxPeerOutstandingRanges = rapid.IntRange(1, 4).Draw(t,
		"outstanding")
	fsm := NewHeaderSyncFSM(0, propHash(0), cfg)
	fsm.SeedCommittedAnchor()
	for height := uint32(propSpan); height <= propHeight; height += propSpan {
		fsm.AddOrConfirmAnchor(height, propHash(height), true)
	}

	return &fsmModel{
		fsm:    fsm,
		nextID: 1,
		epochs: make(map[RangeID]uint64),
	}
}

// peerID draws one of the property-test peer IDs.
func (m *fsmModel) peerID(t *rapid.T) string {
	return peerID(rapid.IntRange(1, propPeers).Draw(t, "peer"))
}

// rangeID draws a planned range ID, or NoRange when none exist.
func (m *fsmModel) rangeID(t *rapid.T) RangeID {
	if m.nextID == 1 {
		return NoRange
	}
	return RangeID(rapid.IntRange(1, int(m.nextID-1)).Draw(t, "range"))
}

// AddPeer registers or refreshes a peer.
func (m *fsmModel) AddPeer(t *rapid.T) {
	_ = m.fsm.AddPeer(PeerSnapshot{
		ID:   m.peerID(t),
		Rank: rapid.IntRange(-2, 2).Draw(t, "rank"),
	})
}

// RemovePeer disconnects a peer.
func (m *fsmModel) RemovePeer(t *rapid.T) {
	m.fsm.RemovePeer(m.peerID(t))
}

// UpdatePeerState moves a peer between scheduling states.
func (m *fsmModel) UpdatePeerState(t *rapid.T) {
	state := PeerState(rapid.IntRange(0, int(PeerDown)).Draw(t, "state"))
	_ = m.fsm.UpdatePeerState(m.peerID(t), state)
}

// UpdatePeerRTT changes peer ranking.
func (m *fsmModel) UpdatePeerRTT(t *rapid.T) {
	rtt := time.Duration(rapid.IntRange(0, 5).Draw(t, "rtt")) *
		100 * time.Millisecond
	_ = m.fsm.UpdatePeerRTT(m.peerID(t), rtt)
}

// PlanRange plans one grid span above the tip.
func (m *fsmModel) PlanRange(t *rapid.T) {
	start := uint32(rapid.IntRange(0, propHeight/propSpan-1).Draw(t,
		"span")) * propSpan
	_, ok, err := m.fsm.PlanRange(m.nextID, start, start+propSpan)
	if err == nil && ok {
		m.nextID++
	}
}

// PlanAll plans every confirmed span above the tip at once.
func (m *fsmModel) PlanAll(t *rapid.T) {
	if m.fsm.CommittedTip() >= propHeight {
		return
	}
	result, err := m.fsm.PlanConfirmedAnchorRanges(
		m.nextID, m.fsm.CommittedTip(), propHeight,
	)
	require.NoError(t, err)
	m.nextID = result.NextID
}

// Assign hands the next queued range to an owner.
func (m *fsmModel) Assign(t *rapid.T) {
	m.fsm.AssignNextQueuedRange()
}

// Start starts a peer's next owned range.
func (m *fsmModel) Start(t *rapid.T) {
	_, _, _ = m.fsm.StartPeerRange(m.peerID(t))
}

// Steal lets a peer steal work.
func (m *fsmModel) Steal(t *rapid.T) {
	if rapid.Bool().Draw(t, "explicit") {
		_, _, _ = m.fsm.StealRange(m.peerID(t), m.rangeID(t))
		return
	}
	_, _, _ = m.fsm.StealNextRange(m.peerID(t))
}

// Complete reports a completion, possibly with a stale epoch.
func (m *fsmModel) Complete(t *rapid.T) {
	id := m.rangeID(t)
	rng, ok := m.fsm.RangeByID(id)
	if !ok {
		return
	}
	epoch := rng.LeaseEpoch
	if rapid.Bool().Draw(t, "stale") && epoch > 0 {
		epoch--
	}
	peer := rng.OwnerPeer
	if rapid.Bool().Draw(t, "other_peer") || peer == NoPeer {
		peer = m.peerID(t)
	}
	_, _ = m.fsm.CompleteRangeAt(
		peer, id, epoch, rapid.Bool().Draw(t, "valid"), time.Unix(0, 0),
	)
}

// Commit drains ready ranges.
func (m *fsmModel) Commit(t *rapid.T) {
	ready := m.fsm.ReadyRanges()
	result := m.fsm.CommitReadyRanges()
	require.Equal(t, ready, result.Committed,
		"commit drained a different set than ReadyRanges reported")
	for _, id := range result.Committed {
		rng, ok := m.fsm.RangeByID(id)
		require.True(t, ok)
		require.Equal(t, RangeCommitted, rng.State)
		require.LessOrEqual(t, rng.StopHeight, m.fsm.CommittedTip())
	}
}

// Requeue returns a staged range to the queue, as after an ask failure.
func (m *fsmModel) Requeue(t *rapid.T) {
	m.fsm.RequeueStagedRange(m.rangeID(t))
}

// Invalidate rejects a staged range.
func (m *fsmModel) Invalidate(t *rapid.T) {
	_, _ = m.fsm.InvalidateRange(m.rangeID(t))
}

// AdvanceTip moves the durable tip as the serial writer would: forward on
// the honest grid, or back to a lower grid point.
func (m *fsmModel) AdvanceTip(t *rapid.T) {
	height := uint32(rapid.IntRange(0, propHeight/propSpan).Draw(t,
		"tip")) * propSpan
	if height < m.fsm.CommittedTip() {
		m.rewound = true
	}
	m.fsm.AdvanceCommittedTip(height, propHash(height))
}

// Check asserts the scheduler invariants after every op.
func (m *fsmModel) Check(t *rapid.T) {
	tip := m.fsm.CommittedTip()
	if !m.rewound {
		require.GreaterOrEqual(t, tip, m.tip, "committed tip went back")
	}
	m.tip, m.rewound = tip, false
	require.Equal(t, propHash(tip), m.fsm.CommittedHash())

	// Every lease has one owner, and the peer table agrees with it.
	owners := make(map[RangeID]string)
	for _, peer := range m.fsm.Peers() {
		ids := append([]RangeID(nil), peer.LocalRanges...)
		if peer.ActiveRange != NoRange {
			ids = append(ids, peer.ActiveRange)
		}
		for _, id := range ids {
			owner, dup := owners[id]
			require.False(t, dup, "range %d owned by %s and %s",
				id, owner, peer.ID)
			owners[id] = peer.ID
		}
		if peer.ActiveRange != NoRange {
			rng, ok := m.fsm.RangeByID(peer.ActiveRange)
			require.True(t, ok)
			require.Equal(t, RangeActive, rng.State)
			require.Equal(t, peer.ID, rng.OwnerPeer)
		}
	}

	// Epochs never go back, and live work never overlaps.
	var live []HeaderRange
	for _, rng := range m.fsm.Ranges() {
		require.GreaterOrEqual(t, rng.LeaseEpoch, m.epochs[rng.ID])
		m.epochs[rng.ID] = rng.LeaseEpoch

		switch rng.State {
		case RangeQueued, RangeOwned, RangeActive, RangeStaged:
			require.GreaterOrEqual(t, rng.StartHeight, tip,
				"live range %+v below the tip %d", rng, tip)
			live = append(live, rng)
		}
		if rng.State == RangeStaged {
			require.True(t, rng.Valid)
		}
	}
	for i := range live {
		for j := i + 1; j < len(live); j++ {
			a, b := live[i], live[j]
			require.False(t, a.StartHeight < b.StopHeight &&
				b.StartHeight < a.StopHeight,
				"live ranges overlap: %+v %+v", a, b)
		}
	}

	// Ready ranges chain upward from the tip.
	prev := tip
	for _, id := range m.fsm.ReadyRanges() {
		rng, ok := m.fsm.RangeByID(id)
		require.True(t, ok)
		require.Equal(t, prev, rng.StartHeight)
		prev = rng.StopHeight
	}

	require.True(t, commitLogStrictlyIncreasing(m.fsm.CommitLog()))
}

// TestHeaderSyncFSMProperties runs random op sequences against the FSM. Its
// invariants mirror the header-sync DST oracle at the pure FSM level, where
// thousands of sequences run per second.
func TestHeaderSyncFSMProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		model := newFSMModel(t)
		t.Repeat(rapid.StateMachineActions(model))
	})
}
