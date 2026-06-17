package headersync

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/stretchr/testify/require"
)

func TestCheckpointAnchorsPlanAdjacentRanges(t *testing.T) {
	fsm := newTestFSM(0, 100)
	fsm.SeedCommittedAnchor()
	added := fsm.AddCheckpointAnchors([]chaincfg.Checkpoint{
		{Height: 10, Hash: hashPtr(testHash(110))},
		{Height: 20, Hash: hashPtr(testHash(120))},
	})
	require.Equal(t, 2, added)

	result, err := fsm.PlanConfirmedAnchorRanges(1, 0, 20)
	require.NoError(t, err)
	require.EqualValues(t, 3, result.NextID)
	require.Len(t, result.Ranges, 2)
	require.EqualValues(t, 0, result.Ranges[0].StartHeight)
	require.EqualValues(t, 10, result.Ranges[0].StopHeight)
	require.EqualValues(t, 10, result.Ranges[1].StartHeight)
	require.EqualValues(t, 20, result.Ranges[1].StopHeight)
}

func TestDiscoveredAnchorRequiresConfirmationBeforePlanning(t *testing.T) {
	fsm := newTestFSM(0, 100)
	fsm.SeedCommittedAnchor()
	fsm.AddDiscoveredAnchor(10, testHash(110))

	result, err := fsm.PlanConfirmedAnchorRanges(1, 0, 10)
	require.NoError(t, err)
	require.Empty(t, result.Ranges)

	fsm.AddDiscoveredAnchor(10, testHash(110))
	result, err = fsm.PlanConfirmedAnchorRanges(1, 0, 10)
	require.NoError(t, err)
	require.Len(t, result.Ranges, 1)
	require.EqualValues(t, 0, result.Ranges[0].StartHeight)
	require.EqualValues(t, 10, result.Ranges[0].StopHeight)
}

func TestPlanConfirmedAnchorRangesSkipsExistingSpan(t *testing.T) {
	fsm := newTestFSM(0, 100)
	fsm.SeedCommittedAnchor()
	addTrustedAnchor(t, fsm, 10, 110)
	mustPlanRange(t, fsm, 1, 0, 10)

	result, err := fsm.PlanConfirmedAnchorRanges(2, 0, 10)
	require.NoError(t, err)
	require.Empty(t, result.Ranges)
	require.Equal(t, 1, result.SkippedExisting)
	require.EqualValues(t, 2, result.NextID)
}

func TestPlanConfirmedAnchorRangesSkipsOversizedSpan(t *testing.T) {
	fsm := newTestFSM(0, 100)
	fsm.SeedCommittedAnchor()
	addTrustedAnchor(t, fsm, DefaultMaxRangeHeaders+1, 110)

	result, err := fsm.PlanConfirmedAnchorRanges(
		1, 0, DefaultMaxRangeHeaders+1,
	)
	require.NoError(t, err)
	require.Empty(t, result.Ranges)
	require.EqualValues(t, 1, result.NextID)
}

func hashPtr(hash Hash) *Hash {
	hashCopy := hash

	return &hashCopy
}
