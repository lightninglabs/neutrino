package querydispatch

import (
	"context"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightninglabs/neutrino/headersync"
	"github.com/lightninglabs/neutrino/querysync"
	"github.com/stretchr/testify/require"
)

func TestHeadersyncAssignsThroughQuerysync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	headerManager, queryManager := newManagers(t, ctx)
	addAnchor(t, ctx, headerManager, 0, 100)
	addAnchor(t, ctx, headerManager, 10, 110)
	addAnchor(t, ctx, headerManager, 20, 120)

	require.NoError(t, headerManager.AddPeer(ctx, headersync.PeerSnapshot{
		ID:          "loaded",
		Rank:        0,
		State:       headersync.PeerReady,
		LocalRanges: []headersync.RangeID{99, 100},
	}))
	require.NoError(t, headerManager.AddPeer(ctx, headersync.PeerSnapshot{
		ID:    "empty",
		Rank:  10,
		State: headersync.PeerReady,
	}))
	planRange(t, ctx, headerManager, 1, 0, 10)
	planRange(t, ctx, headerManager, 2, 10, 20)

	assignment, ok, err := headerManager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "empty", assignment.PeerID)

	headerPeer, ok, err := headerManager.SnapshotPeer(ctx, "empty")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []headersync.RangeID{1}, headerPeer.LocalRanges)

	queryPeer, ok, err := queryManager.SnapshotPeer(ctx, "empty")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []querysync.TaskID{1}, queryPeer.LocalWork)
}

func TestHeadersyncStealsThroughQuerysync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	headerManager, queryManager := newManagers(t, ctx)
	addAnchor(t, ctx, headerManager, 0, 100)
	addAnchor(t, ctx, headerManager, 10, 110)
	addAnchor(t, ctx, headerManager, 20, 120)
	addAnchor(t, ctx, headerManager, 30, 130)

	require.NoError(t, headerManager.AddPeer(ctx, headersync.PeerSnapshot{
		ID:    "donor",
		Rank:  0,
		State: headersync.PeerReady,
	}))
	planRange(t, ctx, headerManager, 1, 0, 10)
	planRange(t, ctx, headerManager, 2, 10, 20)
	planRange(t, ctx, headerManager, 3, 20, 30)

	for i := 0; i < 3; i++ {
		_, ok, err := headerManager.AssignNextQueuedRange(ctx)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.NoError(t, headerManager.AddPeer(ctx, headersync.PeerSnapshot{
		ID:    "thief",
		Rank:  10,
		State: headersync.PeerReady,
	}))

	result, ok, err := headerManager.StealNextRange(ctx, "thief")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "donor", result.DonorID)
	require.Equal(t, "thief", result.ThiefID)
	require.EqualValues(t, 1, result.RangeID)
	require.Equal(t, headersync.StealReasonDispatch, result.Reason)

	headerThief, ok, err := headerManager.SnapshotPeer(ctx, "thief")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []headersync.RangeID{1, 2}, headerThief.LocalRanges)

	queryThief, ok, err := queryManager.SnapshotPeer(ctx, "thief")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []querysync.TaskID{1, 2}, queryThief.LocalWork)
}

func TestHeadersyncActiveStealFallsBackToFSM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	headerManager, queryManager := newManagers(t, ctx)
	addAnchor(t, ctx, headerManager, 0, 100)
	addAnchor(t, ctx, headerManager, 10, 110)

	require.NoError(t, headerManager.AddPeer(ctx, headersync.PeerSnapshot{
		ID:    "donor",
		Rank:  0,
		State: headersync.PeerReady,
	}))
	require.NoError(t, headerManager.AddPeer(ctx, headersync.PeerSnapshot{
		ID:    "thief",
		Rank:  10,
		State: headersync.PeerReady,
	}))
	planRange(t, ctx, headerManager, 1, 0, 10)

	_, ok, err := headerManager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	started, ok, err := headerManager.StartPeerRange(ctx, "donor")
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 1, started.RangeID)

	require.NoError(t, headerManager.UpdatePeerState(
		ctx, "donor", headersync.PeerQuarantined,
	))

	result, ok, err := headerManager.StealNextRange(ctx, "thief")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "donor", result.DonorID)
	require.Equal(t, "thief", result.ThiefID)
	require.EqualValues(t, 1, result.RangeID)
	require.EqualValues(t, started.LeaseEpoch, result.OldLeaseEpoch)
	require.EqualValues(t, started.LeaseEpoch+1, result.LeaseEpoch)
	require.Equal(t, headersync.StealReasonActiveQuarantined, result.Reason)

	headerThief, ok, err := headerManager.SnapshotPeer(ctx, "thief")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []headersync.RangeID{1}, headerThief.LocalRanges)

	queryThief, ok, err := queryManager.SnapshotPeer(ctx, "thief")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []querysync.TaskID{1}, queryThief.LocalWork)
}

func TestHeadersyncStartAndCompleteMirrorQuerysyncState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	headerManager, queryManager := newManagers(t, ctx)
	addAnchor(t, ctx, headerManager, 0, 100)
	addAnchor(t, ctx, headerManager, 10, 110)
	require.NoError(t, headerManager.AddPeer(ctx, headersync.PeerSnapshot{
		ID:    "peer",
		State: headersync.PeerReady,
	}))
	planRange(t, ctx, headerManager, 1, 0, 10)
	_, ok, err := headerManager.AssignNextQueuedRange(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	assignment, ok, err := headerManager.StartPeerRange(ctx, "peer")
	require.NoError(t, err)
	require.True(t, ok)

	queryPeer, ok, err := queryManager.SnapshotPeer(ctx, "peer")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, querysync.PeerBusy, queryPeer.State)
	require.Empty(t, queryPeer.LocalWork)

	_, err = headerManager.CompleteRange(
		ctx, "peer", assignment.RangeID, assignment.LeaseEpoch, true,
		time.Unix(100, 0),
	)
	require.NoError(t, err)

	queryPeer, ok, err = queryManager.SnapshotPeer(ctx, "peer")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, querysync.PeerReady, queryPeer.State)
	require.Empty(t, queryPeer.LocalWork)
}

func newManagers(t *testing.T, ctx context.Context) (
	*headersync.ManagerActor, *querysync.ManagerActor) {

	t.Helper()

	queryManager := querysync.NewManagerActor(
		querysync.NewSchedulerFSM(querysync.DefaultConfig()), 8,
	)
	queryManager.Start(ctx)
	t.Cleanup(queryManager.Stop)

	headerManager := headersync.NewManagerActor(
		headersync.NewHeaderSyncFSM(0, testHash(100),
			headersync.DefaultConfig()), 8,
	)
	headerManager.SetDispatchController(New(queryManager))
	headerManager.Start(ctx)
	t.Cleanup(headerManager.Stop)

	return headerManager, queryManager
}

func addAnchor(t *testing.T, ctx context.Context,
	manager *headersync.ManagerActor, height uint32, hashByte byte) {

	t.Helper()

	_, ok, err := manager.AddAnchor(ctx, height, testHash(hashByte), true)
	require.NoError(t, err)
	require.True(t, ok)
}

func planRange(t *testing.T, ctx context.Context,
	manager *headersync.ManagerActor, id headersync.RangeID,
	startHeight, stopHeight uint32) {

	t.Helper()

	_, ok, err := manager.PlanRange(ctx, id, startHeight, stopHeight)
	require.NoError(t, err)
	require.True(t, ok)
}

func testHash(value byte) chainhash.Hash {
	var hash chainhash.Hash
	hash[0] = value

	return hash
}
