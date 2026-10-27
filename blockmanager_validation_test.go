package neutrino

import (
	"errors"
	"testing"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/lightninglabs/neutrino/headersync"
	"github.com/stretchr/testify/require"
)

func discoveryTestHeaders(t *testing.T, bm *blockManager,
	parent *wire.BlockHeader, count int) []*wire.BlockHeader {

	t.Helper()
	headers := make([]*wire.BlockHeader, 0, count)
	for i := 0; i < count; i++ {
		header := &wire.BlockHeader{
			Version:   4,
			PrevBlock: parent.BlockHash(),
			Bits:      parent.Bits,
			Timestamp: parent.Timestamp.Add(time.Second),
		}
		mineDiscoveryHeader(t, bm, header)
		headers = append(headers, header)
		parent = header
	}
	return headers
}

func mineDiscoveryHeader(t *testing.T, bm *blockManager,
	header *wire.BlockHeader) {

	t.Helper()
	for {
		err := blockchain.CheckBlockHeaderSanity(
			header, bm.cfg.ChainParams.PowLimit,
			bm.cfg.TimeSource, 0,
		)
		if err == nil {
			return
		}
		var rule blockchain.RuleError
		require.ErrorAs(t, err, &rule)
		require.Equal(t, blockchain.ErrHighHash, rule.ErrorCode)
		header.Nonce++
		require.Less(t, header.Nonce, uint32(10000))
	}
}

func stageDiscovery(t *testing.T, bm *blockManager, peerID string,
	request headersync.AnchorRequest,
	headers []*wire.BlockHeader) headersync.HeaderRange {

	t.Helper()
	bm.headerSync.Session().TrackAnchor(peerID, request)
	result, ok, err := bm.headerSync.Controller().FinishAnchorHeaders(
		t.Context(), peerID, headers, headersync.DefaultMaxRangeHeaders,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, result.Staged)
	bm.headerSync.Session().StageRange(result.Range.ID, headers, peerID)
	return result.Range
}

func TestInvalidDiscoveryRecovers(t *testing.T) {
	t.Parallel()
	bm, store, _, err := setupBlockManager(t)
	require.NoError(t, err)
	bm.headerSync.Start(t.Context())
	t.Cleanup(bm.headerSync.Stop)
	genesis, _, err := store.ChainTip()
	require.NoError(t, err)
	request := headersync.AnchorRequest{
		StartHash: genesis.BlockHash(), StopHeight: 2000,
	}
	bad := &wire.BlockHeader{
		PrevBlock: genesis.BlockHash(),
		Timestamp: genesis.Timestamp.Add(time.Second),
	}
	rng := stageDiscovery(t, bm, "bad", request,
		[]*wire.BlockHeader{bad})
	err = bm.commitReadyHeaderSyncRanges(t.Context())
	var invalid *headerSyncValidationError
	require.ErrorAs(t, err, &invalid)
	require.Equal(t, rng.ID, invalid.rangeID)
	_, height, err := store.ChainTip()
	require.NoError(t, err)
	require.Zero(t, height)
	_, staged := bm.headerSync.Session().StagedRange(rng.ID)
	require.False(t, staged)
	failed, ok, err := bm.headerSync.Manager().SnapshotRange(
		t.Context(), rng.ID,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, headersync.RangeFailed, failed.State)

	// A different valid boundary may overlap the rejected segment. Recovery
	// must not require a manager restart or discarding trusted anchors.
	honest := discoveryTestHeaders(t, bm, genesis, 2)
	stageDiscovery(t, bm, "honest", request, honest)
	require.NoError(t, bm.commitReadyHeaderSyncRanges(t.Context()))
	tip, height, err := store.ChainTip()
	require.NoError(t, err)
	require.EqualValues(t, 2, height)
	require.Equal(t, honest[1].BlockHash(), tip.BlockHash())
}

type failingHeaderStore struct {
	headerfs.BlockHeaderStore
	writeErr error
}

func (s *failingHeaderStore) WriteHeaders(
	headers ...headerfs.BlockHeader) error {

	if s.writeErr != nil {
		return s.writeErr
	}
	return s.BlockHeaderStore.WriteHeaders(headers...)
}

func TestHeaderCommitStorageFailurePreservesStaging(t *testing.T) {
	t.Parallel()
	bm, store, _, err := setupBlockManager(t)
	require.NoError(t, err)
	bm.headerSync.Start(t.Context())
	t.Cleanup(bm.headerSync.Stop)
	genesis, _, err := store.ChainTip()
	require.NoError(t, err)
	headers := discoveryTestHeaders(t, bm, genesis, 2)
	rng := stageDiscovery(t, bm, "peer", headersync.AnchorRequest{
		StartHash: genesis.BlockHash(), StopHeight: 2000,
	}, headers)
	writeErr := errors.New("temporary storage failure")
	failing := &failingHeaderStore{
		BlockHeaderStore: store, writeErr: writeErr,
	}
	bm.cfg.BlockHeaders = failing
	require.ErrorIs(t,
		bm.commitReadyHeaderSyncRanges(t.Context()), writeErr)
	_, staged := bm.headerSync.Session().StagedRange(rng.ID)
	require.True(t, staged)
	ready, err := bm.headerSync.Manager().ReadyRanges(t.Context())
	require.NoError(t, err)
	require.Equal(t, []headersync.RangeID{rng.ID}, ready)
	_, height, err := store.ChainTip()
	require.NoError(t, err)
	require.Zero(t, height)

	failing.writeErr = nil
	require.NoError(t, bm.commitReadyHeaderSyncRanges(t.Context()))
	_, height, err = store.ChainTip()
	require.NoError(t, err)
	require.EqualValues(t, 2, height)
}

type failingAncestorStore struct {
	headerfs.BlockHeaderStore
	failHeight uint32
	readErr    error
}

func (s *failingAncestorStore) FetchHeaderByHeight(
	height uint32) (*wire.BlockHeader, error) {

	if height == s.failHeight && s.readErr != nil {
		return nil, s.readErr
	}
	return s.BlockHeaderStore.FetchHeaderByHeight(height)
}

func TestHeaderAncestorReadFailurePreservesStaging(t *testing.T) {
	t.Parallel()
	bm, store, _, err := setupBlockManager(t)
	require.NoError(t, err)
	ctx := t.Context()
	bm.headerSync.Start(ctx)
	t.Cleanup(bm.headerSync.Stop)
	genesis, _, err := store.ChainTip()
	require.NoError(t, err)

	// A shortened history has a higher median than the complete history.
	// Fail the second ancestor read to exercise error propagation through a
	// child HeaderCtx, not just the initial context passed to btcd.
	history := discoveryTestHeaders(t, bm, genesis, 9)
	for _, seconds := range []int{90, 100} {
		parent := history[len(history)-1]
		header := discoveryTestHeaders(t, bm, parent, 1)[0]
		header.Timestamp = genesis.Timestamp.Add(
			time.Duration(seconds) * time.Second,
		)
		mineDiscoveryHeader(t, bm, header)
		history = append(history, header)
	}
	for i, header := range history {
		require.NoError(t, store.WriteHeaders(headerfs.BlockHeader{
			BlockHeader: header, Height: uint32(i + 1),
		}))
	}
	parent := history[10]
	bm.headerList.ResetHeaderState(headerlist.Node{
		Height: 11, Header: *parent,
	})
	bm.headerTip, bm.headerTipHash = 11, parent.BlockHash()
	manager := bm.headerSync.Manager()
	_, err = manager.AdvanceCommittedTip(ctx, 11, parent.BlockHash())
	require.NoError(t, err)
	_, _, err = bm.headerSync.RegisterPeer(ctx, headersync.PeerSnapshot{
		ID: "honest", State: headersync.PeerReady,
	})
	require.NoError(t, err)

	candidate := discoveryTestHeaders(t, bm, parent, 1)[0]
	candidate.Timestamp = genesis.Timestamp.Add(50 * time.Second)
	mineDiscoveryHeader(t, bm, candidate)
	rng := stageDiscovery(t, bm, "honest", headersync.AnchorRequest{
		StartHeight: 11,
		StartHash:   parent.BlockHash(),
		StopHeight:  2000,
	}, []*wire.BlockHeader{candidate})
	readErr := errors.New("temporary ancestor read failure")
	failing := &failingAncestorStore{
		BlockHeaderStore: store, failHeight: 9, readErr: readErr,
	}
	bm.cfg.BlockHeaders = failing
	err = bm.commitReadyHeaderSyncRanges(ctx)
	require.ErrorIs(t, err, readErr)
	var invalid *headerSyncValidationError
	require.False(t, errors.As(err, &invalid))
	_, staged := bm.headerSync.Session().StagedRange(rng.ID)
	require.True(t, staged)
	ready, err := manager.ReadyRanges(ctx)
	require.NoError(t, err)
	require.Equal(t, []headersync.RangeID{rng.ID}, ready)
	peer, ok, err := manager.SnapshotPeer(ctx, "honest")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, headersync.PeerReady, peer.State)
	require.EqualValues(t, 11, bm.headerTip)

	failing.readErr = nil
	require.NoError(t, bm.commitReadyHeaderSyncRanges(ctx))
	tip, height, err := store.ChainTip()
	require.NoError(t, err)
	require.EqualValues(t, 12, height)
	require.Equal(t, candidate.BlockHash(), tip.BlockHash())
}
