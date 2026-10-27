package neutrino

import (
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headersync"
	"github.com/stretchr/testify/require"
)

type headerDSTResult struct {
	Height uint32
	Hash   chainhash.Hash
	Events []string
}

// dstMedianTime aligns the chain clock with its genesis while retaining virtual
// elapsed time. synctest starts in 2000, before SimNet's genesis timestamp.
type dstMedianTime struct {
	blockchain.MedianTimeSource
	offset time.Duration
}

func (s dstMedianTime) AdjustedTime() time.Time {
	return s.MedianTimeSource.AdjustedTime().Add(s.offset)
}

// runHeaderDST drives the production actor graph, structural/contextual
// validation, and ordered store commit. The simulated transport supplies only
// responses and faults; it does not reimplement scheduling or ownership.
// synctest controls time and actor quiescence, while bytes select fault order.
func runHeaderDST(t *testing.T, script []byte) headerDSTResult {
	t.Helper()
	var result headerDSTResult
	synctest.Test(t, func(t *testing.T) {
		var flags, latency, deadline byte
		if len(script) > 0 {
			flags = script[0]
		}
		if len(script) > 1 {
			latency = script[1]
		}
		if len(script) > 2 {
			deadline = script[2]
		}
		bm, store, _, err := setupBlockManager(t)
		require.NoError(t, err)
		ctx := t.Context()
		runtime := bm.headerSync
		runtime.Start(ctx)
		defer runtime.Stop()
		manager := runtime.Manager()
		genesis, _, err := store.ChainTip()
		require.NoError(t, err)
		offset := time.Until(genesis.Timestamp.Add(time.Hour))
		bm.cfg.TimeSource = dstMedianTime{
			MedianTimeSource: bm.cfg.TimeSource,
			offset:           offset,
		}
		headers := discoveryTestHeaders(t, bm, genesis, 4)
		for _, height := range []uint32{2, 4} {
			_, ok, err := manager.AddAnchor(
				ctx, height, headers[height-1].BlockHash(),
				true,
			)
			require.NoError(t, err)
			require.True(t, ok)
		}

		if flags&4 != 0 {
			bad := &wire.BlockHeader{PrevBlock: genesis.BlockHash()}
			stageDiscovery(t, bm, "bad", headersync.AnchorRequest{
				StartHash:  genesis.BlockHash(),
				StopHeight: 2000,
			}, []*wire.BlockHeader{bad})
			var invalid *headerSyncValidationError
			require.ErrorAs(t,
				bm.commitReadyHeaderSyncRanges(ctx), &invalid)
			result.Events = append(result.Events, "invalidated")
		}

		slowPeer := headersync.PeerSnapshot{
			ID: "slow", State: headersync.PeerReady,
		}
		slow, _, err := runtime.RegisterPeer(ctx, slowPeer)
		require.NoError(t, err)
		synctest.Wait()
		firstID := runtime.Session().AllocateRangeID()
		_, ok, err := manager.PlanRange(ctx, firstID, 0, 2)
		require.NoError(t, err)
		require.True(t, ok)
		_, ok, err = manager.AssignNextQueuedRange(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		first, ok, err := slow.RequestRange(ctx)
		require.NoError(t, err)
		require.True(t, ok)

		fastPeer := headersync.PeerSnapshot{
			ID: "fast", State: headersync.PeerReady,
		}
		fast, _, err := runtime.RegisterPeer(ctx, fastPeer)
		require.NoError(t, err)
		synctest.Wait()
		secondID := runtime.Session().AllocateRangeID()
		_, ok, err = manager.PlanRange(ctx, secondID, 2, 4)
		require.NoError(t, err)
		require.True(t, ok)
		assigned, ok, err := manager.AssignNextQueuedRange(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "fast", assigned.PeerID)
		second, ok, err := fast.RequestRange(ctx)
		require.NoError(t, err)
		require.True(t, ok)

		deliver := func(peer *headersync.PeerActor,
			assignment headersync.RangeAssignment,
			data []*wire.BlockHeader) {

			t.Helper()
			rng, ok, err := manager.SnapshotRange(
				ctx, assignment.RangeID,
			)
			require.NoError(t, err)
			require.True(t, ok)
			require.NoError(t, headersync.ValidateHeaderRange(
				rng, data,
			))
			completion, err := peer.CompleteRange(
				ctx, assignment.RangeID, assignment.LeaseEpoch,
				true, time.Now(),
			)
			require.NoError(t, err)
			require.True(t, completion.Accepted)
			runtime.Session().StageRange(
				assignment.RangeID, data, assignment.PeerID,
			)
		}
		time.Sleep(time.Duration(latency%8) * time.Millisecond)
		deliver(fast, second, headers[2:])
		require.NoError(t, bm.commitReadyHeaderSyncRanges(ctx))
		_, height, err := store.ChainTip()
		require.NoError(t, err)
		require.Zero(t, height, "out-of-order staging advanced the tip")
		result.Events = append(result.Events, "later_range_staged")

		// A transport timeout is an input, like a real peer timer. The
		// harness controls delivery, not the steal decision.
		expired := make(chan struct{})
		time.AfterFunc(time.Duration(deadline%8+1)*time.Second, func() {
			close(expired)
		})
		<-expired
		if flags&1 != 0 {
			require.NoError(t, runtime.UnregisterPeer(ctx, "slow"))
			_, ok, err = manager.AssignNextQueuedRange(ctx)
			require.NoError(t, err)
			require.True(t, ok)
			result.Events = append(result.Events, "disconnected")
		} else {
			require.NoError(t, runtime.UpdatePeerState(
				ctx, "slow", headersync.PeerBlocked,
			))
			synctest.Wait()
			steal, ok, err := fast.RequestStolenRange(ctx)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, firstID, steal.RangeID)
			result.Events = append(result.Events, "stolen")
		}
		replacement, ok, err := fast.RequestRange(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		require.Greater(t, replacement.LeaseEpoch, first.LeaseEpoch)

		late := func() {
			t.Helper()
			completion, err := manager.CompleteRange(
				ctx, "slow", first.RangeID, first.LeaseEpoch,
				true, time.Now(),
			)
			require.NoError(t, err)
			require.True(t, completion.Stale)
			result.Events = append(result.Events, "stale")
		}
		if flags&8 != 0 {
			late()
		}
		deliver(fast, replacement, headers[:2])
		if flags&2 != 0 {
			writeErr := errors.New("simulated unavailable store")
			failing := &failingHeaderStore{
				BlockHeaderStore: store, writeErr: writeErr,
			}
			bm.cfg.BlockHeaders = failing
			require.ErrorIs(t,
				bm.commitReadyHeaderSyncRanges(ctx), writeErr)
			result.Events = append(result.Events, "write_failed")
			failing.writeErr = nil
		}
		require.NoError(t, bm.commitReadyHeaderSyncRanges(ctx))
		if flags&8 == 0 {
			late()
		}
		tip, height, err := store.ChainTip()
		require.NoError(t, err)
		require.EqualValues(t, 4, height)
		require.Equal(t, headers[3].BlockHash(), tip.BlockHash())
		require.Equal(t, height, bm.headerTip)
		for _, id := range []headersync.RangeID{firstID, secondID} {
			rng, ok, err := manager.SnapshotRange(ctx, id)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, headersync.RangeCommitted, rng.State)
			_, staged := runtime.Session().StagedRange(id)
			require.False(t, staged)
		}
		metrics, err := manager.Metrics(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 2, metrics.RangesCommitted)
		result.Height, result.Hash = height, tip.BlockHash()
		synctest.Wait()
	})
	return result
}

func TestHeaderSyncDSTReplay(t *testing.T) {
	for flags := byte(0); flags < 16; flags++ {
		script := []byte{flags, flags * 7, flags * 3}
		t.Run(fmt.Sprintf("%x", script), func(t *testing.T) {
			first := runHeaderDST(t, script)
			require.Equal(t, first, runHeaderDST(t, script))
		})
	}
}

func FuzzHeaderSyncDST(f *testing.F) {
	for flags := byte(0); flags < 16; flags++ {
		f.Add([]byte{flags, flags * 7, flags * 3})
	}
	f.Fuzz(func(t *testing.T, script []byte) {
		first := runHeaderDST(t, script)
		require.Equal(t, first, runHeaderDST(t, script))
	})
}
