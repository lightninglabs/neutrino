package neutrino

import (
	"container/list"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/stretchr/testify/require"
)

func TestSerialPartialWriteRetainsObservedPeerHeight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := newMemBlockHeaderStore(params)
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		bm := newCFHeaderTestManager(t, blocks, filters)
		bm.cfg.TimeSource = dstMedianTime{
			MedianTimeSource: bm.cfg.TimeSource,
			offset: time.Until(
				params.GenesisBlock.Header.Timestamp.Add(
					time.Hour,
				),
			),
		}
		bm.startHeaderSyncActors()
		t.Cleanup(func() { require.NoError(t, bm.Stop()) })
		store := &dstHeaderStore{
			BlockHeaderStore: blocks,
			mode:             dstStoreTorn,
		}
		bm.cfg.BlockHeaders = store
		recorder := &locatorRecorder{}
		bm.cfg.HeaderTransport = recorder
		peer := newTestServerPeer(t, "127.0.0.1:8333", 0)
		bm.syncPeer = peer
		peers := list.New()
		peers.PushBack(peer)
		message := wire.NewMsgHeaders()
		for _, header := range dstChains().honest[1:3] {
			require.NoError(t, message.AddBlockHeader(header))
		}
		bm.handleHeadersMsgWithPeers(
			peers,
			&headersMsg{peer: peer, headers: message},
		)
		settleHeaderCommit(t, bm, peers)
		_, durable, err := store.ChainTip()
		require.NoError(t, err)
		require.EqualValues(t, 1, durable)
		store.mode = dstStoreOK
		time.Sleep(time.Second)
		synctest.Wait()
		bm.handlePeerMessage(peers, <-bm.timerChan)
		t.Logf(
			"durable=%d peerHeight=%d candidates=%d syncPeer=%v "+
				"followupRequests=%d",
			durable,
			peer.LastBlock(),
			peers.Len(),
			bm.SyncPeer() != nil,
			len(recorder.requests),
		)
		require.NotZero(t, peers.Len(),
			"retry removed peer with the validated missing suffix")
		require.NotEmpty(
			t,
			recorder.requests,
			"retry must request the missing suffix",
		)
	})
}

// failingFetchHeaderStore fails the lookup of one header hash.
type failingFetchHeaderStore struct {
	headerfs.BlockHeaderStore
	failHash chainhash.Hash
}

// FetchHeader fails for the configured hash.
func (s *failingFetchHeaderStore) FetchHeader(
	hash *chainhash.Hash) (*wire.BlockHeader, uint32, error) {

	if *hash == s.failHash {
		return nil, 0, errCFHeaderRead
	}

	return s.BlockHeaderStore.FetchHeader(hash)
}

// TestReorgWorkComparisonStoreReadFailure is the regression for a nil
// dereference in the reorg work comparison. The walk over the known chain
// leaves the validation list and reads a parent from the store. A failed read
// used to leave the header nil and crash the handler. It must now abandon the
// message without punishing the peer and let the retry request the headers
// again.
func TestReorgWorkComparisonStoreReadFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		fixture := dstChains()
		blocks := newMemBlockHeaderStore(params)
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)

		var headers []headerfs.BlockHeader
		for h := uint32(1); h <= dstChainHeight; h++ {
			headers = append(headers, headerfs.BlockHeader{
				BlockHeader: fixture.honest[h], Height: h,
			})
		}
		require.NoError(t, blocks.WriteHeaders(headers...))

		bm := newCFHeaderTestManager(t, blocks, filters)
		bm.cfg.TimeSource = dstMedianTime{
			MedianTimeSource: bm.cfg.TimeSource,
			offset: time.Until(
				params.GenesisBlock.Header.Timestamp.Add(
					time.Hour,
				),
			),
		}

		// The validation list holds only the tip, so the walk reads the
		// tip's parent from the store.
		bm.cfg.BlockHeaders = &failingFetchHeaderStore{
			BlockHeaderStore: blocks,
			failHash: fixture.honest[dstChainHeight-1].
				BlockHash(),
		}
		peer := newTestServerPeer(
			t, "127.0.0.1:8333", int32(len(fixture.reorg)-1),
		)
		bm.syncPeer = peer
		peers := list.New()
		peers.PushBack(peer)

		msg := wire.NewMsgHeaders()
		for _, header := range fixture.reorg[dstReorgFork+1:] {
			require.NoError(t, msg.AddBlockHeader(header))
		}
		bm.handleHeadersMsgWithPeers(
			peers, &headersMsg{peer: peer, headers: msg},
		)
		synctest.Wait()

		require.False(t, dstDisconnected(peer),
			"an honest peer was punished for a local read error")
		require.Nil(t, bm.pendingHeaderCommit)
		require.NotNil(t, bm.headerCommitRetryTimer,
			"no retry was scheduled")
		_, height, err := blocks.ChainTip()
		require.NoError(t, err)
		require.EqualValues(t, dstChainHeight, height)
	})
}

// failingTipFilterStore fails filter tip reads while armed.
type failingTipFilterStore struct {
	headerfs.FilterHeaderStore
	fail bool
}

// ChainTip fails while armed.
func (s *failingTipFilterStore) ChainTip() (*chainhash.Hash, uint32, error) {
	if s.fail {
		return nil, 0, errCFHeaderRead
	}

	return s.FilterHeaderStore.ChainTip()
}

// newPendingCommitFixture returns a manager over three honest blocks with a
// peer list, ready for a hand-built pending commit.
func newPendingCommitFixture(t *testing.T) (*blockManager,
	*failingTipFilterStore, *list.List, *ServerPeer) {

	params := &chaincfg.SimNetParams
	fixture := dstChains()
	blocks := newMemBlockHeaderStore(params)
	underlying, err := newMemFilterHeaderStore(params)
	require.NoError(t, err)
	filters := &failingTipFilterStore{FilterHeaderStore: underlying}

	var headers []headerfs.BlockHeader
	for h := uint32(1); h <= 3; h++ {
		headers = append(headers, headerfs.BlockHeader{
			BlockHeader: fixture.honest[h], Height: h,
		})
	}
	require.NoError(t, blocks.WriteHeaders(headers...))

	bm := newCFHeaderTestManager(t, blocks, filters)
	bm.cfg.HeaderTransport = &locatorRecorder{}
	peer := newTestServerPeer(t, "127.0.0.1:8333", 3)
	peers := list.New()
	peers.PushBack(peer)

	return bm, filters, peers, peer
}

// TestCheckpointRollbackSnapshotDisconnectsPeer checks that a checkpoint
// rollback whose mutation reports an error, and is completed by the
// reconciliation snapshot, still disconnects the peer that sent the bad
// header, and restarts sync when that peer was already gone.
func TestCheckpointRollbackSnapshotDisconnectsPeer(t *testing.T) {
	for _, syncPeerGone := range []bool{false, true} {
		name := fmt.Sprintf("sync_peer_gone=%v", syncPeerGone)
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				bm, _, peers, peer := newPendingCommitFixture(t)
				other := newTestServerPeer(
					t, "127.0.0.2:8333", 3,
				)
				peers.PushBack(other)
				bm.syncPeer = peer
				if syncPeerGone {
					bm.syncPeer = nil
				}

				node := headerlist.Node{
					Header: *dstChains().honest[1],
					Height: 1,
				}
				bm.pendingHeaderCommit =
					&pendingChainHeaderCommit{
						checkpoint: &node,
						message: &headersMsg{
							peer: peer,
						},
					}
				failed := chainStoreResult{
					reply: chainStoreReply{
						Err: errDSTWrite,
					},
				}
				bm.finishAsyncHeaderCommit(peers, failed)
				settleHeaderCommit(t, bm, peers)

				require.True(t, dstDisconnected(peer))
				if syncPeerGone {
					require.NotNil(t, bm.SyncPeer(),
						"sync was not restarted")
				}
			})
		})
	}
}

// TestSnapshotFilterFailureKeepsHeaderIntake checks that a snapshot whose
// block side succeeded does not freeze header intake when only the filter
// side fails.
func TestSnapshotFilterFailureKeepsHeaderIntake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, filters, peers, _ := newPendingCommitFixture(t)
		bm.pendingHeaderCommit = &pendingChainHeaderCommit{
			snapshot: true,
		}
		filters.fail = true
		bm.chainStore.tryAsk(
			t.Context(), chainStoreSnapshotRequest{},
			bm.chainStoreResult,
		)
		result := <-bm.chainStoreResult
		require.True(t, result.reply.BlockTipValid)
		require.Error(t, result.reply.Err)

		bm.finishAsyncHeaderCommit(peers, result)
		require.Nil(t, bm.pendingHeaderCommit,
			"header intake stayed frozen")
		require.EqualValues(t, 3, bm.headerList.Back().Height)
	})
}
