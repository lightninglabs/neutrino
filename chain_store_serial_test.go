package neutrino

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/stretchr/testify/require"
)

// TestSerialHeaderPreparationDoesNotPublish verifies validation uses scratch
// state and the prepared payload does not alias peer-owned headers.
func TestSerialHeaderPreparationDoesNotPublish(t *testing.T) {
	params := &chaincfg.SimNetParams
	blocks := newMemBlockHeaderStore(params)
	filters, err := newMemFilterHeaderStore(params)
	require.NoError(t, err)
	bm := newCFHeaderTestManager(t, blocks, filters)
	fixture := dstChains()
	header := *fixture.honest[1]
	prepared, handled, err := bm.prepareSerialHeaderExtension(
		[]*wire.BlockHeader{&header},
	)
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, int32(0), prepared.parent.Height)
	require.Equal(t, params.GenesisBlock.BlockHash(), prepared.parent.Hash)
	require.Len(t, prepared.nodes, 1)
	require.Equal(t, int32(0), bm.headerList.Back().Height)
	_, height, err := blocks.ChainTip()
	require.NoError(t, err)
	require.Zero(t, height)
	original := header.BlockHash()
	header.Nonce++
	require.Equal(t, original, prepared.headers[0].BlockHash())
	require.Equal(t, original, prepared.nodes[0].Header.BlockHash())
}

// TestSerialHeaderAmbiguousWriteReconciles verifies that a failed serial
// write publishes the durable prefix before the retry is scheduled.
func TestSerialHeaderAmbiguousWriteReconciles(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mode   dstStoreMode
		height int32
	}{
		{"ack lost", dstStoreAckLost, 2},
		{"partial", dstStoreTorn, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := newDSTRun(
					t,
					dstConfig{mode: dstModeCheckpointAll},
				)
				d.inv.skipLeases = true
				d.bm.sessionForTest().StopAll()
				d.store.mode = tc.mode
				message := wire.NewMsgHeaders()
				for _, header := range dstChains().honest[1:3] {
					require.NoError(
						t,
						message.AddBlockHeader(header),
					)
				}
				d.deliver(
					&headersMsg{
						peer:    d.current[0],
						headers: message,
					},
				)
				header, height, err := d.store.ChainTip()
				require.NoError(t, err)
				require.Equal(t, uint32(tc.height), height)
				require.Equal(
					t,
					tc.height,
					d.bm.headerList.Back().Height,
				)
				require.Equal(
					t,
					header.BlockHash(),
					d.bm.headerList.Back().Header.
						BlockHash(),
				)
				visible := d.bm.publishedBlockTip.Load()
				require.Equal(t, tc.height, visible.Height)
				require.Equal(
					t,
					header.BlockHash(),
					visible.Hash,
				)
				require.NotNil(t, d.bm.headerCommitRetryTimer)
			})
		})
	}
}

// TestSerialReorgPreparationCopiesHeaders binds the fork and replacement to
// the observed tip without retaining the caller's mutable headers.
func TestSerialReorgPreparationCopiesHeaders(t *testing.T) {
	fixture := dstChains()
	tip := headerlist.Node{Header: *fixture.honest[3], Height: 3}
	fork := *fixture.honest[1]
	replacement := *fixture.fork[1][0]
	forkHash, replacementHash := fork.BlockHash(), replacement.BlockHash()
	prepared := prepareSerialHeaderReorg(tip, fork, 1, replacement)
	require.Equal(t, tip.Header.BlockHash(), prepared.expectedTip.Hash)
	require.Equal(t, int32(3), prepared.expectedTip.Height)
	fork.Nonce++
	replacement.Nonce++
	require.Equal(t, forkHash, prepared.fork.Header.BlockHash())
	require.Equal(t, replacementHash, prepared.replacement.BlockHash())
}

type chainStoreGatedWrite struct {
	headerfs.BlockHeaderStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *chainStoreGatedWrite) WriteHeaders(
	headers ...headerfs.BlockHeader,
) error {

	s.once.Do(func() {
		close(s.entered)
		<-s.release
	})
	return s.BlockHeaderStore.WriteHeaders(headers...)
}

// TestChainStoreDisconnectedPeerQueuedWork checks that priority disconnect
// cannot revive a peer whose earlier messages wait behind a store mutation.
func TestChainStoreDisconnectedPeerQueuedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := newMemBlockHeaderStore(params)
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		gate := &chainStoreGatedWrite{
			BlockHeaderStore: blocks,
			entered: make(
				chan struct{},
			), release: make(chan struct{}),
		}
		bm := newCFHeaderTestManager(t, gate, filters)
		bm.cfg.TimeSource = dstMedianTime{
			MedianTimeSource: bm.cfg.TimeSource,
			offset: time.Until(
				params.GenesisBlock.Header.Timestamp.Add(
					time.Hour,
				),
			),
		}
		bm.headerCooldownID = 1
		bm.headerCooldownTimer = time.AfterFunc(time.Hour, func() {})
		bm.Start()
		t.Cleanup(func() { require.NoError(t, bm.Stop()) })
		writer := newTestServerPeer(t, "127.0.0.1:18331", 1)
		stale := newTestServerPeer(t, "127.0.0.1:18332", 2)
		bm.syncPeerMutex.Lock()
		bm.syncPeer = writer
		bm.syncPeerMutex.Unlock()
		first := wire.NewMsgHeaders()
		require.NoError(t, first.AddBlockHeader(dstChains().honest[1]))
		bm.QueueHeaders(first, writer)
		<-gate.entered
		bm.NewPeer(stale)
		second := wire.NewMsgHeaders()
		require.NoError(t, second.AddBlockHeader(dstChains().honest[2]))
		bm.QueueHeaders(second, stale)
		stale.Disconnect()
		bm.DonePeer(stale)
		bm.timerChan <- &headerSyncCooldownMsg{generation: 1}
		synctest.Wait()
		require.Nil(t, bm.headerCooldownTimer,
			"cooldown control must progress while store is held")
		close(gate.release)
		synctest.Wait()
		_, height, err := blocks.ChainTip()
		require.NoError(t, err)
		require.Equal(t, uint32(1), height)
		_, exists := bm.headerSync.Peer(stale.Addr())
		require.False(t, exists)
	})
}

// TestChainStoreCheckpointMismatchRollsBack verifies that serial checkpoint
// recovery uses the actor before publishing the previous checkpoint.
func TestChainStoreCheckpointMismatchRollsBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := newMemBlockHeaderStore(params)
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		fixture := dstChains()
		require.NoError(t, blocks.WriteHeaders(headerfs.BlockHeader{
			BlockHeader: fixture.honest[1], Height: 1,
		}))
		bm := newCFHeaderTestManager(t, blocks, filters)
		wrong := fixture.fork[1][0].BlockHash()
		bm.cfg.ChainParams.Checkpoints = []chaincfg.Checkpoint{{
			Height: 2, Hash: &wrong,
		}}
		bm.nextCheckpoint = &bm.cfg.ChainParams.Checkpoints[0]
		bm.cfg.TimeSource = dstMedianTime{
			MedianTimeSource: bm.cfg.TimeSource,
			offset: time.Until(
				params.GenesisBlock.Header.Timestamp.Add(
					time.Hour,
				),
			),
		}
		notifications := make(chan blockntfns.BlockNtfn, 2)
		go func() {
			select {
			case n := <-bm.blockNtfnChan:
				notifications <- n
			case <-bm.quit:
			}
		}()
		bm.Start()
		t.Cleanup(func() { require.NoError(t, bm.Stop()) })
		peer := newTestServerPeer(t, "127.0.0.1:18333", 2)
		bm.syncPeerMutex.Lock()
		bm.syncPeer = peer
		bm.syncPeerMutex.Unlock()
		message := wire.NewMsgHeaders()
		require.NoError(t, message.AddBlockHeader(fixture.honest[2]))
		bm.QueueHeaders(message, peer)
		synctest.Wait()
		_, height, err := blocks.ChainTip()
		require.NoError(t, err)
		require.Zero(t, height)
		require.Equal(t, int32(0), bm.headerList.Back().Height)
		require.True(t, peerDisconnected(peer))
		require.Len(t, notifications, 1)
		event := <-notifications
		_, ok := event.(*blockntfns.Disconnected)
		require.True(t, ok)
		require.Equal(t, uint32(1), event.Height())
	})
}

// TestChainStoreKnownPrefixUsesActor checks that a response containing a
// known header followed by a new header still uses nonblocking actor writes.
func TestChainStoreKnownPrefixUsesActor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := newMemBlockHeaderStore(params)
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		fixture := dstChains()
		require.NoError(t, blocks.WriteHeaders(headerfs.BlockHeader{
			BlockHeader: fixture.honest[1], Height: 1,
		}))
		gate := &chainStoreGatedWrite{
			BlockHeaderStore: blocks,
			entered: make(
				chan struct{},
			), release: make(chan struct{}),
		}
		bm := newCFHeaderTestManager(t, gate, filters)
		bm.cfg.TimeSource = dstMedianTime{
			MedianTimeSource: bm.cfg.TimeSource,
			offset: time.Until(
				params.GenesisBlock.Header.Timestamp.Add(
					time.Hour,
				),
			),
		}
		bm.Start()
		t.Cleanup(func() { require.NoError(t, bm.Stop()) })
		writer := newTestServerPeer(t, "127.0.0.1:18334", 2)
		spectator := newTestServerPeer(t, "127.0.0.1:18335", 2)
		bm.registerHeaderSyncPeer(spectator)
		bm.syncPeerMutex.Lock()
		bm.syncPeer = writer
		bm.syncPeerMutex.Unlock()
		message := wire.NewMsgHeaders()
		for _, header := range fixture.honest[1:3] {
			require.NoError(t, message.AddBlockHeader(header))
		}
		bm.QueueHeaders(message, writer)
		<-gate.entered
		bm.DonePeer(spectator)
		synctest.Wait()
		_, exists := bm.headerSync.Peer(spectator.Addr())
		require.False(t, exists)
		close(gate.release)
		synctest.Wait()
		_, height, err := blocks.ChainTip()
		require.NoError(t, err)
		require.Equal(t, uint32(2), height)
	})
}

type chainStoreWriteReadFault struct {
	headerfs.BlockHeaderStore
	failRead bool
}

func (s *chainStoreWriteReadFault) WriteHeaders(
	headers ...headerfs.BlockHeader,
) error {

	if err := s.BlockHeaderStore.WriteHeaders(headers...); err != nil {
		return err
	}
	s.failRead = true
	return errDSTWrite
}

func (s *chainStoreWriteReadFault) ChainTip() (
	*wire.BlockHeader, uint32, error) {

	if s.failRead {
		s.failRead = false
		return nil, 0, errCFHeaderRead
	}
	return s.BlockHeaderStore.ChainTip()
}

// TestChainStoreSerialLostWriteAckReconciles checks that a failed readback
// is recovered by an actor snapshot before handler state advances.
func TestChainStoreSerialLostWriteAckReconciles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := newMemBlockHeaderStore(params)
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		fault := &chainStoreWriteReadFault{BlockHeaderStore: blocks}
		bm := newCFHeaderTestManager(t, fault, filters)
		bm.cfg.TimeSource = dstMedianTime{
			MedianTimeSource: bm.cfg.TimeSource,
			offset: time.Until(
				params.GenesisBlock.Header.Timestamp.Add(
					time.Hour,
				),
			),
		}
		bm.Start()
		t.Cleanup(func() { require.NoError(t, bm.Stop()) })
		peer := newTestServerPeer(t, "127.0.0.1:18336", 1)
		bm.syncPeerMutex.Lock()
		bm.syncPeer = peer
		bm.syncPeerMutex.Unlock()
		message := wire.NewMsgHeaders()
		require.NoError(
			t,
			message.AddBlockHeader(dstChains().honest[1]),
		)
		bm.QueueHeaders(message, peer)
		synctest.Wait()
		_, height, err := blocks.ChainTip()
		require.NoError(t, err)
		require.Equal(t, uint32(1), height)
		require.Equal(t, int32(1), bm.headerList.Back().Height)
		require.Equal(t, int32(1), bm.publishedBlockTip.Load().Height)
		require.Nil(t, bm.pendingHeaderCommit)
		require.NotNil(t, bm.headerCommitRetryTimer)
	})
}
