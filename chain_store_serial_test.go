package neutrino

import (
	"container/list"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/stretchr/testify/require"
)

// TestSerialHeaderPreparationDoesNotPublish verifies validation uses scratch
// state and the prepared payload does not alias peer-owned headers. Nothing
// may become visible before the store owner commits it, and a peer that
// mutates its message afterwards must not change what is committed.
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

	// Neither the in-memory list nor the store has changed.
	require.Equal(t, int32(0), bm.headerList.Back().Height)
	_, height, err := blocks.ChainTip()
	require.NoError(t, err)
	require.Zero(t, height)

	// Mutating the peer's header leaves the prepared copy intact.
	original := header.BlockHash()
	header.Nonce++
	require.Equal(t, original, prepared.headers[0].BlockHash())
	require.Equal(t, original, prepared.nodes[0].Header.BlockHash())
}

// TestSerialHeaderAmbiguousWriteReconciles verifies that a failed serial
// write publishes the durable prefix before the retry is scheduled. The
// durable height depends on whether the store applied none, part or all of
// the batch, and the handler list and published tip must match it exactly.
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

				// Deliver two headers into the faulty store.
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

				// The handler list and published tip match the
				// durable prefix, and a retry is scheduled.
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
// the observed tip without retaining the caller's mutable headers. The bound
// tip lets the store owner reject a reorg that was prepared against a stale
// chain, and the copies keep a later mutation of the peer's message from
// changing what is written.
func TestSerialReorgPreparationCopiesHeaders(t *testing.T) {
	fixture := dstChains()
	tip := headerlist.Node{Header: *fixture.honest[3], Height: 3}
	fork := *fixture.honest[1]
	replacement := *fixture.fork[1][0]
	forkHash, replacementHash := fork.BlockHash(), replacement.BlockHash()
	prepared := prepareSerialHeaderReorg(tip, fork, 1, replacement)

	// The request is bound to the observed tip.
	require.Equal(t, tip.Header.BlockHash(), prepared.expectedTip.Hash)
	require.Equal(t, int32(3), prepared.expectedTip.Height)

	// Mutating the inputs leaves the prepared copies intact.
	fork.Nonce++
	replacement.Nonce++
	require.Equal(t, forkHash, prepared.fork.Header.BlockHash())
	require.Equal(t, replacementHash, prepared.replacement.BlockHash())
}

// chainStoreGatedWrite blocks the first block header write until release is
// closed, and closes entered when that write begins. A test uses it to hold
// the chain store mid-mutation while it drives the rest of the manager.
type chainStoreGatedWrite struct {
	headerfs.BlockHeaderStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

// WriteHeaders blocks the first write on the gate and then forwards it.
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
// If the queued headers were handled after the disconnect, the handler would
// re-register a dead peer and later pick it as the sync peer. The cooldown
// control message must also be consumed while the store is held.
func TestChainStoreDisconnectedPeerQueuedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := newMemBlockHeaderStore(params)
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		gate := &chainStoreGatedWrite{
			BlockHeaderStore: blocks,
			entered:          make(chan struct{}),
			release:          make(chan struct{}),
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

		// The sync peer's headers hold the store inside a write.
		first := wire.NewMsgHeaders()
		require.NoError(t, first.AddBlockHeader(dstChains().honest[1]))
		bm.QueueHeaders(first, writer)
		<-gate.entered

		// Another peer queues work behind that write and then goes
		// away, followed by a cooldown timer message.
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

		// Releasing the write must not revive the stale peer or apply
		// its queued headers.
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
// recovery uses the actor before publishing the previous checkpoint. A header
// that fails the checkpoint must be rolled back through the store owner, so
// the durable chain, the in-memory list and the notification stream stay in
// agreement, and the peer that sent it is dropped.
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

		// The peer sends block 2, which does not match the checkpoint.
		message := wire.NewMsgHeaders()
		require.NoError(t, message.AddBlockHeader(fixture.honest[2]))
		bm.QueueHeaders(message, peer)
		synctest.Wait()

		// The chain returns to genesis, the peer is dropped, and the
		// rolled back block is announced as disconnected.
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
// If the handler wrote synchronously, a slow store would block the handler
// and stall peer removal for every other peer.
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
			entered:          make(chan struct{}),
			release:          make(chan struct{}),
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

		// Removing an unrelated peer completes while the write is held.
		bm.DonePeer(spectator)
		synctest.Wait()
		_, exists := bm.headerSync.Peer(spectator.Addr())
		require.False(t, exists)

		// Releasing the write stores the new header.
		close(gate.release)
		synctest.Wait()
		_, height, err := blocks.ChainTip()
		require.NoError(t, err)
		require.Equal(t, uint32(2), height)
	})
}

// chainStoreWriteReadFault applies a block write and reports it as failed,
// and then fails the next tip read. The caller cannot tell whether the write
// landed, and its first attempt to find out fails too.
type chainStoreWriteReadFault struct {
	headerfs.BlockHeaderStore
	failRead bool
}

// WriteHeaders applies the batch, arms the read failure and reports a lost
// acknowledgement.
func (s *chainStoreWriteReadFault) WriteHeaders(
	headers ...headerfs.BlockHeader,
) error {

	if err := s.BlockHeaderStore.WriteHeaders(headers...); err != nil {
		return err
	}
	s.failRead = true
	return errDSTWrite
}

// ChainTip fails once after a write.
func (s *chainStoreWriteReadFault) ChainTip() (
	*wire.BlockHeader, uint32, error) {

	if s.failRead {
		s.failRead = false
		return nil, 0, errCFHeaderRead
	}
	return s.BlockHeaderStore.ChainTip()
}

// TestChainStoreSerialLostWriteAckReconciles checks that a failed readback
// is recovered by an actor snapshot before handler state advances. The
// handler's header list and the published tip may only move to a height that
// the store confirmed, and a retry must be scheduled for the rest.
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

		// The snapshot found block 1 durable, so the handler state
		// and the published tip agree with the store.
		_, height, err := blocks.ChainTip()
		require.NoError(t, err)
		require.Equal(t, uint32(1), height)
		require.Equal(t, int32(1), bm.headerList.Back().Height)
		require.Equal(t, int32(1), bm.publishedBlockTip.Load().Height)
		require.Nil(t, bm.pendingHeaderCommit)
		require.NotNil(t, bm.headerCommitRetryTimer)
	})
}

// TestSerialPartialWriteRetainsObservedPeerHeight checks that a partially
// applied serial write does not make the handler forget the peer's height.
// The peer announced a longer chain than the store kept, so after the retry
// the handler must still treat it as a sync candidate and request the missing
// suffix. Dropping the peer would leave the node stuck below the best chain.
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

		// The store keeps only the first of the two headers.
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

		// After the store recovers, the retry timer fires and the
		// missing suffix is requested from the same peer.
		store.mode = dstStoreOK
		time.Sleep(time.Second)
		synctest.Wait()
		bm.handlePeerMessage(peers, <-bm.timerChan)
		require.NotZero(t, peers.Len(),
			"retry removed peer with the validated missing suffix")
		require.NotEmpty(
			t,
			recorder.requests,
			"retry must request the missing suffix",
		)
	})
}

// failingFetchHeaderStore fails every lookup of one header hash. It models a
// local read error, as opposed to bad data from a peer.
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

		// Store the full honest chain.
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

		// The peer offers a heavier branch that forks below the tip.
		msg := wire.NewMsgHeaders()
		for _, header := range fixture.reorg[dstReorgFork+1:] {
			require.NoError(t, msg.AddBlockHeader(header))
		}
		bm.handleHeadersMsgWithPeers(
			peers, &headersMsg{peer: peer, headers: msg},
		)
		synctest.Wait()

		// The message is abandoned without a penalty, a retry is
		// scheduled, and the chain is unchanged.
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

// failingTipFilterStore fails filter tip reads while fail is set. It models
// a filter side outage while the block side stays healthy.
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
// peer list, ready for a hand-built pending commit. It also returns the
// filter store, whose tip reads the test can fail, and the sync peer.
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
// header, and restarts sync when that peer was already gone. The error path
// must not lose the punishment, or a peer could keep serving bad checkpoints.
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

				// Hand-build a checkpoint commit whose
				// mutation reports a failure.
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
				failed := chainstore.Result{
					Reply: chainstore.Reply{
						Err: errDSTWrite,
					},
				}
				bm.finishAsyncHeaderCommit(peers, failed)
				settleHeaderCommit(t, bm, peers)

				// The bad peer is dropped, and sync restarts if
				// it was the sync peer.
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
// side fails. The block chain is valid and usable, so a filter outage must
// not stop the node from following new headers.
func TestSnapshotFilterFailureKeepsHeaderIntake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, filters, peers, _ := newPendingCommitFixture(t)
		bm.pendingHeaderCommit = &pendingChainHeaderCommit{
			snapshot: true,
		}

		// Only the filter side fails during the snapshot.
		filters.fail = true
		bm.chainStore.TryAsk(
			t.Context(), chainstore.SnapshotRequest{},
			bm.chainStoreResult,
		)
		result := <-bm.chainStoreResult
		require.True(t, result.Reply.BlockTipValid)
		require.Error(t, result.Reply.Err)

		bm.finishAsyncHeaderCommit(peers, result)
		require.Nil(t, bm.pendingHeaderCommit,
			"header intake stayed frozen")
		require.EqualValues(t, 3, bm.headerList.Back().Height)
	})
}

// TestKnownPrefixDoesNotReadStoreBeforeActorWrite checks that a headers
// response that starts with known headers is handled without any store read
// on the handler goroutine before the actor write begins. A parent lookup
// there would block the handler behind a slow store, and the unrelated peer
// removal must complete while the write is held.
func TestKnownPrefixDoesNotReadStoreBeforeActorWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := newMemBlockHeaderStore(params)
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		fixture := dstChains()
		for i := 1; i <= 2; i++ {
			require.NoError(
				t,
				blocks.WriteHeaders(headerfs.BlockHeader{
					BlockHeader: fixture.honest[i],
					Height:      uint32(i),
				}),
			)
		}
		readFault := &rollbackParentReadFault{
			BlockHeaderStore: blocks,
			failHash:         fixture.honest[1].BlockHash(),
		}
		gate := &chainStoreGatedWrite{
			BlockHeaderStore: readFault,
			entered:          make(chan struct{}),
			release:          make(chan struct{}),
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
		defer close(gate.release)
		writer := newTestServerPeer(t, "127.0.0.1:18434", 3)
		spectator := newTestServerPeer(t, "127.0.0.1:18435", 3)
		bm.registerHeaderSyncPeer(spectator)
		bm.syncPeerMutex.Lock()
		bm.syncPeer = writer
		bm.syncPeerMutex.Unlock()
		message := wire.NewMsgHeaders()
		for _, header := range fixture.honest[1:4] {
			require.NoError(t, message.AddBlockHeader(header))
		}
		bm.QueueHeaders(message, writer)
		<-gate.entered

		// The parent lookup fault was never reached.
		require.False(t, readFault.failed,
			"known prefix should not read the store")

		// Peer removal completes while the write is held.
		bm.DonePeer(spectator)
		synctest.Wait()
		_, exists := bm.headerSync.Peer(spectator.Addr())
		require.False(
			t,
			exists,
			"known prefix must not send persistence back to the "+
				"handler",
		)
	})
}

// TestQueuedRangeReplyHeldCommit checks that a reply which arrived in time is
// not timed out while it waits behind a local store write. The request timer
// measures the peer's latency, not ours, so a slow commit must not get an
// honest peer disconnected.
func TestQueuedRangeReplyHeldCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeCheckpointAll})

		// Pick the requests for the first and the third range, which
		// belong to different peers.
		var first, second dstHeaderRequest
		for _, req := range d.transport.queued {
			if req.start == 0 {
				first = req
			}
			if req.start == 2 {
				second = req
			}
		}
		require.NotNil(t, first.peer)
		require.NotNil(t, second.peer)
		require.NotSame(t, first.peer, second.peer)
		gate := &chainStoreGatedWrite{
			BlockHeaderStore: d.store,
			entered:          make(chan struct{}),
			release:          make(chan struct{}),
		}
		d.bm.cfg.BlockHeaders = gate
		d.bm.Start()
		defer func() { require.NoError(t, d.bm.Stop()) }()

		// The first reply holds the store, and the second reply
		// queues behind it.
		d.bm.QueueHeaders(d.answer(first, dstReplyHonest), first.peer)
		<-gate.entered
		d.bm.QueueHeaders(d.answer(second, dstReplyHonest), second.peer)
		synctest.Wait()
		require.False(t, peerDisconnected(second.peer))

		// The request timeout passes while the write is still held.
		time.Sleep(dstRequestTimeout + time.Millisecond)
		synctest.Wait()
		require.False(
			t,
			peerDisconnected(second.peer),
			"timely queued reply was disconnected during local "+
				"write",
		)

		// Once the write finishes, the queued reply is applied.
		close(gate.release)
		synctest.Wait()
		_, height, err := d.store.ChainTip()
		require.NoError(t, err)
		require.GreaterOrEqual(t, height, second.stop)
		require.False(t, peerDisconnected(second.peer))
	})
}
