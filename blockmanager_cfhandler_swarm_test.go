package neutrino

import (
	"fmt"
	"math/rand"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
	"github.com/lightningnetwork/lnd/actor"
	"github.com/stretchr/testify/require"
)

// swarmBacklogSource lets a real subscription manager read the manager's
// backlog without consuming the notification deliberately held by the test.
type swarmBacklogSource struct{ *blockManager }

func (*swarmBacklogSource) Notifications() <-chan blockntfns.BlockNtfn {
	return nil
}

func isSwarmDisconnected(n blockntfns.BlockNtfn) bool {
	_, ok := n.(*blockntfns.Disconnected)
	return ok
}

// swarmFaultFilter applies a one-shot fault to a real filter store.
type swarmFaultFilter struct {
	headerfs.FilterHeaderStore
	mode         int
	writes       int
	faultBatch   int
	rollbackMode int
	rollbacks    int
	gateEntered  chan struct{}
	gateRelease  chan struct{}
	gateWrite    bool
}

func (s *swarmFaultFilter) WriteHeaders(
	headers ...headerfs.FilterHeader,
) error {

	if s.gateWrite && s.gateEntered != nil {
		close(s.gateEntered)
		s.gateEntered = nil
		<-s.gateRelease
	}
	mode := s.mode
	s.mode = 0
	if mode != 0 {
		s.writes++
		s.faultBatch = len(headers)
	}
	switch mode {
	case 1:
		return errDSTWrite
	case 2:
		if err := s.FilterHeaderStore.WriteHeaders(
			headers...,
		); err != nil {
			return err
		}
		return errDSTWrite
	case 3:
		if len(headers) > 1 {
			if err := s.FilterHeaderStore.WriteHeaders(
				headers[:len(headers)/2]...,
			); err != nil {
				return err
			}
		}
		return errDSTWrite
	default:
		return s.FilterHeaderStore.WriteHeaders(headers...)
	}
}

func (s *swarmFaultFilter) RollbackLastBlock(
	newTip *chainhash.Hash,
) (*headerfs.BlockStamp, error) {

	if !s.gateWrite && s.gateEntered != nil {
		close(s.gateEntered)
		s.gateEntered = nil
		<-s.gateRelease
	}
	mode := s.rollbackMode
	s.rollbackMode = 0
	if mode != 0 {
		s.rollbacks++
	}
	switch mode {
	case 1:
		return nil, errDSTWrite
	case 2:
		_, err := s.FilterHeaderStore.RollbackLastBlock(newTip)
		if err != nil {
			return nil, err
		}
		return nil, errDSTWrite
	default:
		return s.FilterHeaderStore.RollbackLastBlock(newTip)
	}
}

// swarmFaultBlocks injects a one-shot fault at a block-store mutation.
type swarmFaultBlocks struct {
	headerfs.BlockHeaderStore
	writeMode    int
	rollbackMode int
	writes       int
	faultBatch   int
	rollbacks    int
	gateEntered  chan struct{}
	gateRelease  chan struct{}
	gateWrite    bool
}

func (s *swarmFaultBlocks) WriteHeaders(headers ...headerfs.BlockHeader) error {
	if s.gateWrite && s.gateEntered != nil {
		close(s.gateEntered)
		s.gateEntered = nil
		<-s.gateRelease
	}
	if s.writeMode == 3 && len(headers) <= 1 {
		return s.BlockHeaderStore.WriteHeaders(headers...)
	}
	mode := s.writeMode
	s.writeMode = 0
	if mode != 0 {
		s.writes++
		s.faultBatch = len(headers)
	}
	switch mode {
	case 1:
		return errDSTWrite
	case 2:
		if err := s.BlockHeaderStore.WriteHeaders(
			headers...,
		); err != nil {
			return err
		}
		return errDSTWrite
	case 3:
		if len(headers) > 1 {
			if err := s.BlockHeaderStore.WriteHeaders(
				headers[:len(headers)/2]...,
			); err != nil {
				return err
			}
		}
		return errDSTWrite
	default:
		return s.BlockHeaderStore.WriteHeaders(headers...)
	}
}

func (s *swarmFaultBlocks) RollbackLastBlock() (*headerfs.BlockStamp, error) {
	if !s.gateWrite && s.gateEntered != nil {
		close(s.gateEntered)
		s.gateEntered = nil
		<-s.gateRelease
	}
	mode := s.rollbackMode
	s.rollbackMode = 0
	if mode != 0 {
		s.rollbacks++
	}
	switch mode {
	case 1:
		return nil, errDSTWrite
	case 2:
		_, err := s.BlockHeaderStore.RollbackLastBlock()
		if err != nil {
			return nil, err
		}
		return nil, errDSTWrite
	default:
		return s.BlockHeaderStore.RollbackLastBlock()
	}
}

// swarmPipeConn gives the outbound peer TCP-style addresses for its handshake.
type swarmPipeConn struct{ net.Conn }

func (c swarmPipeConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 18445}
}

func (c swarmPipeConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 18444}
}

// swarmRemoteHandshake speaks the remote side of the version handshake and
// drains subsequent peer messages until the connection closes.
func swarmRemoteHandshake(conn net.Conn, height int32) error {
	params := &chaincfg.SimNetParams
	msg, _, err := wire.ReadMessage(conn, wire.ProtocolVersion, params.Net)
	if err != nil {
		return err
	}
	if _, ok := msg.(*wire.MsgVersion); !ok {
		return fmt.Errorf("expected version, got %T", msg)
	}
	addr := wire.NewNetAddressIPPort(
		net.IPv4(127, 0, 0, 1), 18444, wire.SFNodeNetwork,
	)
	version := wire.NewMsgVersion(addr, addr, 1, height)
	version.Services = wire.SFNodeNetwork
	if err := wire.WriteMessage(
		conn, version, wire.ProtocolVersion, params.Net,
	); err != nil {
		return err
	}
	for {
		msg, _, err = wire.ReadMessage(
			conn, wire.ProtocolVersion, params.Net,
		)
		if err != nil {
			return err
		}
		if _, ok := msg.(*wire.MsgVerAck); ok {
			break
		}
	}
	if err := wire.WriteMessage(
		conn, wire.NewMsgVerAck(), wire.ProtocolVersion, params.Net,
	); err != nil {
		return err
	}
	for {
		if _, _, err := wire.ReadMessage(
			conn, wire.ProtocolVersion, params.Net,
		); err != nil {
			return nil
		}
	}
}

// swarmConnectedPeer performs an actual version handshake before entering the
// synctest bubble, so the block manager can register it as a candidate.
func swarmConnectedPeer(t *testing.T, height int32) *ServerPeer {
	t.Helper()
	cfg := peer.Config{
		ChainParams:    &chaincfg.SimNetParams,
		Services:       wire.SFNodeNetwork,
		AllowSelfConns: true,
		NewestBlock: func() (*chainhash.Hash, int32, error) {
			return &chainhash.Hash{}, height, nil
		},
	}
	outgoing, err := peer.NewOutboundPeer(&cfg, "127.0.0.1:18444")
	require.NoError(t, err)
	local, remote := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- swarmRemoteHandshake(remote, height) }()
	outgoing.AssociateConnection(swarmPipeConn{local})
	t.Cleanup(func() {
		outgoing.Disconnect()
		_ = remote.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("remote handshake: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("remote peer did not exit")
		}
		joined := make(chan struct{})
		go func() { outgoing.WaitForDisconnect(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("outbound peer did not exit")
		}
	})
	require.Eventually(
		t, outgoing.VerAckReceived, 5*time.Second, time.Millisecond,
	)
	require.NotZero(t, outgoing.Services()&wire.SFNodeNetwork)
	require.Equal(t, height, outgoing.LastBlock())
	return &ServerPeer{Peer: outgoing}
}

// TestCFHandlerSwarmPipeline runs both production handlers through Start and
// QueueHeaders with a request-bound honest filter source.
func TestCFHandlerSwarmPipeline(t *testing.T) {
	for seed := int64(0); seed < 15; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			var connected *ServerPeer
			if seed == 13 {
				connected = swarmConnectedPeer(
					t,
					int32(len(dstChains().reorg)-1),
				)
			}
			synctest.Test(
				t,
				func(t *testing.T) {
					runSwarmPipeline(t, seed, connected)
				},
			)
		})
	}
}

func runSwarmPipeline(t *testing.T, seed int64, connected *ServerPeer) {
	rng := rand.New(rand.NewSource(seed))
	t.Logf("replay seed=%d filter_write_fault=%d", seed, seed%4)
	params := chaincfg.SimNetParams
	params.Checkpoints = nil
	fixture := dstChains()
	blocks := &swarmFaultBlocks{
		BlockHeaderStore: newMemBlockHeaderStore(&params),
	}
	underlying, err := newMemFilterHeaderStore(&params)
	filters := &swarmFaultFilter{
		FilterHeaderStore: underlying,
		mode:              int(seed % 4),
	}
	require.NoError(t, err)
	var initial []headerfs.BlockHeader
	for h := uint32(1); h <= dstChainHeight; h++ {
		initial = append(
			initial,
			headerfs.BlockHeader{
				BlockHeader: fixture.honest[h],
				Height:      h,
			},
		)
	}
	require.NoError(t, blocks.WriteHeaders(initial...))
	genesis, _, err := filters.ChainTip()
	require.NoError(t, err)
	hashes := make(map[chainhash.Hash]chainhash.Hash)
	headers := map[chainhash.Hash]chainhash.Hash{
		fixture.honest[0].BlockHash(): *genesis,
	}
	for branchIndex, branch := range [][]*wire.BlockHeader{
		fixture.honest, fixture.reorg,
	} {
		seen := make(map[chainhash.Hash]bool)
		for offset, block := range branch[1:] {
			id := block.BlockHash()
			require.False(
				t,
				seen[id],
				"duplicate block hash in branch",
			)
			seen[id] = true
			_, exists := hashes[id]
			if exists {
				require.Equal(t, 1, branchIndex)
				require.LessOrEqual(t, offset+1, dstReorgFork)
				require.Equal(
					t,
					fixture.honest[offset+1].BlockHash(),
					id,
				)
				continue
			}
			filter := chainhash.DoubleHashH(id[:])
			previous := headers[block.PrevBlock]
			hashes[id] = filter
			headers[id] = chainhash.DoubleHashH(
				append(filter[:], previous[:]...),
			)
		}
	}
	bm := newCFHeaderTestManager(t, blocks, filters)
	bm.cfg.ChainParams = params
	bm.cfg.TimeSource = dstMedianTime{
		MedianTimeSource: bm.cfg.TimeSource,
		offset: time.Until(
			params.GenesisBlock.Header.Timestamp.Add(time.Hour),
		),
	}
	ready := make(chan struct{})
	close(ready)
	bm.cfg.firstPeerSignal = ready
	drops := int(seed % 2)
	bm.cfg.Broadcaster = broadcastFunc(
		func(
			b *querysyncwm.Broadcast,
			opts ...querysyncwm.QueryOption,
		) error {

			time.Sleep(
				time.Duration(rng.Intn(5)) * time.Millisecond,
			)
			if drops > 0 {
				drops--
				return errDSTWrite
			}
			request, ok := b.Msg.(*wire.MsgGetCFHeaders)
			require.True(t, ok)
			branch := fixture.honest
			for _, block := range fixture.reorg[dstReorgFork+1:] {
				if block.BlockHash() == request.StopHash {
					branch = fixture.reorg
					break
				}
			}
			stop := -1
			for i, block := range branch {
				if block.BlockHash() == request.StopHash {
					stop = i
					break
				}
			}
			require.GreaterOrEqual(
				t,
				stop,
				int(request.StartHeight),
			)
			predecessor := branch[request.StartHeight-1].BlockHash()
			prev := headers[predecessor]
			response := &wire.MsgCFHeaders{
				FilterType:       request.FilterType,
				StopHash:         request.StopHash,
				PrevFilterHeader: prev,
			}
			selected := branch[request.StartHeight : stop+1]
			for _, block := range selected {
				hash := hashes[block.BlockHash()]
				response.FilterHashes = append(
					response.FilterHashes,
					&hash,
				)
			}
			return newScriptedBroadcaster(
				map[string][]wire.Message{"honest": {response}},
			).Broadcast(b, opts...)
		},
	)
	notifications := make(chan blockntfns.BlockNtfn, 100)
	notificationEntered, notificationRelease := make(
		chan struct{},
	), make(
		chan struct{},
	)
	notificationArmed := false
	go func() {
		for {
			select {
			case n := <-bm.blockNtfnChan:
				if notificationArmed {
					disconnected := isSwarmDisconnected(n)
					if disconnected {
						notificationArmed = false
						close(notificationEntered)
						<-notificationRelease
					}
				}
				notifications <- n
			case <-bm.quit:
				return
			}
		}
	}()
	nextNotification := func() blockntfns.BlockNtfn {
		select {
		case n := <-notifications:
			return n
		default:
			t.Fatal("expected notification was not delivered")
			return nil
		}
	}
	checkDurable := func() {
		_, bh, err := blocks.ChainTip()
		require.NoError(t, err)
		_, fh, err := filters.ChainTip()
		require.NoError(t, err)
		require.LessOrEqual(t, fh, bh)
		for h := uint32(1); h <= fh; h++ {
			block, err := blocks.FetchHeaderByHeight(h)
			require.NoError(t, err)
			id := block.BlockHash()
			got, err := filters.FetchHeader(&id)
			require.NoError(t, err)
			require.Equal(t, headers[id], *got)
		}
		bm.newFilterHeadersMtx.RLock()
		published := bm.filterHeaderTip
		publishedHash := bm.filterHeaderTipHash
		bm.newFilterHeadersMtx.RUnlock()
		require.LessOrEqual(t, published, fh)
		block, err := blocks.FetchHeaderByHeight(published)
		require.NoError(t, err)
		require.Equal(t, block.BlockHash(), publishedHash)
	}
	bm.Start()
	var spectator *ServerPeer
	if seed == 11 || seed == 12 {
		spectator = newTestServerPeer(
			t,
			"127.0.0.1:18447",
			int32(dstChainHeight),
		)
		bm.registerHeaderSyncPeer(spectator)
		_, registered := bm.headerSync.Peer(spectator.Addr())
		require.True(t, registered)
	}
	defer func() { require.NoError(t, bm.Stop()) }()
	synctest.Wait()
	checkDurable()
	time.Sleep(4 * retryTimeout)
	synctest.Wait()
	_, height, err := filters.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(dstChainHeight), height)
	peer := newTestServerPeer(t, "127.0.0.1:18444", dstChainHeight+1)
	bm.syncPeer = peer
	msg := wire.NewMsgHeaders()
	require.NoError(t, msg.AddBlockHeader(fixture.honest[dstChainHeight+1]))
	if seed == 10 {
		filters.gateEntered = make(chan struct{})
		filters.gateRelease = make(chan struct{})
		filters.gateWrite = true
	}
	bm.QueueHeaders(msg, peer)
	synctest.Wait()
	if seed == 10 {
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		release := filters.gateRelease
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		require.Nil(
			t,
			filters.gateEntered,
			"filter write gate was not reached",
		)
		checkDurable()
		queued := make(chan struct{})
		go func() {
			bm.QueueHeaders(wire.NewMsgHeaders(), peer)
			close(queued)
		}()
		synctest.Wait()
		close(release)
		synctest.Wait()
		<-queued
	}
	checkDurable()
	time.Sleep(4 * retryTimeout)
	synctest.Wait()
	_, height, err = filters.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(dstChainHeight+1), height)
	checkDurable()
	for h := uint32(1); h <= height; h++ {
		block, err := blocks.FetchHeaderByHeight(h)
		require.NoError(t, err)
		id := block.BlockHash()
		got, err := filters.FetchHeader(&id)
		require.NoError(t, err)
		require.Equal(t, headers[id], *got)
		n := nextNotification()
		require.Equal(t, h, n.Height())
		notified := n.Header()
		require.Equal(t, id, notified.BlockHash())
	}
	require.Empty(t, notifications)
	replacement := newTestServerPeer(
		t,
		"127.0.0.1:18445",
		int32(len(fixture.reorg)-1),
	)
	if seed == 13 {
		replacement = connected
	}
	bm.syncPeer = replacement
	reorg := wire.NewMsgHeaders()
	for _, block := range fixture.reorg[dstReorgFork+1:] {
		require.NoError(t, reorg.AddBlockHeader(block))
	}
	if seed <= 3 {
		blocks.writeMode = int(seed)
	}
	switch seed {
	case 4:
		filters.rollbackMode = 1
	case 5:
		blocks.rollbackMode = 1
	case 6:
		filters.rollbackMode, blocks.rollbackMode = 2, 2
	case 7:
		filters.gateEntered, filters.gateRelease = make(
			chan struct{},
		), make(
			chan struct{},
		)
	case 8:
		blocks.gateEntered, blocks.gateRelease = make(
			chan struct{},
		), make(
			chan struct{},
		)
	case 9, 12:
		blocks.gateEntered, blocks.gateRelease, blocks.gateWrite = make(
			chan struct{},
		), make(
			chan struct{},
		), true
	case 13:
		blocks.writeMode = 1
	}
	var retryTransport *dstHeaderTransport
	requestsBeforeFault := 0
	if seed == 13 {
		retryTransport = &dstHeaderTransport{
			chain:   fixture.reorg,
			blocked: make(map[string]bool),
			cap:     wire.MaxBlockHeadersPerMsg,
			best:    uint32(len(fixture.reorg) - 1),
		}
		bm.cfg.HeaderTransport = retryTransport
		bm.NewPeer(replacement)
		synctest.Wait()
		_, registered := bm.headerSync.Peer(replacement.Addr())
		require.True(t, registered, "real peer must be registered")
		requestsBeforeFault = retryTransport.sent
	}
	if seed == 11 || seed == 14 {
		notificationArmed = true
	}
	bm.QueueHeaders(reorg, replacement)
	synctest.Wait()
	if seed == 14 {
		defer func() {
			select {
			case <-notificationRelease:
			default:
				close(notificationRelease)
			}
		}()
		select {
		case <-notificationEntered:
		default:
			t.Fatal("notification gate was not reached")
		}
		stopped := make(chan error, 1)
		go func() { stopped <- bm.Stop() }()
		synctest.Wait()
		select {
		case err := <-stopped:
			require.NoError(t, err)
		default:
			t.Fatal(
				"shutdown did not join while notification " +
					"delivery was held",
			)
		}
		_, before, err := blocks.ChainTip()
		require.NoError(t, err)
		close(notificationRelease)
		synctest.Wait()
		_, after, err := blocks.ChainTip()
		require.NoError(t, err)
		require.Equal(
			t,
			before,
			after,
			"store changed after shutdown joined",
		)
		return
	}
	if seed == 11 {
		defer func() {
			select {
			case <-notificationRelease:
			default:
				close(notificationRelease)
			}
		}()
		select {
		case <-notificationEntered:
		default:
			t.Fatal("notification gate was not reached")
		}
		checkDurable()
		var pending []actor.Future[chainStoreReply]
		for i := 0; i < chainStoreMailboxCapacity; i++ {
			pending = append(pending, bm.chainStore.ref.TryAsk(
				t.Context(), chainStoreSnapshotRequest{},
			))
			synctest.Wait()
		}
		full := bm.chainStore.ref.TryAsk(
			t.Context(), chainStoreSnapshotRequest{},
		).
			Await(t.Context())
		require.ErrorIs(t, full.Err(), actor.ErrMailboxFull)
		go bm.DonePeer(spectator)
		synctest.Wait()
		_, registered := bm.headerSync.Peer(spectator.Addr())
		require.False(
			t,
			registered,
			"peer removal must complete while notification "+
				"delivery is held",
		)
		backlogDone := make(chan error, 1)
		go func() {
			_, _, err := bm.NotificationsSinceHeight(1)
			backlogDone <- err
		}()
		synctest.Wait()
		select {
		case err := <-backlogDone:
			require.NoError(t, err)
		default:
			t.Fatal(
				"backlog read blocked while notification " +
					"delivery was held",
			)
		}
		manager := blockntfns.NewSubscriptionManager(
			&swarmBacklogSource{bm},
		)
		manager.Start()
		defer manager.Stop()
		subDone := make(chan error, 1)
		go func() {
			_, err := manager.NewSubscription(1)
			subDone <- err
		}()
		synctest.Wait()
		select {
		case err := <-subDone:
			require.NoError(t, err)
		default:
			t.Fatal(
				"subscription registration blocked while " +
					"notification delivery was held",
			)
		}
		queued := make(chan struct{})
		go func() {
			bm.QueueHeaders(wire.NewMsgHeaders(), replacement)
			close(queued)
		}()
		synctest.Wait()
		close(notificationRelease)
		synctest.Wait()
		<-queued
		for _, request := range pending {
			pendingErr := request.Await(t.Context()).Err()
			if pendingErr != nil {
				require.ErrorIs(
					t, pendingErr, actor.ErrMailboxFull,
				)
			}
		}
	}
	if seed == 7 || seed == 8 || seed == 9 || seed == 12 {
		entered, release := filters.gateEntered, filters.gateRelease
		if seed == 8 || seed == 9 || seed == 12 {
			entered = blocks.gateEntered
			release = blocks.gateRelease
		}
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		require.Nil(t, entered, "store gate was not reached")
		checkDurable()
		if seed == 12 {
			go bm.DonePeer(spectator)
			synctest.Wait()
			_, registered := bm.headerSync.Peer(spectator.Addr())
			require.False(
				t,
				registered,
				"peer removal must complete while store "+
					"mutation is held",
			)
		}
		queued := make(chan struct{})
		go func() {
			bm.QueueHeaders(wire.NewMsgHeaders(), replacement)
			close(queued)
		}()
		synctest.Wait()
		close(release)
		synctest.Wait()
		<-queued
	}
	if seed == 13 {
		// Only requests produced by the manager may trigger a reply.
		for step := 0; step < 8; step++ {
			time.Sleep(retryTimeout)
			synctest.Wait()
			request, ok := retryTransport.take(replacement)
			if !ok {
				continue
			}
			response := wire.NewMsgHeaders()
			for h := request.start + 1; h <= request.stop; h++ {
				require.NoError(
					t,
					response.AddBlockHeader(
						fixture.reorg[h],
					),
				)
			}
			bm.QueueHeaders(response, replacement)
			synctest.Wait()
		}
		t.Logf(
			"manager outbound header requests=%d before_fault=%d",
			retryTransport.sent,
			requestsBeforeFault,
		)
		// A retained validated operation may complete without another
		// request.
	}
	// Resending the identical response checks replay safety. Seed 13
	// supplies no unsolicited response and tests autonomous recovery.
	for attempt := 0; seed != 13 && attempt < 8; attempt++ {
		checkDurable()
		_, blockHeight, err := blocks.ChainTip()
		require.NoError(t, err)
		if blockHeight == uint32(len(fixture.reorg)-1) {
			break
		}
		bm.QueueHeaders(reorg, replacement)
		synctest.Wait()
	}
	checkDurable()
	time.Sleep(4 * retryTimeout)
	synctest.Wait()
	checkDurable()
	if seed >= 1 && seed <= 3 || seed == 13 {
		require.Equal(
			t,
			1,
			blocks.writes,
			"block write fault was not reached",
		)
	}
	if seed == 3 {
		require.Greater(
			t,
			blocks.faultBatch,
			1,
			"partial write needs a multi-header batch",
		)
	}
	if seed == 4 || seed == 6 {
		require.Equal(
			t,
			1,
			filters.rollbacks,
			"filter rollback fault was not reached",
		)
	}
	if seed == 5 || seed == 6 {
		require.Equal(
			t,
			1,
			blocks.rollbacks,
			"block rollback fault was not reached",
		)
	}
	_, height, err = filters.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(len(fixture.reorg)-1), height)
	for h := uint32(dstChainHeight + 1); h > dstReorgFork; h-- {
		n := nextNotification()
		_, ok := n.(*blockntfns.Disconnected)
		require.True(t, ok)
		require.Equal(t, h, n.Height())
		header := n.Header()
		require.Equal(
			t,
			fixture.honest[h].BlockHash(),
			header.BlockHash(),
		)
	}
	for h := uint32(dstReorgFork + 1); h <= height; h++ {
		n := nextNotification()
		_, ok := n.(*blockntfns.Connected)
		require.True(t, ok)
		require.Equal(t, h, n.Height())
		header := n.Header()
		require.Equal(
			t,
			fixture.reorg[h].BlockHash(),
			header.BlockHash(),
		)
	}
	require.Empty(t, notifications)
}

// TestCFHandlerSwarmCheckpointNoWork exercises startup at a complete filter
// checkpoint without issuing a zero-length range request.
func TestCFHandlerSwarmCheckpointNoWork(t *testing.T) {
	synctest.Test(t, runSwarmCheckpointNoWork)
}

func runSwarmCheckpointNoWork(t *testing.T) {
	params := chaincfg.SimNetParams
	params.Checkpoints = nil
	blocks := newMemBlockHeaderStore(&params)
	filters, err := newMemFilterHeaderStore(&params)
	require.NoError(t, err)
	genesis := params.GenesisBlock.Header
	chain := dstExtend(
		&genesis,
		int(wire.CFCheckptInterval),
		time.Second,
	)
	previous, _, err := filters.ChainTip()
	require.NoError(t, err)
	filterHeader := *previous
	for i, block := range chain {
		height := uint32(i + 1)
		require.NoError(
			t,
			blocks.WriteHeaders(
				headerfs.BlockHeader{
					BlockHeader: block,
					Height:      height,
				},
			),
		)
		id := block.BlockHash()
		hash := chainhash.DoubleHashH(id[:])
		filterHeader = chainhash.DoubleHashH(
			append(hash[:], filterHeader[:]...),
		)
		require.NoError(
			t,
			filters.WriteHeaders(
				headerfs.FilterHeader{
					HeaderHash: id,
					Height:     height,
					FilterHash: filterHeader,
				},
			),
		)
	}
	bm := newCFHeaderTestManager(t, blocks, filters)
	bm.cfg.ChainParams = params
	bm.cfg.TimeSource = dstMedianTime{
		MedianTimeSource: bm.cfg.TimeSource,
		offset: time.Until(
			genesis.Timestamp.Add(time.Hour),
		),
	}
	ready := make(chan struct{})
	close(ready)
	bm.cfg.firstPeerSignal = ready
	requests := 0
	bm.cfg.Broadcaster = broadcastFunc(
		func(
			b *querysyncwm.Broadcast,
			options ...querysyncwm.QueryOption,
		) error {

			var response wire.Message
			switch request := b.Msg.(type) {
			case *wire.MsgGetCFCheckpt:
				requests++
				response = &wire.MsgCFCheckpt{
					FilterType: request.FilterType,
					StopHash:   request.StopHash,
					FilterHeaders: []*chainhash.Hash{
						&filterHeader,
					},
				}
			case *wire.MsgGetCFHeaders:
				require.Equal(
					t,
					uint32(wire.CFCheckptInterval+1),
					request.StartHeight,
				)
				hash := chainhash.DoubleHashH(
					request.StopHash[:],
				)
				response = &wire.MsgCFHeaders{
					FilterType:       request.FilterType,
					StopHash:         request.StopHash,
					PrevFilterHeader: filterHeader,
					FilterHashes: []*chainhash.Hash{
						&hash,
					},
				}
			default:
				t.Fatalf("unexpected request %T", b.Msg)
			}
			return newScriptedBroadcaster(
				map[string][]wire.Message{
					"honest": {response},
				},
			).Broadcast(b, options...)
		},
	)
	go func() {
		for {
			select {
			case <-bm.blockNtfnChan:
			case <-bm.quit:
				return
			}
		}
	}()
	bm.Start()
	defer func() { require.NoError(t, bm.Stop()) }()
	synctest.Wait()
	require.Equal(t, 1, requests)
	_, height, err := filters.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(wire.CFCheckptInterval), height)
	next := dstExtend(chain[len(chain)-1], 1, time.Second)[0]
	peer := newTestServerPeer(
		t,
		"127.0.0.1:18446",
		int32(wire.CFCheckptInterval+1),
	)
	bm.syncPeer = peer
	message := wire.NewMsgHeaders()
	require.NoError(t, message.AddBlockHeader(next))
	bm.QueueHeaders(message, peer)
	synctest.Wait()
	_, height, err = filters.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(wire.CFCheckptInterval+1), height)
}
