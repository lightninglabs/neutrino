package neutrino

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/integration/rpctest"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/lightninglabs/neutrino/headersync"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
	"github.com/lightninglabs/neutrino/sqldb"
	"github.com/stretchr/testify/require"
)

const (
	// maxHeight is the height we will generate filter headers up to. We use
	// an odd number of checkpoints to ensure we can test cases where the
	// block manager is only able to fetch filter headers for one checkpoint
	// interval rather than two.
	maxHeight = 21 * uint32(wire.CFCheckptInterval)

	dbOpenTimeout = time.Second * 10
)

// mockDispatcher implements the querysyncwm.Dispatcher interface and allows us
// to set up a custom Query method during tests.
type mockDispatcher struct {
	querysyncwm.WorkManager

	query func(requests []*querysyncwm.Request,
		options ...querysyncwm.QueryOption) chan error
}

var _ querysyncwm.Dispatcher = (*mockDispatcher)(nil)

func (m *mockDispatcher) Query(requests []*querysyncwm.Request,
	options ...querysyncwm.QueryOption) chan error {

	return m.query(requests, options...)
}

func TestHeaderSyncValidationChain(t *testing.T) {
	t.Parallel()

	base := headerlist.NewBoundedMemoryChain(4)
	base.ResetHeaderState(headerlist.Node{
		Height: 10,
		Header: wire.BlockHeader{Nonce: 10},
	})
	baseTip := base.PushBack(headerlist.Node{
		Height: 11,
		Header: wire.BlockHeader{Nonce: 11},
	})

	validationChain := newHeaderSyncValidationChain(base, *baseTip)
	pendingTip := validationChain.PushBack(headerlist.Node{
		Height: 12,
		Header: wire.BlockHeader{Nonce: 12},
	})

	require.Equal(t, int32(12), validationChain.Back().Height)
	require.Equal(t, uint32(12), validationChain.Back().Header.Nonce)
	require.Equal(t, pendingTip, validationChain.Back())

	pending, ok := validationChain.AtHeight(12)
	require.True(t, ok)
	require.Equal(t, uint32(12), pending.Header.Nonce)

	cached, ok := validationChain.AtHeight(10)
	require.True(t, ok)
	require.Equal(t, uint32(10), cached.Header.Nonce)

	_, ok = validationChain.AtHeight(9)
	require.False(t, ok)
}

// setupBlockManager initialises a blockManager to be used in tests.
func setupBlockManager(t *testing.T) (*blockManager, headerfs.BlockHeaderStore,
	headerfs.FilterHeaderStore, error) {

	// Set up the block and filter header stores.
	tempDir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", tempDir+"/weks.db", true, dbOpenTimeout, false,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error opening DB: %s", err)
	}

	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	hdrStore, err := headerfs.NewBlockHeaderStore(
		tempDir, db, &chaincfg.SimNetParams,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error creating block "+
			"header store: %s", err)
	}

	cfStore, err := headerfs.NewFilterHeaderStore(
		tempDir, db, headerfs.RegularFilter, &chaincfg.SimNetParams,
		nil,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error creating filter "+
			"header store: %s", err)
	}

	// Set up a blockManager with the chain service we defined.
	bm, err := newBlockManager(&blockManagerCfg{
		ChainParams:      chaincfg.SimNetParams,
		BlockHeaders:     hdrStore,
		RegFilterHeaders: cfStore,
		QueryDispatcher:  &mockDispatcher{},
		TimeSource:       blockchain.NewMedianTime(),
		BanPeer: func(string, banman.Reason) error {
			return nil
		},
		DisconnectPeer: func(string) error {
			return nil
		},
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to create "+
			"blockmanager: %v", err)
	}
	t.Cleanup(func() { _ = bm.Stop() })

	return bm, hdrStore, cfStore, nil
}

func TestSyncPeerCandidateOrdering(t *testing.T) {
	highHeight := syncPeerCandidate{
		height: 100,
		rtt:    500 * time.Millisecond,
		addr:   "peer-b",
	}
	lowHeight := syncPeerCandidate{
		height: 99,
		rtt:    10 * time.Millisecond,
		addr:   "peer-a",
	}
	require.True(t, syncPeerOrdersBefore(highHeight, lowHeight))

	fast := syncPeerCandidate{
		height: 100,
		rtt:    20 * time.Millisecond,
		addr:   "peer-b",
	}
	slow := syncPeerCandidate{
		height: 100,
		rtt:    200 * time.Millisecond,
		addr:   "peer-a",
	}
	require.True(t, syncPeerOrdersBefore(fast, slow))

	knownRTT := syncPeerCandidate{
		height: 100,
		rtt:    20 * time.Millisecond,
		addr:   "peer-b",
	}
	unknownRTT := syncPeerCandidate{
		height: 100,
		addr:   "peer-a",
	}
	require.True(t, syncPeerOrdersBefore(knownRTT, unknownRTT))

	addrA := syncPeerCandidate{
		height: 100,
		addr:   "peer-a",
	}
	addrB := syncPeerCandidate{
		height: 100,
		addr:   "peer-b",
	}
	require.True(t, syncPeerOrdersBefore(addrA, addrB))
}

func TestCheckpointedCFHeadersBatchTimeoutScales(t *testing.T) {
	t.Parallel()

	require.Equal(
		t, checkpointedCFHeadersMinBatchTimeout,
		checkpointedCFHeadersBatchTimeout(1),
	)

	scaledQueries := int(
		(checkpointedCFHeadersMinBatchTimeout /
			checkpointedCFHeadersPerQueryTimeout) + 1,
	)
	require.Equal(
		t, time.Duration(scaledQueries)*
			checkpointedCFHeadersPerQueryTimeout,
		checkpointedCFHeadersBatchTimeout(scaledQueries),
	)

	hugeQueries := int(
		(checkpointedCFHeadersMaxBatchTimeout /
			checkpointedCFHeadersPerQueryTimeout) + 1,
	)
	require.Equal(
		t, checkpointedCFHeadersMaxBatchTimeout,
		checkpointedCFHeadersBatchTimeout(hugeQueries),
	)
}

func TestWriteCFHeadersMsgPopulatesSQLRows(t *testing.T) {
	t.Parallel()

	backend := sqldb.NewTestBackend(t)
	ctx := t.Context()
	blockStore, err := headerfs.NewSQLBlockHeaderStore(
		ctx, backend.HeaderTxer, &chaincfg.SimNetParams,
	)
	require.NoError(t, err)
	filterStore, err := headerfs.NewSQLFilterHeaderStore(
		ctx, backend.HeaderTxer, headerfs.RegularFilter,
		&chaincfg.SimNetParams, nil,
	)
	require.NoError(t, err)

	bm, err := newBlockManager(&blockManagerCfg{
		ChainParams:      chaincfg.SimNetParams,
		BlockHeaders:     blockStore,
		RegFilterHeaders: filterStore,
		QueryDispatcher:  &mockDispatcher{},
		TimeSource:       blockchain.NewMedianTime(),
		BanPeer: func(string, banman.Reason) error {
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bm.Stop() })
	ntfnDone := make(chan struct{})
	defer close(ntfnDone)
	go func() {
		for {
			select {
			case <-bm.blockNtfnChan:
			case <-ntfnDone:
				return
			}
		}
	}()

	const numHeaders = 3
	blockHeaders := make([]headerfs.BlockHeader, 0, numHeaders)
	prevHeader := &chaincfg.SimNetParams.GenesisBlock.Header
	for height := uint32(1); height <= numHeaders; height++ {
		header := &wire.BlockHeader{
			Bits:      uint32(rand.Int31()),
			Nonce:     uint32(rand.Int31()),
			Timestamp: prevHeader.Timestamp.Add(time.Minute),
			PrevBlock: prevHeader.BlockHash(),
		}
		blockHeaders = append(blockHeaders, headerfs.BlockHeader{
			BlockHeader: header,
			Height:      height,
		})
		prevHeader = header
	}
	require.NoError(t, blockStore.WriteHeaders(blockHeaders...))

	prevFilterHeader, _, err := filterStore.ChainTip()
	require.NoError(t, err)

	filterHashValues := []chainhash.Hash{
		chainhash.DoubleHashH([]byte("filter-1")),
		chainhash.DoubleHashH([]byte("filter-2")),
		chainhash.DoubleHashH([]byte("filter-3")),
	}
	filterHashes := make([]*chainhash.Hash, len(filterHashValues))
	for i := range filterHashValues {
		filterHashes[i] = &filterHashValues[i]
	}

	var expectedFilterHeaders []chainhash.Hash
	lastFilterHeader := *prevFilterHeader
	for _, filterHash := range filterHashes {
		lastFilterHeader = chainhash.DoubleHashH(
			append(filterHash[:], lastFilterHeader[:]...),
		)
		expectedFilterHeaders = append(
			expectedFilterHeaders, lastFilterHeader,
		)
	}

	stopHash := blockHeaders[len(blockHeaders)-1].BlockHash()
	tip, tipHeight, err := bm.writeCFHeadersMsgs(
		[]*wire.MsgCFHeaders{{
			FilterType:       wire.GCSFilterRegular,
			StopHash:         blockHeaders[1].BlockHash(),
			PrevFilterHeader: *prevFilterHeader,
			FilterHashes:     filterHashes[:2],
		}, {
			FilterType:       wire.GCSFilterRegular,
			StopHash:         stopHash,
			PrevFilterHeader: expectedFilterHeaders[1],
			FilterHashes:     filterHashes[2:],
		}},
		filterStore,
	)
	require.NoError(t, err)
	require.Equal(t, uint32(numHeaders), tipHeight)
	require.Equal(
		t,
		expectedFilterHeaders[len(expectedFilterHeaders)-1],
		*tip,
	)

	for i, expected := range expectedFilterHeaders {
		height := uint32(i + 1)
		got, err := filterStore.FetchHeaderByHeight(height)
		require.NoError(t, err)
		require.Equal(t, expected, *got)
	}

	ancestors, startHeight, err := filterStore.FetchHeaderAncestors(
		numHeaders-1, &stopHash,
	)
	require.NoError(t, err)
	require.Equal(t, uint32(1), startHeight)
	require.Equal(t, expectedFilterHeaders, ancestors)
}

func newTestServerPeer(t *testing.T, addr string,
	lastBlock int32) *ServerPeer {

	pp, err := peer.NewOutboundPeer(&peer.Config{
		ChainParams:      &chaincfg.SimNetParams,
		Services:         wire.SFNodeNetwork,
		UserAgentName:    "neutrino-test",
		UserAgentVersion: "0.0.0",
	}, addr)
	require.NoError(t, err)

	sp := &ServerPeer{
		Peer: pp,
	}
	sp.UpdateLastBlockHeight(lastBlock)

	return sp
}

// scriptedPeer is a querysyncwm.Peer that answers every message queued to it
// with a fixed script of responses.
type scriptedPeer struct {
	addr      string
	responses []wire.Message
	recv      chan wire.Message
	quit      chan struct{}

	// first ensures the script only plays in answer to the first message.
	first sync.Once

	mu     sync.Mutex
	queued []wire.Message
}

var _ querysyncwm.Peer = (*scriptedPeer)(nil)

func (p *scriptedPeer) QueueMessageWithEncoding(msg wire.Message,
	_ chan<- struct{}, _ wire.MessageEncoding) {

	p.mu.Lock()
	p.queued = append(p.queued, msg)
	p.mu.Unlock()

	p.first.Do(func() {
		go func() {
			for _, resp := range p.responses {
				select {
				case p.recv <- resp:
				case <-p.quit:
					return
				}
			}
		}()
	})
}

// numQueued returns the number of messages queued to the peer so far.
func (p *scriptedPeer) numQueued() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.queued)
}

func (p *scriptedPeer) SubscribeRecvMsg() (<-chan wire.Message, func()) {
	return p.recv, func() {}
}

func (p *scriptedPeer) Addr() string { return p.addr }

func (p *scriptedPeer) LastPingMicros() int64 { return 0 }

func (p *scriptedPeer) OnDisconnect() <-chan struct{} { return p.quit }

// broadcastFunc adapts a function to the querysyncwm.Broadcaster interface.
type broadcastFunc func(*querysyncwm.Broadcast,
	...querysyncwm.QueryOption) error

// Broadcast calls the underlying function.
func (f broadcastFunc) Broadcast(b *querysyncwm.Broadcast,
	opts ...querysyncwm.QueryOption) error {

	return f(b, opts...)
}

// newScriptedBroadcaster returns a Broadcaster backed by the real work
// manager, whose peers answer a broadcast with the scripted responses for
// their address.
func newScriptedBroadcaster(
	scripts map[string][]wire.Message) querysyncwm.WorkManager {

	peers := newScriptedPeers(scripts)

	return querysyncwm.NewWorkManager(&querysyncwm.Config{
		Peers: func() []querysyncwm.Peer {
			out := make([]querysyncwm.Peer, len(peers))
			for i, p := range peers {
				out[i] = p
			}

			return out
		},
	})
}

// newScriptedPeers creates a peer for each address in the scripts.
func newScriptedPeers(scripts map[string][]wire.Message) []*scriptedPeer {
	peers := make([]*scriptedPeer, 0, len(scripts))
	for addr, responses := range scripts {
		peers = append(peers, &scriptedPeer{
			addr:      addr,
			responses: responses,
			recv:      make(chan wire.Message),
			quit:      make(chan struct{}),
		})
	}

	return peers
}

func TestCFHeadersCollectsHonestConflict(t *testing.T) {
	t.Parallel()

	stop := &wire.BlockHeader{Nonce: 1}
	store := &headerfs.MockBlockHeaderStore{}
	store.On("ChainTip").Return(stop, uint32(1), nil)
	stopHash := stop.BlockHash()

	// Keep collecting timely honest conflicts: the last peer disagrees
	// with the other three.
	scripts := make(map[string][]wire.Message)
	for i := 0; i < 4; i++ {
		hash := chainhash.Hash{1}
		if i == 3 {
			hash = chainhash.Hash{2}
		}

		scripts[fmt.Sprintf("127.0.0.%d:8333", i+1)] = []wire.Message{
			&wire.MsgCFHeaders{
				FilterType:   wire.GCSFilterRegular,
				StopHash:     stopHash,
				FilterHashes: []*chainhash.Hash{&hash},
			},
		}
	}

	bm := &blockManager{cfg: &blockManagerCfg{
		BlockHeaders: store,
		Broadcaster:  newScriptedBroadcaster(scripts),
	}}
	headers, count := bm.getCFHeadersForAllPeers(1, wire.GCSFilterRegular)
	require.Equal(t, 1, count)
	require.Len(t, headers, 4)
	require.NotEqual(t,
		*headers["127.0.0.1:8333"].FilterHashes[0],
		*headers["127.0.0.4:8333"].FilterHashes[0],
	)
}

// TestHeaderAnchorDiscoveryUsesDurableTip verifies that discovery never mixes
// a store height with a hash from an older blockmanager notification snapshot.
func TestHeaderAnchorDiscoveryUsesDurableTip(t *testing.T) {
	t.Parallel()

	bm, store, _, err := setupBlockManager(t)
	require.NoError(t, err)
	bm.headerSync.Start(t.Context())
	t.Cleanup(bm.headerSync.Stop)

	genesis, _, err := store.ChainTip()
	require.NoError(t, err)
	header := wire.BlockHeader{
		PrevBlock: genesis.BlockHash(),
		Nonce:     1,
	}
	require.NoError(t, store.WriteHeaders(headerfs.BlockHeader{
		BlockHeader: &header,
		Height:      1,
	}))

	// The durable tip has advanced, while headerTipHash still identifies
	// genesis. A request must take both boundary fields from the store.
	require.Equal(t, genesis.BlockHash(), bm.headerTipHash)
	stopHash := chainhash.Hash{1}
	bm.nextCheckpoint = &chaincfg.Checkpoint{
		Height: 3001,
		Hash:   &stopHash,
	}
	sp := newTestServerPeer(t, "127.0.0.1:18333", 3001)
	peers := list.New()
	peers.PushBack(sp)

	require.True(t, bm.startHeaderAnchorDiscovery(peers, 3001))
	request, ok := bm.headerSync.Session().FinishAnchor(sp.Addr())
	require.True(t, ok)
	require.Equal(t, uint32(1), request.StartHeight)
	require.Equal(t, header.BlockHash(), request.StartHash)
}

// TestPrunedAnchorResponseKeepsTip checks that a late, already-known response
// cannot republish an older hash after its anchor request has been pruned.
func TestPrunedAnchorResponseKeepsTip(t *testing.T) {
	t.Parallel()

	bm, store, _, err := setupBlockManager(t)
	require.NoError(t, err)
	bm.headerSync.Start(t.Context())
	t.Cleanup(func() {
		bm.headerSync.Stop()
		if bm.headerSyncTimer != nil {
			bm.headerSyncTimer.Stop()
		}
	})

	genesis, _, err := store.ChainTip()
	require.NoError(t, err)
	first := wire.BlockHeader{PrevBlock: genesis.BlockHash(), Nonce: 1}
	second := wire.BlockHeader{PrevBlock: first.BlockHash(), Nonce: 2}
	require.NoError(t, store.WriteHeaders(
		headerfs.BlockHeader{BlockHeader: &first, Height: 1},
		headerfs.BlockHeader{BlockHeader: &second, Height: 2},
	))
	bm.headerList.PushBack(headerlist.Node{Header: first, Height: 1})
	bm.headerList.PushBack(headerlist.Node{Header: second, Height: 2})
	bm.headerTip = 2
	bm.headerTipHash = second.BlockHash()
	stopHash := chainhash.Hash{1}
	bm.nextCheckpoint = &chaincfg.Checkpoint{Height: 3002, Hash: &stopHash}
	sp := newTestServerPeer(t, "127.0.0.1:18333", 3002)
	bm.syncPeer = sp
	peers := list.New()
	peers.PushBack(sp)

	bm.headerSync.Session().TrackAnchor(sp.Addr(), headersync.AnchorRequest{
		StartHeight: 0,
		StartHash:   genesis.BlockHash(),
		StopHeight:  1,
		StopHash:    first.BlockHash(),
	})
	require.Equal(t, 1, bm.headerSync.Session().PruneAnchorsAtOrBefore(2))
	bm.handleHeadersMsgWithPeers(peers, &headersMsg{
		peer: sp,
		headers: &wire.MsgHeaders{
			Headers: []*wire.BlockHeader{&first},
		},
	})

	require.Equal(t, uint32(2), bm.headerTip)
	require.Equal(t, second.BlockHash(), bm.headerTipHash)
	tip, height, err := store.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(2), height)
	require.Equal(t, second.BlockHash(), tip.BlockHash())

	require.True(t, bm.startHeaderAnchorDiscovery(peers, 3002))
	request, ok := bm.headerSync.Session().FinishAnchor(sp.Addr())
	require.True(t, ok)
	require.Equal(t, height, request.StartHeight)
	require.Equal(t, tip.BlockHash(), request.StartHash)
}

func TestHeaderSyncTimeoutFailsOverToNextCandidate(t *testing.T) {
	t.Parallel()

	bm, _, _, err := setupBlockManager(t)
	require.NoError(t, err)
	bm.headerSyncTimeout = time.Hour

	// An unresponsive sync peer is quarantined, never IP-banned.
	var banned, quarantined []string
	bm.cfg.BanPeer = func(addr string, reason banman.Reason) error {
		banned = append(banned, addr)
		return nil
	}
	bm.cfg.QuarantinePeer = func(addr string, _ time.Duration) {
		quarantined = append(quarantined, addr)
	}

	slowPeer := newTestServerPeer(t, "127.0.0.1:8333", 100)
	fastPeer := newTestServerPeer(t, "127.0.0.2:8333", 200)

	peers := list.New()
	peers.PushBack(slowPeer)

	bm.startSync(peers)
	require.Equal(t, slowPeer, bm.SyncPeer())

	peers.PushBack(fastPeer)
	bm.handleHeaderSyncTimeoutMsg(peers, &headerSyncTimeoutMsg{
		peer:        slowPeer,
		requestID:   bm.serialRequestID,
		startHeight: 0,
		timeout:     bm.headerSyncTimeout,
	})

	require.Empty(t, banned)
	require.Equal(t, []string{slowPeer.Addr()}, quarantined)
	require.Equal(t, fastPeer, bm.SyncPeer())
}

// TestSerialTimeoutSurvivesDiscoveryRequest checks that arming an anchor
// discovery deadline does not void a pending serial timeout. The two shared
// one request counter, so the serial timeout failed its identity check and
// returned without disarming, leaving the serial timer armed for good.
func TestSerialTimeoutSurvivesDiscoveryRequest(t *testing.T) {
	t.Parallel()

	bm, _, _, err := setupBlockManager(t)
	require.NoError(t, err)
	bm.headerSyncTimeout = time.Hour

	var quarantined []string
	bm.cfg.QuarantinePeer = func(addr string, _ time.Duration) {
		quarantined = append(quarantined, addr)
	}

	slowPeer := newTestServerPeer(t, "127.0.0.1:8333", 100)
	fastPeer := newTestServerPeer(t, "127.0.0.2:8333", 200)
	peers := list.New()
	peers.PushBack(slowPeer)

	bm.startSync(peers)
	require.Equal(t, slowPeer, bm.SyncPeer())
	require.NotNil(t, bm.headerSyncTimer)
	serialID := bm.serialRequestID

	// A discovery request to another peer arms its own deadline.
	bm.handleDriverEvent(nil, headersync.RequestSent{
		Request: headersync.SendGetHeaders{
			PeerID:     fastPeer.Addr(),
			Kind:       headersync.RequestAnchor,
			StopHeight: 10,
		},
	}, fastPeer)

	peers.PushBack(fastPeer)
	bm.handleHeaderSyncTimeoutMsg(peers, &headerSyncTimeoutMsg{
		peer:        slowPeer,
		requestID:   serialID,
		startHeight: 0,
		timeout:     bm.headerSyncTimeout,
	})

	require.Equal(t, []string{slowPeer.Addr()}, quarantined,
		"the serial timeout was voided by the discovery request")
	require.Equal(t, fastPeer, bm.SyncPeer())
}

// TestHeaderSyncEmptyReplyFromSyncPeer checks how a serial reply that adds no
// headers settles the sync peer's request. A sync peer advertising more than
// our tip that answers with nothing used to keep the sync peer role with its
// deadline disarmed: no request, no timer, and sync stalled until the peer
// disconnected. It must instead hand sync to another peer, without
// punishment. A peer at our tip that answers empty is simply done.
func TestHeaderSyncEmptyReplyFromSyncPeer(t *testing.T) {
	t.Parallel()

	bm, _, _, err := setupBlockManager(t)
	require.NoError(t, err)
	bm.headerSyncTimeout = time.Hour

	var punished []string
	bm.cfg.QuarantinePeer = func(addr string, _ time.Duration) {
		punished = append(punished, addr)
	}
	bm.cfg.BanPeer = func(addr string, _ banman.Reason) error {
		punished = append(punished, addr)
		return nil
	}

	ahead := newTestServerPeer(t, "127.0.0.1:8333", 100)
	other := newTestServerPeer(t, "127.0.0.2:8333", 50)
	peers := list.New()
	peers.PushBack(ahead)
	peers.PushBack(other)
	bm.startSync(peers)
	require.Equal(t, ahead, bm.SyncPeer())

	bm.handleHeadersMsgWithPeers(peers, &headersMsg{
		peer: ahead, headers: wire.NewMsgHeaders(),
	})
	require.Empty(t, punished)
	require.Equal(t, other, bm.SyncPeer(),
		"sync stayed with a peer that cannot serve it")
	require.NotNil(t, bm.headerSyncTimer,
		"the new sync peer's request must carry a deadline")

	// An empty message with no serial request in flight answers
	// nothing and must not demote the sync peer.
	bm.disarmHeaderSyncTimeout()
	bm.handleHeadersMsgWithPeers(peers, &headersMsg{
		peer: other, headers: wire.NewMsgHeaders(),
	})
	require.Equal(t, other, bm.SyncPeer(),
		"unsolicited empty reply demoted the sync peer")
	bm.armHeaderSyncTimeout(other, 0)

	// A sync peer with nothing past our tip answering empty is done.
	done := newTestServerPeer(t, "127.0.0.3:8333", 0)
	peers.PushBack(done)
	bm.syncPeer = done
	bm.handleHeadersMsgWithPeers(peers, &headersMsg{
		peer: done, headers: wire.NewMsgHeaders(),
	})
	require.Nil(t, bm.headerSyncTimer)
	require.Equal(t, done, bm.SyncPeer())
	require.Empty(t, punished)
}

// headers wraps the different headers and filters used throughout the tests.
type headers struct {
	blockHeaders []headerfs.BlockHeader
	cfHeaders    []headerfs.FilterHeader
	checkpoints  []*chainhash.Hash
	filterHashes []chainhash.Hash
}

// generateHeaders generates block headers, filter header and hashes, and
// checkpoints from the given genesis. The onCheckpoint method will be called
// with the current cf header on each checkpoint to modify the derivation of
// the next interval.
func generateHeaders(genesisBlockHeader *wire.BlockHeader,
	genesisFilterHeader *chainhash.Hash,
	onCheckpoint func(*chainhash.Hash)) (*headers, error) {

	var blockHeaders []headerfs.BlockHeader
	blockHeaders = append(blockHeaders, headerfs.BlockHeader{
		BlockHeader: genesisBlockHeader,
		Height:      0,
	})

	var cfHeaders []headerfs.FilterHeader
	cfHeaders = append(cfHeaders, headerfs.FilterHeader{
		HeaderHash: genesisBlockHeader.BlockHash(),
		FilterHash: *genesisFilterHeader,
		Height:     0,
	})

	// The filter hashes (not the filter headers!) will be sent as
	// part of the CFHeaders response, so we also keep track of
	// them.
	genesisFilter, err := builder.BuildBasicFilter(
		chaincfg.SimNetParams.GenesisBlock, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to build genesis filter: %v",
			err)
	}

	genesisFilterHash, err := builder.GetFilterHash(genesisFilter)
	if err != nil {
		return nil, fmt.Errorf("unable to get genesis filter hash: %v",
			err)
	}

	var filterHashes []chainhash.Hash
	filterHashes = append(filterHashes, genesisFilterHash)

	// Also keep track of the current filter header. We use this to
	// calculate the next filter header, as it commits to the
	// previous.
	currentCFHeader := *genesisFilterHeader

	// checkpoints will be the checkpoints passed to
	// getCheckpointedCFHeaders.
	var checkpoints []*chainhash.Hash

	for height := uint32(1); height <= maxHeight; height++ {
		header := heightToHeader(height)
		blockHeader := headerfs.BlockHeader{
			BlockHeader: header,
			Height:      height,
		}

		blockHeaders = append(blockHeaders, blockHeader)

		// It doesn't really matter what filter the filter
		// header commit to, so just use the height as a nonce
		// for the filters.
		filterHash := chainhash.Hash{}
		binary.BigEndian.PutUint32(filterHash[:], height)
		filterHashes = append(filterHashes, filterHash)

		// Calculate the current filter header, and add to our
		// slice.
		currentCFHeader = chainhash.DoubleHashH(
			append(filterHash[:], currentCFHeader[:]...),
		)
		cfHeaders = append(cfHeaders, headerfs.FilterHeader{
			HeaderHash: header.BlockHash(),
			FilterHash: currentCFHeader,
			Height:     height,
		})

		// Each interval we must record a checkpoint.
		if height%wire.CFCheckptInterval == 0 {
			// We must make a copy of the current header to
			// avoid mutation.
			cfh := currentCFHeader
			checkpoints = append(checkpoints, &cfh)

			if onCheckpoint != nil {
				onCheckpoint(&currentCFHeader)
			}
		}
	}

	return &headers{
		blockHeaders: blockHeaders,
		cfHeaders:    cfHeaders,
		checkpoints:  checkpoints,
		filterHashes: filterHashes,
	}, nil
}

// generateResponses generates the MsgCFHeaders messages from the given queries
// and headers.
func generateResponses(msgs []wire.Message,
	headers *headers) ([]*wire.MsgCFHeaders, error) {

	// Craft a response for each message.
	var responses []*wire.MsgCFHeaders
	for _, msg := range msgs {
		// Only GetCFHeaders expected.
		q, ok := msg.(*wire.MsgGetCFHeaders)
		if !ok {
			return nil, fmt.Errorf("got unexpected message %T",
				msg)
		}

		// The start height must be set to a checkpoint height+1.
		if q.StartHeight%wire.CFCheckptInterval != 1 {
			return nil, fmt.Errorf("unexpected start height %v",
				q.StartHeight)
		}

		var prevFilterHeader chainhash.Hash
		switch q.StartHeight {
		// If the start height is 1 the prevFilterHeader is set to the
		// genesis header.
		case 1:
			genesisFilterHeader := headers.cfHeaders[0].FilterHash
			prevFilterHeader = genesisFilterHeader

		// Otherwise we use one of the created checkpoints.
		default:
			j := q.StartHeight/wire.CFCheckptInterval - 1
			prevFilterHeader = *headers.checkpoints[j]
		}

		resp := &wire.MsgCFHeaders{
			FilterType:       q.FilterType,
			StopHash:         q.StopHash,
			PrevFilterHeader: prevFilterHeader,
		}

		// Keep adding filter hashes until we reach the stop hash.
		for h := q.StartHeight; ; h++ {
			resp.FilterHashes = append(
				resp.FilterHashes, &headers.filterHashes[h],
			)

			blockHash := headers.blockHeaders[h].BlockHash()
			if blockHash == q.StopHash {
				break
			}
		}

		responses = append(responses, resp)
	}

	return responses, nil
}

// TestBlockManagerInitialInterval tests that the block manager is able to
// handle checkpointed filter header query responses in out of order, and when
// a partial interval is already written to the store.
func TestBlockManagerInitialInterval(t *testing.T) {
	t.Parallel()

	type testCase struct {
		// permute indicates whether responses should be permutated.
		permute bool

		// partialInterval indicates whether we should write parts of
		// the first checkpoint interval to the filter header store
		// before starting the test.
		partialInterval bool

		// repeat indicates whether responses should be repeated.
		repeat bool
	}

	// Generate all combinations of testcases.
	var testCases []testCase
	b := []bool{false, true}
	for _, perm := range b {
		for _, part := range b {
			for _, rep := range b {
				testCases = append(testCases, testCase{
					permute:         perm,
					partialInterval: part,
					repeat:          rep,
				})
			}
		}
	}

	for _, test := range testCases {
		testDesc := fmt.Sprintf("permute=%v, partial=%v, repeat=%v",
			test.permute, test.partialInterval, test.repeat)

		bm, hdrStore, cfStore, err := setupBlockManager(t)
		if err != nil {
			t.Fatalf("unable to set up ChainService: %v", err)
		}

		// Keep track of the filter headers and block headers. Since
		// the genesis headers are written automatically when the store
		// is created, we query it to add to the slices.
		genesisBlockHeader, _, err := hdrStore.ChainTip()
		require.NoError(t, err)

		genesisFilterHeader, _, err := cfStore.ChainTip()
		require.NoError(t, err)

		headers, err := generateHeaders(
			genesisBlockHeader, genesisFilterHeader, nil,
		)
		require.NoError(t, err)

		// Write all block headers but the genesis, since it is already
		// in the store.
		err = hdrStore.WriteHeaders(headers.blockHeaders[1:]...)
		require.NoError(t, err)

		// We emulate the case where a few filter headers are already
		// written to the store by writing 1/3 of the first interval.
		if test.partialInterval {
			err = cfStore.WriteHeaders(
				headers.cfHeaders[1 : wire.CFCheckptInterval/3]...,
			)
			require.NoError(t, err)
		}
		require.NoError(t, bm.ResetHeaderState())

		// We set up a custom query batch method for this test, as we
		// will use this to feed the blockmanager with our crafted
		// responses.
		bm.cfg.QueryDispatcher.(*mockDispatcher).query = func(
			requests []*querysyncwm.Request,
			options ...querysyncwm.QueryOption) chan error {

			var msgs []wire.Message
			for _, q := range requests {
				msgs = append(msgs, q.Req)
			}

			responses, err := generateResponses(msgs, headers)
			require.NoError(t, err)

			// We permute the response order if the test signals
			// that.
			perm := rand.Perm(len(responses))

			errChan := make(chan error, 1)
			go func() {
				for i, v := range perm {
					index := i
					if test.permute {
						index = v
					}

					// Before handling the response we take
					// copies of the message, as we cannot
					// guarantee that it won't be modified.
					resp := *responses[index]
					resp2 := *responses[index]

					// Let the blockmanager handle the
					// message.
					progress := requests[index].HandleResp(
						msgs[index], &resp, "",
					)

					if !progress.Finished {
						errChan <- fmt.Errorf("got "+
							"response false on "+
							"send of index %d: %v",
							index, testDesc)
						return
					}

					// If we are not testing repeated
					// responses, go on to the next
					// response.
					if !test.repeat {
						continue
					}

					// Otherwise resend the response we
					// just sent.
					progress = requests[index].HandleResp(
						msgs[index], &resp2, "",
					)
					if !progress.Finished {
						errChan <- fmt.Errorf("got "+
							"response false on "+
							"resend of index %d: "+
							"%v", index, testDesc)
						return
					}
				}
				errChan <- nil
			}()

			return errChan
		}

		// We should expect to see notifications for each new filter
		// header being connected.
		startHeight := uint32(1)
		if test.partialInterval {
			startHeight = wire.CFCheckptInterval / 3
		}
		go func() {
			for i := startHeight; i <= maxHeight; i++ {
				ntfn := <-bm.blockNtfnChan
				if _, ok := ntfn.(*blockntfns.Connected); !ok {
					t.Error("expected block connected " +
						"notification")
					return
				}
			}
		}()

		// Call the get checkpointed cf headers method with the
		// checkpoints we created to start the test.
		bm.wg.Add(1)
		go bm.blockHandler()
		t.Cleanup(func() { require.NoError(t, bm.Stop()) })
		bm.getCheckpointedCFHeaders(
			headers.checkpoints, cfStore, wire.GCSFilterRegular,
		)

		// Finally make sure the filter header tip is what we expect.
		tip, tipHeight, err := cfStore.ChainTip()
		require.NoError(t, err)

		require.Equal(t, maxHeight, tipHeight, "tip height")

		lastCheckpoint := headers.checkpoints[len(headers.checkpoints)-1]
		require.Equal(t, *lastCheckpoint, *tip, "tip")
	}
}

// TestBlockManagerInvalidInterval tests that the block manager is able to
// determine it is receiving corrupt checkpoints and filter headers.
func TestBlockManagerInvalidInterval(t *testing.T) {
	t.Parallel()

	type testCase struct {
		// wrongGenesis indicates whether we should start deriving the
		// filters from a wrong genesis.
		wrongGenesis bool

		// intervalMisaligned indicates whether each interval prev hash
		// should not line up with the previous checkpoint.
		intervalMisaligned bool

		// invalidPrevHash indicates whether the interval responses
		// should have a prev hash that doesn't mathc that interval.
		invalidPrevHash bool

		// partialInterval indicates whether we should write parts of
		// the first checkpoint interval to the filter header store
		// before starting the test.
		partialInterval bool

		// firstInvalid is the first interval response we expect the
		// blockmanager to determine is invalid.
		firstInvalid int
	}

	testCases := []testCase{
		// With a set of checkpoints and filter headers calculated from
		// the wrong genesis, the block manager should be able to
		// determine that the first interval doesn't line up.
		{
			wrongGenesis: true,
			firstInvalid: 0,
		},

		// With checkpoints calculated from the wrong genesis, and a
		// partial set of filter headers already written, the first
		// interval response should be considered invalid.
		{
			wrongGenesis:    true,
			partialInterval: true,
			firstInvalid:    0,
		},

		// With intervals not lining up, the second interval response
		// should be determined invalid.
		{
			intervalMisaligned: true,
			firstInvalid:       0,
		},

		// With misaligned intervals and a partial interval written, the
		// second interval response should be considered invalid.
		{
			intervalMisaligned: true,
			partialInterval:    true,
			firstInvalid:       0,
		},

		// With responses having invalid prev hashes, the second
		// interval response should be deemed invalid.
		{
			invalidPrevHash: true,
			firstInvalid:    1,
		},
	}

	for _, test := range testCases {
		bm, hdrStore, cfStore, err := setupBlockManager(t)
		require.NoError(t, err)

		// Create a mock peer to prevent panics when attempting to ban
		// a peer that served an invalid filter header.
		mockPeer := NewServerPeer(&ChainService{}, false)
		mockPeer.Peer, err = peer.NewOutboundPeer(
			NewPeerConfig(mockPeer), "127.0.0.1:8333",
		)
		require.NoError(t, err)

		// Keep track of the filter headers and block headers. Since
		// the genesis headers are written automatically when the store
		// is created, we query it to add to the slices.
		genesisBlockHeader, _, err := hdrStore.ChainTip()
		require.NoError(t, err)

		genesisFilterHeader, _, err := cfStore.ChainTip()
		require.NoError(t, err)

		// To emulate a full node serving us filter headers derived
		// from different genesis than what we have, we flip a bit in
		// the genesis filter header.
		if test.wrongGenesis {
			genesisFilterHeader[0] ^= 1
		}

		headers, err := generateHeaders(
			genesisBlockHeader, genesisFilterHeader,
			func(currentCFHeader *chainhash.Hash) {
				// If we are testing that each interval doesn't
				// line up properly with the previous, we flip
				// a bit in the current header before
				// calculating the next interval checkpoint.
				if test.intervalMisaligned {
					currentCFHeader[0] ^= 1
				}
			},
		)
		require.NoError(t, err)

		// Write all block headers but the genesis, since it is already
		// in the store.
		err = hdrStore.WriteHeaders(headers.blockHeaders[1:]...)
		require.NoError(t, err)

		// We emulate the case where a few filter headers are already
		// written to the store by writing 1/3 of the first interval.
		if test.partialInterval {
			err = cfStore.WriteHeaders(
				headers.cfHeaders[1 : wire.CFCheckptInterval/3]...,
			)
			require.NoError(t, err)
		}
		require.NoError(t, bm.ResetHeaderState())

		bm.cfg.QueryDispatcher.(*mockDispatcher).query = func(
			requests []*querysyncwm.Request,
			options ...querysyncwm.QueryOption) chan error {

			var msgs []wire.Message
			for _, q := range requests {
				msgs = append(msgs, q.Req)
			}
			responses, err := generateResponses(msgs, headers)
			require.NoError(t, err)

			// Since we used the generated checkpoints when
			// creating the responses, we must flip the
			// PrevFilterHeader bit back before sending them if we
			// are checking for misaligned intervals. This to
			// ensure we don't hit the invalid prev hash case.
			if test.intervalMisaligned {
				for i := range responses {
					if i == 0 {
						continue
					}
					responses[i].PrevFilterHeader[0] ^= 1
				}
			}

			// If we are testing for intervals with invalid prev
			// hashes, we flip a bit to corrupt them, regardless of
			// whether we are testing misaligned intervals.
			if test.invalidPrevHash {
				for i := range responses {
					if i == 0 {
						continue
					}
					responses[i].PrevFilterHeader[1] ^= 1
				}
			}

			errChan := make(chan error, 1)
			go func() {
				// Check that the success of the callback match what we
				// expect.
				for i := range responses {
					progress := requests[i].HandleResp(
						msgs[i], responses[i], "",
					)
					if i == test.firstInvalid {
						if progress.Finished {
							t.Errorf("expected interval "+
								"%d to be invalid", i)
							return
						}
						errChan <- fmt.Errorf("invalid interval")
						break
					}

					if !progress.Finished {
						t.Errorf("expected interval %d to be "+
							"valid", i)
						return
					}
				}

				errChan <- nil
			}()

			return errChan
		}

		// We should expect to see notifications for each new filter
		// header being connected.
		startHeight := uint32(1)
		if test.partialInterval {
			startHeight = wire.CFCheckptInterval / 3
		}
		go func() {
			for i := startHeight; i <= maxHeight; i++ {
				ntfn := <-bm.blockNtfnChan
				if _, ok := ntfn.(*blockntfns.Connected); !ok {
					t.Error("expected block connected " +
						"notification")
					return
				}
			}
		}()

		// Start the test by calling the get checkpointed cf headers
		// method with the checkpoints we created.
		bm.wg.Add(1)
		go bm.blockHandler()
		t.Cleanup(func() { require.NoError(t, bm.Stop()) })
		bm.getCheckpointedCFHeaders(
			headers.checkpoints, cfStore, wire.GCSFilterRegular,
		)
	}
}

// buildNonPushScriptFilter creates a CFilter with all output scripts except all
// OP_RETURNS with push-only scripts.
//
// NOTE: this is not a valid filter, only for tests.
func buildNonPushScriptFilter(block *wire.MsgBlock) (*gcs.Filter, error) {
	blockHash := block.BlockHash()
	b := builder.WithKeyHash(&blockHash)

	for _, tx := range block.Transactions {
		for _, txOut := range tx.TxOut {
			// The old version of BIP-158 skipped OP_RETURNs that
			// had a push-only script.
			if txOut.PkScript[0] == txscript.OP_RETURN &&
				txscript.IsPushOnlyScript(txOut.PkScript[1:]) {

				continue
			}

			b.AddEntry(txOut.PkScript)
		}
	}

	return b.Build()
}

// buildAllPkScriptsFilter creates a CFilter with all output scripts, including
// OP_RETURNS.
//
// NOTE: this is not a valid filter, only for tests.
func buildAllPkScriptsFilter(block *wire.MsgBlock) (*gcs.Filter, error) {
	blockHash := block.BlockHash()
	b := builder.WithKeyHash(&blockHash)

	for _, tx := range block.Transactions {
		for _, txOut := range tx.TxOut {
			// An old version of BIP-158 included all output
			// scripts.
			b.AddEntry(txOut.PkScript)
		}
	}

	return b.Build()
}

func assertBadPeers(expBad map[string]struct{}, badPeers []string) error {
	remBad := make(map[string]struct{})
	for p := range expBad {
		remBad[p] = struct{}{}
	}
	for _, p := range badPeers {
		_, ok := remBad[p]
		if !ok {
			return fmt.Errorf("did not expect %v to be bad", p)
		}
		delete(remBad, p)
	}

	if len(remBad) != 0 {
		return fmt.Errorf("did expect more bad peers")
	}

	return nil
}

// TestBlockManagerDetectBadPeers checks that we detect bad peers, like peers
// not responding to our filter query, serving inconsistent filters etc.
func TestBlockManagerDetectBadPeers(t *testing.T) {
	t.Parallel()

	var (
		stopHash        = chainhash.Hash{}
		prev            = chainhash.Hash{}
		startHeight     = uint32(100)
		badIndex        = uint32(5)
		targetIndex     = startHeight + badIndex
		fType           = wire.GCSFilterRegular
		filterBytes, _  = correctFilter.NBytes()
		filterHash, _   = builder.GetFilterHash(correctFilter)
		targetBlockHash = block.BlockHash()

		peers = []string{"good1:1", "good2:1", "bad:1", "good3:1"}
	)

	testCases := []struct {
		// name describes the scenario under test.
		name string

		// filterAnswers is used by each testcase to set the anwers we
		// want each peer to respond with on filter queries.
		filterAnswers func(string, map[string]wire.Message)

		// expInvalid is the set of peers we expect to be classified as
		// serving verified invalid data (and therefore banned).
		expInvalid map[string]struct{}

		// expUnresponsive is the set of peers we expect to be
		// classified as not responding (and therefore disconnected
		// without a ban).
		expUnresponsive map[string]struct{}
	}{
		{
			// We let the "bad" peers not respond to the filter
			// query. A missing response is not proof that they
			// served invalid data, so they should be reported as
			// unresponsive (disconnected without a ban) rather than
			// banned. We still drop them from this round so they
			// cannot stall our sync by only responding with
			// headers.
			name: "unresponsive peers are not banned",
			filterAnswers: func(p string,
				answers map[string]wire.Message) {

				if strings.Contains(p, "bad") {
					return
				}

				answers[p] = wire.NewMsgCFilter(
					fType, &targetBlockHash, filterBytes,
				)
			},
			expInvalid: map[string]struct{}{},
			expUnresponsive: map[string]struct{}{
				"bad:1": {},
			},
		},
		{
			// We let the "bad" peers serve filters that don't hash
			// to the filter headers they have sent. This is
			// verified invalid data, so they should be banned.
			name: "peers serving invalid filters are banned",
			filterAnswers: func(p string,
				answers map[string]wire.Message) {

				filterData := filterBytes
				if strings.Contains(p, "bad") {
					filterData, _ = fakeFilter1.NBytes()
				}

				answers[p] = wire.NewMsgCFilter(
					fType, &targetBlockHash, filterData,
				)
			},
			expInvalid: map[string]struct{}{
				"bad:1": {},
			},
			expUnresponsive: map[string]struct{}{},
		},
	}

	for _, test := range testCases {
		// Create a mock block header store. We only need to be able to
		// serve a header for the target index.
		mBlockHeaderStore := &headerfs.MockBlockHeaderStore{}

		mBlockHeaderStore.On(
			"FetchHeaderAncestors", badIndex-1, &stopHash,
		).Return(make([]wire.BlockHeader, badIndex), targetIndex, nil)

		// We set up the scripted broadcaster to only respond according
		// to the active testcase.
		answers := make(map[string]wire.Message)
		broadcaster := broadcastFunc(func(b *querysyncwm.Broadcast,
			opts ...querysyncwm.QueryOption) error {

			scripts := make(map[string][]wire.Message)
			for p, resp := range answers {
				scripts[p] = []wire.Message{resp}
			}

			return newScriptedBroadcaster(scripts).Broadcast(
				b, opts...,
			)
		})

		for _, p := range peers {
			test.filterAnswers(p, answers)
		}

		// For the CFHeaders, we pretend all peers responded with the same
		// filter headers.
		msg := &wire.MsgCFHeaders{
			FilterType:       fType,
			StopHash:         stopHash,
			PrevFilterHeader: prev,
		}

		for i := uint32(0); i < 2*badIndex; i++ {
			_ = msg.AddCFHash(&filterHash)
		}

		headers := make(map[string]*wire.MsgCFHeaders)
		for _, p := range peers {
			headers[p] = msg
		}

		bm := &blockManager{
			cfg: &blockManagerCfg{
				BlockHeaders: mBlockHeaderStore,
				Broadcaster:  broadcaster,
			},
		}

		// Now trying to detect which peers are bad, we should split
		// them into the verified-invalid set and the unresponsive set.
		invalidPeers, unresponsivePeers, err := bm.detectBadPeers(
			headers, targetIndex, badIndex, fType,
		)
		require.NoError(t, err, test.name)

		require.NoError(
			t, assertBadPeers(test.expInvalid, invalidPeers),
			test.name,
		)
		require.NoError(
			t, assertBadPeers(
				test.expUnresponsive, unresponsivePeers,
			),
			test.name,
		)
	}
}

// TestHandleHeaders checks that we handle headers correctly, and that we
// disconnect peers that serve us bad headers (headers that don't connect to
// each other properly).
func TestHandleHeaders(t *testing.T) {
	t.Parallel()

	// First, we set up a block manager and a fake peer that will act as the
	// test's remote peer.
	bm, _, _, err := setupBlockManager(t)
	require.NoError(t, err)

	fakePeer, err := peer.NewOutboundPeer(&peer.Config{}, "fake:123")
	require.NoError(t, err)

	assertPeerDisconnected := func(shouldBeDisconnected bool) {
		// This is quite hacky but works: We expect the peer to be
		// disconnected, which sets the unexported "disconnected" field
		// to 1.
		refValue := reflect.ValueOf(fakePeer).Elem()
		foo := refValue.FieldByName("disconnect").Int()

		if shouldBeDisconnected {
			require.EqualValues(t, 1, foo)
		} else {
			require.EqualValues(t, 0, foo)
		}
	}

	// We'll want to use actual, real blocks, so we take a miner harness
	// that we can use to generate some.
	harness, err := rpctest.New(
		&chaincfg.SimNetParams, nil, []string{"--txindex"}, "",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, harness.TearDown())
	})

	err = harness.SetUp(false, 0)
	require.NoError(t, err)

	// Generate 200 valid blocks that we then feed to the block manager.
	blockHashes, err := harness.Client.Generate(200)
	require.NoError(t, err)

	hmsg := &headersMsg{
		headers: &wire.MsgHeaders{
			Headers: make([]*wire.BlockHeader, len(blockHashes)),
		},
		peer: &ServerPeer{
			Peer: fakePeer,
		},
	}

	for i := range blockHashes {
		header, err := harness.Client.GetBlockHeader(blockHashes[i])
		require.NoError(t, err)

		hmsg.headers.Headers[i] = header
	}

	// Let's feed in the correct headers. This should work fine and the peer
	// should not be disconnected.
	bm.handleHeadersMsg(hmsg)
	settleHeaderCommit(t, bm, list.New())
	assertPeerDisconnected(false)

	// Now scramble the headers and feed them in again. This should cause
	// the peer to be disconnected.
	rand.Shuffle(len(hmsg.headers.Headers), func(i, j int) {
		hmsg.headers.Headers[i], hmsg.headers.Headers[j] =
			hmsg.headers.Headers[j], hmsg.headers.Headers[i]
	})
	bm.handleHeadersMsg(hmsg)
	assertPeerDisconnected(true)
}

// TestAllPeerBroadcastsUseQueryTimeout checks that the cfilter and
// checkpoint all-peer queries bound each attempt by the QueryTimeout setting.
func TestAllPeerBroadcastsUseQueryTimeout(t *testing.T) {
	t.Parallel()

	var timeouts []time.Duration
	bm := &blockManager{cfg: &blockManagerCfg{
		Broadcaster: broadcastFunc(func(_ *querysyncwm.Broadcast,
			opts ...querysyncwm.QueryOption) error {

			timeouts = append(
				timeouts,
				querysyncwm.ResolveOptions(opts...).Timeout,
			)

			return nil
		}),
	}}

	hash := chainhash.Hash{1}
	bm.fetchFilterFromAllPeers(1, hash, wire.GCSFilterRegular)
	bm.getCheckpts(&hash, wire.GCSFilterRegular)

	require.Equal(t, []time.Duration{QueryTimeout, QueryTimeout}, timeouts)
}

// TestBanAndDisconnect distinguishes invalid data from missing responses.
func TestBanAndDisconnect(t *testing.T) {
	var banned, disconnected []string
	bm := &blockManager{cfg: &blockManagerCfg{
		BanPeer: func(peer string, reason banman.Reason) error {
			require.Equal(t, banman.InvalidFilterHeader, reason)
			banned = append(banned, peer)
			return nil
		},
		DisconnectPeer: func(peer string) error {
			disconnected = append(disconnected, peer)
			return nil
		},
	}}

	acted := bm.banAndDisconnect(
		[]string{"invalid"}, []string{"unresponsive"},
		banman.InvalidFilterHeader,
	)
	require.Equal(t, []string{"invalid"}, banned)
	require.Equal(t, []string{"unresponsive"}, disconnected)
	require.ElementsMatch(t, []string{"invalid", "unresponsive"}, acted)
}

// TestResolveCFHeaderMixedResponders checks that a silent peer does not hide
// a conflict between responsive peers at the same height.
func TestResolveCFHeaderMixedResponders(t *testing.T) {
	correctHash, err := builder.GetFilterHash(correctFilter)
	require.NoError(t, err)
	wrongHash, err := builder.GetFilterHash(fakeFilter1)
	require.NoError(t, err)
	stopHash := block.BlockHash()
	headers := map[string]*wire.MsgCFHeaders{
		"good:1": {
			StopHash:     stopHash,
			FilterHashes: []*chainhash.Hash{&correctHash},
		},
		"wrong:1": {
			StopHash:     stopHash,
			FilterHashes: []*chainhash.Hash{&wrongHash},
		},
		"silent:1": {
			StopHash:     stopHash,
			FilterHashes: []*chainhash.Hash{&correctHash},
		},
	}
	checkpoints := make(map[string][]*chainhash.Hash)
	for peer := range headers {
		checkpoints[peer] = nil
	}
	store := &headerfs.MockBlockHeaderStore{}
	store.On("FetchHeaderAncestors", uint32(0), &stopHash).Return(
		[]wire.BlockHeader{block.Header}, uint32(100), nil,
	)
	var banned, disconnected []string
	bm := &blockManager{cfg: &blockManagerCfg{
		BlockHeaders: store,
		GetBlock: func(chainhash.Hash, ...QueryOption) (
			*btcutil.Block, error) {

			return btcutil.NewBlock(block), nil
		},
		BanPeer: func(addr string, _ banman.Reason) error {
			banned = append(banned, addr)
			return nil
		},
		DisconnectPeer: func(addr string) error {
			disconnected = append(disconnected, addr)
			return nil
		},
	}}
	bm.cfg.Broadcaster = broadcastFunc(func(b *querysyncwm.Broadcast,
		opts ...querysyncwm.QueryOption) error {

		scripts := make(map[string][]wire.Message)
		for addr, filter := range map[string]*gcs.Filter{
			"good:1": correctFilter, "wrong:1": fakeFilter1,
		} {
			payload, err := filter.NBytes()
			require.NoError(t, err)
			hash := block.BlockHash()
			scripts[addr] = []wire.Message{wire.NewMsgCFilter(
				wire.GCSFilterRegular, &hash, payload,
			)}
		}
		return newScriptedBroadcaster(scripts).Broadcast(b, opts...)
	})
	err = bm.resolveCFHeaderMismatches(
		headers, checkpoints, 100, 1, wire.GCSFilterRegular,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"wrong:1"}, banned)
	require.Equal(t, []string{"silent:1"}, disconnected)
	require.Len(t, headers, 1)
	require.Contains(t, headers, "good:1")
	require.Len(t, checkpoints, 1)
	require.Contains(t, checkpoints, "good:1")
}
