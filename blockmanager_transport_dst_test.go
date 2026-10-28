package neutrino

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"sort"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headersync"
	"github.com/stretchr/testify/require"
)

const transportDSTHeight = 12

var errDSTWrite = errors.New("simulated header store unavailable")

type dstHeaderRequest struct {
	peer  *ServerPeer
	start uint32
	stop  uint32
}

// dstHeaderTransport models one bounded outbound slot per connection. It never
// schedules work or synthesizes timeout events. Production request timers and
// message handlers decide when a stalled request loses its lease.
type dstHeaderTransport struct {
	heights map[chainhash.Hash]uint32
	queued  []dstHeaderRequest
	blocked map[string]bool
	sent    int
}

func (d *dstHeaderTransport) SendGetHeaders(peer *ServerPeer,
	locator blockchain.BlockLocator, stop *chainhash.Hash) error {

	if d.blocked[peer.Addr()] {
		return errors.New("simulated outbound queue blocked")
	}
	for _, request := range d.queued {
		if request.peer == peer {
			return errors.New("simulated outbound queue full")
		}
	}
	if len(locator) == 0 || stop == nil {
		return errors.New("missing header request boundaries")
	}
	start, ok := d.heights[*locator[0]]
	if !ok {
		return errors.New("unknown header request locator")
	}
	end := uint32(transportDSTHeight)
	if *stop != zeroHash {
		var known bool
		end, known = d.heights[*stop]
		if !known {
			return errors.New("unknown header request stop")
		}
	}
	d.queued = append(d.queued, dstHeaderRequest{
		peer: peer, start: start, stop: end,
	})
	d.sent++
	return nil
}

type dstHeaderStore struct {
	headerfs.BlockHeaderStore
	unavailable bool
	writes      []uint32
}

func (d *dstHeaderStore) WriteHeaders(headers ...headerfs.BlockHeader) error {
	if d.unavailable {
		return errDSTWrite
	}
	if err := d.BlockHeaderStore.WriteHeaders(headers...); err != nil {
		return err
	}
	for _, header := range headers {
		d.writes = append(d.writes, header.Height)
	}
	return nil
}

type headerTransportDST struct {
	t             *testing.T
	bm            *blockManager
	store         *dstHeaderStore
	transport     *dstHeaderTransport
	peers         *list.List
	current       [3]*ServerPeer
	generation    [3]int
	headers       []*wire.BlockHeader
	recorder      *headersync.EventRecorder
	last          [3]*headersMsg
	timer         *headerSyncTimeoutMsg
	timeouts      int
	steps         []string
	lastTip       uint32
	reverseTimers bool
	epochs        map[headersync.RangeID]uint64
}

func newHeaderTransportDST(t *testing.T) *headerTransportDST {
	t.Helper()
	return newHeaderTransportDSTThrough(t, transportDSTHeight)
}

func newHeaderTransportDSTThrough(t *testing.T,
	plannedHeight uint32) *headerTransportDST {

	t.Helper()
	bm, store, _, err := setupBlockManager(t)
	require.NoError(t, err)
	genesis, _, err := store.ChainTip()
	require.NoError(t, err)
	bm.cfg.TimeSource = dstMedianTime{
		MedianTimeSource: bm.cfg.TimeSource,
		offset:           time.Until(genesis.Timestamp.Add(time.Hour)),
	}
	headers := discoveryTestHeaders(t, bm, genesis, transportDSTHeight)

	// Use cheap SimNet proof of work while enabling the production actor
	// path, which otherwise preserves a serial SimNet compatibility path.
	bm.cfg.ChainParams.Net = chaincfg.TestNet3Params.Net
	for height := uint32(2); height <= transportDSTHeight; height += 2 {
		hash := headers[height-1].BlockHash()
		bm.cfg.ChainParams.Checkpoints = append(
			bm.cfg.ChainParams.Checkpoints,
			chaincfg.Checkpoint{Height: int32(height), Hash: &hash},
		)
	}
	bm.nextCheckpoint = bm.findNextHeaderCheckpoint(0)
	require.NoError(t, bm.initHeaderSyncManager(0, genesis.BlockHash()))
	recorder := &headersync.EventRecorder{}
	bm.headerSync.Manager().SetEventSink(recorder)
	bm.headerSync.Start(t.Context())
	bm.headerSyncTimeout = time.Second
	t.Cleanup(func() { require.NoError(t, bm.Stop()) })
	transport := &dstHeaderTransport{
		heights: map[chainhash.Hash]uint32{genesis.BlockHash(): 0},
		blocked: make(map[string]bool),
	}
	for i, header := range headers {
		transport.heights[header.BlockHash()] = uint32(i + 1)
	}
	bm.cfg.HeaderTransport = transport
	faultStore := &dstHeaderStore{BlockHeaderStore: store}
	bm.cfg.BlockHeaders = faultStore
	d := &headerTransportDST{
		t: t, bm: bm, store: faultStore, transport: transport,
		peers: list.New(), headers: headers, recorder: recorder,
		epochs: make(map[headersync.RangeID]uint64),
	}
	for peer := range d.current {
		d.connect(peer)
	}
	plan, err := bm.headerSync.Manager().PlanConfirmedAnchorRanges(
		t.Context(), 1, 0, plannedHeight,
	)
	require.NoError(t, err)
	require.Len(t, plan.Ranges, int(plannedHeight/2))
	bm.headerSync.Session().SetNextRangeID(plan.NextID)
	bm.dispatchHeaderSyncWork(d.peers)
	d.check("start")
	return d
}

func (d *headerTransportDST) connect(index int) {
	if d.current[index] != nil {
		for element := d.peers.Front(); element != nil; {
			if element.Value == d.current[index] {
				return
			}
			element = element.Next()
		}
		d.bm.unregisterHeaderSyncPeer(d.current[index])
	}
	d.generation[index]++
	peer := newTestServerPeer(d.t,
		fmt.Sprintf("127.0.0.%d:%d", index+1,
			8332+d.generation[index]), transportDSTHeight,
	)
	d.current[index] = peer
	d.peers.PushBack(peer)
	d.bm.registerHeaderSyncPeer(peer)
	// The fixture starts below a checkpoint, not at the serial sync tip.
	if d.bm.syncPeer == nil {
		d.bm.syncPeer = peer
	}
}

func (d *headerTransportDST) pump() {
	d.t.Helper()
	for {
		synctest.Wait()
		var messages []any
	drain:
		for {
			select {
			case message := <-d.bm.peerChan:
				messages = append(messages, message)
			default:
				break drain
			}
		}
		if len(messages) == 0 {
			return
		}
		// Seed the delivery order of callbacks sharing a deadline.
		// synctest alone does not control their order.
		sort.SliceStable(messages, func(i, j int) bool {
			a, aTimer := messages[i].(*headerSyncTimeoutMsg)
			b, bTimer := messages[j].(*headerSyncTimeoutMsg)
			if aTimer != bTimer {
				return aTimer
			}
			if !aTimer {
				return false
			}
			if d.reverseTimers {
				return a.requestID > b.requestID
			}
			return a.requestID < b.requestID
		})
		for _, message := range messages {
			if timer, ok := message.(*headerSyncTimeoutMsg); ok {
				d.timer = timer
				d.timeouts++
			}
			d.bm.handlePeerMessage(d.peers, message)
			synctest.Wait()
			d.check(fmt.Sprintf("message:%T", message))
		}
	}
}

func (d *headerTransportDST) reply(index int, corrupt, partial bool) bool {
	d.t.Helper()
	peer := d.current[index]
	if peer == nil || d.transport.blocked[peer.Addr()] {
		return false
	}
	for i, request := range d.transport.queued {
		if request.peer != peer {
			continue
		}
		d.transport.queued = append(d.transport.queued[:i],
			d.transport.queued[i+1:]...)
		msg := wire.NewMsgHeaders()
		for _, header := range d.headers[request.start:request.stop] {
			copyHeader := *header
			require.NoError(d.t, msg.AddBlockHeader(&copyHeader))
		}
		if partial && len(msg.Headers) > 0 {
			msg.Headers = msg.Headers[:len(msg.Headers)-1]
		}
		if corrupt && len(msg.Headers) > 0 {
			msg.Headers[0].Bits = 0
		}
		d.last[index] = &headersMsg{peer: peer, headers: msg}
		d.deliver(d.last[index])
		return true
	}
	return false
}

func (d *headerTransportDST) deliver(message *headersMsg) {
	d.t.Helper()
	// Encode/decode each delivery so duplicates do not share mutable header
	// objects with earlier callbacks. TCP framing is not simulated.
	var buf bytes.Buffer
	require.NoError(d.t, message.headers.BtcEncode(
		&buf, wire.ProtocolVersion, wire.BaseEncoding,
	))
	decoded := wire.NewMsgHeaders()
	require.NoError(d.t, decoded.BtcDecode(
		&buf, wire.ProtocolVersion, wire.BaseEncoding,
	))
	d.bm.QueueHeaders(decoded, message.peer)
	d.pump()
}

// check is an independent oracle for the P ownership and ordered-commit
// invariants, not another implementation of dispatch or work stealing.
func (d *headerTransportDST) check(event string) {
	d.t.Helper()
	ctx := d.t.Context()
	tip, height, err := d.store.ChainTip()
	require.NoError(d.t, err)
	require.GreaterOrEqual(d.t, height, d.lastTip)
	require.LessOrEqual(d.t, height, uint32(transportDSTHeight))
	if height > 0 {
		require.Equal(d.t,
			d.headers[height-1].BlockHash(), tip.BlockHash(),
		)
	}
	require.Equal(d.t, height, d.bm.headerTip)
	require.Equal(d.t, tip.BlockHash(), d.bm.headerTipHash)
	require.EqualValues(d.t, height, d.bm.headerList.Back().Height,
		"failed write left the validation cache ahead of disk")
	d.lastTip = height
	owners := make(map[headersync.RangeID]string)
	for _, peer := range d.current {
		if peer == nil {
			continue
		}
		snapshot, ok, err := d.bm.headerSync.Manager().SnapshotPeer(
			ctx, peer.Addr(),
		)
		require.NoError(d.t, err)
		if !ok {
			continue
		}
		for _, id := range snapshot.LocalRanges {
			require.NotContains(d.t, owners, id)
			owners[id] = peer.Addr()
		}
		if snapshot.ActiveRange != headersync.NoRange {
			require.NotContains(d.t, owners, snapshot.ActiveRange)
			owners[snapshot.ActiveRange] = peer.Addr()
		}
	}
	nextID := d.bm.headerSync.Session().NextRangeID()
	for id := headersync.RangeID(1); id < nextID; id++ {
		rng, ok, err := d.bm.headerSync.Manager().SnapshotRange(ctx, id)
		require.NoError(d.t, err)
		if !ok {
			continue
		}
		require.GreaterOrEqual(d.t, rng.LeaseEpoch, d.epochs[id],
			"a late callback restored an older lease")
		d.epochs[id] = rng.LeaseEpoch
		switch rng.State {
		case headersync.RangeOwned, headersync.RangeActive:
			require.Equal(d.t, rng.OwnerPeer, owners[id])
		case headersync.RangeCommitted:
			require.True(d.t, rng.Valid)
			require.LessOrEqual(d.t, rng.StopHeight, height)
		case headersync.RangeStaged:
			_, staged := d.bm.headerSync.Session().StagedRange(id)
			require.True(d.t, staged,
				"event=%s range=%+v tip=%d events=%+v",
				event, rng, height, d.recorder.Events())
		}
	}
	for i, written := range d.store.writes {
		require.EqualValues(d.t, i+1, written,
			"duplicate or skipped durable header")
	}
	committed := make(map[headersync.RangeID]bool)
	for _, event := range d.recorder.Events() {
		for _, id := range event.Committed {
			require.False(d.t, committed[id],
				"range committed twice")
			committed[id] = true
		}
	}
	d.steps = append(d.steps, fmt.Sprintf("%s:height=%d", event, height))
}

func (d *headerTransportDST) step(value byte) {
	d.t.Helper()
	index := int(value/10) % len(d.current)
	faultPeer := index % 2 // Peer two remains a viable recovery source.
	switch value % 10 {
	case 0:
		d.reply(index, false, false)
	case 1:
		if d.last[index] != nil {
			d.deliver(d.last[index])
		}
	case 2:
		time.Sleep(time.Duration(value+1) * time.Millisecond)
	case 3:
		// Keep one peer responsive while the other requests stall.
		for d.reply(2, false, false) {
		}
		time.Sleep(2 * time.Second)
	case 4:
		if peer := d.current[faultPeer]; peer != nil {
			message := &donePeerMsg{peer: peer}
			d.bm.handlePeerMessage(d.peers, message)
			d.current[faultPeer] = nil
		}
	case 5:
		d.connect(faultPeer)
		d.bm.dispatchHeaderSyncWork(d.peers)
	case 6:
		if peer := d.current[faultPeer]; peer != nil {
			id := peer.Addr()
			d.transport.blocked[id] = !d.transport.blocked[id]
		}
	case 7:
		d.store.unavailable = !d.store.unavailable
	case 8:
		d.reply(faultPeer, true, false)
	case 9:
		d.reply(faultPeer, false, true)
	}
	d.pump()
	d.check(fmt.Sprintf("input:%d", value))
}

type transportDSTResult struct {
	Events   []headersync.Event
	Steps    []string
	Writes   []uint32
	Timeouts int
	Sent     int
}

func runHeaderTransportDST(t *testing.T, script []byte) transportDSTResult {
	t.Helper()
	return runHeaderTransportDSTThrough(t, script, transportDSTHeight)
}

func runHeaderTransportDSTThrough(t *testing.T, script []byte,
	plannedHeight uint32) transportDSTResult {

	t.Helper()
	var result transportDSTResult
	synctest.Test(t, func(t *testing.T) {
		d := newHeaderTransportDSTThrough(t, plannedHeight)
		d.reverseTimers = len(script) > 0 && script[0]&128 != 0
		// Bound resource use for large fuzz inputs.
		for _, value := range script[:min(len(script), 24)] {
			d.step(value)
		}
		d.store.unavailable = false
		clear(d.transport.blocked)
		d.connect(2)
		d.bm.dispatchHeaderSyncWork(d.peers)
		// Heal the environment, not scheduler state. Responses and real
		// deadlines must finish work without directly invoking commit.
		for rounds := 0; rounds < 32; rounds++ {
			if d.lastTip == transportDSTHeight {
				break
			}

			progressed := false
			for peer := range d.current {
				if d.reply(peer, false, false) {
					progressed = true
				}
			}
			if !progressed {
				time.Sleep(2 * time.Second)
			}
			d.pump()
		}
		d.check("healed")
		if d.lastTip != transportDSTHeight {
			manager := d.bm.headerSync.Manager()
			nextID := d.bm.headerSync.Session().NextRangeID()
			for id := headersync.RangeID(1); id < nextID; id++ {
				rng, _, _ := manager.SnapshotRange(
					t.Context(), id,
				)
				t.Logf("range=%+v", rng)
			}
			t.Logf("steps=%v", d.steps)
		}
		require.EqualValues(t, transportDSTHeight, d.lastTip,
			"recovery did not complete; seed=%x", script)
		result = transportDSTResult{
			Events: d.recorder.Events(), Steps: d.steps,
			Writes: d.store.writes, Timeouts: d.timeouts,
			Sent: d.transport.sent,
		}
	})
	return result
}

func TestHeaderTransportDST(t *testing.T) {
	seeds := [][]byte{
		{}, {20, 10, 0}, {0, 1}, {3}, {133}, {4, 5}, {6, 3},
		{8}, {9}, {7, 20, 10, 0, 7}, {20, 21, 3, 4, 5},
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("%x", seed), func(t *testing.T) {
			first := runHeaderTransportDST(t, seed)
			require.Equal(t, first, runHeaderTransportDST(t, seed))
		})
	}
}

func FuzzHeaderTransportDST(f *testing.F) {
	for _, seed := range [][]byte{{}, {0, 1}, {3}, {7, 20, 10, 0}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed []byte) {
		first := runHeaderTransportDST(t, seed)
		require.Equal(t, first, runHeaderTransportDST(t, seed))
	})
}

func TestHeaderTransportCheckpointRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newHeaderTransportDSTThrough(t, 2)
		d.store.unavailable = true
		for peer := range d.current {
			d.reply(peer, false, false)
		}
		d.pump()
		require.Zero(t, d.lastTip)
		require.NotNil(t, d.bm.headerCommitRetryTimer)
		sent := d.transport.sent
		d.store.unavailable = false
		time.Sleep(time.Second)
		d.pump()
		require.EqualValues(t, 2, d.lastTip)
		require.Greater(t, d.transport.sent, sent,
			"retry must dispatch the next checkpoint span")
	})
}

func TestHeaderTransportDuplicatePreservesRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newHeaderTransportDST(t)
		require.True(t, d.reply(0, false, false))
		manager := d.bm.headerSync.Manager()
		peerID := d.current[0].Addr()
		before, ok, err := manager.SnapshotPeer(t.Context(), peerID)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotEqual(t, headersync.NoRange, before.ActiveRange)
		events := d.recorder.Events()
		d.deliver(d.last[0])
		after, ok, err := manager.SnapshotPeer(t.Context(), peerID)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, before, after,
			"duplicate changed ownership or peer quality")
		require.Equal(t, events, d.recorder.Events())
		require.True(t, d.bm.headerSync.Session().RangeActive(peerID))
	})
}

func TestHeaderTransportFailedCompatibilityWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newHeaderTransportDST(t)
		// Remove request matching to exercise the compatibility writer.
		// The original failing seed entered this path with a duplicate.
		d.bm.headerSync.Session().StopAll()
		d.store.unavailable = true
		message := wire.NewMsgHeaders()
		for _, header := range d.headers[:2] {
			require.NoError(t, message.AddBlockHeader(header))
		}
		d.deliver(&headersMsg{peer: d.current[0], headers: message})
		require.Zero(t, d.lastTip)
		require.Empty(t, d.store.writes)
		require.Zero(t, d.bm.headerList.Back().Height)
	})
}

func FuzzHeaderTransportCheckpointDST(f *testing.F) {
	for _, seed := range [][]byte{{}, {7, 0, 7}, {3}, {133}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed []byte) {
		first := runHeaderTransportDSTThrough(t, seed, 2)
		require.Equal(t, first,
			runHeaderTransportDSTThrough(t, seed, 2))
	})
}

func TestHeaderTransportDSTShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newHeaderTransportDST(t)
		require.NotZero(t, d.bm.headerSync.Session().ActiveRangeCount())
		d.bm.scheduleHeaderCommitRetry()
		require.NoError(t, d.bm.Stop())
		time.Sleep(3 * time.Second)
		synctest.Wait()
		require.Empty(t, d.bm.peerChan,
			"request/retry callback survived shutdown")
		require.Zero(t, d.bm.headerSync.Session().ActiveRangeCount())
		d.bm.QueueHeaders(wire.NewMsgHeaders(), d.current[0])
		require.Empty(t, d.bm.peerChan)
	})
}

func TestHeaderCommitRetryGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newHeaderTransportDST(t)
		d.bm.scheduleHeaderCommitRetry()
		first := d.bm.headerCommitRetryTimer
		generation := d.bm.headerCommitRetryID
		d.bm.scheduleHeaderCommitRetry()
		require.Same(t, first, d.bm.headerCommitRetryTimer)
		d.bm.cancelHeaderCommitRetry()
		d.bm.scheduleHeaderCommitRetry()
		replacement := d.bm.headerCommitRetryTimer
		d.bm.handlePeerMessage(d.peers, &headerSyncCommitRetryMsg{
			generation: generation,
		})
		require.Same(t, replacement, d.bm.headerCommitRetryTimer,
			"obsolete wakeup consumed a replacement timer")
	})
}
