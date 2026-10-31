package neutrino

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
	"github.com/lightninglabs/neutrino/sqldb"
	"github.com/stretchr/testify/require"
)

// The chain-store DST runs the production block and filter header handlers
// through blockManager.Start inside one synctest bubble. The harness replaces
// only the network (a scripted header peer and a scripted cfheaders
// responder), the clock, the notification consumer, and the stores' failure
// behavior. Both handlers, the store owner, header validation, the publication
// watermark and the notification stream are production code.
//
// Every store operation takes virtual time, so a rollback on one goroutine
// can overlap a filter batch commit on another. A layout with two writers
// therefore fails the durable binding invariant below under some seeds, while
// the single-owner layout holds it by construction.

const (
	// chainDSTMaxSteps bounds the scripted phase of one run.
	chainDSTMaxSteps = 48

	// chainDSTHealRounds bounds the heal phase. Each round waits one
	// header retry period and redelivers the heaviest chain, like an
	// honest peer answering a repeated getheaders.
	chainDSTHealRounds = 64
)

// chainDSTFault selects how the next matching store operation fails.
type chainDSTFault uint8

const (
	// chainDSTNone leaves the operation alone.
	chainDSTNone chainDSTFault = iota

	// chainDSTDown fails the operation without applying it.
	chainDSTDown

	// chainDSTAckLost applies the operation and then reports an error,
	// like a commit whose acknowledgement was lost.
	chainDSTAckLost

	// chainDSTTorn applies the first half of a write batch and then
	// reports an error. A rollback treats it as chainDSTAckLost.
	chainDSTTorn
)

// chainDSTConfig is the run configuration decoded from the first script
// bytes.
type chainDSTConfig struct {
	// sql selects the SQLite store instead of the in-memory store.
	sql bool

	// latency is the virtual time of every store operation.
	latency time.Duration

	// respDelay is the virtual time before a cfheaders request is
	// answered.
	respDelay time.Duration

	// filterStart is the filter header height already durable at start.
	filterStart uint32

	// paused starts the run with a stalled notification consumer.
	paused bool
}

// decodeChainDSTScript splits a script into its configuration and its event
// bytes. Missing bytes decode as zero, so every byte string is a valid script.
func decodeChainDSTScript(script []byte) (chainDSTConfig, []byte) {
	var head [2]byte
	copy(head[:], script)

	cfg := chainDSTConfig{
		sql:         head[0]&7 == 7,
		latency:     time.Duration(head[0]>>3&3) * time.Millisecond,
		respDelay:   time.Duration(head[0]>>5&7) * time.Millisecond,
		filterStart: uint32(head[1]&0x0f) % (dstChainHeight + 1),
		paused:      head[1]&0x80 != 0,
	}
	if len(script) <= 2 {
		return cfg, nil
	}
	ops := script[2:]

	return cfg, ops[:min(len(ops), chainDSTMaxSteps)]
}

// chainDSTBlocks is a block header store with virtual latency and one-shot
// faults on writes and rollbacks.
type chainDSTBlocks struct {
	headerfs.BlockHeaderStore
	run *chainDSTRun

	mtx      sync.Mutex
	write    chainDSTFault
	rollback chainDSTFault
}

func (s *chainDSTBlocks) take(rollback bool) chainDSTFault {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	fault := &s.write
	if rollback {
		fault = &s.rollback
	}
	mode := *fault
	*fault = chainDSTNone

	return mode
}

// ChainTip reads the tip after the configured latency.
func (s *chainDSTBlocks) ChainTip() (*wire.BlockHeader, uint32, error) {
	s.run.delay()
	return s.BlockHeaderStore.ChainTip()
}

// WriteHeaders applies a pending write fault to one batch.
func (s *chainDSTBlocks) WriteHeaders(headers ...headerfs.BlockHeader) error {
	s.run.delay()

	mode := s.take(false)
	switch mode {
	case chainDSTDown:
		s.run.faulted("block write down")
		return errDSTWrite

	case chainDSTTorn:
		if half := len(headers) / 2; half > 0 {
			err := s.BlockHeaderStore.WriteHeaders(headers[:half]...)
			if err != nil {
				return err
			}
		}
		s.run.faulted("block write torn")
		return errDSTWrite
	}
	if err := s.BlockHeaderStore.WriteHeaders(headers...); err != nil {
		return err
	}
	if mode == chainDSTAckLost {
		s.run.faulted("block write ack lost")
		return errDSTWrite
	}

	return nil
}

// RollbackLastBlock applies a pending rollback fault.
func (s *chainDSTBlocks) RollbackLastBlock() (*headerfs.BlockStamp, error) {
	s.run.delay()

	mode := s.take(true)
	if mode == chainDSTDown {
		s.run.faulted("block rollback down")
		return nil, errDSTWrite
	}
	stamp, err := s.BlockHeaderStore.RollbackLastBlock()
	if err != nil {
		return nil, err
	}
	if mode != chainDSTNone {
		s.run.faulted("block rollback ack lost")
		return nil, errDSTWrite
	}

	return stamp, nil
}

// chainDSTFilters is the filter header counterpart of chainDSTBlocks.
type chainDSTFilters struct {
	headerfs.FilterHeaderStore
	run *chainDSTRun

	mtx      sync.Mutex
	write    chainDSTFault
	rollback chainDSTFault
}

func (s *chainDSTFilters) take(rollback bool) chainDSTFault {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	fault := &s.write
	if rollback {
		fault = &s.rollback
	}
	mode := *fault
	*fault = chainDSTNone

	return mode
}

// ChainTip reads the tip after the configured latency.
func (s *chainDSTFilters) ChainTip() (*chainhash.Hash, uint32, error) {
	s.run.delay()
	return s.FilterHeaderStore.ChainTip()
}

// WriteHeaders applies a pending write fault to one batch.
func (s *chainDSTFilters) WriteHeaders(
	headers ...headerfs.FilterHeader) error {

	s.run.delay()

	mode := s.take(false)
	switch mode {
	case chainDSTDown:
		s.run.faulted("filter write down")
		return errDSTWrite

	case chainDSTTorn:
		if half := len(headers) / 2; half > 0 {
			err := s.FilterHeaderStore.WriteHeaders(headers[:half]...)
			if err != nil {
				return err
			}
		}
		s.run.faulted("filter write torn")
		return errDSTWrite
	}
	if err := s.FilterHeaderStore.WriteHeaders(headers...); err != nil {
		return err
	}
	if mode == chainDSTAckLost {
		s.run.faulted("filter write ack lost")
		return errDSTWrite
	}

	return nil
}

// RollbackLastBlock applies a pending rollback fault.
func (s *chainDSTFilters) RollbackLastBlock(
	newTip *chainhash.Hash) (*headerfs.BlockStamp, error) {

	s.run.delay()

	mode := s.take(true)
	if mode == chainDSTDown {
		s.run.faulted("filter rollback down")
		return nil, errDSTWrite
	}
	stamp, err := s.FilterHeaderStore.RollbackLastBlock(newTip)
	if err != nil {
		return nil, err
	}
	if mode != chainDSTNone {
		s.run.faulted("filter rollback ack lost")
		return nil, errDSTWrite
	}

	return stamp, nil
}

// chainDSTRun is one simulated run.
type chainDSTRun struct {
	t      *testing.T
	script []byte
	cfg    chainDSTConfig
	fx     *dstFixture
	params chaincfg.Params

	// rawBlocks and rawFilters are the stores below the fault wrappers.
	// The oracle reads them directly, so it consumes no virtual time and
	// injects no fault.
	rawBlocks  headerfs.BlockHeaderStore
	rawFilters headerfs.FilterHeaderStore
	blocks     *chainDSTBlocks
	filters    *chainDSTFilters

	bm   *blockManager
	peer *ServerPeer

	// active is the chain the scripted peer serves.
	active []*wire.BlockHeader

	// filterHeader maps a block hash on either branch to the filter
	// header the oracle expects for it.
	filterHeader map[chainhash.Hash]chainhash.Hash

	// filterHash maps a block hash to its filter hash.
	filterHash map[chainhash.Hash]chainhash.Hash

	// The notification consumer is a goroutine that can stall.
	mtx      sync.Mutex
	running  chan struct{}
	paused   bool
	ntfns    []blockntfns.BlockNtfn
	applied  int
	model    []wire.BlockHeader
	done     chan struct{}
	trace    []string
	settled  bool
	faults   int
	requests int
	probes   int
}

// delay consumes the configured virtual latency of one store operation.
func (d *chainDSTRun) delay() {
	if d.cfg.latency > 0 {
		time.Sleep(d.cfg.latency)
	}
}

// faulted records an injected fault in the trace.
func (d *chainDSTRun) faulted(what string) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	d.faults++
	d.trace = append(d.trace, "fault:"+what)
}

// chainDSTFilterHeader derives the oracle's filter header for a block.
func chainDSTFilterHeader(hash, prev chainhash.Hash) (chainhash.Hash,
	chainhash.Hash) {

	filter := chainhash.DoubleHashH(hash[:])
	header := chainhash.DoubleHashH(append(filter[:], prev[:]...))

	return filter, header
}

// newChainDSTRun builds the stores, the manager and the consumer, and starts
// the manager.
func newChainDSTRun(t *testing.T, script []byte,
	db *sqldb.Backend) *chainDSTRun {

	t.Helper()

	cfg, _ := decodeChainDSTScript(script)
	d := &chainDSTRun{
		t: t, script: script, cfg: cfg, fx: dstChains(),
		params:       chaincfg.SimNetParams,
		filterHeader: make(map[chainhash.Hash]chainhash.Hash),
		filterHash:   make(map[chainhash.Hash]chainhash.Hash),
		done:         make(chan struct{}),
	}
	d.params.Checkpoints = nil

	var err error
	if db != nil {
		d.rawBlocks, err = headerfs.NewSQLBlockHeaderStore(
			t.Context(), db.HeaderTxer, &d.params,
		)
		require.NoError(t, err)
		d.rawFilters, err = headerfs.NewSQLFilterHeaderStore(
			t.Context(), db.HeaderTxer, headerfs.RegularFilter,
			&d.params, nil,
		)
		require.NoError(t, err)
	} else {
		d.rawBlocks = newMemBlockHeaderStore(&d.params)
		d.rawFilters, err = newMemFilterHeaderStore(&d.params)
		require.NoError(t, err)
	}

	// The durable state at start is the honest chain with a filter header
	// prefix, as after an interrupted sync.
	var initial []headerfs.BlockHeader
	for h := uint32(1); h <= dstChainHeight; h++ {
		initial = append(initial, headerfs.BlockHeader{
			BlockHeader: d.fx.honest[h], Height: h,
		})
	}
	require.NoError(t, d.rawBlocks.WriteHeaders(initial...))

	genesis, _, err := d.rawFilters.ChainTip()
	require.NoError(t, err)
	d.filterHeader[*d.params.GenesisHash] = *genesis
	for _, branch := range [][]*wire.BlockHeader{
		d.fx.honest, d.fx.reorg,
	} {
		for _, block := range branch[1:] {
			id := block.BlockHash()
			if _, ok := d.filterHeader[id]; ok {
				continue
			}
			prev := d.filterHeader[block.PrevBlock]
			d.filterHash[id], d.filterHeader[id] = chainDSTFilterHeader(
				id, prev,
			)
		}
	}
	for h := uint32(1); h <= cfg.filterStart; h++ {
		id := d.fx.honest[h].BlockHash()
		require.NoError(t, d.rawFilters.WriteHeaders(headerfs.FilterHeader{
			HeaderHash: id, Height: h, FilterHash: d.filterHeader[id],
		}))
	}

	d.blocks = &chainDSTBlocks{BlockHeaderStore: d.rawBlocks, run: d}
	d.filters = &chainDSTFilters{FilterHeaderStore: d.rawFilters, run: d}

	ready := make(chan struct{})
	close(ready)
	bm, err := newBlockManager(&blockManagerCfg{
		ChainParams:      d.params,
		BlockHeaders:     d.blocks,
		RegFilterHeaders: d.filters,
		QueryDispatcher:  &mockDispatcher{},
		Broadcaster:      broadcastFunc(d.serve),
		firstPeerSignal:  ready,
		TimeSource: dstMedianTime{
			MedianTimeSource: blockchain.NewMedianTime(),
			offset: time.Until(
				d.params.GenesisBlock.Header.Timestamp.Add(
					time.Hour,
				),
			),
		},
		BanPeer: func(string, banman.Reason) error { return nil },
	})
	require.NoError(t, err)
	d.bm = bm

	d.peer = newTestServerPeer(
		t, "127.0.0.1:18444", int32(len(d.fx.reorg)-1),
	)
	bm.syncPeer = d.peer
	d.active = d.fx.honest

	// The consumer starts from the notification baseline of a fresh
	// subscriber: the genesis block.
	d.model = []wire.BlockHeader{d.params.GenesisBlock.Header}
	for h := uint32(1); h <= cfg.filterStart; h++ {
		d.model = append(d.model, *d.fx.honest[h])
	}
	d.running = make(chan struct{})
	if !cfg.paused {
		close(d.running)
	} else {
		d.paused = true
	}
	go d.consume()

	bm.Start()
	synctest.Wait()

	return d
}

// consume drains the manager's notification channel, unless paused.
func (d *chainDSTRun) consume() {
	for {
		d.mtx.Lock()
		running := d.running
		d.mtx.Unlock()

		select {
		case <-running:
		case <-d.done:
			return
		}

		select {
		case n := <-d.bm.blockNtfnChan:
			d.mtx.Lock()
			d.ntfns = append(d.ntfns, n)
			d.mtx.Unlock()
		case <-d.done:
			return
		}
	}
}

// setPaused stalls or resumes the notification consumer.
func (d *chainDSTRun) setPaused(paused bool) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	if paused == d.paused {
		return
	}
	d.paused = paused
	if paused {
		d.running = make(chan struct{})
		return
	}
	close(d.running)
}

// serve answers a cfheaders request from the oracle. The request's stop hash
// may name a block on either branch, including one that a reorg has since
// removed, as a real peer's late reply would.
func (d *chainDSTRun) serve(b *querysyncwm.Broadcast,
	opts ...querysyncwm.QueryOption) error {

	if d.cfg.respDelay > 0 {
		time.Sleep(d.cfg.respDelay)
	}
	request, ok := b.Msg.(*wire.MsgGetCFHeaders)
	if !ok {
		return fmt.Errorf("unexpected request %T", b.Msg)
	}

	var chain []*wire.BlockHeader
	stop := -1
	for _, branch := range [][]*wire.BlockHeader{d.fx.reorg, d.fx.honest} {
		for i, block := range branch {
			if block.BlockHash() == request.StopHash {
				chain, stop = branch, i
			}
		}
		if stop >= 0 {
			break
		}
	}
	if stop < 0 || int(request.StartHeight) > stop ||
		request.StartHeight == 0 {

		return fmt.Errorf("unservable request %v..%v",
			request.StartHeight, request.StopHash)
	}
	d.mtx.Lock()
	d.requests++
	d.mtx.Unlock()

	response := &wire.MsgCFHeaders{
		FilterType: request.FilterType,
		StopHash:   request.StopHash,
		PrevFilterHeader: d.filterHeader[chain[request.StartHeight-1].
			BlockHash()],
	}
	for _, block := range chain[request.StartHeight : stop+1] {
		hash := d.filterHash[block.BlockHash()]
		response.FilterHashes = append(response.FilterHashes, &hash)
	}

	return newScriptedBroadcaster(
		map[string][]wire.Message{"honest": {response}},
	).Broadcast(b, opts...)
}

// durableTip returns the durable block tip.
func (d *chainDSTRun) durableTip() (wire.BlockHeader, uint32) {
	header, height, err := d.rawBlocks.ChainTip()
	require.NoError(d.t, err)

	return *header, height
}

// deliver queues a header message from the scripted peer.
func (d *chainDSTRun) deliver(headers []*wire.BlockHeader) {
	if len(headers) == 0 {
		return
	}
	msg := wire.NewMsgHeaders()
	for _, header := range headers {
		require.NoError(d.t, msg.AddBlockHeader(header))
	}
	d.bm.QueueHeaders(msg, d.peer)
}

// suffix returns the part of the peer's chain above the highest block that
// the durable chain already shares with it.
func (d *chainDSTRun) suffix() []*wire.BlockHeader {
	_, tip := d.durableTip()
	for h := min(int(tip), len(d.active)-1); h > 0; h-- {
		stored, err := d.rawBlocks.FetchHeaderByHeight(uint32(h))
		require.NoError(d.t, err)
		if stored.BlockHash() == d.active[h].BlockHash() {
			return d.active[h+1:]
		}
	}

	return d.active[1:]
}

// step runs one scripted event and then the oracle.
func (d *chainDSTRun) step(value byte) {
	event := "noop"
	d.settled = false
	arg := int(value / 10)
	mode := chainDSTFault(1 + arg%3)

	switch value % 10 {
	case 0:
		event = "extend"
		if next := d.suffix(); len(next) > 0 {
			d.deliver(next[:1])
		}

	case 1:
		event = "reorg"
		d.active = d.fx.reorg
		d.deliver(d.suffix())

	case 2:
		event = fmt.Sprintf("arm_block_write_%d", mode)
		d.blocks.mtx.Lock()
		d.blocks.write = mode
		d.blocks.mtx.Unlock()

	case 3:
		event = fmt.Sprintf("arm_filter_write_%d", mode)
		d.filters.mtx.Lock()
		d.filters.write = mode
		d.filters.mtx.Unlock()

	case 4:
		event = fmt.Sprintf("arm_block_rollback_%d", mode)
		d.blocks.mtx.Lock()
		d.blocks.rollback = mode
		d.blocks.mtx.Unlock()

	case 5:
		event = fmt.Sprintf("arm_filter_rollback_%d", mode)
		d.filters.mtx.Lock()
		d.filters.rollback = mode
		d.filters.mtx.Unlock()

	case 6:
		wait := time.Duration(1+arg%9) * time.Millisecond
		if arg%9 == 8 {
			wait = retryTimeout
			d.settled = true
		}
		event = fmt.Sprintf("sleep_%v", wait)
		time.Sleep(wait)

	case 7:
		d.mtx.Lock()
		paused := !d.paused
		d.mtx.Unlock()
		event = fmt.Sprintf("consumer_paused_%v", paused)
		d.setPaused(paused)

	case 8:
		event = "probe"
		d.probe()

	case 9:
		event = "redeliver"
		d.deliver(d.suffix())
	}

	d.mtx.Lock()
	d.trace = append(d.trace, event)
	d.mtx.Unlock()
	synctest.Wait()
	d.check(event)
}

// probe checks that the header control path stays responsive. Removing a
// peer is consumed by blockHandler even while a chain store commit is pending
// and while a stalled notification consumer holds the actor, so it must
// complete without waiting for either.
func (d *chainDSTRun) probe() {
	d.probes++
	peer := newTestServerPeer(
		d.t, fmt.Sprintf("127.0.0.1:%d", 19000+d.probes), 0,
	)
	d.bm.registerHeaderSyncPeer(peer)
	if _, ok := d.bm.headerSync.Peer(peer.Addr()); !ok {
		d.fail("probe", "probe peer was not registered")
	}
	go d.bm.DonePeer(peer)
	synctest.Wait()
	if _, ok := d.bm.headerSync.Peer(peer.Addr()); ok {
		d.fail("probe", "peer removal parked behind the chain store")
	}
}

// fail reports a violated property together with the replay information.
func (d *chainDSTRun) fail(event, format string, args ...any) {
	d.t.Helper()

	d.mtx.Lock()
	trace := strings.Join(d.trace, " ")
	d.mtx.Unlock()
	d.t.Fatalf("event %q: %s\nseed=%s\ntrace: %s", event,
		fmt.Sprintf(format, args...), hex.EncodeToString(d.script),
		trace)
}

// check runs the oracle. Each property is stated against the durable stores
// and the delivered notifications, never against the manager's own caches
// alone.
func (d *chainDSTRun) check(event string) {
	d.t.Helper()

	if n := d.bm.chainStore.rejected.Load(); n != 0 {
		d.fail(event, "the chain store mailbox refused %d commands, "+
			"dropping the headers they carried", n)
	}
	blockHash := d.checkBlocks(event)
	d.checkFilters(event, blockHash)
	d.checkNotifications(event)

	// The published tip is a cache that the store owner updates inside
	// a turn, between store operations that each take virtual time. It
	// is checked only where no turn can be in progress: after a wait
	// longer than any turn, and at the end of the run.
	if d.settled {
		d.checkPublished(event, blockHash)
	}
}

// checkBlocks verifies the block header chain and returns its hashes by
// height.
func (d *chainDSTRun) checkBlocks(event string) []chainhash.Hash {
	d.t.Helper()

	_, tip := d.durableTip()
	hashes := make([]chainhash.Hash, tip+1)
	hashes[0] = *d.params.GenesisHash
	for h := uint32(1); h <= tip; h++ {
		header, err := d.rawBlocks.FetchHeaderByHeight(h)
		if err != nil {
			d.fail(event, "block header %d unreadable: %v", h, err)
		}
		if header.PrevBlock != hashes[h-1] {
			d.fail(event, "block header %d does not extend %d", h,
				h-1)
		}
		hashes[h] = header.BlockHash()
	}

	return hashes
}

// checkFilters verifies the binding invariant: every stored filter header
// belongs to the block the block header store holds at that height, and the
// published filter tip is durable and on the block chain.
func (d *chainDSTRun) checkFilters(event string, blockHash []chainhash.Hash) {
	d.t.Helper()

	_, fh, err := d.rawFilters.ChainTip()
	require.NoError(d.t, err)
	if fh >= uint32(len(blockHash)) {
		d.fail(event, "filter tip %d above block tip %d", fh,
			len(blockHash)-1)
	}
	for h := uint32(1); h <= fh; h++ {
		want := d.filterHeader[blockHash[h]]
		byHeight, err := d.rawFilters.FetchHeaderByHeight(h)
		if err != nil {
			d.fail(event, "filter header %d unreadable: %v", h, err)
		}
		byBlock, err := d.rawFilters.FetchHeader(&blockHash[h])
		if err != nil {
			d.fail(event, "filter header at height %d is not "+
				"bound to block %v: %v", h, blockHash[h], err)
		}
		if *byHeight != want || *byBlock != want {
			d.fail(event, "filter header at height %d does not "+
				"match block %v", h, blockHash[h])
		}
	}

}

// checkPublished verifies that the published filter tip is durable and names
// the block at its height.
func (d *chainDSTRun) checkPublished(event string, blockHash []chainhash.Hash) {
	d.t.Helper()

	_, fh, err := d.rawFilters.ChainTip()
	require.NoError(d.t, err)

	d.bm.newFilterHeadersMtx.RLock()
	published := d.bm.filterHeaderTip
	publishedHash := d.bm.filterHeaderTipHash
	d.bm.newFilterHeadersMtx.RUnlock()
	if published > fh {
		d.fail(event, "published filter tip %d above durable %d",
			published, fh)
	}
	if publishedHash != blockHash[published] {
		d.fail(event, "published filter tip %d names block %v, "+
			"store has %v", published, publishedHash,
			blockHash[published])
	}
}

// checkNotifications replays newly delivered notifications into the
// consumer's model chain. A connect must extend the model tip. A disconnect
// must remove the model tip, and one above the model tip is the rollback of
// a block that was never connected, which only has to describe its own
// parent.
func (d *chainDSTRun) checkNotifications(event string) {
	d.t.Helper()

	d.mtx.Lock()
	pending := append([]blockntfns.BlockNtfn(nil), d.ntfns[d.applied:]...)
	d.applied = len(d.ntfns)
	d.mtx.Unlock()

	for _, n := range pending {
		header := n.Header()
		tipHeight := uint32(len(d.model) - 1)
		tipHash := d.model[tipHeight].BlockHash()

		switch n := n.(type) {
		case *blockntfns.Connected:
			if n.Height() != tipHeight+1 ||
				header.PrevBlock != tipHash {

				d.fail(event, "connect %d %v does not extend "+
					"notified tip %d %v", n.Height(),
					header.BlockHash(), tipHeight, tipHash)
			}
			d.model = append(d.model, header)

		case *blockntfns.Disconnected:
			parent := n.ChainTip()
			if parent.BlockHash() != header.PrevBlock {
				d.fail(event, "disconnect %d names a parent "+
					"that is not its own", n.Height())
			}
			switch {
			case n.Height() > tipHeight:
				// Never connected.

			case n.Height() == tipHeight &&
				header.BlockHash() == tipHash:

				d.model = d.model[:tipHeight]

			default:
				d.fail(event, "disconnect %d %v does not "+
					"remove notified tip %d %v", n.Height(),
					header.BlockHash(), tipHeight, tipHash)
			}
		}
	}
}

// converged reports whether the heal target is reached, and why not.
func (d *chainDSTRun) converged() (bool, string) {
	target := d.fx.reorg[len(d.fx.reorg)-1].BlockHash()
	tip, height := d.durableTip()
	_, fh, err := d.rawFilters.ChainTip()
	require.NoError(d.t, err)

	d.bm.newFilterHeadersMtx.RLock()
	published := d.bm.filterHeaderTip
	d.bm.newFilterHeadersMtx.RUnlock()

	d.mtx.Lock()
	defer d.mtx.Unlock()
	modelHeight := uint32(len(d.model) - 1)
	switch {
	case tip.BlockHash() != target:
		return false, fmt.Sprintf("block tip %d is not the target",
			height)
	case fh != height:
		return false, fmt.Sprintf("filter tip %d below block tip %d",
			fh, height)
	case published != height:
		return false, fmt.Sprintf("published tip %d below %d",
			published, height)
	case modelHeight != height ||
		d.model[modelHeight].BlockHash() != target:

		return false, fmt.Sprintf("notified tip %d below %d",
			modelHeight, height)
	}

	return true, ""
}

// heal clears every fault, resumes the consumer and lets an honest peer
// redeliver the heaviest chain until the stack converges.
func (d *chainDSTRun) heal() {
	d.t.Helper()

	// Let operations that the script left in flight reach their armed
	// faults first. Store latency can otherwise outlast the script, and
	// clearing the faults here would hide them from the run.
	time.Sleep(retryTimeout)
	synctest.Wait()
	d.settled = true
	d.check("pre_heal")

	for _, store := range []*sync.Mutex{&d.blocks.mtx, &d.filters.mtx} {
		store.Lock()
	}
	d.blocks.write, d.blocks.rollback = chainDSTNone, chainDSTNone
	d.filters.write, d.filters.rollback = chainDSTNone, chainDSTNone
	d.blocks.mtx.Unlock()
	d.filters.mtx.Unlock()

	d.setPaused(false)
	d.active = d.fx.reorg
	synctest.Wait()

	var why string
	for round := 0; round < chainDSTHealRounds; round++ {
		d.deliver(d.suffix())
		time.Sleep(retryTimeout)
		synctest.Wait()
		d.settled = true
		d.check("heal")

		var ok bool
		if ok, why = d.converged(); ok {
			d.mtx.Lock()
			d.trace = append(d.trace,
				fmt.Sprintf("healed_round_%d", round))
			d.mtx.Unlock()
			return
		}
	}
	d.fail("heal", "no convergence after %d rounds: %s",
		chainDSTHealRounds, why)
}

// result is the comparable outcome of a run, used to check determinism.
func (d *chainDSTRun) result() string {
	_, height := d.durableTip()
	_, fh, err := d.rawFilters.ChainTip()
	require.NoError(d.t, err)

	d.mtx.Lock()
	defer d.mtx.Unlock()

	return fmt.Sprintf("blocks=%d filters=%d ntfns=%d faults=%d "+
		"requests=%d probes=%d trace=%s", height, fh, len(d.ntfns),
		d.faults, d.requests, d.probes, strings.Join(d.trace, " "))
}

// runChainStoreDST runs one script and returns its outcome.
func runChainStoreDST(t *testing.T, script []byte) string {
	t.Helper()

	cfg, ops := decodeChainDSTScript(script)

	var db *sqldb.Backend
	if cfg.sql {
		// External database startup must use real time.
		db = sqldb.NewTestBackend(t)
	}

	var outcome string
	synctest.Test(t, func(t *testing.T) {
		if db != nil {
			t.Cleanup(func() { require.NoError(t, db.Close()) })
		}
		d := newChainDSTRun(t, script, db)
		d.check("start")

		for _, value := range ops {
			d.step(value)
		}
		d.heal()

		// Shutdown must join the store owner, and a store that was
		// quiet at shutdown must still satisfy every property.
		require.NoError(t, d.bm.Stop())
		close(d.done)
		synctest.Wait()
		d.settled = true
		d.check("stop")

		outcome = d.result()
	})

	return outcome
}

// chainDSTCorpus is the fixed seed corpus. Each entry is replayed twice to
// check determinism. The head bytes select store latency, response delay and
// the initial filter height, the remaining bytes are events (see step).
var chainDSTCorpus = [][]byte{
	// Quiet run, no events.
	{},

	// Reorg while the filter sync is still catching up.
	{0x18, 0x00, 1, 6, 6},

	// Slow store and slow response; reorg, then a response lands mid
	// rollback.
	{0x38, 0x04, 0, 0, 1, 6, 16, 26, 36},

	// A filter write whose acknowledgement is lost, then a reorg.
	{0x18, 0x0a, 13, 6, 1, 6, 6},

	// Block write ack lost during the reorg replacement.
	{0x18, 0x0c, 12, 1, 6, 6, 9},

	// Torn block write.
	{0x08, 0x0c, 22, 1, 9, 9},

	// Block and filter writes that fail without applying.
	{0x08, 0x0c, 32, 1, 9, 9},
	{0x08, 0x0a, 33, 6, 1, 9, 9},

	// Torn filter write.
	{0x08, 0x0a, 23, 6, 1, 9, 9},

	// Rollback faults on both stores.
	{0x18, 0x0c, 5, 15, 1, 9, 9},
	{0x18, 0x0c, 4, 14, 1, 9, 9},

	// Slow consumer across a reorg.
	{0x18, 0x0c, 7, 1, 8, 6, 16, 8, 7, 6},
	{0x98, 0x8c, 1, 8, 6, 26, 8},

	// SQLite store.
	{0x07, 0x0c, 1, 6, 6},
	{0x1f, 0x05, 3, 1, 6, 36, 6},
}

// TestChainStoreDST replays the corpus and checks that each run is
// deterministic.
func TestChainStoreDST(t *testing.T) {
	for _, script := range chainDSTCorpus {
		t.Run(hex.EncodeToString(script), func(t *testing.T) {
			first := runChainStoreDST(t, script)
			require.Equal(t, first, runChainStoreDST(t, script),
				"same seed produced a different trace")
		})
	}
}

// TestChainStoreDSTFaultCoverage checks that the corpus entries meant to
// exercise a write fault do fire it. The fault mode is 1 + (value/10)%3, so
// the tens digit 0, 1 and 2 select down, ack lost and torn.
func TestChainStoreDSTFaultCoverage(t *testing.T) {
	for _, test := range []struct {
		script []byte
		fault  string
	}{
		{[]byte{0x18, 0x0a, 13, 6, 1, 6, 6}, "fault:filter write ack lost"},
		{[]byte{0x18, 0x0c, 12, 1, 6, 6, 9}, "fault:block write ack lost"},
		{[]byte{0x08, 0x0c, 22, 1, 9, 9}, "fault:block write torn"},
		{[]byte{0x08, 0x0c, 32, 1, 9, 9}, "fault:block write down"},
		{[]byte{0x08, 0x0a, 33, 6, 1, 9, 9}, "fault:filter write down"},
		{[]byte{0x08, 0x0a, 23, 6, 1, 9, 9}, "fault:filter write torn"},
	} {
		t.Run(test.fault, func(t *testing.T) {
			outcome := runChainStoreDST(t, test.script)
			require.Contains(t, outcome, test.fault)
		})
	}
}

// chainDSTSwarmScript derives one swarm script.
func chainDSTSwarmScript(rng *rand.Rand) []byte {
	script := make([]byte, 2+rng.IntN(chainDSTMaxSteps))
	for i := range script {
		script[i] = byte(rng.Uint32())
	}

	return script
}

// TestChainStoreDSTSwarm runs a reproducible campaign. -dst.seed picks the
// generator and -dst.swarm the number of scripts. A failure prints the script
// in hex, which TestChainStoreDSTReplay replays.
func TestChainStoreDSTSwarm(t *testing.T) {
	rng := rand.New(rand.NewPCG(*dstSwarmSeedFlag, 1))
	start := time.Now()
	for i := 0; i < *dstSwarmRuns; i++ {
		script := chainDSTSwarmScript(rng)
		ok := t.Run(fmt.Sprintf("%d_%x", i, script), func(t *testing.T) {
			runChainStoreDST(t, script)
		})
		if !ok {
			t.Fatalf("swarm script %d failed after %d runs: %x", i,
				i+1, script)
		}
	}
	elapsed := time.Since(start)
	t.Logf("chain store swarm: seed=%d runs=%d elapsed=%v runs/s=%.1f",
		*dstSwarmSeedFlag, *dstSwarmRuns, elapsed,
		float64(*dstSwarmRuns)/elapsed.Seconds())
}

// TestChainStoreDSTReplay replays one script given with -dst.replay.
func TestChainStoreDSTReplay(t *testing.T) {
	if *dstReplay == "" {
		t.Skip("no -dst.replay seed")
	}
	script, err := hex.DecodeString(*dstReplay)
	require.NoError(t, err)
	runChainStoreDST(t, script)
}

// FuzzChainStoreDST explores scripts under the coverage-guided fuzzer.
func FuzzChainStoreDST(f *testing.F) {
	for _, script := range chainDSTCorpus {
		f.Add(script)
	}
	f.Fuzz(func(t *testing.T, script []byte) {
		first := runChainStoreDST(t, script)
		if sha256.Sum256(script)[0]%16 == 0 {
			require.Equal(t, first, runChainStoreDST(t, script))
		}
	})
}
