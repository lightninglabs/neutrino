// NOTE: THIS API IS UNSTABLE RIGHT NOW AND WILL GO MOSTLY PRIVATE SOON.

package neutrino

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/lightninglabs/neutrino/chainsync"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/lightninglabs/neutrino/headersync"
	"github.com/lightninglabs/neutrino/headersync/querydispatch"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
)

const (
	// numMaxMemHeaders is the max number of headers to store in memory for
	// a particular peer. By bounding this value, we're able to closely
	// control our effective memory usage during initial sync and re-org
	// handling. This value should be set a "sane" re-org size, such that
	// we're able to properly handle re-orgs in size strictly less than
	// this value.
	numMaxMemHeaders = 10000

	// retryTimeout is the time we'll wait between failed queries to fetch
	// filter checkpoints and headers.
	retryTimeout = 3 * time.Second

	// defaultHeaderSyncTimeout is the time we'll wait for the active block
	// header sync peer to answer a getheaders request before failing over
	// to another candidate.
	defaultHeaderSyncTimeout = 30 * time.Second

	// headerSyncOpTimeout bounds one driver event from the block handler.
	// Shutdown cancels it early.
	headerSyncOpTimeout = 5 * time.Second

	// maxHeaderSyncTimeout caps RTT-derived header sync timeouts. This
	// keeps a very slow ping sample from creating another long global
	// stall.
	maxHeaderSyncTimeout = 2 * time.Minute

	// headerSyncRTTMultiplier scales the latest ping RTT into a per-peer
	// header request timeout floor.
	headerSyncRTTMultiplier = 8

	// defaultHeaderPeerCooldown is the minimum decay interval before a
	// blocked or quarantined header peer can be considered schedulable
	// again if it remains connected.
	defaultHeaderPeerCooldown = headersync.DefaultPeerCooldown

	// defaultHeaderPeerQueryQuarantine is the short exact-address
	// exclusion used to keep a rejected header peer out of query dispatch
	// immediately after a fork/timeout. This is intentionally shorter than
	// the header scheduling cooldown so legitimate reorg recovery can
	// still happen promptly when that peer's chain later becomes best.
	defaultHeaderPeerQueryQuarantine = 10 * time.Second

	// headerSyncActorMailboxSize sizes the mailbox of the querysync
	// dispatch actor that headersync uses.
	headerSyncActorMailboxSize = 128

	// headerSyncBootstrapMinPeers is the minimum candidate peer set we try
	// to collect before leasing the first header range. This avoids giving
	// the genesis checkpoint range to the first peer that happens to
	// connect when we have no RTT or response-quality samples yet.
	headerSyncBootstrapMinPeers = 3

	// headerSyncBootstrapMaxWait caps the cold-start admission delay so
	// sparse networks can still sync from a single viable peer.
	headerSyncBootstrapMaxWait = 2 * time.Second

	// maxCFCheckptsPerQuery is the maximum number of filter header
	// checkpoints we can query for within a single message over the wire.
	maxCFCheckptsPerQuery = wire.MaxCFHeadersPerMsg / wire.CFCheckptInterval

	// maxCFHeadersMsgsPerWriteBatch caps how many contiguous cfheaders
	// responses we commit in a single ordered store transaction. Each
	// response carries up to wire.MaxCFHeadersPerMsg filter hashes, so
	// this caps the hot initial-sync write at roughly 64k rows while
	// avoiding thousands of tiny sqlite transactions.
	maxCFHeadersMsgsPerWriteBatch = 32

	// cfHeaderCollectTimeout bounds uncheckpointed response collection.
	// Early agreement is not authentication: collect remaining peers so a
	// prompt honest conflict still reaches mismatch detection.
	cfHeaderCollectTimeout = 2 * time.Second

	// checkpointedCFHeadersMinBatchTimeout is the minimum total timeout
	// for checkpointed cfheader batch queries. The legacy default of 30s
	// is too short for large initial-sync batches because it cancels all
	// remaining 2,000-header range requests after one slow segment.
	checkpointedCFHeadersMinBatchTimeout = 2 * time.Minute

	// checkpointedCFHeadersPerQueryTimeout scales the total batch timeout
	// with the number of range requests. This is an upper bound only; fast
	// batches still finish as soon as all requests return.
	checkpointedCFHeadersPerQueryTimeout = 250 * time.Millisecond

	// checkpointedCFHeadersMaxBatchTimeout keeps a pathological peer mix
	// from pinning cfheader sync forever while still allowing
	// testnet3-size batches enough room to retry and steal work.
	checkpointedCFHeadersMaxBatchTimeout = 15 * time.Minute
)

// zeroHash is the zero value hash (all zeros). It is defined as a convenience.
var zeroHash chainhash.Hash

// newPeerMsg signifies a newly connected peer to the block handler.
type newPeerMsg struct {
	peer *ServerPeer
}

// invMsg packages a bitcoin inv message and the peer it came from together so
// the block handler has access to that information.
type invMsg struct {
	inv  *wire.MsgInv
	peer *ServerPeer
}

// headersMsg packages a bitcoin headers message and the peer it came from
// together so the block handler has access to that information.
type headersMsg struct {
	headers *wire.MsgHeaders
	peer    *ServerPeer
}

// donePeerMsg signifies a newly disconnected peer to the block handler.
type donePeerMsg struct {
	peer *ServerPeer
}

// headerSyncTimeoutMsg is sent when the active block header sync peer has not
// answered the outstanding getheaders request within the derived timeout.
type headerSyncTimeoutMsg struct {
	peer        *ServerPeer
	requestID   uint64
	startHeight uint32
	timeout     time.Duration
	rangeID     headersync.RangeID
	leaseEpoch  uint64
	anchor      bool
}

// headerSyncBootstrapMsg asks the block handler to retry initial header sync
// after the cold-start peer admission window expires.
type headerSyncBootstrapMsg struct{}

// headerSyncCooldownMsg wakes the block handler when a header peer's cooldown
// expires, so the peer can be scheduled again without waiting for an
// unrelated event.
type headerSyncCooldownMsg struct {
	generation uint64
}

// headerSyncCommitRetryMsg retries retained staging after a storage failure.
type headerSyncCommitRetryMsg struct {
	generation uint64
}

// blockManagerCfg holds options and dependencies needed by the blockManager
// during operation.
type blockManagerCfg struct {
	// ChainParams is the chain that we're running on.
	ChainParams chaincfg.Params

	// BlockHeaders is the store where blockheaders are persistently
	// stored.
	BlockHeaders headerfs.BlockHeaderStore

	// RegFilterHeaders is the store where filter headers for the regular
	// compact filters are persistently stored.
	RegFilterHeaders headerfs.FilterHeaderStore

	// TimeSource is used to access a time estimate based on the clocks of
	// the connected peers.
	TimeSource blockchain.MedianTimeSource

	// QueryDispatcher is used to make queries to connected Bitcoin peers.
	QueryDispatcher querysyncwm.Dispatcher

	// BanPeer bans and disconnects the given peer.
	BanPeer func(addr string, reason banman.Reason) error

	// DisconnectPeer disconnects a peer without recording a ban.
	DisconnectPeer func(addr string) error

	// QuarantinePeer temporarily excludes one exact peer address from
	// query dispatch without applying an IP-wide ban.
	QuarantinePeer func(addr string, duration time.Duration)

	// GetBlock fetches a block from the p2p network.
	GetBlock func(chainhash.Hash, ...QueryOption) (*btcutil.Block, error)

	// firstPeerSignal is a channel that's sent upon once the main daemon
	// has made its first peer connection. We use this to ensure we don't
	// try to perform any queries before we have our first peer.
	firstPeerSignal <-chan struct{}

	// HeaderSyncTimeout is the base timeout for block header getheaders
	// requests. If zero, defaultHeaderSyncTimeout is used.
	HeaderSyncTimeout time.Duration

	// HeaderTransport sends header requests to the peer. If nil, requests
	// use the existing Bitcoin peer transport.
	HeaderTransport headerTransport

	// Broadcaster sends a single message to all connected peers and
	// collects their responses. It backs the all-peer checks used to
	// sanity check compact filter headers and checkpoints.
	Broadcaster querysyncwm.Broadcaster
}

// blockManager provides a concurrency safe block manager for handling all
// incoming blocks.
type blockManager struct { // nolint:maligned
	started  int32 // To be used atomically.
	shutdown int32 // To be used atomically.

	cfg *blockManagerCfg

	// blkHeaderProgressLogger is a progress logger that we'll use to
	// update the number of blocker headers we've processed in the past 10
	// seconds within the log.
	blkHeaderProgressLogger *headerProgressLogger

	// fltrHeaderProgessLogger is a process logger similar to the one
	// above, but we'll use it to update the progress of the set of filter
	// headers that we've verified in the past 10 seconds.
	fltrHeaderProgessLogger *headerProgressLogger

	// genesisHeader is the filter header of the genesis block.
	genesisHeader chainhash.Hash

	// headerTip will be set to the current block header tip at all times.
	// Callers MUST hold the lock below each time they read/write from this
	// field.
	headerTip uint32

	// headerTipHash will be set to the hash of the current block header
	// tip at all times. Callers MUST hold the lock below each time they
	// read/write from this field.
	headerTipHash chainhash.Hash

	// publishedBlockTip is an immutable status snapshot safe to read while
	// a store write holds its internal lock.
	publishedBlockTip atomic.Pointer[headerfs.BlockStamp]

	// newHeadersMtx is the mutex that should be held when reading/writing
	// the headerTip variable above.
	//
	// NOTE: When using this mutex along with newFilterHeadersMtx at the
	// same time, newHeadersMtx should always be acquired first.
	newHeadersMtx sync.RWMutex

	// newHeadersSignal is condition variable which will be used to notify
	// any waiting callers (via Broadcast()) that the tip of the current
	// chain has changed. This is useful when callers need to know we have
	// a new tip, but not necessarily each block that was connected during
	// switch over.
	newHeadersSignal *sync.Cond

	// filterHeaderTip will be set to the height of the current filter
	// header tip at all times. Callers MUST hold the lock below each time
	// they read/write from this field.
	filterHeaderTip uint32

	// filterHeaderTipHash will be set to the current block hash of the
	// block at height filterHeaderTip at all times. Callers MUST hold the
	// lock below each time they read/write from this field.
	filterHeaderTipHash chainhash.Hash

	// newFilterHeadersMtx is the mutex that should be held when
	// reading/writing the filterHeaderTip variable above.
	//
	// NOTE: When using this mutex along with newHeadersMtx at the same
	// time, newHeadersMtx should always be acquired first.
	newFilterHeadersMtx sync.RWMutex

	// newFilterHeadersSignal is condition variable which will be used to
	// notify any waiting callers (via Broadcast()) that the tip of the
	// current filter header chain has changed. This is useful when callers
	// need to know we have a new tip, but not necessarily each filter
	// header that was connected during switch over.
	newFilterHeadersSignal *sync.Cond

	// syncPeer points to the peer that we're currently syncing block
	// headers from.
	syncPeer *ServerPeer

	// syncPeerMutex protects the above syncPeer pointer at all times.
	syncPeerMutex sync.RWMutex

	// peerChan is a channel for messages that come from peers
	peerChan chan interface{}

	// chainStore owns runtime header store mutations once integrated.
	chainStore          *chainstore.Client
	storeOps            *chainstore.Store
	chainStoreResult    chan chainstore.Result
	pendingHeaderCommit *pendingChainHeaderCommit
	donePeerChan        chan *ServerPeer
	timerChan           chan interface{}
	timeoutChan         chan *headerSyncTimeoutMsg

	// blockNtfnChan is a channel in which the latest block notifications
	// for the tip of the chain will be sent upon.
	blockNtfnChan chan blockntfns.BlockNtfn

	wg   sync.WaitGroup
	quit chan struct{}

	// quitCtx is cancelled on shutdown, so pending headersync driver events
	// return at once instead of waiting out their deadline.
	quitCtx    context.Context
	quitCancel context.CancelFunc

	headerList     headerlist.Chain
	reorgList      headerlist.Chain
	nextCheckpoint *chaincfg.Checkpoint
	lastRequested  chainhash.Hash

	headerSyncTimeout time.Duration

	// serialRequestID identifies the serial getheaders deadline. It is
	// separate from the driver's request IDs so that arming a range or
	// discovery deadline does not void a serial timeout that is still
	// pending.
	serialRequestID uint64

	headerSyncTimer        *time.Timer
	headerCommitRetryTimer *time.Timer
	headerCommitRetryID    uint64

	headerCooldownTimer *time.Timer
	headerCooldownID    uint64

	headerSyncBootstrapFirstSeen time.Time
	headerSyncBootstrapTimer     *time.Timer

	headerSync *headersync.Runtime

	minRetargetTimespan int64 // target timespan / adjustment factor
	maxRetargetTimespan int64 // target timespan * adjustment factor
	blocksPerRetarget   int32 // target timespan / target time per block
}

// newBlockManager returns a new bitcoin block manager. Use Start to begin
// processing asynchronous block and inv updates.
func newBlockManager(cfg *blockManagerCfg) (*blockManager, error) {
	retarget, err := chainsync.NewRetarget(cfg.ChainParams)
	if err != nil {
		return nil, err
	}

	bm := blockManager{
		cfg:              cfg,
		peerChan:         make(chan interface{}, MaxPeers*3),
		donePeerChan:     make(chan *ServerPeer, MaxPeers),
		timerChan:        make(chan interface{}, MaxPeers*3),
		timeoutChan:      make(chan *headerSyncTimeoutMsg, MaxPeers*3),
		chainStoreResult: make(chan chainstore.Result, 1),
		blockNtfnChan:    make(chan blockntfns.BlockNtfn),
		blkHeaderProgressLogger: newBlockProgressLogger(
			"Processed", "block", log,
		),
		fltrHeaderProgessLogger: newBlockProgressLogger(
			"Verified", "filter header", log,
		),
		headerList: headerlist.NewBoundedMemoryChain(
			numMaxMemHeaders,
		),
		reorgList: headerlist.NewBoundedMemoryChain(
			numMaxMemHeaders,
		),
		headerSyncTimeout:   cfg.HeaderSyncTimeout,
		quit:                make(chan struct{}),
		blocksPerRetarget:   retarget.BlocksPerRetarget,
		minRetargetTimespan: retarget.MinRetargetTimespan,
		maxRetargetTimespan: retarget.MaxRetargetTimespan,
	}
	bm.storeOps = chainstore.NewStore(&chainStoreHost{manager: &bm})
	if bm.headerSyncTimeout == 0 {
		bm.headerSyncTimeout = defaultHeaderSyncTimeout
	}
	bm.quitCtx, bm.quitCancel = context.WithCancel(context.Background())

	// Next we'll create the two signals that goroutines will use to wait
	// on a particular header chain height before starting their normal
	// duties.
	bm.newHeadersSignal = sync.NewCond(&bm.newHeadersMtx)
	bm.newFilterHeadersSignal = sync.NewCond(&bm.newFilterHeadersMtx)

	// We fetch the genesis header to use for verifying the first received
	// interval.
	genesisHeader, err := cfg.RegFilterHeaders.FetchHeaderByHeight(0)
	if err != nil {
		return nil, err
	}
	bm.genesisHeader = *genesisHeader

	// Initialize the next checkpoint based on the current height.
	header, height, err := cfg.BlockHeaders.ChainTip()
	if err != nil {
		return nil, err
	}
	bm.nextCheckpoint = bm.findNextHeaderCheckpoint(int32(height))
	bm.headerList.ResetHeaderState(headerlist.Node{
		Header: *header,
		Height: int32(height),
	})
	bm.headerTip = height
	bm.headerTipHash = header.BlockHash()
	bm.publishedBlockTip.Store(&headerfs.BlockStamp{
		Height: int32(height), Hash: bm.headerTipHash,
		Timestamp: header.Timestamp,
	})
	if err := bm.initHeaderSyncManager(
		height,
		bm.headerTipHash,
	); err != nil {
		return nil, err
	}

	// Finally, we'll set the filter header tip so any goroutines waiting
	// on the condition obtain the correct initial state.
	_, bm.filterHeaderTip, err = cfg.RegFilterHeaders.ChainTip()
	if err != nil {
		return nil, err
	}

	// We must also ensure the filter header tip hash is set to the block
	// hash at the filter tip height.
	fh, err := cfg.BlockHeaders.FetchHeaderByHeight(bm.filterHeaderTip)
	if err != nil {
		return nil, err
	}
	bm.filterHeaderTipHash = fh.BlockHash()

	// The chain store actor exists from construction so that every
	// header and filter header write goes through it, whether or not
	// the handlers run. Stop joins it.
	bm.chainStore, err = chainstore.NewClient(bm.storeOps)
	if err != nil {
		return nil, err
	}

	return &bm, nil
}

func (b *blockManager) initHeaderSyncManager(height uint32,
	hash chainhash.Hash) error {

	if b.headerSync != nil {
		b.headerSync.Stop()
	}

	runtime, seededCheckpoints := querydispatch.NewRuntime(
		headersync.ChainPoint{
			Height: height,
			Hash:   hash,
		},
		b.cfg.ChainParams.Checkpoints,
		headerSyncActorMailboxSize,
	)
	b.headerSync = runtime

	log.Infof("Initialized headersync manager at tip height=%d "+
		"hash=%s with %d trusted checkpoint anchors", height,
		hash, seededCheckpoints)

	return nil
}

// ResetHeaderState re-reads the chain tips from the header stores and
// reinitializes the block manager's internal tracking state. This must be
// called after headers have been imported into the stores outside of the block
// manager (e.g., via chainimport) but before the block manager is started, so
// that it begins syncing from the correct chain tip rather than the stale
// state captured at construction time.
func (b *blockManager) ResetHeaderState() error {
	// Re-read the block header chain tip from the store.
	header, height, err := b.cfg.BlockHeaders.ChainTip()
	if err != nil {
		return fmt.Errorf("failed to read block header chain "+
			"tip: %w", err)
	}
	b.nextCheckpoint = b.findNextHeaderCheckpoint(int32(height))
	b.headerList.ResetHeaderState(headerlist.Node{
		Header: *header,
		Height: int32(height),
	})
	b.headerTip = height
	b.headerTipHash = header.BlockHash()
	b.publishedBlockTip.Store(&headerfs.BlockStamp{
		Height: int32(height), Hash: b.headerTipHash,
		Timestamp: header.Timestamp,
	})
	if err := b.initHeaderSyncManager(height, b.headerTipHash); err != nil {
		return err
	}

	// Re-read the filter header chain tip from the store.
	_, b.filterHeaderTip, err = b.cfg.RegFilterHeaders.ChainTip()
	if err != nil {
		return fmt.Errorf("failed to read filter header chain "+
			"tip: %w", err)
	}

	// Ensure the filter header tip hash is set to the block hash at the
	// filter tip height.
	fh, err := b.cfg.BlockHeaders.FetchHeaderByHeight(b.filterHeaderTip)
	if err != nil {
		return fmt.Errorf("failed to fetch block header at filter "+
			"tip height %d: %w", b.filterHeaderTip, err)
	}
	b.filterHeaderTipHash = fh.BlockHash()

	log.Infof("Block manager header state reset: block tip=%d, "+
		"filter tip=%d", height, b.filterHeaderTip)

	return nil
}

// Start begins the core block handler which processes block and inv messages.
func (b *blockManager) Start() {
	// Already started?
	if atomic.AddInt32(&b.started, 1) != 1 {
		return
	}

	log.Trace("Starting block manager")
	b.startHeaderSyncActors()

	b.wg.Add(2)
	go b.blockHandler()
	go func() {
		defer b.wg.Done()

		log.Debug("Waiting for peer connection...")

		// Before starting the cfHandler we want to make sure we are
		// connected with at least one peer.
		select {
		case <-b.cfg.firstPeerSignal:
		case <-b.quit:
			return
		}

		log.Debug("Peer connected, starting cfHandler.")
		b.cfHandler()
	}()
}

// Stop gracefully shuts down the block manager by stopping all asynchronous
// handlers and waiting for them to finish.
func (b *blockManager) Stop() error {
	if atomic.AddInt32(&b.shutdown, 1) != 1 {
		log.Warnf("Block manager is already in the process of " +
			"shutting down")
		return nil
	}

	// We'll send out update signals before the quit to ensure that any
	// goroutines waiting on them will properly exit.
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Millisecond * 50)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
			}

			b.newHeadersSignal.Broadcast()
			b.newFilterHeadersSignal.Broadcast()
		}
	}()

	log.Infof("Block manager shutting down")
	close(b.quit)
	b.quitCancel()
	b.wg.Wait()

	// Stop every block handler timer. A callback that fires after quit
	// closed sees both quit and a buffered peerChan ready, and select may
	// still enqueue a message nobody will read.
	b.cancelHeaderCommitRetry()
	b.disarmHeaderSyncTimeout()
	b.clearHeaderSyncBootstrapDelay()
	b.cancelHeaderCooldownWakeup()
	b.stopHeaderSyncActors()

	if err := b.chainStore.Stop(); err != nil {
		close(done)
		return err
	}
	close(done)
	return nil
}

func (b *blockManager) startHeaderSyncActors() {
	if b.headerSync == nil {
		return
	}

	b.headerSync.Start(context.Background())
	log.Infof("Started headersync manager with querysync-backed " +
		"dispatch")
}

func (b *blockManager) stopHeaderSyncActors() {
	b.headerSync.Stop()
}

// NewPeer informs the block manager of a newly active peer.
func (b *blockManager) NewPeer(sp *ServerPeer) {
	// Ignore if we are shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	select {
	case b.peerChan <- &newPeerMsg{peer: sp}:
	case <-b.quit:
		return
	}
}

// handleNewPeerMsg deals with new peers that have signalled they may be
// considered as a sync peer (they have already successfully negotiated). It
// also starts syncing if needed. It is invoked from the syncHandler goroutine.
func (b *blockManager) handleNewPeerMsg(peers *list.List, sp *ServerPeer) {
	// Ignore if in the process of shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	log.Infof("New valid peer %s (%s)", sp, sp.UserAgent())

	// Ignore the peer if it's not a sync candidate.
	if !b.isSyncCandidate(sp) {
		return
	}

	// Add the peer as a candidate to sync from.
	peers.PushBack(sp)
	b.registerHeaderSyncPeer(sp)

	// If we're current with our sync peer and the new peer is advertising
	// a higher block than the newest one we know of, request headers from
	// the new peer.
	_, height, err := b.cfg.BlockHeaders.ChainTip()
	if err != nil {
		log.Criticalf("Couldn't retrieve block header chain tip: %s",
			err)
		return
	}
	if height < uint32(sp.StartingHeight()) && b.BlockHeadersSynced() {
		locator, err := b.cfg.BlockHeaders.LatestBlockLocator()
		if err != nil {
			log.Criticalf("Couldn't retrieve latest block "+
				"locator: %s", err)
			return
		}
		stopHash := &zeroHash
		err = b.requestHeaders(sp, locator, stopHash, height)
		if err != nil {
			log.Warnf("Failed to send getheaders message to "+
				"peer %s: %s", sp.Addr(), err)
		}
	}

	// While parallel header sync is running, startSync returns at once,
	// so hand the new peer work directly. Otherwise a sync whose other
	// peers are all blocked, with no request or timer outstanding, never
	// schedules anything again: a new peer is the event that unsticks it.
	if b.SyncPeer() != nil && !b.allowSerialHeaderFallback() {
		b.continueHeaderSync(peers)
		return
	}

	// Start syncing by choosing the best candidate if needed.
	b.startSync(peers)
}

// DonePeer informs the blockmanager that a peer has disconnected.
func (b *blockManager) DonePeer(sp *ServerPeer) {
	// Ignore if we are shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	select {
	case b.donePeerChan <- sp:
	case <-b.quit:
		return
	}
}

// handleDonePeerMsg deals with peers that have signalled they are done. It
// removes the peer as a candidate for syncing and in the case where it was the
// current sync peer, attempts to select a new best peer to sync from. It is
// invoked from the syncHandler goroutine.
func (b *blockManager) handleDonePeerMsg(peers *list.List, sp *ServerPeer) {
	// Remove the peer from the list of candidate peers.
	for e := peers.Front(); e != nil; e = e.Next() {
		if e.Value == sp {
			peers.Remove(e)
			break
		}
	}

	log.Infof("Lost peer %s", sp)
	done := b.handleDriverEvent(
		nil, headersync.PeerDone{PeerID: sp.Addr()},
	)
	hadHeaderSyncWork := done.result.HadWork
	b.unregisterHeaderSyncPeer(sp)

	// Attempt to find a new peer to sync from if the quitting peer is the
	// sync peer. Also, reset the header state.
	if b.SyncPeer() != nil && b.SyncPeer() == sp {
		b.syncPeerMutex.Lock()
		b.syncPeer = nil
		b.syncPeerMutex.Unlock()
		if b.pendingHeaderCommit != nil {
			return
		}
		// Reset the cache and the visible tip together, so they keep
		// describing the same header.
		if err := b.resetHeaderListToTip(); err != nil {
			return
		}
		b.startSync(peers)
		return
	}

	if hadHeaderSyncWork && b.pendingHeaderCommit == nil {
		b.continueHeaderSync(peers)
	}
}

// sendHeaderTimeout keeps timeout decisions behind queued peer responses
// while allowing other controls to run during a store mutation. A response
// arriving just after a deadline can also be handled before punishment.
func (b *blockManager) sendHeaderTimeout(msg *headerSyncTimeoutMsg) {
	select {
	case b.timeoutChan <- msg:
	case <-b.quit:
	}
}

// blockHandler is the main handler for the block manager. It must be run as a
// goroutine. It processes block and inv messages in a separate goroutine from
// the peer handlers so the block (MsgBlock) messages are handled by a single
// thread without needing to lock memory data structures. This is important
// because the block manager controls which blocks are needed and how the
// fetching should proceed.
func (b *blockManager) blockHandler() {
	defer b.wg.Done()

	candidatePeers := list.New()
	var timeout *headerSyncTimeoutMsg
	var queuedBeforeTimeout int
out:
	for {
		if timeout != nil && queuedBeforeTimeout == 0 &&
			b.pendingHeaderCommit == nil {

			b.handleHeaderSyncTimeoutMsg(candidatePeers, timeout)
			timeout = nil
			continue
		}
		peerChan := b.peerChan
		timeoutChan := b.timeoutChan
		if b.pendingHeaderCommit != nil {
			peerChan, timeoutChan = nil, nil
		}
		if timeout != nil {
			timeoutChan = nil
		}
		select {
		case m := <-peerChan:
			b.handlePeerMessage(candidatePeers, m)
			if timeout != nil && queuedBeforeTimeout > 0 {
				queuedBeforeTimeout--
			}
		case peer := <-b.donePeerChan:
			b.handleDonePeerMsg(candidatePeers, peer)
		case timer := <-b.timerChan:
			b.handlePeerMessage(candidatePeers, timer)
		case msg := <-timeoutChan:
			timeout = msg
			queuedBeforeTimeout = len(b.peerChan)
		case result := <-b.chainStoreResult:
			b.finishAsyncHeaderCommit(candidatePeers, result)
		case <-b.quit:
			break out
		}
	}

	log.Trace("Block handler done")
}

// handlePeerMessage is the serialized peer-event entry point. Keeping dispatch
// separate from the goroutine lets virtual-time transports exercise the same
// response, disconnect, and timer handlers without starting filter sync.
func (b *blockManager) handlePeerMessage(peers *list.List, message any) {
	switch msg := message.(type) {
	case *newPeerMsg:
		if peerDisconnected(msg.peer) {
			return
		}
		b.handleNewPeerMsg(peers, msg.peer)
	case *invMsg:
		if peerDisconnected(msg.peer) {
			return
		}
		b.handleInvMsg(msg)
	case *headersMsg:
		if peerDisconnected(msg.peer) {
			return
		}
		b.handleHeadersMsgWithPeers(peers, msg)
	case *headerSyncTimeoutMsg:
		b.handleHeaderSyncTimeoutMsg(peers, msg)
	case *headerSyncBootstrapMsg:
		b.headerSyncBootstrapTimer = nil
		b.startSync(peers)
	case *headerSyncCooldownMsg:
		if msg.generation != b.headerCooldownID {
			return
		}
		b.headerCooldownTimer = nil

		// Dispatch recovers every peer whose cooldown expired; arm
		// the next wakeup for any still cooling down. While a serial
		// request is in flight, resume the planned work and discovery
		// but do not start the serial path: continuing sync could
		// reselect the sync peer under that request. Dispatch alone
		// would leave a span that waited on the cooling peers without
		// any discovery to restart it.
		if b.headerSyncTimer != nil {
			b.resumeHeaderSyncWork(peers)
		} else {
			b.continueHeaderSync(peers)
		}
		b.handleDriverEvent(nil, headersync.CooldownWakeDue{})
	case *headerSyncCommitRetryMsg:
		if msg.generation != b.headerCommitRetryID {
			return
		}
		b.headerCommitRetryTimer = nil
		if err := b.commitReadyHeaderSyncRanges(); err != nil {
			log.Warnf("headersync commit_retry_failed: %v", err)
			b.rejectInvalidHeaderCommit(peers, err)
			return
		}
		b.continueHeaderSync(peers)
	case *donePeerMsg:
		b.handleDonePeerMsg(peers, msg.peer)
	default:
		log.Warnf("Invalid message type in block handler: %T", msg)
	}
}

// peerDisconnected rejects queued work from a peer whose disconnect overtook
// its message while a chain store mutation was pending.
func peerDisconnected(sp *ServerPeer) bool {
	if sp == nil || sp.Peer == nil {
		return false
	}
	select {
	case <-sp.Done():
		return true
	default:
		return false
	}
}

// SyncPeer returns the current sync peer.
func (b *blockManager) SyncPeer() *ServerPeer {
	b.syncPeerMutex.Lock()
	defer b.syncPeerMutex.Unlock()

	return b.syncPeer
}

// isSyncCandidate returns whether or not the peer is a candidate to consider
// syncing from.
func (b *blockManager) isSyncCandidate(sp *ServerPeer) bool {
	// The peer is not a candidate for sync if it's not a full node.
	return sp.Services()&wire.SFNodeNetwork == wire.SFNodeNetwork
}

// findNextHeaderCheckpoint returns the next checkpoint after the passed
// height. It returns nil when there is not one either because the height is
// already later than the final checkpoint or there are none for the current
// network.
func (b *blockManager) findNextHeaderCheckpoint(
	height int32,
) *chaincfg.Checkpoint {
	// There is no next checkpoint if there are none for this current
	// network.
	checkpoints := b.cfg.ChainParams.Checkpoints
	if len(checkpoints) == 0 {
		return nil
	}

	// There is no next checkpoint if the height is already after the final
	// checkpoint.
	finalCheckpoint := &checkpoints[len(checkpoints)-1]
	if height >= finalCheckpoint.Height {
		return nil
	}

	// Find the next checkpoint.
	nextCheckpoint := finalCheckpoint
	for i := len(checkpoints) - 2; i >= 0; i-- {
		if height >= checkpoints[i].Height {
			break
		}
		nextCheckpoint = &checkpoints[i]
	}
	return nextCheckpoint
}

// findPreviousHeaderCheckpoint returns the last checkpoint before the passed
// height. It returns a checkpoint matching the genesis block when the height
// is earlier than the first checkpoint or there are no checkpoints for the
// current network. This is used for resetting state when a malicious peer
// sends us headers that don't lead up to a known checkpoint.
func (b *blockManager) findPreviousHeaderCheckpoint(
	height int32,
) *chaincfg.Checkpoint {
	// Start with the genesis block - earliest checkpoint to which our code
	// will want to reset
	prevCheckpoint := &chaincfg.Checkpoint{
		Height: 0,
		Hash:   b.cfg.ChainParams.GenesisHash,
	}

	// Find the latest checkpoint lower than height or return genesis block
	// if there are none.
	checkpoints := b.cfg.ChainParams.Checkpoints
	for i := 0; i < len(checkpoints); i++ {
		if height <= checkpoints[i].Height {
			break
		}
		prevCheckpoint = &checkpoints[i]
	}

	return prevCheckpoint
}

type syncPeerCandidate struct {
	peer   *ServerPeer
	height int32
	rtt    time.Duration
	addr   string
}

func newSyncPeerCandidate(peer *ServerPeer) syncPeerCandidate {
	return syncPeerCandidate{
		peer:   peer,
		height: peer.LastBlock(),
		rtt:    time.Duration(peer.LastPingMicros()) * time.Microsecond,
		addr:   peer.Addr(),
	}
}

func syncPeerOrdersBefore(candidate, current syncPeerCandidate) bool {
	if candidate.height != current.height {
		return candidate.height > current.height
	}

	switch {
	case candidate.rtt > 0 && current.rtt > 0 &&
		candidate.rtt != current.rtt:
		return candidate.rtt < current.rtt

	case candidate.rtt > 0 && current.rtt == 0:
		return true

	case candidate.rtt == 0 && current.rtt > 0:
		return false
	}

	return candidate.addr < current.addr
}

func (b *blockManager) headerTimeoutForPeer(peer *ServerPeer) time.Duration {
	timeout := b.headerSyncTimeout
	if timeout == 0 {
		timeout = defaultHeaderSyncTimeout
	}

	rtt := time.Duration(peer.LastPingMicros()) * time.Microsecond
	if rtt > 0 {
		rttTimeout := rtt * headerSyncRTTMultiplier
		if rttTimeout > timeout {
			timeout = rttTimeout
		}
	}

	if timeout > maxHeaderSyncTimeout {
		return maxHeaderSyncTimeout
	}

	return timeout
}

func (b *blockManager) armHeaderSyncTimeout(peer *ServerPeer,
	startHeight uint32) {

	b.serialRequestID++
	requestID := b.serialRequestID
	timeout := b.headerTimeoutForPeer(peer)

	if b.headerSyncTimer != nil {
		b.headerSyncTimer.Stop()
	}

	b.headerSyncTimer = time.AfterFunc(timeout, func() {
		b.sendHeaderTimeout(&headerSyncTimeoutMsg{
			peer:        peer,
			requestID:   requestID,
			startHeight: startHeight,
			timeout:     timeout,
		})
	})
}

// advanceHeaderSyncTip reconciles the headersync FSM with a durable tip that
// the serial writer persisted, and drops the staging of ranges it retired.
func (b *blockManager) advanceHeaderSyncTip(height uint32,
	hash chainhash.Hash) {

	outcome := b.handleDriverEvent(nil, headersync.AdvanceTip{
		Tip: headersync.ChainPoint{Height: height, Hash: hash},
	})
	if outcome.err != nil {
		log.Warnf("headersync unable to advance to serial tip "+
			"height=%d: %v", height, outcome.err)
	}
}

// handleNoProgressReply settles an empty serial reply. From a peer that is
// not the sync peer it means nothing. From the sync peer it answers the
// request: if the peer advertises nothing past our tip, sync is done and the
// deadline is disarmed. If it advertises more but sent none, it cannot serve
// us from our locator. It is not punished, since it did answer, but it stops
// being the sync peer and a candidate, so startSync moves on rather than asking
// it again forever. Keeping it while disarming the deadline would leave sync
// with no request and no timer until the peer disconnected.
func (b *blockManager) handleNoProgressReply(peers *list.List,
	peer *ServerPeer) {

	// With no serial request in flight the reply answers nothing, for
	// example an unsolicited or late empty message, so it is no reason
	// to demote the best peer.
	if peer != b.SyncPeer() || b.headerSyncTimer == nil {
		return
	}
	b.disarmHeaderSyncTimeout()

	_, height, err := b.cfg.BlockHeaders.ChainTip()
	if err != nil {
		log.Errorf("Unable to read header tip: %v", err)
		return
	}
	if peer.LastBlock() <= int32(height) {
		return
	}

	log.Infof("Sync peer %s advertises height %d but sent no headers "+
		"past %d; selecting a new sync peer", peer.Addr(),
		peer.LastBlock(), height)

	removeSyncCandidate(peers, peer)
	b.syncPeerMutex.Lock()
	b.syncPeer = nil
	b.syncPeerMutex.Unlock()
	b.startSync(peers)
}

// disarmHeaderSyncTimeout cancels the serial getheaders deadline. Bumping the
// request ID also voids a timeout callback that already fired but has not been
// handled yet.
func (b *blockManager) disarmHeaderSyncTimeout() {
	if b.headerSyncTimer != nil {
		b.headerSyncTimer.Stop()
		b.headerSyncTimer = nil
	}
	b.serialRequestID++
}

func (b *blockManager) requestHeaders(peer *ServerPeer,
	locator blockchain.BlockLocator, stopHash *chainhash.Hash,
	startHeight uint32) error {

	if err := b.sendHeaderRequest(peer, locator, stopHash); err != nil {
		return err
	}

	b.armHeaderSyncTimeout(peer, startHeight)
	return nil
}

func (b *blockManager) shouldDeferHeaderSyncBootstrap(peers *list.List,
	bestHeight uint32) bool {

	if b.headerSync == nil || b.nextCheckpoint == nil ||
		b.headerSync.Driver().Activity().HasWork() || bestHeight == 0 {

		return false
	}

	_, height, err := b.cfg.BlockHeaders.ChainTip()
	if err != nil || height != 0 {
		return false
	}

	if peers.Len() >= headerSyncBootstrapMinPeers {
		b.clearHeaderSyncBootstrapDelay()
		return false
	}

	now := time.Now()
	if b.headerSyncBootstrapFirstSeen.IsZero() {
		b.headerSyncBootstrapFirstSeen = now
		log.Infof("headersync bootstrap_deferred peers=%d "+
			"min_peers=%d max_wait=%v", peers.Len(),
			headerSyncBootstrapMinPeers,
			headerSyncBootstrapMaxWait)
	}

	if now.Sub(b.headerSyncBootstrapFirstSeen) >=
		headerSyncBootstrapMaxWait {

		b.clearHeaderSyncBootstrapDelay()
		return false
	}

	if b.headerSyncBootstrapTimer == nil {
		b.headerSyncBootstrapTimer = time.AfterFunc(
			headerSyncBootstrapMaxWait, func() {
				select {
				case b.timerChan <- &headerSyncBootstrapMsg{}:
				case <-b.quit:
				}
			},
		)
	}

	return true
}

func (b *blockManager) clearHeaderSyncBootstrapDelay() {
	b.headerSyncBootstrapFirstSeen = time.Time{}
	if b.headerSyncBootstrapTimer != nil {
		b.headerSyncBootstrapTimer.Stop()
		b.headerSyncBootstrapTimer = nil
	}
}

// startSync will choose the best peer among the available candidate peers to
// download/sync the blockchain from. When syncing is already running, it
// simply returns. It also examines the candidates for any which are no longer
// candidates and removes them as needed.
func (b *blockManager) startSync(peers *list.List) {
	if b.pendingHeaderCommit != nil {
		return
	}
	// Return now if we're already syncing.
	if b.syncPeer != nil {
		return
	}

	_, bestHeight, err := b.cfg.BlockHeaders.ChainTip()
	if err != nil {
		log.Errorf("Failed to get hash and height for the "+
			"latest block: %s", err)
		return
	}

	var bestPeer *ServerPeer
	var bestCandidate syncPeerCandidate
	var enext *list.Element
	for e := peers.Front(); e != nil; e = enext {
		enext = e.Next()
		sp := e.Value.(*ServerPeer)

		// Remove sync candidate peers that are no longer candidates
		// due to passing their latest known block.
		//
		// NOTE: The < is intentional as opposed to <=. While
		// techcnically the peer doesn't have a later block when it's
		// equal, it will likely have one soon so it is a reasonable
		// choice. It also allows the case where both are at 0 such as
		// during regression test.
		if sp.LastBlock() < int32(bestHeight) {
			peers.Remove(e)
			continue
		}

		candidate := newSyncPeerCandidate(sp)
		if bestPeer == nil ||
			syncPeerOrdersBefore(candidate, bestCandidate) {

			bestPeer = sp
			bestCandidate = candidate
		}
	}

	// Start syncing from the best peer if one was selected.
	if bestPeer != nil {
		if bestPeer.LastBlock() > 0 {
			bestHeight := uint32(bestPeer.LastBlock())
			if b.shouldDeferHeaderSyncBootstrap(peers, bestHeight) {
				return
			}
			if b.startHeaderSync(peers, bestPeer, bestHeight) {
				b.clearHeaderSyncBootstrapDelay()
				return
			}
		}

		locator, err := b.cfg.BlockHeaders.LatestBlockLocator()
		if err != nil {
			log.Errorf("Failed to get block locator for the "+
				"latest block: %s", err)
			return
		}

		log.Infof("Syncing to block height %d from peer %s",
			bestPeer.LastBlock(), bestPeer.Addr())

		// Now that we know we have a new sync peer, we'll lock it in
		// within the proper attribute.
		b.syncPeerMutex.Lock()
		b.syncPeer = bestPeer
		b.syncPeerMutex.Unlock()

		// By default will use the zero hash as our stop hash to query
		// for all the headers beyond our view of the network based on
		// our latest block locator.
		stopHash := &zeroHash

		// If we're still within the range of the set checkpoints, then
		// we'll use the next checkpoint to guide the set of headers we
		// fetch, setting our stop hash to the next checkpoint hash.
		if b.nextCheckpoint != nil &&
			int32(bestHeight) < b.nextCheckpoint.Height {

			log.Infof("Downloading headers for blocks %d to "+
				"%d from peer %s", bestHeight+1,
				b.nextCheckpoint.Height, bestPeer.Addr())

			stopHash = b.nextCheckpoint.Hash
		} else {
			log.Infof("Fetching set of headers from tip "+
				"(height=%v) from peer %s", bestHeight,
				bestPeer.Addr())
		}

		// With our stop hash selected, we'll kick off the sync from
		// this peer with an initial GetHeaders message.
		if err := b.requestHeaders(
			bestPeer, locator, stopHash, bestHeight,
		); err != nil {
			log.Warnf("Failed to send getheaders message to "+
				"peer %s: %s", bestPeer.Addr(), err)

			removeSyncCandidate(peers, bestPeer)

			b.syncPeerMutex.Lock()
			if b.syncPeer == bestPeer {
				b.syncPeer = nil
			}
			b.syncPeerMutex.Unlock()

			b.startSync(peers)
		}
	} else {
		log.Warnf("No sync peer candidates available")
	}
}

// IsFullySynced returns whether or not the block manager believed it is fully
// synced to the connected peers, meaning both block headers and filter headers
// are current.
func (b *blockManager) IsFullySynced() bool {
	_, blockHeaderHeight, err := b.cfg.BlockHeaders.ChainTip()
	if err != nil {
		return false
	}

	_, filterHeaderHeight, err := b.cfg.RegFilterHeaders.ChainTip()
	if err != nil {
		return false
	}

	// If the block headers and filter headers are not at the same height,
	// we cannot be fully synced.
	if blockHeaderHeight != filterHeaderHeight {
		return false
	}

	// Block and filter headers being at the same height, return whether
	// our block headers are synced.
	return b.BlockHeadersSynced()
}

// BlockHeadersSynced returns whether or not the block manager believes its
// block headers are synced with the connected peers.
func (b *blockManager) BlockHeadersSynced() bool {
	b.syncPeerMutex.RLock()
	defer b.syncPeerMutex.RUnlock()

	// Status must not wait on a store write holding the backend lock.
	tip := b.publishedBlockTip.Load()
	if tip == nil {
		return false
	}
	height := uint32(tip.Height)

	// There is no last checkpoint if checkpoints are disabled or there are
	// none for this current network.
	checkpoints := b.cfg.ChainParams.Checkpoints
	if len(checkpoints) != 0 {
		// We aren't current if the newest block we know of isn't ahead
		// of all checkpoints.
		if checkpoints[len(checkpoints)-1].Height >= int32(height) {
			return false
		}
	}

	// If we have a syncPeer and are below the block we are syncing to, we
	// are not current.
	if b.syncPeer != nil && int32(height) < b.syncPeer.LastBlock() {
		return false
	}

	// If our time source (median times of all the connected peers) is at
	// least 24 hours ahead of our best known block, we aren't current.
	minus24Hours := b.cfg.TimeSource.AdjustedTime().Add(-24 * time.Hour)
	if tip.Timestamp.Before(minus24Hours) {
		return false
	}

	// If we have no sync peer, we can assume we're current for now.
	if b.syncPeer == nil {
		return true
	}

	// If we have a syncPeer and the peer reported a higher known block
	// height on connect than we know the peer already has, we're probably
	// not current. If the peer is lying to us, other code will disconnect
	// it and then we'll re-check and notice that we're actually current.
	return b.syncPeer.LastBlock() >= b.syncPeer.StartingHeight()
}

// QueueInv adds the passed inv message and peer to the block handling queue.
func (b *blockManager) QueueInv(inv *wire.MsgInv, sp *ServerPeer) {
	// No channel handling here because peers do not need to block on inv
	// messages.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	select {
	case b.peerChan <- &invMsg{inv: inv, peer: sp}:
	case <-b.quit:
		return
	}
}

// QueueHeaders adds the passed headers message and peer to the block handling
// queue.
func (b *blockManager) QueueHeaders(headers *wire.MsgHeaders, sp *ServerPeer) {
	// No channel handling here because peers do not need to block on
	// headers messages.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	select {
	case b.peerChan <- &headersMsg{headers: headers, peer: sp}:
	case <-b.quit:
		return
	}
}

// onBlockConnected queues a block notification that extends the current chain.
func (b *blockManager) onBlockConnected(
	header wire.BlockHeader,
	height uint32,
) {

	select {
	case b.blockNtfnChan <- blockntfns.NewBlockConnected(header, height):
	case <-b.quit:
	}
}

// onBlockDisconnected queues a block notification that reorgs the current
// chain.
func (b *blockManager) onBlockDisconnected(headerDisconnected wire.BlockHeader,
	heightDisconnected uint32, newChainTip wire.BlockHeader) {

	select {
	case b.blockNtfnChan <- blockntfns.NewBlockDisconnected(
		headerDisconnected, heightDisconnected, newChainTip,
	):
	case <-b.quit:
	}
}

// Notifications exposes a receive-only channel in which the latest block
// notifications for the tip of the chain can be received.
func (b *blockManager) Notifications() <-chan blockntfns.BlockNtfn {
	return b.blockNtfnChan
}

// NotificationsSinceHeight returns a backlog of block notifications starting
// from the given height to the tip of the chain. When providing a height of 0,
// a backlog will not be delivered.
func (b *blockManager) NotificationsSinceHeight(
	height uint32) ([]blockntfns.BlockNtfn, uint32, error) {

	b.newFilterHeadersMtx.RLock()
	bestHeight := b.filterHeaderTip
	b.newFilterHeadersMtx.RUnlock()

	// If a height of 0 is provided by the caller, then a backlog of
	// notifications is not needed.
	if height == 0 {
		return nil, bestHeight, nil
	}

	// If the best height matches the filter header tip, then we're done
	// and don't need to proceed any further.
	if bestHeight == height {
		return nil, bestHeight, nil
	}

	// If the request has a height later than a height we've yet to come
	// across in the chain, we'll return an error to indicate so to the
	// caller.
	if height > bestHeight {
		return nil, 0, fmt.Errorf("request with height %d is greater "+
			"than best height known %d", height, bestHeight)
	}

	// Otherwise, we need to read block headers from disk to deliver a
	// backlog to the caller before we proceed.
	blocks := make([]blockntfns.BlockNtfn, 0, bestHeight-height)
	for i := height + 1; i <= bestHeight; i++ {
		header, err := b.cfg.BlockHeaders.FetchHeaderByHeight(i)
		if err != nil {
			return nil, 0, err
		}

		blocks = append(
			blocks,
			blockntfns.NewBlockConnected(*header, i),
		)
	}

	return blocks, bestHeight, nil
}
