// NOTE: THIS API IS UNSTABLE RIGHT NOW AND WILL GO MOSTLY PRIVATE SOON.

package neutrino

import (
	"container/list"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/gcs"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/lightninglabs/neutrino/headerfs"
)

const (
	// maxTimeOffset is the maximum duration a block time is allowed to be
	// ahead of the curent time. This is currently 2 hours.
	maxTimeOffset = 2 * time.Hour
)

var (
	// FilterTypes is a map of filter types to synchronize to a lookup
	// function for the service's store for that filter type.
	FilterTypes = map[wire.FilterType]func(
		*ChainService) *headerfs.FilterHeaderStore{
		wire.GCSFilterRegular: func(
			s *ChainService) *headerfs.FilterHeaderStore {
			return s.RegFilterHeaders
		},
		wire.GCSFilterExtended: func(
			s *ChainService) *headerfs.FilterHeaderStore {
			return s.ExtFilterHeaders
		},
	}
)

// zeroHash is the zero value hash (all zeros).  It is defined as a convenience.
var zeroHash chainhash.Hash

// newPeerMsg signifies a newly connected peer to the block handler.
type newPeerMsg struct {
	peer *ServerPeer
}

// invMsg packages a bitcoin inv message and the peer it came from together
// so the block handler has access to that information.
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

// txMsg packages a bitcoin tx message and the peer it came from together
// so the block handler has access to that information.
type txMsg struct {
	tx   *btcutil.Tx
	peer *ServerPeer
}

// isCurrentMsg is a message type to be sent across the message channel for
// requesting whether or not the block manager believes it is synced with
// the currently connected peers.
type isCurrentMsg struct {
	reply chan bool
}

// headerNode is used as a node in a list of headers that are linked together
// between checkpoints.
type headerNode struct {
	height int32
	header *wire.BlockHeader
}

// blockManager provides a concurrency safe block manager for handling all
// incoming blocks.
type blockManager struct {
	server          *ChainService
	started         int32
	shutdown        int32
	requestedBlocks map[chainhash.Hash]struct{}
	progressLogger  *blockProgressLogger
	syncPeer        *ServerPeer
	syncPeerMutex   sync.Mutex

	// peerChan is a channel for messages that come from peers
	peerChan chan interface{}

	wg   sync.WaitGroup
	quit chan struct{}

	headerList     *list.List
	reorgList      *list.List
	startHeader    *list.Element
	nextCheckpoint *chaincfg.Checkpoint
	lastRequested  chainhash.Hash

	startCFHeaderSync chan struct{}

	minRetargetTimespan int64 // target timespan / adjustment factor
	maxRetargetTimespan int64 // target timespan * adjustment factor
	blocksPerRetarget   int32 // target timespan / target time per block
}

// newBlockManager returns a new bitcoin block manager.  Use Start to begin
// processing asynchronous block and inv updates.
func newBlockManager(s *ChainService) (*blockManager, error) {
	targetTimespan := int64(s.chainParams.TargetTimespan / time.Second)
	targetTimePerBlock := int64(s.chainParams.TargetTimePerBlock / time.Second)
	adjustmentFactor := s.chainParams.RetargetAdjustmentFactor

	bm := blockManager{
		server:              s,
		requestedBlocks:     make(map[chainhash.Hash]struct{}),
		peerChan:            make(chan interface{}, MaxPeers*3),
		progressLogger:      newBlockProgressLogger("Processed", log),
		headerList:          list.New(),
		reorgList:           list.New(),
		quit:                make(chan struct{}),
		blocksPerRetarget:   int32(targetTimespan / targetTimePerBlock),
		minRetargetTimespan: targetTimespan / adjustmentFactor,
		maxRetargetTimespan: targetTimespan * adjustmentFactor,
		startCFHeaderSync:   make(chan struct{}),
	}

	// Initialize the next checkpoint based on the current height.
	header, height, err := s.BlockHeaders.ChainTip()
	if err != nil {
		return nil, err
	}
	bm.nextCheckpoint = bm.findNextHeaderCheckpoint(int32(height))
	bm.resetHeaderState(header, int32(height))

	return &bm, nil
}

// Start begins the core block handler which processes block and inv messages.
func (b *blockManager) Start() {
	// Already started?
	if atomic.AddInt32(&b.started, 1) != 1 {
		return
	}

	log.Trace("Starting block manager")
	b.wg.Add(2)
	go b.blockHandler()
	go b.cfHandler()
}

// Stop gracefully shuts down the block manager by stopping all asynchronous
// handlers and waiting for them to finish.
func (b *blockManager) Stop() error {
	if atomic.AddInt32(&b.shutdown, 1) != 1 {
		log.Warnf("Block manager is already in the process of " +
			"shutting down")
		return nil
	}

	log.Infof("Block manager shutting down")
	close(b.quit)
	b.wg.Wait()
	return nil
}

// NewPeer informs the block manager of a newly active peer.
func (b *blockManager) NewPeer(sp *ServerPeer) {
	// Ignore if we are shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}
	b.peerChan <- &newPeerMsg{peer: sp}
}

// handleNewPeerMsg deals with new peers that have signalled they may be
// considered as a sync peer (they have already successfully negotiated).  It
// also starts syncing if needed.  It is invoked from the syncHandler
// goroutine.
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

	// If we're current with our sync peer and the new peer is advertising
	// a higher block than the newest one we know of, request headers from
	// the new peer.
	_, height, err := b.server.BlockHeaders.ChainTip()
	if err != nil {
		log.Criticalf("Couldn't retrieve block header chain tip: %s",
			err)
		return
	}
	if b.current() && height < uint32(sp.StartingHeight()) {
		locator, err := b.server.BlockHeaders.LatestBlockLocator()
		if err != nil {
			log.Criticalf("Couldn't retrieve latest block "+
				"locator: %s", err)
			return
		}
		stopHash := &zeroHash
		sp.PushGetHeadersMsg(locator, stopHash)
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

	b.peerChan <- &donePeerMsg{peer: sp}
}

// handleDonePeerMsg deals with peers that have signalled they are done.  It
// removes the peer as a candidate for syncing and in the case where it was the
// current sync peer, attempts to select a new best peer to sync from.  It is
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

	// Attempt to find a new peer to sync from if the quitting peer is the
	// sync peer.  Also, reset the header state.
	if b.syncPeer != nil && b.syncPeer == sp {
		b.syncPeerMutex.Lock()
		b.syncPeer = nil
		b.syncPeerMutex.Unlock()
		header, height, err := b.server.BlockHeaders.ChainTip()
		if err != nil {
			return
		}
		b.resetHeaderState(header, int32(height))
		b.startSync(peers)
	}
}

// cfHandler is the cfheader download handler for the block manager. It must be
// run as a goroutine. It requests and processes cfheaders messages in a
// separate goroutine from the peer handlers.
func (b *blockManager) cfHandler() {
	// If a loop ends with a quit, we want to signal that the goroutine is
	// done.
	defer func() {
		log.Trace("Committed filter header handler done")
		b.wg.Done()
	}()

	// Wait for block header sync to complete first.
	select {
	case <-b.startCFHeaderSync:
	case <-b.quit:
		return
	}

	// Query all peers for their checkpoints.
	lastHeader, _, err := b.server.BlockHeaders.ChainTip()
	if err != nil {
		log.Critical(err)
		return
	}
	lastHash := lastHeader.BlockHash()

	// At this point, we have good checkpoints and we've banned most peers
	// that aren't on the same chain as we are. Now, we download all of
	// the headers that match those checkpoints. We launch the downloads
	// for both types of headers simultaneously and then wait for them to
	// both return. Each one returns when its store is caught up with
	// the block headers.
	checkpoints := make(map[wire.FilterType][]*chainhash.Hash)
	var mtxChkpt sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(FilterTypes))
	for fType, storeLookup := range FilterTypes {
		// Launch a goroutine to get all of the
		go func() {
			// Get the header store for this filter type.
			store := storeLookup(b.server)
			var goodCheckpoints []*chainhash.Hash
			for len(goodCheckpoints) == 0 {
				select {
				case <-b.quit:
					return
				default:
				}
				allCheckpoints := b.getCheckpts(&lastHash,
					fType)
				if len(allCheckpoints) == 0 {
					time.Sleep(QueryTimeout)
					continue
				}
				goodCheckpoints, err = b.resolveConflict(
					allCheckpoints, store, fType)
				if err != nil {
					log.Debugf("got error attempting "+
						"to determine correct cfheader"+
						" checkpoints: %v, trying "+
						"again", err)
					time.Sleep(QueryTimeout)
				}
			}
			b.getAllCFHeaders(goodCheckpoints, store, fType)
			mtxChkpt.Lock()
			checkpoints[fType] = goodCheckpoints
			mtxChkpt.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()

}

// getAllCFHeaders catches a filter header store up with the block header
// store from the network. It assumes that the filter header store matches the
// checkpoints up to the tip of the store.
func (b *blockManager) getAllCFHeaders(checkpoints []*chainhash.Hash,
	store *headerfs.FilterHeaderStore, fType wire.FilterType) {

	// We keep going until we've caught up the filter header store with
	// the latest known checkpoint.
	var curHeight uint32
	var err error

	// Get the current height of the filter header store and don't take
	// an error for an answer unless we're quitting.
	for curHeight == 0 {
		select {
		case <-b.quit:
			return
		default:
		}
		_, curHeight, err = store.ChainTip()
		if err != nil {
			curHeight = 0
			// Don't beat up storage too much if we're failing.
			time.Sleep(QueryTimeout)
		}
		break
	}

	// Generate all of the requests we'll be batching.
	queryMsgs := make([]*wire.MsgGetCFHeaders, 0, len(checkpoints))
	for i, _ := range checkpoints {
		// If we have the header for this checkpoint, we can skip doing
		// anything with it. We also already have
		if (i+1)*wire.CFCheckptInterval < int(curHeight) {
			continue
		}

		// Create a new query message for the checkpoint and append
		// it.
		stopHeader, err := b.server.BlockHeaders.FetchHeaderByHeight(
			uint32((i + 1) * wire.CFCheckptInterval))
		if err != nil {
			// Try to recover this.
			select {
			case <-b.quit:
				return
			default:
				i--
				time.Sleep(QueryTimeout)
				continue
			}
		}
		stopHash := stopHeader.BlockHash()
		queryMsg := wire.NewMsgGetCFHeaders(fType,
			uint32(i*wire.CFCheckptInterval+1), &stopHash)
		queryMsgs = append(queryMsgs, queryMsg)
	}

	for curHeight < uint32(len(checkpoints)*wire.CFCheckptInterval) {
		// Quit if requested.
		select {
		case <-b.quit:
			return
		default:
		}

		// Update current height.
		curHeight = 0
		for curHeight == 0 {
			select {
			case <-b.quit:
				return
			default:
			}
			_, curHeight, err = store.ChainTip()
			if err != nil {
				curHeight = 0
				// Don't beat up storage too much if we're
				// failing.
				time.Sleep(QueryTimeout)
			}
		}
	}
}

// resolveConflict finds the correct checkpoint information, rewinds the header
// store if it's incorrect, and bans any peers giving us incorrect header
// information.
func (b *blockManager) resolveConflict(
	checkpoints map[*ServerPeer][]*chainhash.Hash,
	store *headerfs.FilterHeaderStore, fType wire.FilterType) (
	[]*chainhash.Hash, error) {

	heightDiff, err := checkCFCheckptSanity(checkpoints, store)
	if err != nil {
		return nil, err
	}

	// If we got -1, we have full agreement between all peers and the store.
	if heightDiff == -1 {
		// Take the first peer's checkpoint list and return it.
		for _, checkpts := range checkpoints {
			return checkpts, nil
		}
	}

	// Delete any responses that have fewer checkpoints than where we see a
	// mismatch.
	for peer, checkpts := range checkpoints {
		if len(checkpts) < heightDiff {
			delete(checkpoints, peer)
		}
	}

	if len(checkpoints) == 0 {
		return nil, fmt.Errorf("no peer is serving good cfheaders")
	}

	// Now we get all of the mismatched CFHeaders from peers, and check
	// which ones are valid.
	startHeight := uint32(heightDiff) * wire.CFCheckptInterval
	headers := b.getCFHeadersForAllPeers(startHeight, fType)

	// Make sure we're working off the same baseline. Otherwise, we
	// want to go back and get checkpoints again.
	var hash chainhash.Hash
	for _, msg := range headers {
		if hash == zeroHash {
			hash = msg.PrevFilterHeader
		} else if hash != msg.PrevFilterHeader {
			return nil, fmt.Errorf("mismatch between filter " +
				"headers expected to be the same")
		}
	}

	// For each header, go through and check whether all headers messages
	// have the same filter hash. If we find a difference, get the block
	// and calculate the filter.
	for i := 0; i < wire.MaxCFHeadersPerMsg; i++ {
		hash = zeroHash
		var mismatch bool
		for _, msg := range headers {
			if len(msg.FilterHashes) <= i {
				continue
			}

			if hash == zeroHash {
				hash = *msg.FilterHashes[i]
				continue
			}

			if hash != *msg.FilterHashes[i] {
				// We've found a mismatch!
				mismatch = true
				break
			}
		}

		if mismatch {
			u32i := uint32(i)
			// Get the block for this height.
			header, err := b.server.BlockHeaders.
				FetchHeaderByHeight(startHeight + u32i)
			if err != nil {
				return nil, err
			}
			block, err := b.server.GetBlockFromNetwork(
				header.BlockHash())
			if err != nil {
				return nil, err
			}

			// Calculate the filter.
			var filter *gcs.Filter
			switch fType {
			case wire.GCSFilterRegular:
				filter, err = builder.BuildBasicFilter(
					block.MsgBlock())
			case wire.GCSFilterExtended:
				filter, err = builder.BuildExtFilter(
					block.MsgBlock())
			}
			if err != nil {
				return nil, err
			}

			// Hash the filter to see what the correct value
			// should be.
			fHash, err := builder.GetFilterHash(filter)
			if err != nil {
				return nil, err
			}

			// Compare the calculated filter hash against received
			// filter hashes and throw out any mismatching peers.
			for peer, msg := range headers {
				if len(msg.FilterHashes) <= i {
					continue
				}

				if *msg.FilterHashes[i] != fHash {
					// Got a mismatching peer
					b.server.BanPeer(peer)
					peer.Disconnect()
					delete(headers, peer)
					delete(checkpoints, peer)
				}
			}

			// Compare the calculated filter against the stored
			// filter headers, if any, and roll back if the stored
			// header at this height doesn't match the calculated
			// filter hash.
			_, maxHeight, err := store.ChainTip()
			if err != nil {
				return nil, err
			}

			if u32i <= maxHeight {
				/*curHeader, err := store.FetchHeaderByHeight(
					u32i)
				if err != nil {
					return nil, err
				}*/
				lastHeader, err := store.FetchHeaderByHeight(
					u32i - 1)
				if err != nil {
					return nil, err
				}
				calcHeader, err := builder.MakeHeaderForFilter(
					filter, *lastHeader)
				if err != nil {
					return nil, err
				}
				if *lastHeader != calcHeader {
					// Roll back stored headers to previous
					// height, which is the last one that
					// agrees with the network.
				}
			}
		}
	}

	// Any mismatches have now been thrown out. Delete any checkpoint
	// lists that don't have matching headers, as these are peers that
	// didn't respond, and ban them from future queries.
	for peer := range checkpoints {
		if _, ok := headers[peer]; !ok {
			b.server.BanPeer(peer)
			peer.Disconnect()
			delete(checkpoints, peer)
		}
	}

	// Check sanity again. If we're sane, return a matching checkpoint
	// list. If not, return an error and download checkpoints from
	// remaining peers.
	heightDiff, err = checkCFCheckptSanity(checkpoints, store)
	if err != nil {
		return nil, err
	}

	// If we got -1, we have full agreement between all peers and the store.
	if heightDiff == -1 {
		// Take the first peer's checkpoint list and return it.
		for _, checkpts := range checkpoints {
			return checkpts, nil
		}
	}

	// Otherwise, return an error and allow the loop which calls this
	// function to call it again with the new set of peers.
	return nil, fmt.Errorf("got mismatched checkpoints")
}

// getCFHeadersForAllPeers runs a query for cfheaders at a specific height and
// returns a map of responses from all peers.
func (b *blockManager) getCFHeadersForAllPeers(height uint32,
	fType wire.FilterType) map[*ServerPeer]*wire.MsgCFHeaders {

	// Create the map we're returning.
	headers := make(map[*ServerPeer]*wire.MsgCFHeaders)

	// Get the header we expect at the last height represented by the
	// checkpoint number passed to us.
	stopHeader, err := b.server.BlockHeaders.FetchHeaderByHeight(
		height + wire.MaxCFHeadersPerMsg - 1)
	if err != nil {
		return headers
	}

	// Calculate the hash and use it to create the query message.
	stopHash := stopHeader.BlockHash()
	msg := wire.NewMsgGetCFHeaders(fType, height, &stopHash)

	// Send the query to all peers and record their responses in the map.
	b.server.queryAllPeers(
		msg,
		func(sp *ServerPeer, resp wire.Message, quit chan<- struct{},
			peerQuit chan<- struct{}) {
			switch m := resp.(type) {
			case *wire.MsgCFHeaders:
				if m.StopHash == stopHash &&
					m.FilterType == fType {
					headers[sp] = m
					// We got an answer from this peer so
					// that peer's goroutine can stop.
					close(peerQuit)
				}
			}
		},
	)

	return headers
}

// getCheckpts runs a query for cfcheckpts against all peers and returns a map
// of responses.
func (b *blockManager) getCheckpts(lastHash *chainhash.Hash,
	fType wire.FilterType) map[*ServerPeer][]*chainhash.Hash {

	checkpoints := make(map[*ServerPeer][]*chainhash.Hash)
	getCheckptMsg := wire.NewMsgGetCFCheckpt(fType, lastHash)
	b.server.queryAllPeers(
		getCheckptMsg,
		func(sp *ServerPeer, resp wire.Message, quit chan<- struct{},
			peerQuit chan<- struct{}) {
			switch m := resp.(type) {
			case *wire.MsgCFCheckpt:
				if m.FilterType == fType &&
					m.StopHash == *lastHash {
					checkpoints[sp] = m.FilterHeaders
					close(peerQuit)
				}
			}
		},
	)
	return checkpoints
}

// checkCFCheckptSanity checks whether all peers which have responded agree.
// If so, it returns -1; otherwise, it returns the earliest index at which at
// least one of the peers differs. The checkpoints are also checked against the
// existing store up to the tip of the store. If all of the peers match but
// the store doesn't, the height at which the mismatch occurs is returned.
func checkCFCheckptSanity(cp map[*ServerPeer][]*chainhash.Hash,
	headerStore *headerfs.FilterHeaderStore) (int, error) {

	// Get the known best header to compare against checkpoints.
	_, storeTip, err := headerStore.ChainTip()
	if err != nil {
		return 0, err
	}

	// Determine the maximum length of each peer's checkpoint list. If they
	// differ, we don't return yet because we want to make sure they match
	// up to the shortest one.
	maxLen := 0
	for _, checkpoints := range cp {
		if len(checkpoints) > maxLen {
			maxLen = len(checkpoints)
		}
	}

	// Compare the actual checkpoints against each other and anything
	// stored in the header store.
	for i := 0; i < maxLen; i++ {
		var checkpoint chainhash.Hash
		for _, checkpoints := range cp {
			if i >= len(checkpoints) {
				return i, nil
			}
			if checkpoint == (zeroHash) {
				checkpoint = *checkpoints[i]
			}
			if checkpoint != *checkpoints[i] {
				return i, nil
			}
		}
		if uint32((i+1)*wire.CFCheckptInterval) <= storeTip {
			header, err := headerStore.FetchHeaderByHeight(
				uint32((i + 1) * wire.CFCheckptInterval))
			if err != nil {
				return i, err
			}
			if *header != checkpoint {
				return i, nil
			}
		}
	}
	return -1, nil
}

// blockHandler is the main handler for the block manager.  It must be run as a
// goroutine.  It processes block and inv messages in a separate goroutine from
// the peer handlers so the block (MsgBlock) messages are handled by a single
// thread without needing to lock memory data structures.  This is important
// because the block manager controls which blocks are needed and how
// the fetching should proceed.
func (b *blockManager) blockHandler() {
	candidatePeers := list.New()
out:
	for {
		// Now check peer messages and quit channels.
		select {
		case m := <-b.peerChan:
			switch msg := m.(type) {
			case *newPeerMsg:
				b.handleNewPeerMsg(candidatePeers, msg.peer)

			case *invMsg:
				b.handleInvMsg(msg)

			case *headersMsg:
				b.handleHeadersMsg(msg)

			case *donePeerMsg:
				b.handleDonePeerMsg(candidatePeers, msg.peer)

			case isCurrentMsg:
				msg.reply <- b.current()

			default:
				log.Warnf("Invalid message type in block "+
					"handler: %T", msg)
			}

		case <-b.quit:
			break out
		}
	}

	b.wg.Done()
	log.Trace("Block handler done")
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

// findNextHeaderCheckpoint returns the next checkpoint after the passed height.
// It returns nil when there is not one either because the height is already
// later than the final checkpoint or there are none for the current network.
func (b *blockManager) findNextHeaderCheckpoint(height int32) *chaincfg.Checkpoint {
	// There is no next checkpoint if there are none for this current
	// network.
	checkpoints := b.server.chainParams.Checkpoints
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
// current network. This is used for resetting state when a malicious peer sends
// us headers that don't lead up to a known checkpoint.
func (b *blockManager) findPreviousHeaderCheckpoint(height int32) *chaincfg.Checkpoint {
	// Start with the genesis block - earliest checkpoint to which our
	// code will want to reset
	prevCheckpoint := &chaincfg.Checkpoint{
		Height: 0,
		Hash:   b.server.chainParams.GenesisHash,
	}

	// Find the latest checkpoint lower than height or return genesis block
	// if there are none.
	checkpoints := b.server.chainParams.Checkpoints
	for i := 0; i < len(checkpoints); i++ {
		if height <= checkpoints[i].Height {
			break
		}
		prevCheckpoint = &checkpoints[i]
	}
	return prevCheckpoint
}

// resetHeaderState sets the headers-first mode state to values appropriate for
// syncing from a new peer.
func (b *blockManager) resetHeaderState(newestHeader *wire.BlockHeader,
	newestHeight int32) {
	b.headerList.Init()
	b.startHeader = nil

	// Add an entry for the latest known block into the header pool.
	// This allows the next downloaded header to prove it links to the chain
	// properly.
	node := headerNode{header: newestHeader, height: newestHeight}
	b.headerList.PushBack(&node)
}

// startSync will choose the best peer among the available candidate peers to
// download/sync the blockchain from.  When syncing is already running, it
// simply returns.  It also examines the candidates for any which are no longer
// candidates and removes them as needed.
func (b *blockManager) startSync(peers *list.List) {
	// Return now if we're already syncing.
	if b.syncPeer != nil {
		return
	}

	best, err := b.server.BestSnapshot()
	if err != nil {
		log.Errorf("Failed to get hash and height for the "+
			"latest block: %s", err)
		return
	}

	var bestPeer *ServerPeer
	var enext *list.Element
	for e := peers.Front(); e != nil; e = enext {
		enext = e.Next()
		sp := e.Value.(*ServerPeer)

		// Remove sync candidate peers that are no longer candidates
		// due to passing their latest known block.
		//
		// NOTE: The < is intentional as opposed to <=.  While
		// techcnically the peer doesn't have a later block when it's
		// equal, it will likely have one soon so it is a reasonable
		// choice.  It also allows the case where both are at 0 such as
		// during regression test.
		if sp.LastBlock() < best.Height {
			peers.Remove(e)
			continue
		}

		// TODO: Use a better algorithm to choose the best peer.
		// For now, just pick the candidate with the highest last block.
		if bestPeer == nil || sp.LastBlock() > bestPeer.LastBlock() {
			bestPeer = sp
		}
	}

	// Start syncing from the best peer if one was selected.
	if bestPeer != nil {
		// Clear the requestedBlocks if the sync peer changes,
		// otherwise we may ignore blocks we need that the last sync
		// peer failed to send.
		b.requestedBlocks = make(map[chainhash.Hash]struct{})

		locator, err := b.server.BlockHeaders.LatestBlockLocator()
		if err != nil {
			log.Errorf("Failed to get block locator for the "+
				"latest block: %s", err)
			return
		}

		log.Infof("Syncing to block height %d from peer %s",
			bestPeer.LastBlock(), bestPeer.Addr())

		// When the current height is less than a known checkpoint we
		// can use block headers to learn about which blocks comprise
		// the chain up to the checkpoint and perform less validation
		// for them.  This is possible since each header contains the
		// hash of the previous header and a merkle root.  Therefore if
		// we validate all of the received headers link together
		// properly and the checkpoint hashes match, we can be sure the
		// hashes for the blocks in between are accurate.  Further,
		// once the full blocks are downloaded, the merkle root is
		// computed and compared against the value in the header which
		// proves the full block hasn't been tampered with.
		//
		// Once we have passed the final checkpoint, or checkpoints are
		// disabled, use standard inv messages learn about the blocks
		// and fully validate them.  Finally, regression test mode does
		// not support the headers-first approach so do normal block
		// downloads when in regression test mode.
		b.syncPeerMutex.Lock()
		b.syncPeer = bestPeer
		b.syncPeerMutex.Unlock()
		if b.nextCheckpoint != nil && best.Height < b.nextCheckpoint.Height {

			b.syncPeer.PushGetHeadersMsg(locator, b.nextCheckpoint.Hash)
			log.Infof("Downloading headers for blocks %d to "+
				"%d from peer %s", best.Height+1,
				b.nextCheckpoint.Height, bestPeer.Addr())

			// This will get adjusted when we process headers if we
			// request more headers than the peer is willing to
			// give us in one message.
		} else {
			b.syncPeer.PushGetBlocksMsg(locator, &zeroHash)
		}
	} else {
		log.Warnf("No sync peer candidates available")
	}
}

// current returns true if we believe we are synced with our peers, false if we
// still have blocks to check
func (b *blockManager) current() bool {
	// Figure out the latest block we know.
	header, height, err := b.server.BlockHeaders.ChainTip()
	if err != nil {
		return false
	}

	// There is no last checkpoint if checkpoints are disabled or there are
	// none for this current network.
	checkpoints := b.server.chainParams.Checkpoints
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
	minus24Hours := b.server.timeSource.AdjustedTime().Add(-24 * time.Hour)
	if header.Timestamp.Before(minus24Hours) {
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
	return b.SyncPeer().LastBlock() >= b.SyncPeer().StartingHeight()
}

// IsCurrent returns whether or not the block manager believes it is synced with
// the connected peers.
//
// TODO(roasbeef): hook into WC implementation
func (b *blockManager) IsCurrent() bool {
	reply := make(chan bool)
	b.peerChan <- isCurrentMsg{reply: reply}
	return <-reply
}

// QueueInv adds the passed inv message and peer to the block handling queue.
func (b *blockManager) QueueInv(inv *wire.MsgInv, sp *ServerPeer) {
	// No channel handling here because peers do not need to block on inv
	// messages.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	b.peerChan <- &invMsg{inv: inv, peer: sp}
}

// handleInvMsg handles inv messages from all peers.
// We examine the inventory advertised by the remote peer and act accordingly.
func (b *blockManager) handleInvMsg(imsg *invMsg) {
	// Attempt to find the final block in the inventory list.  There may
	// not be one.
	lastBlock := -1
	invVects := imsg.inv.InvList
	for i := len(invVects) - 1; i >= 0; i-- {
		if invVects[i].Type == wire.InvTypeBlock {
			lastBlock = i
			break
		}
	}

	// If this inv contains a block announcement, and this isn't coming from
	// our current sync peer or we're current, then update the last
	// announced block for this peer. We'll use this information later to
	// update the heights of peers based on blocks we've accepted that they
	// previously announced.
	if lastBlock != -1 && (imsg.peer != b.syncPeer || b.current()) {
		imsg.peer.UpdateLastAnnouncedBlock(&invVects[lastBlock].Hash)
	}

	// Ignore invs from peers that aren't the sync if we are not current.
	// Helps prevent dealing with orphans.
	if imsg.peer != b.syncPeer && !b.current() {
		return
	}

	// If our chain is current and a peer announces a block we already
	// know of, then update their current block height.
	if lastBlock != -1 && b.current() {
		height, err := b.server.BlockHeaders.HeightFromHash(&invVects[lastBlock].Hash)
		if err == nil {
			imsg.peer.UpdateLastBlockHeight(int32(height))
		}
	}

	// Add blocks to the cache of known inventory for the peer.
	for _, iv := range invVects {
		if iv.Type == wire.InvTypeBlock {
			imsg.peer.AddKnownInventory(iv)
		}
	}

	// If this is the sync peer or we're current, get the headers for the
	// announced blocks and update the last announced block.
	if lastBlock != -1 && (imsg.peer == b.syncPeer || b.current()) {
		lastEl := b.headerList.Back()
		var lastHash chainhash.Hash
		if lastEl != nil {
			lastHash = lastEl.Value.(*headerNode).header.BlockHash()
		}

		// Only send getheaders if we don't already know about the last
		// block hash being announced.
		if lastHash != invVects[lastBlock].Hash && lastEl != nil &&
			b.lastRequested != invVects[lastBlock].Hash {

			// Make a locator starting from the latest known header
			// we've processed.
			locator := make(blockchain.BlockLocator, 0,
				wire.MaxBlockLocatorsPerMsg)
			locator = append(locator, &lastHash)

			// Add locator from the database as backup.
			knownLocator, err := b.server.BlockHeaders.LatestBlockLocator()
			if err == nil {
				locator = append(locator, knownLocator...)
			}

			// Get headers based on locator.
			err = imsg.peer.PushGetHeadersMsg(locator,
				&invVects[lastBlock].Hash)
			if err != nil {
				log.Warnf("Failed to send getheaders message "+
					"to peer %s: %s", imsg.peer.Addr(), err)
				return
			}
			b.lastRequested = invVects[lastBlock].Hash
		}
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

	b.peerChan <- &headersMsg{headers: headers, peer: sp}
}

// handleHeadersMsg handles headers messages from all peers.
func (b *blockManager) handleHeadersMsg(hmsg *headersMsg) {
	msg := hmsg.headers
	numHeaders := len(msg.Headers)

	// Nothing to do for an empty headers message.
	if numHeaders == 0 {
		return
	}

	// For checking to make sure blocks aren't too far in the future as of
	// the time we receive the headers message.
	maxTimestamp := b.server.timeSource.AdjustedTime().
		Add(maxTimeOffset)

	// We'll attempt to write the entire batch of validated headers
	// atomically in order to improve peformance.
	headerWriteBatch := make([]headerfs.BlockHeader, 0, len(msg.Headers))

	// Process all of the received headers ensuring each one connects to
	// the previous and that checkpoints match.
	receivedCheckpoint := false
	var finalHash *chainhash.Hash
	var finalHeight int32
	for i, blockHeader := range msg.Headers {
		blockHash := blockHeader.BlockHash()
		finalHash = &blockHash

		// Ensure there is a previous header to compare against.
		prevNodeEl := b.headerList.Back()
		if prevNodeEl == nil {
			log.Warnf("Header list does not contain a previous" +
				"element as expected -- disconnecting peer")
			hmsg.peer.Disconnect()
			return
		}

		// Ensure the header properly connects to the previous one,
		// that the proof of work is good, and that the header's
		// timestamp isn't too far in the future, and add it to the
		// list of headers.
		node := headerNode{header: blockHeader}
		prevNode := prevNodeEl.Value.(*headerNode)
		prevHash := prevNode.header.BlockHash()
		if prevHash.IsEqual(&blockHeader.PrevBlock) {
			err := b.checkHeaderSanity(blockHeader, maxTimestamp,
				false)
			if err != nil {
				log.Warnf("Header doesn't pass sanity check: "+
					"%s -- disconnecting peer", err)
				hmsg.peer.Disconnect()
				return
			}

			node.height = prevNode.height + 1
			finalHeight = node.height

			// This header checks out, so we'll add it to our write
			// batch.
			headerWriteBatch = append(headerWriteBatch, headerfs.BlockHeader{
				BlockHeader: blockHeader,
				Height:      uint32(node.height),
			})

			hmsg.peer.UpdateLastBlockHeight(node.height)
			b.progressLogger.LogBlockHeight(blockHeader, node.height)

			// Finally initialize the header ->
			// map[filterHash]*peer map for filter header
			// validation purposes later.
			e := b.headerList.PushBack(&node)
			if b.startHeader == nil {
				b.startHeader = e
			}
		} else {
			// The block doesn't connect to the last block we know.
			// We will need to do some additional checks to process
			// possible reorganizations or incorrect chain on
			// either our or the peer's side.
			//
			// If we got these headers from a peer that's not our
			// sync peer, they might not be aligned correctly or
			// even on the right chain. Just ignore the rest of the
			// message. However, if we're current, this might be a
			// reorg, in which case we'll either change our sync
			// peer or disconnect the peer that sent us these bad
			// headers.
			if hmsg.peer != b.syncPeer && !b.current() {
				return
			}

			// Check if this is the last block we know of. This is
			// a shortcut for sendheaders so that each redundant
			// header doesn't cause a disk read.
			if blockHash == prevHash {
				continue
			}

			// Check if this block is known. If so, we continue to
			// the next one.
			_, _, err := b.server.BlockHeaders.FetchHeader(&blockHash)
			if err == nil {
				continue
			}

			// Check if the previous block is known. If it is, this
			// is probably a reorg based on the estimated latest
			// block that matches between us and the peer as
			// derived from the block locator we sent to request
			// these headers. Otherwise, the headers don't connect
			// to anything we know and we should disconnect the
			// peer.
			backHead, backHeight, err := b.server.BlockHeaders.FetchHeader(
				&blockHeader.PrevBlock,
			)
			if err != nil {
				log.Warnf("Received block header that does not"+
					" properly connect to the chain from"+
					" peer %s (%s) -- disconnecting",
					hmsg.peer.Addr(), err)
				hmsg.peer.Disconnect()
				return
			}

			// We've found a branch we weren't aware of. If the
			// branch is earlier than the latest synchronized
			// checkpoint, it's invalid and we need to disconnect
			// the reporting peer.
			prevCheckpoint := b.findPreviousHeaderCheckpoint(
				prevNode.height)
			if backHeight < uint32(prevCheckpoint.Height) {
				log.Errorf("Attempt at a reorg earlier than a "+
					"checkpoint past which we've already "+
					"synchronized -- disconnecting peer "+
					"%s", hmsg.peer.Addr())
				hmsg.peer.Disconnect()
				return
			}

			// Check the sanity of the new branch. If any of the
			// blocks don't pass sanity checks, disconnect the
			// peer.  We also keep track of the work represented by
			// these headers so we can compare it to the work in
			// the known good chain.
			b.reorgList.Init()
			b.reorgList.PushBack(&headerNode{
				header: backHead,
				height: int32(backHeight),
			})
			totalWork := big.NewInt(0)
			for j, reorgHeader := range msg.Headers[i:] {
				err = b.checkHeaderSanity(reorgHeader,
					maxTimestamp, true)
				if err != nil {
					log.Warnf("Header doesn't pass sanity"+
						" check: %s -- disconnecting "+
						"peer", err)
					hmsg.peer.Disconnect()
					return
				}
				totalWork.Add(totalWork,
					blockchain.CalcWork(reorgHeader.Bits))
				b.reorgList.PushBack(&headerNode{
					header: reorgHeader,
					height: int32(backHeight+1) + int32(j),
				})
			}
			log.Tracef("Sane reorg attempted. Total work from "+
				"reorg chain: %v", totalWork)

			// All the headers pass sanity checks. Now we calculate
			// the total work for the known chain.
			knownWork := big.NewInt(0)

			// This should NEVER be nil because the most recent
			// block is always pushed back by resetHeaderState
			knownEl := b.headerList.Back()
			var knownHead *wire.BlockHeader
			for j := uint32(prevNode.height); j > backHeight; j-- {
				if knownEl != nil {
					knownHead = knownEl.Value.(*headerNode).header
					knownEl = knownEl.Prev()
				} else {
					knownHead, _, err = b.server.BlockHeaders.FetchHeader(
						&knownHead.PrevBlock)
					if err != nil {
						log.Criticalf("Can't get block"+
							"header for hash %s: "+
							"%v",
							knownHead.PrevBlock,
							err)
						// Should we panic here?
					}
				}
				knownWork.Add(knownWork,
					blockchain.CalcWork(knownHead.Bits))
			}

			log.Tracef("Total work from known chain: %v", knownWork)

			// Compare the two work totals and reject the new chain
			// if it doesn't have more work than the previously
			// known chain. Disconnect if it's actually less than
			// the known chain.
			switch knownWork.Cmp(totalWork) {
			case 1:
				log.Warnf("Reorg attempt that has less work "+
					"than known chain from peer %s -- "+
					"disconnecting", hmsg.peer.Addr())
				hmsg.peer.Disconnect()
				fallthrough
			case 0:
				return
			default:
			}

			// At this point, we have a valid reorg, so we roll
			// back the existing chain and add the new block
			// header.  We also change the sync peer. Then we can
			// continue with the rest of the headers in the message
			// as if nothing has happened.
			b.syncPeerMutex.Lock()
			b.syncPeer = hmsg.peer
			b.syncPeerMutex.Unlock()
			_, err = b.server.rollBackToHeight(backHeight)
			if err != nil {
				panic(fmt.Sprintf("Rollback failed: %s", err))
				// Should we panic here?
			}

			hdrs := headerfs.BlockHeader{
				BlockHeader: blockHeader,
				Height:      backHeight + 1,
			}
			err = b.server.BlockHeaders.WriteHeaders(hdrs)
			if err != nil {
				log.Criticalf("Couldn't write block to "+
					"database: %s", err)
				// Should we panic here?
			}

			b.resetHeaderState(backHead, int32(backHeight))
			b.headerList.PushBack(&headerNode{
				header: blockHeader,
				height: int32(backHeight + 1),
			})
		}

		// Verify the header at the next checkpoint height matches.
		if b.nextCheckpoint != nil && node.height == b.nextCheckpoint.Height {
			nodeHash := node.header.BlockHash()
			if nodeHash.IsEqual(b.nextCheckpoint.Hash) {
				receivedCheckpoint = true
				log.Infof("Verified downloaded block "+
					"header against checkpoint at height "+
					"%d/hash %s", node.height, nodeHash)
			} else {
				log.Warnf("Block header at height %d/hash "+
					"%s from peer %s does NOT match "+
					"expected checkpoint hash of %s -- "+
					"disconnecting", node.height,
					nodeHash, hmsg.peer.Addr(),
					b.nextCheckpoint.Hash)
				prevCheckpoint := b.findPreviousHeaderCheckpoint(
					node.height)
				log.Infof("Rolling back to previous validated "+
					"checkpoint at height %d/hash %s",
					prevCheckpoint.Height,
					prevCheckpoint.Hash)
				_, err := b.server.rollBackToHeight(uint32(
					prevCheckpoint.Height))
				if err != nil {
					log.Criticalf("Rollback failed: %s",
						err)
					// Should we panic here?
				}
				hmsg.peer.Disconnect()
				return
			}
			break
		}
	}

	log.Tracef("Writing header batch of %v block headers",
		len(headerWriteBatch))
	if len(headerWriteBatch) > 0 {
		// With all the headers in this batch validated, we'll write
		// them all in a single transaction such that this entire batch
		// is atomic.
		err := b.server.BlockHeaders.WriteHeaders(headerWriteBatch...)
		if err != nil {
			panic(fmt.Sprintf("unable to write block header: %v",
				err))
		}
	}

	// When this header is a checkpoint, find the next checkpoint.
	if receivedCheckpoint {
		b.nextCheckpoint = b.findNextHeaderCheckpoint(finalHeight)
	}

	// If not current, request the next batch of headers starting from the
	// latest known header and ending with the next checkpoint.
	if !b.current() || b.server.chainParams.Net == chaincfg.SimNetParams.Net {
		locator := blockchain.BlockLocator([]*chainhash.Hash{finalHash})
		nextHash := zeroHash
		if b.nextCheckpoint != nil {
			nextHash = *b.nextCheckpoint.Hash
		}
		err := hmsg.peer.PushGetHeadersMsg(locator, &nextHash)
		if err != nil {
			log.Warnf("Failed to send getheaders message to "+
				"peer %s: %s", hmsg.peer.Addr(), err)
			return
		}
	}

	// If we've caught up to the headers, we can let the cfheader sync
	// know to catch up to the block header store.
	if b.current() {
		select {
		case b.startCFHeaderSync <- struct{}{}:
		case <-b.quit:
		// If the cfheader sync is too busy to notice our notification,
		// we move along. It'll catch up next time.
		default:
		}
	}
}

// checkHeaderSanity checks the PoW, and timestamp of a block header.
func (b *blockManager) checkHeaderSanity(blockHeader *wire.BlockHeader,
	maxTimestamp time.Time, reorgAttempt bool) error {
	diff, err := b.calcNextRequiredDifficulty(
		blockHeader.Timestamp, reorgAttempt)
	if err != nil {
		return err
	}
	stubBlock := btcutil.NewBlock(&wire.MsgBlock{
		Header: *blockHeader,
	})
	err = blockchain.CheckProofOfWork(stubBlock,
		blockchain.CompactToBig(diff))
	if err != nil {
		return err
	}
	// Ensure the block time is not too far in the future.
	if blockHeader.Timestamp.After(maxTimestamp) {
		return fmt.Errorf("block timestamp of %v is too far in the "+
			"future", blockHeader.Timestamp)
	}
	return nil
}

// calcNextRequiredDifficulty calculates the required difficulty for the block
// after the passed previous block node based on the difficulty retarget rules.
func (b *blockManager) calcNextRequiredDifficulty(newBlockTime time.Time,
	reorgAttempt bool) (uint32, error) {

	hList := b.headerList
	if reorgAttempt {
		hList = b.reorgList
	}

	lastNodeEl := hList.Back()

	// Genesis block.
	if lastNodeEl == nil {
		return b.server.chainParams.PowLimitBits, nil
	}

	lastNode := lastNodeEl.Value.(*headerNode)

	// Return the previous block's difficulty requirements if this block
	// is not at a difficulty retarget interval.
	if (lastNode.height+1)%b.blocksPerRetarget != 0 {
		// For networks that support it, allow special reduction of the
		// required difficulty once too much time has elapsed without
		// mining a block.
		if b.server.chainParams.ReduceMinDifficulty {
			// Return minimum difficulty when more than the desired
			// amount of time has elapsed without mining a block.
			reductionTime := int64(
				b.server.chainParams.MinDiffReductionTime /
					time.Second)
			allowMinTime := lastNode.header.Timestamp.Unix() +
				reductionTime
			if newBlockTime.Unix() > allowMinTime {
				return b.server.chainParams.PowLimitBits, nil
			}

			// The block was mined within the desired timeframe, so
			// return the difficulty for the last block which did
			// not have the special minimum difficulty rule applied.
			prevBits, err := b.findPrevTestNetDifficulty(hList)
			if err != nil {
				return 0, err
			}
			return prevBits, nil
		}

		// For the main network (or any unrecognized networks), simply
		// return the previous block's difficulty requirements.
		return lastNode.header.Bits, nil
	}

	// Get the block node at the previous retarget (targetTimespan days
	// worth of blocks).
	firstNode, err := b.server.BlockHeaders.FetchHeaderByHeight(
		uint32(lastNode.height + 1 - b.blocksPerRetarget),
	)
	if err != nil {
		return 0, err
	}

	// Limit the amount of adjustment that can occur to the previous
	// difficulty.
	actualTimespan := lastNode.header.Timestamp.Unix() -
		firstNode.Timestamp.Unix()
	adjustedTimespan := actualTimespan
	if actualTimespan < b.minRetargetTimespan {
		adjustedTimespan = b.minRetargetTimespan
	} else if actualTimespan > b.maxRetargetTimespan {
		adjustedTimespan = b.maxRetargetTimespan
	}

	// Calculate new target difficulty as:
	//  currentDifficulty * (adjustedTimespan / targetTimespan)
	// The result uses integer division which means it will be slightly
	// rounded down.  Bitcoind also uses integer division to calculate this
	// result.
	oldTarget := blockchain.CompactToBig(lastNode.header.Bits)
	newTarget := new(big.Int).Mul(oldTarget, big.NewInt(adjustedTimespan))
	targetTimeSpan := int64(b.server.chainParams.TargetTimespan /
		time.Second)
	newTarget.Div(newTarget, big.NewInt(targetTimeSpan))

	// Limit new value to the proof of work limit.
	if newTarget.Cmp(b.server.chainParams.PowLimit) > 0 {
		newTarget.Set(b.server.chainParams.PowLimit)
	}

	// Log new target difficulty and return it.  The new target logging is
	// intentionally converting the bits back to a number instead of using
	// newTarget since conversion to the compact representation loses
	// precision.
	newTargetBits := blockchain.BigToCompact(newTarget)
	log.Debugf("Difficulty retarget at block height %d", lastNode.height+1)
	log.Debugf("Old target %08x (%064x)", lastNode.header.Bits, oldTarget)
	log.Debugf("New target %08x (%064x)", newTargetBits,
		blockchain.CompactToBig(newTargetBits))
	log.Debugf("Actual timespan %v, adjusted timespan %v, target timespan %v",
		time.Duration(actualTimespan)*time.Second,
		time.Duration(adjustedTimespan)*time.Second,
		b.server.chainParams.TargetTimespan)

	return newTargetBits, nil
}

// findPrevTestNetDifficulty returns the difficulty of the previous block which
// did not have the special testnet minimum difficulty rule applied.
func (b *blockManager) findPrevTestNetDifficulty(hList *list.List) (uint32, error) {
	startNodeEl := hList.Back()

	// Genesis block.
	if startNodeEl == nil {
		return b.server.chainParams.PowLimitBits, nil
	}

	startNode := startNodeEl.Value.(*headerNode)

	// Search backwards through the chain for the last block without
	// the special rule applied.
	iterEl := startNodeEl
	iterNode := startNode.header
	iterHeight := startNode.height
	for iterNode != nil && iterHeight%b.blocksPerRetarget != 0 &&
		iterNode.Bits == b.server.chainParams.PowLimitBits {

		// Get the previous block node.  This function is used over
		// simply accessing iterNode.parent directly as it will
		// dynamically create previous block nodes as needed.  This
		// helps allow only the pieces of the chain that are needed
		// to remain in memory.
		iterHeight--
		el := iterEl.Prev()
		if el != nil {
			iterNode = el.Value.(*headerNode).header
		} else {
			node, err := b.server.BlockHeaders.FetchHeaderByHeight(
				uint32(iterHeight),
			)
			if err != nil {
				log.Errorf("GetBlockByHeight: %s", err)
				return 0, err
			}
			iterNode = node
		}
	}

	// Return the found difficulty or the minimum difficulty if no
	// appropriate block was found.
	lastBits := b.server.chainParams.PowLimitBits
	if iterNode != nil {
		lastBits = iterNode.Bits
	}
	return lastBits, nil
}
