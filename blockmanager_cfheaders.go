package neutrino

// This file holds regular filter header sync: the filter sync loop that runs
// the filtersync.Syncer, the checkpointed and uncheckpointed query paths, and
// the resolution of conflicts between peers.

import (
	"bytes"
	"fmt"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/lightninglabs/neutrino/chainsync"
	"github.com/lightninglabs/neutrino/filtersync"
	"github.com/lightninglabs/neutrino/headerfs"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
)

// filterSyncLoop runs regular filter header sync. It must be run as a
// goroutine. It feeds events to a filtersync.Syncer, which decides what to do
// next, and performs the effects the Syncer returns on this goroutine: the
// tip feed waits, the retry delays and the network rounds. The network rounds
// block this goroutine for as long as they take, which is fine because nothing
// else sends to it and the tip feed is level-triggered, so a tip change that
// lands during a round is seen by the next wait.
func (b *blockManager) filterSyncLoop() {
	defer log.Trace("Committed filter header handler done")

	syncer := filtersync.NewSyncer(filtersync.Config{
		BlockCheckpoints: b.cfg.ChainParams.Checkpoints,
		RetryDelay:       retryTimeout,
	})

	// Each batch of effects ends with one that yields an event, and the
	// Syncer returns no effects once it has stopped, so the loop ends when
	// a batch is empty.
	effects := syncer.Handle(filtersync.Start{Tips: b.tipFeed.Snapshot()})
	for len(effects) > 0 {
		var next []filtersync.Effect
		for _, effect := range effects {
			event := b.runFilterSyncEffect(effect)
			if event == nil {
				continue
			}

			next = syncer.Handle(event)
			break
		}

		effects = next
	}
}

// runFilterSyncEffect performs one effect of the filter sync loop and returns
// the event it yields, or nil for a PollQuit that found the quit channel open.
func (b *blockManager) runFilterSyncEffect(
	effect filtersync.Effect) filtersync.Event {

	store := b.cfg.RegFilterHeaders

	switch e := effect.(type) {
	case filtersync.WaitForTips:
		tips, ok := b.tipFeed.Wait(
			b.quit, b.filterSyncReady(e.Condition),
		)
		if !ok {
			return filtersync.Quit{}
		}

		return filtersync.TipsChanged{Tips: tips}

	case filtersync.ReadBlockTip:
		lastHeader, lastHeight, err := b.cfg.BlockHeaders.ChainTip()
		if err != nil {
			return filtersync.BlockTipRead{Err: err}
		}

		return filtersync.BlockTipRead{
			Hash:   lastHeader.BlockHash(),
			Height: lastHeight,
			Tips:   b.tipFeed.Snapshot(),
		}

	case filtersync.FetchCheckpoints:
		return filtersync.CheckpointsFetched{
			ByPeer: b.getCheckpts(&e.StopHash, e.FilterType),
		}

	case filtersync.ResolveConflict:
		good, err := b.resolveConflict(
			e.Checkpoints, store, e.FilterType,
		)

		return filtersync.ConflictResolved{Good: good, Err: err}

	case filtersync.FetchCheckpointed:
		err := b.getCheckpointedCFHeaders(
			e.Checkpoints, store, e.FilterType,
		)
		if err != nil {
			return filtersync.CheckpointedFetched{Err: err}
		}

		return filtersync.CheckpointedFetched{
			HeadersSynced: b.BlockHeadersSynced(),
			Tips:          b.tipFeed.Snapshot(),
		}

	case filtersync.FetchUncheckpointed:
		return filtersync.UncheckpointedFetched{
			Err: b.getUncheckpointedCFHeaders(e.FilterType),
		}

	case filtersync.RetryAfter:
		select {
		case <-time.After(e.Delay):
			return filtersync.RetryElapsed{
				Tips: b.tipFeed.Snapshot(),
			}

		case <-b.quit:
			return filtersync.Quit{}
		}

	case filtersync.PollQuit:
		select {
		case <-b.quit:
			return filtersync.Quit{}
		default:
			return nil
		}
	}

	log.Errorf("Filter sync got unknown effect %T", effect)

	return filtersync.Quit{}
}

// filterSyncReady returns the tip feed predicate for a wait condition.
func (b *blockManager) filterSyncReady(
	cond filtersync.WaitCondition) func(chainstore.Tips) bool {

	if cond == filtersync.WaitTipMismatch {
		return func(tips chainstore.Tips) bool {
			return tips.FilterHash != tips.BlockHash
		}
	}

	return func(tips chainstore.Tips) bool {
		return tips.FilterHeight+wire.CFCheckptInterval <=
			tips.BlockHeight || b.BlockHeadersSynced()
	}
}

// getUncheckpointedCFHeaders gets the next batch of cfheaders from the
// network, if it can, and resolves any conflicts between them. It then writes
// any verified headers to the store.
func (b *blockManager) getUncheckpointedCFHeaders(
	fType wire.FilterType) error {

	// Reconcile and capture a consistent filter and block query plan.
	snapshot, err := b.submitCFHeaderReconciliation()
	if err != nil {
		return err
	}
	filterTip := snapshot.FilterTip
	filtHeight := snapshot.FilterHeight
	blockHeight := snapshot.BlockHeight

	// If the block height is somehow before the filter height, then this
	// means that we may still be handling a re-org, so we'll bail our so
	// we can retry after a timeout.
	if blockHeight < filtHeight {
		return fmt.Errorf("reorg in progress, waiting to get "+
			"uncheckpointed cfheaders (block height %d, filter "+
			"height %d", blockHeight, filtHeight)
	}

	// If the heights match, then we're fully synced, so we don't need to
	// do anything from there.
	if blockHeight == filtHeight {
		log.Tracef("cfheaders already caught up to blocks")

		// The owner snapshot already reconciled any durable prefix.
		return nil
	}

	log.Infof("Attempting to fetch set of un-checkpointed filters "+
		"at height=%v, hash=%v", blockHeight, snapshot.BlockHash)

	// Query all peers for the responses.
	startHeight := filtHeight + 1
	headers, numHeaders := b.getCFHeadersForAllPeersAt(
		startHeight, fType, snapshot.StopHash, snapshot.StopHeight,
	)

	// Ban any peer that responds with the wrong prev filter header.
	for peer, msg := range headers {
		if msg.PrevFilterHeader != filterTip {
			err := b.cfg.BanPeer(peer, banman.InvalidFilterHeader)
			if err != nil {
				log.Errorf(
					"Unable to ban peer %v: %v",
					peer,
					err,
				)
			}
			delete(headers, peer)
		}
	}

	if len(headers) == 0 {
		return fmt.Errorf("couldn't get cfheaders from peers")
	}

	if err := b.resolveCFHeaderMismatches(
		headers, nil, startHeight, numHeaders, fType,
	); err != nil {
		return err
	}

	// Get the longest filter hash chain and write it to the store.
	key, maxLen := "", 0
	for peer, msg := range headers {
		if len(msg.FilterHashes) > maxLen {
			key, maxLen = peer, len(msg.FilterHashes)
		}
	}

	// We'll now fetch the set of pristine headers from the map. If ALL the
	// peers were banned, then we won't have a set of headers at all. We'll
	// return nil so we can go to the top of the loop and fetch from a new
	// set of peers.
	pristineHeaders, ok := headers[key]
	if !ok {
		return fmt.Errorf("all peers served bogus headers, retrying " +
			"with new set")
	}

	_, _, err = b.submitCFHeaders(
		[]*wire.MsgCFHeaders{pristineHeaders},
	)
	return err
}

// checkpointedCFHeadersQuery holds all information necessary to perform and
// handle a query for checkpointed filter headers.
type checkpointedCFHeadersQuery struct {
	blockMgr    *blockManager
	msgs        []wire.Message
	checkpoints []*chainhash.Hash
	stopHashes  map[chainhash.Hash]uint32
	headerChan  chan *wire.MsgCFHeaders
}

// requests creates the query.Requests for this CF headers query.
func (c *checkpointedCFHeadersQuery) requests() []*querysyncwm.Request {
	reqs := make([]*querysyncwm.Request, len(c.msgs))
	for idx, m := range c.msgs {
		reqs[idx] = &querysyncwm.Request{
			Req:        m,
			HandleResp: c.handleResponse,
		}
	}
	return reqs
}

// handleResponse is the internal response handler used for requests for this
// CFHeaders query.
func (c *checkpointedCFHeadersQuery) handleResponse(req, resp wire.Message,
	peerAddr string) querysyncwm.Progress {

	r, ok := resp.(*wire.MsgCFHeaders)
	if !ok {
		// We are only looking for cfheaders messages.
		return querysyncwm.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	q, ok := req.(*wire.MsgGetCFHeaders)
	if !ok {
		// We sent a getcfheaders message, so that's what we should be
		// comparing against.
		return querysyncwm.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	// The response doesn't match the query.
	if q.FilterType != r.FilterType || q.StopHash != r.StopHash {
		return querysyncwm.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	checkPointIndex, ok := c.stopHashes[r.StopHash]
	if !ok {
		// We never requested a matching stop hash.
		return querysyncwm.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	// Use either the genesis header or the previous checkpoint index as
	// the previous checkpoint when verifying that the filter headers in
	// the response match up.
	prevCheckpoint := &c.blockMgr.genesisHeader
	if checkPointIndex > 0 {
		prevCheckpoint = c.checkpoints[checkPointIndex-1]
	}

	// The index of the next checkpoint will depend on whether the query
	// was able to allocate maxCFCheckptsPerQuery.
	nextCheckPointIndex := checkPointIndex + maxCFCheckptsPerQuery - 1
	if nextCheckPointIndex >= uint32(len(c.checkpoints)) {
		nextCheckPointIndex = uint32(len(c.checkpoints)) - 1
	}
	nextCheckpoint := c.checkpoints[nextCheckPointIndex]

	// The response doesn't match the checkpoint.
	if !filtersync.VerifyCheckpoint(prevCheckpoint, nextCheckpoint, r) {
		log.Warnf("Checkpoints at index %v don't match response!!!",
			checkPointIndex)

		// If the peer gives us a header that doesn't match what we
		// know to be the best checkpoint, then we'll ban the peer so
		// we can re-allocate the query elsewhere.
		err := c.blockMgr.cfg.BanPeer(
			peerAddr, banman.InvalidFilterHeaderCheckpoint,
		)
		if err != nil {
			log.Errorf("Unable to ban peer %v: %v", peerAddr, err)
		}

		return querysyncwm.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	// At this point, the response matches the query, and the relevant
	// checkpoint we got earlier, so we'll deliver the verified headers on
	// the headerChan. We'll also return a Progress indicating the query
	// finished, that the peer looking for the answer to this query can
	// move on to the next query.
	select {
	case c.headerChan <- r:
	case <-c.blockMgr.quit:
		return querysyncwm.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	return querysyncwm.Progress{
		Finished:   true,
		Progressed: true,
	}
}

// getCheckpointedCFHeaders catches a filter header store up with the
// checkpoints we got from the network. It assumes that the filter header store
// matches the checkpoints up to the tip of the store. A store or response
// error is returned rather than panicking, since a transient SQL failure must
// not crash the host process; the caller retries.
func (b *blockManager) getCheckpointedCFHeaders(checkpoints []*chainhash.Hash,
	store headerfs.FilterHeaderStore, fType wire.FilterType) error {

	// We keep going until we've caught up the filter header store with the
	// latest known checkpoint.
	curHeader, curHeight, err := store.ChainTip()
	if err != nil {
		return fmt.Errorf("failed getting chaintip from filter "+
			"store: %w", err)
	}

	initialFilterHeader := curHeader

	log.Infof("Fetching set of checkpointed cfheaders filters from "+
		"height=%v, hash=%v", curHeight, curHeader)

	// The starting interval is the checkpoint index that we'll be starting
	// from based on our current height in the filter header index.
	startingInterval := curHeight / wire.CFCheckptInterval

	log.Infof("Starting to query for cfheaders from "+
		"checkpoint_interval=%v, checkpoints=%v", startingInterval,
		len(checkpoints))

	// We'll determine how many queries we'll make based on our starting
	// interval and our set of checkpoints. Each query will attempt to
	// fetch maxCFCheckptsPerQuery intervals worth of filter headers. If
	// maxCFCheckptsPerQuery is not a factor of the number of checkpoint
	// intervals to fetch, then an additional query will exist that spans
	// the remaining checkpoint intervals.
	numCheckpts := uint32(len(checkpoints)) - startingInterval
	numQueries := (numCheckpts + maxCFCheckptsPerQuery - 1) /
		maxCFCheckptsPerQuery
	queryMsgs := make([]wire.Message, 0, numQueries)

	// We'll also create an additional set of maps that we'll use to
	// re-order the responses as we get them in.
	queryResponses := make(map[uint32]*wire.MsgCFHeaders, numQueries)
	stopHashes := make(map[chainhash.Hash]uint32, numQueries)

	// Generate all of the requests we'll be batching and space to store
	// the responses. Also make a map of stophash to index to make it
	// easier to match against incoming responses.
	//
	// TODO(roasbeef): extract to func to test
	currentInterval := startingInterval
	for currentInterval < uint32(len(checkpoints)) {
		// Each checkpoint is spaced wire.CFCheckptInterval after the
		// prior one, so we'll fetch headers in batches using the
		// checkpoints as a guide. Our queries will consist of
		// maxCFCheckptsPerQuery unless we don't have enough
		// checkpoints to do so. In that case, our query will consist
		// of whatever is left.
		startHeightRange :=
			(currentInterval * wire.CFCheckptInterval) + 1

		nextInterval := currentInterval + maxCFCheckptsPerQuery
		if nextInterval > uint32(len(checkpoints)) {
			nextInterval = uint32(len(checkpoints))
		}
		endHeightRange := nextInterval * wire.CFCheckptInterval

		log.Tracef("Checkpointed cfheaders request start_range=%v, "+
			"end_range=%v", startHeightRange, endHeightRange)

		// In order to fetch the range, we'll need the block header for
		// the end of the height range.
		stopHeader, err := b.cfg.BlockHeaders.FetchHeaderByHeight(
			endHeightRange,
		)
		if err != nil {
			return fmt.Errorf("failed getting block header at "+
				"height %v: %w", endHeightRange, err)
		}
		stopHash := stopHeader.BlockHash()

		// Once we have the stop hash, we can construct the query
		// message itself.
		queryMsg := wire.NewMsgGetCFHeaders(
			fType, startHeightRange, &stopHash,
		)

		// We'll mark that the ith interval is queried by this message,
		// and also map the stop hash back to the index of this
		// message.
		queryMsgs = append(queryMsgs, queryMsg)
		stopHashes[stopHash] = currentInterval

		// With the query starting at the current interval constructed,
		// we'll move onto the next one.
		currentInterval = nextInterval
	}

	// With nothing left to fetch, make sure everything durable is
	// published; a failed write may have landed. A read error here is
	// returned so the caller retries.
	batchesCount := len(queryMsgs)
	if batchesCount == 0 {
		_, err := b.submitCFHeaderReconciliation()
		return err
	}

	log.Infof("Attempting to query for %v cfheader batches", batchesCount)

	// We'll track the next interval we expect to receive headers for.
	currentInterval = startingInterval

	// With the set of messages constructed, we'll now request the batch
	// all at once. This message will distributed the header requests
	// amongst all active peers, effectively sharding each query
	// dynamically.
	headerChan := make(chan *wire.MsgCFHeaders, len(queryMsgs))
	q := checkpointedCFHeadersQuery{
		blockMgr:    b,
		msgs:        queryMsgs,
		checkpoints: checkpoints,
		stopHashes:  stopHashes,
		headerChan:  headerChan,
	}

	// Hand the queries to the work manager, and consume the verified
	// responses as they come back.
	//
	// A cfheader batch can hold hundreds of sub-requests (one per
	// CFCheckptInterval; growing with chain length), so we bound it on two
	// axes. ProgressTimeout is the primary gate: as long as some peer
	// answers a request within the window the batch keeps running, and we
	// abort if the peer pool goes silent. Timeout is a generous hard
	// ceiling that caps total runtime; without it a single peer trickling
	// one response every (progressTimeout - epsilon) could keep the idle
	// timer alive for hours and amplify a slow-peer attack.
	//
	errChan := b.cfg.QueryDispatcher.Query(
		q.requests(), querysyncwm.Cancel(b.quit),
		querysyncwm.NoRetryMax(),
		querysyncwm.Timeout(
			filtersync.CheckpointedCFHeadersBatchTimeout(
				batchesCount,
			),
		),
		querysyncwm.ProgressTimeout(30*time.Second),
	)

	// Keep waiting for more headers as long as we haven't received an
	// answer for our last checkpoint, and no error is encountered.
	for {
		var r *wire.MsgCFHeaders
		select {
		case r = <-headerChan:
		case err := <-errChan:
			switch {
			case err == querysyncwm.ErrWorkManagerShuttingDown:
				return nil
			case err != nil:
				log.Errorf("Query finished with error before "+
					"all responses received: %v", err)
				return nil
			}

			// The query did finish successfully, but continue to
			// allow picking up the last header sent on the
			// headerChan.
			continue

		case <-b.quit:
			return nil
		}

		checkPointIndex := stopHashes[r.StopHash]

		// Find the first and last height for the blocks represented by
		// this message.
		startHeight := checkPointIndex*wire.CFCheckptInterval + 1
		lastHeight := startHeight + uint32(len(r.FilterHashes)) - 1

		log.Debugf("Got cfheaders from height=%v to "+
			"height=%v, prev_hash=%v", startHeight,
			lastHeight, r.PrevFilterHeader)

		// If this is out of order but not yet written, we can verify
		// that the checkpoints match, and then store them.
		if startHeight > curHeight+1 {
			log.Debugf("Got response for headers at "+
				"height=%v, only at height=%v, stashing",
				startHeight, curHeight)
		}

		// If this is out of order stuff that's already been written,
		// we can ignore it.
		if lastHeight <= curHeight {
			log.Debugf("Received out of order reply "+
				"end_height=%v, already written", lastHeight)
			continue
		}

		// Add the verified response to our cache.
		queryResponses[checkPointIndex] = r

		// Then, we cycle through any cached messages, collecting
		// contiguous responses into bounded write batches. This
		// preserves strict ordered commits while avoiding one sqlite
		// transaction per 2,000-header cfheaders response.
		for {
			writeBatch := make(
				[]*wire.MsgCFHeaders, 0,
				maxCFHeadersMsgsPerWriteBatch,
			)
			adjustedInitialRange := false

			for len(writeBatch) < maxCFHeadersMsgsPerWriteBatch {
				// If we don't yet have the next response, then
				// we'll break out so we can wait for the peers
				// to respond with this message.
				r, ok := queryResponses[currentInterval]
				if !ok {
					break
				}

				// We have another response to write, so delete
				// it from the cache and add it to the batch.
				delete(queryResponses, currentInterval)

				startHeight := currentInterval*
					wire.CFCheckptInterval + 1

				// If this is the very first range we've
				// requested, we may already have a portion of
				// the headers written to disk.
				//
				// TODO(roasbeef): can eventually special case
				// handle this at the top
				if !adjustedInitialRange &&
					bytes.Equal(
						curHeader[:],
						initialFilterHeader[:],
					) {

					// So we'll set the prev header to our
					// best known header, and seek within
					// the header range a bit so we don't
					// write any duplicate headers.
					r.PrevFilterHeader = *curHeader
					offset := curHeight + 1 - startHeight
					if offset > uint32(
						len(r.FilterHashes),
					) {

						return fmt.Errorf(
							"initial "+
								"cfheaders "+
								"offset %d "+
								"exceeds "+
								"response "+
								"size %d",
							offset,
							len(r.FilterHashes),
						)
					}
					r.FilterHashes = r.FilterHashes[offset:]
					startHeight += offset
					adjustedInitialRange = true

					log.Debugf(
						"Using offset %d for initial "+
							"filter header range "+
							"(new prev_hash=%v)",
						offset,
						r.PrevFilterHeader,
					)
				}

				if len(r.FilterHashes) == 0 {
					currentInterval = startHeight /
						wire.CFCheckptInterval
					continue
				}

				writeBatch = append(writeBatch, r)

				lastHeight := startHeight +
					uint32(len(r.FilterHashes)) - 1
				currentInterval = lastHeight /
					wire.CFCheckptInterval
			}

			if len(writeBatch) == 0 {
				break
			}

			log.Debugf("Writing %d cfheaders response(s) at "+
				"height=%v to next checkpoint", len(writeBatch),
				curHeight)

			// As we write the set of headers to disk, we also
			// obtain the hash of the last filter header we've
			// written to disk so we can properly set the
			// PrevFilterHeader field of the next message.
			curHeader, curHeight, err = b.submitCFHeaders(
				writeBatch,
			)
			if err != nil {
				return fmt.Errorf("couldn't write "+
					"cfheaders messages: %w", err)
			}

			// Update the next interval to write to reflect our
			// current height.
			currentInterval = curHeight / wire.CFCheckptInterval
		}

		// If the current interval is beyond our checkpoints, we are
		// done.
		if currentInterval >= uint32(len(checkpoints)) {
			log.Infof("Successfully got filter headers "+
				"for %d checkpoints", len(checkpoints))
			break
		}
	}

	return nil
}

// submitCFHeaderReconciliation asks the chain store actor to reconcile
// publication and returns a filter query snapshot.
func (b *blockManager) submitCFHeaderReconciliation() (
	chainstore.FilterSnapshot, error) {

	result := b.chainStore.Ask(b.quitCtx, chainstore.SnapshotRequest{})
	if result.IsErr() {
		return chainstore.FilterSnapshot{}, result.Err()
	}
	reply := result.UnwrapOr(chainstore.Reply{})

	return reply.Filter, reply.Err
}

// submitCFHeaders sends a completed filter batch to the chain store actor,
// which checks it against the block header store and writes it.
func (b *blockManager) submitCFHeaders(
	msgs []*wire.MsgCFHeaders) (*chainhash.Hash, uint32, error) {

	result := b.chainStore.Ask(
		b.quitCtx, chainstore.FilterCommit{Messages: msgs},
	)
	if result.IsErr() {
		return nil, 0, result.Err()
	}
	reply := result.UnwrapOr(chainstore.Reply{})
	if reply.Err != nil {
		return nil, 0, reply.Err
	}
	hash := reply.Filter.FilterTip

	return &hash, reply.Filter.FilterHeight, nil
}

// resolveConflict finds the correct checkpoint information, rewinds the header
// store if it's incorrect, and bans any peers giving us incorrect header
// information.
func (b *blockManager) resolveConflict(
	checkpoints map[string][]*chainhash.Hash,
	store headerfs.FilterHeaderStore, fType wire.FilterType) (
	[]*chainhash.Hash, error) {

	// First check the served checkpoints against the hardcoded ones.
	for peer, cp := range checkpoints {
		for i, header := range cp {
			height := uint32((i + 1) * wire.CFCheckptInterval)
			err := chainsync.ValidateCFHeader(
				b.cfg.ChainParams, fType, height, header,
			)
			if err == chainsync.ErrCheckpointMismatch {
				log.Warnf("Banning peer=%v since served "+
					"checkpoints didn't match our "+
					"checkpoint at height %d", peer, height)

				err := b.cfg.BanPeer(
					peer,
					banman.InvalidFilterHeaderCheckpoint,
				)
				if err != nil {
					log.Errorf("Unable to ban peer %v: %v",
						peer, err)
				}
				delete(checkpoints, peer)
				break
			}
			if err != nil {
				return nil, err
			}
		}
	}

	if len(checkpoints) == 0 {
		return nil, fmt.Errorf("no peer is serving good cfheader " +
			"checkpoints")
	}

	// Check if the remaining checkpoints are sane.
	heightDiff, err := filtersync.CheckCFCheckptSanity(checkpoints, store)
	if err != nil {
		return nil, err
	}

	// If we got -1, we have full agreement between all peers and the
	// store.
	if heightDiff == -1 {
		// Take the first peer's checkpoint list and return it.
		for _, checkpts := range checkpoints {
			return checkpts, nil
		}
	}

	log.Warnf(
		"Detected mismatch at index=%v for checkpoints!!!",
		heightDiff,
	)

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
	// TODO(halseth): check if peer serves headers that matches its
	// checkpoints
	startHeight := uint32(heightDiff) * wire.CFCheckptInterval
	headers, numHeaders := b.getCFHeadersForAllPeers(startHeight, fType)

	// Make sure we're working off the same baseline. Otherwise, we want to
	// go back and get checkpoints again.
	var hash chainhash.Hash
	for _, msg := range headers {
		if hash == zeroHash {
			hash = msg.PrevFilterHeader
		} else if hash != msg.PrevFilterHeader {
			return nil, fmt.Errorf("mismatch between filter " +
				"headers expected to be the same")
		}
	}

	if err := b.resolveCFHeaderMismatches(
		headers, checkpoints, startHeight, numHeaders, fType,
	); err != nil {
		return nil, err
	}

	// Drop peers that supplied checkpoints but failed to answer the
	// follow-up header query without banning them.
	for peer := range checkpoints {
		if _, ok := headers[peer]; !ok {
			if err := b.cfg.DisconnectPeer(peer); err != nil {
				log.Errorf("Unable to disconnect peer %v: %v",
					peer, err)
			}
			delete(checkpoints, peer)
		}
	}

	// Check sanity again. If we're sane, return a matching checkpoint
	// list. If not, return an error and download checkpoints from
	// remaining peers.
	heightDiff, err = filtersync.CheckCFCheckptSanity(checkpoints, store)
	if err != nil {
		return nil, err
	}

	// If we got -1, we have full agreement between all peers and the
	// store.
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

// resolveCFHeaderMismatches removes conflicting peers before advancing to the
// next filter hash. A first pass can remove nonresponders while responsive
// peers still disagree, so each height is checked again until it agrees.
func (b *blockManager) resolveCFHeaderMismatches(
	headers map[string]*wire.MsgCFHeaders,
	checkpoints map[string][]*chainhash.Hash, startHeight uint32,
	numHeaders int, fType wire.FilterType) error {

	for i := 0; i < numHeaders; i++ {
		for filtersync.CheckForCFHeaderMismatch(headers, i) {
			height := startHeight + uint32(i)
			invalid, unresponsive, err := b.detectBadPeers(
				headers, height, uint32(i), fType,
			)
			if err != nil {
				return err
			}

			before := len(headers)
			actedOn := b.banAndDisconnect(
				invalid, unresponsive,
				banman.InvalidFilterHeader,
			)
			for _, peer := range actedOn {
				delete(headers, peer)
				delete(checkpoints, peer)
			}
			if len(headers) == before {
				return fmt.Errorf("filter header mismatch "+
					"at height %d", height)
			}
		}
	}
	return nil
}

// banAndDisconnect bans every peer that served verified invalid filter data
// using the given reason, and disconnects (without banning) every peer that
// did not respond to a follow-up query. It returns all peers that were acted
// on so the caller can drop them from its working maps.
func (b *blockManager) banAndDisconnect(invalidPeers,
	unresponsivePeers []string, banReason banman.Reason) []string {

	if len(invalidPeers) > 0 {
		log.Warnf("Banning %v peers due to invalid filters",
			len(invalidPeers))
	}
	for _, peer := range invalidPeers {
		err := b.cfg.BanPeer(peer, banReason)
		if err != nil {
			log.Errorf("Unable to ban peer %v: %v", peer, err)
		}
	}

	if len(unresponsivePeers) > 0 {
		log.Warnf("Disconnecting %v unresponsive peers without ban",
			len(unresponsivePeers))
	}
	for _, peer := range unresponsivePeers {
		err := b.cfg.DisconnectPeer(peer)
		if err != nil {
			log.Errorf("Unable to disconnect peer %v: %v", peer,
				err)
		}
	}

	actedOn := make([]string, 0, len(invalidPeers)+len(unresponsivePeers))
	actedOn = append(actedOn, invalidPeers...)
	actedOn = append(actedOn, unresponsivePeers...)

	return actedOn
}

// detectBadPeers fetches filters and the block at the given height to attempt
// to detect which peers are serving bad filters. It returns two separate sets
// of peers. invalidPeers are peers that we verified served invalid filter
// data, and should be banned. unresponsivePeers are peers that did not answer
// the follow-up filter query. A missing response is not proof that a peer
// served invalid data, so those peers should be disconnected rather than
// banned.
func (b *blockManager) detectBadPeers(headers map[string]*wire.MsgCFHeaders,
	targetHeight, filterIndex uint32,
	fType wire.FilterType) ([]string, []string, error) {

	var invalidPeers, unresponsivePeers []string

	log.Warnf("Detected cfheader mismatch at height=%v!!!", targetHeight)

	// Resolve the disputed block from the response stop hash, so a reorg
	// cannot mix filter data from different branches.
	var stop chainhash.Hash
	var count int
	for _, msg := range headers {
		if len(msg.FilterHashes) <= int(filterIndex) {
			return nil, nil, fmt.Errorf(
				"filter index %d out of range", filterIndex,
			)
		}
		if count == 0 {
			stop, count = msg.StopHash, len(msg.FilterHashes)
			continue
		}
		if msg.StopHash != stop || len(msg.FilterHashes) != count {
			return nil, nil, fmt.Errorf(
				"inconsistent cfheader response range",
			)
		}
	}
	if count == 0 {
		return nil, nil, fmt.Errorf("no cfheader responses")
	}
	numAncestors := uint32(count-1) - filterIndex
	ancestors, height, err := b.cfg.BlockHeaders.FetchHeaderAncestors(
		numAncestors, &stop,
	)
	if err != nil {
		return nil, nil, err
	}
	if height != targetHeight || len(ancestors) != int(numAncestors)+1 {
		return nil, nil, fmt.Errorf(
			"cfheader response does not cover height %d",
			targetHeight,
		)
	}
	header := &ancestors[0]

	// Fetch filters from the peers in question.
	// TODO(halseth): query only peers from headers map.
	filtersFromPeers := b.fetchFilterFromAllPeers(
		targetHeight, header.BlockHash(), fType,
	)

	for peer, msg := range headers {
		filter, ok := filtersFromPeers[peer]

		// If a peer did not respond, we have not proven that it served
		// invalid data. We still drop it from this round so it cannot
		// stall our sync by only answering header queries, but we mark
		// it unresponsive so the caller disconnects it instead of
		// banning it.
		if !ok {
			log.Warnf("Peer %v did not respond to filter "+
				"request, disconnecting without ban", peer)
			unresponsivePeers = append(unresponsivePeers, peer)
			continue
		}

		// If the peer is serving filters that isn't consistent with
		// its filter hashes, ban it.
		hash, err := builder.GetFilterHash(filter)
		if err != nil {
			return nil, nil, err
		}
		if hash != *msg.FilterHashes[filterIndex] {
			log.Warnf("Peer %v serving filters not consistent "+
				"with filter hashes, considering bad.", peer)
			invalidPeers = append(invalidPeers, peer)
		}
	}

	// If we already found peers to act on, ban or disconnect them without
	// doing the more expensive block-based reconciliation.
	if len(invalidPeers) != 0 || len(unresponsivePeers) != 0 {
		return invalidPeers, unresponsivePeers, nil
	}

	// If all peers responded with consistent filters and hashes, get the
	// block and use it to detect who is serving bad filters.
	block, err := b.cfg.GetBlock(header.BlockHash())
	if err != nil {
		return nil, nil, err
	}

	log.Warnf("Attempting to reconcile cfheader mismatch amongst %v peers",
		len(headers))

	invalidPeers, err = filtersync.ResolveFilterMismatchFromBlock(
		block.MsgBlock(), fType, filtersFromPeers,

		// We'll require a strict majority of our peers to agree on
		// filters.
		(len(filtersFromPeers)+2)/2,
	)
	if err != nil {
		return nil, nil, err
	}

	return invalidPeers, unresponsivePeers, nil
}

// getCFHeadersForAllPeers runs a query for cfheaders at a specific height and
// returns a map of responses from all peers. The second return value is the
// number for cfheaders in each response.
func (b *blockManager) getCFHeadersForAllPeers(height uint32,
	fType wire.FilterType) (map[string]*wire.MsgCFHeaders, int) {

	// Get the header we expect at either the tip of the block header store
	// or at the end of the maximum-size response message, whichever is
	// larger.
	stopHeader, stopHeight, err := b.cfg.BlockHeaders.ChainTip()
	if err != nil {
		return nil, 0
	}
	if stopHeight-height >= wire.MaxCFHeadersPerMsg {
		stopHeader, err = b.cfg.BlockHeaders.FetchHeaderByHeight(
			height + wire.MaxCFHeadersPerMsg - 1,
		)
		if err != nil {
			return nil, 0
		}

		// We'll make sure we also update our stopHeight so we know how
		// many headers to expect below.
		stopHeight = height + wire.MaxCFHeadersPerMsg - 1
	}

	return b.getCFHeadersForAllPeersAt(
		height, fType, stopHeader.BlockHash(), stopHeight,
	)
}

// getCFHeadersForAllPeersAt queries an already selected stop block.
func (b *blockManager) getCFHeadersForAllPeersAt(height uint32,
	fType wire.FilterType, stopHash chainhash.Hash, stopHeight uint32) (
	map[string]*wire.MsgCFHeaders, int) {

	headers := make(map[string]*wire.MsgCFHeaders)
	// Calculate the hash and use it to create the query message.
	msg := wire.NewMsgGetCFHeaders(fType, height, &stopHash)
	numHeaders := int(stopHeight - height + 1)

	// Send the query to all peers and record their responses in the map.
	err := b.cfg.Broadcaster.Broadcast(
		&querysyncwm.Broadcast{
			Msg: msg,
			HandleResp: func(peer querysyncwm.Peer,
				resp wire.Message,
				ctl *querysyncwm.BroadcastControl) {

				m, isHeaders := resp.(*wire.MsgCFHeaders)
				if !isHeaders {
					return
				}

				if m.StopHash != stopHash ||
					m.FilterType != fType ||
					len(m.FilterHashes) != numHeaders {

					return
				}

				headers[peer.Addr()] = m

				// We got an answer from this peer so that
				// peer's goroutine can stop.
				ctl.PeerDone()
			},
		},
		querysyncwm.Timeout(cfHeaderCollectTimeout),
	)
	if err != nil {
		log.Debugf("CF header broadcast ended early: %v", err)
	}

	return headers, numHeaders
}

// fetchFilterFromAllPeers attempts to fetch a filter for the target filter
// type and blocks from all peers connected to the block manager. This method
// returns a map which allows the caller to match a peer to the filter it
// responded with.
func (b *blockManager) fetchFilterFromAllPeers(
	height uint32, blockHash chainhash.Hash,
	filterType wire.FilterType) map[string]*gcs.Filter {

	// We'll use this map to collate all responses we receive from each
	// peer.
	filterResponses := make(map[string]*gcs.Filter)

	// We'll now request the target filter from each peer, using a stop
	// hash at the target block hash to ensure we only get a single filter.
	fitlerReqMsg := wire.NewMsgGetCFilters(filterType, height, &blockHash)
	err := b.cfg.Broadcaster.Broadcast(&querysyncwm.Broadcast{
		Msg: fitlerReqMsg,
		HandleResp: func(peer querysyncwm.Peer, resp wire.Message,
			ctl *querysyncwm.BroadcastControl) {

			switch response := resp.(type) {
			// We're only interested in "cfilter" messages.
			case *wire.MsgCFilter:
				// If the response doesn't match our request.
				// Ignore this message.
				if blockHash != response.BlockHash ||
					filterType != response.FilterType {

					return
				}

				// Now that we know we have the proper filter,
				// we'll decode it into an object the caller
				// can utilize.
				gcsFilter, err := gcs.FromNBytes(
					builder.DefaultP, builder.DefaultM,
					response.Data,
				)
				if err != nil {
					// Malformed filter data. We can ignore
					// this message.
					return
				}

				// Now that we're able to properly parse this
				// filter, we'll assign it to its source peer,
				// and stop listening to it as it has answered.
				filterResponses[peer.Addr()] = gcsFilter
				ctl.PeerDone()

			default:
			}
		},
	}, querysyncwm.Timeout(QueryTimeout))
	if err != nil {
		log.Debugf("Filter broadcast ended early: %v", err)
	}

	return filterResponses
}

// getCheckpts runs a query for cfcheckpts against all peers and returns a map
// of responses.
func (b *blockManager) getCheckpts(lastHash *chainhash.Hash,
	fType wire.FilterType) map[string][]*chainhash.Hash {

	checkpoints := make(map[string][]*chainhash.Hash)
	getCheckptMsg := wire.NewMsgGetCFCheckpt(fType, lastHash)
	err := b.cfg.Broadcaster.Broadcast(&querysyncwm.Broadcast{
		Msg: getCheckptMsg,
		HandleResp: func(peer querysyncwm.Peer, resp wire.Message,
			ctl *querysyncwm.BroadcastControl) {

			m, isCheckpoint := resp.(*wire.MsgCFCheckpt)
			if !isCheckpoint {
				return
			}

			if m.FilterType == fType && m.StopHash == *lastHash {
				checkpoints[peer.Addr()] = m.FilterHeaders
				ctl.PeerDone()
			}
		},
	}, querysyncwm.Timeout(QueryTimeout))
	if err != nil {
		log.Debugf("Checkpoint broadcast ended early: %v", err)
	}

	return checkpoints
}
