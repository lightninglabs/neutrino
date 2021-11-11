// NOTE: THIS API IS UNSTABLE RIGHT NOW.

package neutrino

import (
	"fmt"
	"sync"
	"time"

	"github.com/lightninglabs/neutrino/query"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/gcs"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/lightninglabs/neutrino/cache"
	"github.com/lightninglabs/neutrino/filterdb"
	"github.com/lightninglabs/neutrino/pushtx"
)

var (
	// QueryTimeout specifies how long to wait for a peer to answer a
	// query.
	QueryTimeout = time.Second * 10

	// QueryBatchTimeout is the total time we'll wait for a batch fetch
	// query to complete.
	// TODO(halseth): instead use timeout since last received response?
	QueryBatchTimeout = time.Second * 30

	// QueryPeerCooldown is the time we'll wait before re-assigning a query
	// to a peer that previously failed because of a timeout.
	QueryPeerCooldown = time.Second * 5

	// QueryNumRetries specifies how many times to retry sending a query to
	// each peer before we've concluded we aren't going to get a valid
	// response. This allows to make up for missed messages in some
	// instances.
	QueryNumRetries = 2

	// QueryPeerConnectTimeout specifies how long to wait for the
	// underlying chain service to connect to a peer before giving up
	// on a query in case we don't have any peers.
	QueryPeerConnectTimeout = time.Second * 30

	// QueryEncoding specifies the default encoding (witness or not) for
	// `getdata` and other similar messages.
	QueryEncoding = wire.WitnessEncoding

	// ErrFilterFetchFailed is returned in case fetching a compact filter
	// fails.
	ErrFilterFetchFailed = fmt.Errorf("unable to fetch cfilter")
)

// queries are a set of options that can be modified per-query, unlike global
// options.
//
// TODO: Make more query options that override global options.
type queryOptions struct {
	// maxBatchSize is the maximum items that the query should return in the
	// case the optimisticBatch option is used. It saves bandwidth in the case
	// the caller has a limited amount of items to fetch but still wants to use
	// batching.
	maxBatchSize int64

	// timeout lets the query know how long to wait for a peer to answer
	// the query before moving onto the next peer.
	timeout time.Duration

	// peerConnectTimeout lets the query know how long to wait for the
	// underlying chain service to connect to a peer before giving up
	// on a query in case we don't have any peers.
	peerConnectTimeout time.Duration

	// doneChan lets the query signal the caller when it's done, in case
	// it's run in a goroutine.
	doneChan chan<- struct{}

	// encoding lets the query know which encoding to use when queueing
	// messages to a peer.
	encoding wire.MessageEncoding

	// numRetries tells the query how many times to retry asking each peer
	// the query.
	numRetries uint8

	// optimisticBatch indicates whether we expect more calls to follow,
	// and that we should attempt to batch more items with the query such
	// that they can be cached, avoiding the extra round trip.
	optimisticBatch optimisticBatchType
}

// optimisticBatchType is a type indicating the kind of batching we want to
// execute with a query.
type optimisticBatchType uint8

const (
	// noBatch indicates no other than the specified item should be
	// queried.
	noBatch optimisticBatchType = iota

	// forwardBatch is used to indicate we should also query for items
	// following, as they most likely will be fetched next.
	forwardBatch

	// reverseBatch is used to indicate we should also query for items
	// preceding, as they most likely will be fetched next.
	reverseBatch
)

// QueryOption is a functional option argument to any of the network query
// methods, such as GetBlock and GetCFilter (when that resorts to a network
// query). These are always processed in order, with later options overriding
// earlier ones.
type QueryOption func(*queryOptions)

// defaultQueryOptions returns a queryOptions set to package-level defaults.
func defaultQueryOptions() *queryOptions {
	return &queryOptions{
		timeout:            QueryTimeout,
		numRetries:         uint8(QueryNumRetries),
		peerConnectTimeout: QueryPeerConnectTimeout,
		encoding:           QueryEncoding,
		optimisticBatch:    noBatch,
	}
}

// applyQueryOptions updates a queryOptions set with functional options.
func (qo *queryOptions) applyQueryOptions(options ...QueryOption) {
	for _, option := range options {
		option(qo)
	}
}

// Timeout is a query option that lets the query know how long to wait for each
// peer we ask the query to answer it before moving on.
func Timeout(timeout time.Duration) QueryOption {
	return func(qo *queryOptions) {
		qo.timeout = timeout
	}
}

// NumRetries is a query option that lets the query know the maximum number of
// times each peer should be queried. The default is one.
func NumRetries(numRetries uint8) QueryOption {
	return func(qo *queryOptions) {
		qo.numRetries = numRetries
	}
}

// PeerConnectTimeout is a query option that lets the query know how long to
// wait for the underlying chain service to connect to a peer before giving up
// on a query in case we don't have any peers.
func PeerConnectTimeout(timeout time.Duration) QueryOption {
	return func(qo *queryOptions) {
		qo.peerConnectTimeout = timeout
	}
}

// Encoding is a query option that allows the caller to set a message encoding
// for the query messages.
func Encoding(encoding wire.MessageEncoding) QueryOption {
	return func(qo *queryOptions) {
		qo.encoding = encoding
	}
}

// DoneChan allows the caller to pass a channel that will get closed when the
// query is finished.
func DoneChan(doneChan chan<- struct{}) QueryOption {
	return func(qo *queryOptions) {
		qo.doneChan = doneChan
	}
}

// OptimisticBatch allows the caller to tell that items following the requested
// one should be included in the query.
func OptimisticBatch() QueryOption {
	return func(qo *queryOptions) {
		qo.optimisticBatch = forwardBatch
	}
}

// OptimisticReverseBatch allows the caller to tell that items preceding the
// requested one should be included in the query.
func OptimisticReverseBatch() QueryOption {
	return func(qo *queryOptions) {
		qo.optimisticBatch = reverseBatch
	}
}

// MaxBatchSize allows the caller to limit the number of items fetched
// in a batch.
func MaxBatchSize(maxSize int64) QueryOption {
	return func(qo *queryOptions) {
		qo.maxBatchSize = maxSize
	}
}

// We provide 3 kinds of queries:
//
// * queryAllPeers allows a single query to be broadcast to all peers, and
//   then waits for as many peers as possible to answer that query within
//   a timeout. This allows for doing things like checking cfilter checkpoints.
//
// * queryPeers allows a single query to be passed to one peer at a time until
//   the query is deemed answered. This is good for getting a single piece of
//   data, such as a filter or a block.
//
// * queryBatch allows a batch of queries to be distributed among all peers,
//   recirculating upon timeout.
//
// TODO(aakselrod): maybe abstract the query scheduler into a functional option
// and provide some presets (including the ones below) prior to factoring out
// the query API into its own package?

// queryAllPeers is a helper function that sends a query to all peers and waits
// for a timeout specified by the QueryTimeout package-level variable or the
// Timeout functional option. The NumRetries option is set to 1 by default
// unless overridden by the caller.
func (s *ChainService) queryAllPeers(
	// queryMsg is the message to broadcast to all peers.
	queryMsg wire.Message,

	// checkResponse is called for every message within the timeout period.
	// The quit channel lets the query know to terminate because the
	// required response has been found. This is done by closing the
	// channel. The peerQuit lets the query know to terminate the query for
	// the peer which sent the response, allowing releasing resources for
	// peers which respond quickly while continuing to wait for slower
	// peers to respond and nonresponsive peers to time out.
	checkResponse func(sp *ServerPeer, resp wire.Message,
		quit chan<- struct{}, peerQuit chan<- struct{}),

	// options takes functional options for executing the query.
	options ...QueryOption) {

	// Starting with the set of default options, we'll apply any specified
	// functional options to the query.
	qo := defaultQueryOptions()
	qo.numRetries = 1
	qo.applyQueryOptions(options...)

	// This is done in a single-threaded query because the peerState is
	// held in a single thread. This is the only part of the query
	// framework that requires access to peerState, so it's done once per
	// query.
	peers := s.Peers()

	// This will be shared state between the per-peer goroutines.
	queryQuit := make(chan struct{})
	allQuit := make(chan struct{})
	var wg sync.WaitGroup
	msgChan := make(chan spMsg)
	subscription := spMsgSubscription{
		msgChan:  msgChan,
		quitChan: allQuit,
	}

	// Now we start a goroutine for each peer which manages the peer's
	// message subscription.
	peerQuits := make(map[string]chan struct{})
	for _, sp := range peers {
		sp.subscribeRecvMsg(subscription)
		wg.Add(1)
		peerQuits[sp.Addr()] = make(chan struct{})
		go func(sp *ServerPeer, peerQuit <-chan struct{}) {
			defer wg.Done()

			defer sp.unsubscribeRecvMsgs(subscription)

			for i := uint8(0); i < qo.numRetries; i++ {
				timeout := time.After(qo.timeout)
				sp.QueueMessageWithEncoding(queryMsg,
					nil, qo.encoding)
				select {
				case <-queryQuit:
					return
				case <-s.quit:
					return
				case <-peerQuit:
					return
				case <-timeout:
				}
			}
		}(sp, peerQuits[sp.Addr()])
	}

	// This goroutine will wait until all of the peer-query goroutines have
	// terminated, and then initiate a query shutdown.
	go func() {
		wg.Wait()

		// Make sure our main goroutine and the subscription know to
		// quit.
		close(allQuit)

		// Close the done channel, if any.
		if qo.doneChan != nil {
			close(qo.doneChan)
		}
	}()

	// Loop for any messages sent to us via our subscription channel and
	// check them for whether they satisfy the query. Break the loop when
	// allQuit is closed.
checkResponses:
	for {
		select {
		case <-queryQuit:
			break checkResponses

		case <-s.quit:
			break checkResponses

		case <-allQuit:
			break checkResponses

		// A message has arrived over the subscription channel, so we
		// execute the checkResponses callback to see if this ends our
		// query session.
		case sm := <-msgChan:
			// TODO: This will get stuck if checkResponse gets
			// stuck. This is a caveat for callers that should be
			// fixed before exposing this function for public use.
			select {
			case <-peerQuits[sm.sp.Addr()]:
			default:
				checkResponse(sm.sp, sm.msg, queryQuit,
					peerQuits[sm.sp.Addr()])
			}
		}
	}
}

// getFilterFromCache returns a filter from ChainService's FilterCache if it
// exists, returning nil and error if it doesn't.
func (s *ChainService) getFilterFromCache(blockHash *chainhash.Hash,
	filterType filterdb.FilterType) (*gcs.Filter, error) {

	cacheKey := cache.FilterCacheKey{
		BlockHash:  *blockHash,
		FilterType: filterType,
	}

	filterValue, err := s.FilterCache.Get(cacheKey)
	if err != nil {
		return nil, err
	}

	return filterValue.(*cache.CacheableFilter).Filter, nil
}

// putFilterToCache inserts a given filter in ChainService's FilterCache.
func (s *ChainService) putFilterToCache(blockHash *chainhash.Hash,
	filterType filterdb.FilterType, filter *gcs.Filter) (bool, error) { // nolint:unparam

	cacheKey := cache.FilterCacheKey{
		BlockHash:  *blockHash,
		FilterType: filterType,
	}
	return s.FilterCache.Put(cacheKey, &cache.CacheableFilter{Filter: filter})
}

// cfiltersQuery is a struct that holds all the information necessary to
// perform batch GetCFilters request, and handle the responses.
type cfiltersQuery struct {
	filterType       wire.FilterType
	startHeight      int64
	stopHeight       int64
	stopHash         *chainhash.Hash
	msg              wire.Message
	filterHeaders    []chainhash.Hash
	headerIndex      map[chainhash.Hash]int
	filterChan       chan *filterResponse
	targetHash       chainhash.Hash
	targetFilterChan chan *gcs.Filter
}

// request couples a query message with the handler to be used for the response
// in a query.Request struct.
func (q *cfiltersQuery) request() *query.Request {
	return &query.Request{
		Req:        q.msg,
		HandleResp: q.handleResponse,
	}
}

// handleResponse validates that the cfilter response we get from a peer is
// sane given the getcfilter query that we made.
func (q *cfiltersQuery) handleResponse(req, resp wire.Message,
	_ string) query.Progress {

	// The request must have been a "getcfilters" msg.
	request, ok := req.(*wire.MsgGetCFilters)
	if !ok {
		return query.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	// We're only interested in "cfilter" messages.
	response, ok := resp.(*wire.MsgCFilter)
	if !ok {
		return query.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	// If the request filter type doesn't match the type we were expecting,
	// ignore this message.
	if q.filterType != request.FilterType {
		return query.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	// If the response filter type doesn't match what we were expecting,
	// ignore this message.
	if q.filterType != response.FilterType {
		return query.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	// If this filter is for a block not in our index, we can ignore it, as
	// we either already got it, or it is out of our queried range.
	i, ok := q.headerIndex[response.BlockHash]
	if !ok {
		return query.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	filter, err := gcs.FromNBytes(
		builder.DefaultP, builder.DefaultM, response.Data,
	)
	if err != nil {
		// Malformed filter data. We can ignore this message.
		return query.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	// Now that we have a proper filter, ensure that re-calculating the
	// filter header hash for the header _after_ the filter in the chain
	// checks out. If not, we can ignore this response.
	curHeader := q.filterHeaders[i]
	prevHeader := q.filterHeaders[i-1]
	filterHeader, err := builder.MakeHeaderForFilter(filter, prevHeader)
	if err != nil {
		return query.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	if filterHeader != curHeader {
		return query.Progress{
			Finished:   false,
			Progressed: false,
		}
	}

	// At this point the filter matches what we know about it, and we
	// declare it sane. We send it into a channel to be processed elsewhere.
	q.filterChan <- &filterResponse{
		blockHash: &response.BlockHash,
		filter:    filter,
	}

	if response.BlockHash == q.targetHash {
		q.targetFilterChan <- filter
	}

	// We delete the entry for this filter from the headerIndex to indicate
	// that we have received it.
	delete(q.headerIndex, response.BlockHash)

	// If there are still entries left in the headerIndex then the query
	// has made progress but has not yet completed.
	if len(q.headerIndex) != 0 {
		return query.Progress{
			Finished:   false,
			Progressed: true,
		}
	}

	// The headerIndex is empty and so this query is complete.
	close(q.filterChan)
	close(q.targetFilterChan)
	return query.Progress{
		Finished:   true,
		Progressed: true,
	}
}

// filterResponse links a filter with its associate block hash.
type filterResponse struct {
	blockHash *chainhash.Hash
	filter    *gcs.Filter
}

// prepareCFiltersQuery creates a cfiltersQuery that can be used to fetch a
// CFilter fo the given block hash.
func (s *ChainService) prepareCFiltersQuery(blockHash chainhash.Hash,
	filterType wire.FilterType, optimisticBatch optimisticBatchType,
	maxBatchSize int64) (*cfiltersQuery, error) {

	_, height, err := s.BlockHeaders.FetchHeader(&blockHash)
	if err != nil {
		return nil, fmt.Errorf("unable to get header for start "+
			"block=%v: %v", blockHash, err)
	}

	bestBlock, err := s.BestBlock()
	if err != nil {
		return nil, fmt.Errorf("unable to get best block: %v", err)
	}
	bestHeight := int64(bestBlock.Height)

	// If the query specifies an optimistic batch we will attempt to fetch
	// the maximum number of filters, which is defaulted to
	// wire.MaxGetCFiltersReqRange, in anticipation of calls for the following
	// or preceding filters.
	var startHeight, stopHeight int64
	batchSize := int64(wire.MaxGetCFiltersReqRange)

	// If the query specifies a maximum batch size, we will limit the number of
	// requested filters accordingly.
	if maxBatchSize > 0 && maxBatchSize < wire.MaxGetCFiltersReqRange {
		batchSize = maxBatchSize
	}

	switch optimisticBatch {

	// No batching, the start and stop height will be the same.
	case noBatch:
		startHeight = int64(height)
		stopHeight = int64(height)

	// Forward batch, fetch as many of the following filters as possible.
	case forwardBatch:
		startHeight = int64(height)
		stopHeight = startHeight + batchSize - 1

	// Reverse batch, fetch as many of the preceding filters as possible.
	case reverseBatch:
		stopHeight = int64(height)
		startHeight = stopHeight - batchSize + 1
	}

	// Block 1 is the earliest one we can fetch.
	if startHeight < 1 {
		startHeight = 1
	}

	// If the stop height with the maximum batch size is above our best
	// known block, then we use the best block height instead.
	if stopHeight > bestHeight {
		stopHeight = bestHeight
	}

	stopHash, err := s.GetBlockHash(stopHeight)
	if err != nil {
		return nil, fmt.Errorf("unable to get hash for "+
			"stopHeight=%d: %v", stopHeight, err)
	}

	queryMsg := wire.NewMsgGetCFilters(
		filterType, uint32(startHeight), stopHash,
	)

	// In order to verify the authenticity of the received filters, we'll
	// fetch the block headers and filter headers in the range
	// [startHeight-1, stopHeight]. We go one below our startHeight since
	// the hash of the previous block is needed for validation.
	numFilters := uint32(stopHeight - startHeight + 1)
	blockHeaders, _, err := s.BlockHeaders.FetchHeaderAncestors(
		numFilters, stopHash,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get %d block header "+
			"ancestors for stopHash=%v: %v", numFilters,
			stopHash, err)
	}

	if len(blockHeaders) != int(numFilters)+1 {
		return nil, fmt.Errorf("expected %d block headers, got %d",
			numFilters+1, len(blockHeaders))
	}

	filterHeaders, _, err := s.RegFilterHeaders.FetchHeaderAncestors(
		numFilters, stopHash,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get %d filter header "+
			"ancestors for stopHash=%v: %v", numFilters, stopHash,
			err)
	}

	if len(filterHeaders) != int(numFilters)+1 {
		return nil, fmt.Errorf("expected %d filter headers, got %d",
			numFilters+1, len(filterHeaders))
	}

	// We create a header index such that we can easily index into our
	// header slices for a given block hash in the received response,
	// without consulting the database. This also keeps track of which
	// blocks we are still awaiting a response for. We start at index=1, as
	// 0 is for the block startHeight-1, which is only needed for
	// validation.
	headerIndex := make(map[chainhash.Hash]int, len(blockHeaders)-1)
	for i := 1; i < len(blockHeaders); i++ {
		block := blockHeaders[i]
		headerIndex[block.BlockHash()] = i
	}

	// We'll immediately respond to the caller with the requested filter
	// when it is received, so we make a channel to notify on when it's
	// ready.
	filterChan := make(chan *gcs.Filter, 1)

	return &cfiltersQuery{
		filterType:       filterType,
		startHeight:      startHeight,
		stopHeight:       stopHeight,
		stopHash:         stopHash,
		msg:              queryMsg,
		filterHeaders:    filterHeaders,
		targetHash:       blockHash,
		headerIndex:      headerIndex,
		filterChan:       make(chan *filterResponse, numFilters),
		targetFilterChan: filterChan,
	}, nil
}

// GetCFilter gets a cfilter from the database. Failing that, it requests the
// cfilter from the network and writes it to the database. If extended is true,
// an extended filter will be queried for. Otherwise, we'll fetch the regular
// filter.
func (s *ChainService) GetCFilter(blockHash chainhash.Hash,
	filterType wire.FilterType, options ...QueryOption) (*gcs.Filter, error) {

	// The only supported filter atm is the regular filter, so we'll reject
	// all other filters.
	if filterType != wire.GCSFilterRegular {
		return nil, fmt.Errorf("unknown filter type: %v", filterType)
	}

	// Based on if extended is true or not, we'll set up our set of
	// querying, and db-write functions.
	dbFilterType := filterdb.RegularFilter

	// First check the cache to see if we already have this filter. If
	// so, then we can return it an exit early.
	filter, err := s.getFilterFromCache(&blockHash, dbFilterType)
	if err == nil && filter != nil {
		return filter, nil
	}
	if err != nil && err != cache.ErrElementNotFound {
		return nil, err
	}

	// If not in cache, check if it's in database, returning early if yes.
	filter, err = s.FilterDB.FetchFilter(&blockHash, dbFilterType)
	if err == nil && filter != nil {
		return filter, nil
	}
	if err != nil && err != filterdb.ErrFilterNotFound {
		return nil, err
	}

	// We acquire the mutex ensuring we don't have several redundant
	// CFilter queries running in parallel.
	s.mtxCFilter.Lock()

	// Since another request might have added the filter to the cache while
	// we were waiting for the mutex, we do a final lookup before starting
	// our own query.
	filter, err = s.getFilterFromCache(&blockHash, dbFilterType)
	if err == nil && filter != nil {
		s.mtxCFilter.Unlock()
		return filter, nil
	}
	if err != nil && err != cache.ErrElementNotFound {
		s.mtxCFilter.Unlock()
		return nil, err
	}

	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)

	// We didn't get the filter from the DB, so we'll try to get it from
	// the network.
	q, err := s.prepareCFiltersQuery(
		blockHash, filterType, qo.optimisticBatch, qo.maxBatchSize,
	)
	if err != nil {
		s.mtxCFilter.Unlock()
		return nil, err
	}

	// With all the necessary items retrieved, we'll launch our concurrent
	// query to the set of connected peers.
	log.Debugf("Fetching filters for heights=[%v, %v], stophash=%v",
		q.startHeight, q.stopHeight, q.stopHash)

	persistChan := make(chan *filterResponse, len(q.headerIndex))
	go func() {
		defer close(persistChan)
		defer s.mtxCFilter.Unlock()

		// Hand the query to the work manager, and consume the verified
		// responses as they come back.
		errChan := s.queryDispatcher.Query(
			[]*query.Request{q.request()}, query.Cancel(s.quit),
			query.Encoding(qo.encoding),
		)

		var (
			resp *filterResponse
			ok   bool
		)

		for {
			select {
			case resp, ok = <-q.filterChan:
				if !ok {
					// If filterChan is closed then the
					// query has finished successfully.
					return
				}

			case err := <-errChan:
				switch {
				case err == query.ErrWorkManagerShuttingDown:
					return

				case err != nil:
					log.Errorf("Query finished with "+
						"error before all responses "+
						"received: %v", err)
					return
				}

				// The query did finish successfully, but
				// continue to allow picking up the last filter
				// sent on the filterChan.
				continue

			case <-s.quit:
				return
			}

			// Put the filter in the cache and persistToDisk if the
			// caller requested it.
			// TODO(halseth): for an LRU we could take care to
			//  insert the next height filter last.
			dbFilterType := filterdb.RegularFilter
			evict, err := s.putFilterToCache(
				resp.blockHash, dbFilterType, resp.filter,
			)
			if err != nil {
				log.Warnf("Couldn't write filter to cache: %v",
					err)
			}

			// TODO(halseth): dynamically increase/decrease the
			//  batch size to match our cache capacity.
			numFilters := q.stopHeight - q.startHeight + 1
			if evict && s.FilterCache.Len() < int(numFilters) {
				log.Debugf("Items evicted from the cache "+
					"with less than %d elements. Consider "+
					"increasing the cache size...",
					numFilters)
			}

			persistChan <- resp
		}
	}()

	if s.persistToDisk {
		// Persisting to disk is the bottleneck for fetching filters.
		// So we run the persisting logic in a separate goroutine so
		// that we can unlock the mtxCFilter mutex as soon as we are
		// done with caching the filters in order to allow more
		// GetCFilter calls from the caller sooner.
		go func() {
			var (
				resp *filterResponse
				ok   bool
			)

			for {
				select {
				case resp, ok = <-persistChan:
					if !ok {
						return
					}

				case <-s.quit:
					return
				}

				err = s.FilterDB.PutFilter(
					resp.blockHash, resp.filter,
					dbFilterType,
				)
				if err != nil {
					log.Warnf("Couldn't write filter to "+
						"filterDB: %v", err)
				}

				log.Tracef("Wrote filter for block %s, type %d",
					resp.blockHash, dbFilterType)
			}
		}()
	}

	var ok bool
	var resultFilter *gcs.Filter

	// We will wait for the query to finish before we return the requested
	// filter to the caller.
	for {
		select {

		case filter, ok = <-q.targetFilterChan:
			if !ok {
				// Query has finished, if we have a result we'll
				// return it.
				if resultFilter == nil {
					return nil, ErrFilterFetchFailed
				}

				return resultFilter, nil
			}

			// We'll store the filter so we can return it later to
			// the caller.
			resultFilter = filter

		case <-s.quit:
			return nil, ErrShuttingDown
		}
	}
}

// GetBlock gets a block by requesting it from the network, one peer at a
// time, until one answers. If the block is found in the cache, it will be
// returned immediately.
func (s *ChainService) GetBlock(blockHash chainhash.Hash,
	options ...QueryOption) (*btcutil.Block, error) {

	// Fetch the corresponding block header from the database. If this
	// isn't found, then we don't have the header for this block so we
	// can't request it.
	blockHeader, height, err := s.BlockHeaders.FetchHeader(&blockHash)
	if err != nil || blockHeader.BlockHash() != blockHash {
		return nil, fmt.Errorf("couldn't get header for block %s "+
			"from database", blockHash)
	}

	// Starting with the set of default options, we'll apply any specified
	// functional options to the query so that we can check what inv type
	// to use.
	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)
	invType := wire.InvTypeWitnessBlock
	if qo.encoding == wire.BaseEncoding {
		invType = wire.InvTypeBlock
	}

	// Create an inv vector for getting this block.
	inv := wire.NewInvVect(invType, &blockHash)

	// If the block is already in the cache, we can return it immediately.
	blockValue, err := s.BlockCache.Get(*inv)
	if err == nil && blockValue != nil {
		return blockValue.(*cache.CacheableBlock).Block, err
	}
	if err != nil && err != cache.ErrElementNotFound {
		return nil, err
	}

	// Construct the appropriate getdata message to fetch the target block.
	getData := wire.NewMsgGetData()
	_ = getData.AddInvVect(inv)

	var foundBlock *btcutil.Block
	request := &query.Request{
		Req: getData,
		HandleResp: func(req, resp wire.Message, peer string) query.Progress {
			// The request must have been a "getdata" msg.
			_, ok := req.(*wire.MsgGetData)
			if !ok {
				return query.Progress{
					Finished:   false,
					Progressed: false,
				}
			}

			// We're only interested in "block" messages.
			response, ok := resp.(*wire.MsgBlock)
			if !ok {
				return query.Progress{
					Finished:   false,
					Progressed: false,
				}
			}

			// If this isn't the block we asked for, ignore it.
			if response.BlockHash() != blockHash {
				return query.Progress{
					Finished:   false,
					Progressed: false,
				}
			}

			block := btcutil.NewBlock(response)

			// Only set height if btcutil hasn't
			// automagically put one in.
			if block.Height() == btcutil.BlockHeightUnknown {
				block.SetHeight(int32(height))
			}

			// If this claims our block but doesn't pass
			// the sanity check, the peer is trying to
			// bamboozle us.
			if err := blockchain.CheckBlockSanity(
				block,
				// We don't need to check PoW because
				// by the time we get here, it's been
				// checked during header
				// synchronization
				s.chainParams.PowLimit,
				s.timeSource,
			); err != nil {
				log.Warnf("Invalid block for %s "+
					"received from %s -- ",
					blockHash, peer)
				fmt.Println(err)

				return query.Progress{
					Finished:   false,
					Progressed: false,
				}
			}

			// TODO(roasbeef): modify CheckBlockSanity to
			// also check witness commitment

			// At this point, the block matches what we
			// know about it and we declare it sane. We can
			// kill the query and pass the response back to
			// the caller.
			foundBlock = block
			return query.Progress{
				Finished:   true,
				Progressed: true,
			}
		},
	}

	errChan := s.queryDispatcher.Query(
		[]*query.Request{request}, query.Encoding(qo.encoding),
		query.Cancel(s.quit),
	)

	select {
	case err := <-errChan:
		if err != nil {
			return nil, err
		}
	case <-s.quit:
		return nil, ErrShuttingDown
	}

	if foundBlock == nil {
		return nil, fmt.Errorf("couldn't retrieve block %s from "+
			"network", blockHash)
	}

	// Add block to the cache before returning it.
	_, err = s.BlockCache.Put(*inv, &cache.CacheableBlock{Block: foundBlock})
	if err != nil {
		log.Warnf("couldn't write block to cache: %v", err)
	}

	return foundBlock, nil
}

// sendTransaction sends a transaction to all peers. It returns an error if any
// peer rejects the transaction.
//
// TODO: Better privacy by sending to only one random peer and watching
// propagation, requires better peer selection support in query API.
//
// TODO(wilmer): Move to pushtx package after introducing a query package. This
// cannot be done at the moment due to circular dependencies.
func (s *ChainService) sendTransaction(tx *wire.MsgTx, options ...QueryOption) error {
	// Starting with the set of default options, we'll apply any specified
	// functional options to the query so that we can check what inv type
	// to use. Broadcast the inv to all peers, responding to any getdata
	// messages for the transaction.
	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)
	invType := wire.InvTypeWitnessTx
	if qo.encoding == wire.BaseEncoding {
		invType = wire.InvTypeTx
	}

	// Create an inv.
	txHash := tx.TxHash()
	inv := wire.NewMsgInv()
	_ = inv.AddInvVect(wire.NewInvVect(invType, &txHash))

	// We'll gather all of the peers who replied to our query, along with
	// the ones who rejected it and their reason for rejecting it. We'll use
	// this to determine whether our transaction was actually rejected.
	numReplied := 0
	rejections := make(map[pushtx.BroadcastError]int)

	// Send the peer query and listen for getdata.
	s.queryAllPeers(
		inv,
		func(sp *ServerPeer, resp wire.Message, quit chan<- struct{},
			peerQuit chan<- struct{}) {

			switch response := resp.(type) {
			// A peer has replied with a GetData message, so we'll
			// send them the transaction.
			case *wire.MsgGetData:
				for _, vec := range response.InvList {
					if vec.Hash == txHash {
						sp.QueueMessageWithEncoding(
							tx, nil, qo.encoding,
						)

						numReplied++
					}
				}

			// A peer has rejected our transaction for whatever
			// reason. Rather than returning to the caller upon the
			// first rejection, we'll gather them all to determine
			// whether it is critical/fatal.
			case *wire.MsgReject:
				// Ensure this rejection is for the transaction
				// we're attempting to broadcast.
				if response.Hash != txHash {
					return
				}

				broadcastErr := pushtx.ParseBroadcastError(
					response, sp.Addr(),
				)
				rejections[*broadcastErr]++
			}
		},
		append(
			[]QueryOption{Timeout(s.broadcastTimeout)},
			options...,
		)...,
	)

	// If none of our peers replied to our query, we'll avoid returning an
	// error as the reliable broadcaster will take care of broadcasting this
	// transaction upon every block connected/disconnected.
	if numReplied == 0 {
		log.Debugf("No peers replied to inv message for transaction %v",
			tx.TxHash())
		return nil
	}

	// If all of our peers who replied to our query also rejected our
	// transaction, we'll deem that there was actually something wrong with
	// it so we'll return the most rejected error between all of our peers.
	//
	// TODO(wilmer): This might be too naive, some rejections are more
	// critical than others.
	//
	// TODO(wilmer): This does not cover the case where a peer also rejected
	// our transaction but didn't send the response within our given timeout
	// and certain other cases. Due to this, we should probably decide on a
	// threshold of rejections instead.
	if numReplied == len(rejections) {
		log.Warnf("All peers rejected transaction %v checking errors",
			tx.TxHash())

		mostRejectedCount := 0
		var mostRejectedErr pushtx.BroadcastError

		for broadcastErr, count := range rejections {
			if count > mostRejectedCount {
				mostRejectedCount = count
				mostRejectedErr = broadcastErr
			}
		}

		return &mostRejectedErr
	}

	return nil
}
