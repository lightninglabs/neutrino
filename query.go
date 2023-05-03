// NOTE: THIS API IS UNSTABLE RIGHT NOW.

package neutrino

import (
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/gcs"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/cache"
	"github.com/lightninglabs/neutrino/filterdb"
	"github.com/lightninglabs/neutrino/pushtx"
	"github.com/lightninglabs/neutrino/query"
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

	// QueryRejectTimeout is the time we'll wait after sending a response to
	// an INV query for a potential reject answer. If we don't get a reject
	// before this delay, we assume the TX was accepted.
	QueryRejectTimeout = time.Second

	// QueryInvalidTxThreshold is the threshold for the fraction of peers
	// that need to respond to a TX with a code of pushtx.Invalid to count
	// it as invalid, even if not all peers respond. This currently
	// corresponds to 60% of peers that need to reject.
	QueryInvalidTxThreshold float32 = 0.6

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

	// noProgress will be used to indicate to a query.WorkManager that a
	// response makes no progress towards the completion of the query.
	noProgress = query.Progress{
		Finished:   false,
		Progressed: false,
	}
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

	// rejectTimeout is the time we'll wait after sending a response to an
	// INV query for a potential reject answer. If we don't get a reject
	// before this delay, we assume the TX was accepted. This option is only
	// used when publishing a transaction.
	rejectTimeout time.Duration

	// doneChan lets the query signal the caller when it's done, in case
	// it's run in a goroutine.
	doneChan chan<- struct{}

	// encoding lets the query know which encoding to use when queueing
	// messages to a peer.
	encoding wire.MessageEncoding

	// numRetries tells the query how many times to retry asking each peer
	// the query.
	numRetries uint8

	// invalidTxThreshold is the threshold for the fraction of peers
	// that need to respond to a TX with a code of pushtx.Invalid to count
	// it as invalid, even if not all peers respond. This option is only
	// used when publishing a transaction.
	invalidTxThreshold float32

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
		rejectTimeout:      QueryRejectTimeout,
		encoding:           QueryEncoding,
		invalidTxThreshold: QueryInvalidTxThreshold,
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

// InvalidTxThreshold is the threshold for the fraction of peers that need to
// respond to a TX with a code of pushtx.Invalid to count it as invalid, even
// if not all peers respond.
//
// NOTE: This option is currently only used when publishing a transaction.
func InvalidTxThreshold(invalidTxThreshold float32) QueryOption {
	return func(qo *queryOptions) {
		qo.invalidTxThreshold = invalidTxThreshold
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

// RejectTimeout is the time we'll wait after sending a response to an INV
// query for a potential reject answer. If we don't get a reject before this
// delay, we assume the TX was accepted.
//
// NOTE: This option is currently only used when publishing a transaction.
func RejectTimeout(rejectTimeout time.Duration) QueryOption {
	return func(qo *queryOptions) {
		qo.rejectTimeout = rejectTimeout
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

// queryChainServicePeers is a helper function that sends a query to one or
// more peers of the given ChainService, and waits for an answer. The timeout
// for queries is set by the QueryTimeout package-level variable or the Timeout
// functional option.
func queryChainServicePeers(
	// s is the ChainService to use.
	s *ChainService,

	// queryMsg is the message to send to each peer selected by selectPeer.
	queryMsg wire.Message,

	// checkResponse is called for every message within the timeout period.
	// The quit channel lets the query know to terminate because the
	// required response has been found. This is done by closing the
	// channel.
	checkResponse func(sp *ServerPeer, resp wire.Message,
		quit chan<- struct{}),

	// options takes functional options for executing the query.
	options ...QueryOption) {

	// Starting with the set of default options, we'll apply any specified
	// functional options to the query.
	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)

	// We get an initial view of our peers, to be updated each time a peer
	// query times out.
	queryPeer := s.blockManager.SyncPeer()
	peerTries := make(map[string]uint8)

	// This will be state used by the peer query goroutine.
	queryQuit := make(chan struct{})
	subQuit := make(chan struct{})

	// Increase this number to be able to handle more queries at once as
	// each channel gets results for all queries, otherwise messages can
	// get mixed and there's a vicious cycle of retries causing a bigger
	// message flood, more of which get missed.
	msgChan := make(chan spMsg)
	subscription := spMsgSubscription{
		msgChan:  msgChan,
		quitChan: subQuit,
	}

	// Loop for any messages sent to us via our subscription channel and
	// check them for whether they satisfy the query. Break the loop if
	// it's time to quit.
	peerTimeout := time.NewTimer(qo.timeout)
	connectionTimeout := time.NewTimer(qo.peerConnectTimeout)
	connectionTicker := connectionTimeout.C
	if queryPeer != nil {
		peerTries[queryPeer.Addr()]++
		queryPeer.subscribeRecvMsg(subscription)
		queryPeer.QueueMessageWithEncoding(queryMsg, nil, qo.encoding)
	}
checkResponses:
	for {
		select {
		case <-connectionTicker:
			// When we time out, we're done.
			if queryPeer != nil {
				queryPeer.unsubscribeRecvMsgs(subscription)
			}
			break checkResponses

		case <-queryQuit:
			// Same when we get a quit signal.
			if queryPeer != nil {
				queryPeer.unsubscribeRecvMsgs(subscription)
			}
			break checkResponses

		case <-s.quit:
			// Same when chain server's quit is signaled.
			if queryPeer != nil {
				queryPeer.unsubscribeRecvMsgs(subscription)
			}
			break checkResponses

		// A message has arrived over the subscription channel, so we
		// execute the checkResponses callback to see if this ends our
		// query session.
		case sm := <-msgChan:
			// TODO: This will get stuck if checkResponse gets
			// stuck. This is a caveat for callers that should be
			// fixed before exposing this function for public use.
			checkResponse(sm.sp, sm.msg, queryQuit)

			// Each time we receive a response from the current
			// peer, we'll reset the main peer timeout as they're
			// being responsive.
			if !peerTimeout.Stop() {
				select {
				case <-peerTimeout.C:
				default:
				}
			}
			peerTimeout.Reset(qo.timeout)

			// Also at this point, if the peerConnectTimeout is
			// still active, then we can disable it, as we're
			// receiving responses from the current peer.
			if connectionTicker != nil && !connectionTimeout.Stop() {
				select {
				case <-connectionTimeout.C:
				default:
				}
			}
			connectionTicker = nil

		// The current peer we're querying has failed to answer the
		// query. Time to select a new peer and query it.
		case <-peerTimeout.C:
			if queryPeer != nil {
				queryPeer.unsubscribeRecvMsgs(subscription)
			}

			queryPeer = nil
			for _, peer := range s.Peers() {
				// If the peer is no longer connected, we'll
				// skip them.
				if !peer.Connected() {
					continue
				}

				// If we've yet to try this peer, we'll make
				// sure to do so. If we've exceeded the number
				// of tries we should retry this peer, then
				// we'll skip them.
				numTries, ok := peerTries[peer.Addr()]
				if ok && numTries >= qo.numRetries {
					continue
				}

				queryPeer = peer

				// Found a peer we can query.
				peerTries[queryPeer.Addr()]++
				queryPeer.subscribeRecvMsg(subscription)
				queryPeer.QueueMessageWithEncoding(
					queryMsg, nil, qo.encoding,
				)
				break
			}

			// If at this point, we don't yet have a query peer,
			// then we'll exit now as all the peers are exhausted.
			if queryPeer == nil {
				break checkResponses
			}
		}
	}

	// Close the subscription quit channel and the done channel, if any.
	close(subQuit)
	peerTimeout.Stop()
	if qo.doneChan != nil {
		close(qo.doneChan)
	}
}

// getFilterFromCache returns a filter from ChainService's FilterCache if it
// exists, returning nil and error if it doesn't.
func (s *ChainService) getFilterFromCache(blockHash *chainhash.Hash,
	filterType filterdb.FilterType) (*gcs.Filter, error) {

	cacheKey := FilterCacheKey{
		BlockHash:  *blockHash,
		FilterType: filterType,
	}

	filterValue, err := s.FilterCache.Get(cacheKey)
	if err != nil {
		return nil, err
	}

	return filterValue.Filter, nil
}

// putFilterToCache inserts a given filter in ChainService's FilterCache.
func (s *ChainService) putFilterToCache(blockHash *chainhash.Hash,
	filterType filterdb.FilterType, filter *gcs.Filter) (bool, error) { // nolint:unparam

	cacheKey := FilterCacheKey{
		BlockHash:  *blockHash,
		FilterType: filterType,
	}
	return s.FilterCache.Put(cacheKey, &CacheableFilter{Filter: filter})
}

// cfiltersQuery is a struct that holds all the information necessary to
// perform batch GetCFilters request, and handle the responses.
type cfiltersQuery struct {
	filterType    wire.FilterType
	startHeight   int64
	stopHeight    int64
	stopHash      *chainhash.Hash
	filterHeaders []chainhash.Hash
	headerIndex   map[chainhash.Hash]int
	targetHash    chainhash.Hash
	filterChan    chan *gcs.Filter
	options       []QueryOption
}

// queryMsg returns the wire message to perform this query.
func (q *cfiltersQuery) queryMsg() wire.Message {
	return wire.NewMsgGetCFilters(
		q.filterType, uint32(q.startHeight), q.stopHash,
	)
}

// prepareCFiltersQuery creates a cfiltersQuery that can be used to fetch a
// CFilter fo the given block hash.
func (s *ChainService) prepareCFiltersQuery(blockHash chainhash.Hash,
	filterType wire.FilterType, options ...QueryOption) (
	*cfiltersQuery, error) {

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

	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)

	// If the query specifies an optimistic batch we will attempt to fetch
	// the maximum number of filters, which is defaulted to
	// wire.MaxGetCFiltersReqRange, in anticipation of calls for the following
	// or preceding filters.
	var startHeight, stopHeight int64
	batchSize := int64(wire.MaxGetCFiltersReqRange)

	// If the query specifies a maximum batch size, we will limit the number of
	// requested filters accordingly.
	if qo.maxBatchSize > 0 && qo.maxBatchSize < wire.MaxGetCFiltersReqRange {
		batchSize = qo.maxBatchSize
	}

	switch qo.optimisticBatch {
	// No batching, the start and stop height will be the same.
	case noBatch:
		startHeight = int64(height)
		stopHeight = int64(height)

	// Forward batch, fetch as many of the following filters as possible.
	case forwardBatch:
		startHeight = int64(height)
		stopHeight = startHeight + batchSize - 1

		// We need a longer timeout, since we are going to receive more
		// than a single response.
		options = append(options, Timeout(QueryBatchTimeout))

	// Reverse batch, fetch as many of the preceding filters as possible.
	case reverseBatch:
		stopHeight = int64(height)
		startHeight = stopHeight - batchSize + 1

		// We need a longer timeout, since we are going to receive more
		// than a single response.
		options = append(options, Timeout(QueryBatchTimeout))
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
		filterType:    filterType,
		startHeight:   startHeight,
		stopHeight:    stopHeight,
		stopHash:      stopHash,
		filterHeaders: filterHeaders,
		headerIndex:   headerIndex,
		targetHash:    blockHash,
		filterChan:    filterChan,
		options:       options,
	}, nil
}

// handleCFiltersRespons is called every time we receive a response for the
// GetCFilters request.
func (s *ChainService) handleCFiltersResponse(q *cfiltersQuery,
	resp wire.Message, quit chan<- struct{}) {

	// We're only interested in "cfilter" messages.
	response, ok := resp.(*wire.MsgCFilter)
	if !ok {
		return
	}

	// If the response doesn't match our request, ignore this message.
	if q.filterType != response.FilterType {
		return
	}

	// If this filter is for a block not in our index, we can ignore it, as
	// we either already got it, or it is out of our queried range.
	i, ok := q.headerIndex[response.BlockHash]
	if !ok {
		return
	}

	gotFilter, err := gcs.FromNBytes(
		builder.DefaultP, builder.DefaultM, response.Data,
	)
	if err != nil {
		// Malformed filter data. We can ignore this message.
		return
	}

	// Now that we have a proper filter, ensure that re-calculating the
	// filter header hash for the header _after_ the filter in the chain
	// checks out. If not, we can ignore this response.
	curHeader := q.filterHeaders[i]
	prevHeader := q.filterHeaders[i-1]
	gotHeader, err := builder.MakeHeaderForFilter(
		gotFilter, prevHeader,
	)
	if err != nil {
		return
	}

	if gotHeader != curHeader {
		return
	}

	// At this point, the filter matches what we know about it and we
	// declare it sane. If this is the filter requested initially, send it
	// to the caller immediately.
	if response.BlockHash == q.targetHash {
		q.filterChan <- gotFilter
	}

	// Put the filter in the cache and persistToDisk if the caller
	// requested it.
	// TODO(halseth): for an LRU we could take care to insert the next
	// height filter last.
	dbFilterType := filterdb.RegularFilter
	evict, err := s.putFilterToCache(
		&response.BlockHash, dbFilterType, gotFilter,
	)
	if err != nil {
		log.Warnf("Couldn't write filter to cache: %v", err)
	}

	// TODO(halseth): dynamically increase/decrease the batch size to match
	// our cache capacity.
	numFilters := q.stopHeight - q.startHeight + 1
	if evict && s.FilterCache.Len() < int(numFilters) {
		log.Debugf("Items evicted from the cache with less "+
			"than %d elements. Consider increasing the "+
			"cache size...", numFilters)
	}

	qo := defaultQueryOptions()
	qo.applyQueryOptions(q.options...)
	if s.persistToDisk {
		err = s.FilterDB.PutFilter(
			&response.BlockHash, gotFilter, dbFilterType,
		)
		if err != nil {
			log.Warnf("Couldn't write filter to filterDB: "+
				"%v", err)
		}

		log.Tracef("Wrote filter for block %s, type %d",
			&response.BlockHash, dbFilterType)
	}

	// Finally, we can delete it from the headerIndex.
	delete(q.headerIndex, response.BlockHash)

	// If the headerIndex is empty, we got everything we wanted, and can
	// exit.
	if len(q.headerIndex) == 0 {
		close(quit)
	}
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

	// We didn't get the filter from the DB, so we'll try to get it from
	// the network.
	query, err := s.prepareCFiltersQuery(blockHash, filterType, options...)
	if err != nil {
		s.mtxCFilter.Unlock()
		return nil, err
	}

	// With all the necessary items retrieved, we'll launch our concurrent
	// query to the set of connected peers.
	log.Debugf("Fetching filters for heights=[%v, %v], stophash=%v",
		query.startHeight, query.stopHeight, query.stopHash)

	go func() {
		defer s.mtxCFilter.Unlock()
		defer close(query.filterChan)

		s.queryPeers(
			// Send a wire.MsgGetCFilters.
			query.queryMsg(),

			// Check responses and if we get one that matches, end
			// the query early.
			func(_ *ServerPeer, resp wire.Message, quit chan<- struct{}) {
				s.handleCFiltersResponse(query, resp, quit)
			},
			query.options...,
		)

		// If there are elements left to receive, the query failed.
		if len(query.headerIndex) > 0 {
			numFilters := query.stopHeight - query.startHeight + 1
			numRecv := numFilters - int64(len(query.headerIndex))
			log.Errorf("Query failed with %d out of %d filters "+
				"received", numRecv, numFilters)
			return
		}
	}()

	var ok bool
	var resultFilter *gcs.Filter

	// We will wait for the query to finish before we return the requested
	// filter to the caller.
	for {
		select {
		case filter, ok = <-query.filterChan:
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
		return blockValue.Block, err
	}
	if err != nil && err != cache.ErrElementNotFound {
		return nil, err
	}

	// Construct the appropriate getdata message to fetch the target block.
	getData := wire.NewMsgGetData()
	_ = getData.AddInvVect(inv)

	var foundBlock *btcutil.Block

	// handleResp will be called for each message received from a peer. It
	// will be used to signal to the work manager whether progress has been
	// made or not.
	handleResp := func(req, resp wire.Message, peer string) query.Progress {
		// The request must have been a "getdata" msg.
		_, ok := req.(*wire.MsgGetData)
		if !ok {
			return noProgress
		}

		// We're only interested in "block" responses.
		response, ok := resp.(*wire.MsgBlock)
		if !ok {
			return noProgress
		}

		// If this isn't the block we asked for, ignore it.
		if response.BlockHash() != blockHash {
			return noProgress
		}
		block := btcutil.NewBlock(response)

		// Only set height if btcutil hasn't automagically put one in.
		if block.Height() == btcutil.BlockHeightUnknown {
			block.SetHeight(int32(height))
		}

		// If this claims our block but doesn't pass the sanity check,
		// the peer is trying to bamboozle us.
		if err := blockchain.CheckBlockSanity(
			block,
			// We don't need to check PoW because by the time we get
			// here, it's been checked during header synchronization
			s.chainParams.PowLimit,
			s.timeSource,
		); err != nil {
			log.Warnf("Invalid block for %s received from %s: %v",
				blockHash, peer, err)

			// Ban and disconnect the peer.
			err = s.BanPeer(peer, banman.InvalidBlock)
			if err != nil {
				log.Errorf("Unable to ban peer %v: %v", peer,
					err)
			}

			return noProgress
		}

		// TODO(roasbeef): modify CheckBlockSanity to also check witness
		// commitment

		// At this point, the block matches what we know about it, and
		// we declare it sane. We can kill the query and pass the
		// response back to the caller.
		foundBlock = block
		return query.Progress{
			Finished:   true,
			Progressed: true,
		}
	}

	// Prepare the query request.
	request := &query.Request{
		Req:        getData,
		HandleResp: handleResp,
	}

	// Prepare the query options.
	queryOpts := []query.QueryOption{
		query.Encoding(qo.encoding),
		query.NumRetries(qo.numRetries),
		query.Cancel(s.quit),
	}

	// Send the request to the work manager and await a response.
	errChan := s.workManager.Query([]*query.Request{request}, queryOpts...)
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
	_, err = s.BlockCache.Put(*inv, &CacheableBlock{Block: foundBlock})
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

	// We'll gather all the peers who replied to our query, along with
	// the ones who rejected it and their reason for rejecting it. We'll use
	// this to determine whether our transaction was actually rejected.
	replies := make(map[int32]struct{})
	rejections := make(map[int32]*pushtx.BroadcastError)
	rejectCodes := make(map[pushtx.BroadcastErrorCode]int)

	// closers is a map that tracks the delayed closers we need to make sure
	// the peer quit channel is closed after a timeout.
	closers := make(map[int32]*delayedCloser)

	// Send the peer query and listen for getdata.
	s.queryAllPeers(
		inv,
		func(sp *ServerPeer, resp wire.Message, quit chan<- struct{},
			peerQuit chan<- struct{}) {

			// The "closer" can be used to either close the peer
			// quit channel after a certain timeout or immediately.
			closer, ok := closers[sp.ID()]
			if !ok {
				closer = newDelayedCloser(
					peerQuit, qo.rejectTimeout,
				)
				closers[sp.ID()] = closer
			}

			switch response := resp.(type) {
			// A peer has replied with a GetData message, so we'll
			// send them the transaction.
			case *wire.MsgGetData:
				for _, vec := range response.InvList {
					if vec.Hash == txHash {
						sp.QueueMessageWithEncoding(
							tx, nil, qo.encoding,
						)

						// Peers might send the INV
						// request multiple times, we
						// need to de-duplicate them
						// using a map.
						replies[sp.ID()] = struct{}{}

						// Okay, so the peer responded
						// with an INV message, and we
						// sent out the TX. If we never
						// hear anything back from the
						// peer it means they accepted
						// the tx. If we get a reject,
						// things are clear as well. But
						// what if they are just slow to
						// respond? We'll give them
						// another bit of time to
						// respond.
						closer.closeEventually(s.quit)
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
				rejections[sp.ID()] = broadcastErr
				rejectCodes[broadcastErr.Code]++

				log.Debugf("Transaction %v rejected by peer "+
					"%v: code = %v, reason = %q", txHash,
					sp.Addr(), broadcastErr.Code,
					broadcastErr.Reason)

				// A reject message is final, so we can close
				// the peer quit channel now, we don't expect
				// any further messages.
				closer.closeNow()
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
	if len(replies) == 0 {
		log.Debugf("No peers replied to inv message for transaction %v",
			txHash)
		return nil
	}

	// firstRejectWithCode returns the first reject error that we have for
	// a certain error code.
	firstRejectWithCode := func(code pushtx.BroadcastErrorCode) error {
		for _, broadcastErr := range rejections {
			if broadcastErr.Code == code {
				return broadcastErr
			}
		}

		// We can't really get here unless something is totally wrong in
		// the above error mapping code.
		return fmt.Errorf("invalid error mapping")
	}

	// If all of our peers who replied to our query also rejected our
	// transaction, we'll deem that there was actually something wrong with
	// it, so we'll return the most rejected error between all of our peers.
	log.Debugf("Got replies from %d peers and %d rejections", len(replies),
		len(rejections))
	if len(replies) == len(rejections) {
		log.Warnf("All peers rejected transaction %v checking errors",
			txHash)

		// First, find the reject code that was returned most often.
		var (
			mostRejectedCount = 0
			mostRejectedCode  pushtx.BroadcastErrorCode
		)
		for code, count := range rejectCodes {
			if count > mostRejectedCount {
				mostRejectedCount = count
				mostRejectedCode = code
			}
		}

		// Then return the first error we have for that code (it doesn't
		// really matter which one, as long as our error code parsing is
		// correct).
		return firstRejectWithCode(mostRejectedCode)
	}

	// We did get some rejections, but not from all peers. Perhaps that's
	// due to some peers responding too slowly. Or it could also be a
	// malicious peer trying to block us from publishing a TX. That's why
	// we want to check if more than 60% of the peers (by default) that
	// replied in time also sent a reject, we can be pretty certain that
	// this TX is probably invalid.
	if len(rejections) > 0 {
		numInvalid := float32(rejectCodes[pushtx.Invalid])
		numPeersResponded := float32(len(replies))

		log.Debugf("Of %d peers that replied, %d think the TX is "+
			"invalid", numPeersResponded, numInvalid)

		// 60% or more (by default) of the peers declared this TX as
		// invalid.
		if numInvalid/numPeersResponded >= qo.invalidTxThreshold {
			log.Warnf("Threshold of %d reached (%d out of %d "+
				"peers), declaring TX %v as invalid",
				qo.invalidTxThreshold, numInvalid,
				numPeersResponded, txHash)

			return firstRejectWithCode(pushtx.Invalid)
		}
	}

	return nil
}

// delayedCloser is a struct that makes sure a subject channel is closed at some
// point, either after a delay or immediately.
type delayedCloser struct {
	subject chan<- struct{}
	timeout time.Duration
	once    sync.Once
}

// newDelayedCloser creates a new delayed closer for the given subject channel.
func newDelayedCloser(subject chan<- struct{},
	timeout time.Duration) *delayedCloser {

	return &delayedCloser{
		subject: subject,
		timeout: timeout,
	}
}

// closeEventually closes the subject channel after the configured timeout or
// immediately if the quit channel is closed.
func (t *delayedCloser) closeEventually(quit chan struct{}) {
	go func() {
		defer t.closeNow()

		select {
		case <-time.After(t.timeout):
		case <-quit:
		}
	}()
}

// closeNow immediately closes the subject channel. This can safely be called
// multiple times as it will only attempt to close the channel at most once.
func (t *delayedCloser) closeNow() {
	t.once.Do(func() {
		close(t.subject)
	})
}
