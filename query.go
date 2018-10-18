// NOTE: THIS API IS UNSTABLE RIGHT NOW.

package neutrino

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/gcs"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/davecgh/go-spew/spew"
	"github.com/lightninglabs/neutrino/cache"
	"github.com/lightninglabs/neutrino/filterdb"
)

var (
	// QueryTimeout specifies how long to wait for a peer to answer a
	// query.
	QueryTimeout = time.Second * 3

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
)

// queries are a set of options that can be modified per-query, unlike global
// options.
//
// TODO: Make more query options that override global options.
type queryOptions struct {
	// timeout lets the query know how long to wait for a peer to answer
	// the query before moving onto the next peer.
	timeout time.Duration

	// numRetries tells the query how many times to retry asking each peer
	// the query.
	numRetries uint8

	// peerConnectTimeout lets the query know how long to wait for the
	// underlying chain service to connect to a peer before giving up
	// on a query in case we don't have any peers.
	peerConnectTimeout time.Duration

	// encoding lets the query know which encoding to use when queueing
	// messages to a peer.
	encoding wire.MessageEncoding

	// doneChan lets the query signal the caller when it's done, in case
	// it's run in a goroutine.
	doneChan chan<- struct{}

	// persistToDisk indicates whether the filter should also be written
	// to disk in addition to the memory cache. For "normal" wallets, they'll
	// almost never need to re-match a filter once it's been fetched unless
	// they're doing something like a key import.
	persistToDisk bool
}

// filterCacheKey represents the key used for FilterCache of the ChainService.
type filterCacheKey struct {
	blockHash  *chainhash.Hash
	filterType filterdb.FilterType
}

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

// PersistToDisk allows the caller to tell that the filter should be kept
// on disk once it's found.
func PersistToDisk() QueryOption {
	return func(qo *queryOptions) {
		qo.persistToDisk = true
	}
}

// queryState is an atomically updated per-query state for each query in a
// batch.
//
// State transitions are:
//
// * queryWaitSubmit->queryWaitResponse - send query to peer
// * queryWaitResponse->queryWaitSubmit - query timeout with no acceptable
//   response
// * queryWaitResponse->queryAnswered - acceptable response to query received
type queryState uint32

const (
	// Waiting to be submitted to a peer.
	queryWaitSubmit queryState = iota

	// Submitted to a peer, waiting for reply.
	queryWaitResponse

	// Valid reply received.
	queryAnswered
)

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

// queryChainServiceBatch is a helper function that sends a batch of queries to
// the entire pool of peers of the given ChainService, attempting to get them
// all answered unless the quit channel is closed. It continues to update its
// view of the connected peers in case peers connect or disconnect during the
// query. The package-level QueryTimeout parameter, overridable by the Timeout
// option, determines how long a peer waits for a query before moving onto the
// next one. The NumRetries option and the QueryNumRetries package-level
// variable are ignored; the query continues until it either completes or the
// passed quit channel is closed.  For memory efficiency, we attempt to get
// responses as close to ordered as we can, so that the caller can cache as few
// responses as possible before committing to storage.
//
// TODO(aakselrod): support for more than one in-flight query per peer to
// reduce effects of latency.
func queryChainServiceBatch(
	// s is the ChainService to use.
	s *ChainService,

	// queryMsgs is a slice of queries for which the caller wants responses.
	queryMsgs []wire.Message,

	// checkResponse is called for every received message to see if it
	// answers the query message. It should return true if so.
	checkResponse func(sp *ServerPeer, query wire.Message,
		resp wire.Message) bool,

	// queryQuit forces the query to end before it's complete.
	queryQuit <-chan struct{},

	// options takes functional options for executing the query.
	options ...QueryOption) {

	// Starting with the set of default options, we'll apply any specified
	// functional options to the query.
	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)

	// Shared state between this goroutine and the per-peer goroutines.
	queryStates := make([]uint32, len(queryMsgs))

	// subscription allows us to subscribe to notifications from peers.
	msgChan := make(chan spMsg, len(queryMsgs))
	subQuit := make(chan struct{})
	subscription := spMsgSubscription{
		msgChan:  msgChan,
		quitChan: subQuit,
	}
	defer close(subQuit)

	// peerStates and its companion mutex allow the peer goroutines to
	// tell the main goroutine what query they're currently working on.
	peerStates := make(map[string]wire.Message)
	var mtxPeerStates sync.RWMutex

	peerGoroutine := func(sp *ServerPeer, quit <-chan struct{},
		matchSignal <-chan struct{}) {

		// Subscribe to messages from the peer.
		sp.subscribeRecvMsg(subscription)
		defer sp.unsubscribeRecvMsgs(subscription)
		defer func() {
			mtxPeerStates.Lock()
			delete(peerStates, sp.Addr())
			mtxPeerStates.Unlock()
		}()

		// Track the last query our peer failed to answer and skip over
		// it for the next attempt. This helps prevent most instances
		// of the same peer being asked for the same query every time.
		firstUnfinished, handleQuery := 0, -1

		for firstUnfinished < len(queryMsgs) {
			select {
			case <-queryQuit:
				return
			case <-s.quit:
				return
			case <-quit:
				return
			default:
			}

			handleQuery = -1

			for i := firstUnfinished; i < len(queryMsgs); i++ {
				// If this query is finished and we're at
				// firstUnfinished, update firstUnfinished.
				if i == firstUnfinished &&
					atomic.LoadUint32(&queryStates[i]) ==
						uint32(queryAnswered) {
					firstUnfinished++

					log.Tracef("Query #%v already answered, "+
						"skipping", i)
					continue
				}

				// We check to see if the query is waiting to
				// be handled. If so, we mark it as being
				// handled. If not, we move to the next one.
				if !atomic.CompareAndSwapUint32(
					&queryStates[i],
					uint32(queryWaitSubmit),
					uint32(queryWaitResponse),
				) {
					log.Tracef("Query #%v already being "+
						"queried for, skipping", i)
					continue
				}

				// The query is now marked as in-process. We
				// begin to process it.
				handleQuery = i
				sp.QueueMessageWithEncoding(queryMsgs[i],
					nil, qo.encoding)
				break
			}

			// Regardless of whether we have a query or not, we
			// need a timeout.
			timeout := time.After(qo.timeout)
			if handleQuery == -1 {
				if firstUnfinished == len(queryMsgs) {
					// We've now answered all the queries.
					return
				}

				// We have nothing to work on but not all
				// queries are answered yet. Wait for a query
				// timeout, or a quit signal, then see if
				// anything needs our help.
				select {
				case <-queryQuit:
					return
				case <-s.quit:
					return
				case <-quit:
					return
				case <-timeout:
					if sp.Connected() {
						continue
					} else {
						return
					}
				}
			}

			// We have a query we're working on.
			mtxPeerStates.Lock()
			peerStates[sp.Addr()] = queryMsgs[handleQuery]
			mtxPeerStates.Unlock()
			select {
			case <-queryQuit:
				return
			case <-s.quit:
				return
			case <-quit:
				return
			case <-timeout:
				// We failed, so set the query state back to
				// zero and update our lastFailed state.
				atomic.StoreUint32(&queryStates[handleQuery],
					uint32(queryWaitSubmit))
				if !sp.Connected() {
					return
				}

				log.Tracef("Query for #%v failed, moving "+
					"on: %v", handleQuery,
					newLogClosure(func() string {
						return spew.Sdump(queryMsgs[handleQuery])
					}))

			case <-matchSignal:
				// We got a match signal so we can mark this
				// query a success.
				atomic.StoreUint32(&queryStates[handleQuery],
					uint32(queryAnswered))

				log.Tracef("Query #%v answered, updating state",
					handleQuery)
			}
		}
	}

	// peerQuits holds per-peer quit channels so we can kill the goroutines
	// when they disconnect.
	peerQuits := make(map[string]chan struct{})

	// matchSignals holds per-peer answer channels that get a notice that
	// the query got a match. If it's the peer's match, the peer can
	// mark the query a success and move on to the next query ahead of
	// timeout.
	matchSignals := make(map[string]chan struct{})

	// Clean up on exit.
	defer func() {
		for _, quitChan := range peerQuits {
			close(quitChan)
		}
	}()

	for {
		// Update our view of peers, starting new workers for new peers
		// and removing disconnected/banned peers.
		for _, peer := range s.Peers() {
			sp := peer.Addr()
			if _, ok := peerQuits[sp]; !ok && peer.Connected() {
				peerQuits[sp] = make(chan struct{})
				matchSignals[sp] = make(chan struct{})
				go peerGoroutine(
					peer, peerQuits[sp], matchSignals[sp],
				)
			}

		}

		for peer, quitChan := range peerQuits {
			p := s.PeerByAddr(peer)
			if p == nil || !p.Connected() {
				close(quitChan)
				close(matchSignals[peer])
				delete(peerQuits, peer)
				delete(matchSignals, peer)
			}
		}

		select {
		case msg := <-msgChan:
			mtxPeerStates.RLock()
			curQuery := peerStates[msg.sp.Addr()]
			mtxPeerStates.RUnlock()
			if checkResponse(msg.sp, curQuery, msg.msg) {
				select {
				case <-queryQuit:
					return
				case <-s.quit:
					return
				case matchSignals[msg.sp.Addr()] <- struct{}{}:
				}
			}
		case <-time.After(qo.timeout):
			// Check if we're done; if so, quit.
			allDone := true
			for i := 0; i < len(queryStates); i++ {
				if atomic.LoadUint32(&queryStates[i]) !=
					uint32(queryAnswered) {
					allDone = false
				}
			}
			if allDone {
				return
			}
		case <-queryQuit:
			return

		case <-s.quit:
			return
		}
	}
}

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
	peerTimeout := time.NewTicker(qo.timeout)
	timeout := time.After(qo.peerConnectTimeout)
	if queryPeer != nil {
		peerTries[queryPeer.Addr()]++
		queryPeer.subscribeRecvMsg(subscription)
		queryPeer.QueueMessageWithEncoding(queryMsg, nil, qo.encoding)
	}
checkResponses:
	for {
		select {
		case <-timeout:
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

		// The current peer we're querying has failed to answer the
		// query. Time to select a new peer and query it.
		case <-peerTimeout.C:
			if queryPeer != nil {
				queryPeer.unsubscribeRecvMsgs(subscription)
			}

			queryPeer = nil
			for _, curPeer := range s.Peers() {
				if curPeer != nil && curPeer.Connected() &&
					peerTries[curPeer.Addr()] < qo.numRetries {

					curPeer := curPeer
					queryPeer = curPeer

					// Found a peer we can query.
					peerTries[queryPeer.Addr()]++
					queryPeer.subscribeRecvMsg(subscription)
					queryPeer.QueueMessageWithEncoding(
						queryMsg, nil, qo.encoding,
					)
					break
				}
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

	cacheKey := filterCacheKey{blockHash: blockHash, filterType: filterType}

	filterValue, err := s.FilterCache.Get(cacheKey)
	if err != nil {
		return nil, err
	}

	return filterValue.(*cache.CacheableFilter).Filter, nil
}

// putFilterToCache inserts a given filter in ChainService's FilterCache.
func (s *ChainService) putFilterToCache(blockHash *chainhash.Hash,
	filterType filterdb.FilterType, filter *gcs.Filter) error {

	cacheKey := filterCacheKey{blockHash: blockHash, filterType: filterType}
	return s.FilterCache.Put(cacheKey, &cache.CacheableFilter{Filter: filter})
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

	// Only get one CFilter at a time to avoid redundancy from mutliple
	// rescans running at once.
	s.mtxCFilter.Lock()
	defer s.mtxCFilter.Unlock()

	// Based on if extended is true or not, we'll set up our set of
	// querying, and db-write functions.
	getHeader := s.RegFilterHeaders.FetchHeader
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

	// We didn't get the filter from the DB, so we'll set it to nil and try
	// to get it from the network.
	filter = nil

	// In order to verify the authenticity of the filter, we'll fetch the
	// target block header so we can retrieve the hash of the prior block,
	// which is required to fetch the filter header for that block.
	block, height, err := s.BlockHeaders.FetchHeader(&blockHash)
	if err != nil {
		return nil, err
	}
	if block.BlockHash() != blockHash {
		return nil, fmt.Errorf("Couldn't get header for block %s "+
			"from database", blockHash)
	}

	log.Debugf("Fetching filter for height=%v, hash=%v", height, blockHash)

	// In addition to fetching the block header, we'll fetch the filter
	// headers (for this particular filter type) from the database. These
	// are required in order to verify the authenticity of the filter.
	curHeader, err := getHeader(&blockHash)
	if err != nil {
		return nil, fmt.Errorf("Couldn't get cfheader for block %s "+
			"from database", blockHash)
	}
	prevHeader, err := getHeader(&block.PrevBlock)
	if err != nil {
		return nil, fmt.Errorf("Couldn't get cfheader for block %s "+
			"from database", blockHash)
	}

	// With all the necessary items retrieved, we'll launch our concurrent
	// query to the set of connected peers.
	s.queryPeers(
		// Send a wire.MsgGetCFilters
		wire.NewMsgGetCFilters(filterType, height, &blockHash),

		// Check responses and if we get one that matches, end the
		// query early.
		func(sp *ServerPeer, resp wire.Message, quit chan<- struct{}) {
			switch response := resp.(type) {
			// We're only interested in "cfilter" messages.
			case *wire.MsgCFilter:
				// Only keep this going if we haven't already
				// found a filter, or we risk closing an
				// already closed channel.
				if filter != nil {
					return
				}

				// If the response doesn't match our request.
				// Ignore this message.
				if blockHash != response.BlockHash ||
					filterType != response.FilterType {
					return
				}

				gotFilter, err := gcs.FromNBytes(
					builder.DefaultP, builder.DefaultM,
					response.Data,
				)
				if err != nil {
					// Malformed filter data. We can ignore
					// this message.
					return
				}

				// Now that we have a proper filter, ensure
				// that re-calculating the filter header hash
				// for the header _after_ the filter in the
				// chain checks out. If not, we can ignore this
				// response.
				if gotHeader, err := builder.
					MakeHeaderForFilter(gotFilter,
						*prevHeader); err != nil ||
					gotHeader != *curHeader {
					return
				}

				// At this point, the filter matches what we
				// know about it and we declare it sane. We can
				// kill the query and pass the response back to
				// the caller.
				filter = gotFilter
				close(quit)
			default:
			}
		},
		options...,
	)

	if filter != nil {
		// If we found a filter, put it in the cache and persistToDisk if
		// the caller requested it.
		err := s.putFilterToCache(&blockHash, dbFilterType, filter)
		if err != nil {
			log.Warnf("couldn't write filter to cache: %v", err)
		}

		qo := defaultQueryOptions()
		qo.applyQueryOptions(options...)
		if qo.persistToDisk {
			err := s.FilterDB.PutFilter(&blockHash, filter, dbFilterType)
			if err != nil {
				return nil, err
			}

			log.Tracef("Wrote filter for block %s, type %d",
				blockHash, filterType)
		}
	}

	return filter, nil
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
		return nil, fmt.Errorf("Couldn't get header for block %s "+
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
	getData.AddInvVect(inv)

	// The block is only updated from the checkResponse function argument,
	// which is always called single-threadedly. We don't check the block
	// until after the query is finished, so we can just write to it
	// naively.
	var foundBlock *btcutil.Block
	s.queryPeers(
		// Send a wire.GetDataMsg
		getData,

		// Check responses and if we get one that matches, end the
		// query early.
		func(sp *ServerPeer, resp wire.Message,
			quit chan<- struct{}) {
			switch response := resp.(type) {
			// We're only interested in "block" messages.
			case *wire.MsgBlock:
				// Only keep this going if we haven't already
				// found a block, or we risk closing an already
				// closed channel.
				if foundBlock != nil {
					return
				}

				// If this isn't our block, ignore it.
				if response.BlockHash() != blockHash {
					return
				}
				block := btcutil.NewBlock(response)

				// Only set height if btcutil hasn't
				// automagically put one in.
				if block.Height() == btcutil.BlockHeightUnknown {
					block.SetHeight(int32(height))
				}

				// If this claims our block but doesn't pass
				// the sanity check, the peer is trying to
				// bamboozle us. Disconnect it.
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
						"received from %s -- "+
						"disconnecting peer", blockHash,
						sp.Addr())
					sp.Disconnect()
					return
				}

				// TODO(roasbeef): modify CheckBlockSanity to
				// also check witness commitment

				// At this point, the block matches what we
				// know about it and we declare it sane. We can
				// kill the query and pass the response back to
				// the caller.
				foundBlock = block
				close(quit)
			default:
			}
		},
		options...,
	)
	if foundBlock == nil {
		return nil, fmt.Errorf("Couldn't retrieve block %s from "+
			"network", blockHash)
	}

	// Add block to the cache before returning it.
	err = s.BlockCache.Put(*inv, &cache.CacheableBlock{foundBlock})
	if err != nil {
		log.Warnf("couldn't write block to cache: %v", err)
	}

	return foundBlock, nil
}

// SendTransaction sends a transaction to all peers. It returns an error if any
// peer rejects the transaction.
//
// TODO: Better privacy by sending to only one random peer and watching
// propagation, requires better peer selection support in query API.
func (s *ChainService) SendTransaction(tx *wire.MsgTx, options ...QueryOption) error {

	var err error

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
	inv.AddInvVect(wire.NewInvVect(invType, &txHash))

	// Send the peer query and listen for getdata.
	s.queryAllPeers(
		inv,
		func(sp *ServerPeer, resp wire.Message, quit chan<- struct{},
			peerQuit chan<- struct{}) {
			switch response := resp.(type) {
			case *wire.MsgGetData:
				for _, vec := range response.InvList {
					if vec.Hash == txHash {
						sp.QueueMessageWithEncoding(
							tx, nil, qo.encoding)
					}
				}
			case *wire.MsgReject:
				if response.Hash == txHash {
					err = fmt.Errorf("Transaction %s "+
						"rejected by %s: %s",
						tx.TxHash(), sp.Addr(),
						response.Reason)
					log.Errorf(err.Error())
					close(quit)
				}
			}
		},
		// Default to 500ms timeout. Default for queryAllPeers is a
		// single try.
		append(
			[]QueryOption{Timeout(time.Millisecond * 500)},
			options...,
		)...,
	)

	return err
}
