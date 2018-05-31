// NOTE: THIS API IS UNSTABLE RIGHT NOW.

package neutrino

import (
	"fmt"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/gcs"
	"github.com/btcsuite/btcutil/gcs/builder"
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

	// QueryTimeBetweenPeerEnums specifies how long to wait between attempts
	// to enumerate the peers to which the underlying chain service is
	// connected. This value is only set globally, not as a query
	// option.
	QueryTimeBetweenPeerEnums = time.Millisecond * 200
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

	// doneChan lets the query signal the caller when it's done, in case
	// it's run in a goroutine.
	doneChan chan<- struct{}
}

// QueryOption is a functional option argument to any of the network query
// methods, such as GetBlockFromNetwork and GetCFilter (when that resorts to a
// network query). These are always processed in order, with later options
// overriding earlier ones.
type QueryOption func(*queryOptions)

// defaultQueryOptions returns a queryOptions set to package-level defaults.
func defaultQueryOptions() *queryOptions {
	return &queryOptions{
		timeout:            QueryTimeout,
		numRetries:         uint8(QueryNumRetries),
		peerConnectTimeout: QueryPeerConnectTimeout,
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

// DoneChan allows the caller to pass a channel that will get closed when the
// query is finished.
func DoneChan(doneChan chan<- struct{}) QueryOption {
	return func(qo *queryOptions) {
		qo.doneChan = doneChan
	}
}

// queryPeers is a helper function that sends a query to one or more peers and
// waits for an answer. The timeout for queries is set by the QueryTimeout
// package-level variable.
func (s *ChainService) queryPeers(
	// queryMsg is the message to send to each peer selected by selectPeer.
	queryMsg wire.Message,

	// checkResponse is caled for every message within the timeout period.
	// The quit channel lets the query know to terminate because the
	// required response has been found. This is done by closing the
	// channel.
	checkResponse func(sp *ServerPeer, resp wire.Message,
		quit chan<- struct{}),

	// options takes functional options for executing the query.
	options ...QueryOption) {

	// Starting witht he set of default options, we'll apply any specified
	// functional options to the query.
	qo := defaultQueryOptions()
	for _, option := range options {
		option(qo)
	}

	// We get an initial view of our peers, to be updated each time a peer
	// query times out.
	curPeer := s.blockManager.SyncPeer()
	peerTries := make(map[string]uint8)

	// This will be state used by the peer query goroutine.
	quit := make(chan struct{})
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
	if curPeer != nil {
		peerTries[curPeer.Addr()]++
		curPeer.subscribeRecvMsg(subscription)
		curPeer.QueueMessageWithEncoding(queryMsg, nil,
			wire.WitnessEncoding)
	}
checkResponses:
	for {
		select {
		case <-timeout:
			// When we time out, we're done.
			if curPeer != nil {
				curPeer.unsubscribeRecvMsgs(subscription)
			}
			break checkResponses

		case <-quit:
			// Same when we get a quit signal.
			if curPeer != nil {
				curPeer.unsubscribeRecvMsgs(subscription)
			}
			break checkResponses

		// A message has arrived over the subscription channel, so we
		// execute the checkResponses callback to see if this ends our
		// query session.
		case sm := <-msgChan:
			// TODO: This will get stuck if checkResponse gets
			// stuck. This is a caveat for callers that should be
			// fixed before exposing this function for public use.
			checkResponse(sm.sp, sm.msg, quit)

		// The current peer we're querying has failed to answer the
		// query. Time to select a new peer and query it.
		case <-peerTimeout.C:
			if curPeer != nil {
				curPeer.unsubscribeRecvMsgs(subscription)
			}

			curPeer = nil
			for _, curPeer = range s.Peers() {
				if curPeer != nil && curPeer.Connected() &&
					peerTries[curPeer.Addr()] <
						qo.numRetries {
					// Found a peer we can query.
					peerTries[curPeer.Addr()]++
					curPeer.subscribeRecvMsg(subscription)
					curPeer.QueueMessageWithEncoding(
						queryMsg, nil,
						wire.WitnessEncoding)
					break
				}
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

// GetCFilter gets a cfilter from the database. Failing that, it requests the
// cfilter from the network and writes it to the database. If extended is true,
// an extended filter will be queried for. Otherwise, we'll fetch the regular
// filter.
func (s *ChainService) GetCFilter(blockHash chainhash.Hash,
	filterType wire.FilterType, options ...QueryOption) (*gcs.Filter,
	error) {
	// Only get one CFilter at a time to avoid redundancy from mutliple
	// rescans running at once.
	s.mtxCFilter.Lock()
	defer s.mtxCFilter.Unlock()

	// Based on if extended is true or not, we'll set up our set of
	// querying, and db-write functions.
	getHeader := s.RegFilterHeaders.FetchHeader
	dbFilterType := filterdb.RegularFilter
	if filterType == wire.GCSFilterExtended {
		getHeader = s.ExtFilterHeaders.FetchHeader
		dbFilterType = filterdb.ExtendedFilter
	}

	// First check the database to see if we already have this filter. If
	// so, then we can return it an exit early.
	filter, err := s.FilterDB.FetchFilter(&blockHash, dbFilterType)
	if err == nil && filter != nil {
		return filter, nil
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
					builder.DefaultP, response.Data)
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

	// If we've found a filter, write it to the database for next time.
	if filter != nil {
		err := s.FilterDB.PutFilter(&blockHash, filter, dbFilterType)
		if err != nil {
			return nil, err
		}

		log.Tracef("Wrote filter for block %s, type %d",
			blockHash, filterType)
	}

	return filter, nil
}

// GetBlockFromNetwork gets a block by requesting it from the network, one peer
// at a time, until one answers.
//
// TODO(roasbeef): add query option to indicate if the caller wants witness
// data or not.
func (s *ChainService) GetBlockFromNetwork(blockHash chainhash.Hash,
	options ...QueryOption) (*btcutil.Block, error) {

	// Fetch the corresponding block header from the database. If this
	// isn't found, then we don't have the header for this block s we can't
	// request it.
	blockHeader, height, err := s.BlockHeaders.FetchHeader(&blockHash)
	if err != nil || blockHeader.BlockHash() != blockHash {
		return nil, fmt.Errorf("Couldn't get header for block %s "+
			"from database", blockHash)
	}

	// Construct the appropriate getdata message to fetch the target block.
	getData := wire.NewMsgGetData()
	getData.AddInvVect(wire.NewInvVect(wire.InvTypeWitnessBlock,
		&blockHash))

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

	return foundBlock, nil
}

// SendTransaction sends a transaction to each peer. It returns an error if any
// peer rejects the transaction.
//
// TODO: Better privacy by sending to only one random peer and watching
// propagation, requires better peer selection support in query API.
func (s *ChainService) SendTransaction(tx *wire.MsgTx, options ...QueryOption) error {

	var err error
	s.queryPeers(
		tx,
		func(sp *ServerPeer, resp wire.Message, quit chan<- struct{}) {
			switch response := resp.(type) {
			case *wire.MsgReject:
				if response.Hash == tx.TxHash() {
					err = fmt.Errorf("Transaction %s "+
						"rejected by %s: %s",
						tx.TxHash(), sp.Addr(),
						response.Reason)
					log.Errorf(err.Error())
					close(quit)
				}
			}
		},
		options...,
	)

	return err
}
