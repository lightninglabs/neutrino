// NOTE: THIS API IS UNSTABLE RIGHT NOW.

package neutrino

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/gcs"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/btcsuite/btcwallet/waddrmgr"
	"github.com/lightninglabs/neutrino/headerfs"
)

var (
	// zeroOutPoint indicates that we should match on an output's script
	// when dispatching a spend notification.
	zeroOutPoint wire.OutPoint

	// ErrRescanExit is an error returned to the caller in case the ongoing
	// rescan exits.
	ErrRescanExit = errors.New("rescan exited")
)

// rescanOptions holds the set of functional parameters for Rescan.
type rescanOptions struct {
	chain *ChainService

	queryOptions []QueryOption

	ntfn rpcclient.NotificationHandlers

	startTime  time.Time
	startBlock *waddrmgr.BlockStamp

	endBlock *waddrmgr.BlockStamp

	watchAddrs  []btcutil.Address
	watchInputs []InputWithScript
	watchList   [][]byte
	txIdx       uint32

	update <-chan *updateOptions
	quit   <-chan struct{}
}

// RescanOption is a functional option argument to any of the rescan and
// notification subscription methods. These are always processed in order, with
// later options overriding earlier ones.
type RescanOption func(ro *rescanOptions)

func defaultRescanOptions() *rescanOptions {
	return &rescanOptions{}
}

// QueryOptions pass onto the underlying queries.
func QueryOptions(options ...QueryOption) RescanOption {
	return func(ro *rescanOptions) {
		ro.queryOptions = options
	}
}

// NotificationHandlers specifies notification handlers for the rescan. These
// will always run in the same goroutine as the caller.
func NotificationHandlers(ntfn rpcclient.NotificationHandlers) RescanOption {
	return func(ro *rescanOptions) {
		ro.ntfn = ntfn
	}
}

// StartBlock specifies the start block. The hash is checked first; if there's
// no such hash (zero hash avoids lookup), the height is checked next. If the
// height is 0 or the start block isn't specified, starts from the genesis
// block. This block is assumed to already be known, and no notifications will
// be sent for this block. The rescan uses the latter of StartBlock and
// StartTime.
func StartBlock(startBlock *waddrmgr.BlockStamp) RescanOption {
	return func(ro *rescanOptions) {
		ro.startBlock = startBlock
	}
}

// StartTime specifies the start time. The time is compared to the timestamp of
// each block, and the rescan only begins once the first block crosses that
// timestamp. When using this, it is advisable to use a margin of error and
// start rescans slightly earlier than required. The rescan uses the latter of
// StartBlock and StartTime.
func StartTime(startTime time.Time) RescanOption {
	return func(ro *rescanOptions) {
		ro.startTime = startTime
	}
}

// EndBlock specifies the end block. The hash is checked first; if there's no
// such hash (zero hash avoids lookup), the height is checked next. If the
// height is 0 or in the future or the end block isn't specified, the quit
// channel MUST be specified as Rescan will sync to the tip of the blockchain
// and continue to stay in sync and pass notifications. This is enforced at
// runtime.
func EndBlock(endBlock *waddrmgr.BlockStamp) RescanOption {
	return func(ro *rescanOptions) {
		ro.endBlock = endBlock
	}
}

// WatchAddrs specifies the addresses to watch/filter for. Each call to this
// function adds to the list of addresses being watched rather than replacing
// the list. Each time a transaction spends to the specified address, the
// outpoint is added to the WatchOutPoints list.
func WatchAddrs(watchAddrs ...btcutil.Address) RescanOption {
	return func(ro *rescanOptions) {
		ro.watchAddrs = append(ro.watchAddrs, watchAddrs...)
	}
}

// InputWithScript couples an previous outpoint along with its input script.
// We'll use the prev script to match the filter itself, but then scan for the
// particular outpoint when we need to make a notification decision.
type InputWithScript struct {
	// OutPoint identifies the previous output to watch.
	OutPoint wire.OutPoint

	// PkScript is the script of the previous output.
	PkScript []byte
}

// WatchInputs specifies the outpoints to watch for on-chain spends. We also
// require the script as we'll match on the script, but then notify based on
// the outpoint. Each call to this function adds to the list of outpoints being
// watched rather than replacing the list.
func WatchInputs(watchInputs ...InputWithScript) RescanOption {
	return func(ro *rescanOptions) {
		ro.watchInputs = append(ro.watchInputs, watchInputs...)
	}
}

// TxIdx specifies a hint transaction index into the block in which the UTXO is
// created (eg, coinbase is 0, next transaction is 1, etc.)
func TxIdx(txIdx uint32) RescanOption {
	return func(ro *rescanOptions) {
		ro.txIdx = txIdx
	}
}

// QuitChan specifies the quit channel. This can be used by the caller to let
// an indefinite rescan (one with no EndBlock set) know it should gracefully
// shut down. If this isn't specified, an end block MUST be specified as Rescan
// must know when to stop. This is enforced at runtime.
func QuitChan(quit <-chan struct{}) RescanOption {
	return func(ro *rescanOptions) {
		ro.quit = quit
	}
}

// updateChan specifies an update channel. This is for internal use by the
// Rescan.Update functionality.
func updateChan(update <-chan *updateOptions) RescanOption {
	return func(ro *rescanOptions) {
		ro.update = update
	}
}

// rescan is a single-threaded function that uses headers from the database and
// functional options as arguments.
func (s *ChainService) rescan(options ...RescanOption) error {
	// First, we'll apply the set of default options, then serially apply
	// all the options that've been passed in.
	ro := defaultRescanOptions()
	ro.endBlock = &waddrmgr.BlockStamp{
		Hash:   chainhash.Hash{},
		Height: 0,
	}
	for _, option := range options {
		option(ro)
	}
	ro.chain = s

	// If we have something to watch, create a watch list. The watch list
	// can be composed of a set of scripts, outpoints, and txids.
	for _, addr := range ro.watchAddrs {
		script, err := txscript.PayToAddrScript(addr)
		if err != nil {
			return err
		}

		ro.watchList = append(ro.watchList, script)
	}
	for _, input := range ro.watchInputs {
		ro.watchList = append(ro.watchList, input.PkScript)
	}

	// Check that we have either an end block or a quit channel.
	if ro.endBlock != nil {
		// If the end block hash is non-nil, then we'll query the
		// database to find out the stop height.
		if (ro.endBlock.Hash != chainhash.Hash{}) {
			_, height, err := s.BlockHeaders.FetchHeader(
				&ro.endBlock.Hash,
			)
			if err != nil {
				ro.endBlock.Hash = chainhash.Hash{}
			} else {
				ro.endBlock.Height = int32(height)
			}
		}

		// If the ending hash it nil, then check to see if the target
		// height is non-nil. If not, then we'll use this to find the
		// stopping hash.
		if (ro.endBlock.Hash == chainhash.Hash{}) {
			if ro.endBlock.Height != 0 {
				header, err := s.BlockHeaders.FetchHeaderByHeight(
					uint32(ro.endBlock.Height))
				if err == nil {
					ro.endBlock.Hash = header.BlockHash()
				} else {
					ro.endBlock = &waddrmgr.BlockStamp{}
				}
			}
		}
	} else {
		ro.endBlock = &waddrmgr.BlockStamp{}
	}

	// If we don't have a quit channel, and the end height is still
	// unspecified, then we'll exit out here.
	if ro.quit == nil && ro.endBlock.Height == 0 {
		return fmt.Errorf("Rescan request must specify a quit channel" +
			" or valid end block")
	}

	// Track our position in the chain.
	var (
		curHeader wire.BlockHeader
		curStamp  waddrmgr.BlockStamp
	)

	// If no start block is specified, start the scan from our current best
	// block.
	if ro.startBlock == nil {
		bs, err := s.BestBlock()
		if err != nil {
			return err
		}
		ro.startBlock = bs
	}
	curStamp = *ro.startBlock

	// To find our starting block, either the start hash should be set, or
	// the start height should be set. If neither is, then we'll be
	// starting from the genesis block.
	if (curStamp.Hash != chainhash.Hash{}) {
		header, height, err := s.BlockHeaders.FetchHeader(&curStamp.Hash)
		if err == nil {
			curHeader = *header
			curStamp.Height = int32(height)
		} else {
			curStamp.Hash = chainhash.Hash{}
		}
	}
	if (curStamp.Hash == chainhash.Hash{}) {
		if curStamp.Height == 0 {
			curStamp.Hash = *s.chainParams.GenesisHash
		} else {
			header, err := s.BlockHeaders.FetchHeaderByHeight(
				uint32(curStamp.Height))
			if err == nil {
				curHeader = *header
				curStamp.Hash = curHeader.BlockHash()
			} else {
				curHeader = s.chainParams.GenesisBlock.Header
				curStamp.Hash = *s.chainParams.GenesisHash
				curStamp.Height = 0
			}
		}
	}

	s.blockManager.newFilterHeadersMtx.RLock()
	filterHeaderHeight := s.blockManager.filterHeaderTip
	s.blockManager.newFilterHeadersMtx.RUnlock()

	log.Debugf("Waiting for filter headers (height=%v) to catch up the "+
		"rescan start (height=%v)", filterHeaderHeight, curStamp.Height)

	// We'll wait here at this point until we have enough filter headers to
	// actually start walking forwards in the chain. To be able to wake up
	// in cause we are being asked to exit, we'll launch a new goroutine to
	// wait.
	done := make(chan struct{})
	go func() {
		s.blockManager.newFilterHeadersMtx.Lock()
		for s.blockManager.filterHeaderTip < uint32(curStamp.Height) {
			s.blockManager.newFilterHeadersSignal.Wait()

			// While we're awake, check to see if we need to exit.
			select {
			case <-ro.quit:
				s.blockManager.newFilterHeadersMtx.Unlock()
				return
			default:
			}
		}
		s.blockManager.newFilterHeadersMtx.Unlock()
		close(done)
	}()

	// Now wait for either filter headers to be fully synced, or we are
	// quitting. We also queue any incoming rescan updates, such that we
	// can apply them when the filters are synced.
	var updates []*updateOptions
filterHeaderWaitLoop:
	for {
		select {
		case update := <-ro.update:
			updates = append(updates, update)

		case <-done:
			break filterHeaderWaitLoop

		case <-ro.quit:
			// Broadcast the header signal such that the goroutine
			// can wake up and exit.
			s.blockManager.newFilterHeadersSignal.Broadcast()
			return ErrRescanExit
		}
	}

	// If any updates were queued while waiting for the filter headers to
	// sync, apply them now.
	for _, upd := range updates {
		_, err := ro.updateFilter(upd, &curStamp, &curHeader)
		if err != nil {
			return err
		}
	}

	log.Debugf("Starting rescan from known block %d (%s)", curStamp.Height,
		curStamp.Hash)

	// Compare the start time to the start block. If the start time is
	// later, cycle through blocks until we find a block timestamp later
	// than the start time, and begin filter download at that block. Since
	// time is non-monotonic between blocks, we look for the first block to
	// trip the switch, and download filters from there, rather than
	// checking timestamps at each block.
	scanning := ro.startTime.Before(curHeader.Timestamp)

	// Listen for notifications.
	blockConnected := make(chan wire.BlockHeader)
	blockDisconnected := make(chan wire.BlockHeader)

	var (
		subscription *blockSubscription
		err          error
	)

	// blockRetryInterval is the interval in which we'll continually re-try
	// to fetch the latest filter from our peers.
	//
	// TODO(roasbeef): add exponential back-off
	blockRetryInterval := time.Millisecond * 100

	// blockReFetchTimer is a stoppable timer that we'll use to reminder
	// ourselves to refetch a block in the case that we're unable to fetch
	// the filter for a block the first time around.
	var blockReFetchTimer *time.Timer

	resetBlockReFetchTimer := func(headerTip wire.BlockHeader, height int32) {
		// If so, then we'll avoid notifying the block, and will
		// instead add this to our retry queue, as we should be getting
		// block disconnected notifications in short order.
		if blockReFetchTimer != nil {
			blockReFetchTimer.Stop()
		}

		log.Infof("Setting timer to attempt to re-fetch filter for "+
			"hash=%v, height=%v", headerTip.BlockHash(), height)

		// We'll start a timer to re-send this header so we re-process
		// if in the case that we don't get a re-org soon afterwards.
		blockReFetchTimer = time.AfterFunc(blockRetryInterval, func() {
			log.Infof("Resending rescan header for block hash=%v, "+
				"height=%v", headerTip.BlockHash(), height)

			select {
			case blockConnected <- headerTip:
			case <-ro.quit:
			}
		})
	}

	// Loop through blocks, one at a time. This relies on the underlying
	// ChainService API to send blockConnected and blockDisconnected
	// notifications in the correct order.
	current := false
rescanLoop:
	for {
		// If we've reached the ending height or hash for this rescan,
		// then we'll exit.
		if curStamp.Hash == ro.endBlock.Hash ||
			(ro.endBlock.Height > 0 &&
				curStamp.Height == ro.endBlock.Height) {
			return nil
		}

		// If we're current, we wait for notifications that will be
		// delivered each time a block is connecting, disconnecting, or
		// we can an update to the filter we should be looking for.
		switch current {
		case true:
			// Wait for a signal that we have a newly connected
			// header and cfheader, or a newly disconnected header;
			// alternatively, forward ourselves to the next block
			// if possible.
			select {

			case <-ro.quit:
				return ErrRescanExit

			// An update mesage has just come across, if it points
			// to a prior point in the chain, then we may need to
			// rewind a bit in order to provide the client all its
			// requested client.
			case update := <-ro.update:
				rewound, err := ro.updateFilter(
					update, &curStamp, &curHeader,
				)
				if err != nil {
					return err
				}

				// If we have to rewind our state, then we'll
				// mark ourselves as not current so we can walk
				// forward in the chain again until we we are
				// current. This is our way of doing a manual
				// rescan.
				if rewound {
					log.Tracef("Rewound to block %d (%s), "+
						"no longer current",
						curStamp.Height, curStamp.Hash)

					current = false
					s.unsubscribeBlockMsgs(subscription)
					subscription = nil
				}

			case header := <-blockConnected:
				// If we've somehow missed a header in the
				// range, then we'll mark ourselves as not
				// current so we can walk down the chain and
				// notify the callers of blocks we may have
				// missed.
				//
				// It's possible due to the nature of the
				// current subscription system that we get a
				// duplicate block. We'll catch this and
				// continue forwards to avoid an unnecessary
				// state transition back to the !current state.
				if header.PrevBlock != curStamp.Hash &&
					header.BlockHash() != curStamp.Hash {

					log.Debugf("Rescan got out of order "+
						"block %s with prevblock %s, "+
						"curHeader: %s",
						header.BlockHash(),
						header.PrevBlock,
						curStamp.Hash)

					current = false
					continue rescanLoop
				}

				// Do not process block until we have all
				// filter headers. Don't worry, the block will
				// get re-queued every time there is a new
				// filter available. However, if it's a
				// duplicate block notification, then we can
				// re-process it without any issues.
				if header.BlockHash() != curStamp.Hash &&
					!s.hasFilterHeadersByHeight(uint32(curStamp.Height+1)) {
					log.Warnf("Missing filter header for "+
						"height=%v, skipping",
						curStamp.Height+1)
					continue rescanLoop
				}

				// As this could be a re-try, we'll ensure that
				// we don't incorrectly increment our current
				// time stamp.
				if curStamp.Hash != header.BlockHash() {
					curHeader = header
					curStamp.Hash = header.BlockHash()
					curStamp.Height++
				}

				log.Tracef("Rescan got block %d (%s)", curStamp.Height,
					curStamp.Hash)

				// We're only scanning if the header is beyond
				// the horizon of our start time.
				if !scanning {
					scanning = ro.startTime.Before(
						curHeader.Timestamp,
					)
				}

				// If we're actually scanning and we have a
				// non-empty watch list, then we'll attempt to
				// fetch the filter from the network.
				var blockFilter *gcs.Filter
				queryOptions := NumRetries(0)
				blockFilter, err = s.GetCFilter(
					curStamp.Hash, wire.GCSFilterRegular,
					queryOptions,
				)

				switch {
				// If the block index doesn't know about
				// this block, then it's likely we're mid
				// re-org so we'll accept this as we
				// account for it below.
				case err == headerfs.ErrHashNotFound:

				case err != nil:
					return fmt.Errorf("unable to get "+
						"filter for hash=%v: %v",
						curStamp.Hash, err)
				}

				// If the filter is nil, then this either means
				// that we don't have any peers to fetch this
				// filter from, or the peer(s) that we're
				// trying to fetch from are in the progress of
				// a re-org.
				if blockFilter == nil {
					// TODO(halseth): this is racy, as
					// blocks can come in before we
					// refetch.
					resetBlockReFetchTimer(
						header, curStamp.Height,
					)
					continue
				}

				err := s.notifyBlockWithFilter(
					ro, &curHeader, &curStamp, blockFilter,
				)
				if err != nil {
					return err
				}

				// We'll successfully fetched this current
				// block, so we'll reset the retry timer back
				// to nil.
				blockReFetchTimer = nil

			case header := <-blockDisconnected:
				log.Debugf("Rescan disconnect block %d (%s)\n",
					curStamp.Height, curStamp.Hash)

				// Only deal with it if it's the current block
				// we know about. Otherwise, it's in the
				// future.
				if header.BlockHash() == curStamp.Hash {
					// Run through notifications. This is
					// all single-threaded. We include
					// deprecated calls as they're still
					// used, for now.
					if ro.ntfn.OnFilteredBlockDisconnected != nil {
						ro.ntfn.OnFilteredBlockDisconnected(
							curStamp.Height,
							&curHeader)
					}
					if ro.ntfn.OnBlockDisconnected != nil {
						ro.ntfn.OnBlockDisconnected(
							&curStamp.Hash,
							curStamp.Height,
							curHeader.Timestamp)
					}

					header := s.getReorgTip(header.PrevBlock)
					curHeader = *header
					curStamp.Hash = header.BlockHash()
					curStamp.Height--

					// Now that we got a re-org, if we had
					// a re-fetch timer going, we'll re-set
					// is at the new header tip.
					if blockReFetchTimer != nil {
						resetBlockReFetchTimer(
							*header, curStamp.Height,
						)
					}
				}
			}

		// If we're not yet current, then we'll walk down the chain
		// until we reach the tip of the chain as we know it. At this
		// point, we'll be "current" again.
		case false:

			// Apply all queued filter updates.
		updateFilterLoop:
			for {
				select {
				case update := <-ro.update:
					_, err := ro.updateFilter(
						update, &curStamp, &curHeader,
					)
					if err != nil {
						return err
					}

				default:
					break updateFilterLoop
				}
			}

			bestBlock, err := s.BestBlock()
			if err != nil {
				return err
			}

			// Since we're not current, we try to manually advance
			// the block. If the next height is above the best
			// height known to the chain service, then we mark
			// ourselves as current and follow notifications.
			nextHeight := curStamp.Height + 1
			if nextHeight > bestBlock.Height {
				log.Debugf("Rescan became current at %d (%s), "+
					"subscribing to block notifications",
					curStamp.Height, curStamp.Hash)

				current = true

				// Ensure we cancel the old subscroption if
				// we're going back to scan for missed blocks.
				if subscription != nil {
					s.unsubscribeBlockMsgs(subscription)
				}

				// Subscribe to block notifications.
				subscription, err = s.subscribeBlockMsg(
					uint32(curStamp.Height), blockConnected,
					blockDisconnected, nil,
				)
				if err != nil {
					return fmt.Errorf("unable to register "+
						"block subscription: %v", err)
				}
				defer func() {
					if subscription != nil {
						s.unsubscribeBlockMsgs(subscription)
						subscription = nil
					}
				}()

				continue rescanLoop
			}

			// If the next height is known to the chain service,
			// then we'll fetch the next block and send a
			// notification, maybe also scanning the filters for
			// the block.
			header, err := s.BlockHeaders.FetchHeaderByHeight(
				uint32(nextHeight),
			)
			if err != nil {
				return err
			}

			curHeader = *header
			curStamp.Height++
			curStamp.Hash = header.BlockHash()

			if !scanning {
				scanning = ro.startTime.Before(curHeader.Timestamp)
			}
			err = s.notifyBlock(ro, curHeader, curStamp, scanning)
			if err != nil {
				return err
			}
		}
	}
}

// notifyBlock calls appropriate listeners based on the block filter.
func (s *ChainService) notifyBlock(ro *rescanOptions,
	curHeader wire.BlockHeader, curStamp waddrmgr.BlockStamp,
	scanning bool) error {

	// Find relevant transactions based on watch list. If scanning is
	// false, we can safely assume this block has no relevant transactions.
	var relevantTxs []*btcutil.Tx
	if len(ro.watchList) != 0 && scanning {
		// If we have a non-empty watch list, then we need to see if it
		// matches the rescan's filters, so we get the basic filter
		// from the DB or network.
		matched, err := s.blockFilterMatches(ro, &curStamp.Hash)
		if err != nil {
			return err
		}

		if matched {
			relevantTxs, err = s.extractBlockMatches(ro, &curStamp)
			if err != nil {
				return err
			}
		}
	}

	if ro.ntfn.OnFilteredBlockConnected != nil {
		ro.ntfn.OnFilteredBlockConnected(curStamp.Height, &curHeader,
			relevantTxs)
	}

	if ro.ntfn.OnBlockConnected != nil {
		ro.ntfn.OnBlockConnected(&curStamp.Hash,
			curStamp.Height, curHeader.Timestamp)
	}

	return nil
}

// extractBlockMatches fetches the target block from the network, and filters
// out any relevant transactions found within the block.
func (s *ChainService) extractBlockMatches(ro *rescanOptions,
	curStamp *waddrmgr.BlockStamp) ([]*btcutil.Tx, error) {

	// We've matched. Now we actually get the block and cycle through the
	// transactions to see which ones are relevant.
	block, err := s.GetBlock(curStamp.Hash, ro.queryOptions...)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("Couldn't get block %d (%s) from "+
			"network", curStamp.Height, curStamp.Hash)
	}

	blockHeader := block.MsgBlock().Header
	blockDetails := btcjson.BlockDetails{
		Height: block.Height(),
		Hash:   block.Hash().String(),
		Time:   blockHeader.Timestamp.Unix(),
	}

	relevantTxs := make([]*btcutil.Tx, 0, len(block.Transactions()))
	for txIdx, tx := range block.Transactions() {
		txDetails := blockDetails
		txDetails.Index = txIdx

		var relevant bool

		if ro.spendsWatchedInput(tx) {
			relevant = true
			if ro.ntfn.OnRedeemingTx != nil {
				ro.ntfn.OnRedeemingTx(tx, &txDetails)
			}
		}

		// Even though the transaction may already be known as relevant
		// and there might not be a notification callback, we need to
		// call paysWatchedAddr anyway as it updates the rescan
		// options.
		pays, err := ro.paysWatchedAddr(tx)
		if err != nil {
			return nil, err
		}

		if pays {
			relevant = true
			if ro.ntfn.OnRecvTx != nil {
				ro.ntfn.OnRecvTx(tx, &txDetails)
			}
		}

		if relevant {
			relevantTxs = append(relevantTxs, tx)
		}
	}

	return relevantTxs, nil
}

// notifyBlockWithFilter calls appropriate listeners based on the block filter.
// This differs from notifyBlock in that is expects the caller to already have
// obtained the target filter.
func (s *ChainService) notifyBlockWithFilter(ro *rescanOptions,
	curHeader *wire.BlockHeader, curStamp *waddrmgr.BlockStamp,
	filter *gcs.Filter) error {

	// Based on what we find within the block or the filter, we'll be
	// sending out a set of notifications with transactions that are
	// relevant to the rescan.
	var relevantTxs []*btcutil.Tx

	// If we actually have a filter, then we'll go ahead an attempt to
	// match the items within the filter to ensure we create any relevant
	// notifications.
	if filter != nil {
		matched, err := s.matchBlockFilter(ro, filter, &curStamp.Hash)
		if err != nil {
			return err
		}

		if matched {
			relevantTxs, err = s.extractBlockMatches(ro, curStamp)
			if err != nil {
				return err
			}
		}
	}

	if ro.ntfn.OnFilteredBlockConnected != nil {
		ro.ntfn.OnFilteredBlockConnected(curStamp.Height, curHeader,
			relevantTxs)
	}

	if ro.ntfn.OnBlockConnected != nil {
		ro.ntfn.OnBlockConnected(&curStamp.Hash,
			curStamp.Height, curHeader.Timestamp)
	}

	return nil
}

// matchBlockFilter returns whether the block filter matches the watched items.
// If this returns false, it means the block is certainly not interesting to
// us. This method differs from blockFilterMatches in that it expects the
// filter to already be obtained, rather than fetching the filter from the
// network.
func (s *ChainService) matchBlockFilter(ro *rescanOptions, filter *gcs.Filter,
	blockHash *chainhash.Hash) (bool, error) {

	// Now that we have the filter as well as the block hash of the block
	// used to construct the filter, we'll check to see if the block
	// matches any items in our watch list.
	key := builder.DeriveKey(blockHash)
	matched, err := filter.MatchAny(key, ro.watchList)
	if err != nil {
		return false, err
	}

	return matched, nil
}

// blockFilterMatches returns whether the block filter matches the watched
// items. If this returns false, it means the block is certainly not interesting
// to us.
func (s *ChainService) blockFilterMatches(ro *rescanOptions,
	blockHash *chainhash.Hash) (bool, error) {

	// TODO(roasbeef): need to ENSURE always get filter

	key := builder.DeriveKey(blockHash)
	bFilter, err := s.GetCFilter(*blockHash, wire.GCSFilterRegular)
	if err != nil {
		if err == headerfs.ErrHashNotFound {
			// Block has been reorged out from under us.
			return false, nil
		}
		return false, err
	}

	// If we found the basic filter, and the filter isn't
	// "nil", then we'll check the items in the watch list
	// against it.
	if bFilter != nil && bFilter.N() != 0 {
		// We see if any relevant transactions match.
		matched, err := bFilter.MatchAny(key, ro.watchList)
		if matched || err != nil {
			return matched, err
		}
	}

	// We don't need the extended filter, since all of the things a rescan
	// can watch for are currently added to the same watch list and
	// available in the basic filter. In the future, we can watch for
	// data pushes in input scripts (incl. P2SH and witness). In the
	// meantime, we return false if the basic filter didn't match our
	// watch list.
	return false, nil
}

// hasFilterHeadersByHeight checks whether both the basic and extended filter
// headers for a particular height are known.
func (s *ChainService) hasFilterHeadersByHeight(height uint32) bool {
	_, regFetchErr := s.RegFilterHeaders.FetchHeaderByHeight(height)
	return regFetchErr == nil
}

// updateFilter atomically updates the filter and rewinds to the specified
// height if not 0.
func (ro *rescanOptions) updateFilter(update *updateOptions,
	curStamp *waddrmgr.BlockStamp, curHeader *wire.BlockHeader) (bool, error) {

	ro.watchAddrs = append(ro.watchAddrs, update.addrs...)
	ro.watchInputs = append(ro.watchInputs, update.inputs...)

	for _, addr := range update.addrs {
		script, err := txscript.PayToAddrScript(addr)
		if err != nil {
			return false, err
		}

		ro.watchList = append(ro.watchList, script)
	}
	for _, input := range update.inputs {
		ro.watchList = append(ro.watchList, input.PkScript)
	}
	for _, txid := range update.txIDs {
		ro.watchList = append(ro.watchList, txid[:])
	}

	// If we don't need to rewind, then we can exit early.
	if update.rewind == 0 {
		return false, nil
	}

	var (
		header  *wire.BlockHeader
		height  uint32
		rewound bool
		err     error
	)

	// If we need to rewind, then we'll walk backwards in the chain until
	// we arrive at the block _just_ before the rewind.
	for curStamp.Height > int32(update.rewind) {
		if ro.ntfn.OnBlockDisconnected != nil &&
			!update.disableDisconnectedNtfns {
			ro.ntfn.OnBlockDisconnected(&curStamp.Hash,
				curStamp.Height, curHeader.Timestamp)
		}
		if ro.ntfn.OnFilteredBlockDisconnected != nil &&
			!update.disableDisconnectedNtfns {
			ro.ntfn.OnFilteredBlockDisconnected(curStamp.Height,
				curHeader)
		}

		// We just disconnected a block above, so we're now in rewind
		// mode. We set this to true here so we properly send
		// notifications even if it was just a 1 block rewind.
		rewound = true

		// Rewind and continue.
		header, height, err = ro.chain.BlockHeaders.FetchHeader(
			&curHeader.PrevBlock,
		)
		if err != nil {
			return rewound, err
		}

		*curHeader = *header
		curStamp.Height = int32(height)
		curStamp.Hash = curHeader.BlockHash()
	}

	return rewound, nil
}

// spendsWatchedInput returns whether the transaction matches the filter by
// spending a watched input.
func (ro *rescanOptions) spendsWatchedInput(tx *btcutil.Tx) bool {
	for _, in := range tx.MsgTx().TxIn {
		for _, input := range ro.watchInputs {
			switch {
			// If we're watching for a zero outpoint, then we should
			// match on the output script being spent instead.
			case input.OutPoint == zeroOutPoint:
				pkScript, err := txscript.ComputePkScript(
					in.SignatureScript, in.Witness,
				)
				if err != nil {
					continue
				}

				if bytes.Equal(pkScript.Script(), input.PkScript) {
					return true
				}

			// Otherwise, we'll match on the outpoint being spent.
			case in.PreviousOutPoint == input.OutPoint:
				return true
			}
		}
	}
	return false
}

// paysWatchedAddr returns whether the transaction matches the filter by having
// an output paying to a watched address. If that is the case, this also
// updates the filter to watch the newly created output going forward.
func (ro *rescanOptions) paysWatchedAddr(tx *btcutil.Tx) (bool, error) {
	anyMatchingOutputs := false

txOutLoop:
	for outIdx, out := range tx.MsgTx().TxOut {
		pkScript := out.PkScript

		for _, addr := range ro.watchAddrs {
			// We'll convert the address into its matching pkScript
			// to in order to check for a match.
			addrScript, err := txscript.PayToAddrScript(addr)
			if err != nil {
				return false, err
			}

			// If the script doesn't match, we'll move onto the
			// next one.
			if !bytes.Equal(pkScript, addrScript) {
				continue
			}

			// At this state, we have a matching output so we'll
			// mark this transaction as matching.
			anyMatchingOutputs = true

			// Update the filter by also watching this created
			// outpoint for the event in the future that it's
			// spent.
			hash := tx.Hash()
			outPoint := wire.OutPoint{
				Hash:  *hash,
				Index: uint32(outIdx),
			}
			ro.watchInputs = append(ro.watchInputs, InputWithScript{
				PkScript: pkScript,
				OutPoint: outPoint,
			})
			ro.watchList = append(ro.watchList, pkScript)

			continue txOutLoop
		}
	}

	return anyMatchingOutputs, nil
}

// Rescan is an object that represents a long-running rescan/notification
// client with updateable filters. It's meant to be close to a drop-in
// replacement for the btcd rescan and notification functionality used in
// wallets. It only contains information about whether a goroutine is running.
type Rescan struct {
	started uint32

	running    chan struct{}
	updateChan chan *updateOptions

	options []RescanOption

	chain *ChainService

	errMtx sync.Mutex
	err    error

	wg sync.WaitGroup
}

// NewRescan returns a rescan object that runs in another goroutine and has an
// updatable filter. It returns the long-running rescan object, and a channel
// which returns any error on termination of the rescan process.
func (s *ChainService) NewRescan(options ...RescanOption) *Rescan {
	return &Rescan{
		running:    make(chan struct{}),
		options:    options,
		updateChan: make(chan *updateOptions),
		chain:      s,
	}
}

// WaitForShutdown waits until all goroutines associated with the rescan have
// exited. This method is to be called once the passed quitchan (if any) has
// been closed.
func (r *Rescan) WaitForShutdown() {
	r.wg.Wait()
}

// Start kicks off the rescan goroutine, which will begin to scan the chain
// according to the specified rescan options.
func (r *Rescan) Start() <-chan error {
	errChan := make(chan error, 1)

	if !atomic.CompareAndSwapUint32(&r.started, 0, 1) {
		errChan <- fmt.Errorf("Rescan already started")
		return errChan
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()

		rescanArgs := append(r.options, updateChan(r.updateChan))
		err := r.chain.rescan(rescanArgs...)

		close(r.running)

		r.errMtx.Lock()
		r.err = err
		r.errMtx.Unlock()

		errChan <- err
	}()

	return errChan
}

// updateOptions are a set of functional parameters for Update.
type updateOptions struct {
	addrs                    []btcutil.Address
	inputs                   []InputWithScript
	txIDs                    []chainhash.Hash
	rewind                   uint32
	disableDisconnectedNtfns bool
}

// UpdateOption is a functional option argument for the Rescan.Update method.
type UpdateOption func(uo *updateOptions)

func defaultUpdateOptions() *updateOptions {
	return &updateOptions{}
}

// AddAddrs adds addresses to the filter.
func AddAddrs(addrs ...btcutil.Address) UpdateOption {
	return func(uo *updateOptions) {
		uo.addrs = append(uo.addrs, addrs...)
	}
}

// AddInputs adds inputs to watch to the filter.
func AddInputs(inputs ...InputWithScript) UpdateOption {
	return func(uo *updateOptions) {
		uo.inputs = append(uo.inputs, inputs...)
	}
}

// Rewind rewinds the rescan to the specified height (meaning, disconnects down
// to the block immediately after the specified height) and restarts it from
// that point with the (possibly) newly expanded filter. Especially useful when
// called in the same Update() as one of the previous three options.
func Rewind(height uint32) UpdateOption {
	return func(uo *updateOptions) {
		uo.rewind = height
	}
}

// DisableDisconnectedNtfns tells the rescan not to send `OnBlockDisconnected`
// and `OnFilteredBlockDisconnected` notifications when rewinding.
func DisableDisconnectedNtfns(disabled bool) UpdateOption {
	return func(uo *updateOptions) {
		uo.disableDisconnectedNtfns = disabled
	}
}

// Update sends an update to a long-running rescan/notification goroutine.
func (r *Rescan) Update(options ...UpdateOption) error {

	ro := defaultRescanOptions()
	for _, option := range r.options {
		option(ro)
	}

	uo := defaultUpdateOptions()
	for _, option := range options {
		option(uo)
	}

	select {
	case r.updateChan <- uo:
	case <-ro.quit:
		return ErrRescanExit

	case <-r.running:
		errStr := "Rescan is already done and cannot be updated."
		r.errMtx.Lock()
		if r.err != nil {
			errStr += fmt.Sprintf(" It returned error: %s", r.err)
		}
		r.errMtx.Unlock()
		return fmt.Errorf(errStr)
	}

	return nil
}

// SpendReport is a struct which describes the current spentness state of a
// particular output. In the case that an output is spent, then the spending
// transaction and related details will be populated. Otherwise, only the
// target unspent output in the chain will be returned.
type SpendReport struct {
	// SpendingTx is the transaction that spent the output that a spend
	// report was requested for.
	//
	// NOTE: This field will only be populated if the target output has
	// been spent.
	SpendingTx *wire.MsgTx

	// SpendingTxIndex is the input index of the transaction above which
	// spends the target output.
	//
	// NOTE: This field will only be populated if the target output has
	// been spent.
	SpendingInputIndex uint32

	// SpendingTxHeight is the hight of the block that included the
	// transaction  above which spent the target output.
	//
	// NOTE: This field will only be populated if the target output has
	// been spent.
	SpendingTxHeight uint32

	// Output is the raw output of the target outpoint.
	//
	// NOTE: This field will only be populated if the target is still
	// unspent.
	Output *wire.TxOut
}

// GetUtxo gets the appropriate TxOut or errors if it's spent. The option
// WatchOutPoints (with a single outpoint) is required. StartBlock can be used
// to give a hint about which block the transaction is in, and TxIdx can be
// used to give a hint of which transaction in the block matches it (coinbase
// is 0, first normal transaction is 1, etc.).
//
// TODO(roasbeef): WTB utxo-commitments
func (s *ChainService) GetUtxo(options ...RescanOption) (*SpendReport, error) {
	// Before we start we'll fetch the set of default options, and apply
	// any user specified options in a functional manner.
	ro := defaultRescanOptions()
	ro.startBlock = &waddrmgr.BlockStamp{
		Hash:   *s.chainParams.GenesisHash,
		Height: 0,
	}
	for _, option := range options {
		option(ro)
	}

	// As this is meant to fetch UTXO's, the options MUST specify exactly
	// one outpoint.
	if len(ro.watchInputs) != 1 {
		return nil, fmt.Errorf("must pass exactly one OutPoint")
	}

	req, err := s.utxoScanner.Enqueue(
		&ro.watchInputs[0], uint32(ro.startBlock.Height),
	)
	if err != nil {
		return nil, err
	}

	// Wait for the result to be delivered by the rescan or until a shutdown
	// is signaled.
	report, err := req.Result(ro.quit)
	if err != nil {
		log.Debugf("Error finding spends for %s: %v",
			ro.watchInputs[0].OutPoint.String(), err)
		return nil, err
	}

	return report, nil
}

// getReorgTip gets a block header from the chain service's cache. This is only
// required until the block subscription API is factored out into its own
// package.
//
// TODO(aakselrod): Get rid of this as described above.
func (s *ChainService) getReorgTip(hash chainhash.Hash) *wire.BlockHeader {
	s.mtxReorgHeader.RLock()
	defer s.mtxReorgHeader.RUnlock()
	return s.reorgedBlockHeaders[hash]
}
