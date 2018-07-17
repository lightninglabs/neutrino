// NOTE: THIS API IS UNSTABLE RIGHT NOW.

package neutrino

import (
	"bytes"
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
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/btcsuite/btcwallet/waddrmgr"
	"github.com/lightninglabs/neutrino/headerfs"
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

// Rescan is a single-threaded function that uses headers from the database and
// functional options as arguments.
func (s *ChainService) Rescan(options ...RescanOption) error {

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
			_, height, err := s.BlockHeaders.FetchHeader(&ro.endBlock.Hash)
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
	if ro.startBlock == nil {
		bs, err := s.BestSnapshot()
		if err != nil {
			return err
		}
		ro.startBlock = bs
	}
	curStamp = *ro.startBlock

	// To find our starting block, either the start hash should be set, or
	// the start height should be set. If neither is, then we'll be
	// starting from the gensis block.
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

	log.Tracef("Starting rescan from known block %d (%s)", curStamp.Height,
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
	var subscription *blockSubscription

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

		// If we're current, we wait for notifications.
		switch current {
		case true:
			// Wait for a signal that we have a newly connected
			// header and cfheader, or a newly disconnected header;
			// alternatively, forward ourselves to the next block
			// if possible.
			select {

			case <-ro.quit:
				return nil

			// An update mesage has just come across, if it points
			// to a prior point in the chain, then we may need to
			// rewind a bit in order to provide the client all its
			// requested client.
			case update := <-ro.update:
				rewound, err := ro.updateFilter(update, &curStamp,
					&curHeader)
				if err != nil {
					return err
				}

				if rewound {
					log.Tracef("Rewound to block %d (%s), no longer current",
						curStamp.Height, curStamp.Hash)

					current = false
					s.unsubscribeBlockMsgs(subscription)
					subscription = nil
				}

			case header := <-blockConnected:
				// Only deal with the next block from what we
				// know about. Otherwise, it's in the future.
				if header.PrevBlock != curStamp.Hash {
					log.Debugf("Rescan got out of order block %s with "+
						"prevblock %s", header.BlockHash(), header.PrevBlock)
					continue rescanLoop
				}

				// Do not process block until we have all filter headers. Don't
				// worry, the block will get requeued every time there is a new
				// filter available.
				if !s.hasFilterHeadersByHeight(uint32(curStamp.Height + 1)) {
					continue rescanLoop
				}

				curHeader = header
				curStamp.Hash = header.BlockHash()
				curStamp.Height++
				log.Tracef("Rescan got block %d (%s)", curStamp.Height, curStamp.Hash)

				if !scanning {
					scanning = ro.startTime.Before(curHeader.Timestamp)
				}
				err := s.notifyBlock(ro, &curHeader, &curStamp, scanning)
				if err != nil {
					return err
				}

			case header := <-blockDisconnected:
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
					header := s.getReorgTip(
						header.PrevBlock)
					curHeader = *header
					curStamp.Hash = header.BlockHash()
					curStamp.Height--
				}
			}
		case false:

			// Apply all queued filter updates.
		updateFilterLoop:
			for {
				select {
				case update := <-ro.update:
					_, err := ro.updateFilter(update, &curStamp, &curHeader)
					if err != nil {
						return err
					}

				default:
					break updateFilterLoop
				}
			}

			// Since we're not current, we try to manually advance the block. We
			// are only interested in blocks that we already have both filter
			// headers for. If we fail, we mark outselves as current and follow
			// notifications.
			nextHeight := uint32(curStamp.Height + 1)
			if !s.hasFilterHeadersByHeight(nextHeight) {
				log.Tracef("Rescan became current at %d (%s), "+
					"subscribing to block notifications",
					curStamp.Height, curStamp.Hash)
				current = true
				// Subscribe to block notifications.
				subscription = s.subscribeBlockMsg(blockConnected,
					blockConnected, blockDisconnected, nil)
				defer func() {
					if subscription != nil {
						s.unsubscribeBlockMsgs(subscription)
						subscription = nil
					}
				}()
				continue rescanLoop
			}

			header, err := s.BlockHeaders.FetchHeaderByHeight(nextHeight)
			if err != nil {
				return err
			}

			curHeader = *header
			curStamp.Height++
			curStamp.Hash = header.BlockHash()

			if !scanning {
				scanning = ro.startTime.Before(curHeader.Timestamp)
			}
			err = s.notifyBlock(ro, &curHeader, &curStamp, scanning)
			if err != nil {
				return err
			}
		}
	}
}

// notifyBlock calls appropriate listeners based on the block filter.
func (s *ChainService) notifyBlock(ro *rescanOptions,
	curHeader *wire.BlockHeader, curStamp *waddrmgr.BlockStamp,
	scanning bool) error {

	// Find relevant transactions based on watch list. If scanning is false,
	// we can safely assume this block has no relevant transactions.
	var relevantTxs []*btcutil.Tx
	if len(ro.watchList) != 0 && scanning {
		// If we have a non-empty watch list, then we need to
		// see if it matches the rescan's filters, so we get
		// the basic filter from the DB or network.
		matched, err := s.blockFilterMatches(ro, &curStamp.Hash)
		if err != nil {
			return err
		}

		if matched {
			// We've matched. Now we actually get the block and
			// cycle through the transactions to see which ones are
			// relevant.
			block, err := s.GetBlockFromNetwork(curStamp.Hash,
				ro.queryOptions...)
			if err != nil {
				return err
			}
			if block == nil {
				return fmt.Errorf("Couldn't get block %d "+
					"(%s) from network", curStamp.Height,
					curStamp.Hash)
			}

			blockHeader := block.MsgBlock().Header
			blockDetails := btcjson.BlockDetails{
				Height: block.Height(),
				Hash:   block.Hash().String(),
				Time:   blockHeader.Timestamp.Unix(),
			}

			relevantTxs = make([]*btcutil.Tx, 0, len(block.Transactions()))
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

				// Even though the transaction may already be
				// known as relevant and there might not be a
				// notification callback, we need to call
				// paysWatchedAddr anyway as it updates the
				// rescan options.
				pays, err := ro.paysWatchedAddr(tx)
				if err != nil {
					return err
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

// blockFilterMatches returns whether the block filter matches the watched
// items. If this returns false, it means the block is certainly not interesting
// to us.
func (s *ChainService) blockFilterMatches(ro *rescanOptions,
	blockHash *chainhash.Hash) (bool, error) {

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
			if in.PreviousOutPoint == input.OutPoint {
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
	running    uint32
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
		running:    1,
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

	r.wg.Add(1)
	go func() {
		rescanArgs := append(r.options, updateChan(r.updateChan))
		err := r.chain.Rescan(rescanArgs...)

		r.wg.Done()
		atomic.StoreUint32(&r.running, 0)

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
	running := atomic.LoadUint32(&r.running)
	if running != 1 {
		errStr := "Rescan is already done and cannot be updated."
		r.errMtx.Lock()
		if r.err != nil {
			errStr += fmt.Sprintf(" It returned error: %s", r.err)
		}
		r.errMtx.Unlock()
		return fmt.Errorf(errStr)
	}
	uo := defaultUpdateOptions()
	for _, option := range options {
		option(uo)
	}
	r.updateChan <- uo
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

	// As this is meant to fetch UTXO's, the options MUST specify at least
	// a single input.
	if len(ro.watchInputs) != 1 {
		return nil, fmt.Errorf("must pass exactly one InputWithScript")
	}
	watchList := [][]byte{ro.watchInputs[0].PkScript}

	originTxID := ro.watchInputs[0].OutPoint.Hash

	// Track our position in the chain.
	curHeader, curHeight, err := s.BlockHeaders.ChainTip()
	if err != nil {
		return nil, err
	}

	curStamp := &waddrmgr.BlockStamp{
		Hash:   curHeader.BlockHash(),
		Height: int32(curHeight),
	}

	// Find our earliest possible block, which may be a hash.
	if (ro.startBlock.Hash != chainhash.Hash{}) {
		_, height, err := s.BlockHeaders.FetchHeader(&ro.startBlock.Hash)
		if err == nil {
			ro.startBlock.Height = int32(height)
		} else {
			ro.startBlock.Hash = chainhash.Hash{}
		}
	}

	// Alternatively, our earliest possible block by actually be a height.
	if (ro.startBlock.Hash == chainhash.Hash{}) {
		if ro.startBlock.Height == 0 {
			ro.startBlock.Hash = *s.chainParams.GenesisHash
		} else if ro.startBlock.Height < int32(curHeight) {
			header, err := s.BlockHeaders.FetchHeaderByHeight(
				uint32(ro.startBlock.Height))
			if err == nil {
				ro.startBlock.Hash = header.BlockHash()
			} else {
				ro.startBlock.Hash = *s.chainParams.GenesisHash
				ro.startBlock.Height = 0
			}
		} else {
			ro.startBlock.Height = int32(curHeight)
			ro.startBlock.Hash = curHeader.BlockHash()
		}
	}

	log.Tracef("Starting scan for output spend from known block %d (%s) "+
		"back to block %d (%s)", curStamp.Height, curStamp.Hash,
		ro.startBlock.Height, ro.startBlock.Hash)

	for {
		// Check the basic filter for the spend and the extended filter
		// for the transaction in which the outpoint is funded.
		filter, err := s.GetCFilter(
			curStamp.Hash, wire.GCSFilterRegular, ro.queryOptions...,
		)
		if err != nil {
			return nil, fmt.Errorf("Couldn't get basic "+
				"filter for block %d (%s)", curStamp.Height,
				curStamp.Hash)
		}
		matched := false
		if filter != nil {
			filterKey := builder.DeriveKey(&curStamp.Hash)
			matched, err = filter.MatchAny(filterKey, watchList)
		}
		if err != nil {
			return nil, err
		}

		// If either is matched, download the block and check to see
		// what we have.
		if matched {
			block, err := s.GetBlockFromNetwork(
				curStamp.Hash, ro.queryOptions...,
			)
			if err != nil {
				return nil, err
			}
			if block == nil {
				return nil, fmt.Errorf("Couldn't get "+
					"block %d (%s)", curStamp.Height,
					curStamp.Hash)
			}

			// If we've spent the output in this block, return an
			// error stating that the output is spent.
			for _, tx := range block.Transactions() {
				for i, ti := range tx.MsgTx().TxIn {
					if ti.PreviousOutPoint == ro.watchInputs[0].OutPoint {
						return &SpendReport{
							SpendingTx:         tx.MsgTx(),
							SpendingInputIndex: uint32(i),
							SpendingTxHeight:   uint32(curStamp.Height),
						}, nil
					}
				}
			}

			// If we found the transaction that created the output,
			// then it's not spent and we can return the TxOut.
			for _, tx := range block.Transactions() {
				if *(tx.Hash()) == originTxID {
					outputs := tx.MsgTx().TxOut
					outputIndex := ro.watchInputs[0].OutPoint.Index
					targetOutput := outputs[outputIndex]

					return &SpendReport{
						Output: targetOutput,
					}, nil
				}
			}
		}

		// Otherwise, iterate backwards until we've gone too far.
		curStamp.Height--
		if curStamp.Height < ro.startBlock.Height {
			return nil, fmt.Errorf("Transaction %s not found "+
				"since start block %d (%s)",
				originTxID, curStamp.Height+1,
				curStamp.Hash)
		}

		// Fetch the previous header so we can continue our walk
		// backwards.
		header, err := s.BlockHeaders.FetchHeaderByHeight(
			uint32(curStamp.Height),
		)
		if err != nil {
			return nil, err
		}
		curStamp.Hash = header.BlockHash()
	}
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
