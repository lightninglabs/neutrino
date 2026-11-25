// NOTE: THIS API IS UNSTABLE RIGHT NOW.

package neutrino

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/filtermatch"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/rescan"
)

var (
	// ErrRescanExit is an error returned to the caller in case the ongoing
	// rescan exits.
	ErrRescanExit = errors.New("rescan exited")
)

// ScanSource supplies the headers, blocks and notifications for a rescan.
// NewRescanWithMatcher uses these operations with a caller-supplied matcher.
type ScanSource interface {
	// ChainParams returns the parameters of the current chain.
	ChainParams() chaincfg.Params

	// BestBlock retrieves the most recent block's height and hash where we
	// can scan. A basic-filter source has both headers ready at this height.
	BestBlock() (*headerfs.BlockStamp, error)

	// GetBlockHeaderByHeight returns the header of the block with the given
	// height.
	GetBlockHeaderByHeight(uint32) (*wire.BlockHeader, error)

	// GetBlockHeader returns the header of the block with the given hash.
	GetBlockHeader(*chainhash.Hash) (*wire.BlockHeader, uint32, error)

	// GetBlock returns the block with the given hash.
	GetBlock(chainhash.Hash, ...QueryOption) (*btcutil.Block, error)

	// Subscribe returns a block subscription that delivers block
	// notifications in order. The bestHeight parameter can be used to
	// signal that a backlog of notifications should be delivered from this
	// height. When providing a bestHeight of 0, a backlog will not be
	// delivered.
	//
	// TODO(wilmer): extend with best hash as well.
	Subscribe(bestHeight uint32) (*blockntfns.Subscription, error)

	// IsCurrent returns true if the backend chain thinks that its view of
	// the network is current.
	IsCurrent() bool
}

// ChainSource supplies filters of one representation in addition to chain
// data. F is chosen by the provider. The BIP158 source uses *gcs.Filter.
// A matcher that obtains its evidence elsewhere needs only ScanSource.
type ChainSource[F any] interface {
	ScanSource

	// GetCFilter retrieves the requested filter for the given block.
	GetCFilter(chainhash.Hash, wire.FilterType, ...QueryOption) (F, error)
}

// BasicChainSource supplies the BIP158 filters used by NewRescan.
type BasicChainSource = ChainSource[*gcs.Filter]

// ScanProgressHandler is used in rescanOptions to update the caller with the
// rescan progress.
type ScanProgressHandler func(lastProcessedBlock uint32)

// rescanOptions holds the set of functional parameters for Rescan.
type rescanOptions struct {
	queryOptions []QueryOption

	ntfn            rpcclient.NotificationHandlers
	progressHandler ScanProgressHandler

	startTime  time.Time
	startBlock *headerfs.BlockStamp

	endBlock *headerfs.BlockStamp

	watchAddrs  []address.Address
	watchInputs []InputWithScript

	// watchList holds the filter entries the utxo scanner matches.
	watchList [][]byte
	txIdx     uint32

	quit <-chan struct{}
}

// RescanOption is a functional option argument to any of the rescan and
// notification subscription methods. These are always processed in order, with
// later options overriding earlier ones.
type RescanOption func(ro *rescanOptions)

func defaultRescanOptions() *rescanOptions {
	return &rescanOptions{}
}

// QueryOptions pass onto the rescan's block fetches. Filter fetches use the
// rescan's own options, as they always have.
func QueryOptions(options ...QueryOption) RescanOption {
	return func(ro *rescanOptions) {
		ro.queryOptions = options
	}
}

// NotificationHandlers specifies notification handlers for the rescan. The
// rescan calls OnFilteredBlockConnected, OnBlockConnected,
// OnFilteredBlockDisconnected, OnBlockDisconnected, OnRedeemingTx and
// OnRecvTx, and no other handler. They all run on one goroutine, one at a
// time, in chain order, and must not call back into the rescan, so a handler
// must not call Update.
func NotificationHandlers(ntfn rpcclient.NotificationHandlers) RescanOption {
	return func(ro *rescanOptions) {
		ro.ntfn = ntfn
	}
}

// ProgressHandler specifies a handler to be used when the utxo
// scanner reports its progress.
// The passed handler should be non-blocking for the rescan to continue
// normally.
func ProgressHandler(
	handler ScanProgressHandler) RescanOption {

	return func(ro *rescanOptions) {
		ro.progressHandler = handler
	}
}

// StartBlock specifies the start block. The hash is checked first; if there's
// no such hash (zero hash avoids lookup), the height is checked next, and if
// the height is 0 or isn't known either, the rescan starts from the genesis
// block. If StartBlock isn't given at all, the rescan starts from the current
// best block, so it only reports blocks connected from then on. This block is
// assumed to already be known, and no notifications will be sent for this
// block. The rescan uses the latter of StartBlock and StartTime.
func StartBlock(startBlock *headerfs.BlockStamp) RescanOption {
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
func EndBlock(endBlock *headerfs.BlockStamp) RescanOption {
	return func(ro *rescanOptions) {
		ro.endBlock = endBlock
	}
}

// WatchAddrs specifies the addresses to watch/filter for. Each call to this
// function adds to the list of addresses being watched rather than replacing
// the list. Each time a transaction spends to the specified address, the
// outpoint is added to the WatchOutPoints list.
func WatchAddrs(watchAddrs ...address.Address) RescanOption {
	return func(ro *rescanOptions) {
		ro.watchAddrs = append(ro.watchAddrs, watchAddrs...)
	}
}

// InputWithScript couples an previous outpoint along with its input script.
// We'll use the prev script to match the filter itself, but then scan for the
// particular outpoint when we need to make a notification decision.
//
// It has the same fields as filtermatch.InputWithScript, which the rescan
// actor matches with, and converts to it field for field.
type InputWithScript struct {
	// OutPoint identifies the previous output to watch.
	OutPoint wire.OutPoint

	// PkScript is the script of the previous output.
	PkScript []byte
}

// matchInputs converts inputs to the type the rescan actor matches with.
func matchInputs(inputs []InputWithScript) []filtermatch.InputWithScript {
	if inputs == nil {
		return nil
	}

	converted := make([]filtermatch.InputWithScript, len(inputs))
	for i, input := range inputs {
		converted[i] = filtermatch.InputWithScript(input)
	}

	return converted
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

// newRescanChain adapts chain to the rescan actor's ScanChain.
func newRescanChain(chain ScanSource,
	queryOptions []QueryOption) *rescanChain {

	return &rescanChain{src: chain, queryOptions: queryOptions}
}

// rescanChain maps actor block fetches onto cancellable queries with the
// caller's block query options.
type rescanChain struct {
	src          ScanSource
	queryOptions []QueryOption
}

// A compile-time check that rescanChain implements rescan.ScanChain.
var _ rescan.ScanChain = (*rescanChain)(nil)

// shutdownErr returns ErrShuttingDown in place of err once the chain is a
// ChainService that has begun to stop, so a header read that fails because
// the service closed its stores ends the rescan with ErrShuttingDown, as a
// fetch does, rather than with the store's error.
func (c *rescanChain) shutdownErr(err error) error {
	chainSource, ok := c.src.(*RescanChainSource)
	if err != nil && ok && atomic.LoadInt32(&chainSource.shutdown) != 0 {
		return ErrShuttingDown
	}

	return err
}

// BestBlock is part of the rescan.ScanChain interface.
func (c *rescanChain) BestBlock() (*headerfs.BlockStamp, error) {
	best, err := c.src.BestBlock()
	return best, c.shutdownErr(err)
}

// GetBlockHeaderByHeight is part of the rescan.ScanChain interface.
func (c *rescanChain) GetBlockHeaderByHeight(
	height uint32) (*wire.BlockHeader, error) {

	header, err := c.src.GetBlockHeaderByHeight(height)
	return header, c.shutdownErr(err)
}

// GetBlockHeader is part of the rescan.ScanChain interface.
func (c *rescanChain) GetBlockHeader(
	hash *chainhash.Hash) (*wire.BlockHeader, uint32, error) {

	header, height, err := c.src.GetBlockHeader(hash)
	return header, height, c.shutdownErr(err)
}

// GetBlock is part of the rescan.ScanChain interface.
func (c *rescanChain) GetBlock(ctx context.Context,
	hash chainhash.Hash) (*btcutil.Block, error) {

	options := append(
		append([]QueryOption(nil), c.queryOptions...),
		CancelQuery(ctx.Done()),
	)

	return c.src.GetBlock(hash, options...)
}

// basicMatcher maps the scan's prefetch hint and cancellation onto a
// regular compact filter query. Block query options remain independent.
func basicMatcher(getFilter func(chainhash.Hash, wire.FilterType,
	...QueryOption) (*gcs.Filter, error)) filtermatch.BlockMatcher {

	fetch := func(ctx context.Context, hash chainhash.Hash, batch bool) (
		*gcs.Filter, error) {

		option := NumRetries(2)
		if batch {
			option = OptimisticBatch()
		}
		return getFilter(
			hash, wire.GCSFilterRegular, option, CancelQuery(ctx.Done()),
		)
	}
	return filtermatch.NewBlockMatcher(filtermatch.BasicFilterSource(fetch))
}

// Subscribe is part of the rescan.ScanChain interface.
func (c *rescanChain) Subscribe(
	bestHeight uint32) (*blockntfns.Subscription, error) {

	return c.src.Subscribe(bestHeight)
}

// IsCurrent is part of the rescan.ScanChain interface.
func (c *rescanChain) IsCurrent() bool {
	return c.src.IsCurrent()
}

// MarkAsConfirmed is part of the rescan.ScanChain interface. Like the
// legacy rescan loop, it tells the broadcaster only when the chain is a
// ChainService's RescanChainSource.
func (c *rescanChain) MarkAsConfirmed(txHash chainhash.Hash) {
	if chainSource, ok := c.src.(*RescanChainSource); ok {
		chainSource.broadcaster.MarkAsConfirmed(txHash)
	}
}

// newRescanConfig applies options and resolves the start and end blocks
// against chain exactly as the legacy rescan loop did, and returns the
// resulting actor configuration and the options it was built from.
func newRescanConfig(chain ScanSource, matcher filtermatch.BlockMatcher,
	options ...RescanOption) (*rescan.ActorConfig, *rescanOptions, error) {

	// First, we'll apply the set of default options, then serially apply
	// all the options that've been passed in.
	ro := defaultRescanOptions()
	ro.endBlock = &headerfs.BlockStamp{
		Hash:   chainhash.Hash{},
		Height: 0,
	}
	for _, option := range options {
		option(ro)
	}

	// Every watched address must have a payment script.
	for _, addr := range ro.watchAddrs {
		if _, err := txscript.PayToAddrScript(addr); err != nil {
			return nil, nil, err
		}
	}

	// Check that we have either an end block or a quit channel.
	if ro.endBlock != nil {
		// If the end block hash is non-nil, then we'll query the
		// database to find out the stop height.
		if (ro.endBlock.Hash != chainhash.Hash{}) {
			_, height, err := chain.GetBlockHeader(
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
				header, err := chain.GetBlockHeaderByHeight(
					uint32(ro.endBlock.Height),
				)
				if err == nil {
					ro.endBlock.Hash = header.BlockHash()
				} else {
					ro.endBlock = &headerfs.BlockStamp{}
				}
			}
		}
	} else {
		ro.endBlock = &headerfs.BlockStamp{}
	}

	// If we don't have a quit channel, and the end height is still
	// unspecified, then we'll exit out here.
	if ro.quit == nil && ro.endBlock.Height == 0 {
		return nil, nil, fmt.Errorf("rescan request must specify a " +
			"quit channel or valid end block")
	}

	// If no start block is specified, start the scan from our current best
	// block.
	if ro.startBlock == nil {
		bs, err := chain.BestBlock()
		if err != nil {
			return nil, nil, err
		}
		ro.startBlock = bs
	}
	curStamp := *ro.startBlock
	var curHeader wire.BlockHeader

	// To find our starting block, either the start hash should be set, or
	// the start height should be set. If neither is, then we'll be
	// starting from the genesis block.
	if (curStamp.Hash != chainhash.Hash{}) {
		header, height, err := chain.GetBlockHeader(&curStamp.Hash)
		if err == nil {
			curHeader = *header
			curStamp.Height = int32(height)
		} else {
			curStamp.Hash = chainhash.Hash{}
		}
	}
	if (curStamp.Hash == chainhash.Hash{}) {
		var header *wire.BlockHeader
		if curStamp.Height != 0 {
			header, _ = chain.GetBlockHeaderByHeight(
				uint32(curStamp.Height),
			)
		}

		if header != nil {
			curHeader = *header
			curStamp.Hash = curHeader.BlockHash()
		} else {
			chainParams := chain.ChainParams()
			curHeader = chainParams.GenesisBlock.Header
			curStamp.Hash = *chainParams.GenesisHash
			curStamp.Height = 0
		}
	}

	return &rescan.ActorConfig{
		BlockMatcher: matcher,
		Chain:        newRescanChain(chain, ro.queryOptions),
		StartStamp:   curStamp,
		StartHeader:  curHeader,
		StartTime:    ro.startTime,
		WatchAddrs:   ro.watchAddrs,
		WatchInputs:  matchInputs(ro.watchInputs),
		EndBlock:     *ro.endBlock,

		// The legacy rescan retried a block it could not fetch after
		// 100ms, and ended with ErrShuttingDown once the service
		// stopped.
		RetryInterval: legacyRetryInterval,
		ErrShutdown:   ErrShuttingDown,

		Callbacks: legacyCallbacks(ro.ntfn),
	}, ro, nil
}

// legacyCallbacks maps the notification handlers the legacy rescan loop
// called onto the actor's callbacks, which have the same signatures and run in
// the same order. The rescan never called the other handlers, so it still
// does not.
func legacyCallbacks(ntfn rpcclient.NotificationHandlers) rescan.Callbacks {
	// The deprecated handlers are the ones wallets still set.
	//
	//nolint:staticcheck
	return rescan.Callbacks{
		OnFilteredBlockConnected:    ntfn.OnFilteredBlockConnected,
		OnBlockConnected:            ntfn.OnBlockConnected,
		OnFilteredBlockDisconnected: ntfn.OnFilteredBlockDisconnected,
		OnBlockDisconnected:         ntfn.OnBlockDisconnected,
		OnRedeemingTx:               ntfn.OnRedeemingTx,
		OnRecvTx:                    ntfn.OnRecvTx,
	}
}

// legacyRetryInterval is how long the rescan waits before it retries a block
// whose filter or body it could not fetch.
const legacyRetryInterval = 100 * time.Millisecond

// Rescan is an object that represents a long-running rescan/notification
// client with updateable filters. It's meant to be close to a drop-in
// replacement for the btcd rescan and notification functionality used in
// wallets.
//
// A Rescan runs on a rescan.RescanActor. Start resolves its options and
// starts the actor, Update sends the actor a watch-list update and waits for
// it to apply, and the rescan ends when the quit channel closes, when it
// reaches its end block, or when its block subscription fails because the
// ChainService stopped.
type Rescan struct { // nolint:maligned
	started uint32 // To be used atomically.

	// running is closed once the rescan has ended and its actor has shut
	// down. err holds why it ended.
	running chan struct{}

	// ready is closed once Start has created the actor, which is set in
	// actor before.
	ready chan struct{}
	actor *rescan.RescanActor

	options []RescanOption

	chain   ScanSource
	matcher filtermatch.BlockMatcher

	errMtx sync.Mutex
	err    error

	wg sync.WaitGroup
}

// NewRescan returns a wallet rescan backed by chain's compact filters.
func NewRescan(chain BasicChainSource, options ...RescanOption) *Rescan {
	var matcher filtermatch.BlockMatcher
	if src, ok := chain.(interface {
		BlockMatcher() filtermatch.BlockMatcher
	}); ok {
		matcher = src.BlockMatcher()
	}
	if matcher == nil {
		matcher = basicMatcher(chain.GetCFilter)
	}
	return newRescan(chain, matcher, options...)
}

// NewRescanWithMatcher scans chain with the supplied backend. F ties the
// backend's candidates to their verifier. Chain access does not depend on F.
func NewRescanWithMatcher[F any](chain ScanSource, backend filtermatch.Backend[F],
	options ...RescanOption) *Rescan {

	return newRescan(chain, filtermatch.NewBlockMatcher(backend), options...)
}

func newRescan(chain ScanSource, matcher filtermatch.BlockMatcher,
	options ...RescanOption) *Rescan {

	return &Rescan{
		running: make(chan struct{}),
		ready:   make(chan struct{}),
		options: options,
		chain:   chain,
		matcher: matcher,
	}
}

// WaitForShutdown waits until all goroutines associated with the rescan have
// exited. This method is to be called once the passed quitchan (if any) has
// been closed. Once it returns, no notification handler runs again.
func (r *Rescan) WaitForShutdown() {
	r.wg.Wait()
}

// Start kicks off the rescan goroutine, which will begin to scan the chain
// according to the specified rescan options. The returned channel receives
// exactly one value once the rescan has ended: nil if it reached its end
// block, ErrRescanExit if the quit channel closed, and otherwise the error
// that ended it.
func (r *Rescan) Start() <-chan error {
	errChan := make(chan error, 1)

	if !atomic.CompareAndSwapUint32(&r.started, 0, 1) {
		errChan <- fmt.Errorf("rescan already started")
		return errChan
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()

		err := r.run()

		r.errMtx.Lock()
		r.err = err
		r.errMtx.Unlock()

		close(r.running)

		errChan <- err
	}()

	return errChan
}

// run creates and starts the rescan actor, waits for the rescan to end, and
// shuts the actor down. It returns why the rescan ended.
func (r *Rescan) run() error {
	cfg, ro, err := newRescanConfig(r.chain, r.matcher, r.options...)
	if err != nil {
		return err
	}

	actor, err := rescan.NewRescanActor(*cfg)
	if err != nil {
		return err
	}
	r.actor = actor
	close(r.ready)

	actor.Start()

	select {
	case <-ro.quit:
		err = ErrRescanExit

	case <-actor.Done():
		err = actor.Err()

		// The legacy rescan ended with the header store's own error.
		var storeErr *rescan.HeaderStoreError
		if errors.As(err, &storeErr) {
			err = storeErr.Err
		}
	}

	actor.Stop()
	actor.WaitForShutdown()

	return err
}

// updateOptions are a set of functional parameters for Update.
type updateOptions struct {
	addrs                    []address.Address
	inputs                   []InputWithScript
	rewind                   uint32
	disableDisconnectedNtfns bool
}

// UpdateOption is a functional option argument for the Rescan.Update method.
type UpdateOption func(uo *updateOptions)

func defaultUpdateOptions() *updateOptions {
	return &updateOptions{}
}

// AddAddrs adds addresses to the filter.
func AddAddrs(addrs ...address.Address) UpdateOption {
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

// Update sends an update to a long-running rescan/notification goroutine. It
// returns once the rescan has applied the update, so every block notified
// after it returns is scanned with the new addresses and inputs. A rewind
// requested by the update is performed after Update returns. Update fails
// with ErrRescanExit once the quit channel has closed, and with an error once
// the rescan has ended.
func (r *Rescan) Update(options ...UpdateOption) error {
	ro := defaultRescanOptions()
	for _, option := range r.options {
		option(ro)
	}

	uo := defaultUpdateOptions()
	for _, option := range options {
		option(uo)
	}

	// Wait for Start to create the actor. The legacy rescan blocked an
	// Update sent before Start the same way.
	select {
	case <-r.ready:
	case <-ro.quit:
		return ErrRescanExit
	case <-r.running:
		return r.doneErr()
	}

	update := rescan.WatchUpdate{
		Addrs:                    uo.addrs,
		Inputs:                   matchInputs(uo.inputs),
		DisableDisconnectedNtfns: uo.disableDisconnectedNtfns,
	}

	// A zero rewind height asks for no rewind.
	if uo.rewind != 0 {
		update.RewindTo = &uo.rewind
	}

	// Like the legacy Update, wait as long as the rescan takes to read
	// the update, unless it ends first. The actor answers after at most
	// the block it is scanning.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-ro.quit:
		case <-r.running:
		case <-ctx.Done():
		}
		cancel()
	}()

	err := r.actor.AddWatchAddrs(ctx, update)
	if err == nil {
		return nil
	}

	// An update that reaches a rescan that is shutting down fails like
	// one sent after it ended.
	if errors.Is(err, rescan.ErrStopped) ||
		errors.Is(err, context.Canceled) {

		select {
		case <-ro.quit:
			return ErrRescanExit
		case <-r.running:
			return r.doneErr()
		}
	}

	return err
}

// doneErr returns the error Update reports once the rescan has ended.
func (r *Rescan) doneErr() error {
	errStr := "Rescan is already done and cannot be updated."
	r.errMtx.Lock()
	if r.err != nil {
		errStr += fmt.Sprintf(" It returned error: %s", r.err)
	}
	r.errMtx.Unlock()

	return errors.New(errStr)
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

	// SpendingTxHeight is the height of the block that included the
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

	// BlockHash is the block hash of the block that includes the unspent
	// output.
	//
	// NOTE: This field will only be populated if the target is still
	// unspent.
	BlockHash *chainhash.Hash

	// BlockHeight is the height of the block that includes the unspent output.
	//
	// NOTE: This field will only be populated if the target is still
	// unspent.
	BlockHeight uint32

	// BlockIndex is the index of the output's transaction in its block.
	//
	// NOTE: This field will only be populated if the target is still
	// unspent.
	BlockIndex uint32
}

// GetUtxo gets the appropriate TxOut or errors if it's spent. The option
// WatchOutPoints (with a single outpoint) is required. StartBlock can be used
// to give a hint about which block the transaction is in, and TxIdx can be
// used to give a hint of which transaction in the block matches it (coinbase
// is 0, first normal transaction is 1, etc.).
//
// TODO(roasbeef): WTB utxo-commitments.
func (s *ChainService) GetUtxo(options ...RescanOption) (*SpendReport, error) {
	// Before we start we'll fetch the set of default options, and apply
	// any user specified options in a functional manner.
	ro := defaultRescanOptions()
	ro.startBlock = &headerfs.BlockStamp{
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
		&ro.watchInputs[0], uint32(ro.startBlock.Height), ro.progressHandler,
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
