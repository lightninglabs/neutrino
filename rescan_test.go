package neutrino

import (
	"errors"
	"os"
	"reflect"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/gcs"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/wire"
	"github.com/davecgh/go-spew/spew"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
)

// mockChainSource is a mock implementation of the ChainSource interface that
// aims to ease testing the behavior of the Rescan struct.
type mockChainSource struct {
	ntfnChan       chan blockntfns.BlockNtfn
	filtersQueried chan chainhash.Hash

	mu                    sync.Mutex // all fields below are protected
	bestBlock             headerfs.BlockStamp
	blockHeightIndex      map[chainhash.Hash]uint32
	blockHashesByHeight   map[uint32]*chainhash.Hash
	blockHeaders          map[chainhash.Hash]*wire.BlockHeader
	blocks                map[chainhash.Hash]*btcutil.Block
	failGetFilter         bool // if true, returns nil filter in GetCFilter
	filters               map[chainhash.Hash]*gcs.Filter
	filterHeadersByHeight map[uint32]*chainhash.Hash
	subscribeOverride     func(uint32) (*blockntfns.Subscription, error)
}

var _ ChainSource = (*mockChainSource)(nil)

// newMockChainSource creates a new mock chain backed by numBlocks.
func newMockChainSource(numBlocks int) *mockChainSource {
	chain := &mockChainSource{
		ntfnChan:       make(chan blockntfns.BlockNtfn),
		filtersQueried: make(chan chainhash.Hash),

		blockHeightIndex:      make(map[chainhash.Hash]uint32),
		blockHashesByHeight:   make(map[uint32]*chainhash.Hash),
		blockHeaders:          make(map[chainhash.Hash]*wire.BlockHeader),
		blocks:                make(map[chainhash.Hash]*btcutil.Block),
		filterHeadersByHeight: make(map[uint32]*chainhash.Hash),
		filters:               make(map[chainhash.Hash]*gcs.Filter),
	}

	genesisHash := chain.ChainParams().GenesisHash
	genesisBlock := chain.ChainParams().GenesisBlock

	chain.blockHeightIndex[*genesisHash] = 0
	chain.blockHashesByHeight[0] = genesisHash
	chain.blockHeaders[*genesisHash] = &genesisBlock.Header
	chain.blocks[*genesisHash] = btcutil.NewBlock(genesisBlock)

	filter, _ := gcs.FromBytes(0, builder.DefaultP, builder.DefaultM, nil)
	chain.filters[*genesisHash] = filter

	filterHeader, _ := builder.MakeHeaderForFilter(filter, chainhash.Hash{})
	chain.filterHeadersByHeight[0] = &filterHeader

	chain.bestBlock = headerfs.BlockStamp{
		Height:    0,
		Hash:      *genesisHash,
		Timestamp: genesisBlock.Header.Timestamp,
	}

	for i := 0; i < numBlocks-1; i++ {
		chain.addNewBlock(false)
	}

	return chain
}

// addNewBlock advances the chain by one block. The notify boolean can be used
// to notify the new block.
func (c *mockChainSource) addNewBlock(notify bool) headerfs.BlockStamp {
	c.mu.Lock()
	newHeight := uint32(c.bestBlock.Height + 1)
	prevHash := c.bestBlock.Hash
	c.mu.Unlock()

	genesisTimestamp := c.ChainParams().GenesisBlock.Header.Timestamp
	header := &wire.BlockHeader{
		PrevBlock: prevHash,
		Timestamp: genesisTimestamp.Add(
			time.Duration(newHeight) * 10 * time.Minute,
		),
	}

	return c.addNewBlockWithHeader(header, notify)
}

// addNewBlock advances the chain by one block with the given header. The notify
// boolean can be used to notify the new block.
//
// NOTE: The header's PrevBlock should properly point to the best block in the
// chain.
func (c *mockChainSource) addNewBlockWithHeader(header *wire.BlockHeader,
	notify bool) headerfs.BlockStamp {

	c.mu.Lock()
	newHeight := uint32(c.bestBlock.Height + 1)
	newHash := header.BlockHash()
	c.blockHeightIndex[newHash] = newHeight
	c.blockHashesByHeight[newHeight] = &newHash
	c.blockHeaders[newHash] = header
	c.blocks[newHash] = btcutil.NewBlock(wire.NewMsgBlock(header))

	newFilter, _ := gcs.FromBytes(0, builder.DefaultP, builder.DefaultM, nil)
	c.filters[newHash] = newFilter

	newFilterHeader, _ := builder.MakeHeaderForFilter(
		newFilter, *c.filterHeadersByHeight[newHeight-1],
	)
	c.filterHeadersByHeight[newHeight] = &newFilterHeader

	c.bestBlock.Height++
	c.bestBlock.Hash = newHash
	c.bestBlock.Timestamp = header.Timestamp
	bestBlock := c.bestBlock
	c.mu.Unlock()

	if notify {
		c.ntfnChan <- blockntfns.NewBlockConnected(*header, newHeight)
	}

	return bestBlock
}

// rollback rolls back the chain by one block. The notify boolean can be used to
// notify the stale block.
func (c *mockChainSource) rollback(notify bool) headerfs.BlockStamp {
	c.mu.Lock()
	curHeight := uint32(c.bestBlock.Height)
	curHeader := c.blockHeaders[c.bestBlock.Hash]
	prevHeader := c.blockHeaders[curHeader.PrevBlock]

	delete(c.blockHeightIndex, c.bestBlock.Hash)
	delete(c.blockHashesByHeight, curHeight)
	delete(c.blockHeaders, c.bestBlock.Hash)
	delete(c.blocks, c.bestBlock.Hash)

	delete(c.filterHeadersByHeight, curHeight)
	delete(c.filters, c.bestBlock.Hash)

	c.bestBlock.Height--
	c.bestBlock.Hash = curHeader.PrevBlock
	c.bestBlock.Timestamp = prevHeader.Timestamp
	bestBlock := c.bestBlock
	c.mu.Unlock()

	if notify {
		c.ntfnChan <- blockntfns.NewBlockDisconnected(
			*curHeader, curHeight, *prevHeader,
		)
	}

	return bestBlock
}

// rollbackToHeight rolls back the chain to the specified height. The notify
// boolean can be used to notify all stale blocks.
func (c *mockChainSource) rollbackToHeight(height int32, notify bool) {
	c.mu.Lock()
	bestBlock := c.bestBlock
	c.mu.Unlock()

	for bestBlock.Height > height {
		bestBlock = c.rollback(notify)
	}
}

// ChainParams returns the parameters of the current chain.
func (c *mockChainSource) ChainParams() chaincfg.Params {
	return chaincfg.MainNetParams
}

// BestBlock retrieves the most recent block's height and hash where we have
// both the header and filter header ready.
func (c *mockChainSource) BestBlock() (*headerfs.BlockStamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Return a copy to prevent mutating internal state.
	return &headerfs.BlockStamp{
		Height:    c.bestBlock.Height,
		Hash:      c.bestBlock.Hash,
		Timestamp: c.bestBlock.Timestamp,
	}, nil
}

// GetBlockHeaderByHeight returns the header of the block with the given height.
func (c *mockChainSource) GetBlockHeaderByHeight(
	height uint32) (*wire.BlockHeader, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	blockHash, ok := c.blockHashesByHeight[height]
	if !ok {
		return nil, errors.New("block height not found")
	}
	blockHeader, ok := c.blockHeaders[*blockHash]
	if !ok {
		return nil, errors.New("block header not found")
	}
	return blockHeader, nil
}

// GetBlockHeader returns the header of the block with the given hash.
func (c *mockChainSource) GetBlockHeader(
	hash *chainhash.Hash) (*wire.BlockHeader, uint32, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	blockHeader, ok := c.blockHeaders[*hash]
	if !ok {
		return nil, 0, errors.New("block header not found")
	}
	blockHeight, ok := c.blockHeightIndex[*hash]
	if !ok {
		return nil, 0, errors.New("block height not found")
	}
	return blockHeader, blockHeight, nil
}

// GetBlock returns the block with the given hash.
func (c *mockChainSource) GetBlock(hash chainhash.Hash,
	_ ...QueryOption) (*btcutil.Block, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	block, ok := c.blocks[hash]
	if !ok {
		return nil, errors.New("block not found")
	}
	return block, nil
}

// GetFilterHeaderByHeight returns the filter header of the block with the given
// height.
func (c *mockChainSource) GetFilterHeaderByHeight(
	height uint32) (*chainhash.Hash, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	filterHeader, ok := c.filterHeadersByHeight[height]
	if !ok {
		return nil, errors.New("filter header not found")
	}
	return filterHeader, nil
}

// setFailGetFilter determines whether we should fail to retrieve for a block.
func (c *mockChainSource) setFailGetFilter(b bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failGetFilter = b
}

// IsCurrent returns true if the backend chain thinks that its view of
// the network is current.
func (c *mockChainSource) IsCurrent() bool {
	return true
}

// GetCFilter returns the filter of the given type for the block with the given
// hash.
func (c *mockChainSource) GetCFilter(hash chainhash.Hash,
	filterType wire.FilterType, options ...QueryOption) (*gcs.Filter, error) {

	defer func() {
		c.filtersQueried <- hash
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.failGetFilter {
		return nil, errors.New("failed filter")
	}

	filter, ok := c.filters[hash]
	if !ok {
		return nil, errors.New("filter not found")
	}
	return filter, nil
}

// overrideSubscribe allows us to override the mockChainSource's Subscribe
// method implementation to ease testing.
func (c *mockChainSource) overrideSubscribe(
	f func(uint32) (*blockntfns.Subscription, error)) {

	c.mu.Lock()
	defer c.mu.Unlock()
	c.subscribeOverride = f
}

// Subscribe returns a block subscription that delivers block notifications in
// order. The bestHeight parameter can be used to signal that a backlog of
// notifications should be delivered from this height. When providing a
// bestHeight of 0, a backlog will not be delivered.
func (c *mockChainSource) Subscribe(
	bestHeight uint32) (*blockntfns.Subscription, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subscribeOverride != nil {
		return c.subscribeOverride(bestHeight)
	}

	for i := bestHeight + 1; i <= uint32(c.bestBlock.Height); i++ {
		hash := c.blockHashesByHeight[i]
		header := c.blockHeaders[*hash]
		c.ntfnChan <- blockntfns.NewBlockConnected(*header, i)
	}

	return &blockntfns.Subscription{
		Notifications: c.ntfnChan,
		Cancel:        func() {},
	}, nil
}

// rescanTestContext serves as a harness to aid testing the Rescan struct.
type rescanTestContext struct {
	t                  *testing.T
	chain              *mockChainSource
	blocksConnected    chan headerfs.BlockStamp
	blocksDisconnected chan headerfs.BlockStamp
	rescan             *Rescan
	errChan            <-chan error
	quit               chan struct{}
}

// newRescanTestContext creates a new test harness for the Rescan struct backed
// by a chain of numBlocks.
func newRescanTestContext(t *testing.T, numBlocks int, // nolint:unparam
	options ...RescanOption) *rescanTestContext {

	blocksConnected := make(chan headerfs.BlockStamp)
	blocksDisconnected := make(chan headerfs.BlockStamp)
	ntfnHandlers := rpcclient.NotificationHandlers{
		OnFilteredBlockConnected: func(height int32,
			header *wire.BlockHeader, _ []*btcutil.Tx) {

			blocksConnected <- headerfs.BlockStamp{
				Hash:      header.BlockHash(),
				Height:    height,
				Timestamp: header.Timestamp,
			}
		},
		OnFilteredBlockDisconnected: func(height int32,
			header *wire.BlockHeader) {

			blocksDisconnected <- headerfs.BlockStamp{
				Hash:      header.BlockHash(),
				Height:    height,
				Timestamp: header.Timestamp,
			}
		},
	}
	quit := make(chan struct{})

	chain := newMockChainSource(numBlocks)
	rescanOptions := []RescanOption{
		NotificationHandlers(ntfnHandlers), QuitChan(quit),
	}
	rescanOptions = append(rescanOptions, options...)
	rescan := NewRescan(chain, rescanOptions...)

	return &rescanTestContext{
		t:                  t,
		chain:              chain,
		blocksConnected:    blocksConnected,
		blocksDisconnected: blocksDisconnected,
		rescan:             rescan,
		quit:               quit,
	}
}

// start starts the backing rescan.
func (ctx *rescanTestContext) start(waitUntilSynced bool) { // nolint:unparam
	if !waitUntilSynced {
		ctx.errChan = ctx.rescan.Start()
		return
	}

	// Override the mock chain subscribe implementation so that it delivers
	// a signal once synced.
	signal := make(chan struct{})
	deliverSyncSignal := func(bestHeight uint32) (*blockntfns.Subscription, error) {
		if bestHeight == uint32(ctx.chain.bestBlock.Height) {
			close(signal)
		}
		return &blockntfns.Subscription{
			Notifications: ctx.chain.ntfnChan,
			Cancel:        func() {},
		}, nil
	}
	ctx.chain.overrideSubscribe(deliverSyncSignal)

	// Start the rescan and wait for the signal to be delivered.
	ctx.errChan = ctx.rescan.Start()

	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		ctx.t.Fatal("expected to receive synced signal")
	}

	// Once done, we can revert to our original subscribe implementation.
	ctx.chain.overrideSubscribe(nil)
}

// stop stops the rescan test harness and ensures it exits cleanly.
func (ctx *rescanTestContext) stop() {
	close(ctx.quit)

	select {
	case err := <-ctx.errChan:
		if err != ErrRescanExit {
			ctx.t.Fatalf("expected ErrRescanExit upon shutdown, "+
				"got %v", err)
		}

	case <-time.After(time.Second):
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		ctx.t.Fatal("rescan did not exit cleanly")
	}

	select {
	case <-ctx.blocksConnected:
		ctx.t.Fatalf("received unexpected block connected after shutdown")
	case <-ctx.blocksDisconnected:
		ctx.t.Fatalf("received unexpected block disconnected after shutdown")
	case <-ctx.chain.filtersQueried:
		ctx.t.Fatalf("received unexpected filter query after shutdown")
	default:
	}
}

// recvBlockConnected is a helper method used to expect a block connected
// notification being delivered.
func (ctx *rescanTestContext) recvBlockConnected(block headerfs.BlockStamp) {
	ctx.t.Helper()

	select {
	case recvBlock := <-ctx.blocksConnected:
		if !reflect.DeepEqual(recvBlock, block) {
			ctx.t.Fatalf("expected block connected notification "+
				"for %v, got %v", spew.Sdump(block),
				spew.Sdump(recvBlock))
		}
	case <-time.After(time.Second):
		ctx.t.Fatalf("expected to receive block connected "+
			"notification for %v", spew.Sdump(block))
	}
}

// assertFilterQueried asserts that a filter for the given block hash was
// queried for.
func (ctx *rescanTestContext) assertFilterQueried(hash chainhash.Hash) {
	ctx.t.Helper()

	var hashQueried chainhash.Hash
	select {
	case hashQueried = <-ctx.chain.filtersQueried:
		if !hashQueried.IsEqual(&hash) {
			ctx.t.Fatalf("expected to query filter %v, got %v",
				hash, hashQueried)
		}
	case <-time.After(time.Second):
		ctx.t.Fatal("expected rescan to query for filter")
	}
}

// TestRescanNewBlockAfterRetry ensures that the rescan can properly queue
// blocks for which we are missing a filter for.
func TestRescanNewBlockAfterRetry(t *testing.T) {
	t.Parallel()

	// Dummy options to allow the Rescan to fetch block filters.
	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{}),
	}...)
	ctx.start(true)
	defer ctx.stop()

	// Modify the mocked chain such that it fails to retrieve block filters.
	ctx.chain.setFailGetFilter(true)

	// Adding multiple new blocks to the chain should result in the first
	// retry block being queried until successful.
	block1 := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block1.Hash)
	block2 := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block1.Hash)

	// Revert the mocked chain so that block filters can be retrieved
	// successfully.
	ctx.chain.setFailGetFilter(false)

	// The blocks should now be notified in their expected order.
	ctx.assertFilterQueried(block1.Hash)
	ctx.recvBlockConnected(block1)
	ctx.assertFilterQueried(block2.Hash)
	ctx.recvBlockConnected(block2)
}

// TestRescanReorgAllButFirstRetryBlocks ensures the rescan stops retrying
// blocks for which it has received disconnected notifications for.
func TestRescanReorgAllButFirstRetryBlocks(t *testing.T) {
	t.Parallel()

	// Dummy options to allow the Rescan to fetch block filters.
	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{}),
	}...)
	ctx.start(true)
	defer ctx.stop()

	// Modify the mocked chain such that it fails to retrieve block filters.
	ctx.chain.setFailGetFilter(true)

	// Adding multiple new blocks to the chain should result in the first
	// retry block being queried until successful.
	block := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block.Hash)
	ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block.Hash)
	ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block.Hash)

	// We'll then reorg back to the first retry block. This should cause us
	// to stop retrying the following reorged blocks.
	ctx.chain.rollbackToHeight(block.Height, true)

	// Revert the mocked chain so that block filters can be retrieved
	// successfully.
	ctx.chain.setFailGetFilter(false)

	// Since the first block wasn't reorged, we should expect a notification
	// for it.
	ctx.assertFilterQueried(block.Hash)
	ctx.recvBlockConnected(block)
}

// TestRescanReorgAllRetryBlocks ensures the rescan stops retrying blocks for
// which it has received disconnected notifications for and queries for blocks
// of the new chain instead.
func TestRescanReorgAllRetryBlocks(t *testing.T) {
	t.Parallel()

	// Dummy options to allow the Rescan to fetch block filters.
	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{}),
	}...)
	ctx.start(true)
	defer ctx.stop()

	// Note the block which we'll revert back to.
	bestBlock, err := ctx.chain.BestBlock()
	if err != nil {
		ctx.t.Fatalf("unable to retrieve best block: %v", err)
	}

	// Modify the mocked chain such that it fails to retrieve block filters.
	ctx.chain.setFailGetFilter(true)

	// Adding multiple new blocks to the chain should result in the first
	// retry block being queried until successful.
	block1 := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block1.Hash)
	block2 := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block1.Hash)

	// We'll then reorg back to a height where the new blocks we've added
	// and retried are stale, causing us to no longer retry them. The rescan
	// won't deliver any disconnected notifications to its caller since it
	// never delivered connected notifications for them.
	ctx.chain.rollbackToHeight(bestBlock.Height, true)

	// Revert the mocked chain so that block filters can be retrieved
	// successfully.
	ctx.chain.setFailGetFilter(false)

	// We'll then add two new blocks from a different chain to ensure the
	// rescan can properly follow it.
	newBlock1 := ctx.chain.addNewBlockWithHeader(
		&wire.BlockHeader{PrevBlock: bestBlock.Hash}, true,
	)
	if newBlock1.Hash == block1.Hash {
		ctx.t.Fatal("expected different hashes for blocks on " +
			"different chains")
	}
	ctx.assertFilterQueried(newBlock1.Hash)
	ctx.recvBlockConnected(newBlock1)

	newBlock2 := ctx.chain.addNewBlock(true)
	if newBlock2.Hash == block2.Hash {
		ctx.t.Fatal("expected different hashes for blocks on " +
			"different chains")
	}
	ctx.assertFilterQueried(newBlock2.Hash)
	ctx.recvBlockConnected(newBlock2)
}

// TestRescanRetryBlocksAfterCatchingUp ensures the rescan stops retrying blocks
// after catching up with the chain. Catching up assumes that we already have an
// accurate representation of the chain.
func TestRescanRetryBlocksAfterCatchingUp(t *testing.T) {
	t.Parallel()

	// Dummy options to allow the Rescan to fetch block filters.
	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{}),
	}...)
	ctx.start(true)
	defer ctx.stop()

	// Modify the mocked chain such that it fails to retrieve block filters.
	ctx.chain.setFailGetFilter(true)

	// Adding multiple new blocks to the chain should result in the first
	// retry block being queried until successful.
	block1 := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block1.Hash)

	// We'll prevent notifying new blocks and instead notify an invalid
	// block which should prompt the rescan to catch up with the chain
	// manually.
	block2 := ctx.chain.addNewBlock(false)
	block3 := ctx.chain.addNewBlock(false)
	ctx.chain.ntfnChan <- blockntfns.NewBlockConnected(wire.BlockHeader{}, 0)

	// Revert the mocked chain so that block filters can be retrieved
	// successfully.
	ctx.chain.setFailGetFilter(false)

	// The notifications for all the blocks added above should now be
	// delivered as part of catching up.
	ctx.assertFilterQueried(block1.Hash)
	ctx.recvBlockConnected(block1)
	ctx.assertFilterQueried(block2.Hash)
	ctx.recvBlockConnected(block2)
	ctx.assertFilterQueried(block3.Hash)
	ctx.recvBlockConnected(block3)
}
