package rescan

import (
	"errors"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/gcs"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	neutrino "github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
)

// mockChainSource is a mock implementation of neutrino.ChainSource for
// testing the rescan FSM states.
type mockChainSource struct {
	mu sync.Mutex

	bestBlock             headerfs.BlockStamp
	blockHeightIndex      map[chainhash.Hash]uint32
	blockHashesByHeight   map[uint32]*chainhash.Hash
	blockHeaders          map[chainhash.Hash]*wire.BlockHeader
	blocks                map[chainhash.Hash]*btcutil.Block
	filters               map[chainhash.Hash]*gcs.Filter
	filterHeadersByHeight map[uint32]*chainhash.Hash

	ntfnChan chan blockntfns.BlockNtfn
}

var _ neutrino.ChainSource = (*mockChainSource)(nil)

// newMockChain creates a mock chain with numBlocks blocks starting from
// genesis. All blocks have empty filters (no matching transactions).
func newMockChain(numBlocks int) *mockChainSource {
	chain := &mockChainSource{
		blockHeightIndex:      make(map[chainhash.Hash]uint32),
		blockHashesByHeight:   make(map[uint32]*chainhash.Hash),
		blockHeaders:          make(map[chainhash.Hash]*wire.BlockHeader),
		blocks:                make(map[chainhash.Hash]*btcutil.Block),
		filters:               make(map[chainhash.Hash]*gcs.Filter),
		filterHeadersByHeight: make(map[uint32]*chainhash.Hash),
		ntfnChan:              make(chan blockntfns.BlockNtfn, 100),
	}

	genesisHash := chain.ChainParams().GenesisHash
	genesisBlock := chain.ChainParams().GenesisBlock

	chain.blockHeightIndex[*genesisHash] = 0
	chain.blockHashesByHeight[0] = genesisHash
	chain.blockHeaders[*genesisHash] = &genesisBlock.Header
	chain.blocks[*genesisHash] = btcutil.NewBlock(genesisBlock)

	filter, _ := gcs.FromBytes(
		0, builder.DefaultP, builder.DefaultM, nil,
	)
	chain.filters[*genesisHash] = filter

	filterHeader, _ := builder.MakeHeaderForFilter(
		filter, chainhash.Hash{},
	)
	chain.filterHeadersByHeight[0] = &filterHeader

	chain.bestBlock = headerfs.BlockStamp{
		Height:    0,
		Hash:      *genesisHash,
		Timestamp: genesisBlock.Header.Timestamp,
	}

	for i := 0; i < numBlocks-1; i++ {
		chain.addBlock()
	}

	return chain
}

// addBlock advances the chain by one block with an empty filter.
func (c *mockChainSource) addBlock() headerfs.BlockStamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	newHeight := uint32(c.bestBlock.Height + 1)
	prevHash := c.bestBlock.Hash

	genesisTimestamp := chaincfg.MainNetParams.GenesisBlock.Header.Timestamp
	header := &wire.BlockHeader{
		PrevBlock: prevHash,
		Timestamp: genesisTimestamp.Add(
			time.Duration(newHeight) * 10 * time.Minute,
		),
	}

	newHash := header.BlockHash()
	c.blockHeightIndex[newHash] = newHeight
	c.blockHashesByHeight[newHeight] = &newHash
	c.blockHeaders[newHash] = header
	c.blocks[newHash] = btcutil.NewBlock(wire.NewMsgBlock(header))

	emptyFilter, _ := gcs.FromBytes(
		0, builder.DefaultP, builder.DefaultM, nil,
	)
	c.filters[newHash] = emptyFilter

	prevFilterHeader := c.filterHeadersByHeight[newHeight-1]
	filterHeader, _ := builder.MakeHeaderForFilter(
		emptyFilter, *prevFilterHeader,
	)
	c.filterHeadersByHeight[newHeight] = &filterHeader

	c.bestBlock.Height++
	c.bestBlock.Hash = newHash
	c.bestBlock.Timestamp = header.Timestamp

	return c.bestBlock
}

func (c *mockChainSource) ChainParams() chaincfg.Params {
	return chaincfg.MainNetParams
}

func (c *mockChainSource) BestBlock() (*headerfs.BlockStamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return &headerfs.BlockStamp{
		Height:    c.bestBlock.Height,
		Hash:      c.bestBlock.Hash,
		Timestamp: c.bestBlock.Timestamp,
	}, nil
}

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

func (c *mockChainSource) GetBlock(hash chainhash.Hash,
	_ ...neutrino.QueryOption) (*btcutil.Block, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	block, ok := c.blocks[hash]
	if !ok {
		return nil, errors.New("block not found")
	}

	return block, nil
}

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

func (c *mockChainSource) GetCFilter(hash chainhash.Hash,
	_ wire.FilterType,
	_ ...neutrino.QueryOption) (*gcs.Filter, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	filter, ok := c.filters[hash]
	if !ok {
		return nil, errors.New("filter not found")
	}

	return filter, nil
}

func (c *mockChainSource) Subscribe(
	bestHeight uint32) (*blockntfns.Subscription, error) {

	return &blockntfns.Subscription{
		Notifications: c.ntfnChan,
		Cancel:        func() {},
	}, nil
}

func (c *mockChainSource) IsCurrent() bool {
	return true
}
