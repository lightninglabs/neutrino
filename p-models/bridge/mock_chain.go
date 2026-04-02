package bridge

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/gcs"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	neutrino "github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
)

// MockChain is a small test chain that lets the bridge run the actual Go
// rescan actor against deterministic block/filter/subscription behavior.
type MockChain struct {
	mu sync.Mutex

	bestBlock             headerfs.BlockStamp
	blockHeightIndex      map[chainhash.Hash]uint32
	blockHashesByHeight   map[uint32]*chainhash.Hash
	blockHeaders          map[chainhash.Hash]*wire.BlockHeader
	blocks                map[chainhash.Hash]*btcutil.Block
	filters               map[chainhash.Hash]*gcs.Filter
	filterHeadersByHeight map[uint32]*chainhash.Hash

	activeSub *mockSubscription

	filterFailures map[chainhash.Hash]int
	subEvents      []SubscriptionEvent

	subscribeCount atomic.Int32
	cancelCount    atomic.Int32
}

type mockSubscription struct {
	notifications chan blockntfns.BlockNtfn
	closeOnce     sync.Once
}

func newMockSubscription() *mockSubscription {
	return &mockSubscription{
		notifications: make(chan blockntfns.BlockNtfn, 100),
	}
}

func (s *mockSubscription) Close() {
	s.closeOnce.Do(func() {
		close(s.notifications)
	})
}

// SubscriptionEventKind describes a subscription lifecycle action observed by
// the bridge mock chain.
type SubscriptionEventKind string

const (
	SubscriptionStarted  SubscriptionEventKind = "started"
	SubscriptionCanceled SubscriptionEventKind = "canceled"
)

// SubscriptionEvent records one subscribe/cancel edge in the actual Go actor.
type SubscriptionEvent struct {
	Kind   SubscriptionEventKind
	Height uint32
}

var _ neutrino.ChainSource = (*MockChain)(nil)

// NewMockChain creates a linear chain with the requested number of blocks.
func NewMockChain(numBlocks int) *MockChain {
	chain := &MockChain{
		blockHeightIndex:      make(map[chainhash.Hash]uint32),
		blockHashesByHeight:   make(map[uint32]*chainhash.Hash),
		blockHeaders:          make(map[chainhash.Hash]*wire.BlockHeader),
		blocks:                make(map[chainhash.Hash]*btcutil.Block),
		filters:               make(map[chainhash.Hash]*gcs.Filter),
		filterHeadersByHeight: make(map[uint32]*chainhash.Hash),
		filterFailures:        make(map[chainhash.Hash]int),
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
		chain.addBlockLocked(nil)
	}

	return chain
}

// AddBlock extends the chain with an empty block.
func (c *MockChain) AddBlock() headerfs.BlockStamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.addBlockLocked(nil)
}

// AddBlockWithTx extends the chain with a block paying to the given address.
func (c *MockChain) AddBlockWithTx(
	addr btcutil.Address) (headerfs.BlockStamp, *wire.MsgTx, error) {

	pkScript, err := txscript.PayToAddrScript(addr)
	if err != nil {
		return headerfs.BlockStamp{}, nil, err
	}

	tx := &wire.MsgTx{
		Version: 1,
		TxIn: []*wire.TxIn{
			{PreviousOutPoint: wire.OutPoint{Index: 0}},
		},
		TxOut: []*wire.TxOut{
			{Value: 100000, PkScript: pkScript},
		},
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	stamp := c.addBlockLocked([]*wire.MsgTx{tx})

	return stamp, tx, nil
}

// NotifyBlockConnected delivers a connected notification for the target block.
func (c *MockChain) NotifyBlockConnected(height uint32) error {
	c.mu.Lock()

	hash, ok := c.blockHashesByHeight[height]
	if !ok {
		c.mu.Unlock()
		return errors.New("block height not found")
	}

	header, ok := c.blockHeaders[*hash]
	if !ok {
		c.mu.Unlock()
		return errors.New("block header not found")
	}

	sub := c.activeSub
	c.mu.Unlock()

	if sub != nil {
		sub.notifications <- blockntfns.NewBlockConnected(*header, height)
	}

	return nil
}

// NotifyBlockDisconnected delivers a disconnected notification for the target
// block using the previous block as the new chain tip.
func (c *MockChain) NotifyBlockDisconnected(height uint32) error {
	c.mu.Lock()

	if height == 0 {
		c.mu.Unlock()
		return errors.New("genesis block cannot be disconnected")
	}

	hash, ok := c.blockHashesByHeight[height]
	if !ok {
		c.mu.Unlock()
		return errors.New("block height not found")
	}

	header, ok := c.blockHeaders[*hash]
	if !ok {
		c.mu.Unlock()
		return errors.New("block header not found")
	}

	chainTipHash, ok := c.blockHashesByHeight[height-1]
	if !ok {
		c.mu.Unlock()
		return errors.New("chain tip height not found")
	}

	chainTipHeader, ok := c.blockHeaders[*chainTipHash]
	if !ok {
		c.mu.Unlock()
		return errors.New("chain tip header not found")
	}

	sub := c.activeSub
	c.mu.Unlock()

	if sub != nil {
		sub.notifications <- blockntfns.NewBlockDisconnected(
			*header, height, *chainTipHeader,
		)
	}

	return nil
}

// CloseActiveSubscription simulates an unexpected subscription channel closure.
func (c *MockChain) CloseActiveSubscription() {
	c.mu.Lock()
	sub := c.activeSub
	c.activeSub = nil
	c.mu.Unlock()

	if sub != nil {
		sub.Close()
	}
}

// CloseActiveSubscriptionAndAddBlock simulates a subscription dying while the
// chain advances before the actor can resubscribe.
func (c *MockChain) CloseActiveSubscriptionAndAddBlock() headerfs.BlockStamp {
	c.mu.Lock()
	sub := c.activeSub
	c.activeSub = nil
	stamp := c.addBlockLocked(nil)
	c.mu.Unlock()

	if sub != nil {
		sub.Close()
	}

	return stamp
}

// RewriteHeaderAtHeight swaps the height index to point to a new synthetic
// header. This is used by bridge tests to model a chain view change between
// two callback dispatches.
func (c *MockChain) RewriteHeaderAtHeight(height uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash, ok := c.blockHashesByHeight[height]
	if !ok {
		return errors.New("block height not found")
	}

	header, ok := c.blockHeaders[*hash]
	if !ok {
		return errors.New("block header not found")
	}

	altHeader := *header
	altHeader.Timestamp = altHeader.Timestamp.Add(time.Second)
	altHash := altHeader.BlockHash()

	c.blockHashesByHeight[height] = &altHash
	c.blockHeaders[altHash] = &altHeader
	c.blockHeightIndex[altHash] = height

	return nil
}

// FailFilterFetches arranges for GetCFilter to fail the next count times for
// the block at the given height.
func (c *MockChain) FailFilterFetches(height uint32, count int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash, ok := c.blockHashesByHeight[height]
	if !ok {
		return errors.New("block height not found")
	}

	c.filterFailures[*hash] = count

	return nil
}

// SubscribeCount returns how many times Subscribe has been called.
func (c *MockChain) SubscribeCount() int32 {
	return c.subscribeCount.Load()
}

// CancelCount returns how many times a subscription cancel closure has run.
func (c *MockChain) CancelCount() int32 {
	return c.cancelCount.Load()
}

// SubscriptionEvents returns the ordered subscribe/cancel sequence observed by
// the mock chain.
func (c *MockChain) SubscriptionEvents() []SubscriptionEvent {
	c.mu.Lock()
	defer c.mu.Unlock()

	events := make([]SubscriptionEvent, len(c.subEvents))
	copy(events, c.subEvents)

	return events
}

func (c *MockChain) addBlockLocked(
	txs []*wire.MsgTx) headerfs.BlockStamp {

	newHeight := uint32(c.bestBlock.Height + 1)
	prevHash := c.bestBlock.Hash

	genesisTimestamp := chaincfg.MainNetParams.GenesisBlock.Header.Timestamp
	header := &wire.BlockHeader{
		PrevBlock: prevHash,
		Timestamp: genesisTimestamp.Add(
			time.Duration(newHeight) * 10 * time.Minute,
		),
	}

	msgBlock := wire.NewMsgBlock(header)
	for _, tx := range txs {
		_ = msgBlock.AddTransaction(tx)
	}

	block := btcutil.NewBlock(msgBlock)
	newHash := header.BlockHash()

	c.blockHeightIndex[newHash] = newHeight
	c.blockHashesByHeight[newHeight] = &newHash
	c.blockHeaders[newHash] = header
	c.blocks[newHash] = block

	var filter *gcs.Filter
	if len(txs) == 0 {
		filter, _ = gcs.FromBytes(
			0, builder.DefaultP, builder.DefaultM, nil,
		)
	} else {
		filter, _ = builder.BuildBasicFilter(msgBlock, nil)
	}
	c.filters[newHash] = filter

	prevFilterHeader := c.filterHeadersByHeight[newHeight-1]
	filterHeader, _ := builder.MakeHeaderForFilter(
		filter, *prevFilterHeader,
	)
	c.filterHeadersByHeight[newHeight] = &filterHeader

	c.bestBlock.Height++
	c.bestBlock.Hash = newHash
	c.bestBlock.Timestamp = header.Timestamp

	return c.bestBlock
}

func (c *MockChain) ChainParams() chaincfg.Params {
	return chaincfg.MainNetParams
}

func (c *MockChain) BestBlock() (*headerfs.BlockStamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return &headerfs.BlockStamp{
		Height:    c.bestBlock.Height,
		Hash:      c.bestBlock.Hash,
		Timestamp: c.bestBlock.Timestamp,
	}, nil
}

func (c *MockChain) GetBlockHeaderByHeight(
	height uint32) (*wire.BlockHeader, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	hash, ok := c.blockHashesByHeight[height]
	if !ok {
		return nil, errors.New("block height not found")
	}

	header, ok := c.blockHeaders[*hash]
	if !ok {
		return nil, errors.New("block header not found")
	}

	return header, nil
}

func (c *MockChain) GetBlockHeader(
	hash *chainhash.Hash) (*wire.BlockHeader, uint32, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	header, ok := c.blockHeaders[*hash]
	if !ok {
		return nil, 0, errors.New("block header not found")
	}

	height, ok := c.blockHeightIndex[*hash]
	if !ok {
		return nil, 0, errors.New("block height not found")
	}

	return header, height, nil
}

func (c *MockChain) GetBlock(hash chainhash.Hash,
	_ ...neutrino.QueryOption) (*btcutil.Block, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	block, ok := c.blocks[hash]
	if !ok {
		return nil, errors.New("block not found")
	}

	return block, nil
}

func (c *MockChain) GetFilterHeaderByHeight(
	height uint32) (*chainhash.Hash, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	filterHeader, ok := c.filterHeadersByHeight[height]
	if !ok {
		return nil, errors.New("filter header not found")
	}

	return filterHeader, nil
}

func (c *MockChain) GetCFilter(hash chainhash.Hash,
	_ wire.FilterType,
	_ ...neutrino.QueryOption) (*gcs.Filter, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	if remaining := c.filterFailures[hash]; remaining > 0 {
		c.filterFailures[hash] = remaining - 1
		return nil, errors.New("injected filter fetch failure")
	}

	filter, ok := c.filters[hash]
	if !ok {
		return nil, errors.New("filter not found")
	}

	return filter, nil
}

func (c *MockChain) Subscribe(
	height uint32) (*blockntfns.Subscription, error) {

	sub := newMockSubscription()
	backlog := make([]blockntfns.BlockNtfn, 0)

	c.mu.Lock()
	if c.activeSub != nil {
		c.activeSub.Close()
	}
	c.activeSub = sub
	c.subEvents = append(c.subEvents, SubscriptionEvent{
		Kind:   SubscriptionStarted,
		Height: height,
	})

	for h := height + 1; h <= uint32(c.bestBlock.Height); h++ {
		hash, ok := c.blockHashesByHeight[h]
		if !ok {
			continue
		}

		header, ok := c.blockHeaders[*hash]
		if !ok {
			continue
		}

		backlog = append(
			backlog, blockntfns.NewBlockConnected(*header, h),
		)
	}
	c.mu.Unlock()

	for _, ntfn := range backlog {
		sub.notifications <- ntfn
	}

	c.subscribeCount.Add(1)

	var cancelOnce sync.Once
	return &blockntfns.Subscription{
		Notifications: sub.notifications,
		Cancel: func() {
			cancelOnce.Do(func() {
				c.mu.Lock()
				if c.activeSub == sub {
					c.activeSub = nil
				}
				c.subEvents = append(c.subEvents, SubscriptionEvent{
					Kind: SubscriptionCanceled,
				})
				c.mu.Unlock()

				c.cancelCount.Add(1)
				sub.Close()
			})
		},
	}, nil
}

func (c *MockChain) IsCurrent() bool {
	return true
}
