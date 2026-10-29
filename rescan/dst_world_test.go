package rescan

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
)

const (
	// dstBaseHigh is the start height of the modes that cross
	// ProgressInterval, so the progress callback fires within a short
	// scan.
	dstBaseHigh = ProgressInterval - 10

	// dstPlainPrefix is the number of blocks above each base height that
	// never carry a transaction. A birthday inside this prefix then
	// precedes every relevant transaction, as a wallet birthday does.
	dstPlainPrefix = 3

	// dstSpendDepth is how far back a spend block looks for a payment to
	// spend.
	dstSpendDepth = 8
)

// dstAddr names one of the fixture's addresses. The DST watches dstAddrA
// from the start. The property test adds dstAddrB and dstAddrC later.
// Nobody watches dstAddrOther.
type dstAddr uint8

const (
	dstAddrA dstAddr = iota
	dstAddrB
	dstAddrC
	dstAddrOther
	dstAddrCount
)

// dstKind is the content of one fixture block.
type dstKind uint8

const (
	// dstKindPlain is a block with no transactions.
	dstKindPlain dstKind = iota

	// dstKindPayA, dstKindPayB and dstKindPayC pay one fixture address.
	dstKindPayA
	dstKindPayB
	dstKindPayC

	// dstKindOther pays an address nobody watches, so the filter holds
	// data that does not match.
	dstKindOther

	// dstKindSpend spends the newest payment among its recent ancestors.
	dstKindSpend
)

// dstKindPattern maps a height and branch salt to block content. It mixes
// plain blocks, payments to each address, unrelated payments and spends.
var dstKindPattern = [8]dstKind{
	dstKindPlain, dstKindPlain, dstKindPlain, dstKindPayA,
	dstKindPayB, dstKindPayC, dstKindSpend, dstKindOther,
}

// dstBlock is one immutable fixture block. Every field is set once when
// the block is built and never written again, so runs share blocks.
type dstBlock struct {
	hash   chainhash.Hash
	header wire.BlockHeader
	height uint32

	// salt identifies the branch. The honest main chain has salt 0, and
	// each reorg branch has its own salt.
	salt uint32

	parent *dstBlock
	msg    *wire.MsgBlock
	filter *gcs.Filter
	kind   dstKind
}

// stamp returns the block's stamp.
func (b *dstBlock) stamp() headerfs.BlockStamp {
	return headerfs.BlockStamp{
		Hash:      b.hash,
		Height:    int32(b.height),
		Timestamp: b.header.Timestamp,
	}
}

// dstChildKey identifies a block by its parent and branch salt.
type dstChildKey struct {
	parent chainhash.Hash
	salt   uint32
}

// dstHeaderKey identifies a fixture block by header fields without hashing
// the header. Siblings differ in their nonce, which is the branch salt.
type dstHeaderKey struct {
	prev   chainhash.Hash
	merkle chainhash.Hash
	nonce  uint32
}

// headerKey returns the lookup key of a header.
func headerKey(h *wire.BlockHeader) dstHeaderKey {
	return dstHeaderKey{prev: h.PrevBlock, merkle: h.MerkleRoot,
		nonce: h.Nonce}
}

// dstFixture holds every block any run has asked for. Blocks are a pure
// function of their parent and salt, so the fixture is built lazily and
// shared by every run in the process.
type dstFixture struct {
	mu       sync.Mutex
	children map[dstChildKey]*dstBlock
	byHash   map[chainhash.Hash]*dstBlock
	byHeader map[dstHeaderKey]*dstBlock

	genesis *dstBlock
	main    []*dstBlock
	scripts [dstAddrCount][]byte
	addrs   [dstAddrCount]address.Address

	// empty is the shared filter of every block with no transactions.
	empty *gcs.Filter
}

var (
	dstFixtureOnce sync.Once
	dstFixtureVal  *dstFixture
)

// dstChains returns the process-wide fixture. The main chain is built once,
// up to dstBaseHigh plus room for the longest run.
func dstChains() *dstFixture {
	dstFixtureOnce.Do(func() {
		fx := &dstFixture{
			children: make(map[dstChildKey]*dstBlock),
			byHash:   make(map[chainhash.Hash]*dstBlock),
			byHeader: make(map[dstHeaderKey]*dstBlock),
		}
		for i := range fx.addrs {
			addr, err := address.NewAddressPubKeyHash(
				chainhash.HashB([]byte{
					'd', 's', 't', byte(i),
				})[:20],
				&chaincfg.MainNetParams,
			)
			if err != nil {
				panic(err)
			}
			script, err := txscript.PayToAddrScript(addr)
			if err != nil {
				panic(err)
			}
			fx.addrs[i] = addr
			fx.scripts[i] = script
		}
		empty, err := gcs.FromBytes(
			0, builder.DefaultP, builder.DefaultM, nil,
		)
		if err != nil {
			panic(err)
		}
		fx.empty = empty

		params := chaincfg.MainNetParams
		genesis := &dstBlock{
			hash:   *params.GenesisHash,
			header: params.GenesisBlock.Header,
			msg:    params.GenesisBlock,
			filter: empty,
		}
		fx.genesis = genesis
		fx.byHash[genesis.hash] = genesis
		fx.byHeader[headerKey(&genesis.header)] = genesis

		fx.main = append(fx.main, genesis)
		for len(fx.main) < dstBaseHigh+256 {
			tip := fx.main[len(fx.main)-1]
			fx.main = append(fx.main, fx.child(tip, 0))
		}
		dstFixtureVal = fx
	})
	return dstFixtureVal
}

// lookup returns the fixture block with the given hash, if any run built
// it.
func (fx *dstFixture) lookup(hash chainhash.Hash) (*dstBlock, bool) {
	fx.mu.Lock()
	defer fx.mu.Unlock()

	b, ok := fx.byHash[hash]
	return b, ok
}

// byHeaderOf returns the fixture block with the given header without
// hashing it. The oracle uses it on every callback, because hashing headers
// contends on the wire package's shared buffer pool when runs execute in
// parallel.
func (fx *dstFixture) byHeaderOf(h *wire.BlockHeader) (*dstBlock, bool) {
	fx.mu.Lock()
	b, ok := fx.byHeader[headerKey(h)]
	fx.mu.Unlock()
	if !ok || b.header.Version != h.Version || b.header.Bits != h.Bits ||
		!b.header.Timestamp.Equal(h.Timestamp) {

		return nil, false
	}
	return b, true
}

// dstKindAt returns the content of the block at height on the given
// branch.
func dstKindAt(height, salt uint32) dstKind {
	relative := height
	if height >= dstBaseHigh {
		relative = height - dstBaseHigh
	}
	if relative <= dstPlainPrefix {
		return dstKindPlain
	}
	return dstKindPattern[(height*7+salt*5)%8]
}

// child returns the block that extends parent on the branch with the given
// salt, building it the first time it is asked for.
func (fx *dstFixture) child(parent *dstBlock, salt uint32) *dstBlock {
	key := dstChildKey{parent: parent.hash, salt: salt}

	fx.mu.Lock()
	defer fx.mu.Unlock()

	if b, ok := fx.children[key]; ok {
		return b
	}
	b := fx.build(parent, salt)
	if prior, ok := fx.byHash[b.hash]; ok {
		panic(fmt.Sprintf("fixture hash collision at %d: %v and %v",
			b.height, prior.height, b.hash))
	}
	if _, ok := fx.byHeader[headerKey(&b.header)]; ok {
		panic(fmt.Sprintf("fixture header key collision at %d",
			b.height))
	}
	fx.children[key] = b
	fx.byHash[b.hash] = b
	fx.byHeader[headerKey(&b.header)] = b
	return b
}

// build constructs one block. The caller holds fx.mu.
func (fx *dstFixture) build(parent *dstBlock, salt uint32) *dstBlock {
	height := parent.height + 1
	kind := dstKindAt(height, salt)

	// A spend block spends output 0 of the newest payment among its
	// recent ancestors. Without one it stays plain.
	var spent *dstBlock
	if kind == dstKindSpend {
		for a, i := parent, 0; a != nil && i < dstSpendDepth; i++ {
			if a.kind >= dstKindPayA && a.kind <= dstKindPayC {
				spent = a
				break
			}
			a = a.parent
		}
		if spent == nil {
			kind = dstKindPlain
		}
	}

	genesisTime := chaincfg.MainNetParams.GenesisBlock.Header.Timestamp
	header := wire.BlockHeader{
		Version:   1,
		PrevBlock: parent.hash,
		Timestamp: genesisTime.Add(
			time.Duration(height) * 10 * time.Minute,
		),
		Bits:  chaincfg.MainNetParams.GenesisBlock.Header.Bits,
		Nonce: salt,
	}
	msg := wire.NewMsgBlock(&header)

	var prevScripts [][]byte
	if kind != dstKindPlain {
		var tag [8]byte
		binary.LittleEndian.PutUint32(tag[:4], height)
		binary.LittleEndian.PutUint32(tag[4:], salt)
		coinbase := wire.NewMsgTx(1)
		coinbase.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Index: wire.MaxPrevOutIndex},
			SignatureScript:  tag[:],
		})
		coinbase.AddTxOut(wire.NewTxOut(
			50, []byte{txscript.OP_TRUE},
		))
		_ = msg.AddTransaction(coinbase)

		tx := wire.NewMsgTx(1)
		switch kind {
		case dstKindSpend:
			paid := spent.msg.Transactions[1]
			tx.AddTxIn(&wire.TxIn{PreviousOutPoint: wire.OutPoint{
				Hash: paid.TxHash(),
			}})
			prevScripts = append(
				prevScripts, paid.TxOut[0].PkScript,
			)
			tx.AddTxOut(wire.NewTxOut(
				900, fx.scripts[dstAddrOther],
			))

		default:
			to := dstAddrOther
			if kind != dstKindOther {
				to = dstAddr(kind - dstKindPayA)
			}
			funding := chainhash.HashH(append(
				parent.hash[:], tag[:]...,
			))
			tx.AddTxIn(&wire.TxIn{PreviousOutPoint: wire.OutPoint{
				Hash: funding,
			}})
			tx.AddTxOut(wire.NewTxOut(1000, fx.scripts[to]))
		}
		_ = msg.AddTransaction(tx)

		var hashes []byte
		for _, tx := range msg.Transactions {
			txHash := tx.TxHash()
			hashes = append(hashes, txHash[:]...)
		}
		msg.Header.MerkleRoot = chainhash.HashH(hashes)
	}

	filter := fx.empty
	if kind != dstKindPlain {
		var err error
		filter, err = builder.BuildBasicFilter(msg, prevScripts)
		if err != nil {
			panic(err)
		}
	}

	return &dstBlock{
		hash:   msg.Header.BlockHash(),
		header: msg.Header,
		height: height,
		salt:   salt,
		parent: parent,
		msg:    msg,
		filter: filter,
		kind:   kind,
	}
}

// dstWorld is one run's view of the chain: the honest active chain and the
// notification source behind the real SubscriptionManager. The harness
// changes the active chain first and then announces the change, as the
// block manager does.
type dstWorld struct {
	fx *dstFixture

	mu     sync.Mutex
	active []*dstBlock

	// nextSalt is the salt of the next reorg branch.
	nextSalt uint32

	// ntfns is unbuffered, so a send returns only after the manager took
	// the notification and fanned it out to the current subscribers.
	ntfns chan blockntfns.BlockNtfn
}

// newDSTWorld returns a world whose active chain is the main chain up to
// tip.
func newDSTWorld(tip uint32) *dstWorld {
	fx := dstChains()
	return &dstWorld{
		fx:       fx,
		active:   fx.main[: tip+1 : tip+1],
		nextSalt: 1,
		ntfns:    make(chan blockntfns.BlockNtfn),
	}
}

// at returns the active block at height, if there is one.
func (w *dstWorld) at(height uint32) (*dstBlock, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if int(height) >= len(w.active) {
		return nil, false
	}
	return w.active[height], true
}

// tip returns the active tip.
func (w *dstWorld) tip() *dstBlock {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.active[len(w.active)-1]
}

// onActive reports whether hash is on the active chain.
func (w *dstWorld) onActive(b *dstBlock) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return int(b.height) < len(w.active) && w.active[b.height] == b
}

// extend adds one block on the tip's branch and returns it without
// announcing it.
func (w *dstWorld) extend() *dstBlock {
	w.mu.Lock()
	defer w.mu.Unlock()

	tip := w.active[len(w.active)-1]
	next := w.fx.child(tip, tip.salt)
	w.active = append(w.active, next)
	return next
}

// reorg replaces every block above fork with a new branch of n blocks and
// returns the orphaned and the new blocks, both in height order. It does
// not announce anything.
func (w *dstWorld) reorg(fork uint32, n int) ([]*dstBlock, []*dstBlock) {
	w.mu.Lock()
	defer w.mu.Unlock()

	salt := w.nextSalt
	w.nextSalt++
	old := append([]*dstBlock(nil), w.active[fork+1:]...)
	active := append([]*dstBlock(nil), w.active[:fork+1]...)
	var branch []*dstBlock
	for i := 0; i < n; i++ {
		next := w.fx.child(active[len(active)-1], salt)
		active = append(active, next)
		branch = append(branch, next)
	}
	w.active = active
	return old, branch
}

// announceConnected sends a connected notification for b.
func (w *dstWorld) announceConnected(b *dstBlock) {
	w.ntfns <- blockntfns.NewBlockConnected(b.header, b.height)
}

// announceReorg sends the disconnects of old from the top down, then the
// connects of branch from the bottom up, as the block manager does.
func (w *dstWorld) announceReorg(old, branch []*dstBlock) {
	for i := len(old) - 1; i >= 0; i-- {
		b := old[i]
		w.ntfns <- blockntfns.NewBlockDisconnected(
			b.header, b.height, b.parent.header,
		)
	}
	for _, b := range branch {
		w.announceConnected(b)
	}
}

// Notifications is part of the blockntfns.NotificationSource interface.
func (w *dstWorld) Notifications() <-chan blockntfns.BlockNtfn {
	return w.ntfns
}

// NotificationsSinceHeight is part of the blockntfns.NotificationSource
// interface. It follows blockManager.NotificationsSinceHeight: no backlog
// for height 0 or the tip, an error above the tip, and otherwise a connect
// for every active block above height.
func (w *dstWorld) NotificationsSinceHeight(
	height uint32) ([]blockntfns.BlockNtfn, uint32, error) {

	w.mu.Lock()
	defer w.mu.Unlock()

	best := uint32(len(w.active) - 1)
	if height == 0 || height == best {
		return nil, best, nil
	}
	if height > best {
		return nil, 0, fmt.Errorf("request with height %d is "+
			"greater than best height known %d", height, best)
	}
	var backlog []blockntfns.BlockNtfn
	for h := height + 1; h <= best; h++ {
		b := w.active[h]
		backlog = append(
			backlog, blockntfns.NewBlockConnected(b.header, h),
		)
	}
	return backlog, best, nil
}

var _ blockntfns.NotificationSource = (*dstWorld)(nil)

// errDSTFault is the error every injected fetch fault returns.
var errDSTFault = errors.New("dst injected fault")

// dstSource is the ChainSource the actor sees. Subscribe goes to the real
// SubscriptionManager over the world, and every other method goes to the
// world through one fault layer. With no fault armed, every method behaves
// as the honest chain service would.
type dstSource struct {
	w   *dstWorld
	mgr *blockntfns.SubscriptionManager

	mu sync.Mutex

	// filterFail, blockFail and subscribeFail fail the next N calls.
	filterFail    int
	blockFail     int
	subscribeFail int

	// onSubscribe runs once, inside the next successful Subscribe and
	// before the subscription registers.
	onSubscribe func()

	// onFilter and onHeader run inside each filter fetch and each header
	// lookup by hash. A non-nil error fails the call.
	onFilter func(*dstBlock) error
	onHeader func(chainhash.Hash) error

	// gateEntered and gateRelease hold the next filter fetch until the
	// harness releases it.
	gateEntered, gateRelease chan struct{}

	subscriptions int
}

var _ ChainSource = (*dstSource)(nil)

// clearFaults disarms every fault for the heal phase.
func (s *dstSource) clearFaults() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.filterFail, s.blockFail, s.subscribeFail = 0, 0, 0
}

// ChainParams is part of the ChainSource interface.
func (s *dstSource) ChainParams() chaincfg.Params {
	return chaincfg.MainNetParams
}

// BestBlock is part of the ChainSource interface.
func (s *dstSource) BestBlock() (*headerfs.BlockStamp, error) {
	stamp := s.w.tip().stamp()
	return &stamp, nil
}

// GetBlockHeaderByHeight is part of the ChainSource interface.
func (s *dstSource) GetBlockHeaderByHeight(
	height uint32) (*wire.BlockHeader, error) {

	b, ok := s.w.at(height)
	if !ok {
		return nil, fmt.Errorf("height %d not found", height)
	}
	header := b.header
	return &header, nil
}

// GetBlockHeader is part of the ChainSource interface. Like the header
// store, it knows only the active chain: an orphaned hash is not found.
func (s *dstSource) GetBlockHeader(
	hash *chainhash.Hash) (*wire.BlockHeader, uint32, error) {

	s.mu.Lock()
	hook := s.onHeader
	s.mu.Unlock()
	if hook != nil {
		if err := hook(*hash); err != nil {
			return nil, 0, err
		}
	}

	b, ok := s.w.fx.lookup(*hash)
	if !ok || !s.w.onActive(b) {
		return nil, 0, headerfs.ErrHashNotFound
	}
	header := b.header
	return &header, b.height, nil
}

// GetBlock is part of the ChainSource interface. Peers serve any block they
// have seen, including orphans.
func (s *dstSource) GetBlock(ctx context.Context,
	hash chainhash.Hash) (*btcutil.Block, error) {

	s.mu.Lock()
	if s.blockFail > 0 {
		s.blockFail--
		s.mu.Unlock()
		return nil, errDSTFault
	}
	s.mu.Unlock()

	b, ok := s.w.fx.lookup(hash)
	if !ok {
		return nil, fmt.Errorf("block %v not found", hash)
	}
	block := btcutil.NewBlock(b.msg)
	block.SetHeight(int32(b.height))
	return block, nil
}

// GetFilterHeaderByHeight is part of the ChainSource interface. The rescan
// package never calls it, so the DST does not simulate filter headers.
func (s *dstSource) GetFilterHeaderByHeight(uint32) (*chainhash.Hash,
	error) {

	return nil, errors.New("dst does not simulate filter headers")
}

// GetCFilter is part of the ChainSource interface. Like the filter cache, it
// serves orphaned blocks' filters too.
func (s *dstSource) GetCFilter(ctx context.Context,
	hash chainhash.Hash, batch bool) (*gcs.Filter, error) {

	b, ok := s.w.fx.lookup(hash)
	if !ok {
		return nil, fmt.Errorf("filter %v not found", hash)
	}

	s.mu.Lock()
	hook := s.onFilter
	entered, release := s.gateEntered, s.gateRelease
	s.gateEntered, s.gateRelease = nil, nil
	s.mu.Unlock()
	if hook != nil {
		if err := hook(b); err != nil {
			return nil, err
		}
	}
	if entered != nil {
		close(entered)
		<-release
	}

	s.mu.Lock()
	if s.filterFail > 0 {
		s.filterFail--
		s.mu.Unlock()
		return nil, errDSTFault
	}
	s.mu.Unlock()

	return b.filter, nil
}

// Subscribe is part of the ChainSource interface. It registers with the
// real SubscriptionManager, which delivers the backlog from bestHeight.
func (s *dstSource) Subscribe(
	bestHeight uint32) (*blockntfns.Subscription, error) {

	s.mu.Lock()
	if s.subscribeFail > 0 {
		s.subscribeFail--
		s.mu.Unlock()
		return nil, errDSTFault
	}
	hook := s.onSubscribe
	s.onSubscribe = nil
	s.mu.Unlock()
	if hook != nil {
		hook()
	}

	sub, err := s.mgr.NewSubscription(bestHeight)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.subscriptions++
	s.mu.Unlock()
	return sub, nil
}

// MarkAsConfirmed is part of the ChainSource interface. The DST broadcasts
// nothing.
func (s *dstSource) MarkAsConfirmed(chainhash.Hash) {}

// IsCurrent is part of the ChainSource interface.
func (s *dstSource) IsCurrent() bool {
	return true
}
