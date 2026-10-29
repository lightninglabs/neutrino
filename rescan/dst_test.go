package rescan

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	neutrino "github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// dstChain injects faults at the ChainSource boundary used by the actor.
type dstChain struct {
	*mockChainWithTxs
	muFault                                          sync.Mutex
	filterFailures, blockFailures, subscribeFailures int
	filterEntered, filterRelease                     chan struct{}
	subscribeSuccesses                               int
	onSubscribe                                      func()

	onFilter          func(chainhash.Hash) error
	onHeader          func(chainhash.Hash) error
	headerFailures    map[chainhash.Hash]int
	headerFailureHits int
}

func (c *dstChain) GetBlockHeader(hash *chainhash.Hash) (
	*wire.BlockHeader, uint32, error) {

	c.muFault.Lock()
	hook := c.onHeader
	c.muFault.Unlock()
	if hook != nil {
		if err := hook(*hash); err != nil {
			return nil, 0, err
		}
	}
	c.muFault.Lock()
	if c.headerFailures[*hash] != 0 {
		if c.headerFailures[*hash] > 0 {
			c.headerFailures[*hash]--
		}
		c.headerFailureHits++
		c.muFault.Unlock()
		return nil, 0, headerfs.ErrHashNotFound
	}
	c.muFault.Unlock()
	return c.mockChainSource.GetBlockHeader(hash)
}

func (c *dstChain) GetCFilter(hash chainhash.Hash, typ wire.FilterType,
	opts ...neutrino.QueryOption) (*gcs.Filter, error) {

	c.muFault.Lock()
	hook := c.onFilter
	c.muFault.Unlock()
	if hook != nil {
		if err := hook(hash); err != nil {
			return nil, err
		}
	}
	c.muFault.Lock()
	entered, release := c.filterEntered, c.filterRelease
	c.filterEntered, c.filterRelease = nil, nil
	c.muFault.Unlock()
	if entered != nil {
		close(entered)
		<-release
	}
	c.muFault.Lock()
	if c.filterFailures > 0 {
		c.filterFailures--
		c.muFault.Unlock()
		return nil, errors.New("dst filter outage")
	}
	c.muFault.Unlock()
	return c.mockChainSource.GetCFilter(hash, typ, opts...)
}
func (c *dstChain) GetBlock(hash chainhash.Hash,
	opts ...neutrino.QueryOption) (*btcutil.Block, error) {

	c.muFault.Lock()
	if c.blockFailures > 0 {
		c.blockFailures--
		c.muFault.Unlock()
		return nil, errors.New("dst block outage")
	}
	c.muFault.Unlock()
	return c.mockChainSource.GetBlock(hash, opts...)
}
func (c *dstChain) Subscribe(height uint32) (*blockntfns.Subscription, error) {
	c.muFault.Lock()
	if c.subscribeFailures > 0 {
		c.subscribeFailures--
		c.muFault.Unlock()
		return nil, errors.New("dst subscribe outage")
	}
	c.subscribeSuccesses++
	hook := c.onSubscribe
	c.onSubscribe = nil
	c.muFault.Unlock()
	if hook != nil {
		hook()
	}
	return c.mockChainSource.Subscribe(height)
}

// notifyDSTReorg delivers the chain source's ordered reorg notifications.
func notifyDSTReorg(chain *dstChain, old [6]wire.BlockHeader) {
	for h := uint32(5); h >= 2; h-- {
		chain.ntfnChan <- blockntfns.NewBlockDisconnected(
			old[h], h, old[h-1],
		)
	}
	for h := uint32(2); h <= 5; h++ {
		chain.mu.Lock()
		active := *chain.blockHashesByHeight[h]
		header := *chain.blockHeaders[active]
		chain.mu.Unlock()
		chain.ntfnChan <- blockntfns.NewBlockConnected(header, h)
	}
}

type dstEvent struct {
	kind   string
	height int32
	hash   chainhash.Hash
}
type dstOracle struct {
	tip      []chainhash.Hash
	events   []dstEvent
	payments []chainhash.Hash
	err      string
}

func (o *dstOracle) connect(height int32, h *wire.BlockHeader) {
	hash := h.BlockHash()
	if int(height) != len(o.tip) || h.PrevBlock != o.tip[len(o.tip)-1] {
		o.err = fmt.Sprintf("connect %d %v does not follow tip %d %v",
			height, hash, len(o.tip)-1, o.tip[len(o.tip)-1])
		return
	}
	o.tip = append(o.tip, hash)
	o.events = append(o.events, dstEvent{"connect", height, hash})
}
func (o *dstOracle) disconnect(height int32, h *wire.BlockHeader) {
	hash := h.BlockHash()
	if int(height) != len(o.tip)-1 || hash != o.tip[len(o.tip)-1] {
		o.err = fmt.Sprintf("disconnect %d %v does not match tip %d %v",
			height, hash, len(o.tip)-1, o.tip[len(o.tip)-1])
		return
	}
	o.tip = o.tip[:len(o.tip)-1]
	o.events = append(o.events, dstEvent{"disconnect", height, hash})
}

type dstOutcome struct {
	events   []dstEvent
	payments []chainhash.Hash
	tip      chainhash.Hash
	height   int32
}

func runRescanDST(t *testing.T, script []byte) dstOutcome {
	t.Helper()
	var result dstOutcome
	synctest.Test(t, func(t *testing.T) {
		if len(script) > 16 {
			script = script[:16]
		}
		var flags byte
		if len(script) > 0 {
			flags = script[0]
		}
		chain := &dstChain{mockChainWithTxs: newMockChainWithTxs(4)}
		addr, err := address.NewAddressPubKeyHash(
			chainhash.HashB([]byte("dst-payment"))[:20],
			&chaincfg.MainNetParams,
		)
		require.NoError(t, err)
		paymentStamp, payment := chain.addBlockWithTx(addr)
		chain.addBlock()
		genesis := chain.ChainParams().GenesisBlock.Header
		oracle := &dstOracle{tip: []chainhash.Hash{genesis.BlockHash()}}
		var errorsReported int
		var historicalReorg bool
		if flags&1 != 0 {
			chain.filterFailures = 1
		}
		if flags&2 != 0 {
			chain.blockFailures = 1
		}
		if flags&16 != 0 {
			chain.subscribeFailures = 1
		}
		if flags&32 != 0 {
			// Replace the tip after catch-up, inside Subscribe.
			// A height-only backlog contains no replacement event.
			chain.onSubscribe = func() {
				chain.mu.Lock()
				defer chain.mu.Unlock()
				old := chain.bestBlock.Hash
				header := *chain.blockHeaders[old]
				header.Nonce++
				hash := header.BlockHash()
				chain.blockHeaders[hash] = &header
				height := uint32(chain.bestBlock.Height)
				chain.blockHeightIndex[hash] = height
				chain.blockHashesByHeight[height] = &hash
				block := wire.NewMsgBlock(&header)
				chain.blocks[hash] = btcutil.NewBlock(block)
				chain.filters[hash] = chain.filters[old]
				chain.bestBlock.Hash = hash
			}
		}
		if flags&64 != 0 {
			// Replace the branch during the filter fetch.
			chain.onFilter = func(hash chainhash.Hash) error {
				if hash != paymentStamp.Hash {
					return nil
				}
				historicalReorg = true
				chain.muFault.Lock()
				chain.onFilter = nil
				chain.muFault.Unlock()
				chain.mu.Lock()
				parentHeight := uint32(3)
				var oldHeaders [6]wire.BlockHeader
				if flags&128 != 0 {
					parentHeight = 1
					hashes := chain.blockHashesByHeight
					headers := chain.blockHeaders
					for h := uint32(1); h <= 5; h++ {
						hash := *hashes[h]
						oldHeaders[h] = *headers[hash]
					}
				}
				hashes := chain.blockHashesByHeight
				parent := *hashes[parentHeight]
				parentHeader := *chain.blockHeaders[parent]
				chain.bestBlock = headerfs.BlockStamp{
					Hash:      parent,
					Height:    int32(parentHeight),
					Timestamp: parentHeader.Timestamp,
				}
				chain.mu.Unlock()
				if parentHeight == 1 {
					chain.muFault.Lock()
					failures := map[chainhash.Hash]int{
						oldHeaders[2].BlockHash(): -1,
						oldHeaders[3].BlockHash(): -1,
					}
					chain.headerFailures = failures
					chain.muFault.Unlock()
					fork := chain.addBlock()
					chain.mu.Lock()
					header := *chain.blockHeaders[fork.Hash]
					header.Nonce++
					newHash := header.BlockHash()
					chain.blockHeaders[newHash] = &header
					chain.blockHeightIndex[newHash] = 2
					chain.blockHashesByHeight[2] = &newHash
					block := wire.NewMsgBlock(&header)
					wrapped := btcutil.NewBlock(block)
					chain.blocks[newHash] = wrapped
					filters := chain.filters
					filters[newHash] = filters[fork.Hash]
					chain.bestBlock.Hash = newHash
					chain.mu.Unlock()
					chain.addBlock()
				}
				replacement, _ := chain.addBlockWithTx(addr)
				chain.mu.Lock()
				oldBlock := chain.blocks[replacement.Hash]
				block := oldBlock.MsgBlock().Copy()
				block.Header.Nonce++
				newHash := block.Header.BlockHash()
				filter, err := builder.BuildBasicFilter(
					block, nil,
				)
				if err != nil {
					chain.mu.Unlock()
					return err
				}
				header := block.Header
				chain.blockHeaders[newHash] = &header
				chain.blockHeightIndex[newHash] = 4
				chain.blockHashesByHeight[4] = &newHash
				chain.blocks[newHash] = btcutil.NewBlock(block)
				chain.filters[newHash] = filter
				chain.bestBlock.Hash = newHash
				chain.mu.Unlock()
				chain.addBlock()
				if parentHeight == 1 {
					notifyDSTReorg(chain, oldHeaders)
				}
				return headerfs.ErrHashNotFound
			}
		}
		actor := NewRescanActor(ActorConfig{
			Chain: chain,
			StartStamp: headerfs.BlockStamp{
				Hash: genesis.BlockHash(),
			},
			StartHeader: genesis,
			WatchAddrs:  []address.Address{addr},
			BatchSize:   1 + int(flags%3),
			OnError:     func(error) { errorsReported++ },
			Callbacks: Callbacks{
				OnBlockConnected: func(h int32,
					hdr *wire.BlockHeader, _ time.Time) {

					oracle.connect(h, hdr)
				},
				OnBlockDisconnected: func(h int32,
					hdr *wire.BlockHeader) {

					oracle.disconnect(h, hdr)
				},
				OnFilteredBlockConnected: func(_ int32,
					_ *wire.BlockHeader,
					txs []*btcutil.Tx) {

					for _, tx := range txs {
						oracle.payments = append(
							oracle.payments,
							*tx.Hash(),
						)
					}
				},
			},
		})
		actor.Start()
		defer func() { actor.Stop(); actor.WaitForShutdown() }()
		synctest.Wait()
		for i := 0; i < 4; i++ {
			time.Sleep(filterRetryInterval)
			synctest.Wait()
		}
		if flags&64 != 0 {
			require.True(t, historicalReorg, "missing reorg")
		}
		chain.muFault.Lock()
		subscriptions := chain.subscribeSuccesses
		chain.muFault.Unlock()
		require.Positive(
			t,
			subscriptions,
			"subscription did not recover, seed %x",
			script,
		)
		state, err := actor.CurrentState()
		require.NoError(t, err)
		cur, ok := state.(*StateCurrent)
		require.True(t, ok, "recovery state %T, seed %x", state, script)
		require.Equal(
			t,
			int32(5),
			cur.CurStamp.Height,
			"seed %x",
			script,
		)
		bestAtHandoff, err := chain.BestBlock()
		require.NoError(t, err)
		require.Equal(
			t,
			bestAtHandoff.Hash,
			cur.CurStamp.Hash,
			"handoff tip, seed %x",
			script,
		)
		require.Empty(t, oracle.err, "seed %x", script)
		require.Contains(
			t,
			oracle.payments,
			payment.TxHash(),
			"missed payment at %d, seed %x",
			paymentStamp.Height,
			script,
		)
		if flags&8 != 0 {
			chain.mu.Lock()
			hashes := chain.blockHashesByHeight
			headers := chain.blockHeaders
			old3 := *headers[*hashes[3]]
			old4 := *headers[*hashes[4]]
			old5 := *headers[*hashes[5]]
			chain.mu.Unlock()
			chain.muFault.Lock()
			chain.onHeader = func(hash chainhash.Hash) error {
				if hash != old4.BlockHash() {
					return nil
				}
				chain.muFault.Lock()
				chain.onHeader = nil
				chain.headerFailureHits++
				chain.headerFailures = map[chainhash.Hash]int{
					old4.BlockHash(): -1,
					old5.BlockHash(): -1,
				}
				chain.muFault.Unlock()
				chain.mu.Lock()
				chain.bestBlock = headerfs.BlockStamp{
					Hash: old3.BlockHash(), Height: 3,
					Timestamp: old3.Timestamp,
				}
				chain.mu.Unlock()
				replacement, _ := chain.addBlockWithTx(addr)
				chain.mu.Lock()
				oldBlock := chain.blocks[replacement.Hash]
				block := oldBlock.MsgBlock().Copy()
				block.Header.Nonce = old4.Nonce + 1
				hdr := block.Header
				newHash := hdr.BlockHash()
				if newHash == old4.BlockHash() {
					chain.mu.Unlock()
					return fmt.Errorf(
						"replacement equals orphan %s",
						newHash,
					)
				}
				filter, err := builder.BuildBasicFilter(
					block, nil,
				)
				if err != nil {
					chain.mu.Unlock()
					return err
				}
				chain.blockHeaders[newHash] = &hdr
				chain.blockHeightIndex[newHash] = 4
				chain.blockHashesByHeight[4] = &newHash
				chain.blocks[newHash] = btcutil.NewBlock(block)
				chain.filters[newHash] = filter
				chain.bestBlock.Hash = newHash
				chain.mu.Unlock()
				next := chain.addBlock()
				disconnect := blockntfns.NewBlockDisconnected
				chain.ntfnChan <- disconnect(old5, 5, old4)
				chain.ntfnChan <- disconnect(old4, 4, old3)
				chain.ntfnChan <- blockntfns.NewBlockConnected(
					hdr, 4,
				)
				chain.ntfnChan <- blockntfns.NewBlockConnected(
					*chain.blockHeaders[next.Hash], 5,
				)
				return headerfs.ErrHashNotFound
			}
			chain.muFault.Unlock()
		}
		rewind := uint32(2)
		require.NoError(t, actor.AddWatchAddrs(nil, nil, &rewind))
		synctest.Wait()
		for i := 0; i < 2; i++ {
			time.Sleep(filterRetryInterval)
			synctest.Wait()
		}
		require.Empty(t, oracle.err, "seed %x", script)
		if flags&8 != 0 {
			chain.muFault.Lock()
			hits := chain.headerFailureHits
			chain.muFault.Unlock()
			require.Positive(t, hits, "missing rewind header fault")
		}
		require.Len(
			t,
			oracle.payments,
			2,
			"rewind missed payment, seed %x",
			script,
		)
		state, err = actor.CurrentState()
		require.NoError(t, err)
		cur, ok = state.(*StateCurrent)
		require.True(t, ok, "state %T", state)
		require.Equal(t, int32(5), cur.CurStamp.Height)
		// Replace a live block, possibly with a retry pending.
		base := chain.bestBlock
		baseHeader := *chain.blockHeaders[base.Hash]
		old := chain.addBlock()
		oldHeader := *chain.blockHeaders[old.Hash]
		if flags&4 != 0 {
			chain.muFault.Lock()
			chain.filterFailures = 1
			chain.muFault.Unlock()
		}
		chain.ntfnChan <- blockntfns.NewBlockConnected(
			oldHeader,
			uint32(old.Height),
		)
		synctest.Wait()
		chain.mu.Lock()
		chain.bestBlock = base
		chain.mu.Unlock()
		replacement := chain.addBlock()
		chain.mu.Lock()
		header := *chain.blockHeaders[replacement.Hash]
		header.Nonce++
		newHash := header.BlockHash()
		chain.blockHeaders[newHash] = &header
		chain.blockHeightIndex[newHash] = uint32(replacement.Height)
		chain.blockHashesByHeight[uint32(replacement.Height)] = &newHash
		chain.blocks[newHash] = btcutil.NewBlock(
			wire.NewMsgBlock(&header),
		)
		chain.filters[newHash] = chain.filters[replacement.Hash]
		chain.bestBlock.Hash = newHash
		chain.mu.Unlock()
		chain.ntfnChan <- blockntfns.NewBlockDisconnected(
			oldHeader,
			uint32(old.Height),
			baseHeader,
		)
		synctest.Wait()
		chain.ntfnChan <- blockntfns.NewBlockConnected(
			header,
			uint32(replacement.Height),
		)
		synctest.Wait()
		errorsAtRecovery := errorsReported
		for i := 0; i < 3; i++ {
			time.Sleep(filterRetryInterval)
			synctest.Wait()
		}
		require.Empty(
			t,
			oracle.err,
			"reorg callback continuity, seed %x",
			script,
		)
		require.Equal(
			t,
			errorsAtRecovery,
			errorsReported,
			"obsolete retry after recovery, seed %x",
			script,
		)
		state, err = actor.CurrentState()
		require.NoError(t, err)
		cur, ok = state.(*StateCurrent)
		require.True(
			t,
			ok,
			"reorg recovery state %T, seed %x",
			state,
			script,
		)
		require.Equal(
			t,
			newHash,
			cur.CurStamp.Hash,
			"stale retry changed tip, seed %x",
			script,
		)
		require.Equal(t, replacement.Height, cur.CurStamp.Height)
		// Each byte chooses a stimulus. Updates can overtake retries.
		for _, action := range script {
			switch action % 4 {
			case 0:
				state, err := actor.CurrentState()
				require.NoError(t, err)
				current, currentOK := state.(*StateCurrent)
				best, err := chain.BestBlock()
				require.NoError(t, err)
				gate := action&128 != 0 && currentOK &&
					current.CurStamp.Hash == best.Hash
				var entered, release chan struct{}
				if gate {
					entered = make(chan struct{})
					release = make(chan struct{})
				}
				chain.muFault.Lock()
				chain.filterEntered = entered
				chain.filterRelease = release
				chain.filterFailures = int((action >> 2) % 3)
				chain.muFault.Unlock()
				stamp := chain.addBlock()
				hdr := *chain.blockHeaders[stamp.Hash]
				chain.ntfnChan <- blockntfns.NewBlockConnected(
					hdr,
					uint32(stamp.Height),
				)
				if gate {
					<-entered
					done := make(chan error, 1)
					go func() {
						rewind := uint32(2)
						done <- actor.AddWatchAddrs(
							nil,
							nil,
							&rewind,
						)
					}()
					synctest.Wait()
					close(release)
					require.NoError(t, <-done)
				}
			case 1:
				time.Sleep(filterRetryInterval)
			case 2:
				// Rewind even when a retry is outstanding.
				rewind := uint32(2)
				require.NoError(
					t,
					actor.AddWatchAddrs(nil, nil, &rewind),
				)
			case 3:
				require.NoError(
					t,
					actor.AddWatchAddrs(nil, nil, nil),
				)
			}
			synctest.Wait()
			// Separate retry deadlines from successive stimuli.
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			require.Empty(
				t,
				oracle.err,
				"callback continuity after action %x, seed %x",
				action,
				script,
			)
		}
		chain.muFault.Lock()
		chain.filterFailures = 0
		chain.blockFailures = 0
		chain.muFault.Unlock()
		for i := 0; i < 20; i++ {
			time.Sleep(filterRetryInterval)
			synctest.Wait()
		}
		require.Empty(
			t,
			oracle.err,
			"callback continuity after recovery, seed %x",
			script,
		)
		state, err = actor.CurrentState()
		require.NoError(t, err)
		cur, ok = state.(*StateCurrent)
		require.True(t, ok, "final state %T, seed %x", state, script)
		best, err := chain.BestBlock()
		require.NoError(t, err)
		require.Equal(
			t,
			best.Hash,
			cur.CurStamp.Hash,
			"final tip, seed %x",
			script,
		)
		require.Equal(
			t,
			best.Height,
			cur.CurStamp.Height,
			"final height, seed %x",
			script,
		)
		require.Equal(
			t,
			cur.CurStamp.Hash,
			oracle.tip[len(oracle.tip)-1],
			"callback tip, seed %x",
			script,
		)
		result = dstOutcome{
			events: append([]dstEvent(nil), oracle.events...),
			payments: append(
				[]chainhash.Hash(nil), oracle.payments...,
			),
			tip:    cur.CurStamp.Hash,
			height: cur.CurStamp.Height,
		}
	})
	return result
}

var dstSeeds = [][]byte{
	{0}, {1}, {2}, {3}, {4}, {7, 4}, {8, 2, 1, 0},
	{12, 0, 1, 2, 3}, {128}, {132, 1}, {8}, {16}, {32}, {64}, {192},
}

func TestRescanDST(t *testing.T) {
	for _, seed := range dstSeeds {
		t.Run(fmt.Sprintf("%x", seed), func(t *testing.T) {
			first := runRescanDST(t, seed)
			require.Equal(t, first, runRescanDST(t, seed))
		})
	}
}
func FuzzRescanDST(f *testing.F) {
	for _, seed := range dstSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed []byte) {
		first := runRescanDST(t, seed)
		require.Equal(t, first, runRescanDST(t, seed))
	})
}
