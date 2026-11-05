package neutrino

import (
	"fmt"
	"math/rand"
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
	"github.com/stretchr/testify/require"
)

// producerConfig bounds every generated schedule and makes fuzz inputs
// replayable.
type producerConfig struct {
	filterFault, blockFault, filterRollback, blockRollback int
	oldHeight                                              int
	delay                                                  time.Duration
	early                                                  bool
}

func producerConfigForSeed(seed int64) producerConfig {
	rng := rand.New(rand.NewSource(seed))
	c := producerConfig{
		filterFault:    rng.Intn(4),
		blockFault:     rng.Intn(4),
		filterRollback: rng.Intn(3),
		blockRollback:  rng.Intn(3),
		oldHeight:      dstReorgFork + 1 + rng.Intn(4),
		delay:          time.Duration(rng.Intn(5)) * time.Millisecond,
		early:          rng.Intn(2) == 1,
	}
	if seed == 16 {
		c = producerConfig{filterFault: 1, oldHeight: 12, early: true}
	}
	return c
}

// TestCFHandlerGeneratedProducer runs bounded reproducible configurations.
func TestCFHandlerGeneratedProducer(t *testing.T) {
	for seed := int64(0); seed < 65; seed++ {
		t.Run(
			fmt.Sprintf("seed_%d", seed),
			func(t *testing.T) {
				runCFHandlerProducer(
					t, producerConfigForSeed(seed),
				)
			},
		)
	}
}

// FuzzCFHandlerProducer explores bounded fault combinations and timing while
// using the same real-handler scenario as the deterministic campaign.
func FuzzCFHandlerProducer(f *testing.F) {
	for seed := int64(0); seed < 65; seed++ {
		c := producerConfigForSeed(seed)
		early := uint8(0)
		if c.early {
			early = 1
		}
		f.Add(
			uint8(c.filterFault),
			uint8(c.blockFault),
			uint8(c.filterRollback),
			uint8(c.blockRollback),
			uint8(c.oldHeight-dstReorgFork-1),
			uint8(c.delay/time.Millisecond),
			early,
		)
	}
	f.Fuzz(
		func(
			t *testing.T, filterWrite, blockWrite, filterRollback,
			blockRollback, height, delay, early uint8,
		) {

			c := producerConfig{
				filterFault:    int(filterWrite % 4),
				blockFault:     int(blockWrite % 4),
				filterRollback: int(filterRollback % 3),
				blockRollback:  int(blockRollback % 3),
				oldHeight: dstReorgFork + 1 + int(
					height%4,
				),
				delay: time.Duration(
					delay%5,
				) * time.Millisecond,
				early: early%2 == 1,
			}
			runCFHandlerProducer(t, c)
		},
	)
}

func runCFHandlerProducer(t *testing.T, c producerConfig) {
	enableDSTLog()
	t.Helper()
	filterFault, blockFault := c.filterFault, c.blockFault
	filterRollback, blockRollback := c.filterRollback, c.blockRollback
	oldHeight, delay, early := c.oldHeight, c.delay, c.early
	t.Logf(
		"filter_write=%d block_write=%d filter_rollback=%d "+
			"block_rollback=%d old_height=%d delay=%s early=%t",
		filterFault,
		blockFault,
		filterRollback,
		blockRollback,
		oldHeight,
		delay,
		early,
	)
	fixture := dstChains()
	peer := swarmConnectedPeer(t, int32(len(fixture.reorg)-1))
	synctest.Test(t, func(t *testing.T) {
		params := chaincfg.SimNetParams
		params.Checkpoints = nil
		blocks := &swarmFaultBlocks{
			BlockHeaderStore: newMemBlockHeaderStore(&params),
		}
		base, err := newMemFilterHeaderStore(&params)
		require.NoError(t, err)
		filters := &swarmFaultFilter{
			FilterHeaderStore: base,
			mode:              filterFault,
		}
		initial := make([]headerfs.BlockHeader, 0, oldHeight)
		for h := 1; h <= oldHeight; h++ {
			initial = append(
				initial,
				headerfs.BlockHeader{
					BlockHeader: fixture.honest[h],
					Height:      uint32(h),
				},
			)
		}
		require.NoError(t, blocks.WriteHeaders(initial...))
		genesis, _, err := filters.ChainTip()
		require.NoError(t, err)
		hashes := make(map[chainhash.Hash]chainhash.Hash)
		headers := map[chainhash.Hash]chainhash.Hash{
			fixture.honest[0].BlockHash(): *genesis,
		}
		for branchIndex, branch := range [][]*wire.BlockHeader{
			fixture.honest, fixture.reorg,
		} {
			seen := make(map[chainhash.Hash]bool)
			for i, block := range branch[1:] {
				id := block.BlockHash()
				require.False(
					t,
					seen[id],
					"duplicate branch hash",
				)
				seen[id] = true
				if _, ok := hashes[id]; ok {
					require.Equal(t, 1, branchIndex)
					require.LessOrEqual(
						t,
						i+1,
						dstReorgFork,
					)
					require.Equal(
						t,
						fixture.honest[i+1].BlockHash(),
						id,
					)
					continue
				}
				filter := chainhash.DoubleHashH(id[:])
				prev := headers[block.PrevBlock]
				hashes[id] = filter
				headers[id] = chainhash.DoubleHashH(
					append(filter[:], prev[:]...),
				)
			}
		}
		bm := newCFHeaderTestManager(t, blocks, filters)
		bm.cfg.ChainParams = params
		bm.cfg.TimeSource = dstMedianTime{
			MedianTimeSource: bm.cfg.TimeSource,
			offset: time.Until(
				params.GenesisBlock.Header.Timestamp.Add(
					time.Hour,
				),
			),
		}
		ready := make(chan struct{})
		close(ready)
		bm.cfg.firstPeerSignal = ready
		bm.cfg.Broadcaster = broadcastFunc(
			func(
				b *querysyncwm.Broadcast,
				opts ...querysyncwm.QueryOption,
			) error {

				time.Sleep(delay)
				req, ok := b.Msg.(*wire.MsgGetCFHeaders)
				require.True(t, ok)
				branch := fixture.honest
				reorgHeaders := fixture.reorg[dstReorgFork+1:]
				for _, block := range reorgHeaders {
					if block.BlockHash() == req.StopHash {
						branch = fixture.reorg
						break
					}
				}
				stop := -1
				for i, block := range branch {
					if block.BlockHash() == req.StopHash {
						stop = i
						break
					}
				}
				require.GreaterOrEqual(
					t,
					stop,
					int(req.StartHeight),
				)
				previousBlock := branch[req.StartHeight-1]
				predecessor := previousBlock.BlockHash()
				previous := headers[predecessor]
				response := &wire.MsgCFHeaders{
					FilterType:       req.FilterType,
					StopHash:         req.StopHash,
					PrevFilterHeader: previous,
				}
				selected := branch[req.StartHeight : stop+1]
				for _, block := range selected {
					hash := hashes[block.BlockHash()]
					response.FilterHashes = append(
						response.FilterHashes,
						&hash,
					)
				}
				return newScriptedBroadcaster(
					map[string][]wire.Message{
						"honest": {response},
					},
				).Broadcast(b, opts...)
			},
		)
		notifications := make(chan blockntfns.BlockNtfn, 128)
		go func() {
			for {
				select {
				case n := <-bm.blockNtfnChan:
					notifications <- n
				case <-bm.quit:
					return
				}
			}
		}()
		if early {
			filters.gateWrite = true
			filters.gateEntered, filters.gateRelease = make(
				chan struct{},
			), make(
				chan struct{},
			)
		}
		bm.Start()
		defer func() { require.NoError(t, bm.Stop()) }()
		if early {
			defer func() {
				select {
				case <-filters.gateRelease:
				default:
					close(filters.gateRelease)
				}
			}()
		}
		check := func() {
			_, bh, err := blocks.ChainTip()
			require.NoError(t, err)
			_, fh, err := filters.ChainTip()
			require.NoError(t, err)
			require.LessOrEqual(t, fh, bh)
			for h := uint32(1); h <= fh; h++ {
				block, err := blocks.FetchHeaderByHeight(h)
				require.NoError(t, err)
				id := block.BlockHash()
				got, err := filters.FetchHeader(&id)
				require.NoError(t, err)
				require.Equal(t, headers[id], *got)
			}
			bm.newFilterHeadersMtx.RLock()
			published := bm.filterHeaderTip
			publishedHash := bm.filterHeaderTipHash
			bm.newFilterHeadersMtx.RUnlock()
			require.LessOrEqual(t, published, fh)
			block, err := blocks.FetchHeaderByHeight(published)
			require.NoError(t, err)
			require.Equal(t, block.BlockHash(), publishedHash)
		}
		synctest.Wait()
		if early {
			time.Sleep(10 * time.Millisecond)
			synctest.Wait()
			require.Nil(
				t,
				filters.gateEntered,
				"filter write gate was not reached",
			)
		} else {
			time.Sleep(4 * retryTimeout)
			synctest.Wait()
		}
		check()
		_, height, err := filters.ChainTip()
		require.NoError(t, err)
		if !early {
			require.Equal(t, uint32(oldHeight), height)
		}
		transport := &dstHeaderTransport{
			chain:   fixture.reorg,
			blocked: make(map[string]bool),
			cap:     wire.MaxBlockHeadersPerMsg,
			best:    uint32(len(fixture.reorg) - 1),
		}
		bm.cfg.HeaderTransport = transport
		bm.syncPeerMutex.Lock()
		bm.syncPeer = peer
		bm.syncPeerMutex.Unlock()
		bm.NewPeer(peer)
		synctest.Wait()
		_, registered := bm.headerSync.Peer(peer.Addr())
		require.True(t, registered)
		blocks.writeMode = blockFault
		blocks.rollbackMode = blockRollback
		filters.rollbackMode = filterRollback
		reorg := wire.NewMsgHeaders()
		reorgHeaders := fixture.reorg[dstReorgFork+1:]
		for _, block := range reorgHeaders {
			require.NoError(t, reorg.AddBlockHeader(block))
		}
		if early {
			queued := make(chan struct{})
			started := make(chan struct{})
			go func() {
				close(started)
				bm.QueueHeaders(reorg, peer)
				close(queued)
			}()
			synctest.Wait()
			select {
			case <-started:
			default:
				t.Fatal(
					"reorg was not scheduled while " +
						"filter write was held",
				)
			}
			close(filters.gateRelease)
			synctest.Wait()
			<-queued
		} else {
			bm.QueueHeaders(reorg, peer)
			synctest.Wait()
		}
		check()
		for step := 0; step < 12; step++ {
			time.Sleep(retryTimeout)
			synctest.Wait()
			check()
			request, ok := transport.take(peer)
			if !ok {
				continue
			}
			response := wire.NewMsgHeaders()
			for h := request.start + 1; h <= request.stop; h++ {
				require.NoError(
					t,
					response.AddBlockHeader(
						fixture.reorg[h],
					),
				)
			}
			bm.QueueHeaders(response, peer)
			synctest.Wait()
			check()
		}
		if filterFault != 0 {
			require.Zero(
				t,
				filters.mode,
				"filter write fault was not reached",
			)
			require.Equal(
				t,
				1,
				filters.writes,
				"filter write fault count",
			)
			if filterFault == 3 {
				require.Greater(
					t,
					filters.faultBatch,
					1,
					"partial filter write needs a batch",
				)
			}
		}
		if blockFault != 0 {
			require.Equal(
				t,
				1,
				blocks.writes,
				"block write fault was not reached",
			)
		}
		if blockFault == 3 {
			require.Greater(
				t,
				blocks.faultBatch,
				1,
				"partial block write needs a batch",
			)
		}
		if filterRollback != 0 {
			// A fail-before or short partial prefix in the held
			// initial write leaves the filter tip below the fork,
			// so there is no rollback call.
			unreachable := early &&
				(filterFault == 1 || filterFault == 3)
			if unreachable {
				require.Zero(
					t,
					filters.rollbacks,
					"rollback should be unreachable "+
						"below the fork",
				)
				require.Equal(
					t,
					filterRollback,
					filters.rollbackMode,
					"unreached fault must remain armed",
				)
				t.Log(
					"filter rollback fault unreachable: " +
						"filter tip did not reach fork",
				)
			} else {
				require.Equal(
					t,
					1,
					filters.rollbacks,
					"filter rollback fault was not reached",
				)
			}
		}
		if blockRollback != 0 {
			require.Equal(
				t,
				1,
				blocks.rollbacks,
				"block rollback fault was not reached",
			)
		}
		_, height, err = filters.ChainTip()
		require.NoError(t, err)
		require.Equal(
			t,
			uint32(len(fixture.reorg)-1),
			height,
			"healed canonical tip",
		)
		publishedChain := []chainhash.Hash{
			fixture.honest[0].BlockHash(),
		}
		expectedDisconnect := oldHeight
		sawDisconnect := false
		for len(notifications) > 0 {
			n := <-notifications
			header := n.Header()
			id := header.BlockHash()
			switch event := n.(type) {
			case *blockntfns.Connected:
				h := len(publishedChain)
				require.Equal(
					t,
					uint32(h),
					n.Height(),
					"connected height order",
				)
				require.Equal(
					t,
					publishedChain[h-1],
					header.PrevBlock,
					"connected parent order",
				)
				if sawDisconnect {
					require.Equal(
						t,
						dstReorgFork,
						expectedDisconnect,
						"all old disconnects precede "+
							"replacement connects",
					)
					require.Less(t, h, len(fixture.reorg))
					require.Equal(
						t,
						fixture.reorg[h].BlockHash(),
						id,
						"canonical connected hash",
					)
				} else {
					require.LessOrEqual(t, h, oldHeight)
					require.Equal(
						t,
						fixture.honest[h].BlockHash(),
						id,
						"old connected hash",
					)
				}
				publishedChain = append(publishedChain, id)
			case *blockntfns.Disconnected:
				sawDisconnect = true
				h := expectedDisconnect
				require.Greater(
					t,
					h,
					dstReorgFork,
					"duplicate disconnect",
				)
				require.Equal(
					t,
					uint32(h),
					n.Height(),
					"descending disconnected height",
				)
				require.Equal(
					t,
					fixture.honest[h].BlockHash(),
					id,
					"orphan hash",
				)
				previous := event.ChainTip()
				require.Equal(
					t,
					fixture.honest[h-1].BlockHash(),
					previous.BlockHash(),
					"orphan parent",
				)
				if h < len(publishedChain) {
					require.Equal(
						t,
						len(publishedChain)-1,
						h,
						"published disconnect height",
					)
					require.Equal(
						t,
						publishedChain[h],
						id,
						"published disconnect hash",
					)
					publishedChain = publishedChain[:h]
				}
				expectedDisconnect--
			default:
				t.Fatalf("unexpected notification %T", n)
			}
		}
		require.True(t, sawDisconnect)
		require.Equal(
			t,
			dstReorgFork,
			expectedDisconnect,
			"all orphan blocks disconnected",
		)
		require.Len(t, publishedChain, len(fixture.reorg))
		for h, block := range fixture.reorg {
			require.Equal(t, block.BlockHash(), publishedChain[h])
		}
	})
}
