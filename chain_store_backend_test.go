package neutrino

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// TestChainStoreBackendBlockMutation verifies durable parent checks and the
// visible snapshot around a block write and paired rollback.
func TestChainStoreBackendBlockMutation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := newMemBlockHeaderStore(params)
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		bm := newCFHeaderTestManager(t, blocks, filters)
		client := bm.chainStore
		notifications := make(chan blockntfns.BlockNtfn, 1)
		go func() {
			select {
			case n := <-bm.blockNtfnChan:
				notifications <- n
			case <-bm.quit:
			}
		}()

		genesis := params.GenesisBlock.Header
		header := wire.BlockHeader{
			PrevBlock: genesis.BlockHash(),
			Timestamp: genesis.Timestamp.Add(time.Minute),
		}
		request := chainStoreBlockCommit{
			ExpectedParent: headerfs.BlockStamp{
				Height: 0, Hash: genesis.BlockHash(),
			},
			Headers: []headerfs.BlockHeader{{
				BlockHeader: &header, Height: 1,
			}},
		}
		result := client.ask(t.Context(), request)
		require.False(t, result.IsErr())
		reply := result.UnwrapOr(chainStoreReply{})
		require.NoError(t, reply.Err)
		require.Equal(t, header.BlockHash(), reply.BlockTip.Hash)
		require.Equal(t, int32(1), reply.BlockTip.Height)
		visible := bm.publishedBlockTip.Load()
		require.Equal(t, reply.BlockTip, *visible)

		// A second command prepared on the old parent is rejected.
		result = client.ask(t.Context(), request)
		require.False(t, result.IsErr())
		require.Error(t, result.UnwrapOr(chainStoreReply{}).Err)
		_, height, err := blocks.ChainTip()
		require.NoError(t, err)
		require.Equal(t, uint32(1), height)

		result = client.ask(t.Context(), chainStoreRollback{
			ExpectedTip: reply.BlockTip, Height: 0,
		})
		require.False(t, result.IsErr())
		rolledBack := result.UnwrapOr(chainStoreReply{})
		require.NoError(t, rolledBack.Err)
		require.Equal(t, genesis.BlockHash(), rolledBack.BlockTip.Hash)
		visible = bm.publishedBlockTip.Load()
		require.Equal(t, rolledBack.BlockTip, *visible)
		n := <-notifications
		_, ok := n.(*blockntfns.Disconnected)
		require.True(t, ok)
		require.Equal(t, uint32(1), n.Height())
	})
}

// chainStoreBlockedTip detects a status path that tries to read the store.
type chainStoreBlockedTip struct {
	headerfs.BlockHeaderStore
	armed   bool
	entered chan struct{}
	release chan struct{}
}

func (s *chainStoreBlockedTip) ChainTip() (*wire.BlockHeader, uint32, error) {
	if s.armed {
		close(s.entered)
		<-s.release
	}
	return s.BlockHeaderStore.ChainTip()
}

// TestBlockHeadersSyncedUsesPublishedTip checks that status is independent of
// a blocked store read. This does not model a SQL transaction lock.
func TestBlockHeadersSyncedUsesPublishedTip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := &chainStoreBlockedTip{
			BlockHeaderStore: newMemBlockHeaderStore(params),
			entered:          make(chan struct{}),
			release:          make(chan struct{}),
		}
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		bm := newCFHeaderTestManager(t, blocks, filters)
		blocks.armed = true
		done := make(chan bool, 1)
		go func() { done <- bm.BlockHeadersSynced() }()
		synctest.Wait()
		select {
		case <-done:
		case <-blocks.entered:
			close(blocks.release)
			<-done
			t.Fatal("status attempted a store tip read")
		default:
			t.Fatal("status did not return")
		}
	})
}
