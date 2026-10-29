package neutrino

import (
	"context"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/cache/lru"
	"github.com/lightninglabs/neutrino/filterdb"
	"github.com/lightninglabs/neutrino/headerfs"
	querywm "github.com/lightninglabs/neutrino/querysync/workmanager"
	"github.com/stretchr/testify/require"
)

type cancellationFilterDB struct{}

func (cancellationFilterDB) FetchFilter(*chainhash.Hash,
	filterdb.FilterType) (*gcs.Filter, error) {

	return nil, filterdb.ErrFilterNotFound
}
func (cancellationFilterDB) PutFilters(...*filterdb.FilterData) error {
	return nil
}
func (cancellationFilterDB) PurgeFilters(filterdb.FilterType) error {
	return nil
}

// TestChainServiceQueryCancellation checks caller and service cancellation
// reach the dispatcher for production block and compact filter requests.
func TestChainServiceQueryCancellation(t *testing.T) {
	for _, kind := range []string{"block", "filter"} {
		for _, shutdown := range []bool{false, true} {
			name := kind + map[bool]string{
				false: "/caller", true: "/shutdown",
			}[shutdown]
			t.Run(name, func(t *testing.T) {
				testChainServiceCancellation(t, kind, shutdown)
			})
		}
	}
}

func testChainServiceCancellation(t *testing.T, kind string, shutdown bool) {
	t.Helper()
	header := &wire.BlockHeader{Nonce: 1}
	hash := header.BlockHash()
	blocks := &headerfs.MockBlockHeaderStore{}
	blocks.On("FetchHeader", &hash).Return(header, uint32(1), nil)
	blocks.On("ChainTip").Return(header, uint32(1), nil)
	blocks.On("FetchHeaderByHeight", uint32(1)).Return(header, nil)
	blocks.On("FetchHeaderAncestors", uint32(1), &hash).Return(
		[]wire.BlockHeader{{}, *header}, uint32(0), nil,
	)
	filters := &headerfs.MockFilterHeaderStore{}
	filters.On("ChainTip").Return(&chainhash.Hash{}, uint32(1), nil)
	filters.On("FetchHeaderAncestors", uint32(1), &hash).Return(
		[]chainhash.Hash{{}, {}}, uint32(0), nil,
	)
	quit := make(chan struct{})
	caller := make(chan struct{})
	started := make(chan struct{})
	retired := make(chan struct{})
	peers := make(chan querywm.Peer)
	wm := querywm.NewWorkManager(&querywm.Config{
		ConnectedPeers: func() (<-chan querywm.Peer, func(), error) {
			return peers, func() {}, nil
		},
	})
	require.NoError(t, wm.Start())
	t.Cleanup(func() { require.NoError(t, wm.Stop()) })
	dispatch := &mockDispatcher{query: func(req []*querywm.Request,
		opts ...querywm.QueryOption) chan error {

		underlying := wm.Query(req, opts...)
		errChan := make(chan error, 1)
		close(started)
		go func() {
			errChan <- <-underlying
			close(retired)
		}()
		return errChan
	}}
	cs := &ChainService{
		quit: quit, BlockHeaders: blocks, RegFilterHeaders: filters,
		FilterDB: cancellationFilterDB{}, workManager: dispatch,
		BlockCache: lru.NewCache[wire.InvVect, *CacheableBlock](1024),
		FilterCache: lru.NewCache[FilterCacheKey, *CacheableFilter](
			1024,
		),
	}
	result := make(chan error, 1)
	go func() {
		if kind == "block" {
			_, err := cs.GetBlock(hash, CancelQuery(caller))
			result <- err
		} else {
			_, err := cs.GetCFilter(
				hash, wire.GCSFilterRegular,
				CancelQuery(caller),
			)
			result <- err
		}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("query not started")
	}
	want := context.Canceled
	if shutdown {
		close(quit)
		want = ErrShuttingDown
	} else {
		close(caller)
	}
	select {
	case err := <-result:
		require.ErrorIs(t, err, want)
	case <-time.After(time.Second):
		t.Fatal("query did not return")
	}
	select {
	case <-retired:
	case <-time.After(time.Second):
		t.Fatal("dispatcher not canceled")
	}
}

// TestFilterCancellationWhileWaiting checks that an unrelated in-flight
// filter request cannot prevent a caller from stopping.
func TestFilterCancellationWhileWaiting(t *testing.T) {
	sem := make(chan struct{}, 1)
	sem <- struct{}{}
	cs := &ChainService{
		quit: make(chan struct{}), filterQuerySem: sem,
		FilterDB: cancellationFilterDB{},
		FilterCache: lru.NewCache[FilterCacheKey, *CacheableFilter](
			1024,
		),
	}
	cancel := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := cs.GetCFilter(chainhash.Hash{}, wire.GCSFilterRegular,
			CancelQuery(cancel))
		result <- err
	}()
	close(cancel)
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("filter query did not stop while waiting for the slot")
	}
	<-sem
}
