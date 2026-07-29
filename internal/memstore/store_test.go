package memstore

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/filterdb"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

func TestNewStoresInitializesGenesis(t *testing.T) {
	t.Parallel()

	stores, err := New(&chaincfg.SimNetParams)
	require.NoError(t, err)

	blockTip, blockHeight, err := stores.BlockHeaders.ChainTip()
	require.NoError(t, err)
	require.Zero(t, blockHeight)
	require.Equal(t, chaincfg.SimNetParams.GenesisBlock.Header, *blockTip)

	genesisFilter, err := builder.BuildBasicFilter(
		chaincfg.SimNetParams.GenesisBlock, nil,
	)
	require.NoError(t, err)
	storedFilter, err := stores.FilterDB.FetchFilter(
		chaincfg.SimNetParams.GenesisHash, filterdb.RegularFilter,
	)
	require.NoError(t, err)
	require.Equal(t, genesisFilter, storedFilter)

	expectedFilterHeader, err := builder.MakeHeaderForFilter(
		genesisFilter,
		chaincfg.SimNetParams.GenesisBlock.Header.PrevBlock,
	)
	require.NoError(t, err)
	filterTip, filterHeight, err := stores.RegFilterHeaders.ChainTip()
	require.NoError(t, err)
	require.Zero(t, filterHeight)
	require.Equal(t, expectedFilterHeader, *filterTip)
}

func TestNewStoresRequiresParams(t *testing.T) {
	t.Parallel()

	_, err := New(nil)
	require.EqualError(t, err, "chain parameters are required")
}

func testBlockHeaders(count uint32) []headerfs.BlockHeader {
	headers := make([]headerfs.BlockHeader, count)
	prevHash := *chaincfg.SimNetParams.GenesisHash

	for i := uint32(0); i < count; i++ {
		header := chaincfg.SimNetParams.GenesisBlock.Header
		header.PrevBlock = prevHash
		header.Timestamp = header.Timestamp.Add(
			time.Duration(i+1) * time.Minute,
		)
		header.Nonce += i + 1
		headers[i] = headerfs.BlockHeader{
			BlockHeader: &header,
			Height:      i + 1,
		}
		prevHash = header.BlockHash()
	}

	return headers
}

func TestBlockHeaderStoreOperations(t *testing.T) {
	t.Parallel()

	store := NewBlockHeaderStore(&chaincfg.SimNetParams)
	headers := testBlockHeaders(20)
	require.NoError(t, store.WriteHeaders(headers...))

	tip, height, err := store.ChainTip()
	require.NoError(t, err)
	require.EqualValues(t, 20, height)
	require.Equal(t, *headers[19].BlockHeader, *tip)

	tipHash := tip.BlockHash()
	fetched, fetchedHeight, err := store.FetchHeader(&tipHash)
	require.NoError(t, err)
	require.Equal(t, tip, fetched)
	require.Equal(t, height, fetchedHeight)

	ancestors, startHeight, err := store.FetchHeaderAncestors(3, &tipHash)
	require.NoError(t, err)
	require.EqualValues(t, 17, startHeight)
	require.Len(t, ancestors, 4)
	require.Equal(t, *headers[16].BlockHeader, ancestors[0])

	locator, err := store.LatestBlockLocator()
	require.NoError(t, err)
	require.Equal(t, tipHash, *locator[0])
	require.Equal(t, *chaincfg.SimNetParams.GenesisHash,
		*locator[len(locator)-1])

	missing := chainhash.HashH([]byte("missing"))
	_, err = store.FetchHeaderByHeight(21)
	var headerNotFound *headerfs.ErrHeaderNotFound
	require.ErrorAs(t, err, &headerNotFound)
	require.ErrorIs(t, err, io.EOF)
	_, err = store.HeightFromHash(&missing)
	require.ErrorIs(t, err, headerfs.ErrHashNotFound)

	stamp, err := store.RollbackBlockHeaders(2)
	require.NoError(t, err)
	require.EqualValues(t, 18, stamp.Height)
	_, _, err = store.FetchHeader(&tipHash)
	require.ErrorIs(t, err, headerfs.ErrHashNotFound)

	_, err = store.RollbackBlockHeaders(19)
	require.EqualError(t, err,
		"cannot roll back 19 headers when chain height is 18")
}

func TestBlockHeaderStoreRejectsNonContiguousBatch(t *testing.T) {
	t.Parallel()

	store := NewBlockHeaderStore(&chaincfg.SimNetParams)
	headers := testBlockHeaders(2)
	headers[1].Height = 3

	err := store.WriteHeaders(headers...)
	require.EqualError(t, err,
		"non-contiguous block header height 3, expected 2")
	_, height, err := store.ChainTip()
	require.NoError(t, err)
	require.Zero(t, height)
}

func TestFilterHeaderStoreOperations(t *testing.T) {
	t.Parallel()

	store, err := NewFilterHeaderStore(&chaincfg.SimNetParams)
	require.NoError(t, err)

	blockHash1 := chainhash.HashH([]byte("block-1"))
	blockHash2 := chainhash.HashH([]byte("block-2"))
	filterHash1 := chainhash.HashH([]byte("filter-1"))
	filterHash2 := chainhash.HashH([]byte("filter-2"))
	require.NoError(t, store.WriteHeaders(
		headerfs.FilterHeader{
			HeaderHash: blockHash1,
			FilterHash: filterHash1,
			Height:     1,
		},
		headerfs.FilterHeader{
			HeaderHash: blockHash2,
			FilterHash: filterHash2,
			Height:     2,
		},
	))

	tip, height, err := store.ChainTip()
	require.NoError(t, err)
	require.EqualValues(t, 2, height)
	require.Equal(t, filterHash2, *tip)

	ancestors, startHeight, err := store.FetchHeaderAncestors(
		1, &blockHash2,
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, startHeight)
	require.Equal(t, []chainhash.Hash{filterHash1, filterHash2},
		ancestors)

	stamp, err := store.RollbackLastBlock(&blockHash1)
	require.NoError(t, err)
	require.EqualValues(t, 1, stamp.Height)
	require.Equal(t, filterHash1, stamp.Hash)
	_, err = store.FetchHeader(&blockHash2)
	require.ErrorIs(t, err, headerfs.ErrHashNotFound)
}

func TestFilterHeaderStoreSparseBatchMetadata(t *testing.T) {
	t.Parallel()

	stores, err := New(&chaincfg.SimNetParams)
	require.NoError(t, err)
	blockHeaders := testBlockHeaders(2)
	require.NoError(t, stores.BlockHeaders.WriteHeaders(blockHeaders...))

	blockHash1 := blockHeaders[0].BlockHash()
	blockHash2 := blockHeaders[1].BlockHash()
	filterHash1 := chainhash.HashH([]byte("filter-1"))
	filterHash2 := chainhash.HashH([]byte("filter-2"))
	wrongTip := chainhash.HashH([]byte("wrong-tip"))
	err = stores.RegFilterHeaders.WriteHeaders(
		headerfs.FilterHeader{
			FilterHash: filterHash1,
		},
		headerfs.FilterHeader{
			HeaderHash: wrongTip,
			FilterHash: filterHash2,
			Height:     2,
		},
	)
	require.ErrorContains(t, err, "filter header tip block hash")
	_, height, err := stores.RegFilterHeaders.ChainTip()
	require.NoError(t, err)
	require.Zero(t, height)

	require.NoError(t, stores.RegFilterHeaders.WriteHeaders(
		headerfs.FilterHeader{
			FilterHash: filterHash1,
		},
		headerfs.FilterHeader{
			HeaderHash: blockHash2,
			FilterHash: filterHash2,
			Height:     2,
		},
	))

	stored, err := stores.RegFilterHeaders.FetchHeader(&blockHash1)
	require.NoError(t, err)
	require.Equal(t, filterHash1, *stored)
	stored, err = stores.RegFilterHeaders.FetchHeader(&blockHash2)
	require.NoError(t, err)
	require.Equal(t, filterHash2, *stored)

	ancestors, startHeight, err :=
		stores.RegFilterHeaders.FetchHeaderAncestors(1, &blockHash2)
	require.NoError(t, err)
	require.EqualValues(t, 1, startHeight)
	require.Equal(t, []chainhash.Hash{filterHash1, filterHash2},
		ancestors)

	_, err = stores.BlockHeaders.RollbackLastBlock()
	require.NoError(t, err)
	stamp, err := stores.RegFilterHeaders.RollbackLastBlock(&blockHash1)
	require.NoError(t, err)
	require.EqualValues(t, 1, stamp.Height)
	require.Equal(t, filterHash1, stamp.Hash)
}

func TestFilterStoreOperations(t *testing.T) {
	t.Parallel()

	store, err := NewFilterStore(&chaincfg.SimNetParams)
	require.NoError(t, err)

	blockHash := chainhash.HashH([]byte("block"))
	filter, err := builder.BuildBasicFilter(
		chaincfg.SimNetParams.GenesisBlock, nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.PutFilters(&filterdb.FilterData{
		Filter:    filter,
		BlockHash: &blockHash,
		Type:      filterdb.RegularFilter,
	}))

	storedFilter, err := store.FetchFilter(
		&blockHash, filterdb.RegularFilter,
	)
	require.NoError(t, err)
	require.Equal(t, filter, storedFilter)

	require.NoError(t, store.PurgeFilters(filterdb.RegularFilter))
	_, err = store.FetchFilter(&blockHash, filterdb.RegularFilter)
	require.ErrorIs(t, err, filterdb.ErrFilterNotFound)
}

func TestBanStoreOperations(t *testing.T) {
	t.Parallel()

	store := NewBanStore()
	ipNet := &net.IPNet{
		IP:   net.ParseIP("192.0.2.1"),
		Mask: net.CIDRMask(32, 32),
	}
	require.NoError(t, store.BanIPNet(
		ipNet, banman.NoCompactFilters, time.Hour,
	))

	status, err := store.Status(ipNet)
	require.NoError(t, err)
	require.True(t, status.Banned)
	require.Equal(t, banman.NoCompactFilters, status.Reason)

	require.NoError(t, store.UnbanIPNet(ipNet))
	status, err = store.Status(ipNet)
	require.NoError(t, err)
	require.False(t, status.Banned)

	require.NoError(t, store.BanIPNet(
		ipNet, banman.NoCompactFilters, -time.Second,
	))
	status, err = store.Status(ipNet)
	require.NoError(t, err)
	require.False(t, status.Banned)
}

func TestStoresConcurrentAccess(t *testing.T) {
	t.Parallel()

	stores, err := New(&chaincfg.SimNetParams)
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := 0; i < 20; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, _, err := stores.BlockHeaders.ChainTip()
			errs <- err
			_, _, err = stores.RegFilterHeaders.ChainTip()
			errs <- err
			_, err = stores.FilterDB.FetchFilter(
				chaincfg.SimNetParams.GenesisHash,
				filterdb.RegularFilter,
			)
			errs <- err

			blockHash := chainhash.HashH([]byte{byte(i)})
			errs <- stores.FilterDB.PutFilters(&filterdb.FilterData{
				BlockHash: &blockHash,
				Type:      filterdb.RegularFilter,
			})
			_, err = stores.FilterDB.FetchFilter(
				&blockHash, filterdb.RegularFilter,
			)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}
