package neutrino

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/internal/memstore"
	"github.com/stretchr/testify/require"
)

func TestChainServiceCustomStores(t *testing.T) {
	t.Parallel()

	stores, err := memstore.New(&chaincfg.SimNetParams)
	require.NoError(t, err)

	service, err := NewChainService(Config{
		ChainParams: chaincfg.SimNetParams,
		Stores: &ChainServiceStores{
			FilterDB:         stores.FilterDB,
			BlockHeaders:     stores.BlockHeaders,
			RegFilterHeaders: stores.RegFilterHeaders,
			BanStore:         stores.BanStore,
		},
	})
	require.NoError(t, err)
	require.Same(t, stores.FilterDB, service.FilterDB)
	require.Same(t, stores.BlockHeaders, service.BlockHeaders)
	require.Same(t, stores.RegFilterHeaders, service.RegFilterHeaders)
	require.Same(t, stores.BanStore, service.banStore)
}

func TestChainServiceCustomStoresRequireCompleteSet(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		clear   func(*ChainServiceStores)
		errText string
	}{
		{
			name: "filter database",
			clear: func(stores *ChainServiceStores) {
				stores.FilterDB = nil
			},
			errText: "custom filter database is required",
		},
		{
			name: "block headers",
			clear: func(stores *ChainServiceStores) {
				stores.BlockHeaders = nil
			},
			errText: "custom block header store is required",
		},
		{
			name: "filter headers",
			clear: func(stores *ChainServiceStores) {
				stores.RegFilterHeaders = nil
			},
			errText: "custom filter header store is required",
		},
		{
			name: "ban store",
			clear: func(stores *ChainServiceStores) {
				stores.BanStore = nil
			},
			errText: "custom ban store is required",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			memoryStores, err := memstore.New(&chaincfg.SimNetParams)
			require.NoError(t, err)

			stores := &ChainServiceStores{
				FilterDB:         memoryStores.FilterDB,
				BlockHeaders:     memoryStores.BlockHeaders,
				RegFilterHeaders: memoryStores.RegFilterHeaders,
				BanStore:         memoryStores.BanStore,
			}
			testCase.clear(stores)

			_, err = NewChainService(Config{
				ChainParams: chaincfg.SimNetParams,
				Stores:      stores,
			})
			require.EqualError(t, err, testCase.errText)
		})
	}
}

func TestChainServiceCustomStoresRejectFilterHeaderAssertion(t *testing.T) {
	t.Parallel()

	memoryStores, err := memstore.New(&chaincfg.SimNetParams)
	require.NoError(t, err)

	_, err = NewChainService(Config{
		ChainParams: chaincfg.SimNetParams,
		Stores: &ChainServiceStores{
			FilterDB:         memoryStores.FilterDB,
			BlockHeaders:     memoryStores.BlockHeaders,
			RegFilterHeaders: memoryStores.RegFilterHeaders,
			BanStore:         memoryStores.BanStore,
		},
		AssertFilterHeader: &headerfs.FilterHeader{},
	})
	require.EqualError(t, err,
		"filter header assertions are not supported with custom stores")
}
