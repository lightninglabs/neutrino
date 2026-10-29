package neutrino

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// productionFilterChain is a mock chain whose filter fetches go to a real
// ChainService.
type productionFilterChain struct {
	*mockChainSource
	service *ChainService
	entered chan struct{}
	result  chan error
}

// GetCFilter fetches the filter from the ChainService, with the options the
// rescan passed.
func (c *productionFilterChain) GetCFilter(hash chainhash.Hash,
	typ wire.FilterType, opts ...QueryOption) (*gcs.Filter, error) {

	close(c.entered)
	filter, err := c.service.GetCFilter(hash, typ, opts...)
	c.result <- err
	return filter, err
}

// TestRescanQuitCancelsProductionFilterQuery checks that closing a rescan's
// quit channel cancels its filter query through the real ChainService
// dispatcher, while the service keeps running, and that the rescan reports
// only ErrRescanExit.
func TestRescanQuitCancelsProductionFilterQuery(t *testing.T) {
	dir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(dir, "neutrino.db"), true,
		10*time.Second, false,
	)
	require.NoError(t, err)
	defer db.Close()

	service, err := NewChainService(Config{
		DataDir: dir, Database: db, ChainParams: chaincfg.SimNetParams,
	})
	require.NoError(t, err)

	base := newMockChainSource(2)
	header := *base.blockHeaders[*base.blockHashesByHeight[1]]
	require.NoError(t, service.BlockHeaders.WriteHeaders(
		headerfs.BlockHeader{BlockHeader: &header, Height: 1},
	))
	require.NoError(t, service.RegFilterHeaders.WriteHeaders(
		headerfs.FilterHeader{
			HeaderHash: header.BlockHash(), Height: 1,
		},
	))
	require.NoError(t, service.Start(t.Context()))
	defer service.Stop()

	chain := &productionFilterChain{
		mockChainSource: base,
		service:         service,
		entered:         make(chan struct{}),
		result:          make(chan error, 1),
	}
	genesis := base.ChainParams().GenesisHash
	quit := make(chan struct{})
	rescan := NewRescan(
		chain, QuitChan(quit),
		StartBlock(&headerfs.BlockStamp{Hash: *genesis}),
		WatchInputs(InputWithScript{PkScript: []byte{0x51}}),
	)
	errChan := rescan.Start()

	select {
	case <-chain.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("production filter query did not start")
	}
	select {
	case err := <-chain.result:
		t.Fatalf("query returned before cancellation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(quit)
	select {
	case err := <-errChan:
		require.True(t, errors.Is(err, ErrRescanExit), err)
	case <-time.After(5 * time.Second):
		t.Fatal("rescan shutdown did not cancel the production query")
	}
	rescan.WaitForShutdown()
	require.ErrorIs(t, <-chain.result, context.Canceled)
}
