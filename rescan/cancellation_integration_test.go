package rescan

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	neutrino "github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

type productionFilterChain struct {
	*mockChainSource
	service *neutrino.ChainService
	entered chan struct{}
	result  chan error
}

func (c *productionFilterChain) GetCFilter(
	hash chainhash.Hash,
	typ wire.FilterType,
	opts ...neutrino.QueryOption,
) (*gcs.Filter, error) {

	close(c.entered)
	filter, err := c.service.GetCFilter(hash, typ, opts...)
	c.result <- err
	return filter, err
}

// TestActorStopsProductionFilterQuery exercises cancellation through the real
// ChainService dispatcher while leaving the service running.
func TestActorStopsProductionFilterQuery(t *testing.T) {
	dir := t.TempDir()
	db, err := walletdb.Create(
		"bdb",
		filepath.Join(dir, "neutrino.db"),
		true,
		10*time.Second,
		false,
	)
	require.NoError(t, err)
	defer db.Close()
	service, err := neutrino.NewChainService(neutrino.Config{
		DataDir: dir, Database: db, ChainParams: chaincfg.SimNetParams,
	})
	require.NoError(t, err)
	base := newMockChain(2)
	header := base.blockHeaders[*base.blockHashesByHeight[1]]
	require.NoError(
		t,
		service.BlockHeaders.WriteHeaders(
			headerfs.BlockHeader{
				BlockHeader: header, Height: 1,
			},
		),
	)
	require.NoError(
		t,
		service.RegFilterHeaders.WriteHeaders(
			headerfs.FilterHeader{
				HeaderHash: header.BlockHash(), Height: 1,
			},
		),
	)
	require.NoError(t, service.Start(t.Context()))
	defer service.Stop()
	chain := &productionFilterChain{
		mockChainSource: base,
		service:         service,
		entered: make(
			chan struct{},
		),
		result: make(chan error, 1),
	}
	genesis := base.ChainParams().GenesisBlock.Header
	actor := NewRescanActor(ActorConfig{
		Chain:       chain,
		StartStamp:  headerfs.BlockStamp{Hash: genesis.BlockHash()},
		StartHeader: genesis,
		WatchInputs: []neutrino.InputWithScript{
			{PkScript: []byte{0x51}},
		},
	})
	actor.Start()
	select {
	case <-chain.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("production filter query did not start")
	}
	select {
	case err := <-chain.result:
		t.Fatalf(
			"query returned before cancellation: %v",
			err,
		)
	case <-time.After(100 * time.Millisecond):
	}
	done := make(chan struct{})
	go func() { actor.Stop(); actor.WaitForShutdown(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(
			"actor shutdown did not cancel the production query",
		)
	}
	require.ErrorIs(t, <-chain.result, context.Canceled)
}
