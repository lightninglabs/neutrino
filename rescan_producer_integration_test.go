package neutrino_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/integration/rpctest"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	"github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

type producerEvent struct {
	connected bool
	height    int32
	hash      chainhash.Hash
}

// TestRescanActorRealProducer follows real block and filter notifications
// through initial synchronization, extension, and a competing chain reorg.
func TestRescanActorRealProducer(t *testing.T) {
	for _, serviceFirst := range []bool{false, true} {
		name := "actor first"
		if serviceFirst {
			name = "service first"
		}
		t.Run(name, func(t *testing.T) {
			testRescanActorRealProducer(t, serviceFirst)
		})
	}
}

func testRescanActorRealProducer(t *testing.T, serviceFirst bool) {
	h1, err := rpctest.New(&chaincfg.SimNetParams, nil, nil, "")
	require.NoError(t, err)
	require.NoError(t, h1.SetUp(false, 0))
	t.Cleanup(func() { require.NoError(t, h1.TearDown()) })
	h2, err := rpctest.New(&chaincfg.SimNetParams, nil, nil, "")
	require.NoError(t, err)
	require.NoError(t, h2.SetUp(false, 0))
	t.Cleanup(func() { require.NoError(t, h2.TearDown()) })

	initial, err := h1.Client.Generate(5)
	require.NoError(t, err)
	require.NoError(t, h2.Client.AddNode(
		h1.P2PAddress(), rpcclient.ANAdd,
	))
	require.Eventually(t, func() bool {
		tip, _, err := h2.Client.GetBestBlock()
		return err == nil && *tip == *initial[4]
	}, 30*time.Second, 100*time.Millisecond, "initial peer synchronization")
	require.NoError(t, h2.Client.AddNode(
		h1.P2PAddress(), rpcclient.ANRemove,
	))

	dir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(dir, "wallet.db"),
		true, 10*time.Second, false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	svc, err := neutrino.NewChainService(neutrino.Config{
		DataDir: dir, Database: db, ChainParams: chaincfg.SimNetParams,
		AddPeers: []string{h1.P2PAddress()},
	})
	require.NoError(t, err)
	require.NoError(t, svc.Start(t.Context()))
	serviceStopStarted := false
	serviceDone := make(chan error, 1)
	stopService := func() {
		serviceStopStarted = true
		go func() { serviceDone <- svc.Stop() }()
		select {
		case err := <-serviceDone:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("service shutdown timed out")
		}
	}
	t.Cleanup(func() {
		if !serviceStopStarted {
			stopService()
		}
	})
	require.NoError(t, waitForSync(t, svc, h1))

	events := make(chan producerEvent, 64)
	genesis := chaincfg.SimNetParams.GenesisBlock.Header
	quit := make(chan struct{})
	rescan := neutrino.NewRescan(
		&neutrino.RescanChainSource{ChainService: svc},
		neutrino.QuitChan(quit),
		neutrino.StartBlock(&headerfs.BlockStamp{
			Hash: genesis.BlockHash(),
		}),
		neutrino.WatchInputs(neutrino.InputWithScript{
			PkScript: []byte{0x51},
		}),
		neutrino.NotificationHandlers(rpcclient.NotificationHandlers{
			OnBlockConnected: func(hash *chainhash.Hash,
				height int32, _ time.Time) {

				events <- producerEvent{true, height, *hash}
			},
			OnBlockDisconnected: func(hash *chainhash.Hash,
				height int32, _ time.Time) {

				events <- producerEvent{false, height, *hash}
			},
		}),
	)
	errs := rescan.Start()
	actorStopStarted := false
	actorDone := make(chan struct{})
	stopActor := func() {
		actorStopStarted = true
		close(quit)
		go func() {
			rescan.WaitForShutdown()
			close(actorDone)
		}()
		select {
		case <-actorDone:
		case <-time.After(10 * time.Second):
			t.Error("actor shutdown timed out")
		}
	}
	t.Cleanup(func() {
		if !actorStopStarted {
			stopActor()
		}
	})
	next := func(want producerEvent) {
		t.Helper()
		select {
		case got := <-events:
			require.Equal(t, want, got)
		case err := <-errs:
			t.Fatalf("rescan error: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for %+v", want)
		}
	}
	for i, hash := range initial {
		next(producerEvent{true, int32(i + 1), *hash})
	}

	old, err := h1.Client.Generate(2)
	require.NoError(t, err)
	for i, hash := range old {
		next(producerEvent{true, int32(i + 6), *hash})
	}
	fork, err := h2.Client.Generate(3)
	require.NoError(t, err)
	require.NoError(t, h1.Client.AddNode(
		h2.P2PAddress(), rpcclient.ANAdd,
	))
	require.Eventually(t, func() bool {
		tip, _, err := h1.Client.GetBestBlock()
		return err == nil && *tip == *fork[2]
	}, 30*time.Second, 100*time.Millisecond, "reorg peer synchronization")
	for i := len(old) - 1; i >= 0; i-- {
		next(producerEvent{false, int32(i + 6), *old[i]})
	}
	for i, hash := range fork {
		next(producerEvent{true, int32(i + 6), *hash})
	}
	require.NoError(t, waitForSync(t, svc, h1))
	blockTip, blockHeight, err := svc.BlockHeaders.ChainTip()
	require.NoError(t, err)
	filterTip, filterHeight, err := svc.RegFilterHeaders.ChainTip()
	require.NoError(t, err)
	rpcFilter, err := h1.Client.GetCFilterHeader(
		fork[2], wire.GCSFilterRegular,
	)
	require.NoError(t, err)
	require.Equal(t, uint32(8), blockHeight)
	require.Equal(t, blockHeight, filterHeight)
	require.Equal(t, *fork[2], blockTip.BlockHash())
	require.Equal(t, rpcFilter.PrevFilterHeader, *filterTip)
	if serviceFirst {
		stopService()
	}
	stopActor()
	if !serviceFirst {
		stopService()
	}
}
