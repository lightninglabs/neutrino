package neutrino

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/sqldb"
	"github.com/lightninglabs/neutrino/sqldb/sqlc"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"
)

// sqlControlTx delegates to a real executor while instrumenting only its
// transaction-scoped queries. The wrapper hides optional batch methods so
// InsertBlockHeader remains observable.
type sqlControlTx struct {
	sqldb.HeaderTx
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (tx *sqlControlTx) ExecTx(ctx context.Context, opts sqldbv2.TxOptions,
	body func(sqldb.HeaderQueries) error, reset func()) error {

	return tx.HeaderTx.ExecTx(ctx, opts, func(q sqldb.HeaderQueries) error {
		return body(&sqlControlQueries{HeaderQueries: q, tx: tx})
	}, reset)
}

type sqlControlQueries struct {
	sqldb.HeaderQueries
	tx *sqlControlTx
}

func (q *sqlControlQueries) InsertBlockHeader(ctx context.Context,
	arg sqlc.InsertBlockHeaderParams) error {

	if q.tx.armed.CompareAndSwap(true, false) {
		close(q.tx.entered)
		<-q.tx.release
	}
	return q.HeaderQueries.InsertBlockHeader(ctx, arg)
}

// TestChainStoreSQLControlProgress requires unrelated peer removal to proceed
// while a real SQL write holds the block-store mutex and an open transaction.
func TestChainStoreSQLControlProgress(t *testing.T) {
	params := chaincfg.SimNetParams
	params.Checkpoints = nil
	db := sqldb.NewTestBackend(t)
	tx := &sqlControlTx{
		HeaderTx: db.HeaderTxer,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	blocks, err := headerfs.NewSQLBlockHeaderStore(t.Context(), tx, &params)
	require.NoError(t, err)
	filters, err := newMemFilterHeaderStore(&params)
	require.NoError(t, err)
	fixture := dstChains()
	var initial []headerfs.BlockHeader
	for h := uint32(1); h <= dstChainHeight; h++ {
		initial = append(
			initial,
			headerfs.BlockHeader{
				BlockHeader: fixture.honest[h],
				Height:      h,
			},
		)
	}
	require.NoError(t, blocks.WriteHeaders(initial...))
	bm := newCFHeaderTestManager(t, blocks, filters)
	bm.cfg.ChainParams = params
	bm.cfg.TimeSource = dstMedianTime{
		MedianTimeSource: bm.cfg.TimeSource,
		offset: time.Until(
			params.GenesisBlock.Header.Timestamp.Add(time.Hour),
		),
	}
	bm.Start()
	t.Cleanup(
		func() {
			tx.once.Do(func() { close(tx.release) })
			require.NoError(t, bm.Stop())
		},
	)
	spectator := newTestServerPeer(t, "127.0.0.1:18447", dstChainHeight)
	bm.registerHeaderSyncPeer(spectator)
	_, ok := bm.headerSync.Peer(spectator.Addr())
	require.True(t, ok)
	peer := newTestServerPeer(t, "127.0.0.1:18444", dstChainHeight+1)
	bm.syncPeerMutex.Lock()
	bm.syncPeer = peer
	bm.syncPeerMutex.Unlock()
	msg := wire.NewMsgHeaders()
	require.NoError(t, msg.AddBlockHeader(fixture.honest[dstChainHeight+1]))
	tx.armed.Store(true)
	go bm.QueueHeaders(msg, peer)
	select {
	case <-tx.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("SQL insertion gate was not reached")
	}
	// ChainTip must wait for the same mutex, confirming the seam is inside
	// the actual store's critical section, rather than before its lock.
	tipStarted := make(chan struct{})
	tipDone := make(chan uint32, 1)
	go func() {
		close(tipStarted)
		_, height, err := blocks.ChainTip()
		if err == nil {
			tipDone <- height
		}
	}()
	<-tipStarted
	select {
	case <-tipDone:
		t.Fatal("SQL store mutex was not held")
	case <-time.After(50 * time.Millisecond):
	}
	go bm.DonePeer(spectator)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		_, exists := bm.headerSync.Peer(spectator.Addr())
		if !exists {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal(
				"peer removal did not complete while SQL " +
					"write was held",
			)
		case <-tick.C:
		}
	}
	tx.once.Do(func() { close(tx.release) })
	select {
	case height := <-tipDone:
		require.Equal(t, uint32(dstChainHeight+1), height)
	case <-time.After(5 * time.Second):
		t.Fatal("SQL tip read did not resume")
	}
}
