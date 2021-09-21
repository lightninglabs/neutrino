package pushtx

import (
	"math/rand"
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/blockntfns"
)

// createTx is a helper method to create random transactions that spend
// particular inputs.
func createTx(t *testing.T, numOutputs int, inputs ...wire.OutPoint) *wire.MsgTx {
	t.Helper()

	tx := wire.NewMsgTx(1)
	if len(inputs) == 0 {
		tx.AddTxIn(&wire.TxIn{})
	} else {
		for _, input := range inputs {
			tx.AddTxIn(&wire.TxIn{PreviousOutPoint: input})
		}
	}
	for i := 0; i < numOutputs; i++ {
		var pkScript [32]byte
		if _, err := rand.Read(pkScript[:]); err != nil {
			t.Fatal(err)
		}

		tx.AddTxOut(&wire.TxOut{
			Value:    rand.Int63(),
			PkScript: pkScript[:],
		})
	}

	return tx
}

// TestBroadcaster ensures that we can broadcast transactions while it is
// active.
func TestBroadcaster(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Broadcast: func(*wire.MsgTx) error {
			return nil
		},
		SubscribeBlocks: func() (*blockntfns.Subscription, error) {
			return &blockntfns.Subscription{
				Notifications: make(chan blockntfns.BlockNtfn),
				Cancel:        func() {},
			}, nil
		},
		RebroadcastInterval: DefaultRebroadcastInterval,
	}

	broadcaster := NewBroadcaster(cfg)

	if err := broadcaster.Start(); err != nil {
		t.Fatalf("unable to start broadcaster: %v", err)
	}

	tx := &wire.MsgTx{}
	if err := broadcaster.Broadcast(tx); err != nil {
		t.Fatalf("unable to broadcast transaction: %v", err)
	}

	broadcaster.Stop()

	if err := broadcaster.Broadcast(tx); err != ErrBroadcasterStopped {
		t.Fatalf("expected ErrBroadcasterStopped, got %v", err)
	}
}

// TestRebroadcast ensures that we properly rebroadcast transactions upon every
// new block. Transactions that have confirmed should no longer be broadcast.
func TestRebroadcast(t *testing.T) {
	t.Parallel()

	const numTxs = 3

	// We'll start by setting up the broadcaster with channels to mock the
	// behavior of its external dependencies.
	broadcastChan := make(chan *wire.MsgTx, numTxs)
	ntfnChan := make(chan blockntfns.BlockNtfn)

	cfg := &Config{
		Broadcast: func(tx *wire.MsgTx) error {
			broadcastChan <- tx
			return nil
		},
		SubscribeBlocks: func() (*blockntfns.Subscription, error) {
			return &blockntfns.Subscription{
				Notifications: ntfnChan,
				Cancel:        func() {},
			}, nil
		},
		RebroadcastInterval: DefaultRebroadcastInterval,
	}

	broadcaster := NewBroadcaster(cfg)

	if err := broadcaster.Start(); err != nil {
		t.Fatalf("unable to start broadcaster: %v", err)
	}
	defer broadcaster.Stop()

	// We'll then create some test transactions such that they all depend on
	// the previous one, creating a dependency chain. We'll do this to
	// ensure transactions are rebroadcast in the order of their
	// dependencies.
	txs := make([]*wire.MsgTx, 0, numTxs)
	for i := 0; i < numTxs; i++ {
		var tx *wire.MsgTx
		if i == 0 {
			tx = createTx(t, 1)
		} else {
			prevOut := wire.OutPoint{
				Hash:  txs[i-1].TxHash(),
				Index: 0,
			}
			tx = createTx(t, 1, prevOut)
		}
		txs = append(txs, tx)
	}

	// assertBroadcastOrder is a helper closure to ensure that the
	// transactions rebroadcast match the expected order.
	assertBroadcastOrder := func(expectedOrder []*wire.MsgTx) {
		t.Helper()

		for i := 0; i < len(expectedOrder); i++ {
			tx := <-broadcastChan
			if tx.TxHash() != expectedOrder[i].TxHash() {
				t.Fatalf("expected transaction %v, got %v",
					expectedOrder[i].TxHash(), tx.TxHash())
			}
		}
	}

	// Broadcast the transactions. We'll be broadcasting them in order so
	// assertBroadcastOrder is more of a sanity check to ensure that all of
	// the transactions were actually broadcast.
	for _, tx := range txs {
		if err := broadcaster.Broadcast(tx); err != nil {
			t.Fatalf("unable to broadcast transaction %v: %v",
				tx.TxHash(), err)
		}
	}

	assertBroadcastOrder(txs)

	// Now, we'll modify the Broadcast method to mark the first transaction
	// as confirmed, and the second as it being accepted into the mempool.
	broadcaster.cfg.Broadcast = func(tx *wire.MsgTx) error {
		broadcastChan <- tx
		if tx.TxHash() == txs[0].TxHash() {
			return &BroadcastError{Code: Confirmed}
		}
		if tx.TxHash() == txs[1].TxHash() {
			return &BroadcastError{Code: Mempool}
		}
		return nil
	}

	// Trigger a new block notification to rebroadcast the transactions.
	ntfnChan <- blockntfns.NewBlockConnected(wire.BlockHeader{}, 100)

	// They should all be broadcast in their expected dependency order.
	assertBroadcastOrder(txs)

	// Trigger another block notification simulating a reorg in the chain.
	// The transactions should be rebroadcast again to ensure they properly
	// propagate throughout the network.
	ntfnChan <- blockntfns.NewBlockDisconnected(
		wire.BlockHeader{}, 100, wire.BlockHeader{},
	)

	// This time however, only the last two transactions will be rebroadcast
	// since the first one confirmed in the previous rebroadcast attempt.
	assertBroadcastOrder(txs[1:])

	// We now manually mark one of the transactions as confirmed.
	broadcaster.MarkAsConfirmed(txs[1].TxHash())

	// Trigger a new block notification to rebroadcast the transactions.
	ntfnChan <- blockntfns.NewBlockConnected(wire.BlockHeader{}, 101)

	// We assert that only the last transaction is rebroadcast.
	assertBroadcastOrder(txs[2:])

	// Manually mark the last transaction as confirmed.
	broadcaster.MarkAsConfirmed(txs[2].TxHash())

	// Trigger a new block notification.
	ntfnChan <- blockntfns.NewBlockConnected(wire.BlockHeader{}, 102)

	// Assert that no transactions were rebroadcast.
	select {
	case tx := <-broadcastChan:
		t.Fatalf("unexpected rebroadcast of tx %s", tx.TxHash())
	case <-time.Tick(100 * time.Millisecond):
	}
}
