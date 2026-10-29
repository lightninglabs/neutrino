package neutrino

import (
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
)

// addNewBlockWithTxs advances the chain by one block holding txs, with a
// filter built over their outputs and prevScripts, and notifies it.
func (c *mockChainSource) addNewBlockWithTxs(prevScripts [][]byte,
	txs ...*wire.MsgTx) headerfs.BlockStamp {

	header := c.nextHeader()
	msg := wire.NewMsgBlock(header)
	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Index: wire.MaxPrevOutIndex},
	})
	coinbase.AddTxOut(wire.NewTxOut(1, []byte{txscript.OP_TRUE}))
	_ = msg.AddTransaction(coinbase)
	for _, tx := range txs {
		_ = msg.AddTransaction(tx)
	}
	filter, err := builder.BuildBasicFilter(msg, prevScripts)
	if err != nil {
		panic(err)
	}

	// Store the block, its filter and its headers in one step, so the
	// rescan never sees the new tip without them.
	c.mu.Lock()
	height := uint32(c.bestBlock.Height + 1)
	hash := header.BlockHash()
	c.blockHeightIndex[hash] = height
	c.blockHashesByHeight[height] = &hash
	c.blockHeaders[hash] = header
	c.blocks[hash] = btcutil.NewBlock(msg)
	c.filters[hash] = filter
	filterHeader, _ := builder.MakeHeaderForFilter(
		filter, *c.filterHeadersByHeight[height-1],
	)
	c.filterHeadersByHeight[height] = &filterHeader
	c.bestBlock = headerfs.BlockStamp{
		Height: int32(height), Hash: hash, Timestamp: header.Timestamp,
	}
	stamp := c.bestBlock
	c.mu.Unlock()

	c.notify(stamp)

	return stamp
}

// notify sends a connect notification for the block at stamp.
func (c *mockChainSource) notify(stamp headerfs.BlockStamp) {
	c.mu.Lock()
	header := *c.blockHeaders[stamp.Hash]
	c.mu.Unlock()

	c.ntfnChan <- blockntfns.NewBlockConnected(header, uint32(stamp.Height))
}

// TestWatchedAddressScripts checks initial and updated addresses, duplicates,
// and the outpoints the rescan then watches, through the public API: every
// output that pays a watched address becomes a watched outpoint, and no
// other output does.
func TestWatchedAddressScripts(t *testing.T) {
	makeAddr := func(n byte) address.Address {
		t.Helper()
		program := make([]byte, 32)
		program[0] = n
		addr, err := address.NewAddressWitnessScriptHash(
			program, &chaincfg.SimNetParams,
		)
		if err != nil {
			t.Fatal(err)
		}
		return addr
	}
	scriptOf := func(addr address.Address) []byte {
		t.Helper()
		script, err := txscript.PayToAddrScript(addr)
		if err != nil {
			t.Fatal(err)
		}
		return script
	}
	a, b, other := makeAddr(1), makeAddr(2), makeAddr(3)

	type event struct {
		kind     string
		index    int
		relevant int
	}
	var (
		mu     sync.Mutex
		events []event
	)
	record := func(e event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	quit := make(chan struct{})
	chain := newMockChainSource(10)

	// The mock reports every query on an unbuffered channel. Drain them.
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-chain.filtersQueried:
			case <-chain.blocksQueried:
			case <-done:
				return
			}
		}
	}()

	rescan := NewRescan(
		chain, QuitChan(quit), WatchAddrs(a, a),
		NotificationHandlers(rpcclient.NotificationHandlers{
			OnRecvTx: func(_ *btcutil.Tx,
				d *btcjson.BlockDetails) {

				record(event{kind: "rv", index: d.Index})
			},
			OnRedeemingTx: func(_ *btcutil.Tx,
				d *btcjson.BlockDetails) {

				record(event{kind: "rd", index: d.Index})
			},
			OnFilteredBlockConnected: func(_ int32,
				_ *wire.BlockHeader, txs []*btcutil.Tx) {

				record(event{kind: "fc", relevant: len(txs)})
			},
		}),
	)
	errChan := rescan.Start()
	defer func() {
		close(quit)
		<-errChan
		rescan.WaitForShutdown()
	}()
	if err := rescan.Update(AddAddrs(b)); err != nil {
		t.Fatal(err)
	}

	// One transaction pays a, other, b and a again, so three of its
	// outputs become watched outpoints.
	pay := wire.NewMsgTx(2)
	for _, addr := range []address.Address{a, other, b, a} {
		pay.AddTxOut(wire.NewTxOut(1, scriptOf(addr)))
	}
	payHash := pay.TxHash()
	chain.addNewBlockWithTxs(nil, pay)

	// A block that pays only an unwatched address is not relevant.
	unmatched := wire.NewMsgTx(2)
	unmatched.AddTxOut(wire.NewTxOut(1, scriptOf(other)))
	chain.addNewBlockWithTxs(nil, unmatched)

	// One transaction spends each output of pay. Only the spends of the
	// three watched outputs are relevant.
	var (
		spends      []*wire.MsgTx
		prevScripts [][]byte
	)
	for i, out := range pay.TxOut {
		spend := wire.NewMsgTx(2)
		spend.AddTxIn(&wire.TxIn{PreviousOutPoint: wire.OutPoint{
			Hash: payHash, Index: uint32(i),
		}})
		spend.AddTxOut(wire.NewTxOut(1, []byte{txscript.OP_TRUE}))
		spends = append(spends, spend)
		prevScripts = append(prevScripts, out.PkScript)
	}
	chain.addNewBlockWithTxs(prevScripts, spends...)

	want := []event{
		{kind: "rv", index: 1}, {kind: "fc", relevant: 1},
		{kind: "fc"},
		{kind: "rd", index: 1}, {kind: "rd", index: 3},
		{kind: "rd", index: 4}, {kind: "fc", relevant: 3},
	}
	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		got := append([]event(nil), events...)
		mu.Unlock()
		if len(got) >= len(want) {
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("event %d: want %+v, got "+
						"%+v", i, want[i], got[i])
				}
			}
			return
		}

		select {
		case <-deadline:
			t.Fatalf("want events %+v, got %+v", want, got)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
