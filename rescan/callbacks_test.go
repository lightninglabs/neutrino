package rescan

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/filtermatch"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// callbackLog records every callback, and every MarkAsConfirmed, as one
// line each, in the order they ran.
type callbackLog struct {
	mu     sync.Mutex
	events []string
}

// add appends one line to the log.
func (l *callbackLog) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.events = append(l.events, fmt.Sprintf(format, args...))
}

// take returns the lines logged so far and clears the log.
func (l *callbackLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	events := l.events
	l.events = nil
	return events
}

// callbacks returns a callback set that logs every legacy callback.
func (l *callbackLog) callbacks() Callbacks {
	return Callbacks{
		OnFilteredBlockConnected: func(h int32, _ *wire.BlockHeader,
			txs []*btcutil.Tx) {

			l.add("fc %d %d", h, len(txs))
		},
		OnBlockConnected: func(_ *chainhash.Hash, h int32,
			_ time.Time) {

			l.add("bc %d", h)
		},
		OnFilteredBlockDisconnected: func(h int32,
			_ *wire.BlockHeader) {

			l.add("fd %d", h)
		},
		OnBlockDisconnected: func(_ *chainhash.Hash, h int32,
			_ time.Time) {

			l.add("bd %d", h)
		},
		OnRedeemingTx: func(tx *btcutil.Tx,
			d *btcjson.BlockDetails) {

			l.add("rd %d %d", d.Height, d.Index)
		},
		OnRecvTx: func(tx *btcutil.Tx, d *btcjson.BlockDetails) {
			l.add("rv %d %d", d.Height, d.Index)
		},
	}
}

// confirmingChain is a mock chain that logs MarkAsConfirmed.
type confirmingChain struct {
	*mockChainSource
	log *callbackLog
}

// MarkAsConfirmed logs the confirmation.
func (c *confirmingChain) MarkAsConfirmed(chainhash.Hash) {
	c.log.add("confirm")
}

// addBlockWithTxs adds a block holding txs, with a filter built over their
// outputs and prevScripts, and returns its stamp.
func (c *mockChainSource) addBlockWithTxs(prevScripts [][]byte,
	txs ...*wire.MsgTx) headerfs.BlockStamp {

	c.mu.Lock()
	defer c.mu.Unlock()

	height := uint32(c.bestBlock.Height + 1)
	genesis := chaincfg.MainNetParams.GenesisBlock.Header.Timestamp
	header := &wire.BlockHeader{
		PrevBlock: c.bestBlock.Hash,
		Timestamp: genesis.Add(
			time.Duration(height) * 10 * time.Minute,
		),
	}
	msg := wire.NewMsgBlock(header)

	// A coinbase first, so the relevant transactions sit at index 1
	// onward as they would in a real block.
	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Index: wire.MaxPrevOutIndex},
		SignatureScript:  []byte{byte(height)},
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

	hash := header.BlockHash()
	block := btcutil.NewBlock(msg)
	block.SetHeight(int32(height))

	c.blockHeightIndex[hash] = height
	c.blockHashesByHeight[height] = &hash
	c.blockHeaders[hash] = header
	c.blocks[hash] = block
	c.filters[hash] = filter
	filterHeader, _ := builder.MakeHeaderForFilter(
		filter, *c.filterHeadersByHeight[height-1],
	)
	c.filterHeadersByHeight[height] = &filterHeader

	c.bestBlock = headerfs.BlockStamp{
		Height:    int32(height),
		Hash:      hash,
		Timestamp: header.Timestamp,
	}

	return c.bestBlock
}

// newCallbackTestAddr returns a P2WSH address on mainnet.
func newCallbackTestAddr(t *testing.T, n byte) address.Address {
	t.Helper()

	program := make([]byte, 32)
	program[0] = n
	addr, err := address.NewAddressWitnessScriptHash(
		program, &chaincfg.MainNetParams,
	)
	require.NoError(t, err)

	return addr
}

// waitCurrent waits until the actor is current at height.
func waitCurrent(t *testing.T, actor *RescanActor, height int32) {
	t.Helper()

	require.Eventually(t, func() bool {
		state, err := actor.CurrentState(context.Background())
		if err != nil {
			return false
		}
		current, ok := state.(*StateCurrent)
		return ok && current.CurStamp.Height == height
	}, 5*time.Second, 5*time.Millisecond)
}

// TestActorTxCallbackOrder checks that a relevant transaction gets the
// legacy per-transaction callbacks, OnRedeemingTx before OnRecvTx, and is
// marked confirmed, all before the block's own callbacks.
func TestActorTxCallbackOrder(t *testing.T) {
	log := &callbackLog{}
	base := newMockChain(3)
	chain := &confirmingChain{mockChainSource: base, log: log}

	addr := newCallbackTestAddr(t, 1)
	addrScript, err := txscript.PayToAddrScript(addr)
	require.NoError(t, err)
	watched := filtermatch.InputWithScript{
		OutPoint: wire.OutPoint{Hash: chainhash.Hash{1}, Index: 2},
		PkScript: []byte{txscript.OP_TRUE, txscript.OP_TRUE},
	}

	// The relevant transaction both spends the watched outpoint and
	// pays the watched address.
	tx := wire.NewMsgTx(2)
	tx.AddTxIn(&wire.TxIn{PreviousOutPoint: watched.OutPoint})
	tx.AddTxOut(wire.NewTxOut(5, addrScript))
	stamp := base.addBlockWithTxs([][]byte{watched.PkScript}, tx)

	genesis := base.ChainParams().GenesisBlock.Header
	actor := mustNewRescanActor(t, ActorConfig{
		Chain:       chain,
		StartStamp:  headerfs.BlockStamp{Hash: genesis.BlockHash()},
		StartHeader: genesis,
		WatchAddrs:  []address.Address{addr},
		WatchInputs: []filtermatch.InputWithScript{watched},
		Callbacks:   log.callbacks(),
	})
	actor.Start()
	defer func() { actor.Stop(); actor.WaitForShutdown() }()

	waitCurrent(t, actor, stamp.Height)
	require.Equal(t, []string{
		"fc 1 0", "bc 1", "fc 2 0", "bc 2",
		"rd 3 1", "rv 3 1", "confirm", "fc 3 1", "bc 3",
	}, log.take())
}

// TestActorDisconnectOrder checks the legacy disconnect callback orders: a
// rewind reports OnBlockDisconnected first, a disconnect notification
// reports OnFilteredBlockDisconnected first, and a rewind that disables
// them reports neither.
func TestActorDisconnectOrder(t *testing.T) {
	log := &callbackLog{}
	chain := newMockChain(5)
	genesis := chain.ChainParams().GenesisBlock.Header
	actor := mustNewRescanActor(t, ActorConfig{
		Chain:       chain,
		StartStamp:  headerfs.BlockStamp{Hash: genesis.BlockHash()},
		StartHeader: genesis,
		Callbacks:   log.callbacks(),
	})
	actor.Start()
	defer func() { actor.Stop(); actor.WaitForShutdown() }()

	waitCurrent(t, actor, 4)
	log.take()

	rewind := uint32(2)
	require.NoError(t, addWatch(actor, WatchUpdate{
		RewindTo: &rewind,
	}))
	waitCurrent(t, actor, 4)
	require.Equal(t, []string{
		"bd 4", "fd 4", "bd 3", "fd 3",
		"fc 3 0", "bc 3", "fc 4 0", "bc 4",
	}, log.take())

	require.NoError(t, addWatch(actor, WatchUpdate{
		RewindTo:                 &rewind,
		DisableDisconnectedNtfns: true,
	}))
	waitCurrent(t, actor, 4)
	require.Equal(t, []string{
		"fc 3 0", "bc 3", "fc 4 0", "bc 4",
	}, log.take())

	// A disconnect notification for the tip reports the filtered
	// callback first.
	tipHash := chain.blockHashesByHeight[4]
	tip := *chain.blockHeaders[*tipHash]
	parent := *chain.blockHeaders[tip.PrevBlock]
	chain.ntfnChan <- blockntfns.NewBlockDisconnected(tip, 4, parent)
	waitCurrent(t, actor, 3)
	require.Equal(t, []string{"fd 4", "bd 4"}, log.take())
}

// TestActorEndBlock checks that the actor stops once it has scanned its end
// block, reports nothing above it, and ends with no error.
func TestActorEndBlock(t *testing.T) {
	log := &callbackLog{}
	chain := newMockChain(10)
	genesis := chain.ChainParams().GenesisBlock.Header
	actor := mustNewRescanActor(t, ActorConfig{
		Chain:       chain,
		StartStamp:  headerfs.BlockStamp{Hash: genesis.BlockHash()},
		StartHeader: genesis,
		EndBlock:    headerfs.BlockStamp{Height: 3},
		Callbacks:   log.callbacks(),
	})
	actor.Start()
	defer func() { actor.Stop(); actor.WaitForShutdown() }()

	select {
	case <-actor.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("rescan did not stop at its end block")
	}
	require.NoError(t, actor.Err())
	require.Equal(t, []string{
		"fc 1 0", "bc 1", "fc 2 0", "bc 2", "fc 3 0", "bc 3",
	}, log.take())

	state, err := actor.CurrentState(context.Background())
	require.NoError(t, err)
	require.IsType(t, &StateTerminal{}, state)
}
