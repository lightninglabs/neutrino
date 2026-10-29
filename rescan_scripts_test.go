package neutrino

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// TestWatchedAddressScripts checks initial and updated addresses, duplicates,
// and the resulting outpoints and filter entries for multiple outputs.
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
	a, b, other := makeAddr(1), makeAddr(2), makeAddr(3)
	rs, err := newRescanState(
		newMockChainSource(0), WatchAddrs(a, a),
		QuitChan(make(chan struct{})),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = rs.opts.updateFilter(
		rs.chain, &updateOptions{addrs: []address.Address{b}},
		&headerfs.BlockStamp{}, &wire.BlockHeader{},
	)
	if err != nil {
		t.Fatal(err)
	}

	msg := wire.NewMsgTx(2)
	for _, addr := range []address.Address{a, other, b, a} {
		script, err := txscript.PayToAddrScript(addr)
		if err != nil {
			t.Fatal(err)
		}
		msg.AddTxOut(wire.NewTxOut(1, script))
	}
	tx := btcutil.NewTx(msg)
	matched, err := rs.opts.paysWatchedAddr(tx)
	if err != nil || !matched {
		t.Fatalf("matched=%v err=%v", matched, err)
	}
	if len(rs.opts.watchInputs) != 3 || len(rs.opts.watchList) != 6 {
		t.Fatalf("unexpected input or filter count: %d %d",
			len(rs.opts.watchInputs), len(rs.opts.watchList))
	}
	for i, index := range []uint32{0, 2, 3} {
		input := rs.opts.watchInputs[i]
		outputScript := msg.TxOut[index].PkScript
		if input.OutPoint.Hash != *tx.Hash() ||
			input.OutPoint.Index != index ||
			string(input.PkScript) != string(outputScript) {

			t.Fatalf("incorrect watched input %d: %+v", i, input)
		}
	}
	unmatched := wire.NewMsgTx(2)
	script, err := txscript.PayToAddrScript(other)
	if err != nil {
		t.Fatal(err)
	}
	unmatched.AddTxOut(wire.NewTxOut(1, script))
	matched, err = rs.opts.paysWatchedAddr(btcutil.NewTx(unmatched))
	if err != nil || matched || len(rs.opts.watchInputs) != 3 {
		t.Fatalf("unexpected non-match: %v %v", matched, err)
	}
}

// BenchmarkWatchedAddressScripts measures the common unmatched-output path at
// different watch set sizes, excluding one-time address conversion.
func BenchmarkWatchedAddressScripts(b *testing.B) {
	for _, size := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("addresses=%d", size), func(b *testing.B) {
			ro := defaultRescanOptions()
			for i := 0; i < size; i++ {
				script := make([]byte, 34)
				binary.LittleEndian.PutUint64(
					script[2:], uint64(i),
				)
				ro.watchScripts[string(script)] = struct{}{}
			}
			msg := wire.NewMsgTx(2)
			for i := 0; i < 2000; i++ {
				script := make([]byte, 34)
				binary.LittleEndian.PutUint64(
					script[2:], uint64(size+i),
				)
				msg.AddTxOut(wire.NewTxOut(1, script))
			}
			tx := btcutil.NewTx(msg)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				matched, err := ro.paysWatchedAddr(tx)
				if err != nil || matched {
					b.Fatal("unexpected match")
				}
			}
		})
	}
}
