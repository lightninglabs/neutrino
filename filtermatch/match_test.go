package filtermatch

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/stretchr/testify/require"
)

// TestPaysWatchedScript checks that every output paying a watched script,
// and no other, becomes an input to watch, in output order.
func TestPaysWatchedScript(t *testing.T) {
	a, b, other := []byte{0x51, 1}, []byte{0x51, 2}, []byte{0x51, 3}
	set := map[string]struct{}{string(a): {}, string(b): {}}
	scripts := func(script []byte) bool {
		_, ok := set[string(script)]
		return ok
	}

	msg := wire.NewMsgTx(2)
	for _, script := range [][]byte{a, other, b, a} {
		msg.AddTxOut(wire.NewTxOut(1, script))
	}
	tx := btcutil.NewTx(msg)

	paid := PaysWatchedScript(tx, scripts)
	require.Len(t, paid, 3)
	for i, index := range []uint32{0, 2, 3} {
		require.Equal(t, wire.OutPoint{
			Hash: *tx.Hash(), Index: index,
		}, paid[i].OutPoint)
		require.Equal(t, msg.TxOut[index].PkScript, paid[i].PkScript)
	}

	unmatched := wire.NewMsgTx(2)
	unmatched.AddTxOut(wire.NewTxOut(1, other))
	require.Nil(t, PaysWatchedScript(btcutil.NewTx(unmatched), scripts))
}

// TestSpendsWatchedInput checks the outpoint match, and the script match of a
// watched input with the zero outpoint.
func TestSpendsWatchedInput(t *testing.T) {
	watched := wire.OutPoint{Hash: chainhash.Hash{1}, Index: 2}

	spend := wire.NewMsgTx(2)
	spend.AddTxIn(&wire.TxIn{PreviousOutPoint: watched})
	tx := btcutil.NewTx(spend)

	require.True(t, SpendsWatchedInput(tx, []InputWithScript{
		{OutPoint: watched},
	}))
	require.False(t, SpendsWatchedInput(tx, []InputWithScript{
		{OutPoint: wire.OutPoint{Hash: chainhash.Hash{1}, Index: 3}},
	}))

	// A P2WKH spend matches a zero-outpoint input watching its script.
	pubKey := make([]byte, 33)
	pubKey[0] = 0x02
	witnessSpend := wire.NewMsgTx(2)
	witnessSpend.AddTxIn(&wire.TxIn{
		PreviousOutPoint: watched,
		Witness:          wire.TxWitness{make([]byte, 72), pubKey},
	})
	witnessTx := btcutil.NewTx(witnessSpend)

	pkScript := []byte{0x00, 0x14}
	pkScript = append(pkScript, address.Hash160(pubKey)...)
	require.True(t, SpendsWatchedInput(witnessTx, []InputWithScript{
		{PkScript: pkScript},
	}))
	require.False(t, SpendsWatchedInput(witnessTx, []InputWithScript{
		{PkScript: []byte{0x51}},
	}))
}

// BenchmarkPaysWatchedScript measures the common unmatched-output path at
// different watch set sizes.
func BenchmarkPaysWatchedScript(b *testing.B) {
	for _, size := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("addresses=%d", size), func(b *testing.B) {
			set := make(map[string]struct{}, size)
			for i := 0; i < size; i++ {
				script := make([]byte, 34)
				binary.LittleEndian.PutUint64(
					script[2:], uint64(i),
				)
				set[string(script)] = struct{}{}
			}
			scripts := func(script []byte) bool {
				_, ok := set[string(script)]
				return ok
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
				if PaysWatchedScript(tx, scripts) != nil {
					b.Fatal("unexpected match")
				}
			}
		})
	}
}
