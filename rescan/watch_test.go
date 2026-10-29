package rescan

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/stretchr/testify/require"

	"github.com/btcsuite/btcd/address/v2"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/filtermatch"
)

// newWatchTestAddr returns a distinct P2WSH address for n.
func newWatchTestAddr(t testing.TB, n uint64) address.Address {
	program := make([]byte, 32)
	binary.LittleEndian.PutUint64(program, n)
	addr, err := address.NewAddressWitnessScriptHash(
		program, &chaincfg.MainNetParams,
	)
	if err != nil {
		t.Fatal(err)
	}

	return addr
}

// TestWatchUpdatesAppendOnly checks that extending a watch state in place
// never changes what an earlier state watches, even when a discarded
// successor wrote past the earlier state's slices first.
func TestWatchUpdatesAppendOnly(t *testing.T) {
	a, b, c := newWatchTestAddr(t, 1), newWatchTestAddr(t, 2),
		newWatchTestAddr(t, 3)
	scriptOf := func(addr address.Address) []byte {
		script, err := txscript.PayToAddrScript(addr)
		require.NoError(t, err)
		return script
	}

	base, err := (&WatchState{}).ApplyUpdate(&AddWatchAddrsEvent{
		Addrs: []address.Address{a},
	})
	require.NoError(t, err)

	// A successor that the FSM then discards, as after a failed
	// transition, followed by the one that lives on.
	discarded, err := base.ApplyUpdate(&AddWatchAddrsEvent{
		Addrs: []address.Address{b},
	})
	require.NoError(t, err)
	require.True(t, discarded.watchesScript(scriptOf(b)))
	kept, err := base.ApplyUpdate(&AddWatchAddrsEvent{
		Addrs: []address.Address{c},
	})
	require.NoError(t, err)

	require.True(t, base.watchesScript(scriptOf(a)))
	require.False(t, base.watchesScript(scriptOf(b)))
	require.False(t, base.watchesScript(scriptOf(c)))
	require.Len(t, base.List, 1)

	require.True(t, kept.watchesScript(scriptOf(a)))
	require.False(t, kept.watchesScript(scriptOf(b)))
	require.True(t, kept.watchesScript(scriptOf(c)))
	require.Equal(t, [][]byte{scriptOf(a), scriptOf(c)}, kept.List)

	// A clone owns its memory: appending to it leaves the original
	// alone, and it answers lookups on its own.
	clone := kept.Clone()
	require.Equal(t, len(clone.List), cap(clone.List))
	_, err = clone.ApplyUpdate(&AddWatchAddrsEvent{
		Addrs: []address.Address{b},
	})
	require.NoError(t, err)
	require.False(t, kept.watchesScript(scriptOf(b)))
	require.Len(t, kept.List, 2)
	require.True(t, clone.watchesScript(scriptOf(c)))
}

// BenchmarkWatchUpdates applies N single-address, single-input watch-list
// updates in a row, the pattern of lnd's chain view, which sends one update
// per channel. It reports the cost per update, which must stay flat as N
// grows: each update costs O(1) amortized in the size of the watch list.
func BenchmarkWatchUpdates(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 50_000} {
		addrs := make([]address.Address, n)
		inputs := make([]filtermatch.InputWithScript, n)
		for i := range addrs {
			program := make([]byte, 32)
			binary.LittleEndian.PutUint64(program, uint64(i))
			addr, err := address.NewAddressWitnessScriptHash(
				program, &chaincfg.MainNetParams,
			)
			if err != nil {
				b.Fatal(err)
			}
			addrs[i] = addr
			inputs[i] = filtermatch.InputWithScript{
				OutPoint: wire.OutPoint{
					Hash:  chainhash.Hash(program),
					Index: uint32(i),
				},
				PkScript: append(
					[]byte{0x00, 0x20}, program...,
				),
			}
		}

		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			start := time.Now()
			for i := 0; i < b.N; i++ {
				var ws WatchState
				for j := 0; j < n; j++ {
					next, err := ws.ApplyUpdate(
						&AddWatchAddrsEvent{
							Addrs:  addrs[j : j+1],
							Inputs: inputs[j : j+1],
						},
					)
					if err != nil {
						b.Fatal(err)
					}
					ws = next
				}
			}
			perUpdate := time.Since(start) / time.Duration(b.N*n)
			b.ReportMetric(float64(perUpdate.Nanoseconds()),
				"ns/update")
		})
	}
}
