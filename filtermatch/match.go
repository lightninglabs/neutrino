// Package filtermatch matches compact block filters and block transactions
// against a watch list of payment scripts and outpoints. The public rescan in
// the neutrino package, the rescan actor in neutrino/rescan and the utxo
// scanner all match through it, so a matching fix lands once.
//
// It depends on nothing else in neutrino, so both the root package and the
// rescan package can import it without a cycle.
package filtermatch

import (
	"bytes"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

// InputWithScript couples an previous outpoint along with its input script.
// We'll use the prev script to match the filter itself, but then scan for the
// particular outpoint when we need to make a notification decision.
type InputWithScript struct {
	// OutPoint identifies the previous output to watch.
	OutPoint wire.OutPoint

	// PkScript is the script of the previous output.
	PkScript []byte
}

// zeroOutPoint is the outpoint of a watched input that matches on the script
// being spent rather than on the outpoint itself.
var zeroOutPoint wire.OutPoint

// MatchFilter reports whether filter, the filter of the block with the given
// hash, matches any entry of watchList. A false result means the block is
// certainly not interesting.
func MatchFilter(filter *gcs.Filter, blockHash *chainhash.Hash,
	watchList [][]byte) (bool, error) {

	return filter.MatchAny(builder.DeriveKey(blockHash), watchList)
}

// SpendsWatchedInput reports whether tx spends one of the watched inputs. An
// input with the zero outpoint matches any spend of its script instead.
func SpendsWatchedInput(tx *btcutil.Tx, inputs []InputWithScript) bool {
	for _, in := range tx.MsgTx().TxIn {
		for _, input := range inputs {
			switch {
			case input.OutPoint == zeroOutPoint:
				pkScript, err := txscript.ComputePkScript(
					in.SignatureScript, in.Witness,
				)
				if err != nil {
					continue
				}

				script := pkScript.Script()
				if bytes.Equal(script, input.PkScript) {
					return true
				}

			case in.PreviousOutPoint == input.OutPoint:
				return true
			}
		}
	}

	return false
}

// PaysWatchedScript returns an input for every output of tx that pays one of
// the watched address scripts, keyed by the script bytes, so the caller can
// watch for the spend of each. It returns nil if tx pays none of them.
//
// The scripts must hold only address scripts. The script of a watched input
// must not be in it, since paying that script is not a payment to the wallet.
func PaysWatchedScript(tx *btcutil.Tx,
	scripts map[string]struct{}) []InputWithScript {

	var paid []InputWithScript
	for outIdx, out := range tx.MsgTx().TxOut {
		if _, ok := scripts[string(out.PkScript)]; !ok {
			continue
		}

		paid = append(paid, InputWithScript{
			OutPoint: wire.OutPoint{
				Hash:  *tx.Hash(),
				Index: uint32(outIdx),
			},
			PkScript: out.PkScript,
		})
	}

	return paid
}
