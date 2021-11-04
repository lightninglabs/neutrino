package neutrino

import (
	"fmt"

	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/gcs"
	"github.com/btcsuite/btcutil/gcs/builder"
)

// VerifyBasicBlockFilter asserts that a given block filter was constructed
// correctly and according to the rules of BIP-0158 to contain both the output's
// pk scripts as well as the pk scripts the inputs are spending.
func VerifyBasicBlockFilter(filter *gcs.Filter, block *btcutil.Block) (int,
	error) {

	var (
		opReturnMatches int
		key             = builder.DeriveKey(block.Hash())
	)
	for idx, tx := range block.Transactions() {
		// Skip coinbase transaction.
		if idx == 0 {
			continue
		}

		// Check outputs first.
		for outIdx, txOut := range tx.MsgTx().TxOut {
			switch {
			// If the script itself is blank, then we'll skip this
			// as it doesn't contain any useful information.
			case len(txOut.PkScript) == 0:
				continue

			// We'll also skip any OP_RETURN scripts as well since
			// we don't index these in order to avoid a circular
			// dependency.
			case txOut.PkScript[0] == txscript.OP_RETURN:
				// Previous versions of the filters did include
				// OP_RETURNs. To be able disconnect bad peers
				// still serving these old filters we attempt to
				// check if there's an unexpected match. Since
				// there might be false positives, an OP_RETURN
				// can still match filters not including them.
				// Therefore, we count the number of such
				// unexpected matches for each peer, such that
				// we can ban peers matching more than the rest.
				match, err := filter.Match(key, txOut.PkScript)
				if err != nil {
					// Mark peer bad if we cannot match on
					// its filter.
					return 0, fmt.Errorf("error "+
						"validating block %v outpoint "+
						"%v:%d script %x: %v",
						block.Hash(), tx.Hash(), outIdx,
						txOut.PkScript, err)
				}

				// If it matches on the OP_RETURN output, we
				// increase the op return counter.
				if match {
					opReturnMatches++
				}

				continue
			}

			// This is a "normal" script where we definitely expect
			// a match.
			match, err := filter.Match(key, txOut.PkScript)
			if err != nil {
				return 0, fmt.Errorf("error validating block "+
					"%v outpoint %v:%d script %x: %v",
					block.Hash(), tx.Hash(), outIdx,
					txOut.PkScript, err)
			}

			if !match {
				return 0, fmt.Errorf("filter for block %v is "+
					"invalid, outpoint %v:%d script %x "+
					"wasn't matched by filter",
					block.Hash(), tx.Hash(), outIdx,
					txOut.PkScript)
			}
		}

		// Now we can go through all inputs and check that the filter
		// also included any pk scripts of the outputs being _spent_.
		// We can do this for witness items since the witness always
		// contains the full script as the last element on the stack.
		for inIdx, in := range tx.MsgTx().TxIn {
			// There are too many edge cases to cover for non-
			// witness scripts. And in LN land we're interested in
			// witness spends only anyway. Therefore let's skip any
			// input that has no witness.
			//
			// TODO(guggero): Add all those edge cases to
			// ComputePkScript?
			if len(in.Witness) == 0 {
				continue
			}

			// The only input type that has both set is a nested
			// P2PKH (P2SH-P2WKH). We can verify that one because
			// the script hash has to be HASH160(OP_PUSH32 <PKH>).
			script, err := txscript.ComputePkScript(
				in.SignatureScript, in.Witness,
			)

			// Just skip any inputs that we can't derive the pk
			// script from.
			if err == txscript.ErrUnsupportedScriptType {
				log.Tracef("Skipping filter validation for "+
					"input %d of tx %v in block %v "+
					"because script type is not supported "+
					"for validating against filter", inIdx,
					tx.Hash(), block.Hash())

				continue
			}

			// Something else went wrong. We can't really say the
			// filter is faulty though so we also just skip over
			// this input.
			if err != nil {
				log.Debug("Skipping filter validation for "+
					"input %d of tx %v in block %v "+
					"because computing the script failed: "+
					"%v", inIdx, block.Hash(), err)

				continue
			}

			match, err := filter.Match(key, script.Script())
			if err != nil {
				return 0, fmt.Errorf("error validating block "+
					"%v input %d of tx %v script %x: %v",
					block.Hash(), inIdx, tx.Hash(),
					script.Script(), err)
			}

			if !match {
				log.Errorf("filter for block %v might be "+
					"invalid, input %d of tx %v spends "+
					"pk script %x which wasn't matched by "+
					"filter. The input likely spends a "+
					"taproot output which is not yet"+
					"supported", block.Hash(), inIdx,
					tx.Hash(), script.Script())
			}
		}
	}

	return opReturnMatches, nil
}
