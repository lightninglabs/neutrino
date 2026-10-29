package neutrino

import (
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/lightninglabs/neutrino/filtermatch"
)

// VerifyBasicBlockFilter asserts that a given block filter was constructed
// correctly and according to the rules of BIP-0158 to contain both the output's
// pk scripts as well as the pk scripts the inputs are spending.
func VerifyBasicBlockFilter(filter *gcs.Filter, block *btcutil.Block) (int,
	error) {

	return filtermatch.VerifyBasicBlockFilter(filter, block)
}
