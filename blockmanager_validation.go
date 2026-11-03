package neutrino

import (
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/lightninglabs/neutrino/headersync"
)

// checkHeaderSanity validates a header through headersync. The headers of a
// reorg attempt validate against the reorg list, and everything else validates
// against the cache unless the caller passes its own chain.
func (b *blockManager) checkHeaderSanity(blockHeader *wire.BlockHeader,
	reorgAttempt bool, prevNodeHeight int32,
	prevNodeHeader *wire.BlockHeader, headerChain headerlist.Chain) error {

	hList := b.headerList
	if reorgAttempt {
		hList = b.reorgList
	}
	if headerChain != nil {
		hList = headerChain
	}

	validator := headersync.HeaderValidator{
		Params:              &b.cfg.ChainParams,
		BlocksPerRetarget:   b.blocksPerRetarget,
		MinRetargetTimespan: b.minRetargetTimespan,
		MaxRetargetTimespan: b.maxRetargetTimespan,
		TimeSource:          b.cfg.TimeSource,
		Store:               b.cfg.BlockHeaders,
	}

	return validator.Check(
		blockHeader, prevNodeHeight, prevNodeHeader, hList,
	)
}
