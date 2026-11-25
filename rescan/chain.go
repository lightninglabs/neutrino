package rescan

import (
	"context"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
)

// ScanChain supplies chain data and notifications independently of the
// filter format used to select blocks.
type ScanChain interface {
	// BestBlock returns the latest block ready to scan. A basic-filter
	// source has both the block header and the filter header at this tip.
	BestBlock() (*headerfs.BlockStamp, error)

	// GetBlockHeaderByHeight returns the header of the active block at
	// the given height.
	GetBlockHeaderByHeight(uint32) (*wire.BlockHeader, error)

	// GetBlockHeader returns the header of the block with the given hash
	// and its height.
	GetBlockHeader(*chainhash.Hash) (*wire.BlockHeader, uint32, error)

	// GetBlock fetches the block with the given hash. The fetch returns
	// early, with an error, once ctx is done.
	GetBlock(ctx context.Context, hash chainhash.Hash) (*btcutil.Block,
		error)

	// Subscribe returns a subscription that delivers block notifications
	// in order. A non-zero bestHeight asks for a backlog of connects for
	// every block above it. Zero asks for no backlog.
	Subscribe(bestHeight uint32) (*blockntfns.Subscription, error)

	// IsCurrent reports whether the chain believes its view of the
	// network is current.
	IsCurrent() bool

	// MarkAsConfirmed tells the chain that a relevant transaction was
	// found in a scanned block, so it can stop rebroadcasting it. The
	// actor calls it for every relevant transaction, after the
	// transaction's OnRedeemingTx and OnRecvTx callbacks, as the legacy
	// rescan did. A chain that broadcasts nothing does nothing.
	MarkAsConfirmed(txHash chainhash.Hash)
}

// ChainSource extends chain access with a provider-selected filter type.
type ChainSource[F any] interface {
	ScanChain

	// GetCFilter fetches the filter of the block with the
	// given hash. batch asks for the filters of the following blocks in
	// the same query, since a historical scan fetches them next. The fetch
	// returns early, with an error, once ctx is done.
	GetCFilter(ctx context.Context, hash chainhash.Hash,
		batch bool) (F, error)
}

// BasicChainSource is the BIP158 source used by the default matcher.
type BasicChainSource = ChainSource[*gcs.Filter]
