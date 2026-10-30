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

// ChainSource is the chain the rescan actor scans. It is narrower than the
// neutrino package's ChainSource: the actor never names a neutrino query
// option, so this package does not import the neutrino package, and the
// neutrino package can run its public rescan on the actor. The neutrino
// package adapts its own ChainSource to this one and maps the context and the
// batch flag onto its query options.
type ChainSource interface {
	// BestBlock returns the most recent block for which both the header
	// and the filter header are stored.
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

	// GetCFilter fetches the regular compact filter of the block with the
	// given hash. batch asks for the filters of the following blocks in
	// the same query, since a historical scan fetches them next. The fetch
	// returns early, with an error, once ctx is done.
	GetCFilter(ctx context.Context, hash chainhash.Hash,
		batch bool) (*gcs.Filter, error)

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
