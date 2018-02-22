package headerfs

import (
	"github.com/roasbeef/btcd/blockchain"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcd/wire"
	"github.com/roasbeef/btcwallet/waddrmgr"
)

// BlockHeaderDB is the interface to the persistent block header store
// An implementation of BlockHeaderDB is a fully fledged database for Bitcoin block headers.
type BlockHeaderDB interface {
	// WriteHeaders writes a set of headers to disk and updates the index in a
	// single atomic transaction.
	WriteHeaders(hdrs ...BlockHeader) error

	// LatestBlockLocator returns the latest block locator object based on the tip
	// of the current main chain from the PoV of the database and flat files.
	LatestBlockLocator() (blockchain.BlockLocator, error)

	// BlockLocatorFromHash computes a block locator given a particular hash. The
	// standard Bitcoin algorithm to compute block locators are employed.
	BlockLocatorFromHash(hash *chainhash.Hash) (blockchain.BlockLocator, error)

	// CheckConnectivity cycles through all of the block headers on disk, from last
	// to first, and makes sure they all connect to each other. Additionally, at
	// each block header, we also ensure that the index entry for that height and
	// hash also match up properly.
	CheckConnectivity() error

	// ChainTip returns the best known block header and height for the
	// BlockHeaderStore.
	ChainTip() (*wire.BlockHeader, uint32, error)

	// FetchHeader attempts to retrieve a block header determined by the passed
	// block height.
	FetchHeader(hash *chainhash.Hash) (*wire.BlockHeader, uint32, error)

	// FetchHeaderByHeight attempts to retrieve a target block header based on a
	// block height.
	FetchHeaderByHeight(height uint32) (*wire.BlockHeader, error)

	// HeightFromHash returns the height of a particualr block header given its
	// hash.
	HeightFromHash(hash *chainhash.Hash) (uint32, error)

	// RollbackLastBlock rollsback both the index, and on-disk header file by a
	// _single_ header. This method is meant to be used in the case of re-org which
	// disconnects the latest block header from the end of the main chain. The
	// information about the new header tip after truncation is returned.
	RollbackLastBlock() (*waddrmgr.BlockStamp, error)
}

// BlockHeaderStore implements the BlockHeaderDB interface
// Create an instance with: NewBlockHeaderStore
//
// The BlockHeaderStore combines a flat file to store
// the block headers with a database instance for managing the index into the
// set of flat files.
//
type BlockHeaderStore struct {
	*headerStore
}

// BlockHeader is a Bitcoin block header that also has its height included.
type BlockHeader struct {
	*wire.BlockHeader

	// Height is the height of this block header within the current main
	// chain.
	Height uint32
}
