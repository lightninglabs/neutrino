package headerfs

import (
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcwallet/waddrmgr"
)

// interface to persistent filter header store
// An implementation of FilterHeaderDB is a fully fledged database for any variant of filter headers.
type FilterHeaderDB interface {
	// FetchHeader returns the filter header that corresponds to the passed block
	// height.
	FetchHeader(hash *chainhash.Hash) (*chainhash.Hash, error)

	// FetchHeaderByHeight returns the filter header for a particular block height.
	FetchHeaderByHeight(height uint32) (*chainhash.Hash, error)

	// WriteHeaders writes a batch of filter headers to persistent storage. The
	// headers themselves are appended to the flat file, and then the index updated
	// to reflect the new entires.
	WriteHeaders(hdrs ...FilterHeader) error

	// ChainTip returns the latest filter header and height known to the
	// FilterHeaderStore.
	ChainTip() (*chainhash.Hash, uint32, error)

	// RollbackLastBlock rollsback both the index, and on-disk header file by a
	// _single_ filter header. This method is meant to be used in the case of
	// re-org which disconnects the latest filter header from the end of the main
	// chain. The information about the latest header tip after truncation is
	// returnd.
	RollbackLastBlock(newTip *chainhash.Hash) (*waddrmgr.BlockStamp, error)
}

// FilterHeaderStore implements the FilterHeaderDB interface
// Create an instance with: NewFilterHeaderStore
//
// The FilterHeaderStore combines a flat file to store the block headers with a
// database instance for managing the index into the set of flat files.
type FilterHeaderStore struct {
	*headerStore
}

// FilterHeader represents a filter header (basic or extended). The filter
// header itself is coupled with the block height and hash of the filter's
// block.
type FilterHeader struct {
	// HeaderHash is the hash of the block header that this filter header
	// corresponds to.
	HeaderHash chainhash.Hash

	// FilterHash is the filter header itself.
	FilterHash chainhash.Hash

	// Height is the block height of the filter header in the main chain.
	Height uint32
}
