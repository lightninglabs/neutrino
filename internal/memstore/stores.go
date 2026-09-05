// Package memstore provides volatile, concurrency-safe ChainService storage
// backends for tests and integration fixtures.
package memstore

import (
	"fmt"

	"github.com/btcsuite/btcd/chaincfg/v2"
)

// Stores groups a complete set of in-memory ChainService storage backends.
type Stores struct {
	FilterDB         *FilterStore
	BlockHeaders     *BlockHeaderStore
	RegFilterHeaders *FilterHeaderStore
	BanStore         *BanStore
}

// New returns a complete set of stores initialized for the target chain.
func New(params *chaincfg.Params) (*Stores, error) {
	if params == nil {
		return nil, fmt.Errorf("chain parameters are required")
	}

	filterDB, err := NewFilterStore(params)
	if err != nil {
		return nil, err
	}

	blockHeaders := NewBlockHeaderStore(params)
	filterHeaders, err := newFilterHeaderStore(params, blockHeaders)
	if err != nil {
		return nil, err
	}

	return &Stores{
		FilterDB:         filterDB,
		BlockHeaders:     blockHeaders,
		RegFilterHeaders: filterHeaders,
		BanStore:         NewBanStore(),
	}, nil
}
