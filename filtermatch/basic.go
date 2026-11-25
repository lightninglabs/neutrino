package filtermatch

import (
	"context"
	"fmt"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/chainhash/v2"
)

// BasicFilterSource fetches BIP158 filters checked against the chain's
// regular filter headers.
type BasicFilterSource func(context.Context, chainhash.Hash, bool) (
	*gcs.Filter, error)

// Match implements Backend for basic filters.
func (fetch BasicFilterSource) Match(ctx context.Context, hash chainhash.Hash,
	scripts [][]byte, batch bool) (Decision[*gcs.Filter], error) {

	var unknown Decision[*gcs.Filter]
	filter, err := fetch(ctx, hash, batch)
	if err != nil {
		return unknown, err
	}
	if filter == nil {
		return unknown, fmt.Errorf("missing basic filter for %v", hash)
	}
	if filter.N() == 0 {
		return Skip[*gcs.Filter](), nil
	}

	hit, err := MatchFilter(filter, &hash, scripts)
	if err != nil {
		return unknown, err
	}

	if !hit {
		return Skip[*gcs.Filter](), nil
	}
	return Candidate(filter), nil
}

// Verify implements Backend for basic filters.
func (BasicFilterSource) Verify(filter *gcs.Filter, block *btcutil.Block) error {
	_, err := VerifyBasicBlockFilter(filter, block)
	return err
}
