package filtermatch

import (
	"context"
	"fmt"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
)

type decisionKind uint8

const (
	skip decisionKind = iota + 1
	candidate
)

// Decision is a filter backend's answer for one block. Only Skip and
// Candidate produce usable decisions. The zero value fails closed.
type Decision[F any] struct {
	kind     decisionKind
	evidence F
}

// Skip records a conclusive negative for the entire watch list.
func Skip[F any]() Decision[F] {
	return Decision[F]{kind: skip}
}

// Candidate retains the evidence needed to check a fetched block.
func Candidate[F any](evidence F) Decision[F] {
	return Decision[F]{kind: candidate, evidence: evidence}
}

// Backend selects a block for the watched scripts. A backend must establish
// a negative before returning Skip. Unknown or unavailable data is an error.
// F is the evidence its Verify method needs on a candidate.
//
// Match must honor ctx and treat scripts as read-only for the duration of the
// call. batch is a prefetch hint for consecutive historical queries. A
// service can call a backend concurrently for several scans.
type Backend[F any] interface {
	Match(ctx context.Context, hash chainhash.Hash, scripts [][]byte,
		batch bool) (Decision[F], error)

	Verify(evidence F, block *btcutil.Block) error
}

// BlockMatcher selects blocks for a scan. NewBlockMatcher binds a backend's
// evidence to its verifier. Consumers can only fetch the checked block.
type BlockMatcher interface {
	match(context.Context, chainhash.Hash, [][]byte, bool) (
		func(*btcutil.Block) error, error)
}

type matcher[F any] struct {
	backend Backend[F]
}

// NewBlockMatcher adapts a typed filter backend for use by either scanner.
func NewBlockMatcher[F any](backend Backend[F]) BlockMatcher {
	return matcher[F]{backend: backend}
}

func (m matcher[F]) match(ctx context.Context, hash chainhash.Hash,
	scripts [][]byte, batch bool) (func(*btcutil.Block) error, error) {

	decision, err := m.backend.Match(ctx, hash, scripts, batch)
	if err != nil {
		return nil, err
	}

	switch decision.kind {
	case skip:
		return nil, nil
	case candidate:
		return func(block *btcutil.Block) error {
			return m.backend.Verify(decision.evidence, block)
		}, nil
	default:
		return nil, fmt.Errorf("no filter decision for block %v", hash)
	}
}

// FetchMatchingBlock returns nil on a conclusive miss. On a possible match
// it fetches the requested block and checks it before returning it. A fetch
// or verification failure never produces a block for the wallet to process.
func FetchMatchingBlock(ctx context.Context, matcher BlockMatcher,
	hash chainhash.Hash, scripts [][]byte, batch bool,
	getBlock func(context.Context, chainhash.Hash) (*btcutil.Block, error)) (
	*btcutil.Block, error) {

	if matcher == nil {
		return nil, fmt.Errorf("missing block matcher")
	}
	verify, err := matcher.match(ctx, hash, scripts, batch)
	if err != nil || verify == nil {
		return nil, err
	}

	block, err := getBlock(ctx, hash)
	if err != nil {
		return nil, err
	}
	if block == nil || *block.Hash() != hash {
		return nil, fmt.Errorf("did not receive requested block %v", hash)
	}
	if err := verify(block); err != nil {
		return nil, fmt.Errorf("verifying filter against block %v: %w",
			hash, err)
	}

	return block, nil
}
