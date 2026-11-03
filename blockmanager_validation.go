package neutrino

// This file holds contextual block header validation, and the header contexts
// that the blockchain package's difficulty checks read from.

import (
	"fmt"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
)

// areHeadersConnected returns true if the passed block headers are connected
// to each other correctly.
func areHeadersConnected(headers []*wire.BlockHeader) bool {
	var (
		lastHeader chainhash.Hash
		emptyHash  chainhash.Hash
	)
	for _, blockHeader := range headers {
		blockHash := blockHeader.BlockHash()

		// If we haven't yet set lastHeader, set it now.
		if lastHeader == emptyHash {
			lastHeader = blockHash

			continue
		}

		// Ensure that blockHeader.PrevBlock matches lastHeader.
		if blockHeader.PrevBlock != lastHeader {
			return false
		}

		lastHeader = blockHash
	}

	return true
}

// checkHeaderSanity performs contextual and context-less checks on the passed
// wire.BlockHeader. This function calls blockchain.CheckBlockHeaderContext for
// the contextual check and blockchain.CheckBlockHeaderSanity for context-less
// checks.
func (b *blockManager) checkHeaderSanity(blockHeader *wire.BlockHeader,
	reorgAttempt bool, prevNodeHeight int32,
	prevNodeHeader *wire.BlockHeader, headerChain headerlist.Chain) error {

	// Create the lightHeaderCtx for the blockHeader's parent.
	hList := b.headerList
	if reorgAttempt {
		hList = b.reorgList
	}
	if headerChain != nil {
		hList = headerChain
	}

	parentHeaderCtx := newLightHeaderCtx(
		prevNodeHeight, prevNodeHeader, b.cfg.BlockHeaders, hList,
	)
	var ancestorErr error
	parentHeaderCtx.ancestorErr = &ancestorErr

	// Create a lightChainCtx as well.
	chainCtx := newLightChainCtx(
		&b.cfg.ChainParams, b.blocksPerRetarget, b.minRetargetTimespan,
		b.maxRetargetTimespan,
	)

	var emptyFlags blockchain.BehaviorFlags
	err := blockchain.CheckBlockHeaderContext(
		blockHeader, parentHeaderCtx, emptyFlags, chainCtx, true,
	)
	if ancestorErr != nil {
		return fmt.Errorf("unable to read header ancestor: %w",
			ancestorErr)
	}
	if err != nil {
		return err
	}

	return blockchain.CheckBlockHeaderSanity(
		blockHeader, b.cfg.ChainParams.PowLimit, b.cfg.TimeSource,
		emptyFlags,
	)
}

// lightChainCtx is an implementation of the blockchain.ChainCtx interface and
// gives a neutrino node the ability to contextually validate headers it
// receives.
type lightChainCtx struct {
	params              *chaincfg.Params
	blocksPerRetarget   int32
	minRetargetTimespan int64
	maxRetargetTimespan int64
}

// newLightChainCtx returns a new lightChainCtx instance from the passed
// arguments.
func newLightChainCtx(params *chaincfg.Params, blocksPerRetarget int32,
	minRetargetTimespan, maxRetargetTimespan int64) *lightChainCtx {

	return &lightChainCtx{
		params:              params,
		blocksPerRetarget:   blocksPerRetarget,
		minRetargetTimespan: minRetargetTimespan,
		maxRetargetTimespan: maxRetargetTimespan,
	}
}

// ChainParams returns the configured chain parameters.
//
// NOTE: Part of the blockchain.ChainCtx interface.
func (l *lightChainCtx) ChainParams() *chaincfg.Params {
	return l.params
}

// BlocksPerRetarget returns the number of blocks before retargeting occurs.
//
// NOTE: Part of the blockchain.ChainCtx interface.
func (l *lightChainCtx) BlocksPerRetarget() int32 {
	return l.blocksPerRetarget
}

// MinRetargetTimespan returns the minimum amount of time used in the
// difficulty calculation.
//
// NOTE: Part of the blockchain.ChainCtx interface.
func (l *lightChainCtx) MinRetargetTimespan() int64 {
	return l.minRetargetTimespan
}

// MaxRetargetTimespan returns the maximum amount of time used in the
// difficulty calculation.
//
// NOTE: Part of the blockchain.ChainCtx interface.
func (l *lightChainCtx) MaxRetargetTimespan() int64 {
	return l.maxRetargetTimespan
}

// VerifyCheckpoint returns false as the lightChainCtx does not need to
// validate checkpoints. This is already done inside the handleHeadersMsg
// function.
//
// NOTE: Part of the blockchain.ChainCtx interface.
func (l *lightChainCtx) VerifyCheckpoint(int32, *chainhash.Hash) bool {
	return false
}

// FindPreviousCheckpoint returns nil values since the lightChainCtx does not
// need to validate against checkpoints. This is already done inside the
// handleHeadersMsg function.
//
// NOTE: Part of the blockchain.ChainCtx interface.
func (l *lightChainCtx) FindPreviousCheckpoint() (blockchain.HeaderCtx, error) {
	return nil, nil
}

// lightHeaderCtx is an implementation of the blockchain.HeaderCtx interface.
// It is used so neutrino can perform contextual header validation checks.
type lightHeaderCtx struct {
	height    int32
	bits      uint32
	timestamp int64

	store      headerfs.BlockHeaderStore
	headerList headerlist.Chain

	// node, if non-nil, is the headerList node corresponding to this
	// header. When set, RelativeAncestorCtx will use the node's skip-list
	// pointer for O(log n) ancestor lookups instead of walking back from
	// the chain tip on every call.
	node *headerlist.Node

	// ancestorErr is shared by a validation's ancestor contexts. HeaderCtx
	// cannot return lookup errors, so retain them instead of accepting a
	// truncated history or blaming peer data for a storage failure.
	ancestorErr *error
}

// newLightHeaderCtx returns an instance of a lightHeaderCtx to be used when
// contextually validating headers.
func newLightHeaderCtx(height int32, header *wire.BlockHeader,
	store headerfs.BlockHeaderStore,
	headerList headerlist.Chain) *lightHeaderCtx {

	return &lightHeaderCtx{
		height:     height,
		bits:       header.Bits,
		timestamp:  header.Timestamp.Unix(),
		store:      store,
		headerList: headerList,
	}
}

// Height returns the height for the underlying header this context was created
// from.
//
// NOTE: Part of the blockchain.HeaderCtx interface.
func (l *lightHeaderCtx) Height() int32 {
	return l.height
}

// Bits returns the difficulty bits for the underlying header this context was
// created from.
//
// NOTE: Part of the blockchain.HeaderCtx interface.
func (l *lightHeaderCtx) Bits() uint32 {
	return l.bits
}

// Timestamp returns the timestamp for the underlying header this context was
// created from.
//
// NOTE: Part of the blockchain.HeaderCtx interface.
func (l *lightHeaderCtx) Timestamp() int64 {
	return l.timestamp
}

// Parent returns the parent of the underlying header this context was created
// from.
//
// NOTE: Part of the blockchain.HeaderCtx interface.
func (l *lightHeaderCtx) Parent() blockchain.HeaderCtx {
	// The parent is just an ancestor with distance 1.
	return l.RelativeAncestorCtx(1)
}

// RelativeAncestorCtx returns the ancestor that is distance blocks before the
// underlying header in the chain.
//
// NOTE: Part of the blockchain.HeaderCtx interface.
func (l *lightHeaderCtx) RelativeAncestorCtx(
	distance int32) blockchain.HeaderCtx {

	ancestorHeight := l.height - distance
	if ancestorHeight < 0 {
		return nil
	}

	var (
		ancestor *wire.BlockHeader
		err      error
	)

	// We'll first attempt to resolve the ancestor through the headerList's
	// skip-list. When we already know the current node, we can jump
	// straight from it; otherwise we anchor on the chain tip and let
	// Ancestor short-circuit if the target is in range.
	var ancestorNode *headerlist.Node
	if l.node != nil {
		ancestorNode = l.node.Ancestor(ancestorHeight)
	} else if l.headerList != nil {
		backNode := l.headerList.Back()
		if backNode != nil {
			ancestorNode = backNode.Ancestor(ancestorHeight)
		}
	}

	if ancestorNode != nil {
		ancestor = &ancestorNode.Header
	}

	// If the ancestor has already aged out of the in-memory headerList,
	// fall back to the on-disk header store.
	if ancestor == nil {
		ancestor, err = l.store.FetchHeaderByHeight(
			uint32(ancestorHeight),
		)
		if err != nil {
			if l.ancestorErr != nil && *l.ancestorErr == nil {
				*l.ancestorErr = err
			}
			return nil
		}
	}

	// Carry the resolved headerList node into the returned context so that
	// any further ancestor walks from it can keep using the skip-list
	// rather than walking from the tip again.
	ancestorCtx := newLightHeaderCtx(
		ancestorHeight, ancestor, l.store, l.headerList,
	)
	ancestorCtx.node = ancestorNode
	ancestorCtx.ancestorErr = l.ancestorErr

	return ancestorCtx
}
