package headersync

import (
	"fmt"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
)

// NumMaxMemHeaders is the max number of headers to store in memory for a
// particular peer. By bounding this value, we're able to closely control our
// effective memory usage during initial sync and re-org handling. This value
// should be set a "sane" re-org size, such that we're able to properly handle
// re-orgs in size strictly less than this value.
const NumMaxMemHeaders = 10000

// TipConfig is what a TipState needs from its owner. The stores and the time
// source are functions, because the owner may swap them after construction and
// validation must read the current ones each time.
type TipConfig struct {
	// Params are the chain's consensus parameters. The checkpoints come
	// from here.
	Params *chaincfg.Params

	// BlocksPerRetarget is the length of a difficulty period.
	BlocksPerRetarget int32

	// MinRetargetTimespan is the shortest timespan a retarget adjusts for.
	MinRetargetTimespan int64

	// MaxRetargetTimespan is the longest timespan a retarget adjusts for.
	MaxRetargetTimespan int64

	// TimeSource returns the median time source for the future timestamp
	// check.
	TimeSource func() blockchain.MedianTimeSource

	// BlockHeaders returns the block header store that serves ancestors
	// which have left the validation cache.
	BlockHeaders func() headerfs.BlockHeaderStore
}

// TipState owns the header validation cache of the block header handler: the
// recent chain that new headers validate against, the scratch chain that a
// competing branch validates against, and the cursor at the next checkpoint.
// The handler is the only goroutine that touches it, so it takes no locks.
type TipState struct {
	cfg TipConfig

	// headerList is the validation cache. It trails the durable store
	// until a commit lands.
	headerList headerlist.Chain

	// reorgList is the scratch chain that a competing branch validates
	// against.
	reorgList headerlist.Chain

	// nextCheckpoint is the first checkpoint above the cache tip, or nil
	// when the tip is past the last one.
	nextCheckpoint *chaincfg.Checkpoint
}

// NewTipState returns a state with empty caches. The caller must Reset it to
// the chain tip before use.
func NewTipState(cfg TipConfig) *TipState {
	return &TipState{
		cfg: cfg,
		headerList: headerlist.NewBoundedMemoryChain(
			NumMaxMemHeaders,
		),
		reorgList: headerlist.NewBoundedMemoryChain(
			NumMaxMemHeaders,
		),
	}
}

// Tip returns the newest node of the validation cache, or nil when the cache
// is empty. Nodes link back to their parents through Prev.
func (t *TipState) Tip() *headerlist.Node {
	return t.headerList.Back()
}

// NextCheckpoint returns the first checkpoint above the cache tip, or nil when
// the tip is past the last checkpoint.
func (t *TipState) NextCheckpoint() *chaincfg.Checkpoint {
	return t.nextCheckpoint
}

// FetchHeader returns a header and its height from the block header store. The
// store holds the durable chain, which can be ahead of the validation cache.
func (t *TipState) FetchHeader(hash *chainhash.Hash) (*wire.BlockHeader,
	uint32, error) {

	return t.cfg.BlockHeaders().FetchHeader(hash)
}

// FetchHeaderByHeight returns the header at a height of the block header
// store.
func (t *TipState) FetchHeaderByHeight(height uint32) (*wire.BlockHeader,
	error) {

	return t.cfg.BlockHeaders().FetchHeaderByHeight(height)
}

// Reset replaces the validation cache with the single node and moves the
// checkpoint cursor past it.
func (t *TipState) Reset(node headerlist.Node) {
	t.headerList.ResetHeaderState(node)
	t.nextCheckpoint = t.FindNextCheckpoint(node.Height)
}

// Extend appends nodes that are now durable to the validation cache and moves
// the checkpoint cursor past the last one. The nodes must connect to the
// cache tip and must not be empty.
func (t *TipState) Extend(nodes []headerlist.Node) {
	for _, node := range nodes {
		t.headerList.PushBack(node)
	}

	t.nextCheckpoint = t.FindNextCheckpoint(nodes[len(nodes)-1].Height)
}

// ResetReorg starts a competing branch at the fork point.
func (t *TipState) ResetReorg(fork headerlist.Node) {
	t.reorgList.ResetHeaderState(fork)
}

// AppendReorg adds a header of the competing branch that passed validation.
func (t *TipState) AppendReorg(node headerlist.Node) {
	t.reorgList.PushBack(node)
}

// FindNextCheckpoint returns the next checkpoint after the passed height. It
// returns nil when there is not one either because the height is already
// later than the final checkpoint or there are none for the current network.
func (t *TipState) FindNextCheckpoint(height int32) *chaincfg.Checkpoint {
	// There is no next checkpoint if there are none for this current
	// network.
	checkpoints := t.cfg.Params.Checkpoints
	if len(checkpoints) == 0 {
		return nil
	}

	// There is no next checkpoint if the height is already after the final
	// checkpoint.
	finalCheckpoint := &checkpoints[len(checkpoints)-1]
	if height >= finalCheckpoint.Height {
		return nil
	}

	// Find the next checkpoint.
	nextCheckpoint := finalCheckpoint
	for i := len(checkpoints) - 2; i >= 0; i-- {
		if height >= checkpoints[i].Height {
			break
		}
		nextCheckpoint = &checkpoints[i]
	}

	return nextCheckpoint
}

// FindPreviousCheckpoint returns the last checkpoint before the passed height.
// It returns a checkpoint matching the genesis block when the height is
// earlier than the first checkpoint or there are no checkpoints for the
// current network. This is used for resetting state when a malicious peer
// sends us headers that don't lead up to a known checkpoint.
func (t *TipState) FindPreviousCheckpoint(height int32) *chaincfg.Checkpoint {
	// Start with the genesis block - earliest checkpoint to which our code
	// will want to reset
	prevCheckpoint := &chaincfg.Checkpoint{
		Height: 0,
		Hash:   t.cfg.Params.GenesisHash,
	}

	// Find the latest checkpoint lower than height or return genesis block
	// if there are none.
	checkpoints := t.cfg.Params.Checkpoints
	for i := 0; i < len(checkpoints); i++ {
		if height <= checkpoints[i].Height {
			break
		}
		prevCheckpoint = &checkpoints[i]
	}

	return prevCheckpoint
}

// CheckHeader validates a header against the validation cache. Its parent is
// prevHeader at prevHeight.
func (t *TipState) CheckHeader(header *wire.BlockHeader, prevHeight int32,
	prevHeader *wire.BlockHeader) error {

	return t.check(header, prevHeight, prevHeader, t.headerList)
}

// CheckReorgHeader validates a header of a competing branch against the
// reorg chain. Its parent is prevHeader at prevHeight.
func (t *TipState) CheckReorgHeader(header *wire.BlockHeader,
	prevHeight int32, prevHeader *wire.BlockHeader) error {

	return t.check(header, prevHeight, prevHeader, t.reorgList)
}

// check validates a header through the header validator, against the given
// chain.
func (t *TipState) check(header *wire.BlockHeader, prevHeight int32,
	prevHeader *wire.BlockHeader, chain headerlist.Chain) error {

	validator := HeaderValidator{
		Params:              t.cfg.Params,
		BlocksPerRetarget:   t.cfg.BlocksPerRetarget,
		MinRetargetTimespan: t.cfg.MinRetargetTimespan,
		MaxRetargetTimespan: t.cfg.MaxRetargetTimespan,
		TimeSource:          t.cfg.TimeSource(),
		Store:               t.cfg.BlockHeaders(),
	}

	return validator.Check(header, prevHeight, prevHeader, chain)
}

// ReconcileWithStore fast-forwards the validation cache to the durable block
// header tip.
//
// A header write can report an error after it landed: a SQL commit whose
// acknowledgement was lost, or a store that wrote part of a batch before
// failing. Memory then trails disk. Retrying the same batch from memory would
// either rewrite durable heights (a primary-key conflict on SQL, a duplicate
// append that corrupts the legacy flat file) or, once planning has moved the
// FSM to the durable tip, stage ranges that never connect to the stale cache.
// Either way header sync stalls until restart. Every header on disk passed
// contextual validation before it was written, so adopting it here is safe.
func (t *TipState) ReconcileWithStore() error {
	back := t.headerList.Back()
	if back == nil {
		return fmt.Errorf("header list missing previous element")
	}
	store := t.cfg.BlockHeaders()
	_, height, err := store.ChainTip()
	if err != nil {
		return err
	}
	if height <= uint32(back.Height) {
		return nil
	}

	prev := *back
	for next := uint32(back.Height) + 1; next <= height; next++ {
		header, err := store.FetchHeaderByHeight(next)
		if err != nil {
			return err
		}

		// A durable chain that does not extend the cache is a reorg,
		// which the serial path owns. Leave the cache alone.
		if header.PrevBlock != prev.Header.BlockHash() {
			return fmt.Errorf("durable header height=%d does not "+
				"extend cached tip height=%d", next,
				prev.Height)
		}
		prev = headerlist.Node{Header: *header, Height: int32(next)}
		t.headerList.PushBack(prev)
	}

	log.Infof("headersync reconciled visible tip height=%d -> %d with "+
		"durable store", back.Height, height)

	t.nextCheckpoint = t.FindNextCheckpoint(int32(height))

	return nil
}

// ValidationError distinguishes permanently invalid peer data from retryable
// storage or lookup failures. Only the former invalidates staging.
type ValidationError struct {
	// RangeID is the range whose headers failed validation.
	RangeID RangeID

	// PeerID is the peer that supplied the headers.
	PeerID string

	// Err is the reason the headers are invalid.
	Err error
}

// Error returns the validation failure.
func (e *ValidationError) Error() string { return e.Err.Error() }

// Unwrap returns the validation failure.
func (e *ValidationError) Unwrap() error { return e.Err }
