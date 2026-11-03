package headersync

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
)

// RangeCommit is a staged range that is ready to validate for a commit.
type RangeCommit struct {
	// Range is the range as the FSM planned it, trimmed of any prefix
	// that is already durable.
	Range HeaderRange

	// Staged are the headers a peer delivered for the range.
	Staged StagedRange
}

// VerifiedCheckpoint is a checkpoint that a prepared commit reached with the
// right hash.
type VerifiedCheckpoint struct {
	// Height is the height of the checkpoint.
	Height int32

	// Hash is the hash of the checkpoint.
	Hash chainhash.Hash

	// PeerID is the peer that supplied the checkpoint header.
	PeerID string

	// RangeID is the range that held the checkpoint header.
	RangeID RangeID
}

// PreparedCommit is a batch of ranges that passed validation against scratch
// state. Nothing in it is visible until the store commits the batch and the
// caller finishes it with FinishRange.
type PreparedCommit struct {
	// ExpectedParent is the tip the batch was validated against.
	ExpectedParent headerfs.BlockStamp

	// Headers is the batch for the store.
	Headers []headerfs.BlockHeader

	// Nodes is the same batch for the validation cache.
	Nodes []headerlist.Node

	// VerifiedCheckpoints are the checkpoints the batch passed.
	VerifiedCheckpoints []VerifiedCheckpoint

	// FinalHeight is the height of the last header.
	FinalHeight int32

	// FinalHash is the hash of the last header.
	FinalHash chainhash.Hash

	// NextCheckpoint is the checkpoint above the last header.
	NextCheckpoint *chaincfg.Checkpoint
}

// ValidationChain overlays headers prepared in the current commit batch on top
// of the long-lived header cache. Contextual validation can then see freshly
// prepared parents without losing access to older cached ancestors.
type ValidationChain struct {
	base    headerlist.Chain
	pending *headerlist.BoundedMemoryChain
}

// NewValidationChain returns an overlay on base that starts at seed, which
// must be the tip of base.
func NewValidationChain(base headerlist.Chain,
	seed headerlist.Node) *ValidationChain {

	pending := headerlist.NewBoundedMemoryChain(NumMaxMemHeaders)
	pending.ResetHeaderState(seed)

	return &ValidationChain{
		base:    base,
		pending: pending,
	}
}

// ResetHeaderState restarts the overlay at the node. The base is untouched.
func (h *ValidationChain) ResetHeaderState(n headerlist.Node) {
	h.pending.ResetHeaderState(n)
}

// Back returns the newest node of the overlay, or of the base when the
// overlay is empty.
func (h *ValidationChain) Back() *headerlist.Node {
	if back := h.pending.Back(); back != nil {
		return back
	}

	return h.base.Back()
}

// Front returns the oldest node of the base.
func (h *ValidationChain) Front() *headerlist.Node {
	return h.base.Front()
}

// PushBack adds a node to the overlay.
func (h *ValidationChain) PushBack(n headerlist.Node) *headerlist.Node {
	return h.pending.PushBack(n)
}

// AtHeight returns the node at the height, looking in the overlay first.
func (h *ValidationChain) AtHeight(height int32) (*headerlist.Node, bool) {
	if node, ok := h.pending.AtHeight(height); ok {
		return node, true
	}

	return h.base.AtHeight(height)
}

// rangePrep is the scratch state of one PrepareRangeHeaders call. It tracks
// the parent of the next header as the batch grows.
type rangePrep struct {
	tip      *TipState
	prepared PreparedCommit
	chain    *ValidationChain

	prevHash   chainhash.Hash
	prevHeight int32
	prevHeader wire.BlockHeader
}

// PrepareRangeHeaders validates the ranges as one batch that extends the
// validation cache, without changing the cache. On failure, the error is a
// ValidationError when the peer's data is at fault, and any other error is
// worth retrying.
func (t *TipState) PrepareRangeHeaders(
	ranges []RangeCommit) (PreparedCommit, error) {

	var prepared PreparedCommit
	if len(ranges) == 0 {
		return prepared, nil
	}

	totalHeaders := 0
	for _, commitRange := range ranges {
		totalHeaders += len(commitRange.Staged.Headers)
	}
	if totalHeaders == 0 {
		return prepared, ErrNoHeaders
	}

	prepared.Headers = make([]headerfs.BlockHeader, 0, totalHeaders)
	prepared.Nodes = make([]headerlist.Node, 0, totalHeaders)
	prepared.NextCheckpoint = t.nextCheckpoint

	prevNodeEl := t.headerList.Back()
	if prevNodeEl == nil {
		return prepared, fmt.Errorf(
			"header list missing previous element",
		)
	}

	prevHash := prevNodeEl.Header.BlockHash()
	prepared.ExpectedParent = headerfs.BlockStamp{
		Height:    prevNodeEl.Height,
		Hash:      prevHash,
		Timestamp: prevNodeEl.Header.Timestamp,
	}

	prep := &rangePrep{
		tip:        t,
		prepared:   prepared,
		chain:      NewValidationChain(t.headerList, *prevNodeEl),
		prevHash:   prevHash,
		prevHeight: prevNodeEl.Height,
		prevHeader: prevNodeEl.Header,
	}
	for _, commitRange := range ranges {
		if err := prep.addRange(commitRange); err != nil {
			return prep.prepared, err
		}
	}

	return prep.prepared, nil
}

// addRange validates one staged range on top of the batch so far.
func (p *rangePrep) addRange(commitRange RangeCommit) error {
	rng := commitRange.Range
	headers := commitRange.Staged.Headers
	peerID := commitRange.Staged.PeerID

	if len(headers) == 0 {
		return &ValidationError{
			RangeID: rng.ID, PeerID: peerID, Err: ErrNoHeaders,
		}
	}

	var rangeFinalHeight int32
	for _, blockHeader := range headers {
		err := p.addHeader(rng, peerID, blockHeader)
		if err != nil {
			return err
		}

		rangeFinalHeight = p.prevHeight
	}

	if rangeFinalHeight != int32(rng.StopHeight) {
		err := fmt.Errorf("headersync range_id=%d "+
			"final_height=%d stop_height=%d", rng.ID,
			rangeFinalHeight, rng.StopHeight)

		return &ValidationError{
			RangeID: rng.ID, PeerID: peerID, Err: err,
		}
	}

	return nil
}

// addHeader validates a header of the range against the batch so far, then
// adds it to the batch.
func (p *rangePrep) addHeader(rng HeaderRange, peerID string,
	blockHeader *wire.BlockHeader) error {

	invalid := func(err error) error {
		return &ValidationError{
			RangeID: rng.ID, PeerID: peerID, Err: err,
		}
	}
	blockHash := blockHeader.BlockHash()

	if !p.prevHash.IsEqual(&blockHeader.PrevBlock) {
		err := fmt.Errorf("headersync staged "+
			"range_id=%d does not connect to "+
			"current "+
			"tip hash=%s prev=%s", rng.ID, p.prevHash,
			blockHeader.PrevBlock)
		if uint32(p.prevHeight) == rng.StartHeight &&
			p.prevHash == rng.StartHash {

			return invalid(err)
		}

		return err
	}

	err := p.tip.check(
		blockHeader, p.prevHeight, &p.prevHeader, p.chain,
	)
	if err != nil {
		var ruleErr blockchain.RuleError
		if errors.As(err, &ruleErr) {
			err = invalid(err)
		}

		return fmt.Errorf("headersync range_id=%d header sanity "+
			"failed: %w", rng.ID, err)
	}

	node := headerlist.Node{
		Header: *blockHeader,
		Height: p.prevHeight + 1,
	}

	headerCopy := *blockHeader
	p.prepared.Headers = append(p.prepared.Headers, headerfs.BlockHeader{
		BlockHeader: &headerCopy,
		Height:      uint32(node.Height),
	})
	p.prepared.Nodes = append(p.prepared.Nodes, node)
	p.chain.PushBack(node)

	err = p.checkCheckpoint(rng, peerID, node, blockHash)
	if err != nil {
		return invalid(err)
	}

	p.prepared.FinalHash = blockHash
	p.prepared.FinalHeight = node.Height
	p.prevHash = blockHash
	p.prevHeight = node.Height
	p.prevHeader = node.Header

	return nil
}

// checkCheckpoint compares a node that reached the next checkpoint with it,
// and moves the cursor to the following checkpoint when it matches.
func (p *rangePrep) checkCheckpoint(rng HeaderRange, peerID string,
	node headerlist.Node, blockHash chainhash.Hash) error {

	next := p.prepared.NextCheckpoint
	if next == nil || node.Height != next.Height {
		return nil
	}

	if !blockHash.IsEqual(next.Hash) {
		return fmt.Errorf("headersync checkpoint mismatch "+
			"height=%d got=%s want=%s peer=%s",
			node.Height, blockHash, next.Hash, peerID)
	}

	p.prepared.VerifiedCheckpoints = append(
		p.prepared.VerifiedCheckpoints, VerifiedCheckpoint{
			Height:  node.Height,
			Hash:    blockHash,
			PeerID:  peerID,
			RangeID: rng.ID,
		},
	)
	p.prepared.NextCheckpoint = p.tip.FindNextCheckpoint(node.Height)

	return nil
}

// FinishRange adopts a prepared batch into the validation cache after the
// store has committed it, and logs the checkpoints it verified.
func (t *TipState) FinishRange(prepared PreparedCommit) {
	verifiedByHeight := make(
		map[int32]VerifiedCheckpoint, len(prepared.VerifiedCheckpoints),
	)
	for _, checkpoint := range prepared.VerifiedCheckpoints {
		verifiedByHeight[checkpoint.Height] = checkpoint
	}

	for _, node := range prepared.Nodes {
		t.headerList.PushBack(node)
		if checkpoint, ok := verifiedByHeight[node.Height]; ok {
			log.Infof("headersync verified checkpoint height=%d "+
				"hash=%s peer=%s range_id=%d",
				checkpoint.Height,
				checkpoint.Hash, checkpoint.PeerID,
				checkpoint.RangeID)
		}
	}

	t.nextCheckpoint = t.FindNextCheckpoint(prepared.FinalHeight)
}

// SerialExtension is a batch of headers validated against the handler's
// validation cache, ready for the durable write. It holds its own copies, since
// the write completes on another goroutine.
type SerialExtension struct {
	// Parent is the tip the batch was validated against.
	Parent headerfs.BlockStamp

	// Headers is the batch for the store.
	Headers []headerfs.BlockHeader

	// Nodes is the same batch for the validation cache.
	Nodes []headerlist.Node
}

// SerialCheckpointMismatch requests rollback to the last verified checkpoint.
type SerialCheckpointMismatch struct {
	// Height is the height of the checkpoint that the header failed.
	Height int32
}

// Error returns the height of the failed checkpoint.
func (e *SerialCheckpointMismatch) Error() string {
	return fmt.Sprintf("header does not match checkpoint at %d", e.Height)
}

// PrepareSerialExtension validates an extending batch on scratch state.
// Reorganizations and checkpoint rejection remain on the legacy path. The
// bool reports whether the batch extends the tip, and so whether the result
// and error mean anything.
func (t *TipState) PrepareSerialExtension(
	headers []*wire.BlockHeader) (SerialExtension, bool, error) {

	var prepared SerialExtension
	back := t.headerList.Back()
	if back == nil || headers[0].PrevBlock != back.Header.BlockHash() {
		return prepared, false, nil
	}
	prepared.Parent = headerfs.BlockStamp{Height: back.Height,
		Hash: back.Header.BlockHash(), Timestamp: back.Header.Timestamp}
	validation := NewValidationChain(t.headerList, *back)
	previous := back.Header
	height := back.Height
	for _, header := range headers {
		if err := t.check(
			header, height, &previous, validation,
		); err != nil {
			return SerialExtension{}, true, err
		}
		height++
		if t.nextCheckpoint != nil &&
			height == t.nextCheckpoint.Height &&
			header.BlockHash() != *t.nextCheckpoint.Hash {

			return SerialExtension{}, true,
				&SerialCheckpointMismatch{
					Height: height,
				}
		}
		copyHeader := *header
		node := headerlist.Node{Header: copyHeader, Height: height}
		prepared.Headers = append(
			prepared.Headers,
			headerfs.BlockHeader{
				BlockHeader: &copyHeader,
				Height:      uint32(height),
			},
		)
		prepared.Nodes = append(prepared.Nodes, node)
		validation.PushBack(node)
		previous = copyHeader
		if t.nextCheckpoint != nil &&
			height == t.nextCheckpoint.Height {

			break
		}
	}

	return prepared, true, nil
}
