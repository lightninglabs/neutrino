package headersync

// This file holds the serial header path's decisions: what to do with a
// headers message that follows the tip. The path is how a node follows the
// tip from inv and headers messages, including reorgs, and the only one on
// regtest and simnet. The driver decides, and the executor performs the peer
// I/O and the chain store submissions.

import (
	"errors"
	"math/big"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
)

// TipHeaders reports a non-empty headers message that no parallel range or
// anchor claimed. The driver decides whether it extends the tip, only holds
// known headers, carries a reorg, or is invalid, and answers with effects.
// An empty message depends on the executor's timer state, so the executor
// settles it and never sends it here.
type TipHeaders struct {
	// PeerID is the address of the peer that sent the headers.
	PeerID string

	// Headers are the headers of the message. The slice must not be
	// empty.
	Headers []*wire.BlockHeader

	// IsSyncPeer is set when the sender is the current sync peer.
	IsSyncPeer bool

	// HeadersSynced is set when the node believes it is current.
	HeadersSynced bool
}

func (TipHeaders) driverEvent() {}

// RollbackToCheckpoint asks for the rollback to the last verified checkpoint
// before a height, after a header of the peer failed the checkpoint at that
// height. The executor reports it for a reorg whose verdict came back.
type RollbackToCheckpoint struct {
	// PeerID is the address of the peer that sent the failing header.
	PeerID string

	// Height is the height of the checkpoint that the header failed.
	Height int32
}

func (RollbackToCheckpoint) driverEvent() {}

// DisconnectTipPeer asks the executor to disconnect a peer that sent invalid
// serial headers. Unlike DisconnectPeer it leaves the sync candidates alone,
// since the done-peer message removes the peer from them.
type DisconnectTipPeer struct {
	// PeerID is the address of the peer.
	PeerID string
}

func (DisconnectTipPeer) driverEffect() {}

// UpdatePeerHeight asks the executor to record the height a peer has proven
// that it holds.
type UpdatePeerHeight struct {
	// PeerID is the address of the peer.
	PeerID string

	// Height is the height of the last header the peer delivered.
	Height int32
}

func (UpdatePeerHeight) driverEffect() {}

// DisarmSerialTimeout asks the executor to cancel the serial getheaders
// deadline, since the sync peer answered with progress.
type DisarmSerialTimeout struct{}

func (DisarmSerialTimeout) driverEffect() {}

// CommitExtension asks the executor to write a batch that extends the tip
// through the chain store, without waiting for the write. The executor tracks
// the write so that it can finish the extension when the write lands.
type CommitExtension struct {
	// PeerID is the address of the peer that sent the batch.
	PeerID string

	// Extension is the validated batch.
	Extension SerialExtension
}

func (CommitExtension) driverEffect() {}

// CommitReorg asks the executor to submit a validated fork as one chain store
// mutation, so that no filter commit can land between the rollback and the
// replacement branch. The store decides whether the branch has enough work,
// so nothing may act on the branch until its verdict arrives.
type CommitReorg struct {
	// PeerID is the address of the peer that sent the branch.
	PeerID string

	// Reorg binds the branch to the tip and fork it was validated against.
	Reorg SerialReorg

	// Headers is the branch for the store. It is empty when Mismatch is
	// set.
	Headers []headerfs.BlockHeader

	// Nodes is the same branch for the validation cache. It is empty when
	// Mismatch is set.
	Nodes []headerlist.Node

	// Work is the work of the headers from the first replacement on.
	Work *big.Int

	// Mismatch is the height of a checkpoint the branch failed, or zero.
	// The store only compares work in that case.
	Mismatch int32
}

func (CommitReorg) driverEffect() {}

// CheckpointRollback asks the executor to roll the chain back to the last
// verified checkpoint. The peer that sent the failing header is dropped once
// the rollback lands.
type CheckpointRollback struct {
	// PeerID is the address of the peer that sent the failing header.
	PeerID string

	// Node is the checkpoint header, which becomes the new tip.
	Node headerlist.Node

	// ExpectedTip is the tip the rollback was planned against.
	ExpectedTip headerfs.BlockStamp
}

func (CheckpointRollback) driverEffect() {}

// SerialReorg binds a validated replacement to the durable tip and fork that
// the driver observed when preparing it.
type SerialReorg struct {
	// ExpectedTip is the durable tip the fork was compared against.
	ExpectedTip headerfs.BlockStamp

	// Fork is the last header both branches share.
	Fork headerlist.Node

	// Replacement is the first header of the new branch.
	Replacement headerfs.BlockHeader
}

// nodeStamp returns the block stamp of a validation cache entry.
func nodeStamp(node headerlist.Node) headerfs.BlockStamp {
	return headerfs.BlockStamp{
		Height:    node.Height,
		Hash:      node.Header.BlockHash(),
		Timestamp: node.Header.Timestamp,
	}
}

// NewSerialReorg captures the tip and fork a reorg was validated against,
// along with its first replacement header.
func NewSerialReorg(tip headerlist.Node, fork wire.BlockHeader,
	forkHeight uint32, replacement wire.BlockHeader) SerialReorg {

	return SerialReorg{
		ExpectedTip: nodeStamp(tip),
		Fork: headerlist.Node{
			Header: fork, Height: int32(forkHeight),
		},
		Replacement: headerfs.BlockHeader{
			BlockHeader: &replacement, Height: forkHeight + 1,
		},
	}
}

// handleTipHeaders decides what a headers message that follows the tip means.
func (d *Driver) handleTipHeaders(event TipHeaders, result *DriverResult) {
	// Explicitly check that each header in the message builds off of the
	// previous one. This is a quick sanity check to avoid doing the more
	// expensive checks below if we know the headers are invalid.
	if !HeadersConnected(event.Headers) {
		log.Warnf("Headers received from peer don't connect")
		d.disconnectTipPeer(event.PeerID, result)

		return
	}

	// A batch that extends the validation chain is committed through the
	// chain store actor. What remains are headers that do not connect to
	// our tip: known headers, a reorg, or garbage.
	if d.extendSerialTip(event, result) {
		return
	}

	d.followSerialFork(event, result)
}

// disconnectTipPeer asks for the peer to be disconnected.
func (d *Driver) disconnectTipPeer(peerID string, result *DriverResult) {
	result.Effects = append(
		result.Effects, DisconnectTipPeer{PeerID: peerID},
	)
}

// extendSerialTip commits a batch that extends the validation chain, without
// waiting for the write. It returns false when the batch does not extend the
// tip, so the caller can treat it as known headers or a reorg.
func (d *Driver) extendSerialTip(event TipHeaders,
	result *DriverResult) bool {

	back := d.tip.Tip()
	if back == nil {
		return false
	}

	headers, ok := headersAfterTip(back, event.Headers)
	if !ok {
		return false
	}

	prepared, handled, err := d.tip.PrepareSerialExtension(headers)
	if !handled {
		return false
	}
	if err != nil {
		d.rejectSerialExtension(event.PeerID, err, result)

		return true
	}

	last := prepared.Nodes[len(prepared.Nodes)-1]
	result.Effects = append(result.Effects, UpdatePeerHeight{
		PeerID: event.PeerID,
		Height: last.Height,
	})
	if event.IsSyncPeer {
		result.Effects = append(result.Effects, DisarmSerialTimeout{})
	}
	result.Effects = append(result.Effects, CommitExtension{
		PeerID:    event.PeerID,
		Extension: prepared,
	})

	return true
}

// headersAfterTip returns the part of a connected batch that follows the cache
// tip. It reports false when the batch has nothing past the tip.
func headersAfterTip(back *headerlist.Node,
	headers []*wire.BlockHeader) ([]*wire.BlockHeader, bool) {

	backHash := back.Header.BlockHash()
	if headers[0].PrevBlock == backHash {
		return headers, true
	}

	// A connected response may contain old headers before the current tip.
	// Find that tip without querying the store.
	for i, header := range headers {
		if header.BlockHash() == backHash {
			rest := headers[i+1:]

			return rest, len(rest) > 0
		}
	}

	return nil, false
}

// rejectSerialExtension answers a batch that failed validation: a checkpoint
// mismatch rolls the chain back, and anything else drops the peer.
func (d *Driver) rejectSerialExtension(peerID string, err error,
	result *DriverResult) {

	var mismatch *SerialCheckpointMismatch
	if errors.As(err, &mismatch) {
		d.planCheckpointRollback(peerID, mismatch.Height, result)

		return
	}

	log.Warnf("Header doesn't pass sanity check: %s -- "+
		"disconnecting peer", err)
	d.disconnectTipPeer(peerID, result)
}

// forkAttach is where a header that does not extend the tip attaches to the
// chain we know.
type forkAttach struct {
	// tip is the validation cache tip the fork is compared against.
	tip *headerlist.Node

	// head is the stored parent of the first unknown header.
	head *wire.BlockHeader

	// height is the height of head.
	height uint32
}

// followSerialFork handles headers that do not connect to our tip. Late
// responses may contain only headers already in the store. They must not
// publish a new tip or start a request from an old hash, so the loop skips
// them. Such a reply usually answers an older request, so it says nothing
// about the outstanding one: its deadline stays armed.
func (d *Driver) followSerialFork(event TipHeaders, result *DriverResult) {
	for i, blockHeader := range event.Headers {
		attach, stop := d.locateFork(event, blockHeader, result)
		if stop {
			return
		}
		if attach == nil {
			continue
		}

		work, ok := d.validateFork(event, i, attach, result)
		if !ok {
			return
		}

		// Preserve the validated fork and replacement as an immutable
		// payload for the chain store.
		reorg := NewSerialReorg(
			*attach.tip, *attach.head, attach.height, *blockHeader,
		)
		headers := event.Headers[i:]
		d.planReorg(event.PeerID, reorg, headers, work, result)

		return
	}
}

// locateFork finds where a header attaches to our chain. It returns nil with
// stop unset for a header that is already known, and stop set once the
// message needs no further processing.
func (d *Driver) locateFork(event TipHeaders, blockHeader *wire.BlockHeader,
	result *DriverResult) (*forkAttach, bool) {

	blockHash := blockHeader.BlockHash()

	// Ensure there is a previous header to compare against.
	prevNode := d.tip.Tip()
	if prevNode == nil {
		log.Warnf("Header list does not contain a previous" +
			"element as expected -- disconnecting peer")
		d.disconnectTipPeer(event.PeerID, result)

		return nil, true
	}
	prevHash := prevNode.Header.BlockHash()

	// The block doesn't connect to the last block we know.
	// We will need to do some additional checks to process
	// possible reorganizations or incorrect chain on
	// either our or the peer's side.
	//
	// If we got these headers from a peer that's not our
	// sync peer, they might not be aligned correctly or
	// even on the right chain. Just ignore the rest of the
	// message. However, if we're current, this might be a
	// reorg, in which case we'll either change our sync
	// peer or disconnect the peer that sent us these bad
	// headers.
	if !event.IsSyncPeer && !event.HeadersSynced {
		return nil, true
	}

	// Check if this is the last block we know of. This is
	// a shortcut for sendheaders so that each redundant
	// header doesn't cause a disk read.
	if blockHash == prevHash {
		return nil, false
	}

	// Check if this block is known. If so, we continue to
	// the next one.
	_, _, err := d.tip.FetchHeader(&blockHash)
	if err == nil {
		return nil, false
	}

	// Check if the previous block is known. If it is, this
	// is probably a reorg based on the estimated latest
	// block that matches between us and the peer as
	// derived from the block locator we sent to request
	// these headers. Otherwise, the headers don't connect
	// to anything we know and we should disconnect the
	// peer.
	backHead, backHeight, err := d.tip.FetchHeader(&blockHeader.PrevBlock)
	if err != nil {
		log.Warnf("Received block header that does "+
			"not"+
			" properly connect to the chain from"+
			" peer %s (%s) -- disconnecting",
			event.PeerID, err)
		d.disconnectTipPeer(event.PeerID, result)

		return nil, true
	}

	// We've found a branch we weren't aware of. If the
	// branch is earlier than the latest synchronized
	// checkpoint, it's invalid and we need to disconnect
	// the reporting peer.
	prevCheckpoint := d.tip.FindPreviousCheckpoint(prevNode.Height)
	if backHeight < uint32(prevCheckpoint.Height) {
		log.Errorf("Attempt at a reorg earlier than "+
			"a "+
			"checkpoint past which we've already "+
			"synchronized -- disconnecting peer "+
			"%s", event.PeerID)
		d.disconnectTipPeer(event.PeerID, result)

		return nil, true
	}

	return &forkAttach{
		tip: prevNode, head: backHead, height: backHeight,
	}, false
}

// validateFork checks the sanity of the new branch that starts at the header
// i of the message. If any of the blocks don't pass sanity checks, it asks for
// the peer to be disconnected and reports false. Otherwise it returns the
// work the branch represents, so the store can compare it to the work in the
// known good chain.
func (d *Driver) validateFork(event TipHeaders, i int, attach *forkAttach,
	result *DriverResult) (*big.Int, bool) {

	d.tip.ResetReorg(headerlist.Node{
		Header: *attach.head,
		Height: int32(attach.height),
	})

	totalWork := big.NewInt(0)
	for j, reorgHeader := range event.Headers[i:] {
		// We have to get the parent's height and header to be able to
		// contextually validate this header.
		prevNodeHeight := attach.height + uint32(j)

		var prevNodeHeader *wire.BlockHeader
		if i+j == 0 {
			// Use the stored parent if we are using the first
			// header in the Headers slice.
			prevNodeHeader = attach.head
		} else {
			// We can find the parent in the Headers slice by
			// getting the header at index i+j-1.
			prevNodeHeader = event.Headers[i+j-1]
		}

		err := d.tip.CheckReorgHeader(
			reorgHeader, int32(prevNodeHeight), prevNodeHeader,
		)
		if err != nil {
			log.Warnf("Header doesn't pass sanity"+
				" check: %s -- disconnecting "+
				"peer", err)
			d.disconnectTipPeer(event.PeerID, result)

			return nil, false
		}
		totalWork.Add(totalWork, blockchain.CalcWork(reorgHeader.Bits))
		d.tip.AppendReorg(headerlist.Node{
			Header: *reorgHeader,
			Height: int32(attach.height+1) + int32(j),
		})
	}

	return totalWork, true
}

// planReorg asks for a validated fork to be submitted. A branch that crosses a
// checkpoint must match it. If it doesn't, the store only compares work, and
// the executor rolls back if the branch would win. A batch that reaches the
// checkpoint stops there, so the next one validates against the following
// checkpoint.
func (d *Driver) planReorg(peerID string, reorg SerialReorg,
	headers []*wire.BlockHeader, work *big.Int, result *DriverResult) {

	var (
		batch []headerfs.BlockHeader
		nodes []headerlist.Node
		next  = d.tip.NextCheckpoint()
	)
	for i, header := range headers {
		height := reorg.Fork.Height + int32(i) + 1
		if next != nil && height == next.Height &&
			header.BlockHash() != *next.Hash {

			// A checkpoint mismatch only matters for a branch that
			// would win. Otherwise a low work fork could force a
			// rollback for free.
			log.Warnf(
				"Reorg header does not match checkpoint at %d",
				height,
			)
			result.Effects = append(result.Effects, CommitReorg{
				PeerID:   peerID,
				Reorg:    reorg,
				Work:     work,
				Mismatch: height,
			})

			return
		}
		copyHeader := *header
		batch = append(batch, headerfs.BlockHeader{
			BlockHeader: &copyHeader,
			Height:      uint32(height),
		})
		nodes = append(
			nodes,
			headerlist.Node{Header: copyHeader, Height: height},
		)
		if next != nil && height == next.Height {
			break
		}
	}

	result.Effects = append(result.Effects, CommitReorg{
		PeerID:  peerID,
		Reorg:   reorg,
		Headers: batch,
		Nodes:   nodes,
		Work:    work,
	})
}

// planCheckpointRollback asks for the chain to roll back to the last verified
// checkpoint before the height. A failed lookup of the checkpoint header
// schedules a retry instead.
func (d *Driver) planCheckpointRollback(peerID string, height int32,
	result *DriverResult) {

	checkpoint := d.tip.FindPreviousCheckpoint(height)
	header, err := d.tip.FetchHeaderByHeight(uint32(checkpoint.Height))
	if err != nil {
		log.Warnf("Unable to load checkpoint for rollback: %v", err)
		result.Effects = append(result.Effects, ScheduleCommitRetry{})

		return
	}
	tip := d.tip.Tip()
	if tip == nil {
		return
	}

	result.Effects = append(result.Effects, CheckpointRollback{
		PeerID: peerID,
		Node: headerlist.Node{
			Header: *header, Height: checkpoint.Height,
		},
		ExpectedTip: nodeStamp(*tip),
	})
}
