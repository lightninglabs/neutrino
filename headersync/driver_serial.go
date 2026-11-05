package headersync

// This file holds the serial header path's decisions: what to do with a
// headers message that follows the tip. The path is how a node follows the
// tip from inv and headers messages, including reorgs, and the only one on
// regtest and simnet. The driver decides, and the executor performs the peer
// I/O and the chain store submissions.

import (
	"context"
	"errors"
	"math/big"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/chainstore"
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

	// holdsLease is set by the driver when the sender holds the
	// tip-following lease.
	holdsLease bool

	// HeadersSynced is set when the node believes it is current.
	HeadersSynced bool
}

func (TipHeaders) driverEvent() {}

// FollowTip moves the tip-following lease to the executor's new sync peer, or
// releases it when PeerID is empty.
type FollowTip struct {
	// PeerID is the new sync peer, or empty for none.
	PeerID string
}

func (FollowTip) driverEvent() {}

// handleFollowTip moves the tip-following lease.
func (d *Driver) handleFollowTip(event FollowTip) {
	d.runtime.manager.SetTipLease(event.PeerID)
}

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
func (d *Driver) handleTipHeaders(event TipHeaders) []DriverEffect {
	var effects []DriverEffect

	// The lease decides whose headers we trust before we're synced.
	event.holdsLease = d.runtime.manager.TrustsTipPeer(event.PeerID)

	// Explicitly check that each header in the message builds off of the
	// previous one. This is a quick sanity check to avoid doing the more
	// expensive checks below if we know the headers are invalid.
	if !HeadersConnected(event.Headers) {
		log.Warnf("Headers received from peer don't connect")
		effects = append(effects, d.disconnectTipPeer(event.PeerID)...)

		return effects
	}

	// A batch that extends the validation chain is committed through the
	// chain store actor. What remains are headers that do not connect to
	// our tip: known headers, a reorg, or garbage.
	extendEffects, extended := d.extendSerialTip(event)
	effects = append(effects, extendEffects...)
	if extended {
		return effects
	}

	effects = append(effects, d.followSerialFork(event)...)

	return effects
}

// disconnectTipPeer asks for the peer to be disconnected.
func (d *Driver) disconnectTipPeer(peerID string) []DriverEffect {
	return []DriverEffect{DisconnectTipPeer{PeerID: peerID}}
}

// extendSerialTip commits a batch that extends the validation chain, without
// waiting for the write. It returns false when the batch does not extend the
// tip, so the caller can treat it as known headers or a reorg.
func (d *Driver) extendSerialTip(event TipHeaders) ([]DriverEffect, bool) {
	var effects []DriverEffect

	back := d.tip.Tip()
	if back == nil {
		return effects, false
	}

	headers, ok := headersAfterTip(back, event.Headers)
	if !ok {
		return effects, false
	}

	prepared, handled, err := d.tip.PrepareSerialExtension(headers)
	if !handled {
		return effects, false
	}
	if err != nil {
		effects = append(effects, d.rejectSerialExtension(
			event.PeerID, err,
		)...)

		return effects, true
	}

	last := prepared.Nodes[len(prepared.Nodes)-1]
	effects = append(effects, UpdatePeerHeight{
		PeerID: event.PeerID,
		Height: last.Height,
	})
	if event.holdsLease {
		effects = append(effects, DisarmSerialTimeout{})
	}
	effects = append(effects, CommitExtension{
		PeerID:    event.PeerID,
		Extension: prepared,
	})

	return effects, true
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
func (d *Driver) rejectSerialExtension(peerID string,
	err error) []DriverEffect {

	var effects []DriverEffect

	var mismatch *SerialCheckpointMismatch
	if errors.As(err, &mismatch) {
		effects = append(effects, d.planCheckpointRollback(
			peerID, mismatch.Height,
		)...)

		return effects
	}

	log.Warnf("Header doesn't pass sanity check: %s -- "+
		"disconnecting peer", err)
	effects = append(effects, d.disconnectTipPeer(peerID)...)

	return effects
}

// forkLocation is the outcome of looking for where a header attaches. Attach
// is nil for a header that is already known, Stop is set once the message
// needs no further processing, and Effects holds what the lookup asked for on
// the way, such as the peer's disconnect.
type forkLocation struct {
	Attach  *forkAttach
	Stop    bool
	Effects []DriverEffect
}

// forkValidation is the outcome of validating a fork. Work is the work the
// branch represents and is only set when OK is, and Effects holds what the
// validation asked for on the way.
type forkValidation struct {
	Work    *big.Int
	OK      bool
	Effects []DriverEffect
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
func (d *Driver) followSerialFork(event TipHeaders) []DriverEffect {
	var effects []DriverEffect

	for i, blockHeader := range event.Headers {
		located := d.locateFork(event, blockHeader)
		effects = append(effects, located.Effects...)
		if located.Stop {
			return effects
		}
		attach := located.Attach
		if attach == nil {
			continue
		}

		validated := d.validateFork(event, i, attach)
		effects = append(effects, validated.Effects...)
		if !validated.OK {
			return effects
		}

		// Preserve the validated fork and replacement as an immutable
		// payload for the chain store.
		reorg := NewSerialReorg(
			*attach.tip, *attach.head, attach.height, *blockHeader,
		)
		headers := event.Headers[i:]
		effects = append(effects, d.planReorg(
			event.PeerID, reorg, headers, validated.Work,
		)...)

		return effects
	}

	return effects
}

// locateFork finds where a header attaches to our chain. It returns nil with
// stop unset for a header that is already known, and stop set once the
// message needs no further processing.
func (d *Driver) locateFork(event TipHeaders,
	blockHeader *wire.BlockHeader) forkLocation {

	var effects []DriverEffect

	blockHash := blockHeader.BlockHash()

	// Ensure there is a previous header to compare against.
	prevNode := d.tip.Tip()
	if prevNode == nil {
		log.Warnf("Header list does not contain a previous" +
			"element as expected -- disconnecting peer")
		effects = append(effects, d.disconnectTipPeer(event.PeerID)...)

		return forkLocation{Stop: true, Effects: effects}
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
	if !event.holdsLease && !event.HeadersSynced {
		return forkLocation{Stop: true, Effects: effects}
	}

	// Check if this is the last block we know of. This is
	// a shortcut for sendheaders so that each redundant
	// header doesn't cause a disk read.
	if blockHash == prevHash {
		return forkLocation{}
	}

	// Check if this block is known. If so, we continue to
	// the next one.
	_, _, err := d.tip.FetchHeader(&blockHash)
	if err == nil {
		return forkLocation{}
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
		effects = append(effects, d.disconnectTipPeer(event.PeerID)...)

		return forkLocation{Stop: true, Effects: effects}
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
		effects = append(effects, d.disconnectTipPeer(event.PeerID)...)

		return forkLocation{Stop: true, Effects: effects}
	}

	return forkLocation{Attach: &forkAttach{
		tip: prevNode, head: backHead, height: backHeight,
	}}
}

// validateFork checks the sanity of the new branch that starts at the header
// i of the message. If any of the blocks don't pass sanity checks, it asks for
// the peer to be disconnected and reports false. Otherwise it returns the
// work the branch represents, so the store can compare it to the work in the
// known good chain.
func (d *Driver) validateFork(event TipHeaders, i int,
	attach *forkAttach) forkValidation {

	var effects []DriverEffect

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
			effects = append(effects, d.disconnectTipPeer(
				event.PeerID,
			)...)

			return forkValidation{Effects: effects}
		}
		totalWork.Add(totalWork, blockchain.CalcWork(reorgHeader.Bits))
		d.tip.AppendReorg(headerlist.Node{
			Header: *reorgHeader,
			Height: int32(attach.height+1) + int32(j),
		})
	}

	return forkValidation{Work: totalWork, OK: true}
}

// planReorg asks for a validated fork to be submitted. A branch that crosses a
// checkpoint must match it. If it doesn't, the store only compares work, and
// the executor rolls back if the branch would win. A batch that reaches the
// checkpoint stops there, so the next one validates against the following
// checkpoint.
func (d *Driver) planReorg(peerID string, reorg SerialReorg,
	headers []*wire.BlockHeader, work *big.Int) []DriverEffect {

	var effects []DriverEffect

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
			effects = append(effects, CommitReorg{
				PeerID:   peerID,
				Reorg:    reorg,
				Work:     work,
				Mismatch: height,
			})

			return effects
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

	effects = append(effects, CommitReorg{
		PeerID:  peerID,
		Reorg:   reorg,
		Headers: batch,
		Nodes:   nodes,
		Work:    work,
	})

	return effects
}

// planCheckpointRollback asks for the chain to roll back to the last verified
// checkpoint before the height. A failed lookup of the checkpoint header
// schedules a retry instead.
func (d *Driver) planCheckpointRollback(peerID string,
	height int32) []DriverEffect {

	var effects []DriverEffect

	checkpoint := d.tip.FindPreviousCheckpoint(height)
	header, err := d.tip.FetchHeaderByHeight(uint32(checkpoint.Height))
	if err != nil {
		log.Warnf("Unable to load checkpoint for rollback: %v", err)
		effects = append(effects, ScheduleCommitRetry{})

		return effects
	}
	tip := d.tip.Tip()
	if tip == nil {
		return effects
	}

	effects = append(effects, CheckpointRollback{
		PeerID: peerID,
		Node: headerlist.Node{
			Header: *header, Height: checkpoint.Height,
		},
		ExpectedTip: nodeStamp(*tip),
	})

	return effects
}

// ExtensionCommitted reports that the chain store wrote a serial extension.
// The driver adopts it into the cache and asks for the headers after it.
type ExtensionCommitted struct {
	// PeerID is the address of the peer that sent the batch.
	PeerID string

	// Extension is the batch the store wrote.
	Extension SerialExtension
}

func (ExtensionCommitted) driverEvent() {}

// ReorgCommitted reports the chain store's verdict on a submitted fork, and
// that the store switched to the branch when the verdict accepted it.
type ReorgCommitted struct {
	// PeerID is the address of the peer that sent the branch.
	PeerID string

	// Reorg is the fork the store was asked to switch to.
	Reorg SerialReorg

	// Nodes is the branch for the validation cache. It is empty when
	// Mismatch is set.
	Nodes []headerlist.Node

	// Mismatch is the height of a checkpoint the branch failed, or zero.
	Mismatch int32

	// Verdict is the store's work comparison.
	Verdict chainstore.ReorgVerdict
}

func (ReorgCommitted) driverEvent() {}

// CheckpointRolledBack reports that the chain store rolled back to the last
// verified checkpoint. The driver resets the cache there and drops the peer
// whose header failed the next checkpoint.
type CheckpointRolledBack struct {
	// PeerID is the address of the peer that sent the failing header.
	PeerID string

	// Node is the checkpoint header, now the tip.
	Node headerlist.Node
}

func (CheckpointRolledBack) driverEvent() {}

// SnapshotAdopted reports the durable tip the executor read back after a
// chain store mutation failed. Our view of the chain is unknown until then,
// so the driver resets the cache to it and retries whatever is staged.
type SnapshotAdopted struct {
	// Node is the durable tip.
	Node headerlist.Node

	// CheckpointPeerID is the address of the peer whose header failed a
	// checkpoint, when the failed mutation was the rollback that followed.
	// The failure must not lose the punishment, or the peer could keep
	// serving bad checkpoints.
	CheckpointPeerID string
}

func (SnapshotAdopted) driverEvent() {}

// ContinueSerial asks the executor to ask the sync peer for the headers after
// a batch that became durable, up to the stop hash.
type ContinueSerial struct {
	// PeerID is the address of the peer that sent the batch.
	PeerID string

	// FinalHash is the hash of the last header of the batch.
	FinalHash Hash

	// FinalHeight is the height of FinalHash.
	FinalHeight int32

	// StopHash is the hash of the next checkpoint, or zero past the last
	// one.
	StopHash Hash
}

func (ContinueSerial) driverEffect() {}

// handleExtensionCommitted adopts a written extension and asks for the
// headers after it.
func (d *Driver) handleExtensionCommitted(ctx context.Context,
	event ExtensionCommitted) []DriverEffect {

	var effects []DriverEffect

	nodes := event.Extension.Nodes
	d.logProgress(nodes)
	d.tip.Extend(nodes)

	last := nodes[len(nodes)-1]
	hash := last.Header.BlockHash()
	d.advanceTip(ctx, uint32(last.Height), hash)
	effects = append(effects, d.continueSerial(
		event.PeerID, hash, last.Height,
	)...)

	return effects
}

// handleReorgCommitted acts on the store's verdict: a losing branch costs the
// peer its connection, a tie changes nothing, a winning branch that failed a
// checkpoint rolls the chain back, and a winning branch rebuilds the cache
// and makes its sender the sync peer.
func (d *Driver) handleReorgCommitted(ctx context.Context,
	event ReorgCommitted) []DriverEffect {

	var effects []DriverEffect

	peerID := event.PeerID
	switch {
	// A branch with less work is an invalid claim, and so is a tie that
	// also failed a checkpoint, so we drop the peer.
	case event.Verdict == chainstore.ReorgLessWork,
		event.Verdict == chainstore.ReorgEqualWork &&
			event.Mismatch != 0:

		log.Warnf("Reorg attempt that has less work than known chain "+
			"or fails a checkpoint, from peer %s -- disconnecting",
			peerID)
		effects = append(effects, d.disconnectTipPeer(peerID)...)
		effects = append(effects, d.restartSyncIfIdle()...)

		return effects

	// A tie changes nothing, and says nothing bad about the peer.
	case event.Verdict == chainstore.ReorgEqualWork:
		effects = append(effects, d.restartSyncIfIdle()...)

		return effects

	// The branch would win, but it failed a checkpoint.
	case event.Mismatch != 0:
		effects = append(effects, d.planCheckpointRollback(
			peerID, event.Mismatch,
		)...)

		return effects
	}

	// The branch won. It answered the sync peer's request, and the peer
	// that delivered it becomes the sync peer, unless it has gone away.
	nodes := event.Nodes
	last := nodes[len(nodes)-1]
	effects = append(effects, UpdatePeerHeight{
		PeerID: peerID,
		Height: last.Height,
	})
	if d.runtime.manager.TrustsTipPeer(peerID) {
		effects = append(effects, DisarmSerialTimeout{})
	}
	effects = append(effects, SetSyncPeer{
		PeerID: peerID, UnlessGone: true,
	})

	d.tip.Reset(event.Reorg.Fork)
	d.tip.Extend(nodes)
	d.logProgress(nodes)

	hash := last.Header.BlockHash()
	d.advanceTip(ctx, uint32(last.Height), hash)
	effects = append(effects, d.continueSerial(
		peerID, hash, last.Height,
	)...)

	return effects
}

// continueSerial asks for the headers after a durable batch, aimed at the
// next checkpoint while there is one.
func (d *Driver) continueSerial(peerID string, finalHash Hash,
	finalHeight int32) []DriverEffect {

	var effects []DriverEffect

	effect := ContinueSerial{
		PeerID:      peerID,
		FinalHash:   finalHash,
		FinalHeight: finalHeight,
	}
	if next := d.tip.NextCheckpoint(); next != nil {
		effect.StopHash = *next.Hash
	}

	effects = append(effects, effect)

	return effects
}

// handleCheckpointRolledBack resets the cache to the checkpoint and drops the
// peer whose header failed the next one.
func (d *Driver) handleCheckpointRolledBack(ctx context.Context,
	event CheckpointRolledBack) []DriverEffect {

	var effects []DriverEffect

	node := event.Node
	d.tip.Reset(node)
	d.advanceTip(ctx, uint32(node.Height), node.Header.BlockHash())

	effects = append(effects, d.rejectCheckpointPeer(event.PeerID)...)

	return effects
}

// handleSnapshotAdopted resets the cache to the durable tip after a failed
// mutation and retries whatever is staged. A snapshot that completed a
// checkpoint rollback still drops the peer.
func (d *Driver) handleSnapshotAdopted(ctx context.Context,
	event SnapshotAdopted) []DriverEffect {

	var effects []DriverEffect

	node := event.Node
	d.tip.Reset(node)
	d.advanceTip(ctx, uint32(node.Height), node.Header.BlockHash())

	// The retry wakeup goes before the restart, which may re-enter the
	// driver and so must be the last effect. Arming the wakeup commutes
	// with a restart, which never cancels it.
	effects = append(effects, ScheduleCommitRetry{})
	if event.CheckpointPeerID != "" {
		effects = append(effects, d.rejectCheckpointPeer(
			event.CheckpointPeerID,
		)...)
	}

	return effects
}

// rejectCheckpointPeer disconnects the peer whose header failed a checkpoint
// after the rollback completed. If that peer was the sync peer and already
// went away while the rollback was pending, no peer-done handling restarts
// sync, so it restarts here.
func (d *Driver) rejectCheckpointPeer(peerID string) []DriverEffect {
	var effects []DriverEffect

	effects = append(effects, d.disconnectTipPeer(peerID)...)
	effects = append(effects, d.restartSyncIfIdle()...)

	return effects
}

// restartSyncIfIdle restarts sync when the sync peer went away while a
// mutation's outcome was pending. The peer-done handling skips the restart
// while a commit is pending, so nothing else would.
func (d *Driver) restartSyncIfIdle() []DriverEffect {
	var effects []DriverEffect

	if d.runtime.manager.TipLease().PeerID == "" {
		effects = append(effects, StartSync{})
	}

	return effects
}
