package chainstore

import (
	"errors"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// rollbackIntent is an admitted rollback, plus the optional replacement branch
// of a reorg. A rollback is many separate store writes, and any of them can
// fail or apply and then report failure. We retain the intent across those
// failures so that the next command finishes the job: the chain never stays
// part way down, and each disconnect notification is sent exactly once.
type rollbackIntent struct {
	// target is the height of the header that becomes the new tip.
	target uint32

	// targetHeader is the header at target.
	targetHeader wire.BlockHeader

	// step is the single block removal in flight. It's set before either
	// store is touched and cleared once the disconnect has been sent. If
	// it's still set on resume, the removal stopped part way.
	step *disconnectStep

	// rollbackDone is set once the chain reaches target.
	rollbackDone bool

	// replacement is the branch to append once the rollback is done.
	replacement []headerfs.BlockHeader
}

// fork returns the stamp of the header the replacement branch extends.
func (i *rollbackIntent) fork() headerfs.BlockStamp {
	return blockStamp(i.target, i.targetHeader)
}

// replacementCommit expresses the replacement branch as a block commit, so
// that it can share the verification of an ambiguous write.
func (i *rollbackIntent) replacementCommit() BlockCommit {
	return BlockCommit{
		ExpectedParent: i.fork(),
		Headers:        i.replacement,
	}
}

// disconnectStep identifies the removal of one block from the tip, and carries
// what its disconnect notification needs.
type disconnectStep struct {
	// header is the block being removed.
	header wire.BlockHeader

	// height is the height of header.
	height uint32

	// parent is the header that becomes the tip.
	parent wire.BlockHeader
}

// Rollback removes block and filter headers down to the requested height.
func (s *Store) Rollback(request Rollback) (Reply, error) {
	return s.startRollback(request, nil)
}

// Reorg rolls back to the fork height and appends the replacement branch, as
// one retained operation.
func (s *Store) Reorg(request Reorg) (Reply, error) {
	return s.startRollback(Rollback{
		ExpectedTip: request.ExpectedTip,
		Height:      request.ForkHeight,
	}, request.Headers)
}

// startRollback admits a rollback, then runs it to completion. All the checks
// happen before either store changes. From the moment the intent is retained,
// an error means the operation will be resumed, not that it was abandoned.
func (s *Store) startRollback(request Rollback,
	replacement []headerfs.BlockHeader) (Reply, error) {

	if err := s.resumePendingRollback(); err != nil {
		return Reply{}, err
	}

	store := s.host.BlockHeaders()
	tip, height, err := store.ChainTip()
	if err != nil {
		return Reply{}, err
	}
	if int32(height) != request.ExpectedTip.Height ||
		tip.BlockHash() != request.ExpectedTip.Hash {

		return Reply{}, errors.New("block header tip changed")
	}
	if request.Height > height {
		return Reply{}, errors.New("rollback target exceeds block tip")
	}

	target, err := store.FetchHeaderByHeight(request.Height)
	if err != nil {
		return Reply{}, err
	}

	// The intent can outlive this command, so it takes its own copy of the
	// headers instead of aliasing the caller's.
	intent := &rollbackIntent{
		target:       request.Height,
		targetHeader: *target,
		replacement:  copyHeaders(replacement),
	}
	s.pending = intent

	if err := s.rollBackBlocks(intent); err != nil {
		return Reply{}, err
	}
	intent.rollbackDone = true

	if err := s.writeReplacement(intent, false); err != nil {
		progress, readErr := s.verifyBlockWrite(
			intent.replacementCommit(),
		)

		return progress, errors.Join(err, readErr)
	}
	s.pending = nil

	// A plain rollback publishes the target here, since a rollback of zero
	// blocks has published nothing yet. A reorg already published the last
	// replacement header.
	if len(replacement) == 0 {
		s.host.PublishBlockTip(request.Height, *target)

		return Reply{
			BlockTip:      blockStamp(request.Height, *target),
			BlockTipValid: true,
		}, nil
	}

	last := replacement[len(replacement)-1]

	return Reply{
		BlockTip:      blockStamp(last.Height, *last.BlockHeader),
		BlockTipValid: true,
	}, nil
}

// resumePendingRollback finishes a retained rollback, if there is one. Every
// command calls this first.
func (s *Store) resumePendingRollback() error {
	intent := s.pending
	if intent == nil {
		return nil
	}

	if intent.step != nil {
		if err := s.settleInterruptedStep(intent); err != nil {
			return err
		}
	}

	if !intent.rollbackDone {
		if err := s.rollBackBlocks(intent); err != nil {
			return err
		}
		intent.rollbackDone = true
	}

	if err := s.writeReplacement(intent, true); err != nil {
		return err
	}
	s.pending = nil

	return nil
}

// settleInterruptedStep works out how far a failed block removal got, by
// reading only the durable stores. A removal touches the filter store first,
// if the block has a filter header, and the block store second, so there are
// three possible states:
//
//  1. Neither store changed. We clear the step, and rollBackBlocks repeats the
//     removal from the start.
//
//  2. The filter store is below the block, because its header was removed or
//     it never had one, and the block is still there. We finish by removing
//     the block.
//
//  3. Both are gone, and only the publication and notification were lost.
//
// States 2 and 3 end with the parent as the tip, which is when we publish it
// and send the disconnect.
func (s *Store) settleInterruptedStep(intent *rollbackIntent) error {
	step := intent.step

	blocks := s.host.BlockHeaders()
	tip, height, err := blocks.ChainTip()
	if err != nil {
		return err
	}

	if height == step.height && tip.BlockHash() == step.header.BlockHash() {
		_, filterHeight, err := s.host.FilterHeaders().ChainTip()
		if err != nil {
			return err
		}

		// State 1: the filter header is still there, so nothing has
		// happened yet.
		if filterHeight >= step.height {
			intent.step = nil

			return nil
		}

		// State 2: the filter store is already below the block. The
		// published filter tip has to come down first, so that nothing
		// reads a filter tip above the store.
		filterHeader, err := blocks.FetchHeaderByHeight(filterHeight)
		if err != nil {
			return err
		}
		s.host.RewindFilterTip(filterHeight, filterHeader.BlockHash())

		if _, err := blocks.RollbackLastBlock(); err != nil {
			return err
		}

		tip, height = &step.parent, step.height-1
	}

	// Any tip other than the parent means something outside the rollback
	// changed the chain. We can't reason past that, so we report it and
	// keep the intent.
	if height+1 != step.height ||
		tip.BlockHash() != step.parent.BlockHash() {

		return errors.New("unexpected durable tip while resolving " +
			"rollback")
	}

	s.host.PublishBlockTip(height, *tip)
	s.host.BlockDisconnected(step.header, step.height, step.parent)
	intent.step = nil

	return nil
}

// rollBackBlocks removes blocks from the tip until it reaches the target. Each
// removal is recorded in intent.step before either store changes, so a failure
// at any point can be settled later.
func (s *Store) rollBackBlocks(intent *rollbackIntent) error {
	blocks := s.host.BlockHeaders()
	filters := s.host.FilterHeaders()

	header, height, err := blocks.ChainTip()
	if err != nil {
		return err
	}
	_, filterHeight, err := filters.ChainTip()
	if err != nil {
		return err
	}

	for height > intent.target {
		parentHash := header.PrevBlock
		parent, parentHeight, err := blocks.FetchHeader(&parentHash)
		if err != nil {
			return err
		}
		if parentHeight+1 != height {
			return errors.New("rollback parent height mismatch")
		}

		intent.step = &disconnectStep{
			header: *header,
			height: height,
			parent: *parent,
		}

		// The filter tip can lag the block tip, in which case there's
		// no filter header to remove at this height. When there is,
		// it goes first: a filter header must never outlive its block.
		if height <= filterHeight {
			stamp, err := filters.RollbackLastBlock(&parentHash)
			if err != nil {
				return errors.Join(err, s.refreshFilterTip())
			}

			filterHeight = uint32(stamp.Height)
			s.host.RewindFilterTip(filterHeight, parentHash)
		}

		if _, err := blocks.RollbackLastBlock(); err != nil {
			return err
		}

		s.host.PublishBlockTip(parentHeight, *parent)
		s.host.BlockDisconnected(*header, height, *parent)
		intent.step = nil

		header, height = parent, parentHeight
	}

	return nil
}

// writeReplacement appends the replacement branch of a reorg. On a first
// attempt the chain is known to sit at the fork, so we skip the read. On resume
// an earlier write may have landed a prefix, so we read how much and write only
// the remainder.
func (s *Store) writeReplacement(intent *rollbackIntent, resume bool) error {
	if len(intent.replacement) == 0 {
		return nil
	}

	var written uint32
	if resume {
		progress, err := s.verifyBlockWrite(intent.replacementCommit())
		if err != nil {
			return err
		}
		written = uint32(progress.BlockTip.Height) - intent.target
	}
	if written == uint32(len(intent.replacement)) {
		return nil
	}

	remaining := intent.replacement[written:]
	err := s.host.BlockHeaders().WriteHeaders(remaining...)
	if err != nil {
		_, readErr := s.verifyBlockWrite(intent.replacementCommit())

		return errors.Join(err, readErr)
	}

	last := remaining[len(remaining)-1]
	s.host.PublishBlockTip(last.Height, *last.BlockHeader)

	return nil
}

// refreshFilterTip lowers the published filter tip to the durable one, after a
// filter rollback whose outcome is unknown.
func (s *Store) refreshFilterTip() error {
	_, height, err := s.host.FilterHeaders().ChainTip()
	if err != nil {
		return err
	}

	header, err := s.host.BlockHeaders().FetchHeaderByHeight(height)
	if err != nil {
		return err
	}
	s.host.RewindFilterTip(height, header.BlockHash())

	return nil
}

// copyHeaders deep copies a header batch.
func copyHeaders(headers []headerfs.BlockHeader) []headerfs.BlockHeader {
	copied := make([]headerfs.BlockHeader, len(headers))
	for i, entry := range headers {
		header := *entry.BlockHeader
		copied[i] = headerfs.BlockHeader{
			Height:      entry.Height,
			BlockHeader: &header,
		}
	}

	return copied
}
