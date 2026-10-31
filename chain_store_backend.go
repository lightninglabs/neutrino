package neutrino

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// blockManagerChainStore executes store commands without mutating the block
// handler's validation list, checkpoints, or header sync session.
type blockManagerChainStore struct {
	manager *blockManager
	pending *chainStoreRollbackIntent
}

// chainStoreRollbackIntent survives errors between individual rollback steps.
type chainStoreRollbackIntent struct {
	origin           headerfs.BlockStamp
	target           uint32
	step             *chainStorePendingDisconnect
	targetHeader     wire.BlockHeader
	replacement      []headerfs.BlockHeader
	rollbackComplete bool
}

// chainStorePendingDisconnect retains the event identity for one store step.
type chainStorePendingDisconnect struct {
	header wire.BlockHeader
	height uint32
	parent wire.BlockHeader
}

// reconcilePendingDisconnect completes an admitted rollback before another
// operation can observe or mutate its intermediate state.
func (s *blockManagerChainStore) reconcilePendingDisconnect() error {
	intent := s.pending
	if intent == nil {
		return nil
	}
	if intent.step != nil {
		step := intent.step
		header, height, err := s.manager.cfg.BlockHeaders.ChainTip()
		if err != nil {
			return err
		}
		childHash := step.header.BlockHash()
		parentHash := step.parent.BlockHash()
		if height == step.height && header.BlockHash() == childHash {
			_, filterHeight, err :=
				s.manager.cfg.RegFilterHeaders.ChainTip()
			if err != nil {
				return err
			}
			if filterHeight >= step.height {
				intent.step = nil
			} else {
				blocks := s.manager.cfg.BlockHeaders
				filterHeader, err := blocks.FetchHeaderByHeight(
					filterHeight,
				)
				if err != nil {
					return err
				}
				s.manager.rewindPublishedFilterTip(
					filterHeight, filterHeader.BlockHash(),
				)
				_, err = blocks.RollbackLastBlock()
				if err != nil {
					return err
				}
				header, height = &step.parent, step.height-1
			}
		}
		if intent.step != nil {
			if height+1 != step.height ||
				header.BlockHash() != parentHash {

				return fmt.Errorf(
					"unexpected durable tip while " +
						"resolving rollback",
				)
			}
			s.manager.publishStoredBlockTip(height, *header)
			s.manager.onBlockDisconnected(
				step.header,
				step.height,
				step.parent,
			)
			intent.step = nil
		}
	}
	if !intent.rollbackComplete {
		if err := s.rollbackBlocks(intent); err != nil {
			return err
		}
		intent.rollbackComplete = true
	}
	if err := s.applyReplacement(intent, true); err != nil {
		return err
	}
	s.pending = nil
	return nil
}

// rollbackBlocks keeps the admitted intent even when a read between steps
// fails, and retains each disconnect before either store is mutated.
func (s *blockManagerChainStore) rollbackBlocks(
	intent *chainStoreRollbackIntent,
) error {

	blocks := s.manager.cfg.BlockHeaders
	filters := s.manager.cfg.RegFilterHeaders
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
			return fmt.Errorf("rollback parent height mismatch")
		}
		intent.step = &chainStorePendingDisconnect{
			header: *header, height: height, parent: *parent,
		}
		if height <= filterHeight {
			stamp, err := filters.RollbackLastBlock(&parentHash)
			if err != nil {
				return errors.Join(
					err,
					s.manager.refreshFilterHeaderTip(
						filters,
					),
				)
			}
			filterHeight = uint32(stamp.Height)
			s.manager.rewindPublishedFilterTip(
				filterHeight,
				parentHash,
			)
		}
		if _, err := blocks.RollbackLastBlock(); err != nil {
			return err
		}
		s.manager.publishStoredBlockTip(parentHeight, *parent)
		s.manager.onBlockDisconnected(*header, height, *parent)
		intent.step = nil
		header, height = parent, parentHeight
	}
	return nil
}

// applyReplacement resumes a compound reorg from the verified durable prefix.
// The initial call uses the known fork header without a success-path readback.
func (s *blockManagerChainStore) applyReplacement(
	intent *chainStoreRollbackIntent, resume bool) error {

	if len(intent.replacement) == 0 {
		return nil
	}
	parent := headerfs.BlockStamp{Height: int32(intent.target),
		Hash:      intent.targetHeader.BlockHash(),
		Timestamp: intent.targetHeader.Timestamp}
	start := uint32(0)
	if resume {
		progress, err := s.readBlockWriteProgress(chainStoreBlockCommit{
			ExpectedParent: parent, Headers: intent.replacement,
		})
		if err != nil {
			return err
		}
		start = uint32(progress.BlockTip.Height) - intent.target
	}
	if start == uint32(len(intent.replacement)) {
		return nil
	}
	remaining := intent.replacement[start:]
	err := s.manager.cfg.BlockHeaders.WriteHeaders(remaining...)
	if err != nil {
		_, readErr := s.readBlockWriteProgress(chainStoreBlockCommit{
			ExpectedParent: parent, Headers: intent.replacement,
		})
		return errors.Join(err, readErr)
	}
	last := remaining[len(remaining)-1]
	s.manager.publishStoredBlockTip(last.Height, *last.BlockHeader)
	return nil
}

// commitBlocks checks the prepared parent before writing the block batch.
func (s *blockManagerChainStore) commitBlocks(
	request chainStoreBlockCommit) (chainStoreReply, error) {

	var reply chainStoreReply
	if err := s.reconcilePendingDisconnect(); err != nil {
		return reply, err
	}
	if len(request.Headers) == 0 {
		return reply, fmt.Errorf("empty block header batch")
	}
	store := s.manager.cfg.BlockHeaders
	header, height, err := store.ChainTip()
	if err != nil {
		return reply, err
	}
	if int32(height) != request.ExpectedParent.Height ||
		header.BlockHash() != request.ExpectedParent.Hash {

		return reply, fmt.Errorf("block header parent changed")
	}
	first := request.Headers[0]
	if first.Height != height+1 || first.PrevBlock != header.BlockHash() {
		return reply, fmt.Errorf(
			"block header batch does not extend tip",
		)
	}
	if err := store.WriteHeaders(request.Headers...); err != nil {
		// The store can apply a prefix before returning an error. Read
		// the durable tip before reporting progress; never treat zero
		// as genesis.
		progress, readErr := s.readBlockWriteProgress(request)
		return progress, errors.Join(err, readErr)
	}
	last := request.Headers[len(request.Headers)-1]
	s.manager.publishStoredBlockTip(last.Height, *last.BlockHeader)
	reply.BlockTip = headerfs.BlockStamp{
		Height: int32(last.Height), Hash: last.BlockHash(),
		Timestamp: last.Timestamp,
	}
	reply.BlockTipValid = true
	return reply, nil
}

// readBlockWriteProgress verifies that an ambiguous write reached only the
// expected parent or a prefix of the submitted batch before publishing it.
func (s *blockManagerChainStore) readBlockWriteProgress(
	request chainStoreBlockCommit) (chainStoreReply, error) {

	var reply chainStoreReply
	header, height, err := s.manager.cfg.BlockHeaders.ChainTip()
	if err != nil {
		return reply, err
	}
	parentHeight := uint32(request.ExpectedParent.Height)
	if height < parentHeight ||
		height-parentHeight > uint32(len(request.Headers)) {

		return reply, fmt.Errorf(
			"unexpected durable block write height %d",
			height,
		)
	}
	expected := request.ExpectedParent.Hash
	if height > parentHeight {
		expected = request.Headers[height-parentHeight-1].BlockHash()
	}
	if header.BlockHash() != expected {
		return reply, fmt.Errorf(
			"unexpected durable block write hash at %d",
			height,
		)
	}
	reply.BlockTip = headerfs.BlockStamp{
		Height: int32(
			height,
		), Hash: expected, Timestamp: header.Timestamp,
	}
	reply.BlockTipValid = true
	s.manager.publishStoredBlockTip(height, *header)
	return reply, nil
}

// rollback checks the tip and resolves the target before changing either
// store, so a successful operation needs no fallible readback.
func (s *blockManagerChainStore) rollback(
	request chainStoreRollback) (chainStoreReply, error) {

	return s.startRollback(request, nil)
}

// startRollback admits an operation once and keeps it until its rollback and
// optional replacement have both completed.
func (s *blockManagerChainStore) startRollback(
	request chainStoreRollback,
	replacement []headerfs.BlockHeader,
) (chainStoreReply, error) {

	var reply chainStoreReply
	if err := s.reconcilePendingDisconnect(); err != nil {
		return reply, err
	}
	store := s.manager.cfg.BlockHeaders
	tip, height, err := store.ChainTip()
	if err != nil {
		return reply, err
	}
	if int32(height) != request.ExpectedTip.Height ||
		tip.BlockHash() != request.ExpectedTip.Hash {

		return reply, fmt.Errorf("block header tip changed")
	}
	if request.Height > height {
		return reply, fmt.Errorf("rollback target exceeds block tip")
	}
	target, err := store.FetchHeaderByHeight(request.Height)
	if err != nil {
		return reply, err
	}
	copied := make([]headerfs.BlockHeader, len(replacement))
	for i, entry := range replacement {
		header := *entry.BlockHeader
		copied[i] = headerfs.BlockHeader{
			Height:      entry.Height,
			BlockHeader: &header,
		}
	}
	intent := &chainStoreRollbackIntent{
		origin:       request.ExpectedTip,
		target:       request.Height,
		targetHeader: *target,
		replacement:  copied,
	}
	s.pending = intent
	if err := s.rollbackBlocks(intent); err != nil {
		return reply, err
	}
	intent.rollbackComplete = true
	if err := s.applyReplacement(intent, false); err != nil {
		parent := headerfs.BlockStamp{
			Height: int32(
				intent.target,
			), Hash: intent.targetHeader.BlockHash(),
			Timestamp: intent.targetHeader.Timestamp,
		}
		progress, readErr := s.readBlockWriteProgress(
			chainStoreBlockCommit{
				ExpectedParent: parent, Headers: replacement,
			},
		)
		return progress, errors.Join(err, readErr)
	}
	s.pending = nil
	if len(replacement) > 0 {
		last := replacement[len(replacement)-1]
		reply.BlockTip = headerfs.BlockStamp{
			Height:    int32(last.Height),
			Hash:      last.BlockHash(),
			Timestamp: last.Timestamp,
		}
	} else {
		s.manager.publishStoredBlockTip(request.Height, *target)
		reply.BlockTip = headerfs.BlockStamp{
			Height:    int32(request.Height),
			Hash:      target.BlockHash(),
			Timestamp: target.Timestamp,
		}
	}
	reply.BlockTipValid = true
	return reply, nil
}

// reorg applies rollback and replacement as one retained operation.
func (s *blockManagerChainStore) reorg(
	request chainStoreReorg) (chainStoreReply, error) {

	return s.startRollback(chainStoreRollback{
		ExpectedTip: request.ExpectedTip, Height: request.ForkHeight,
	}, request.Headers)
}

// readBlockTip reports only a successfully read durable block tip.
func (s *blockManagerChainStore) readBlockTip() (chainStoreReply, error) {
	var reply chainStoreReply
	header, height, err := s.manager.cfg.BlockHeaders.ChainTip()
	if err != nil {
		return reply, err
	}
	reply.BlockTip = headerfs.BlockStamp{
		Height: int32(height), Hash: header.BlockHash(),
		Timestamp: header.Timestamp,
	}
	reply.BlockHeader = *header
	reply.BlockTipValid = true
	s.manager.publishStoredBlockTip(height, *header)
	return reply, nil
}

// commitFilters applies a verified filter batch on the store actor.
func (s *blockManagerChainStore) commitFilters(
	request chainStoreFilterCommit) (chainStoreReply, error) {

	var reply chainStoreReply
	if err := s.reconcilePendingDisconnect(); err != nil {
		return reply, err
	}
	if err := s.manager.reconcileFilterHeaderTip(
		s.manager.cfg.RegFilterHeaders,
	); err != nil {
		return reply, err
	}
	hash, height, err := s.manager.writeCFHeadersMsgs(
		request.Messages, s.manager.cfg.RegFilterHeaders,
	)
	if hash != nil {
		reply.Filter.filterTip = *hash
		reply.Filter.filterHeight = height
	}
	return reply, err
}

// snapshot reconciles filter publication and returns one query plan.
func (s *blockManagerChainStore) snapshot() (chainStoreReply, error) {
	var reply chainStoreReply
	if err := s.reconcilePendingDisconnect(); err != nil {
		return reply, err
	}
	reply, err := s.readBlockTip()
	if err != nil {
		return reply, err
	}
	reply.Filter, err = s.manager.cfHeaderQuerySnapshot(
		s.manager.cfg.RegFilterHeaders,
	)
	return reply, err
}
