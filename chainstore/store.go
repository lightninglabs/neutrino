package chainstore

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// Host is what the Store needs from the owner of the published tips. The Store
// decides what became durable, and the Host makes it visible to the rest of
// the node.
type Host interface {
	// BlockHeaders returns the block header store.
	BlockHeaders() headerfs.BlockHeaderStore

	// FilterHeaders returns the regular filter header store.
	FilterHeaders() headerfs.FilterHeaderStore

	// PublishBlockTip makes a durable block header the visible tip.
	PublishBlockTip(height uint32, header wire.BlockHeader)

	// PublishedFilterHeight returns the height of the visible filter tip.
	PublishedFilterHeight() uint32

	// PublishFilterTip makes the block at height the visible filter tip.
	PublishFilterTip(height uint32, hash chainhash.Hash)

	// PublishFilterHeaders makes the last of headers, which start at
	// startHeight, the visible filter tip, and sends a connected
	// notification for each one in order.
	PublishFilterHeaders(headers []wire.BlockHeader, startHeight uint32)

	// RewindFilterTip lowers the visible filter tip to height, and does
	// nothing if it's already at or below height. Durable filter headers
	// that were never published stay unpublished.
	RewindFilterTip(height uint32, hash chainhash.Hash)

	// BlockConnected sends a connected notification. It blocks until the
	// notification is accepted or the node shuts down.
	BlockConnected(header wire.BlockHeader, height uint32)

	// BlockDisconnected sends a disconnected notification. It blocks until
	// the notification is accepted or the node shuts down.
	BlockDisconnected(header wire.BlockHeader, height uint32,
		newTip wire.BlockHeader)
}

// Store executes chain store commands. All of its methods must run on the
// actor's goroutine, which is what makes the retained rollback safe to read
// without a lock.
type Store struct {
	host Host

	// pending is an admitted rollback that hasn't finished yet. Every
	// command resumes it first, so no command can observe or extend a
	// half rolled back chain.
	pending *rollbackIntent
}

// A compile time check that Store implements Operations.
var _ Operations = (*Store)(nil)

// NewStore creates a Store that publishes through host.
func NewStore(host Host) *Store {
	return &Store{host: host}
}

// HasPendingRollback reports whether an admitted rollback hasn't finished.
func (s *Store) HasPendingRollback() bool {
	return s.pending != nil
}

// CommitBlocks appends a block header batch if it still extends the durable
// tip it was prepared against.
func (s *Store) CommitBlocks(request BlockCommit) (Reply, error) {
	if err := s.resumePendingRollback(); err != nil {
		return Reply{}, err
	}

	if len(request.Headers) == 0 {
		return Reply{}, errors.New("empty block header batch")
	}

	store := s.host.BlockHeaders()
	header, height, err := store.ChainTip()
	if err != nil {
		return Reply{}, err
	}

	// The caller validated this batch against a tip it read earlier. If
	// that tip has moved, the validation no longer applies.
	if int32(height) != request.ExpectedParent.Height ||
		header.BlockHash() != request.ExpectedParent.Hash {

		return Reply{}, errors.New("block header parent changed")
	}

	first := request.Headers[0]
	if first.Height != height+1 || first.PrevBlock != header.BlockHash() {
		return Reply{}, errors.New(
			"block header batch does not extend tip",
		)
	}

	if err := store.WriteHeaders(request.Headers...); err != nil {
		// The store can apply a prefix before it returns an error, so
		// we read back what landed before reporting any progress.
		progress, readErr := s.verifyBlockWrite(request)

		return progress, errors.Join(err, readErr)
	}

	last := request.Headers[len(request.Headers)-1]
	s.host.PublishBlockTip(last.Height, *last.BlockHeader)

	return Reply{
		BlockTip:      blockStamp(last.Height, *last.BlockHeader),
		BlockTipValid: true,
	}, nil
}

// verifyBlockWrite reads the durable tip after an ambiguous write and checks
// that it's either the expected parent or a prefix of the submitted batch.
// Only then is it published. Anything else means the store holds headers we
// never asked for, which we report instead of adopting.
func (s *Store) verifyBlockWrite(request BlockCommit) (Reply, error) {
	header, height, err := s.host.BlockHeaders().ChainTip()
	if err != nil {
		return Reply{}, err
	}

	parentHeight := uint32(request.ExpectedParent.Height)
	if height < parentHeight ||
		height-parentHeight > uint32(len(request.Headers)) {

		return Reply{}, fmt.Errorf("unexpected durable block write "+
			"height %d", height)
	}

	expected := request.ExpectedParent.Hash
	if height > parentHeight {
		expected = request.Headers[height-parentHeight-1].BlockHash()
	}
	if header.BlockHash() != expected {
		return Reply{}, fmt.Errorf("unexpected durable block write "+
			"hash at %d", height)
	}

	s.host.PublishBlockTip(height, *header)

	return Reply{
		BlockTip:      blockStamp(height, *header),
		BlockTipValid: true,
	}, nil
}

// Snapshot finishes any retained rollback, publishes any durable but
// unpublished filter headers, and reads both tips. A failure on the filter side
// still returns the verified block tip, so the block handler can recover its
// view of the chain while the filter side's error is retried.
func (s *Store) Snapshot() (Reply, error) {
	if err := s.resumePendingRollback(); err != nil {
		return Reply{}, err
	}

	header, height, err := s.host.BlockHeaders().ChainTip()
	if err != nil {
		return Reply{}, err
	}
	s.host.PublishBlockTip(height, *header)

	reply := Reply{
		BlockTip:      blockStamp(height, *header),
		BlockHeader:   *header,
		BlockTipValid: true,
	}

	reply.Filter, err = s.filterSnapshot()

	return reply, err
}

// blockStamp builds the stamp of a block header at a known height.
func blockStamp(height uint32, header wire.BlockHeader) headerfs.BlockStamp {
	return headerfs.BlockStamp{
		Height:    int32(height),
		Hash:      header.BlockHash(),
		Timestamp: header.Timestamp,
	}
}
