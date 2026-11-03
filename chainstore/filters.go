package chainstore

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// CommitFilters appends a verified batch of filter headers. The batch is bound
// to the block store here, on the actor, so it can't land on a block that a
// rollback has already removed.
func (s *Store) CommitFilters(request FilterCommit) (Reply, error) {
	if err := s.resumePendingRollback(); err != nil {
		return Reply{}, err
	}
	if err := s.reconcileFilterTip(); err != nil {
		return Reply{}, err
	}

	var reply Reply
	hash, height, err := s.writeFilterHeaders(request.Messages)
	if hash != nil {
		reply.Filter.FilterTip = *hash
		reply.Filter.FilterHeight = height
	}

	return reply, err
}

// writeFilterHeaders derives the filter headers in msgs, binds each one to its
// block, writes them, and publishes however many became durable. It returns the
// new tip.
func (s *Store) writeFilterHeaders(
	msgs []*wire.MsgCFHeaders) (*chainhash.Hash, uint32, error) {

	if len(msgs) == 0 {
		return nil, 0, errors.New("no cfheaders messages to write")
	}

	// The batch must chain from what we've already stored.
	store := s.host.FilterHeaders()
	tip, tipHeight, err := store.ChainTip()
	if err != nil {
		return nil, 0, err
	}
	if *tip != msgs[0].PrevFilterHeader {
		return nil, 0, fmt.Errorf("attempt to write cfheaders out of "+
			"order, tip=%v (height=%v), prev_hash=%v", *tip,
			tipHeight, msgs[0].PrevFilterHeader)
	}

	batch, lastHeader, stopHash, err := deriveFilterHeaders(msgs)
	if err != nil {
		return nil, 0, err
	}
	if len(batch) == 0 {
		return tip, tipHeight, nil
	}

	// Next, we fetch the block headers these filter headers commit to, by
	// walking back from the stop hash. If the stop block isn't on our chain
	// any more, this fails and the batch is refused: this is the check that
	// keeps a stale batch from before a reorg out of the store.
	blocks, startHeight, err := s.host.BlockHeaders().FetchHeaderAncestors(
		uint32(len(batch)-1), &stopHash,
	)
	if err != nil {
		return nil, 0, err
	}

	// The SQL store keeps each filter header as a row keyed by height and
	// block hash, so every entry needs both, not only the last.
	for i := range batch {
		batch[i].HeaderHash = blocks[i].BlockHash()
		batch[i].Height = startHeight + uint32(i)
	}

	lastHeight := startHeight + uint32(len(batch)) - 1
	log.Debugf("Writing filter headers up to height=%v, hash=%v, "+
		"new_tip=%v", lastHeight, blocks[len(batch)-1].BlockHash(),
		lastHeader)

	// A store can apply a prefix and then report an error, so a failed
	// write publishes however much we can verify landed.
	committed := len(batch)
	writeErr := store.WriteHeaders(batch...)
	if writeErr != nil {
		committed, err = verifyFilterWrite(
			store, batch, *tip, tipHeight, startHeight, lastHeight,
		)
		if err != nil {
			return nil, 0, errors.Join(writeErr, err)
		}
	}
	s.host.PublishFilterHeaders(blocks[:committed], startHeight)

	if writeErr != nil {
		return nil, 0, writeErr
	}

	return &lastHeader, lastHeight, nil
}

// deriveFilterHeaders computes each filter header from its filter hash and the
// previous header, as defined by BIP 157. It returns the headers, with only the
// filter hash filled in, along with the last header and the last stop hash.
func deriveFilterHeaders(msgs []*wire.MsgCFHeaders) ([]headerfs.FilterHeader,
	chainhash.Hash, chainhash.Hash, error) {

	total := 0
	for _, msg := range msgs {
		total += len(msg.FilterHashes)
	}

	var (
		last     = msgs[0].PrevFilterHeader
		batch    = make([]headerfs.FilterHeader, 0, total)
		stopHash chainhash.Hash
	)
	for i, msg := range msgs {
		if msg.PrevFilterHeader != last {
			return nil, last, stopHash, fmt.Errorf("attempt to "+
				"batch disconnected cfheaders at "+
				"msg_index=%d, prev=%v, want=%v", i,
				msg.PrevFilterHeader, last)
		}
		stopHash = msg.StopHash

		for _, hash := range msg.FilterHashes {
			// header = dsha256(filterHash || prevHeader)
			last = chainhash.DoubleHashH(
				append(hash[:], last[:]...),
			)

			batch = append(batch, headerfs.FilterHeader{
				FilterHash: last,
			})
		}
	}

	return batch, last, stopHash, nil
}

// verifyFilterWrite reads the store after a failed write and returns how many
// entries of batch became durable. Every durable entry has to match what we
// submitted, both by height and by block, or we refuse to publish any of it.
func verifyFilterWrite(store headerfs.FilterHeaderStore,
	batch []headerfs.FilterHeader, prevTip chainhash.Hash, prevHeight,
	startHeight, lastHeight uint32) (int, error) {

	durableTip, durableHeight, err := store.ChainTip()
	if err != nil {
		return 0, err
	}

	if startHeight != prevHeight+1 || durableHeight < prevHeight ||
		durableHeight > lastHeight {

		return 0, fmt.Errorf("unexpected filter tip height %d after "+
			"write", durableHeight)
	}

	committed := int(durableHeight - prevHeight)
	if committed == 0 && *durableTip != prevTip {
		return 0, fmt.Errorf("filter tip changed at height %d",
			prevHeight)
	}

	for i := 0; i < committed; i++ {
		entry := batch[i]
		byHeight, err := store.FetchHeaderByHeight(entry.Height)
		if err != nil {
			return 0, err
		}
		byBlock, err := store.FetchHeader(&entry.HeaderHash)
		if err != nil {
			return 0, err
		}
		if *byHeight != entry.FilterHash ||
			*byBlock != entry.FilterHash {

			return 0, fmt.Errorf("filter header mismatch at "+
				"height %d", entry.Height)
		}
	}

	if committed > 0 && *durableTip != batch[committed-1].FilterHash {
		return 0, fmt.Errorf("filter tip mismatch at height %d",
			durableHeight)
	}

	return committed, nil
}

// reconcileFilterTip publishes filter headers that are durable but were never
// published, which happens when a write landed and then reported an error whose
// readback also failed.
func (s *Store) reconcileFilterTip() error {
	store := s.host.FilterHeaders()
	_, durableHeight, err := store.ChainTip()
	if err != nil {
		return err
	}

	published := s.host.PublishedFilterHeight()
	if durableHeight < published {
		return fmt.Errorf("filter tip %d below published height %d",
			durableHeight, published)
	}

	blocks := s.host.BlockHeaders()
	for height := published + 1; height <= durableHeight; height++ {
		header, err := blocks.FetchHeaderByHeight(height)
		if err != nil {
			return err
		}
		byHeight, err := store.FetchHeaderByHeight(height)
		if err != nil {
			return err
		}
		hash := header.BlockHash()
		byBlock, err := store.FetchHeader(&hash)
		if err != nil {
			return err
		}
		if *byHeight != *byBlock {
			return fmt.Errorf("filter header mismatch at height %d",
				height)
		}

		s.host.PublishFilterTip(height, hash)
		s.host.BlockConnected(*header, height)
	}

	return nil
}

// filterSnapshot reconciles the filter tip, then reads both tips and picks the
// stop block for the next query.
func (s *Store) filterSnapshot() (FilterSnapshot, error) {
	if err := s.reconcileFilterTip(); err != nil {
		return FilterSnapshot{}, err
	}

	filterTip, filterHeight, err := s.host.FilterHeaders().ChainTip()
	if err != nil {
		return FilterSnapshot{}, err
	}

	blocks := s.host.BlockHeaders()
	header, height, err := blocks.ChainTip()
	if err != nil {
		return FilterSnapshot{}, err
	}

	snapshot := FilterSnapshot{
		FilterTip:    *filterTip,
		FilterHeight: filterHeight,
		BlockHash:    header.BlockHash(),
		BlockHeight:  height,
		StopHash:     header.BlockHash(),
		StopHeight:   height,
	}

	// One cfheaders message can only cover so many headers, so when the
	// filter tip is further behind than that, we stop short of the tip.
	if height > filterHeight &&
		height-filterHeight > wire.MaxCFHeadersPerMsg {

		snapshot.StopHeight = filterHeight + wire.MaxCFHeadersPerMsg
		header, err = blocks.FetchHeaderByHeight(snapshot.StopHeight)
		if err != nil {
			return FilterSnapshot{}, err
		}
		snapshot.StopHash = header.BlockHash()
	}

	return snapshot, nil
}
