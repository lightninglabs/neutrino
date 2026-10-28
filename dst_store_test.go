package neutrino

import (
	"errors"
	"fmt"
	"sync"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// errDSTWrite is returned by a DST header store whose write is faulted.
var errDSTWrite = errors.New("simulated header store unavailable")

// dstStoreMode selects how the next DST header store write behaves.
type dstStoreMode uint8

const (
	// dstStoreOK writes the batch and reports success.
	dstStoreOK dstStoreMode = iota

	// dstStoreDown writes nothing and reports an error.
	dstStoreDown

	// dstStoreAckLost writes the whole batch durably and then reports an
	// error, like a SQL commit whose acknowledgement was lost.
	dstStoreAckLost

	// dstStoreTorn writes the first half of the batch durably and then
	// reports an error. Neither production store does this on its own,
	// but the BlockHeaderStore contract does not rule it out, and a
	// legacy rollback whose truncate fails comes close.
	dstStoreTorn
)

// String returns a short name for the mode.
func (m dstStoreMode) String() string {
	switch m {
	case dstStoreOK:
		return "ok"
	case dstStoreDown:
		return "down"
	case dstStoreAckLost:
		return "ack_lost"
	case dstStoreTorn:
		return "torn"
	default:
		return fmt.Sprintf("mode(%d)", uint8(m))
	}
}

// memBlockHeaderStore is an in-memory headerfs.BlockHeaderStore for DST. It
// enforces SQL-like append semantics: a write must extend the tip by exactly
// the next height, so a retry that rewrites durable heights fails the same
// way a primary-key conflict does. That keeps an ambiguous commit visible as
// a stall rather than silently tolerated.
type memBlockHeaderStore struct {
	mtx     sync.RWMutex
	headers []wire.BlockHeader
	heights map[chainhash.Hash]uint32
}

// A compile-time assertion that memBlockHeaderStore is a header store.
var _ headerfs.BlockHeaderStore = (*memBlockHeaderStore)(nil)

// newMemBlockHeaderStore returns a store holding only the genesis header.
func newMemBlockHeaderStore(params *chaincfg.Params) *memBlockHeaderStore {
	genesis := params.GenesisBlock.Header
	return &memBlockHeaderStore{
		headers: []wire.BlockHeader{genesis},
		heights: map[chainhash.Hash]uint32{
			genesis.BlockHash(): 0,
		},
	}
}

// ChainTip returns the highest stored header and its height.
func (s *memBlockHeaderStore) ChainTip() (*wire.BlockHeader, uint32, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	tip := s.headers[len(s.headers)-1]
	return &tip, uint32(len(s.headers) - 1), nil
}

// LatestBlockLocator returns an exponentially spaced locator from the tip.
func (s *memBlockHeaderStore) LatestBlockLocator() (blockchain.BlockLocator,
	error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var locator blockchain.BlockLocator
	step := 1
	for height := len(s.headers) - 1; height >= 0; height -= step {
		hash := s.headers[height].BlockHash()
		locator = append(locator, &hash)
		if len(locator) > 10 {
			step *= 2
		}
	}
	if len(locator) == 0 ||
		*locator[len(locator)-1] != s.headers[0].BlockHash() {

		genesis := s.headers[0].BlockHash()
		locator = append(locator, &genesis)
	}

	return locator, nil
}

// FetchHeaderByHeight returns the header at height.
func (s *memBlockHeaderStore) FetchHeaderByHeight(
	height uint32) (*wire.BlockHeader, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if int(height) >= len(s.headers) {
		return nil, fmt.Errorf("height %d beyond tip %d", height,
			len(s.headers)-1)
	}
	header := s.headers[height]
	return &header, nil
}

// FetchHeaderAncestors returns numHeaders ancestors of stop plus stop itself.
func (s *memBlockHeaderStore) FetchHeaderAncestors(numHeaders uint32,
	stop *chainhash.Hash) ([]wire.BlockHeader, uint32, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	end, ok := s.heights[*stop]
	if !ok {
		return nil, 0, fmt.Errorf("unknown stop hash %s", stop)
	}
	if numHeaders > end {
		return nil, 0, fmt.Errorf("ancestors below genesis")
	}
	start := end - numHeaders
	headers := make([]wire.BlockHeader, numHeaders+1)
	copy(headers, s.headers[start:end+1])

	return headers, start, nil
}

// HeightFromHash returns the height of a stored header.
func (s *memBlockHeaderStore) HeightFromHash(
	hash *chainhash.Hash) (uint32, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	height, ok := s.heights[*hash]
	if !ok {
		return 0, fmt.Errorf("unknown hash %s", hash)
	}
	return height, nil
}

// FetchHeader returns a stored header and its height.
func (s *memBlockHeaderStore) FetchHeader(
	hash *chainhash.Hash) (*wire.BlockHeader, uint32, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	height, ok := s.heights[*hash]
	if !ok {
		return nil, 0, fmt.Errorf("unknown hash %s", hash)
	}
	header := s.headers[height]
	return &header, height, nil
}

// WriteHeaders appends a contiguous batch at the next height.
func (s *memBlockHeaderStore) WriteHeaders(
	headers ...headerfs.BlockHeader) error {

	s.mtx.Lock()
	defer s.mtx.Unlock()

	next := uint32(len(s.headers))
	for i, header := range headers {
		if header.Height != next+uint32(i) {
			return fmt.Errorf("non-contiguous header write "+
				"height=%d want=%d", header.Height,
				next+uint32(i))
		}
	}
	for _, header := range headers {
		s.headers = append(s.headers, *header.BlockHeader)
		s.heights[header.BlockHash()] = header.Height
	}

	return nil
}

// RollbackBlockHeaders removes numHeaders headers from the tip.
func (s *memBlockHeaderStore) RollbackBlockHeaders(
	numHeaders uint32) (*headerfs.BlockStamp, error) {

	s.mtx.Lock()
	defer s.mtx.Unlock()

	if int(numHeaders) >= len(s.headers) {
		return nil, fmt.Errorf("rollback reaches genesis")
	}
	for i := 0; i < int(numHeaders); i++ {
		last := s.headers[len(s.headers)-1]
		delete(s.heights, last.BlockHash())
		s.headers = s.headers[:len(s.headers)-1]
	}
	tip := s.headers[len(s.headers)-1]

	return &headerfs.BlockStamp{
		Height:    int32(len(s.headers) - 1),
		Hash:      tip.BlockHash(),
		Timestamp: tip.Timestamp,
	}, nil
}

// RollbackLastBlock removes the tip header.
func (s *memBlockHeaderStore) RollbackLastBlock() (*headerfs.BlockStamp,
	error) {

	return s.RollbackBlockHeaders(1)
}

// memFilterHeaderStore is an in-memory headerfs.FilterHeaderStore for DST.
type memFilterHeaderStore struct {
	mtx     sync.RWMutex
	headers []headerfs.FilterHeader
}

// A compile-time assertion that memFilterHeaderStore is a filter store.
var _ headerfs.FilterHeaderStore = (*memFilterHeaderStore)(nil)

// newMemFilterHeaderStore returns a store holding the genesis filter header,
// computed the same way the legacy store does.
func newMemFilterHeaderStore(
	params *chaincfg.Params) (*memFilterHeaderStore, error) {

	filter, err := builder.BuildBasicFilter(params.GenesisBlock, nil)
	if err != nil {
		return nil, err
	}
	filterHash, err := builder.MakeHeaderForFilter(
		filter, params.GenesisBlock.Header.PrevBlock,
	)
	if err != nil {
		return nil, err
	}

	return &memFilterHeaderStore{
		headers: []headerfs.FilterHeader{{
			HeaderHash: *params.GenesisHash,
			FilterHash: filterHash,
		}},
	}, nil
}

// ChainTip returns the highest filter header and its height.
func (s *memFilterHeaderStore) ChainTip() (*chainhash.Hash, uint32, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	tip := s.headers[len(s.headers)-1].FilterHash
	return &tip, uint32(len(s.headers) - 1), nil
}

// FetchHeader returns the filter header for a block hash.
func (s *memFilterHeaderStore) FetchHeader(
	hash *chainhash.Hash) (*chainhash.Hash, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	for _, header := range s.headers {
		if header.HeaderHash == *hash {
			filterHash := header.FilterHash
			return &filterHash, nil
		}
	}
	return nil, fmt.Errorf("unknown block hash %s", hash)
}

// FetchHeaderAncestors returns numHeaders ancestors of stop plus stop itself.
func (s *memFilterHeaderStore) FetchHeaderAncestors(numHeaders uint32,
	stop *chainhash.Hash) ([]chainhash.Hash, uint32, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	for end, header := range s.headers {
		if header.HeaderHash != *stop {
			continue
		}
		if int(numHeaders) > end {
			return nil, 0, fmt.Errorf("ancestors below genesis")
		}
		start := end - int(numHeaders)
		hashes := make([]chainhash.Hash, 0, numHeaders+1)
		for _, ancestor := range s.headers[start : end+1] {
			hashes = append(hashes, ancestor.FilterHash)
		}
		return hashes, uint32(start), nil
	}
	return nil, 0, fmt.Errorf("unknown stop hash %s", stop)
}

// FetchHeaderByHeight returns the filter header at height.
func (s *memFilterHeaderStore) FetchHeaderByHeight(
	height uint32) (*chainhash.Hash, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if int(height) >= len(s.headers) {
		return nil, fmt.Errorf("height %d beyond tip %d", height,
			len(s.headers)-1)
	}
	filterHash := s.headers[height].FilterHash
	return &filterHash, nil
}

// WriteHeaders appends a contiguous batch at the next height.
func (s *memFilterHeaderStore) WriteHeaders(
	headers ...headerfs.FilterHeader) error {

	s.mtx.Lock()
	defer s.mtx.Unlock()

	next := uint32(len(s.headers))
	for i, header := range headers {
		if header.Height != next+uint32(i) {
			return fmt.Errorf("non-contiguous filter write "+
				"height=%d want=%d", header.Height,
				next+uint32(i))
		}
	}
	s.headers = append(s.headers, headers...)

	return nil
}

// RollbackLastBlock removes the tip filter header.
func (s *memFilterHeaderStore) RollbackLastBlock(
	newTip *chainhash.Hash) (*headerfs.BlockStamp, error) {

	s.mtx.Lock()
	defer s.mtx.Unlock()

	if len(s.headers) <= 1 {
		return nil, fmt.Errorf("rollback reaches genesis")
	}
	s.headers = s.headers[:len(s.headers)-1]

	return &headerfs.BlockStamp{
		Height: int32(len(s.headers) - 1),
		Hash:   *newTip,
	}, nil
}

// dstHeaderStore wraps a block header store with seeded write faults and a
// log of durable writes for the DST oracle.
type dstHeaderStore struct {
	headerfs.BlockHeaderStore

	// mode is the behaviour of the next write. Ambiguous modes reset to
	// dstStoreOK after one write; dstStoreDown persists until cleared.
	mode dstStoreMode

	// writes records the height of every header durably written.
	writes []uint32

	// ambiguous counts writes that landed but reported an error.
	ambiguous int
}

// WriteHeaders applies the current fault mode to one batch.
func (d *dstHeaderStore) WriteHeaders(headers ...headerfs.BlockHeader) error {
	durable := headers
	switch d.mode {
	case dstStoreDown:
		return errDSTWrite

	case dstStoreTorn:
		durable = headers[:len(headers)/2]
	}

	if len(durable) > 0 {
		if err := d.BlockHeaderStore.WriteHeaders(
			durable...,
		); err != nil {
			return err
		}
		for _, header := range durable {
			d.writes = append(d.writes, header.Height)
		}
	}

	if d.mode == dstStoreOK {
		return nil
	}

	// Ambiguous faults affect a single write, like one lost ack.
	d.mode = dstStoreOK
	d.ambiguous++
	return errDSTWrite
}
