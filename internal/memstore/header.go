package memstore

import (
	"fmt"
	"io"
	"sync"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// BlockHeaderStore is an in-memory implementation of
// headerfs.BlockHeaderStore.
type BlockHeaderStore struct {
	mu      sync.RWMutex
	headers []wire.BlockHeader
	heights map[chainhash.Hash]uint32
}

var _ headerfs.BlockHeaderStore = (*BlockHeaderStore)(nil)

// NewBlockHeaderStore returns a block header store initialized with the
// target chain's genesis header.
func NewBlockHeaderStore(params *chaincfg.Params) *BlockHeaderStore {
	genesis := params.GenesisBlock.Header

	return &BlockHeaderStore{
		headers: []wire.BlockHeader{genesis},
		heights: map[chainhash.Hash]uint32{
			genesis.BlockHash(): 0,
		},
	}
}

// ChainTip returns the best known block header and height.
func (s *BlockHeaderStore) ChainTip() (*wire.BlockHeader, uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	height := uint32(len(s.headers) - 1)
	header := s.headers[height]
	return &header, height, nil
}

// LatestBlockLocator returns a locator rooted at the current chain tip.
func (s *BlockHeaderStore) LatestBlockLocator() (blockchain.BlockLocator,
	error) {

	s.mu.RLock()
	defer s.mu.RUnlock()

	height := uint32(len(s.headers) - 1)
	locator := make(blockchain.BlockLocator, 0, 12)
	decrement := uint32(1)

	for {
		hash := s.headers[height].BlockHash()
		locator = append(locator, &hash)
		if height == 0 || len(locator) == wire.MaxBlockLocatorsPerMsg {
			return locator, nil
		}

		if len(locator) > 10 {
			decrement *= 2
		}
		if decrement > height {
			height = 0
		} else {
			height -= decrement
		}
	}
}

// FetchHeaderByHeight returns the block header at the requested height.
func (s *BlockHeaderStore) FetchHeaderByHeight(
	height uint32) (*wire.BlockHeader, error) {

	s.mu.RLock()
	defer s.mu.RUnlock()

	if uint64(height) >= uint64(len(s.headers)) {
		return nil, headerfs.NewErrHeaderNotFound(io.EOF)
	}

	header := s.headers[height]
	return &header, nil
}

// FetchHeaderAncestors returns the requested inclusive header range ending at
// stopHash.
func (s *BlockHeaderStore) FetchHeaderAncestors(numHeaders uint32,
	stopHash *chainhash.Hash) ([]wire.BlockHeader, uint32, error) {

	s.mu.RLock()
	defer s.mu.RUnlock()

	endHeight, ok := s.heights[*stopHash]
	if !ok {
		return nil, 0, headerfs.ErrHashNotFound
	}
	if numHeaders > endHeight {
		return nil, 0, headerfs.NewErrHeaderNotFound(io.EOF)
	}
	startHeight := endHeight - numHeaders

	headers := append(
		[]wire.BlockHeader(nil), s.headers[startHeight:endHeight+1]...,
	)
	return headers, startHeight, nil
}

// HeightFromHash returns the height of a block hash.
func (s *BlockHeaderStore) HeightFromHash(
	hash *chainhash.Hash) (uint32, error) {

	s.mu.RLock()
	defer s.mu.RUnlock()

	height, ok := s.heights[*hash]
	if !ok {
		return 0, headerfs.ErrHashNotFound
	}

	return height, nil
}

// FetchHeader returns a block header and its height by block hash.
func (s *BlockHeaderStore) FetchHeader(
	hash *chainhash.Hash) (*wire.BlockHeader, uint32, error) {

	s.mu.RLock()
	defer s.mu.RUnlock()

	height, ok := s.heights[*hash]
	if !ok {
		return nil, 0, headerfs.ErrHashNotFound
	}

	header := s.headers[height]
	return &header, height, nil
}

// WriteHeaders appends a contiguous batch of block headers atomically.
func (s *BlockHeaderStore) WriteHeaders(hdrs ...headerfs.BlockHeader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nextHeight := uint32(len(s.headers))
	for i := range hdrs {
		if hdrs[i].BlockHeader == nil {
			return fmt.Errorf("block header is required")
		}
		if hdrs[i].Height != nextHeight+uint32(i) {
			return fmt.Errorf("non-contiguous block header height %d, "+
				"expected %d", hdrs[i].Height,
				nextHeight+uint32(i))
		}
	}

	newHeaders := make([]wire.BlockHeader, len(hdrs))
	for i := range hdrs {
		newHeaders[i] = *hdrs[i].BlockHeader
	}

	for i := range newHeaders {
		height := nextHeight + uint32(i)
		s.headers = append(s.headers, newHeaders[i])
		s.heights[newHeaders[i].BlockHash()] = height
	}

	return nil
}

// RollbackBlockHeaders removes headers from the tip without removing genesis.
func (s *BlockHeaderStore) RollbackBlockHeaders(
	numHeaders uint32) (*headerfs.BlockStamp, error) {

	if numHeaders == 0 {
		return &headerfs.BlockStamp{}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tipHeight := uint32(len(s.headers) - 1)
	if numHeaders > tipHeight {
		return nil, fmt.Errorf("cannot roll back %d headers when chain "+
			"height is %d", numHeaders, tipHeight)
	}

	newHeight := tipHeight - numHeaders
	for height := newHeight + 1; height <= tipHeight; height++ {
		delete(s.heights, s.headers[height].BlockHash())
	}
	s.headers = s.headers[:newHeight+1]

	newTip := s.headers[newHeight]
	return &headerfs.BlockStamp{
		Height:    int32(newHeight),
		Hash:      newTip.BlockHash(),
		Timestamp: newTip.Timestamp,
	}, nil
}

// RollbackLastBlock removes the current block header tip.
func (s *BlockHeaderStore) RollbackLastBlock() (*headerfs.BlockStamp, error) {
	return s.RollbackBlockHeaders(1)
}

// FilterHeaderStore is an in-memory implementation of
// headerfs.FilterHeaderStore.
type FilterHeaderStore struct {
	mu          sync.RWMutex
	headers     []chainhash.Hash
	blockHashes []chainhash.Hash
	heights     map[chainhash.Hash]uint32
	blockStore  headerfs.BlockHeaderStore
}

var _ headerfs.FilterHeaderStore = (*FilterHeaderStore)(nil)

// NewFilterHeaderStore returns a filter header store initialized with the
// target chain's genesis filter header.
func NewFilterHeaderStore(
	params *chaincfg.Params) (*FilterHeaderStore, error) {
	return newFilterHeaderStore(params, nil)
}

func newFilterHeaderStore(params *chaincfg.Params,
	blockStore headerfs.BlockHeaderStore) (*FilterHeaderStore, error) {

	genesisFilter, err := builder.BuildBasicFilter(
		params.GenesisBlock, nil,
	)
	if err != nil {
		return nil, err
	}
	genesisHeader, err := builder.MakeHeaderForFilter(
		genesisFilter, params.GenesisBlock.Header.PrevBlock,
	)
	if err != nil {
		return nil, err
	}

	return &FilterHeaderStore{
		headers:     []chainhash.Hash{genesisHeader},
		blockHashes: []chainhash.Hash{*params.GenesisHash},
		heights: map[chainhash.Hash]uint32{
			*params.GenesisHash: 0,
		},
		blockStore: blockStore,
	}, nil
}

// ChainTip returns the current filter header and height.
func (s *FilterHeaderStore) ChainTip() (*chainhash.Hash, uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	height := uint32(len(s.headers) - 1)
	header := s.headers[height]
	return &header, height, nil
}

// FetchHeader returns the filter header associated with a block hash.
func (s *FilterHeaderStore) FetchHeader(
	blockHash *chainhash.Hash) (*chainhash.Hash, error) {

	height, err := s.heightFromBlockHash(blockHash)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if uint64(height) >= uint64(len(s.headers)) {
		return nil, headerfs.NewErrHeaderNotFound(io.EOF)
	}

	header := s.headers[height]
	return &header, nil
}

// FetchHeaderAncestors returns the requested inclusive filter header range
// ending at stopHash.
func (s *FilterHeaderStore) FetchHeaderAncestors(numHeaders uint32,
	stopHash *chainhash.Hash) ([]chainhash.Hash, uint32, error) {

	endHeight, err := s.heightFromBlockHash(stopHash)
	if err != nil {
		return nil, 0, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if numHeaders > endHeight ||
		uint64(endHeight) >= uint64(len(s.headers)) {

		return nil, 0, headerfs.NewErrHeaderNotFound(io.EOF)
	}
	startHeight := endHeight - numHeaders

	headers := append(
		[]chainhash.Hash(nil), s.headers[startHeight:endHeight+1]...,
	)
	return headers, startHeight, nil
}

// FetchHeaderByHeight returns the filter header at the requested height.
func (s *FilterHeaderStore) FetchHeaderByHeight(
	height uint32) (*chainhash.Hash, error) {

	s.mu.RLock()
	defer s.mu.RUnlock()

	if uint64(height) >= uint64(len(s.headers)) {
		return nil, headerfs.NewErrHeaderNotFound(io.EOF)
	}

	header := s.headers[height]
	return &header, nil
}

// WriteHeaders appends a contiguous batch of filter headers atomically.
func (s *FilterHeaderStore) WriteHeaders(
	hdrs ...headerfs.FilterHeader) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(hdrs) == 0 {
		return nil
	}

	nextHeight := uint32(len(s.headers))
	lastHeader := hdrs[len(hdrs)-1]
	expectedTipHeight := nextHeight + uint32(len(hdrs)) - 1
	if lastHeader.Height != expectedTipHeight {
		return fmt.Errorf("filter header tip height %d, expected %d",
			lastHeader.Height, expectedTipHeight)
	}

	if s.blockStore != nil {
		tipHeight, err := s.blockStore.HeightFromHash(
			&lastHeader.HeaderHash,
		)
		if err != nil {
			return fmt.Errorf("filter header tip block hash: %w", err)
		}
		if tipHeight != expectedTipHeight {
			return fmt.Errorf("filter header tip block height %d, "+
				"expected %d", tipHeight, expectedTipHeight)
		}
	} else if lastHeader.HeaderHash == (chainhash.Hash{}) {
		return fmt.Errorf("filter header tip block hash is required")
	}

	blockHashes := make([]chainhash.Hash, len(hdrs))
	for i := range hdrs {
		height := nextHeight + uint32(i)
		blockHashes[i] = hdrs[i].HeaderHash
		if s.blockStore != nil {
			blockHeader, err := s.blockStore.FetchHeaderByHeight(height)
			if err != nil {
				return fmt.Errorf("filter header block at height %d: %w",
					height, err)
			}
			blockHashes[i] = blockHeader.BlockHash()
		}
	}

	for i := range hdrs {
		height := nextHeight + uint32(i)
		s.headers = append(s.headers, hdrs[i].FilterHash)
		s.blockHashes = append(s.blockHashes, blockHashes[i])
		if blockHashes[i] != (chainhash.Hash{}) {
			s.heights[blockHashes[i]] = height
		}
	}

	return nil
}

// RollbackLastBlock removes the current filter header tip.
func (s *FilterHeaderStore) RollbackLastBlock(
	newTip *chainhash.Hash) (*headerfs.BlockStamp, error) {

	newTipHeight, err := s.heightFromBlockHash(newTip)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.headers) == 1 {
		return nil, headerfs.NewErrHeaderNotFound(io.EOF)
	}

	newHeight := uint32(len(s.headers) - 2)
	if newTipHeight != newHeight {
		return nil, fmt.Errorf("filter header rollback tip mismatch")
	}

	oldHeight := newHeight + 1
	delete(s.heights, s.blockHashes[oldHeight])
	s.headers = s.headers[:oldHeight]
	s.blockHashes = s.blockHashes[:oldHeight]

	return &headerfs.BlockStamp{
		Height: int32(newHeight),
		Hash:   s.headers[newHeight],
	}, nil
}

func (s *FilterHeaderStore) heightFromBlockHash(
	blockHash *chainhash.Hash) (uint32, error) {

	if blockHash == nil {
		return 0, headerfs.ErrHashNotFound
	}
	if s.blockStore != nil {
		return s.blockStore.HeightFromHash(blockHash)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	height, ok := s.heights[*blockHash]
	if !ok {
		return 0, headerfs.ErrHashNotFound
	}

	return height, nil
}
