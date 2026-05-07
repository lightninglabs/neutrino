package headerfs

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"

	"github.com/lightninglabs/neutrino/sqldb"
	"github.com/lightninglabs/neutrino/sqldb/sqlc"
)

// SQLBlockHeaderStore is a SQL-backed implementation of BlockHeaderStore.
type SQLBlockHeaderStore struct {
	db          sqldb.HeaderTx
	chainParams *chaincfg.Params

	// mtx preserves the linearizable semantics that callers like
	// blockmanager rely on: a ChainTip immediately following a
	// successful WriteHeaders must observe the new tip.
	mtx sync.RWMutex
}

// A compile-time check to ensure SQLBlockHeaderStore satisfies the
// BlockHeaderStore interface.
var _ BlockHeaderStore = (*SQLBlockHeaderStore)(nil)

// NewSQLBlockHeaderStore returns a new SQL-backed block header store. On a
// freshly-migrated database it inserts the genesis row before returning.
func NewSQLBlockHeaderStore(ctx context.Context, db sqldb.HeaderTx,
	netParams *chaincfg.Params) (*SQLBlockHeaderStore, error) {

	if db == nil {
		return nil, errors.New("nil header tx executor")
	}

	store := &SQLBlockHeaderStore{
		db:          db,
		chainParams: netParams,
	}

	if err := store.ensureGenesis(ctx); err != nil {
		return nil, err
	}

	return store, nil
}

// ensureGenesis writes the genesis block header row into block_headers if
// the table is empty. The check + insert run inside a single tx so multiple
// processes racing on first start cannot insert duplicate genesis rows.
func (s *SQLBlockHeaderStore) ensureGenesis(ctx context.Context) error {
	header := s.chainParams.GenesisBlock.Header
	hash := header.BlockHash()

	var raw bytes.Buffer
	if err := header.Serialize(&raw); err != nil {
		return fmt.Errorf("serialize genesis block header: %w", err)
	}

	return s.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.HeaderQueries) error {
			n, err := q.CountBlockHeaders(ctx)
			if err != nil {
				return err
			}
			if n > 0 {
				return nil
			}

			if err := q.InsertBlockHeader(
				ctx, sqlc.InsertBlockHeaderParams{
					Height:    0,
					BlockHash: hash[:],
					RawHeader: raw.Bytes(),
				},
			); err != nil {
				return err
			}

			return q.UpsertChainTip(
				ctx, sqlc.UpsertChainTipParams{
					ChainType: sqldb.ChainTypeBlock,
					TipHash:   hash[:],
					TipHeight: 0,
				},
			)
		}, sqldbv2.NoOpReset)
}

// ChainTip returns the current chain tip's header and height.
//
// NOTE: Part of the BlockHeaderStore interface.
func (s *SQLBlockHeaderStore) ChainTip() (*wire.BlockHeader, uint32, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var (
		header *wire.BlockHeader
		height uint32
		ctx    = context.Background()
	)
	err := s.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.HeaderQueries) error {
			tip, err := q.GetChainTip(ctx, sqldb.ChainTypeBlock)
			if err != nil {
				return err
			}

			raw, err := q.GetBlockHeaderByHeight(ctx, tip.TipHeight)
			if err != nil {
				return err
			}

			h, err := decodeBlockHeader(raw)
			if err != nil {
				return err
			}

			header = h
			height = uint32(tip.TipHeight)
			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to fetch chain tip: %w", err)
	}

	return header, height, nil
}

// FetchHeaderByHeight returns the block header at the requested height.
//
// NOTE: Part of the BlockHeaderStore interface.
func (s *SQLBlockHeaderStore) FetchHeaderByHeight(height uint32) (
	*wire.BlockHeader, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var (
		header *wire.BlockHeader
		ctx    = context.Background()
	)
	err := s.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.HeaderQueries) error {
			raw, err := q.GetBlockHeaderByHeight(
				ctx, int64(height),
			)
			if err != nil {
				return mapHeaderNotFound(err)
			}

			h, err := decodeBlockHeader(raw)
			if err != nil {
				return err
			}
			header = h
			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, err
	}

	return header, nil
}

// FetchHeader returns the block header for a given hash along with its
// height in the chain.
//
// NOTE: Part of the BlockHeaderStore interface.
func (s *SQLBlockHeaderStore) FetchHeader(hash *chainhash.Hash) (
	*wire.BlockHeader, uint32, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var (
		header *wire.BlockHeader
		height uint32
		ctx    = context.Background()
	)
	err := s.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.HeaderQueries) error {
			row, err := q.GetBlockHeaderByHash(ctx, hash[:])
			if err != nil {
				return mapHeaderNotFound(err)
			}

			h, err := decodeBlockHeader(row.RawHeader)
			if err != nil {
				return err
			}
			header = h
			height = uint32(row.Height)
			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, 0, err
	}

	return header, height, nil
}

// HeightFromHash returns the height of the given block hash.
//
// NOTE: Part of the BlockHeaderStore interface.
func (s *SQLBlockHeaderStore) HeightFromHash(hash *chainhash.Hash) (uint32,
	error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var (
		height uint32
		ctx    = context.Background()
	)
	err := s.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.HeaderQueries) error {
			h, err := q.GetBlockHeaderHeightByHash(ctx, hash[:])
			if err != nil {
				return mapHeaderNotFound(err)
			}
			height = uint32(h)
			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return 0, err
	}
	return height, nil
}

// FetchHeaderAncestors returns the numHeaders headers preceding stopHash plus
// the stopHash header itself, for a total of numHeaders+1 headers. It also
// returns the starting height of the returned range.
//
// NOTE: Part of the BlockHeaderStore interface.
func (s *SQLBlockHeaderStore) FetchHeaderAncestors(numHeaders uint32,
	stopHash *chainhash.Hash) ([]wire.BlockHeader, uint32, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var (
		headers     []wire.BlockHeader
		startHeight uint32
		ctx         = context.Background()
	)

	err := s.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.HeaderQueries) error {
			endHeight, err := q.GetBlockHeaderHeightByHash(
				ctx, stopHash[:],
			)
			if err != nil {
				return mapHeaderNotFound(err)
			}
			if uint32(endHeight) < numHeaders {
				return fmt.Errorf("cannot fetch %d ancestors "+
					"of header at height %d",
					numHeaders, endHeight)
			}

			startHeight = uint32(endHeight) - numHeaders

			rows, err := q.GetBlockHeaderRange(
				ctx, sqlc.GetBlockHeaderRangeParams{
					StartHeight: int64(startHeight),
					EndHeight:   endHeight,
				},
			)
			if err != nil {
				return err
			}

			headers = make([]wire.BlockHeader, len(rows))
			for i, r := range rows {
				h, err := decodeBlockHeader(r.RawHeader)
				if err != nil {
					return err
				}
				headers[i] = *h
			}
			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, 0, err
	}
	return headers, startHeight, nil
}

// LatestBlockLocator returns a block locator anchored at the chain tip.
//
// NOTE: Part of the BlockHeaderStore interface.
func (s *SQLBlockHeaderStore) LatestBlockLocator() (blockchain.BlockLocator,
	error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var (
		locator blockchain.BlockLocator
		ctx     = context.Background()
	)

	err := s.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.HeaderQueries) error {
			tip, err := q.GetChainTip(ctx, sqldb.ChainTypeBlock)
			if err != nil {
				return err
			}

			tipHash, err := chainhash.NewHash(tip.TipHash)
			if err != nil {
				return err
			}
			locator = append(locator, tipHash)

			height := uint32(tip.TipHeight)
			if height == 0 {
				return nil
			}

			decrement := uint32(1)
			for height > 0 &&
				len(locator) < wire.MaxBlockLocatorsPerMsg {

				if len(locator) > 10 {
					decrement *= 2
				}
				if decrement > height {
					height = 0
				} else {
					height -= decrement
				}

				raw, err := q.GetBlockHeaderByHeight(
					ctx, int64(height),
				)
				if err != nil {
					return err
				}
				h, err := decodeBlockHeader(raw)
				if err != nil {
					return err
				}
				bh := h.BlockHash()
				locator = append(locator, &bh)
			}
			return nil
		}, sqldbv2.NoOpReset)
	return locator, err
}

// WriteHeaders inserts the given headers atomically and updates the chain
// tip pointer to the highest header in the batch.
//
// NOTE: Part of the BlockHeaderStore interface.
func (s *SQLBlockHeaderStore) WriteHeaders(hdrs ...BlockHeader) error {
	if len(hdrs) == 0 {
		return nil
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	ctx := context.Background()
	return s.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.HeaderQueries) error {
			var lastHash chainhash.Hash
			var lastHeight uint32

			for _, hdr := range hdrs {
				var raw bytes.Buffer
				if err := hdr.Serialize(&raw); err != nil {
					return err
				}
				hash := hdr.BlockHash()

				err := q.InsertBlockHeader(
					ctx, sqlc.InsertBlockHeaderParams{
						Height:    int64(hdr.Height),
						BlockHash: hash[:],
						RawHeader: raw.Bytes(),
					},
				)
				if err != nil {
					return err
				}

				lastHash = hash
				lastHeight = hdr.Height
			}

			return q.UpsertChainTip(
				ctx, sqlc.UpsertChainTipParams{
					ChainType: sqldb.ChainTypeBlock,
					TipHash:   lastHash[:],
					TipHeight: int64(lastHeight),
				},
			)
		}, sqldbv2.NoOpReset)
}

// RollbackBlockHeaders deletes the n most recent headers and resets the
// chain tip to the previous height. Returns a BlockStamp describing the new
// tip.
//
// NOTE: Part of the BlockHeaderStore interface.
func (s *SQLBlockHeaderStore) RollbackBlockHeaders(n uint32) (*BlockStamp,
	error) {

	if n == 0 {
		return &BlockStamp{}, nil
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	var (
		stamp BlockStamp
		ctx   = context.Background()
	)
	err := s.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.HeaderQueries) error {
			tip, err := q.GetChainTip(ctx, sqldb.ChainTypeBlock)
			if err != nil {
				return err
			}
			tipHeight := uint32(tip.TipHeight)
			if n > tipHeight {
				return fmt.Errorf("cannot roll back %d "+
					"headers when chain height is %d", n,
					tipHeight)
			}

			newTipHeight := tipHeight - n

			raw, err := q.GetBlockHeaderByHeight(
				ctx, int64(newTipHeight),
			)
			if err != nil {
				return err
			}
			newTipHeader, err := decodeBlockHeader(raw)
			if err != nil {
				return err
			}
			newTipHash := newTipHeader.BlockHash()

			err = q.DeleteBlockHeadersFromHeight(
				ctx, int64(newTipHeight),
			)
			if err != nil {
				return err
			}

			err = q.UpsertChainTip(
				ctx, sqlc.UpsertChainTipParams{
					ChainType: sqldb.ChainTypeBlock,
					TipHash:   newTipHash[:],
					TipHeight: int64(newTipHeight),
				},
			)
			if err != nil {
				return err
			}

			stamp = BlockStamp{
				Height:    int32(newTipHeight),
				Hash:      newTipHash,
				Timestamp: newTipHeader.Timestamp,
			}
			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, err
	}
	return &stamp, nil
}

// RollbackLastBlock removes a single header from the tip.
//
// NOTE: Part of the BlockHeaderStore interface.
func (s *SQLBlockHeaderStore) RollbackLastBlock() (*BlockStamp, error) {
	return s.RollbackBlockHeaders(1)
}

// SQLFilterHeaderStore is a SQL-backed implementation of FilterHeaderStore.
type SQLFilterHeaderStore struct {
	db          sqldb.HeaderTx
	chainParams *chaincfg.Params
	indexType   HeaderType

	mtx sync.RWMutex
}

// A compile-time check to ensure SQLFilterHeaderStore satisfies the
// FilterHeaderStore interface.
var _ FilterHeaderStore = (*SQLFilterHeaderStore)(nil)

// NewSQLFilterHeaderStore returns a new SQL-backed filter header store. On a
// fresh DB it inserts the genesis filter header row. When headerStateAssertion
// is non-nil and the asserted height is present but does not match, the
// store transactionally purges every filter header row and re-seeds genesis.
func NewSQLFilterHeaderStore(ctx context.Context, db sqldb.HeaderTx,
	filterType HeaderType, netParams *chaincfg.Params,
	headerStateAssertion *FilterHeader) (*SQLFilterHeaderStore, error) {

	if db == nil {
		return nil, errors.New("nil header tx executor")
	}
	if filterType != RegularFilter {
		return nil, fmt.Errorf("unknown filter type: %v", filterType)
	}

	store := &SQLFilterHeaderStore{
		db:          db,
		chainParams: netParams,
		indexType:   filterType,
	}

	if err := store.ensureGenesis(ctx); err != nil {
		return nil, err
	}

	if headerStateAssertion != nil {
		if err := store.maybeReset(ctx, headerStateAssertion); err != nil {
			return nil, err
		}
	}

	return store, nil
}

// ensureGenesis seeds the filter_headers table and chain_tips row when the
// table is empty.
func (s *SQLFilterHeaderStore) ensureGenesis(ctx context.Context) error {
	genesisHash, genesisFilterHash, err := s.computeGenesis()
	if err != nil {
		return err
	}

	return s.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.HeaderQueries) error {
			n, err := q.CountFilterHeaders(ctx)
			if err != nil {
				return err
			}
			if n > 0 {
				return nil
			}

			err = q.InsertFilterHeader(
				ctx, sqlc.InsertFilterHeaderParams{
					Height:     0,
					BlockHash:  genesisHash[:],
					FilterHash: genesisFilterHash[:],
				},
			)
			if err != nil {
				return err
			}

			return q.UpsertChainTip(
				ctx, sqlc.UpsertChainTipParams{
					ChainType: sqldb.ChainTypeFilter,
					TipHash:   genesisFilterHash[:],
					TipHeight: 0,
				},
			)
		}, sqldbv2.NoOpReset)
}

// maybeReset performs the equivalent of the legacy maybeResetHeaderState,
// but transactionally. If the asserted height is missing the assertion is a
// no-op; if the asserted filter hash differs from the stored value, every
// row in filter_headers is purged and genesis is re-seeded — all within one
// SQL transaction.
func (s *SQLFilterHeaderStore) maybeReset(ctx context.Context,
	assertion *FilterHeader) error {

	genesisHash, genesisFilterHash, err := s.computeGenesis()
	if err != nil {
		return err
	}

	return s.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.HeaderQueries) error {
			row, err := q.GetFilterHeaderByHeight(
				ctx, int64(assertion.Height),
			)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				// Asserted height is not yet known. Mirroring
				// the legacy behaviour, we treat this as a
				// no-op rather than an error.
				return nil

			case err != nil:
				return err
			}

			var stored chainhash.Hash
			copy(stored[:], row.FilterHash)
			if stored == assertion.FilterHash {
				return nil
			}

			// Assertion mismatch: purge and re-seed genesis.
			if err := q.PurgeFilterHeaders(ctx); err != nil {
				return err
			}
			if err := q.DeleteChainTip(
				ctx, sqldb.ChainTypeFilter,
			); err != nil {
				return err
			}

			err = q.InsertFilterHeader(
				ctx, sqlc.InsertFilterHeaderParams{
					Height:     0,
					BlockHash:  genesisHash[:],
					FilterHash: genesisFilterHash[:],
				},
			)
			if err != nil {
				return err
			}
			return q.UpsertChainTip(
				ctx, sqlc.UpsertChainTipParams{
					ChainType: sqldb.ChainTypeFilter,
					TipHash:   genesisFilterHash[:],
					TipHeight: 0,
				},
			)
		}, sqldbv2.NoOpReset)
}

// computeGenesis returns the genesis block hash and the genesis filter
// header hash for the configured chain.
func (s *SQLFilterHeaderStore) computeGenesis() (chainhash.Hash, chainhash.Hash,
	error) {

	var zero chainhash.Hash

	basicFilter, err := builder.BuildBasicFilter(
		s.chainParams.GenesisBlock, nil,
	)
	if err != nil {
		return zero, zero, err
	}

	genesisFilterHash, err := builder.MakeHeaderForFilter(
		basicFilter, s.chainParams.GenesisBlock.Header.PrevBlock,
	)
	if err != nil {
		return zero, zero, err
	}

	return *s.chainParams.GenesisHash, genesisFilterHash, nil
}

// ChainTip returns the tip filter header hash and height.
//
// NOTE: Part of the FilterHeaderStore interface.
func (s *SQLFilterHeaderStore) ChainTip() (*chainhash.Hash, uint32, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var (
		hash   chainhash.Hash
		height uint32
		ctx    = context.Background()
	)
	err := s.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.HeaderQueries) error {
			tip, err := q.GetChainTip(ctx, sqldb.ChainTypeFilter)
			if err != nil {
				return err
			}
			h, err := chainhash.NewHash(tip.TipHash)
			if err != nil {
				return err
			}
			hash = *h
			height = uint32(tip.TipHeight)
			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to fetch chain tip: %w", err)
	}

	return &hash, height, nil
}

// FetchHeader returns the filter header for the given block hash.
//
// NOTE: Part of the FilterHeaderStore interface.
func (s *SQLFilterHeaderStore) FetchHeader(hash *chainhash.Hash) (
	*chainhash.Hash, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var (
		out chainhash.Hash
		ctx = context.Background()
	)
	err := s.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.HeaderQueries) error {
			row, err := q.GetFilterHeaderByBlockHash(ctx, hash[:])
			if err != nil {
				return mapHeaderNotFound(err)
			}
			h, err := chainhash.NewHash(row.FilterHash)
			if err != nil {
				return err
			}
			out = *h
			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// FetchHeaderByHeight returns the filter header at a specific block height.
//
// NOTE: Part of the FilterHeaderStore interface.
func (s *SQLFilterHeaderStore) FetchHeaderByHeight(height uint32) (
	*chainhash.Hash, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var (
		out chainhash.Hash
		ctx = context.Background()
	)
	err := s.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.HeaderQueries) error {
			row, err := q.GetFilterHeaderByHeight(
				ctx, int64(height),
			)
			if err != nil {
				return mapHeaderNotFound(err)
			}
			h, err := chainhash.NewHash(row.FilterHash)
			if err != nil {
				return err
			}
			out = *h
			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// FetchHeaderAncestors returns numHeaders ancestors of stopHash plus stopHash
// itself, for a total of numHeaders+1 entries, along with the starting
// height.
//
// NOTE: Part of the FilterHeaderStore interface.
func (s *SQLFilterHeaderStore) FetchHeaderAncestors(numHeaders uint32,
	stopHash *chainhash.Hash) ([]chainhash.Hash, uint32, error) {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var (
		out         []chainhash.Hash
		startHeight uint32
		ctx         = context.Background()
	)
	err := s.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.HeaderQueries) error {
			row, err := q.GetFilterHeaderByBlockHash(
				ctx, stopHash[:],
			)
			if err != nil {
				return mapHeaderNotFound(err)
			}
			endHeight := uint32(row.Height)
			if endHeight < numHeaders {
				return fmt.Errorf("cannot fetch %d "+
					"ancestors of filter header at "+
					"height %d", numHeaders, endHeight)
			}
			startHeight = endHeight - numHeaders

			rows, err := q.GetFilterHeaderRange(
				ctx, sqlc.GetFilterHeaderRangeParams{
					StartHeight: int64(startHeight),
					EndHeight:   int64(endHeight),
				},
			)
			if err != nil {
				return err
			}

			out = make([]chainhash.Hash, len(rows))
			for i, r := range rows {
				h, err := chainhash.NewHash(r.FilterHash)
				if err != nil {
					return err
				}
				out[i] = *h
			}
			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, 0, err
	}
	return out, startHeight, nil
}

// WriteHeaders inserts a batch of filter headers atomically and updates the
// chain tip pointer.
//
// NOTE: Part of the FilterHeaderStore interface.
func (s *SQLFilterHeaderStore) WriteHeaders(hdrs ...FilterHeader) error {
	if len(hdrs) == 0 {
		return nil
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	ctx := context.Background()
	return s.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.HeaderQueries) error {
			var (
				tipHash   chainhash.Hash
				tipHeight uint32
			)
			for _, hdr := range hdrs {
				err := q.InsertFilterHeader(
					ctx, sqlc.InsertFilterHeaderParams{
						Height:     int64(hdr.Height),
						BlockHash:  hdr.HeaderHash[:],
						FilterHash: hdr.FilterHash[:],
					},
				)
				if err != nil {
					return err
				}
				tipHash = hdr.FilterHash
				tipHeight = hdr.Height
			}

			return q.UpsertChainTip(
				ctx, sqlc.UpsertChainTipParams{
					ChainType: sqldb.ChainTypeFilter,
					TipHash:   tipHash[:],
					TipHeight: int64(tipHeight),
				},
			)
		}, sqldbv2.NoOpReset)
}

// RollbackLastBlock removes the most recent filter header and updates the
// chain tip to point at newTip.
//
// NOTE: Part of the FilterHeaderStore interface.
func (s *SQLFilterHeaderStore) RollbackLastBlock(
	newTip *chainhash.Hash) (*BlockStamp, error) {

	s.mtx.Lock()
	defer s.mtx.Unlock()

	var (
		stamp BlockStamp
		ctx   = context.Background()
	)
	err := s.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.HeaderQueries) error {
			tip, err := q.GetChainTip(ctx, sqldb.ChainTypeFilter)
			if err != nil {
				return err
			}
			if tip.TipHeight == 0 {
				return errors.New("cannot roll back filter " +
					"header chain past genesis")
			}

			newHeight := tip.TipHeight - 1

			row, err := q.GetFilterHeaderByHeight(ctx, newHeight)
			if err != nil {
				return err
			}
			prevHash, err := chainhash.NewHash(row.FilterHash)
			if err != nil {
				return err
			}

			err = q.DeleteFilterHeadersFromHeight(ctx, newHeight)
			if err != nil {
				return err
			}

			tipHash := prevHash[:]
			if newTip != nil {
				tipHash = newTip[:]
			}

			err = q.UpsertChainTip(
				ctx, sqlc.UpsertChainTipParams{
					ChainType: sqldb.ChainTypeFilter,
					TipHash:   tipHash,
					TipHeight: newHeight,
				},
			)
			if err != nil {
				return err
			}

			stamp = BlockStamp{
				Height: int32(newHeight),
				Hash:   *prevHash,
			}
			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, err
	}
	return &stamp, nil
}

// decodeBlockHeader deserializes the raw 80-byte representation of a block
// header. It returns a heap-allocated *wire.BlockHeader to match the existing
// interface return shape.
func decodeBlockHeader(raw []byte) (*wire.BlockHeader, error) {
	var hdr wire.BlockHeader
	if err := hdr.Deserialize(bytes.NewReader(raw)); err != nil {
		return nil, err
	}
	return &hdr, nil
}

// mapHeaderNotFound translates sql.ErrNoRows into the headerfs-specific
// ErrHeaderNotFound type so callers can use the existing type assertion
// pattern (`if _, ok := err.(*ErrHeaderNotFound); ok { ... }`).
func mapHeaderNotFound(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return &ErrHeaderNotFound{err}
	}
	return err
}
