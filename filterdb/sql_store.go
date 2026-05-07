package filterdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/btcsuite/btcd/btcutil/gcs"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"

	"github.com/lightninglabs/neutrino/sqldb"
	"github.com/lightninglabs/neutrino/sqldb/sqlc"
)

// SQLFilterStore is a SQL-backed implementation of the FilterDatabase
// interface.
//
// The store is keyed exclusively by block hash because the only filter type
// neutrino currently uses is RegularFilter; the schema reflects this and the
// generated queries operate against the regular_filters table directly.
type SQLFilterStore struct {
	db          sqldb.FilterTx
	chainParams chaincfg.Params

	mtx sync.RWMutex
}

// A compile-time constraint to ensure SQLFilterStore satisfies the
// FilterDatabase interface.
var _ FilterDatabase = (*SQLFilterStore)(nil)

// NewSQLFilterStore returns a new SQLFilterStore. The caller is expected to
// have already migrated the schema via sqldb.NewBackend; this constructor
// only ensures the genesis filter exists, mirroring the legacy walletdb
// constructor.
func NewSQLFilterStore(ctx context.Context, db sqldb.FilterTx,
	params chaincfg.Params) (*SQLFilterStore, error) {

	if db == nil {
		return nil, errors.New("nil filter tx executor")
	}

	store := &SQLFilterStore{
		db:          db,
		chainParams: params,
	}

	if err := store.ensureGenesis(ctx); err != nil {
		return nil, err
	}

	return store, nil
}

// ensureGenesis writes the genesis basic filter row if the regular_filters
// table is empty for the genesis block. It is idempotent and safe to call on
// every start.
func (f *SQLFilterStore) ensureGenesis(ctx context.Context) error {
	genesisBlock := f.chainParams.GenesisBlock
	genesisHash := f.chainParams.GenesisHash

	basicFilter, err := builder.BuildBasicFilter(genesisBlock, nil)
	if err != nil {
		return fmt.Errorf("build genesis filter: %w", err)
	}

	filterBytes, err := basicFilter.NBytes()
	if err != nil {
		return fmt.Errorf("encode genesis filter: %w", err)
	}

	return f.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.FilterQueries) error {
			_, err := q.GetFilter(ctx, genesisHash[:])
			switch {
			case errors.Is(err, sql.ErrNoRows):
				return q.PutFilter(ctx, sqlc.PutFilterParams{
					BlockHash:   genesisHash[:],
					FilterBytes: filterBytes,
				})

			case err != nil:
				return err

			default:
				// Genesis row already present.
				return nil
			}
		}, sqldbv2.NoOpReset)
}

// PutFilters stores the supplied filters atomically. Filters with an unknown
// type cause the entire batch to fail with an error matching the legacy
// walletdb implementation.
//
// NOTE: This method is part of the FilterDatabase interface.
func (f *SQLFilterStore) PutFilters(filterList ...*FilterData) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	if len(filterList) == 0 {
		return nil
	}

	// Validate types up front so we don't begin a tx for a definitively
	// invalid call.
	for _, fd := range filterList {
		if fd.Type != RegularFilter {
			return fmt.Errorf("unknown filter type: %v", fd.Type)
		}
	}

	ctx := context.Background()
	return f.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.FilterQueries) error {
			for _, fd := range filterList {
				bytes, err := encodeFilter(fd.Filter)
				if err != nil {
					return err
				}

				err = q.PutFilter(ctx, sqlc.PutFilterParams{
					BlockHash:   fd.BlockHash[:],
					FilterBytes: bytes,
				})
				if err != nil {
					return err
				}

				log.Tracef("Wrote filter for block %s, "+
					"type %d", fd.BlockHash, fd.Type)
			}

			return nil
		}, sqldbv2.NoOpReset)
}

// FetchFilter loads a filter from persistent storage. It distinguishes
// between three cases, matching the legacy walletdb semantics:
//   - row absent: return ErrFilterNotFound.
//   - row present, value zero-length: return (nil, nil) — the "deleted
//     filter" sentinel.
//   - row present, value populated: deserialize via gcs.FromNBytes.
//
// NOTE: This method is part of the FilterDatabase interface.
func (f *SQLFilterStore) FetchFilter(blockHash *chainhash.Hash,
	filterType FilterType) (*gcs.Filter, error) {

	if filterType != RegularFilter {
		return nil, fmt.Errorf("unknown filter type")
	}

	f.mtx.RLock()
	defer f.mtx.RUnlock()

	var (
		filter *gcs.Filter
		ctx    = context.Background()
	)
	err := f.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.FilterQueries) error {
			bytes, err := q.GetFilter(ctx, blockHash[:])
			switch {
			case errors.Is(err, sql.ErrNoRows):
				return ErrFilterNotFound

			case err != nil:
				return err
			}

			if len(bytes) == 0 {
				// "Deleted filter" sentinel.
				return nil
			}

			f, err := gcs.FromNBytes(
				builder.DefaultP, builder.DefaultM, bytes,
			)
			if err != nil {
				return err
			}
			filter = f
			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, err
	}

	return filter, nil
}

// PurgeFilters drops every filter of the given type. The legacy walletdb
// implementation deletes and recreates the per-type bucket without
// re-seeding the genesis filter; we match that by not re-inserting genesis
// here.
//
// NOTE: This method is part of the FilterDatabase interface.
func (f *SQLFilterStore) PurgeFilters(fType FilterType) error {
	if fType != RegularFilter {
		return fmt.Errorf("unknown filter type: %v", fType)
	}

	f.mtx.Lock()
	defer f.mtx.Unlock()

	ctx := context.Background()
	return f.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.FilterQueries) error {
			return q.PurgeRegularFilters(ctx)
		}, sqldbv2.NoOpReset)
}

// encodeFilter serialises a (possibly nil) gcs.Filter into bytes. A nil
// filter is encoded as a zero-length byte slice — the "deleted filter"
// sentinel preserved for compatibility with the legacy walletdb store.
func encodeFilter(f *gcs.Filter) ([]byte, error) {
	if f == nil {
		return []byte{}, nil
	}

	return f.NBytes()
}
