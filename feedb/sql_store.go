package feedb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"

	"github.com/lightninglabs/neutrino/sqldb"
	"github.com/lightninglabs/neutrino/sqldb/sqlc"
)

// SQLFeeStore is the SQL-backed implementation of FeeSampleStore, operating
// against the fee_samples table via the sqlc-generated queries. The caller is
// expected to have already migrated the schema via sqldb.NewBackend; this
// store holds only the injected transaction executor.
//
// The table keeps at most one row per height: PutSample upserts on the height
// primary key, so a competing block observed at the same height (a transient
// reorg state before PruneFrom runs) simply replaces the orphaned row.
type SQLFeeStore struct {
	db sqldb.FeeTx
}

// Compile-time check.
var _ FeeSampleStore = (*SQLFeeStore)(nil)

// NewSQLStore returns a SQLFeeStore backed by the given fee transaction
// executor.
func NewSQLStore(db sqldb.FeeTx) (*SQLFeeStore, error) {
	if db == nil {
		return nil, errors.New("nil fee tx executor")
	}

	return &SQLFeeStore{db: db}, nil
}

// rowToSample converts a sqlc row into the exported FeeSample struct.
func rowToSample(row sqlc.FeeSample) (*FeeSample, error) {
	if len(row.BlockHash) != chainhash.HashSize {
		return nil, fmt.Errorf("fee sample h=%d: bad hash size %d",
			row.Height, len(row.BlockHash))
	}

	s := &FeeSample{
		Height:         uint32(row.Height),
		Timestamp:      row.Timestamp,
		TotalFees:      uint64(row.TotalFees),
		TotalWeight:    uint64(row.TotalWeight),
		CoinbaseWeight: uint64(row.CoinbaseWeight),
		MinKnownTxRate: uint64(row.MinKnownTxRate),
		KnownTxCount:   uint16(row.KnownTxCount),
		Flags:          SampleFlag(row.Flags),
	}
	copy(s.BlockHash[:], row.BlockHash)

	return s, nil
}

// sampleToParams converts a FeeSample into upsert parameters. Values that
// would overflow the signed SQL column are rejected rather than silently
// truncated; no real block can produce them, so an overflow indicates a
// corrupted sample.
func sampleToParams(s *FeeSample) (sqlc.UpsertFeeSampleParams, error) {
	if s.TotalFees > math.MaxInt64 || s.TotalWeight > math.MaxInt64 ||
		s.CoinbaseWeight > math.MaxInt64 ||
		s.MinKnownTxRate > math.MaxInt64 {

		return sqlc.UpsertFeeSampleParams{}, fmt.Errorf("fee sample "+
			"h=%d: value overflows int64", s.Height)
	}

	return sqlc.UpsertFeeSampleParams{
		Height:         int64(s.Height),
		BlockHash:      s.BlockHash[:],
		Timestamp:      s.Timestamp,
		TotalFees:      int64(s.TotalFees),
		TotalWeight:    int64(s.TotalWeight),
		CoinbaseWeight: int64(s.CoinbaseWeight),
		MinKnownTxRate: int64(s.MinKnownTxRate),
		KnownTxCount:   int32(s.KnownTxCount),
		Flags:          int32(s.Flags),
	}, nil
}

// PutSample writes a sample, replacing any existing row at the same height.
//
// NOTE: This method is part of the FeeSampleStore interface.
func (f *SQLFeeStore) PutSample(s *FeeSample) error {
	if s == nil {
		return errors.New("nil sample")
	}

	params, err := sampleToParams(s)
	if err != nil {
		return err
	}

	ctx := context.Background()
	err = f.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.FeeQueries) error {
			return q.UpsertFeeSample(ctx, params)
		}, sqldbv2.NoOpReset)
	if err != nil {
		return err
	}

	log.Tracef("Stored fee sample h=%d hash=%s fees=%d weight=%d",
		s.Height, s.BlockHash, s.TotalFees, s.TotalWeight)

	return nil
}

// FetchSample looks up a sample by height.
//
// NOTE: This method is part of the FeeSampleStore interface.
func (f *SQLFeeStore) FetchSample(height uint32) (*FeeSample, error) {
	var (
		sample *FeeSample
		ctx    = context.Background()
	)
	err := f.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.FeeQueries) error {
			row, err := q.GetFeeSampleByHeight(ctx, int64(height))
			switch {
			case errors.Is(err, sql.ErrNoRows):
				return ErrSampleNotFound

			case err != nil:
				return err
			}

			sample, err = rowToSample(row)
			return err
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, err
	}

	return sample, nil
}

// FetchTipN returns up to n samples in descending height order.
//
// NOTE: This method is part of the FeeSampleStore interface.
func (f *SQLFeeStore) FetchTipN(n int) ([]*FeeSample, error) {
	if n <= 0 {
		return nil, nil
	}
	if n > math.MaxInt32 {
		n = math.MaxInt32
	}

	var (
		out []*FeeSample
		ctx = context.Background()
	)
	err := f.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.FeeQueries) error {
			rows, err := q.GetFeeSamplesTipN(ctx, int32(n))
			if err != nil {
				return err
			}

			out = make([]*FeeSample, 0, len(rows))
			for _, row := range rows {
				s, err := rowToSample(row)
				if err != nil {
					return err
				}
				out = append(out, s)
			}

			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// FetchRange returns samples in [min, max] inclusive, ascending by height.
//
// NOTE: This method is part of the FeeSampleStore interface.
func (f *SQLFeeStore) FetchRange(min, max uint32) ([]*FeeSample, error) {
	if min > max {
		return nil, fmt.Errorf("invalid range [%d, %d]", min, max)
	}

	var (
		out []*FeeSample
		ctx = context.Background()
	)
	err := f.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.FeeQueries) error {
			rows, err := q.GetFeeSampleRange(
				ctx, sqlc.GetFeeSampleRangeParams{
					StartHeight: int64(min),
					EndHeight:   int64(max),
				},
			)
			if err != nil {
				return err
			}

			out = make([]*FeeSample, 0, len(rows))
			for _, row := range rows {
				s, err := rowToSample(row)
				if err != nil {
					return err
				}
				out = append(out, s)
			}

			return nil
		}, sqldbv2.NoOpReset)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Tip returns the height of the highest stored sample, or 0 if the store is
// empty.
//
// NOTE: This method is part of the FeeSampleStore interface.
func (f *SQLFeeStore) Tip() (uint32, error) {
	var (
		tip int64
		ctx = context.Background()
	)
	err := f.db.ExecTx(ctx, sqldbv2.ReadTxOpt(),
		func(q sqldb.FeeQueries) error {
			var err error
			tip, err = q.GetFeeSampleTip(ctx)
			return err
		}, sqldbv2.NoOpReset)
	if err != nil {
		return 0, err
	}

	return uint32(tip), nil
}

// PurgeBefore deletes samples with height < cutoff.
//
// NOTE: This method is part of the FeeSampleStore interface.
func (f *SQLFeeStore) PurgeBefore(cutoff uint32) error {
	ctx := context.Background()
	return f.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.FeeQueries) error {
			return q.DeleteFeeSamplesBeforeHeight(
				ctx, int64(cutoff),
			)
		}, sqldbv2.NoOpReset)
}

// PurgeFrom deletes samples with height >= cutoff. Used to drop the orphaned
// suffix on a reorg.
//
// NOTE: This method is part of the FeeSampleStore interface.
func (f *SQLFeeStore) PurgeFrom(cutoff uint32) error {
	ctx := context.Background()
	return f.db.ExecTx(ctx, sqldbv2.WriteTxOpt(),
		func(q sqldb.FeeQueries) error {
			return q.DeleteFeeSamplesFromHeight(ctx, int64(cutoff))
		}, sqldbv2.NoOpReset)
}
