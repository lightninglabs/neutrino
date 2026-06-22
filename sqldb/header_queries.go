package sqldb

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/lightninglabs/neutrino/sqldb/sqlc"
)

const (
	// Modernc/sqlite's binder and statement journal costs grow sharply on
	// very large INSERT statements. A 32-row chunk keeps each 2,000-header
	// network batch in one transaction while avoiding the allocation blow-up
	// observed with 128+ row statements.
	headerInsertChunkSize = 32

	insertBlockHeaderPrefix = `INSERT OR FAIL INTO block_headers
(height, block_hash, raw_header) VALUES `

	insertFilterHeaderPrefix = `INSERT OR FAIL INTO filter_headers
(height, block_hash, filter_hash) VALUES `
)

// HeaderBatchQueries is implemented by the production SQLite header query
// wrapper. Tests and alternate query implementations may keep implementing
// only HeaderQueries; the public helpers below fall back to row-at-a-time
// inserts when this optional extension is absent.
type HeaderBatchQueries interface {
	InsertBlockHeaders(ctx context.Context,
		rows []sqlc.InsertBlockHeaderParams) error
	InsertFilterHeaders(ctx context.Context,
		rows []sqlc.InsertFilterHeaderParams) error
}

// NewSQLiteHeaderQueries wraps the generated sqlc query set with sqlite-only
// hot-path batch insert helpers for the two append-heavy header tables.
func NewSQLiteHeaderQueries(db sqlc.DBTX) HeaderQueries {
	return &headerQueries{
		Queries: sqlc.New(db),
		db:      db,
	}
}

type headerQueries struct {
	*sqlc.Queries

	db sqlc.DBTX
}

var _ HeaderBatchQueries = (*headerQueries)(nil)

type insertSQLCacheKey struct {
	prefix string
	rows   int
}

var insertSQLCache sync.Map

// InsertBlockHeaders writes rows in moderate multi-row chunks. This keeps each
// network batch in one SQL transaction, avoids a sqlite3_step per header, and
// also avoids the binder blow-up we saw with giant 2,000-row INSERTs.
func (q *headerQueries) InsertBlockHeaders(ctx context.Context,
	rows []sqlc.InsertBlockHeaderParams) error {

	args := make([]any, 0, headerInsertChunkSize*3)
	for start := 0; start < len(rows); start += headerInsertChunkSize {
		end := start + headerInsertChunkSize
		if end > len(rows) {
			end = len(rows)
		}

		chunk := rows[start:end]
		args = args[:0]
		for _, row := range chunk {
			args = append(
				args, row.Height, row.BlockHash, row.RawHeader,
			)
		}

		_, err := q.db.ExecContext(
			ctx, multiValueInsertSQL(
				insertBlockHeaderPrefix, len(chunk),
			), args...,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// InsertFilterHeaders writes rows in moderate multi-row chunks.
func (q *headerQueries) InsertFilterHeaders(ctx context.Context,
	rows []sqlc.InsertFilterHeaderParams) error {

	args := make([]any, 0, headerInsertChunkSize*3)
	for start := 0; start < len(rows); start += headerInsertChunkSize {
		end := start + headerInsertChunkSize
		if end > len(rows) {
			end = len(rows)
		}

		chunk := rows[start:end]
		args = args[:0]
		for _, row := range chunk {
			args = append(
				args, row.Height, row.BlockHash, row.FilterHash,
			)
		}

		_, err := q.db.ExecContext(
			ctx, multiValueInsertSQL(
				insertFilterHeaderPrefix, len(chunk),
			), args...,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func multiValueInsertSQL(prefix string, rows int) string {
	key := insertSQLCacheKey{
		prefix: prefix,
		rows:   rows,
	}
	if cached, ok := insertSQLCache.Load(key); ok {
		return cached.(string)
	}

	var builder strings.Builder
	builder.Grow(len(prefix) + rows*18)
	builder.WriteString(prefix)

	arg := 1
	for row := 0; row < rows; row++ {
		if row > 0 {
			builder.WriteString(", ")
		}

		builder.WriteByte('(')
		for col := 0; col < 3; col++ {
			if col > 0 {
				builder.WriteString(", ")
			}
			builder.WriteByte('$')
			builder.WriteString(strconv.Itoa(arg))
			arg++
		}
		builder.WriteByte(')')
	}

	sql := builder.String()
	actual, _ := insertSQLCache.LoadOrStore(key, sql)

	return actual.(string)
}

// InsertBlockHeaders uses a batch implementation when available and otherwise
// preserves compatibility with plain generated sqlc query sets.
func InsertBlockHeaders(ctx context.Context, q HeaderQueries,
	rows []sqlc.InsertBlockHeaderParams) error {

	if len(rows) == 0 {
		return nil
	}

	if batchQ, ok := q.(HeaderBatchQueries); ok {
		return batchQ.InsertBlockHeaders(ctx, rows)
	}

	for _, row := range rows {
		if err := q.InsertBlockHeader(ctx, row); err != nil {
			return err
		}
	}

	return nil
}

// InsertFilterHeaders uses a batch implementation when available and otherwise
// preserves compatibility with plain generated sqlc query sets.
func InsertFilterHeaders(ctx context.Context, q HeaderQueries,
	rows []sqlc.InsertFilterHeaderParams) error {

	if len(rows) == 0 {
		return nil
	}

	if batchQ, ok := q.(HeaderBatchQueries); ok {
		return batchQ.InsertFilterHeaders(ctx, rows)
	}

	for _, row := range rows {
		if err := q.InsertFilterHeader(ctx, row); err != nil {
			return err
		}
	}

	return nil
}
