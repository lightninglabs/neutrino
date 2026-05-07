package sqldb

import (
	"context"
	"time"

	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/lightninglabs/neutrino/sqldb/sqlc"
)

// HeaderQueries is the narrow subset of generated sqlc.Querier methods used
// by neutrino's block and filter header stores. Domains depend on this
// interface (rather than the full Querier) so that mocks and test doubles
// only need to implement the methods relevant to a given domain.
type HeaderQueries interface {
	// Block headers.
	InsertBlockHeader(ctx context.Context,
		arg sqlc.InsertBlockHeaderParams) error
	GetBlockHeaderByHeight(ctx context.Context, height int64) ([]byte,
		error)
	GetBlockHeaderByHash(ctx context.Context, blockHash []byte) (
		sqlc.GetBlockHeaderByHashRow, error)
	GetBlockHeaderHeightByHash(ctx context.Context, blockHash []byte) (
		int64, error)
	GetBlockHeaderRange(ctx context.Context,
		arg sqlc.GetBlockHeaderRangeParams) (
		[]sqlc.GetBlockHeaderRangeRow, error)
	DeleteBlockHeadersFromHeight(ctx context.Context, height int64) error
	CountBlockHeaders(ctx context.Context) (int64, error)

	// Filter headers.
	InsertFilterHeader(ctx context.Context,
		arg sqlc.InsertFilterHeaderParams) error
	GetFilterHeaderByHeight(ctx context.Context, height int64) (
		sqlc.GetFilterHeaderByHeightRow, error)
	GetFilterHeaderByBlockHash(ctx context.Context, blockHash []byte) (
		sqlc.GetFilterHeaderByBlockHashRow, error)
	GetFilterHeaderRange(ctx context.Context,
		arg sqlc.GetFilterHeaderRangeParams) ([]sqlc.FilterHeader,
		error)
	DeleteFilterHeadersFromHeight(ctx context.Context, height int64) error
	PurgeFilterHeaders(ctx context.Context) error
	CountFilterHeaders(ctx context.Context) (int64, error)

	// Chain tips (shared by both header chains).
	GetChainTip(ctx context.Context, chainType int32) (sqlc.GetChainTipRow,
		error)
	UpsertChainTip(ctx context.Context,
		arg sqlc.UpsertChainTipParams) error
	DeleteChainTip(ctx context.Context, chainType int32) error
}

// FilterQueries is the narrow subset of generated sqlc.Querier methods used
// by the filter database (filterdb.SQLFilterStore).
type FilterQueries interface {
	PutFilter(ctx context.Context, arg sqlc.PutFilterParams) error
	GetFilter(ctx context.Context, blockHash []byte) ([]byte, error)
	PurgeRegularFilters(ctx context.Context) error
}

// BanQueries is the narrow subset of generated sqlc.Querier methods used by
// the peer ban store (banman.SQLBanStore).
type BanQueries interface {
	UpsertBan(ctx context.Context, arg sqlc.UpsertBanParams) error
	GetBan(ctx context.Context, ipNet []byte) (sqlc.GetBanRow, error)
	DeleteBan(ctx context.Context, ipNet []byte) error
	DeleteExpiredBans(ctx context.Context, expiration time.Time) (int64,
		error)
}

// Compile-time assertions that the generated *sqlc.Queries struct satisfies
// each narrow domain interface.
var (
	_ HeaderQueries = (*sqlc.Queries)(nil)
	_ FilterQueries = (*sqlc.Queries)(nil)
	_ BanQueries    = (*sqlc.Queries)(nil)
)

// ChainTypeBlock and ChainTypeFilter are the persisted chain_type column
// values on the chain_tips table. They are SQL-layer concerns deliberately
// distinct from headerfs.HeaderType so the on-disk encoding stays decoupled
// from the in-memory enum.
const (
	ChainTypeBlock  int32 = 0
	ChainTypeFilter int32 = 1
)

// HeaderTx is the BatchedTx alias used by the header store implementations.
// All header operations are dispatched through ExecTx; the narrow
// HeaderQueries interface is supplied to the txBody closure inside ExecTx.
type HeaderTx = sqldbv2.BatchedTx[HeaderQueries]

// FilterTx is the BatchedTx alias used by the filter store implementation.
type FilterTx = sqldbv2.BatchedTx[FilterQueries]

// BanTx is the BatchedTx alias used by the ban store implementation.
type BanTx = sqldbv2.BatchedTx[BanQueries]
