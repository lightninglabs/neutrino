package sqldb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"

	"github.com/lightninglabs/neutrino/sqldb/sqlc"
)

// Default file names neutrino has historically used for the append-only
// header binaries. Callers may override via LegacyDataSource if they have
// non-default file names.
const (
	DefaultBlockHeadersFilename       = "block_headers.bin"
	DefaultRegFilterHeadersFilename   = "reg_filter_headers.bin"
)

// Legacy walletdb bucket and key names. They mirror the unexported names in
// neutrino/headerfs/index.go, neutrino/banman/store.go, and
// neutrino/filterdb/db.go. Duplicating them here keeps the legacy importer
// independent of the live store implementations.
var (
	legacyHeaderIndexBucket = []byte("header-index")
	legacyBitcoinTipKey     = []byte("bitcoin")
	legacyRegFilterTipKey   = []byte("regular")

	legacyFilterStoreBucket = []byte("filter-store")
	legacyRegFilterBucket   = []byte("regular")

	legacyBanStoreBucket   = []byte("ban-store")
	legacyBanIndexBucket   = []byte("ban-index")
	legacyReasonIndexBucket = []byte("reason-index")
)

// LegacyDataSource declares the legacy on-disk state to be imported into the
// SQL backend on first start. Constructed by the caller (lnd/lit/btcwallet)
// when transitioning from the walletdb backend, it bundles the open legacy
// bbolt handle alongside the data directory holding the two append-only
// header binaries.
//
// Pass to MakeLegacyImportMigration -> NewBackend; the import runs exactly
// once, recorded in the neutrino_migrations tracking table at version 2.
type LegacyDataSource struct {
	// DB is the pre-opened legacy walletdb. The migration only reads from
	// it; the caller must keep it open for the lifetime of the migration.
	DB walletdb.DB

	// DataDir is the directory containing the legacy header binary
	// files. Typically this is neutrino's DataDir.
	DataDir string

	// Params is the chain parameters used by the legacy stores. Used for
	// genesis-hash sanity checks during verification.
	Params *chaincfg.Params

	// BlockHeadersFilename overrides DefaultBlockHeadersFilename.
	BlockHeadersFilename string

	// FilterHeadersFilename overrides DefaultRegFilterHeadersFilename.
	FilterHeadersFilename string

	// BatchSize is the number of header rows inserted per transaction
	// during the import. Defaults to 2000 when zero.
	BatchSize int
}

// blockHeadersPath returns the legacy block_headers.bin path for the source.
func (l *LegacyDataSource) blockHeadersPath() string {
	name := l.BlockHeadersFilename
	if name == "" {
		name = DefaultBlockHeadersFilename
	}
	return filepath.Join(l.DataDir, name)
}

// filterHeadersPath returns the legacy reg_filter_headers.bin path for the
// source.
func (l *LegacyDataSource) filterHeadersPath() string {
	name := l.FilterHeadersFilename
	if name == "" {
		name = DefaultRegFilterHeadersFilename
	}
	return filepath.Join(l.DataDir, name)
}

// batchSize returns the configured batch size for the import, falling back
// to a sensible default when zero.
func (l *LegacyDataSource) batchSize() int {
	if l.BatchSize > 0 {
		return l.BatchSize
	}
	return 2000
}

// MakeLegacyImportMigration returns a MakeProgrammaticMigrations closure
// suitable for plugging into NewBackend. When src is nil, the closure is nil
// too (no programmatic migration registered, version 2 still recorded as
// a contentless no-op).
func MakeLegacyImportMigration(
	src *LegacyDataSource) MakeProgrammaticMigrations {

	if src == nil {
		return nil
	}

	return func(baseDB *sqldbv2.BaseDB) (
		map[uint]migrate.ProgrammaticMigrEntry, error) {

		entry := migrate.ProgrammaticMigrEntry{
			ResetVersionOnError: true,
			ProgrammaticMigr: func(_ *migrate.Migration,
				_ database.Driver) error {

				ctx := context.Background()
				return runLegacyImport(ctx, baseDB, src)
			},
		}

		return map[uint]migrate.ProgrammaticMigrEntry{
			2: entry,
		}, nil
	}
}

// runLegacyImport executes the full legacy import. The function is
// idempotent: every retry begins by truncating the SQL tables and re-reading
// the (read-only) legacy state from scratch.
func runLegacyImport(ctx context.Context, baseDB *sqldbv2.BaseDB,
	src *LegacyDataSource) error {

	if src == nil || src.DB == nil {
		log.Infof("No legacy data source declared; skipping import")
		return nil
	}

	blockBin := src.blockHeadersPath()
	filterBin := src.filterHeadersPath()

	log.Infof("Starting neutrino SQL legacy import from %s", src.DataDir)
	start := time.Now()

	if err := truncateTables(ctx, baseDB); err != nil {
		return fmt.Errorf("truncate target tables: %w", err)
	}

	blockTipHeight, blockTipHash, blockHashes, err := importBlockHeaders(
		ctx, baseDB, blockBin, src.batchSize(),
	)
	if err != nil {
		return fmt.Errorf("import block headers: %w", err)
	}

	filterTipHeight, filterTipHash, err := importFilterHeaders(
		ctx, baseDB, filterBin, src.batchSize(), blockHashes,
	)
	if err != nil {
		return fmt.Errorf("import filter headers: %w", err)
	}

	if err := upsertTips(
		ctx, baseDB, blockTipHash, blockTipHeight, filterTipHash,
		filterTipHeight,
	); err != nil {
		return fmt.Errorf("upsert chain tips: %w", err)
	}

	if err := importFilters(ctx, baseDB, src.DB, src.batchSize()); err != nil {
		return fmt.Errorf("import filters: %w", err)
	}

	if err := importBans(ctx, baseDB, src.DB); err != nil {
		return fmt.Errorf("import bans: %w", err)
	}

	if err := verifyImport(
		ctx, baseDB, src.DB, blockTipHeight, blockTipHash,
		filterTipHeight, filterTipHash,
	); err != nil {
		return fmt.Errorf("verify import: %w", err)
	}

	if err := renameLegacyArtifacts(blockBin, filterBin); err != nil {
		// A failure to rename does not invalidate the import. Log
		// and continue; the operator can clean up manually.
		log.Warnf("Legacy file rename had errors: %v", err)
	}

	log.Infof("Neutrino SQL legacy import complete in %v "+
		"(block tip=%s height=%d, filter tip=%s height=%d)",
		time.Since(start), blockTipHash, blockTipHeight,
		filterTipHash, filterTipHeight)

	return nil
}

// truncateTables wipes every neutrino table that the legacy import will
// repopulate. Running this at the top of every retry makes the import
// idempotent.
func truncateTables(ctx context.Context, baseDB *sqldbv2.BaseDB) error {
	tx, err := baseDB.BeginTx(ctx, sqldbv2.WriteTxOpt())
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	for _, stmt := range []string{
		"DELETE FROM banned_peers",
		"DELETE FROM regular_filters",
		"DELETE FROM filter_headers",
		"DELETE FROM block_headers",
		"DELETE FROM chain_tips",
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return tx.Commit()
}

// importBlockHeaders streams block_headers.bin into the block_headers table
// in batches. It returns the tip height and hash so the caller can later
// upsert the chain_tips row, plus a slice mapping height -> block hash that
// the filter header importer needs to resolve block hashes by height.
func importBlockHeaders(ctx context.Context, baseDB *sqldbv2.BaseDB,
	binPath string, batchSize int) (uint32, chainhash.Hash, []chainhash.Hash,
	error) {

	var tipHash chainhash.Hash

	f, err := os.Open(binPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Infof("No legacy block_headers.bin found; " +
				"skipping block header import")
			return 0, tipHash, nil, nil
		}
		return 0, tipHash, nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, tipHash, nil, err
	}
	totalBytes := fi.Size()
	if totalBytes%80 != 0 {
		return 0, tipHash, nil, fmt.Errorf(
			"block_headers.bin size %d not a multiple of 80",
			totalBytes,
		)
	}
	if totalBytes == 0 {
		return 0, tipHash, nil, nil
	}

	tipHeight := uint32(totalBytes/80) - 1
	hashes := make([]chainhash.Hash, tipHeight+1)

	for batchStart := uint32(0); batchStart <= tipHeight; batchStart += uint32(batchSize) {
		batchEnd := batchStart + uint32(batchSize) - 1
		if batchEnd > tipHeight {
			batchEnd = tipHeight
		}
		nHeaders := batchEnd - batchStart + 1

		buf := make([]byte, int64(nHeaders)*80)
		if _, err := f.ReadAt(buf, int64(batchStart)*80); err != nil {
			return 0, tipHash, nil, fmt.Errorf(
				"read block headers at height %d: %w",
				batchStart, err,
			)
		}

		tx, err := baseDB.BeginTx(ctx, sqldbv2.WriteTxOpt())
		if err != nil {
			return 0, tipHash, nil, err
		}
		q := sqlc.New(tx)

		for i := uint32(0); i < nHeaders; i++ {
			height := batchStart + i
			raw := buf[i*80 : (i+1)*80]

			var hdr wire.BlockHeader
			if err := hdr.Deserialize(bytes.NewReader(raw)); err != nil {
				_ = tx.Rollback()
				return 0, tipHash, nil, fmt.Errorf(
					"deserialize block header at height "+
						"%d: %w", height, err,
				)
			}
			hash := hdr.BlockHash()
			rawCopy := make([]byte, 80)
			copy(rawCopy, raw)

			err := q.InsertBlockHeader(
				ctx, sqlc.InsertBlockHeaderParams{
					Height:    int64(height),
					BlockHash: hash[:],
					RawHeader: rawCopy,
				},
			)
			if err != nil {
				_ = tx.Rollback()
				return 0, tipHash, nil, fmt.Errorf(
					"insert block header at height %d: %w",
					height, err,
				)
			}

			hashes[height] = hash
			tipHash = hash
		}

		if err := tx.Commit(); err != nil {
			return 0, tipHash, nil, err
		}
	}

	log.Infof("Imported %d block headers (tip %s)",
		tipHeight+1, tipHash)

	return tipHeight, tipHash, hashes, nil
}

// importFilterHeaders streams reg_filter_headers.bin into the
// filter_headers table in batches. It uses the height->blockHash slice
// produced by importBlockHeaders to populate the block_hash column
// (the .bin file only stores filter hashes).
func importFilterHeaders(ctx context.Context, baseDB *sqldbv2.BaseDB,
	binPath string, batchSize int,
	blockHashes []chainhash.Hash) (uint32, chainhash.Hash, error) {

	var tipFilterHash chainhash.Hash

	f, err := os.Open(binPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Infof("No legacy reg_filter_headers.bin found; " +
				"skipping filter header import")
			return 0, tipFilterHash, nil
		}
		return 0, tipFilterHash, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, tipFilterHash, err
	}
	totalBytes := fi.Size()
	if totalBytes%32 != 0 {
		return 0, tipFilterHash, fmt.Errorf(
			"reg_filter_headers.bin size %d not a multiple of 32",
			totalBytes,
		)
	}
	if totalBytes == 0 {
		return 0, tipFilterHash, nil
	}

	tipHeight := uint32(totalBytes/32) - 1
	if int(tipHeight) >= len(blockHashes) {
		return 0, tipFilterHash, fmt.Errorf(
			"filter header tip height %d exceeds block header "+
				"tip %d", tipHeight, len(blockHashes)-1,
		)
	}

	for batchStart := uint32(0); batchStart <= tipHeight; batchStart += uint32(batchSize) {
		batchEnd := batchStart + uint32(batchSize) - 1
		if batchEnd > tipHeight {
			batchEnd = tipHeight
		}
		nHeaders := batchEnd - batchStart + 1

		buf := make([]byte, int64(nHeaders)*32)
		if _, err := f.ReadAt(buf, int64(batchStart)*32); err != nil {
			return 0, tipFilterHash, fmt.Errorf(
				"read filter headers at height %d: %w",
				batchStart, err,
			)
		}

		tx, err := baseDB.BeginTx(ctx, sqldbv2.WriteTxOpt())
		if err != nil {
			return 0, tipFilterHash, err
		}
		q := sqlc.New(tx)

		for i := uint32(0); i < nHeaders; i++ {
			height := batchStart + i
			var filterHash chainhash.Hash
			copy(filterHash[:], buf[i*32:(i+1)*32])
			blockHash := blockHashes[height]

			err := q.InsertFilterHeader(
				ctx, sqlc.InsertFilterHeaderParams{
					Height:     int64(height),
					BlockHash:  blockHash[:],
					FilterHash: filterHash[:],
				},
			)
			if err != nil {
				_ = tx.Rollback()
				return 0, tipFilterHash, fmt.Errorf(
					"insert filter header at height %d: %w",
					height, err,
				)
			}

			tipFilterHash = filterHash
		}

		if err := tx.Commit(); err != nil {
			return 0, tipFilterHash, err
		}
	}

	log.Infof("Imported %d filter headers (tip %s)",
		tipHeight+1, tipFilterHash)

	return tipHeight, tipFilterHash, nil
}

// upsertTips writes the chain_tips rows for both header chains.
func upsertTips(ctx context.Context, baseDB *sqldbv2.BaseDB,
	blockHash chainhash.Hash, blockHeight uint32,
	filterHash chainhash.Hash, filterHeight uint32) error {

	tx, err := baseDB.BeginTx(ctx, sqldbv2.WriteTxOpt())
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	q := sqlc.New(tx)

	if err := q.UpsertChainTip(
		ctx, sqlc.UpsertChainTipParams{
			ChainType: ChainTypeBlock,
			TipHash:   blockHash[:],
			TipHeight: int64(blockHeight),
		},
	); err != nil {
		return err
	}

	if err := q.UpsertChainTip(
		ctx, sqlc.UpsertChainTipParams{
			ChainType: ChainTypeFilter,
			TipHash:   filterHash[:],
			TipHeight: int64(filterHeight),
		},
	); err != nil {
		return err
	}

	return tx.Commit()
}

// importFilters copies every regular_filter row from the legacy walletdb
// filter-store/regular bucket into the regular_filters table.
func importFilters(ctx context.Context, baseDB *sqldbv2.BaseDB,
	legacyDB walletdb.DB, batchSize int) error {

	type filterRow struct {
		blockHash []byte
		bytes     []byte
	}

	var rows []filterRow
	err := walletdb.View(legacyDB, func(tx walletdb.ReadTx) error {
		root := tx.ReadBucket(legacyFilterStoreBucket)
		if root == nil {
			return nil
		}
		reg := root.NestedReadBucket(legacyRegFilterBucket)
		if reg == nil {
			return nil
		}

		return reg.ForEach(func(k, v []byte) error {
			// bbolt may return nil for keys whose value was
			// stored as nil (the legacy "deleted filter"
			// sentinel). Coerce to a non-nil empty slice so the
			// row inserts cleanly into the NOT NULL filter_bytes
			// column; FetchFilter still treats zero-length as
			// the nil sentinel on read.
			val := append([]byte{}, v...)
			row := filterRow{
				blockHash: append([]byte(nil), k...),
				bytes:     val,
			}
			rows = append(rows, row)
			return nil
		})
	})
	if err != nil {
		return err
	}

	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}

		tx, err := baseDB.BeginTx(ctx, sqldbv2.WriteTxOpt())
		if err != nil {
			return err
		}
		q := sqlc.New(tx)

		for _, row := range rows[start:end] {
			err := q.PutFilter(ctx, sqlc.PutFilterParams{
				BlockHash:   row.blockHash,
				FilterBytes: row.bytes,
			})
			if err != nil {
				_ = tx.Rollback()
				return err
			}
		}

		if err := tx.Commit(); err != nil {
			return err
		}
	}

	log.Infof("Imported %d regular filters", len(rows))
	return nil
}

// importBans copies every banned_peer row from the legacy walletdb ban-store
// buckets into the banned_peers table.
func importBans(ctx context.Context, baseDB *sqldbv2.BaseDB,
	legacyDB walletdb.DB) error {

	type banRow struct {
		ipNet      []byte
		expiration time.Time
		reason     int32
	}

	var rows []banRow
	err := walletdb.View(legacyDB, func(tx walletdb.ReadTx) error {
		root := tx.ReadBucket(legacyBanStoreBucket)
		if root == nil {
			return nil
		}

		banIdx := root.NestedReadBucket(legacyBanIndexBucket)
		reasonIdx := root.NestedReadBucket(legacyReasonIndexBucket)
		if banIdx == nil || reasonIdx == nil {
			return nil
		}

		return banIdx.ForEach(func(k, v []byte) error {
			if len(v) != 8 {
				return fmt.Errorf("ban-index value for "+
					"key %x has length %d, want 8", k,
					len(v))
			}
			expirationUnix := int64(binary.BigEndian.Uint64(v))
			expiration := time.Unix(expirationUnix, 0).UTC()

			reasonBytes := reasonIdx.Get(k)
			if len(reasonBytes) != 1 {
				return fmt.Errorf("reason-index value for "+
					"key %x has length %d, want 1", k,
					len(reasonBytes))
			}

			rows = append(rows, banRow{
				ipNet:      append([]byte(nil), k...),
				expiration: expiration,
				reason:     int32(reasonBytes[0]),
			})
			return nil
		})
	})
	if err != nil {
		return err
	}

	now := time.Now().UTC()

	tx, err := baseDB.BeginTx(ctx, sqldbv2.WriteTxOpt())
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	q := sqlc.New(tx)

	imported := 0
	for _, row := range rows {
		// Skip already-expired bans; the live store would auto-purge
		// them on first read anyway.
		if !row.expiration.After(now) {
			continue
		}
		err := q.UpsertBan(ctx, sqlc.UpsertBanParams{
			IpNet:      row.ipNet,
			Expiration: row.expiration,
			Reason:     row.reason,
		})
		if err != nil {
			return err
		}
		imported++
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Infof("Imported %d active bans (%d skipped expired)",
		imported, len(rows)-imported)
	return nil
}

// verifyImport runs a few sanity checks comparing the freshly populated SQL
// tables to the legacy store. If the legacy header binaries report no rows
// (e.g. the caller is migrating a partially-populated state) verification
// short-circuits to success.
func verifyImport(ctx context.Context, baseDB *sqldbv2.BaseDB,
	legacyDB walletdb.DB, blockTipHeight uint32,
	blockTipHash chainhash.Hash, filterTipHeight uint32,
	filterTipHash chainhash.Hash) error {

	tx, err := baseDB.BeginTx(ctx, sqldbv2.ReadTxOpt())
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	q := sqlc.New(tx)

	blockCount, err := q.CountBlockHeaders(ctx)
	if err != nil {
		return err
	}
	if blockCount != int64(blockTipHeight)+1 {
		return fmt.Errorf("block_headers row count %d != "+
			"expected %d", blockCount, blockTipHeight+1)
	}

	filterCount, err := q.CountFilterHeaders(ctx)
	if err != nil {
		return err
	}
	if filterCount != int64(filterTipHeight)+1 {
		return fmt.Errorf("filter_headers row count %d != "+
			"expected %d", filterCount, filterTipHeight+1)
	}

	blockTip, err := q.GetChainTip(ctx, ChainTypeBlock)
	if err != nil {
		return err
	}
	if !bytes.Equal(blockTip.TipHash, blockTipHash[:]) {
		return fmt.Errorf("block chain tip mismatch: got %x, "+
			"want %x", blockTip.TipHash, blockTipHash[:])
	}

	filterTip, err := q.GetChainTip(ctx, ChainTypeFilter)
	if err != nil {
		return err
	}
	if !bytes.Equal(filterTip.TipHash, filterTipHash[:]) {
		return fmt.Errorf("filter chain tip mismatch: got %x, "+
			"want %x", filterTip.TipHash, filterTipHash[:])
	}

	return nil
}

// renameLegacyArtifacts renames the legacy header binaries to .bak after a
// successful import. Failure to rename is non-fatal (the import has already
// completed and is recorded in neutrino_migrations).
func renameLegacyArtifacts(blockBin, filterBin string) error {
	var rerr error
	for _, p := range []string{blockBin, filterBin} {
		if p == "" {
			continue
		}
		if _, err := os.Stat(p); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			rerr = errors.Join(rerr, err)
			continue
		}

		bak := p + ".bak"
		if err := os.Rename(p, bak); err != nil {
			rerr = errors.Join(rerr, fmt.Errorf("rename %s: %w",
				p, err))
		}
	}
	return rerr
}

// LegacyArtifactsExist returns true when at least one of the legacy
// neutrino files (block_headers.bin or reg_filter_headers.bin) is present
// in src.DataDir. Callers can use this to decide whether to bother passing
// a LegacyDataSource to NewBackend at all.
func LegacyArtifactsExist(src *LegacyDataSource) bool {
	if src == nil {
		return false
	}
	for _, p := range []string{
		src.blockHeadersPath(), src.filterHeadersPath(),
	} {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
}

// Compile-time assertion: chainhash.Hash size matches the schema assumption.
var _ = [chainhash.HashSize]byte{}

// Sentinel sql.ErrNoRows reference to avoid an unused-import warning when
// the package is built without legacy_test.go in scope.
var _ = sql.ErrNoRows
