package sqldb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb" // Registers "bdb".
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"

	"github.com/lightninglabs/neutrino/sqldb/sqlc"
)

// Default file names neutrino has historically used for the append-only header
// binaries and the walletdb (bbolt) database. Callers may override via
// LegacyDataSource if they have non-default file names.
const (
	DefaultBlockHeadersFilename     = "block_headers.bin"
	DefaultRegFilterHeadersFilename = "reg_filter_headers.bin"

	// DefaultLegacyBoltFilename is the file name lnd (and most other
	// callers) give the walletdb that holds neutrino's filter and ban
	// buckets. It lives next to the flat files in the neutrino DataDir.
	DefaultLegacyBoltFilename = "neutrino.db"
)

const (
	// blockHeaderSize is the size of a serialized bitcoin block header,
	// which is also the record size of block_headers.bin.
	blockHeaderSize = 80

	// filterHeaderSize is the size of a filter header hash, which is also
	// the record size of reg_filter_headers.bin.
	filterHeaderSize = chainhash.HashSize

	// legacyIndexPrefixBytes is the number of leading hash bytes naming the
	// header index sub-bucket an entry lives in.
	legacyIndexPrefixBytes = 2

	// prevBlockOffset is the offset of the previous block hash inside a
	// serialized block header (after the 4 byte version).
	prevBlockOffset = 4

	// defaultImportBatchSize is the number of rows written per SQL
	// transaction when LegacyDataSource.BatchSize is unset.
	defaultImportBatchSize = 100_000

	// boltOpenTimeout bounds how long the importer waits for the legacy
	// walletdb file lock before failing the import.
	boltOpenTimeout = 10 * time.Second

	// backupSuffix is appended to legacy flat files after a successful
	// import.
	backupSuffix = ".bak"
)

// Legacy walletdb bucket and key names. They mirror the unexported names in
// neutrino/headerfs/index.go, neutrino/banman/store.go, and
// neutrino/filterdb/db.go. Duplicating them here keeps the legacy importer
// independent of the live store implementations.
var (
	legacyFilterStoreBucket = []byte("filter-store")
	legacyRegFilterBucket   = []byte("regular")

	// The header index root bucket and the keys under which it tracks the
	// tips of the two header chains, see headerfs/index.go.
	legacyIndexBucket  = []byte("header-index")
	legacyBitcoinTip   = []byte("bitcoin")
	legacyRegFilterTip = []byte("regular")

	legacyBanStoreBucket    = []byte("ban-store")
	legacyBanIndexBucket    = []byte("ban-index")
	legacyReasonIndexBucket = []byte("reason-index")
)

// LegacyDataSource declares the legacy on-disk state to be imported into the
// SQL backend on first start. Callers normally do not need to build one:
// NewBackendWithLegacy (and so neutrino.NewChainService) detects the legacy
// files in the data directory by itself. A LegacyDataSource is only needed to
// override the detected locations or to hand over an already-open walletdb.
//
// The import runs exactly once, recorded in the neutrino_migrations tracking
// table at version 2.
type LegacyDataSource struct {
	// DB is an optional pre-opened legacy walletdb. The migration only
	// reads from it; the caller must keep it open for the lifetime of the
	// migration. When nil, the importer opens BoltPath read-only itself.
	DB walletdb.DB

	// BoltPath is the legacy walletdb file opened when DB is nil. It
	// defaults to DataDir/neutrino.db. A missing file means there are no
	// legacy filters or bans to import.
	BoltPath string

	// DataDir is the directory containing the legacy header binary files.
	// Typically this is neutrino's DataDir.
	DataDir string

	// Params is the chain parameters used by the legacy stores. Used for
	// genesis-hash sanity checks during verification.
	Params *chaincfg.Params

	// BlockHeadersFilename overrides DefaultBlockHeadersFilename.
	BlockHeadersFilename string

	// FilterHeadersFilename overrides DefaultRegFilterHeadersFilename.
	FilterHeadersFilename string

	// BatchSize is the number of header rows written per SQL transaction
	// during the import. Defaults to 100,000 when zero.
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

// boltPath returns the legacy walletdb path for the source.
func (l *LegacyDataSource) boltPath() string {
	if l.BoltPath != "" {
		return l.BoltPath
	}
	return filepath.Join(l.DataDir, DefaultLegacyBoltFilename)
}

// batchSize returns the configured batch size for the import, falling back to
// a sensible default when zero.
func (l *LegacyDataSource) batchSize() int {
	if l.BatchSize > 0 {
		return l.BatchSize
	}
	return defaultImportBatchSize
}

// fileExists reports whether path names an existing non-empty regular file.
func fileExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.Mode().IsRegular() && fi.Size() > 0
}

// LegacyArtifactsExist returns true when at least one of the legacy neutrino
// files (the two header binaries or the walletdb file) is present for src.
func LegacyArtifactsExist(src *LegacyDataSource) bool {
	if src == nil {
		return false
	}

	return fileExists(src.blockHeadersPath()) ||
		fileExists(src.filterHeadersPath()) ||
		(src.DB == nil && fileExists(src.boltPath()))
}

// DetectLegacyDataSource looks for legacy neutrino state in dataDir using the
// default file names. It returns nil when none of the header binaries or the
// walletdb file exist, which is the case for every fresh install.
func DetectLegacyDataSource(dataDir string,
	params *chaincfg.Params) *LegacyDataSource {

	src := &LegacyDataSource{DataDir: dataDir, Params: params}
	if !LegacyArtifactsExist(src) {
		return nil
	}

	return src
}

// legacyImporter runs the legacy import as programmatic migration version 2
// and remembers which flat files to retire once the migration is recorded.
type legacyImporter struct {
	src *LegacyDataSource

	// retire lists the flat files to rename once the import is durable.
	retire []string
}

// migrations returns the MakeProgrammaticMigrations closure registering the
// import as migration version 2.
func (i *legacyImporter) migrations() MakeProgrammaticMigrations {
	return func(baseDB *sqldbv2.BaseDB) (
		map[uint]migrate.ProgrammaticMigrEntry, error) {

		entry := migrate.ProgrammaticMigrEntry{
			ResetVersionOnError: true,
			ProgrammaticMigr: func(_ *migrate.Migration,
				_ database.Driver) error {

				return i.run(context.Background(), baseDB)
			},
		}

		return map[uint]migrate.ProgrammaticMigrEntry{
			2: entry,
		}, nil
	}
}

// finalize renames the imported flat files to *.bak. It must only run after
// the migration was recorded: renaming earlier would leave a crash window in
// which the retry no longer finds the headers it has to re-import.
func (i *legacyImporter) finalize() {
	if err := renameLegacyArtifacts(i.retire...); err != nil {
		// A failure to rename does not invalidate the import. Log and
		// continue; the operator can clean up manually.
		log.Warnf("Legacy file rename had errors: %v", err)
	}
	i.retire = nil
}

// MakeLegacyImportMigration returns a MakeProgrammaticMigrations closure
// suitable for plugging into NewBackend. When src is nil, the closure is nil
// too (no programmatic migration registered, version 2 still recorded as a
// contentless no-op). Prefer NewBackendWithLegacy, which also detects legacy
// data by itself and retires the flat files only after the import is
// recorded. The flat files are left in place by the closure returned here;
// retiring them before the migration is recorded would risk losing them.
func MakeLegacyImportMigration(
	src *LegacyDataSource) MakeProgrammaticMigrations {

	if src == nil {
		return nil
	}

	imp := &legacyImporter{src: src}

	return imp.migrations()
}

// legacyPlan describes which legacy artifacts exist and were validated.
type legacyPlan struct {
	// blockCount and filterCount are the number of records in the two
	// flat files; zero when the file is absent or empty.
	blockCount  uint32
	filterCount uint32
}

// planFlatFiles stats the flat files and rejects inconsistent combinations
// before any SQL table is touched.
func planFlatFiles(blockBin, filterBin string) (legacyPlan, error) {
	var plan legacyPlan

	count := func(path string, recSize int64) (uint32, error) {
		fi, err := os.Stat(path)
		switch {
		case errors.Is(err, os.ErrNotExist):
			return 0, nil
		case err != nil:
			return 0, err
		}

		if fi.Size()%recSize != 0 {
			return 0, fmt.Errorf("%s: size %d is not a multiple "+
				"of the %d byte record size, the file is "+
				"truncated or corrupt", path, fi.Size(),
				recSize)
		}

		return uint32(fi.Size() / recSize), nil
	}

	var err error
	plan.blockCount, err = count(blockBin, blockHeaderSize)
	if err != nil {
		return plan, err
	}
	plan.filterCount, err = count(filterBin, filterHeaderSize)
	if err != nil {
		return plan, err
	}

	return plan, nil
}

// validate rejects combinations of flat file contents that cannot be
// imported.
func (p legacyPlan) validate(blockBin, filterBin string) error {
	switch {
	// Filter headers are keyed by block hash, which is only recoverable
	// from the block headers.
	case p.filterCount > 0 && p.blockCount == 0:
		return fmt.Errorf("%s has %d filter headers but %s is "+
			"missing or empty", filterBin, p.filterCount, blockBin)

	// The filter header chain trails the block header chain, never leads.
	case p.filterCount > p.blockCount:
		return fmt.Errorf("%s has %d filter headers, more than the %d "+
			"block headers in %s", filterBin, p.filterCount,
			p.blockCount, blockBin)
	}

	return nil
}

// legacyIndexTipHeight returns the height of the chain tip recorded under
// tipKey in the legacy header index of db, and false if the index records no
// such tip.
func legacyIndexTipHeight(db walletdb.DB, tipKey []byte) (uint32, bool,
	error) {

	var (
		height uint32
		found  bool
	)
	err := walletdb.View(db, func(tx walletdb.ReadTx) error {
		root := tx.ReadBucket(legacyIndexBucket)
		if root == nil {
			return nil
		}

		hash := root.Get(tipKey)
		if len(hash) != chainhash.HashSize {
			return nil
		}

		// Entries live in a sub-bucket keyed by the leading hash
		// bytes, or directly in the root bucket for older databases.
		val := root.Get(hash)
		sub := root.NestedReadBucket(hash[:legacyIndexPrefixBytes])
		if sub != nil && sub.Get(hash) != nil {
			val = sub.Get(hash)
		}
		if len(val) != 4 {
			return fmt.Errorf("legacy header index entry for tip "+
				"%x is malformed", hash)
		}

		height = binary.BigEndian.Uint32(val)
		found = true

		return nil
	})

	return height, found, err
}

// reconcileWithIndex trims the flat file record counts to the chain tips
// recorded in the legacy walletdb header index, which is authoritative: the
// legacy stores truncate the files to it on startup, so records past the
// index tip are leftovers of an interrupted write. A file that is shorter than
// its index is imported as is.
func (p *legacyPlan) reconcileWithIndex(db walletdb.DB, blockBin,
	filterBin string) error {

	for _, c := range []struct {
		name  string
		path  string
		tip   []byte
		count *uint32
	}{
		{"block", blockBin, legacyBitcoinTip, &p.blockCount},
		{"filter", filterBin, legacyRegFilterTip, &p.filterCount},
	} {
		height, ok, err := legacyIndexTipHeight(db, c.tip)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		switch {
		// The file lost records the index knows about. What is
		// there is a valid prefix, so import it and let the node
		// sync the rest again.
		case height+1 > *c.count:
			log.Warnf("Legacy %s header index tip is at height "+
				"%d but %s only has %d records", c.name,
				height, c.path, *c.count)

		case height+1 < *c.count:
			log.Warnf("Ignoring %d %s header records in %s past "+
				"the legacy index tip at height %d",
				*c.count-(height+1), c.name, c.path, height)
			*c.count = height + 1
		}
	}

	return nil
}

// run executes the full legacy import. The function is idempotent: every
// retry begins by truncating the SQL tables and re-reading the (read-only)
// legacy state from scratch.
func (i *legacyImporter) run(ctx context.Context,
	baseDB *sqldbv2.BaseDB) error {

	src := i.src
	blockBin := src.blockHeadersPath()
	filterBin := src.filterHeadersPath()

	plan, err := planFlatFiles(blockBin, filterBin)
	if err != nil {
		return err
	}

	// Open the legacy walletdb unless the caller handed us one. A missing
	// file just means there are no legacy filters or bans.
	legacyDB := src.DB
	if legacyDB == nil && fileExists(src.boltPath()) {
		legacyDB, err = walletdb.Open(
			"bdb", src.boltPath(), true, boltOpenTimeout, true,
		)
		if err != nil {
			return fmt.Errorf("open legacy walletdb %s: %w",
				src.boltPath(), err)
		}
		defer legacyDB.Close()
	}

	// Settle on what to import and validate it before any table is
	// touched, so that a bad file fails the import cleanly.
	if legacyDB != nil {
		err = plan.reconcileWithIndex(legacyDB, blockBin, filterBin)
		if err != nil {
			return err
		}
	}
	if err := plan.validate(blockBin, filterBin); err != nil {
		return err
	}

	log.Infof("Starting neutrino SQL legacy import from %s "+
		"(%d block headers, %d filter headers, walletdb=%v)",
		src.DataDir, plan.blockCount, plan.filterCount,
		legacyDB != nil)
	start := time.Now()

	if err := truncateTables(ctx, baseDB); err != nil {
		return fmt.Errorf("truncate target tables: %w", err)
	}

	bw := newBulkWriter(baseDB)

	// Block headers. The hash of every header is kept so the filter
	// headers can be keyed by block hash without another lookup.
	var (
		blockTipHash    chainhash.Hash
		filterTipHash   chainhash.Hash
		blockTipHeight  uint32
		filterTipHeight uint32
		blockHashes     []byte
	)
	if plan.blockCount > 0 {
		blockHashes, err = bw.importBlockHeaders(
			ctx, blockBin, plan.blockCount, src.batchSize(),
			src.Params,
		)
		if err != nil {
			return fmt.Errorf("import block headers: %w", err)
		}

		blockTipHeight = plan.blockCount - 1
		copy(blockTipHash[:], blockHashes[int(blockTipHeight)*
			chainhash.HashSize:])
	}

	if plan.filterCount > 0 {
		err = bw.importFilterHeaders(
			ctx, filterBin, plan.filterCount, src.batchSize(),
			blockHashes,
		)
		if err != nil {
			return fmt.Errorf("import filter headers: %w", err)
		}

		filterTipHeight = plan.filterCount - 1
		err = readRecord(
			filterBin, int64(filterTipHeight), filterHeaderSize,
			filterTipHash[:],
		)
		if err != nil {
			return fmt.Errorf("read filter header tip: %w", err)
		}
	}

	if err := upsertTips(
		ctx, baseDB, plan, blockTipHash, blockTipHeight, filterTipHash,
		filterTipHeight,
	); err != nil {
		return fmt.Errorf("upsert chain tips: %w", err)
	}

	var numFilters int64
	if legacyDB != nil {
		numFilters, err = bw.importFilters(ctx, legacyDB)
		if err != nil {
			return fmt.Errorf("import filters: %w", err)
		}

		if err := importBans(ctx, baseDB, legacyDB); err != nil {
			return fmt.Errorf("import bans: %w", err)
		}
	} else {
		log.Infof("No legacy walletdb found; skipping filter and " +
			"ban import")
	}

	if err := verifyImport(
		ctx, baseDB, plan, blockTipHash, filterTipHash, numFilters,
	); err != nil {
		return fmt.Errorf("verify import: %w", err)
	}

	// The flat files are retired once the migration is recorded, see
	// finalize.
	i.retire = nil
	for _, p := range []string{blockBin, filterBin} {
		if fileExists(p) {
			i.retire = append(i.retire, p)
		}
	}

	log.Infof("Neutrino SQL legacy import complete in %v "+
		"(block tip=%s height=%d, filter tip=%s height=%d)",
		time.Since(start), blockTipHash, blockTipHeight,
		filterTipHash, filterTipHeight)

	return nil
}

// readRecord reads the idx'th recSize byte record of the file at path into
// dst.
func readRecord(path string, idx, recSize int64, dst []byte) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.ReadAt(dst[:recSize], idx*recSize)

	return err
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

// upsertTips writes the chain_tips rows for the header chains that were
// imported.
func upsertTips(ctx context.Context, baseDB *sqldbv2.BaseDB, plan legacyPlan,
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

	if plan.blockCount > 0 {
		err := q.UpsertChainTip(ctx, sqlc.UpsertChainTipParams{
			ChainType: ChainTypeBlock,
			TipHash:   blockHash[:],
			TipHeight: int64(blockHeight),
		})
		if err != nil {
			return err
		}
	}

	if plan.filterCount > 0 {
		err := q.UpsertChainTip(ctx, sqlc.UpsertChainTipParams{
			ChainType: ChainTypeFilter,
			TipHash:   filterHash[:],
			TipHeight: int64(filterHeight),
		})
		if err != nil {
			return err
		}
	}

	return tx.Commit()
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

// verifyImport runs sanity checks comparing the freshly populated SQL tables
// to what was read from the legacy state: row counts, chain tips and, when a
// walletdb was imported, the number of filters.
func verifyImport(ctx context.Context, baseDB *sqldbv2.BaseDB,
	plan legacyPlan, blockTipHash, filterTipHash chainhash.Hash,
	numFilters int64) error {

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
	if blockCount != int64(plan.blockCount) {
		return fmt.Errorf("block_headers row count %d != "+
			"expected %d", blockCount, plan.blockCount)
	}

	filterCount, err := q.CountFilterHeaders(ctx)
	if err != nil {
		return err
	}
	if filterCount != int64(plan.filterCount) {
		return fmt.Errorf("filter_headers row count %d != "+
			"expected %d", filterCount, plan.filterCount)
	}

	var storedFilters int64
	err = tx.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM regular_filters",
	).Scan(&storedFilters)
	if err != nil {
		return err
	}
	if storedFilters != numFilters {
		return fmt.Errorf("regular_filters row count %d != "+
			"expected %d", storedFilters, numFilters)
	}

	if plan.blockCount > 0 {
		blockTip, err := q.GetChainTip(ctx, ChainTypeBlock)
		if err != nil {
			return err
		}
		if !bytes.Equal(blockTip.TipHash, blockTipHash[:]) {
			return fmt.Errorf("block chain tip mismatch: got "+
				"%x, want %x", blockTip.TipHash,
				blockTipHash[:])
		}
	}

	if plan.filterCount > 0 {
		filterTip, err := q.GetChainTip(ctx, ChainTypeFilter)
		if err != nil {
			return err
		}
		if !bytes.Equal(filterTip.TipHash, filterTipHash[:]) {
			return fmt.Errorf("filter chain tip mismatch: got "+
				"%x, want %x", filterTip.TipHash,
				filterTipHash[:])
		}
	}

	return nil
}

// renameLegacyArtifacts renames the given legacy files to *.bak. Failure to
// rename is non-fatal for the caller (the import has already completed and is
// recorded in neutrino_migrations).
func renameLegacyArtifacts(paths ...string) error {
	var rerr error
	for _, p := range paths {
		if p == "" {
			continue
		}
		if _, err := os.Stat(p); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			rerr = errors.Join(rerr, err)
			continue
		}

		if err := os.Rename(p, p+backupSuffix); err != nil {
			rerr = errors.Join(rerr, fmt.Errorf("rename %s: %w",
				p, err))
		}
	}
	return rerr
}

// NewBackendWithLegacy opens the SQL backend like NewBackend and, on the first
// start, imports any legacy walletdb + flat-file state into it.
//
// Which legacy state is imported:
//   - explicit, when non-nil, is used as given (its DataDir and Params default
//     to dataDir and params).
//   - otherwise the legacy files are detected in dataDir under their default
//     names. A fresh install has none and nothing is imported.
//   - cfg.SkipLegacyMigration disables the import entirely. The migration
//     version is still recorded, so a later start without the flag will NOT
//     import; a warning is logged when legacy files are being skipped.
//
// The import runs inside migration version 2, so it only ever runs once. If
// it fails, the error is returned, the version stays unrecorded and the next
// start retries from scratch. Legacy flat files are renamed to *.bak only
// after the import has been recorded.
func NewBackendWithLegacy(dataDir string, cfg *Config,
	params *chaincfg.Params, explicit *LegacyDataSource) (*Backend, error) {

	if cfg != nil && cfg.SkipLegacyMigration {
		if DetectLegacyDataSource(dataDir, params) != nil {
			log.Warnf("SkipLegacyMigration set, legacy neutrino "+
				"files in %s will not be imported", dataDir)
		}

		return NewBackend(dataDir, cfg, nil)
	}

	src := explicit
	if src == nil {
		src = DetectLegacyDataSource(dataDir, params)
	}
	if src == nil {
		return NewBackend(dataDir, cfg, nil)
	}

	// Fill in the defaults on a copy so the caller's value is untouched.
	resolved := *src
	if resolved.DataDir == "" {
		resolved.DataDir = dataDir
	}
	if resolved.Params == nil {
		resolved.Params = params
	}

	imp := &legacyImporter{src: &resolved}
	backend, err := NewBackend(dataDir, cfg, imp.migrations())
	if err != nil {
		return nil, err
	}
	imp.finalize()

	// Files that are still around now were not imported by this start.
	if LegacyArtifactsExist(&resolved) && explicit == nil {
		log.Warnf("Legacy neutrino files in %s were not imported "+
			"because the SQL backend had already completed its "+
			"legacy import", dataDir)
	}

	return backend, nil
}
