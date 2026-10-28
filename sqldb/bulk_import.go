package sqldb

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
)

const (
	// importChunkRows is the number of records the import pipeline reads,
	// hashes and hands to the writer as one unit, and the number of rows
	// per bulk INSERT. Larger statements amortize the per-statement setup
	// cost; gains flatten out beyond this size while the pipeline memory,
	// a few chunks of at most 2.5 MiB each, keeps growing.
	importChunkRows = 32_768

	// maxHashWorkers caps the number of goroutines hashing block headers.
	// Double SHA-256 of 80 bytes is cheap enough that the SQL writer is
	// the bottleneck long before this many workers are busy.
	maxHashWorkers = 4

	// filterStmtRows is the number of filters written per INSERT statement.
	filterStmtRows = 64

	// filterTxMaxRows and filterTxMaxBytes bound a single filter import
	// transaction by row count and payload size, whichever is hit first.
	filterTxMaxRows  = 10_000
	filterTxMaxBytes = 64 << 20
)

// The bulk header statements take a start height, a blob of concatenated block
// hashes, a blob of concatenated 80 byte headers (or 32 byte filter hashes)
// and the row count. The server splits the blobs into rows itself, so one
// statement inserts a whole chunk with four bound parameters instead of three
// per row, which removes the driver's per-row binding cost.
const (
	sqliteBlockHeadersBulk = `
WITH RECURSIVE seq(i) AS (
	SELECT 0 UNION ALL SELECT i + 1 FROM seq WHERE i < ?4 - 1
)
INSERT INTO block_headers (height, block_hash, raw_header)
SELECT ?1 + i, substr(?2, i * 32 + 1, 32), substr(?3, i * 80 + 1, 80)
FROM seq`

	sqliteFilterHeadersBulk = `
WITH RECURSIVE seq(i) AS (
	SELECT 0 UNION ALL SELECT i + 1 FROM seq WHERE i < ?4 - 1
)
INSERT INTO filter_headers (height, block_hash, filter_hash)
SELECT ?1 + i, substr(?2, i * 32 + 1, 32), substr(?3, i * 32 + 1, 32)
FROM seq`

	postgresBlockHeadersBulk = `
INSERT INTO block_headers (height, block_hash, raw_header)
SELECT $1::bigint + i,
	substring($2::bytea FROM i * 32 + 1 FOR 32),
	substring($3::bytea FROM i * 80 + 1 FOR 80)
FROM generate_series(0, $4::int - 1) AS i`

	postgresFilterHeadersBulk = `
INSERT INTO filter_headers (height, block_hash, filter_hash)
SELECT $1::bigint + i,
	substring($2::bytea FROM i * 32 + 1 FOR 32),
	substring($3::bytea FROM i * 32 + 1 FOR 32)
FROM generate_series(0, $4::int - 1) AS i`

	insertFilterPrefix = `INSERT INTO regular_filters
(block_hash, filter_bytes) VALUES `
)

// bulkWriter writes the legacy state into the SQL tables using the fastest
// path available for the backend dialect.
type bulkWriter struct {
	baseDB *sqldbv2.BaseDB

	blockHeadersSQL  string
	filterHeadersSQL string

	// filterSQL caches the multi-row filter INSERT per row count.
	filterSQL map[int]string
}

// newBulkWriter returns a bulkWriter for baseDB's dialect.
func newBulkWriter(baseDB *sqldbv2.BaseDB) *bulkWriter {
	w := &bulkWriter{
		baseDB:           baseDB,
		blockHeadersSQL:  sqliteBlockHeadersBulk,
		filterHeadersSQL: sqliteFilterHeadersBulk,
		filterSQL:        make(map[int]string),
	}
	if baseDB.BackendType == sqldbv2.BackendTypePostgres {
		w.blockHeadersSQL = postgresBlockHeadersBulk
		w.filterHeadersSQL = postgresFilterHeadersBulk
	}

	return w
}

// fileChunk is one read unit of a legacy flat file.
type fileChunk struct {
	// start is the height of the first record in buf.
	start uint32

	// buf holds the raw records.
	buf []byte

	// done is closed once any per-chunk processing has finished.
	done chan struct{}
}

// chunkReader reads a flat file in order, optionally fanning each chunk out
// to worker goroutines, and hands the chunks to a single consumer in file
// order. The reading and processing overlap with the consumer's SQL writes.
type chunkReader struct {
	chunks chan *fileChunk
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// err is the read error, valid once chunks is closed and wg is done.
	err error
}

// startChunkReader starts reading count records of recSize bytes from f.
// process, when non-nil, runs on a worker for every chunk before the chunk is
// released to the consumer through the chunks channel.
func startChunkReader(ctx context.Context, f *os.File, recSize int,
	count uint32, process func(*fileChunk)) *chunkReader {

	// The derived context only stops the reader; SQL work keeps using the
	// caller's context so stopping the reader cannot abort a transaction.
	ctx, cancel := context.WithCancel(ctx)
	workers := min(runtime.GOMAXPROCS(0), maxHashWorkers)
	r := &chunkReader{
		chunks: make(chan *fileChunk, 2),
		cancel: cancel,
	}

	work := make(chan *fileChunk, workers)
	for i := 0; process != nil && i < workers; i++ {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			for c := range work {
				process(c)
				close(c.done)
			}
		}()
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer close(r.chunks)
		defer close(work)

		step := uint32(importChunkRows)
		for start := uint32(0); start < count; start += step {
			n := min(step, count-start)
			c := &fileChunk{
				start: start,
				buf:   make([]byte, int(n)*recSize),
				done:  make(chan struct{}),
			}

			_, err := f.ReadAt(c.buf, int64(start)*int64(recSize))
			if err != nil {
				r.err = fmt.Errorf("read %s at record %d: %w",
					f.Name(), start, err)
				return
			}

			// The chunk goes to the consumer queue first so the
			// consumer sees chunks in file order no matter which
			// worker finishes first.
			select {
			case r.chunks <- c:
			case <-ctx.Done():
				return
			}

			if process == nil {
				close(c.done)
				continue
			}

			select {
			case work <- c:
			case <-ctx.Done():
				return
			}
		}
	}()

	return r
}

// stop cancels the reader, waits for its goroutines and returns any read
// error. It is safe to call more than once.
func (r *chunkReader) stop() error {
	r.cancel()
	r.wg.Wait()

	return r.err
}

// txWindow groups bulk writes into transactions of at least maxRows rows.
type txWindow struct {
	baseDB  *sqldbv2.BaseDB
	maxRows int

	tx   *sql.Tx
	rows int
}

// begin returns the open transaction, starting one if needed.
func (t *txWindow) begin(ctx context.Context) (*sql.Tx, error) {
	if t.tx != nil {
		return t.tx, nil
	}

	tx, err := t.baseDB.BeginTx(ctx, sqldbv2.WriteTxOpt())
	if err != nil {
		return nil, err
	}
	t.tx = tx

	return tx, nil
}

// wrote accounts for n written rows and commits once the window is full.
func (t *txWindow) wrote(n int) error {
	t.rows += n
	if t.rows < t.maxRows {
		return nil
	}

	return t.commit()
}

// commit commits the open transaction, if any.
func (t *txWindow) commit() error {
	if t.tx == nil {
		return nil
	}

	err := t.tx.Commit()
	t.tx, t.rows = nil, 0

	return err
}

// rollback discards the open transaction, if any.
func (t *txWindow) rollback() {
	if t.tx != nil {
		_ = t.tx.Rollback()
		t.tx, t.rows = nil, 0
	}
}

// importBlockHeaders streams block_headers.bin into the block_headers table.
// Reading and double-hashing the headers happens on separate goroutines ahead
// of the single SQL writer. It returns the concatenated block hashes of every
// height, which the filter header import uses to key its rows.
//
// The chain is verified while it is written: the first header must be the
// genesis block of params (when set) and every header must commit to the hash
// of its predecessor.
func (w *bulkWriter) importBlockHeaders(ctx context.Context, path string,
	count uint32, rowsPerTx int, params *chaincfg.Params) ([]byte, error) {

	start := time.Now()

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	hashes := make([]byte, int(count)*chainhash.HashSize)
	hashChunk := func(c *fileChunk) {
		n := len(c.buf) / blockHeaderSize
		for i := 0; i < n; i++ {
			rec := c.buf[i*blockHeaderSize : (i+1)*blockHeaderSize]
			h := chainhash.DoubleHashH(rec)
			off := (int(c.start) + i) * chainhash.HashSize
			copy(hashes[off:], h[:])
		}
	}

	reader := startChunkReader(
		ctx, f, blockHeaderSize, count, hashChunk,
	)
	defer func() { _ = reader.stop() }()

	win := &txWindow{baseDB: w.baseDB, maxRows: rowsPerTx}
	defer win.rollback()

	written := uint32(0)
	for c := range reader.chunks {
		select {
		case <-c.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		n := len(c.buf) / blockHeaderSize
		err := verifyBlockChunk(c, n, hashes, params)
		if err != nil {
			return nil, err
		}

		tx, err := win.begin(ctx)
		if err != nil {
			return nil, err
		}

		lo := int(c.start) * chainhash.HashSize
		hi := lo + n*chainhash.HashSize
		_, err = tx.ExecContext(
			ctx, w.blockHeadersSQL, int64(c.start), hashes[lo:hi],
			c.buf, int64(n),
		)
		if err != nil {
			return nil, fmt.Errorf("insert block headers at "+
				"height %d: %w", c.start, err)
		}

		if err := win.wrote(n); err != nil {
			return nil, err
		}
		written += uint32(n)
	}
	if err := reader.stop(); err != nil {
		return nil, err
	}
	if written != count {
		return nil, fmt.Errorf("wrote %d of %d block headers", written,
			count)
	}
	if err := win.commit(); err != nil {
		return nil, err
	}

	log.Infof("Imported %d block headers in %v (tip %s)", count,
		time.Since(start).Round(time.Millisecond),
		chainhash.Hash(hashes[len(hashes)-chainhash.HashSize:]))

	return hashes, nil
}

// verifyBlockChunk checks that the n headers of c form a chain that continues
// the already hashed headers before it, and that height zero is the genesis
// block of params when params is set.
func verifyBlockChunk(c *fileChunk, n int, hashes []byte,
	params *chaincfg.Params) error {

	for i := 0; i < n; i++ {
		height := int(c.start) + i
		hash := hashes[height*chainhash.HashSize : (height+1)*
			chainhash.HashSize]

		if height == 0 {
			if params != nil && params.GenesisHash != nil &&
				!bytes.Equal(hash, params.GenesisHash[:]) {

				return fmt.Errorf("first block header %x is "+
					"not the genesis block %s of the "+
					"configured chain", hash,
					params.GenesisHash)
			}

			continue
		}

		rec := c.buf[i*blockHeaderSize:]
		prev := rec[prevBlockOffset:][:chainhash.HashSize]
		want := hashes[(height-1)*chainhash.HashSize : height*
			chainhash.HashSize]
		if !bytes.Equal(prev, want) {
			return fmt.Errorf("block header at height %d does "+
				"not connect to the header before it (prev "+
				"%x, want %x)", height, prev, want)
		}
	}

	return nil
}

// importFilterHeaders streams reg_filter_headers.bin into the filter_headers
// table. blockHashes is the concatenated block hash of every height, as
// returned by importBlockHeaders; the flat file itself only stores filter
// hashes.
func (w *bulkWriter) importFilterHeaders(ctx context.Context, path string,
	count uint32, rowsPerTx int, blockHashes []byte) error {

	start := time.Now()

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := startChunkReader(
		ctx, f, filterHeaderSize, count, nil,
	)
	defer func() { _ = reader.stop() }()

	win := &txWindow{baseDB: w.baseDB, maxRows: rowsPerTx}
	defer win.rollback()

	written := uint32(0)
	for c := range reader.chunks {
		n := len(c.buf) / filterHeaderSize
		tx, err := win.begin(ctx)
		if err != nil {
			return err
		}

		lo := int(c.start) * chainhash.HashSize
		hi := lo + n*chainhash.HashSize
		_, err = tx.ExecContext(
			ctx, w.filterHeadersSQL, int64(c.start),
			blockHashes[lo:hi], c.buf, int64(n),
		)
		if err != nil {
			return fmt.Errorf("insert filter headers at height "+
				"%d: %w", c.start, err)
		}

		if err := win.wrote(n); err != nil {
			return err
		}
		written += uint32(n)
	}
	if err := reader.stop(); err != nil {
		return err
	}
	if written != count {
		return fmt.Errorf("wrote %d of %d filter headers", written,
			count)
	}
	if err := win.commit(); err != nil {
		return err
	}

	log.Infof("Imported %d filter headers in %v", count,
		time.Since(start).Round(time.Millisecond))

	return nil
}

// multiRowInsertSQL returns prefix followed by rows value tuples of two
// numbered placeholders each.
func multiRowInsertSQL(prefix string, rows int) string {
	var b strings.Builder
	b.WriteString(prefix)
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("($")
		b.WriteString(strconv.Itoa(2*i + 1))
		b.WriteString(", $")
		b.WriteString(strconv.Itoa(2*i + 2))
		b.WriteByte(')')
	}

	return b.String()
}

// importFilters streams every regular filter from the legacy walletdb
// filter-store/regular bucket into the regular_filters table and returns how
// many it copied. Filters are written straight out of the bbolt read
// transaction in multi-row statements, so memory use does not depend on the
// number of filters.
func (w *bulkWriter) importFilters(ctx context.Context,
	legacyDB walletdb.DB) (int64, error) {

	start := time.Now()

	var (
		total   int64
		txBytes int
		args    = make([]any, 0, 2*filterStmtRows)
		win     = &txWindow{
			baseDB: w.baseDB, maxRows: filterTxMaxRows,
		}
	)
	defer win.rollback()

	// flush executes the buffered rows as one multi-row INSERT.
	flush := func() error {
		rows := len(args) / 2
		if rows == 0 {
			return nil
		}

		tx, err := win.begin(ctx)
		if err != nil {
			return err
		}

		query, ok := w.filterSQL[rows]
		if !ok {
			query = multiRowInsertSQL(insertFilterPrefix, rows)
			w.filterSQL[rows] = query
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}
		args = args[:0]

		return win.wrote(rows)
	}

	err := walletdb.View(legacyDB, func(tx walletdb.ReadTx) error {
		root := tx.ReadBucket(legacyFilterStoreBucket)
		if root == nil {
			return nil
		}
		reg := root.NestedReadBucket(legacyRegFilterBucket)
		if reg == nil {
			return nil
		}

		err := reg.ForEach(func(k, v []byte) error {
			if len(k) != chainhash.HashSize {
				return fmt.Errorf("filter key %x has length "+
					"%d, want %d", k, len(k),
					chainhash.HashSize)
			}

			// The legacy store keeps a "nil filter" sentinel as a
			// nil value. The filter_bytes column is NOT NULL, so
			// bind it as an empty blob; FetchFilter still treats
			// zero-length as the sentinel on read. The key and
			// value stay valid until the bbolt transaction ends,
			// and flush runs before then, so nothing is copied.
			if v == nil {
				v = []byte{}
			}
			args = append(args, k, v)
			total++
			txBytes += len(v)

			if len(args) == 2*filterStmtRows {
				if err := flush(); err != nil {
					return err
				}
			}

			// Close the SQL transaction once it is large enough.
			if txBytes >= filterTxMaxBytes {
				if err := flush(); err != nil {
					return err
				}
				if err := win.commit(); err != nil {
					return err
				}
			}

			// A commit, by size or by row count, starts a new
			// transaction window.
			if win.tx == nil {
				txBytes = 0
			}

			return nil
		})
		if err != nil {
			return err
		}

		return flush()
	})
	if err != nil {
		return 0, err
	}
	if err := win.commit(); err != nil {
		return 0, err
	}

	log.Infof("Imported %d regular filters in %v", total,
		time.Since(start).Round(time.Millisecond))

	return total, nil
}
