package sqldb_test

import (
	"bytes"
	"encoding/binary"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/btcsuite/btclog"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/stretchr/testify/require"

	"github.com/lightninglabs/neutrino/sqldb"
)

var (
	benchHeaders = flag.Int(
		"legacybench.headers", 900_000, "number of synthetic block "+
			"and filter headers in BenchmarkLegacyImport",
	)

	benchLog = flag.Bool(
		"legacybench.log", false, "log the import's progress",
	)

	benchFilters = flag.Int(
		"legacybench.filters", 20_000, "number of synthetic regular "+
			"filters in BenchmarkLegacyImport",
	)
)

const (
	// benchFilterSize is the size of every synthetic GCS filter.
	benchFilterSize = 2048

	// benchBans is the number of synthetic active bans.
	benchBans = 100
)

// writeSyntheticLegacy writes a linked chain of numHeaders block headers, a
// matching run of filter headers, numFilters filters, and a few bans into dir
// using the exact legacy on-disk layout.
func writeSyntheticLegacy(tb testing.TB, dir string, params *chaincfg.Params,
	numHeaders, numFilters int) {

	tb.Helper()

	var blockBuf, filterBuf bytes.Buffer
	blockBuf.Grow(numHeaders * 80)
	filterBuf.Grow(numHeaders * 32)

	hashes := make([]chainhash.Hash, numHeaders)
	hdr := params.GenesisBlock.Header
	for i := 0; i < numHeaders; i++ {
		if i > 0 {
			hdr = wire.BlockHeader{
				Version:   1,
				PrevBlock: hashes[i-1],
				Timestamp: params.GenesisBlock.Header.
					Timestamp.Add(
					time.Duration(i) * time.Minute,
				),
				Bits:  0x1d00ffff,
				Nonce: uint32(i),
			}
			binary.LittleEndian.PutUint32(
				hdr.MerkleRoot[:], uint32(i),
			)
		}
		require.NoError(tb, hdr.Serialize(&blockBuf))
		hashes[i] = hdr.BlockHash()

		fh := chainhash.HashH(hashes[i][:])
		filterBuf.Write(fh[:])
	}
	require.NoError(tb, os.WriteFile(
		filepath.Join(dir, sqldb.DefaultBlockHeadersFilename),
		blockBuf.Bytes(), 0o600,
	))
	require.NoError(tb, os.WriteFile(
		filepath.Join(dir, sqldb.DefaultRegFilterHeadersFilename),
		filterBuf.Bytes(), 0o600,
	))

	db, err := walletdb.Create(
		"bdb", filepath.Join(dir, "neutrino.db"), true,
		10*time.Second, false,
	)
	require.NoError(tb, err)
	defer db.Close()

	filter := bytes.Repeat([]byte{0xAB}, benchFilterSize)
	err = walletdb.Update(db, func(tx walletdb.ReadWriteTx) error {
		root, err := tx.CreateTopLevelBucket([]byte("filter-store"))
		if err != nil {
			return err
		}
		reg, err := root.CreateBucket([]byte("regular"))
		if err != nil {
			return err
		}

		stride := numHeaders / max(numFilters, 1)
		for i := 0; i < numFilters; i++ {
			h := hashes[(i*max(stride, 1))%numHeaders]
			if err := reg.Put(h[:], filter); err != nil {
				return err
			}
		}

		return nil
	})
	require.NoError(tb, err)

	// Active bans. The importer copies the opaque ban key verbatim.
	err = walletdb.Update(db, func(tx walletdb.ReadWriteTx) error {
		root, err := tx.CreateTopLevelBucket([]byte("ban-store"))
		if err != nil {
			return err
		}
		banIdx, err := root.CreateBucket([]byte("ban-index"))
		if err != nil {
			return err
		}
		reasonIdx, err := root.CreateBucket([]byte("reason-index"))
		if err != nil {
			return err
		}

		expiry := make([]byte, 8)
		binary.BigEndian.PutUint64(
			expiry, uint64(time.Now().Add(time.Hour).Unix()),
		)
		for i := 0; i < benchBans; i++ {
			key := []byte{4, 10, 0, byte(i >> 8), byte(i), 0xff}
			if err := banIdx.Put(key, expiry); err != nil {
				return err
			}
			if err := reasonIdx.Put(key, []byte{1}); err != nil {
				return err
			}
		}

		return nil
	})
	require.NoError(tb, err)
}

// copyFile copies src to dst.
func copyFile(tb testing.TB, src, dst string) {
	tb.Helper()

	in, err := os.Open(src)
	require.NoError(tb, err)
	defer in.Close()
	out, err := os.Create(dst)
	require.NoError(tb, err)
	_, err = io.Copy(out, in)
	require.NoError(tb, err)
	require.NoError(tb, out.Close())
}

// BenchmarkLegacyImport measures the full legacy flat-file + walletdb import
// into a fresh SQLite backend (or PostgreSQL when NEUTRINO_LEGACY_TEST_PG_DSN
// is set) using a synthetic mainnet-sized chain. The
// chain size is tunable with -legacybench.headers and -legacybench.filters.
func BenchmarkLegacyImport(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large legacy import benchmark in -short")
	}

	if *benchLog {
		backend := btclog.NewBackend(os.Stdout)
		logger := backend.Logger("SQLD")
		logger.SetLevel(btclog.LevelInfo)
		sqldb.UseLogger(logger)
	}

	params := &chaincfg.MainNetParams
	tmpl := b.TempDir()
	writeSyntheticLegacy(b, tmpl, params, *benchHeaders, *benchFilters)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		for _, name := range []string{
			sqldb.DefaultBlockHeadersFilename,
			sqldb.DefaultRegFilterHeadersFilename, "neutrino.db",
		} {
			copyFile(
				b, filepath.Join(tmpl, name),
				filepath.Join(dir, name),
			)
		}
		b.StartTimer()

		backend := runBenchImport(b, dir, params)

		b.StopTimer()
		require.NoError(b, backend.Close())
		b.StartTimer()
	}
}

// runBenchImport opens a SQLite backend over dir, importing the legacy state.
func runBenchImport(b *testing.B, dir string,
	params *chaincfg.Params) *sqldb.Backend {

	b.Helper()

	cfg := configFor(b, dir)

	// No LegacyDataSource: the files in dir are detected automatically.
	backend, err := sqldb.NewBackendWithLegacy(dir, cfg, params, nil)
	require.NoError(b, err)

	return backend
}
