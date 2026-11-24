package neutrino

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/sqldb"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// TestStartupRepairAfterTornWrites writes a chain to the flat file stores,
// then simulates a power loss in the middle of the last writes: any subset
// of the filter header records of the last batch comes back zeroed, and one
// block header record of the batch may be torn at any byte. After a restart,
// the start up repair must either refuse with an error, which it may only do
// for a block header that was damaged, or leave stores that hold no all-zero
// filter header, with the filter tip right below the lowest zeroed record
// and the block headers untouched.
func TestStartupRepairAfterTornWrites(t *testing.T) {
	t.Parallel()

	// Creating the index buckets dominates the cost of a case, so each
	// case starts from a copy of stores that hold only genesis.
	params := chaincfg.SimNetParams
	template := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(template, sqldb.DefaultLegacyBoltFilename),
		true, time.Second*10, false,
	)
	require.NoError(t, err)
	_, err = headerfs.NewBlockHeaderStore(template, db, &params)
	require.NoError(t, err)
	_, err = headerfs.NewFilterHeaderStore(
		template, db, headerfs.RegularFilter, &params, nil,
	)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	rapid.Check(t, func(rt *rapid.T) {
		testStartupRepairAfterTornWrites(t, rt, template)
	})
}

func testStartupRepairAfterTornWrites(t *testing.T, rt *rapid.T,
	template string) {

	dataDir, err := os.MkdirTemp(t.TempDir(), "repair")
	require.NoError(rt, err)
	require.NoError(rt, os.CopyFS(dataDir, os.DirFS(template)))
	db, err := walletdb.Open(
		"bdb", filepath.Join(dataDir, sqldb.DefaultLegacyBoltFilename),
		true, time.Second*10, false,
	)
	require.NoError(rt, err)
	defer func() { _ = db.Close() }()

	params := chaincfg.SimNetParams
	n := rapid.Uint32Range(1, 24).Draw(rt, "height")
	writeRepairChain(t, dataDir, db, &params, n)

	// Zero any subset of the filter records in the last batch.
	batch := rapid.Uint32Range(1, n).Draw(rt, "batch")
	var lowest uint32
	filterFile, err := os.OpenFile(
		filepath.Join(dataDir, "reg_filter_headers.bin"), os.O_RDWR,
		0644,
	)
	require.NoError(rt, err)
	for height := n - batch + 1; height <= n; height++ {
		if !rapid.Bool().Draw(rt, "zero_filter") {
			continue
		}
		_, err = filterFile.WriteAt(make([]byte, 32), int64(height)*32)
		require.NoError(rt, err)
		if lowest == 0 {
			lowest = height
		}
	}
	require.NoError(rt, filterFile.Close())

	// Maybe tear a block record of the last batch: zero it from some
	// byte on, or up to some byte.
	blockPath := filepath.Join(dataDir, "block_headers.bin")
	before, err := os.ReadFile(blockPath)
	require.NoError(rt, err)
	blockTorn := false
	if rapid.Bool().Draw(rt, "tear_block") {
		torn := rapid.Uint32Range(n-batch+1, n).Draw(rt, "torn_at")
		cut := rapid.IntRange(0, 79).Draw(rt, "tear_at")
		off, length := int64(torn)*80+int64(cut), 80-cut
		if rapid.Bool().Draw(rt, "tear_head") {
			off, length = int64(torn)*80, cut+1
		}
		blockFile, err := os.OpenFile(blockPath, os.O_RDWR, 0644)
		require.NoError(rt, err)
		_, err = blockFile.WriteAt(make([]byte, length), off)
		require.NoError(rt, err)
		require.NoError(rt, blockFile.Close())

		after, err := os.ReadFile(blockPath)
		require.NoError(rt, err)
		blockTorn = !bytes.Equal(before, after)
	}

	// Restart: reopen the stores and run the repair.
	blocks, err := headerfs.NewBlockHeaderStore(dataDir, db, &params)
	require.NoError(rt, err)
	filters, err := headerfs.NewFilterHeaderStore(
		dataDir, db, headerfs.RegularFilter, &params, nil,
	)
	require.NoError(rt, err)

	err = rollBackZeroedFilterHeaders(blocks, filters)
	if blockTorn {
		require.ErrorContains(rt, err, "block header")
		return
	}
	require.NoError(rt, err)

	want := n
	if lowest != 0 {
		want = lowest - 1
	}
	_, height, err := filters.ChainTip()
	require.NoError(rt, err)
	require.Equal(rt, want, height)
	for h := uint32(1); h <= height; h++ {
		header, err := filters.FetchHeaderByHeight(h)
		require.NoError(rt, err)
		require.NotEqual(rt, chainhash.Hash{}, *header, "height %d", h)
	}

	_, blockHeight, err := blocks.ChainTip()
	require.NoError(rt, err)
	require.Equal(rt, n, blockHeight)
}
