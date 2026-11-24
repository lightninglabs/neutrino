package neutrino

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightninglabs/neutrino/filterdb"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/sqldb"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"
)

// TestZeroedFilterTipRolledBack recreates the store from neutrino#451, where
// the filter headers at the tip are all zeros, and checks that the service
// rolls them back on start up, both when it runs on the flat files and when it
// imports them into the SQL store.
func TestZeroedFilterTipRolledBack(t *testing.T) {
	t.Parallel()

	for _, useSQL := range []bool{false, true} {
		name := "flat files"
		if useSQL {
			name = "sql import"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testZeroedFilterTipRolledBack(t, useSQL)
		})
	}
}

func testZeroedFilterTipRolledBack(t *testing.T, useSQL bool) {
	dataDir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(dataDir, sqldb.DefaultLegacyBoltFilename),
		true, time.Second*10, false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	params := chaincfg.SimNetParams
	blocks, err := headerfs.NewBlockHeaderStore(dataDir, db, &params)
	require.NoError(t, err)
	filters, err := headerfs.NewFilterHeaderStore(
		dataDir, db, headerfs.RegularFilter, &params, nil,
	)
	require.NoError(t, err)
	_, err = filterdb.New(db, params)
	require.NoError(t, err)

	// Write three blocks with their filter headers. The first filter
	// header is real, the two above it are zeroed, as a power loss leaves
	// them.
	prev := params.GenesisBlock.Header
	for height := uint32(1); height <= 3; height++ {
		header := wire.BlockHeader{
			Version:   prev.Version,
			PrevBlock: prev.BlockHash(),
			Timestamp: prev.Timestamp.Add(time.Minute),
			Bits:      prev.Bits,
		}
		require.NoError(t, blocks.WriteHeaders(headerfs.BlockHeader{
			BlockHeader: &header, Height: height,
		}))

		var filterHash chainhash.Hash
		if height == 1 {
			filterHash = chainhash.Hash{1}
		}
		require.NoError(t, filters.WriteHeaders(headerfs.FilterHeader{
			HeaderHash: header.BlockHash(),
			FilterHash: filterHash,
			Height:     height,
		}))

		prev = header
	}

	cfg := Config{DataDir: dataDir, ChainParams: params, Database: db}
	if useSQL {
		cfg.SQLConfig = &sqldb.Config{
			Backend: sqldb.BackendSqlite,
			Sqlite:  &sqldbv2.SqliteConfig{},
		}
	}
	service, err := NewChainService(cfg)
	require.NoError(t, err)
	if useSQL {
		t.Cleanup(func() {
			require.NoError(t, service.sqlBackend.Close())
		})
	}

	tip, height, err := service.RegFilterHeaders.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(1), height)
	require.Equal(t, chainhash.Hash{1}, *tip)

	// The block headers are left alone.
	_, blockHeight, err := service.BlockHeaders.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(3), blockHeight)
}

// TestZeroedFilterTipCorruptParent checks that the filter tip repair refuses
// to roll back onto a block header that's corrupt too, rather than leaving the
// filter tip pointing at a block the index doesn't know.
func TestZeroedFilterTipCorruptParent(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(dataDir, sqldb.DefaultLegacyBoltFilename),
		true, time.Second*10, false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	params := chaincfg.SimNetParams
	blocks, err := headerfs.NewBlockHeaderStore(dataDir, db, &params)
	require.NoError(t, err)
	filters, err := headerfs.NewFilterHeaderStore(
		dataDir, db, headerfs.RegularFilter, &params, nil,
	)
	require.NoError(t, err)

	prev := params.GenesisBlock.Header
	for height := uint32(1); height <= 2; height++ {
		header := wire.BlockHeader{
			Version:   prev.Version,
			PrevBlock: prev.BlockHash(),
			Timestamp: prev.Timestamp.Add(time.Minute),
			Bits:      prev.Bits,
		}
		require.NoError(t, blocks.WriteHeaders(headerfs.BlockHeader{
			BlockHeader: &header, Height: height,
		}))
		require.NoError(t, filters.WriteHeaders(headerfs.FilterHeader{
			HeaderHash: header.BlockHash(),
			Height:     height,
		}))
		prev = header
	}

	// Zero the block header at height 1 in the flat file, under the
	// zeroed filter header at height 2.
	blockFile, err := os.OpenFile(
		filepath.Join(dataDir, "block_headers.bin"), os.O_RDWR, 0644,
	)
	require.NoError(t, err)
	_, err = blockFile.WriteAt(make([]byte, 80), 80)
	require.NoError(t, err)
	require.NoError(t, blockFile.Close())

	// The block tip no longer links to the zeroed header below it, so the
	// start up check refuses before the filter repair gets to it.
	err = rollBackZeroedFilterTip(blocks, filters)
	require.ErrorContains(t, err, "block header tip at height 2 is corrupt")

	// The filter store is left untouched.
	_, height, err := filters.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(2), height)
}

// TestZeroedBlockTipRefusesToStart checks that a zeroed block header at the
// tip of the block store, which the filter repair can't roll back, stops the
// start up with an error that says how to recover.
func TestZeroedBlockTipRefusesToStart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(dataDir, sqldb.DefaultLegacyBoltFilename),
		true, time.Second*10, false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	params := chaincfg.SimNetParams
	blocks, err := headerfs.NewBlockHeaderStore(dataDir, db, &params)
	require.NoError(t, err)
	filters, err := headerfs.NewFilterHeaderStore(
		dataDir, db, headerfs.RegularFilter, &params, nil,
	)
	require.NoError(t, err)

	prev := params.GenesisBlock.Header
	for height := uint32(1); height <= 2; height++ {
		header := wire.BlockHeader{
			Version:   prev.Version,
			PrevBlock: prev.BlockHash(),
			Timestamp: prev.Timestamp.Add(time.Minute),
			Bits:      prev.Bits,
		}
		require.NoError(t, blocks.WriteHeaders(headerfs.BlockHeader{
			BlockHeader: &header, Height: height,
		}))
		prev = header
	}

	// Zero the block header at the tip, height 2, in the flat file.
	blockFile, err := os.OpenFile(
		filepath.Join(dataDir, "block_headers.bin"), os.O_RDWR, 0644,
	)
	require.NoError(t, err)
	_, err = blockFile.WriteAt(make([]byte, 80), 160)
	require.NoError(t, err)
	require.NoError(t, blockFile.Close())

	err = rollBackZeroedFilterTip(blocks, filters)
	require.ErrorContains(t, err, "block header tip at height 2 is corrupt")
	require.ErrorContains(t, err, "block_headers.bin")
}
