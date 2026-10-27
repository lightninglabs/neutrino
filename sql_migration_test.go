package neutrino

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/filterdb"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/sqldb"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"
)

func TestSkipLegacyMigration(t *testing.T) {
	t.Parallel()

	legacyDir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(legacyDir, "legacy.db"),
		true, time.Second*10, false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	params := chaincfg.SimNetParams
	blocks, err := headerfs.NewBlockHeaderStore(legacyDir, db, &params)
	require.NoError(t, err)
	header := params.GenesisBlock.Header
	header.PrevBlock = header.BlockHash()
	header.Nonce++
	require.NoError(t, blocks.WriteHeaders(headerfs.BlockHeader{
		BlockHeader: &header, Height: 1,
	}))
	filters, err := headerfs.NewFilterHeaderStore(
		legacyDir, db, headerfs.RegularFilter, &params, nil,
	)
	require.NoError(t, err)
	require.NoError(t, filters.WriteHeaders(headerfs.FilterHeader{
		HeaderHash: header.BlockHash(),
		FilterHash: chainhash.Hash{1},
		Height:     1,
	}))
	_, err = filterdb.New(db, params)
	require.NoError(t, err)
	_, err = banman.NewStore(db)
	require.NoError(t, err)

	service, err := NewChainService(Config{
		DataDir: t.TempDir(), ChainParams: params,
		SQLConfig: &sqldb.Config{
			Backend:             sqldb.BackendSqlite,
			Sqlite:              &sqldbv2.SqliteConfig{},
			SkipLegacyMigration: true,
		},
		LegacyImport: &sqldb.LegacyDataSource{
			DB: db, DataDir: legacyDir, Params: &params,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.sqlBackend.Close()) })
	_, blockHeight, err := service.BlockHeaders.ChainTip()
	require.NoError(t, err)
	require.Zero(t, blockHeight)
	_, filterHeight, err := service.RegFilterHeaders.ChainTip()
	require.NoError(t, err)
	require.Zero(t, filterHeight)
	require.FileExists(t, filepath.Join(
		legacyDir, sqldb.DefaultBlockHeadersFilename,
	))
	require.FileExists(t, filepath.Join(
		legacyDir, sqldb.DefaultRegFilterHeadersFilename,
	))
}

// TestAutoLegacyImport verifies that NewChainService with only SQLConfig set
// imports the legacy flat files and walletdb found in DataDir, without any
// LegacyImport being declared.
func TestAutoLegacyImport(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(dataDir, sqldb.DefaultLegacyBoltFilename),
		true, time.Second*10, false,
	)
	require.NoError(t, err)
	params := chaincfg.SimNetParams
	blocks, err := headerfs.NewBlockHeaderStore(dataDir, db, &params)
	require.NoError(t, err)
	header := params.GenesisBlock.Header
	header.PrevBlock = header.BlockHash()
	header.Nonce++
	require.NoError(t, blocks.WriteHeaders(headerfs.BlockHeader{
		BlockHeader: &header, Height: 1,
	}))
	filters, err := headerfs.NewFilterHeaderStore(
		dataDir, db, headerfs.RegularFilter, &params, nil,
	)
	require.NoError(t, err)
	require.NoError(t, filters.WriteHeaders(headerfs.FilterHeader{
		HeaderHash: header.BlockHash(),
		FilterHash: chainhash.Hash{1},
		Height:     1,
	}))
	_, err = filterdb.New(db, params)
	require.NoError(t, err)

	// The legacy owner must release the walletdb before the upgrade.
	require.NoError(t, db.Close())

	service, err := NewChainService(Config{
		DataDir: dataDir, ChainParams: params,
		SQLConfig: &sqldb.Config{
			Backend: sqldb.BackendSqlite,
			Sqlite:  &sqldbv2.SqliteConfig{},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.sqlBackend.Close()) })

	tip, blockHeight, err := service.BlockHeaders.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(1), blockHeight)
	require.Equal(t, header.BlockHash(), tip.BlockHash())
	_, filterHeight, err := service.RegFilterHeaders.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(1), filterHeight)
	require.FileExists(t, filepath.Join(
		dataDir, sqldb.DefaultBlockHeadersFilename+".bak",
	))
}
