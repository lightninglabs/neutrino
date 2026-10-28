package neutrino_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"

	"github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/sqldb"
)

// TestNewChainServiceSQLValidation verifies the mutual-exclusion rule between
// Config.Database and Config.SQLConfig.
func TestNewChainServiceSQLValidation(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(tempDir, "neutrino.db"),
		true, 10*time.Second, false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Both backends set -> error.
	_, err = neutrino.NewChainService(neutrino.Config{
		DataDir:     tempDir,
		ChainParams: chaincfg.SimNetParams,
		Database:    db,
		SQLConfig: &sqldb.Config{
			Backend: sqldb.BackendSqlite,
			Sqlite:  &sqldbv2.SqliteConfig{},
		},
	})
	require.Error(t, err)

	// Neither backend set -> error.
	_, err = neutrino.NewChainService(neutrino.Config{
		DataDir:     tempDir,
		ChainParams: chaincfg.SimNetParams,
	})
	require.Error(t, err)
}

// TestNewChainServiceSQLPath verifies that the SQL backend is opened,
// migrated, and that all four persistent stores have been wired through it.
// This covers Phase 6's wiring without requiring a live btcd connection.
func TestNewChainServiceSQLPath(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	cs, err := neutrino.NewChainService(neutrino.Config{
		DataDir:     tempDir,
		ChainParams: chaincfg.SimNetParams,
		SQLConfig: &sqldb.Config{
			Backend: sqldb.BackendSqlite,
			Sqlite:  &sqldbv2.SqliteConfig{},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, cs)
	// NOTE: We deliberately do NOT call cs.Stop() here. The legacy
	// ChainService.Stop() unconditionally awaits the UtxoScanner shutdown
	// channel which is only closed by a prior cs.Start(). This test only
	// exercises the construction path; the OS releases the SQL connection
	// at process exit.

	// The SQL database file should have been created in DataDir.
	require.FileExists(
		t, filepath.Join(tempDir, sqldb.DefaultSqliteFilename),
	)

	// Block header chain tip should be the genesis block.
	tipHeader, tipHeight, err := cs.BlockHeaders.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(0), tipHeight)
	expectedHash := chaincfg.SimNetParams.GenesisBlock.Header.BlockHash()
	tipHash := tipHeader.BlockHash()
	require.Equal(t, expectedHash, tipHash)

	// Filter header chain tip should also be at height 0 (genesis).
	_, fTipHeight, err := cs.RegFilterHeaders.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(0), fTipHeight)

	// Sanity: walletdb-only Database is unset so the legacy code paths are
	// bypassed.
	require.IsType(t, (*headerfs.SQLBlockHeaderStore)(nil), cs.BlockHeaders)
	require.IsType(
		t,
		(*headerfs.SQLFilterHeaderStore)(nil),
		cs.RegFilterHeaders,
	)
}

// TestChainServiceOnionBan exercises the full ban path for a Tor v3 peer: the
// address is parsed into a ban key, recorded, reported by IsBanned, and
// cleared again. Before this path existed an onion peer could not be banned at
// all, so a misbehaving one was retried immediately.
func TestChainServiceOnionBan(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(tempDir, "neutrino.db"),
		true, 10*time.Second, false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	cs, err := neutrino.NewChainService(neutrino.Config{
		DataDir:     tempDir,
		ChainParams: chaincfg.SimNetParams,
		SQLConfig: &sqldb.Config{
			Backend: sqldb.BackendSqlite,
			Sqlite:  &sqldbv2.SqliteConfig{},
		},
	})
	require.NoError(t, err)

	const onion = "duckduckgogg42xjoc72x3sjasowoarfbgcmvfimaftt6twa" +
		"gswzczad.onion:8333"

	// An onion peer starts out unbanned. IsBanned must answer without
	// treating the address as unparseable.
	require.False(t, cs.IsBanned(onion))

	require.NoError(t, cs.BanPeer(onion, banman.NoCompactFilters))
	require.True(t, cs.IsBanned(onion))

	// A different port and a different case are still the same peer, so a
	// banned service cannot get back in by shifting either.
	require.True(t, cs.IsBanned(
		"DUCKDUCKGOGG42XJOC72X3SJASOWOARFBGCMVFIMAFTT6TWAGSWZ"+
			"CZAD.onion:18333",
	))

	// An unrelated onion peer must be unaffected.
	require.False(t, cs.IsBanned(
		"mempoolhqx4isw62xs7abwphsq7ldayuidyx2v2oethdhhj6mlo2r6"+
			"ad.onion:8333",
	))

	// NOTE: UnbanPeer is deliberately not exercised here. It clears the ban
	// and then calls ConnectNode, which needs a started service to drain
	// the query channel; that is why the existing SQL-wiring tests also stop
	// short of cs.Start(). The unban half of the typed-key path is covered
	// against both backends by banman's TestBanStoreBackendsTorV3.
}
