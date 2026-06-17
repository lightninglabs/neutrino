package neutrino

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	"github.com/stretchr/testify/require"
)

func TestNewChainServiceUsesQuerySyncWorkManager(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(tempDir, "neutrino.db"),
		true, dbOpenTimeout,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	svc, err := NewChainService(Config{
		DataDir:     tempDir,
		ChainParams: chaincfg.SimNetParams,
		Database:    db,
	})
	require.NoError(t, err)
	require.NotNil(t, svc)

	workManagerType := reflect.TypeOf(svc.workManager)
	require.Equal(
		t, "github.com/lightninglabs/neutrino/querysync/workmanager",
		workManagerType.Elem().PkgPath(),
	)
}
