package neutrino

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/sqldb"
	sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"
)

// TestMissingServicesDisconnectWithoutBan checks that a peer whose version
// lacks compact filter support is disconnected, but not banned. A ban covers
// every port at the address, so it would also lock out a compatible node on
// the same host.
func TestMissingServicesDisconnectWithoutBan(t *testing.T) {
	t.Parallel()

	cs, err := NewChainService(Config{
		DataDir:     t.TempDir(),
		ChainParams: chaincfg.SimNetParams,
		SQLConfig: &sqldb.Config{
			Backend: sqldb.BackendSqlite,
			Sqlite:  &sqldbv2.SqliteConfig{},
		},
	})
	require.NoError(t, err)

	// The remote side of the handshake advertises only SFNodeNetwork.
	connected := swarmConnectedPeer(t, 1)
	sp := NewServerPeer(cs, false)
	sp.Peer = connected.Peer
	require.Zero(t, sp.Services()&wire.SFNodeCF)

	require.Nil(t, sp.OnVersion(nil, &wire.MsgVersion{
		Services:  sp.Services(),
		Timestamp: time.Now(),
	}))

	// The peer is dropped.
	require.Eventually(t, func() bool {
		return !sp.Connected()
	}, 5*time.Second, time.Millisecond)

	// Neither it nor another node at the same address is banned.
	require.False(t, cs.IsBanned(sp.Addr()))
	require.False(t, cs.IsBanned("127.0.0.1:18555"))
}
