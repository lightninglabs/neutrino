package neutrino

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

func TestKnownPrefixDoesNotReadStoreBeforeActorWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := &chaincfg.SimNetParams
		blocks := newMemBlockHeaderStore(params)
		filters, err := newMemFilterHeaderStore(params)
		require.NoError(t, err)
		fixture := dstChains()
		for i := 1; i <= 2; i++ {
			require.NoError(
				t,
				blocks.WriteHeaders(headerfs.BlockHeader{
					BlockHeader: fixture.honest[i],
					Height:      uint32(i),
				}),
			)
		}
		readFault := &reviewRollbackParentRead{
			BlockHeaderStore: blocks,
			failHash:         fixture.honest[1].BlockHash(),
		}
		gate := &chainStoreGatedWrite{
			BlockHeaderStore: readFault,
			entered:          make(chan struct{}),
			release:          make(chan struct{}),
		}
		bm := newCFHeaderTestManager(t, gate, filters)
		bm.cfg.TimeSource = dstMedianTime{
			MedianTimeSource: bm.cfg.TimeSource,
			offset: time.Until(
				params.GenesisBlock.Header.Timestamp.Add(
					time.Hour,
				),
			),
		}
		bm.Start()
		t.Cleanup(func() { require.NoError(t, bm.Stop()) })
		defer close(gate.release)
		writer := newTestServerPeer(t, "127.0.0.1:18434", 3)
		spectator := newTestServerPeer(t, "127.0.0.1:18435", 3)
		bm.registerHeaderSyncPeer(spectator)
		bm.syncPeerMutex.Lock()
		bm.syncPeer = writer
		bm.syncPeerMutex.Unlock()
		message := wire.NewMsgHeaders()
		for _, header := range fixture.honest[1:4] {
			require.NoError(t, message.AddBlockHeader(header))
		}
		bm.QueueHeaders(message, writer)
		<-gate.entered
		require.False(t, readFault.failed,
			"known prefix should not read the store")
		bm.DonePeer(spectator)
		synctest.Wait()
		_, exists := bm.headerSync.Peer(spectator.Addr())
		require.False(
			t,
			exists,
			"known prefix must not send persistence back to the "+
				"handler",
		)
	})
}
