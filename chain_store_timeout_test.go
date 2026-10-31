package neutrino

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueuedRangeReplyHeldCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeCheckpointAll})
		var first, second dstHeaderRequest
		for _, req := range d.transport.queued {
			if req.start == 0 {
				first = req
			}
			if req.start == 2 {
				second = req
			}
		}
		require.NotNil(t, first.peer)
		require.NotNil(t, second.peer)
		require.NotSame(t, first.peer, second.peer)
		gate := &chainStoreGatedWrite{
			BlockHeaderStore: d.store,
			entered:          make(chan struct{}),
			release:          make(chan struct{}),
		}
		d.bm.cfg.BlockHeaders = gate
		d.bm.Start()
		defer func() { require.NoError(t, d.bm.Stop()) }()
		d.bm.QueueHeaders(d.answer(first, dstReplyHonest), first.peer)
		<-gate.entered
		d.bm.QueueHeaders(d.answer(second, dstReplyHonest), second.peer)
		synctest.Wait()
		require.False(t, peerDisconnected(second.peer))
		time.Sleep(dstRequestTimeout + time.Millisecond)
		synctest.Wait()
		require.False(
			t,
			peerDisconnected(second.peer),
			"timely queued reply was disconnected during local "+
				"write",
		)
		close(gate.release)
		synctest.Wait()
		_, height, err := d.store.ChainTip()
		require.NoError(t, err)
		require.GreaterOrEqual(t, height, second.stop)
		require.False(t, peerDisconnected(second.peer))
	})
}
