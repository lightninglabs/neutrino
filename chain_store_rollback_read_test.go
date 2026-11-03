package neutrino

import (
	"testing"
	"testing/synctest"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// lostRollbackReadFault fails the second tip read after a lost rollback
// acknowledgement.
type lostRollbackReadFault struct {
	headerfs.BlockHeaderStore
	armed bool
	reads int
}

func (s *lostRollbackReadFault) RollbackLastBlock() (
	*headerfs.BlockStamp, error) {

	tip, err := s.BlockHeaderStore.RollbackLastBlock()
	if err == nil && !s.armed {
		s.armed = true
		return nil, errDSTWrite
	}
	return tip, err
}

func (s *lostRollbackReadFault) ChainTip() (
	*wire.BlockHeader, uint32, error) {

	if s.armed {
		s.reads++
		if s.reads == 2 {
			return nil, 0, errCFHeaderRead
		}
	}
	return s.BlockHeaderStore.ChainTip()
}

func TestRollbackRecoveryReadRetainsWholeTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := chainStoreFixture(t, 3, 3)
		bm.cfg.BlockHeaders = &lostRollbackReadFault{
			BlockHeaderStore: blocks,
		}
		backend := chainstore.NewStore(&chainStoreHost{manager: bm})
		request := chainstore.Rollback{
			ExpectedTip: *bm.publishedBlockTip.Load(),
			Height:      0,
		}
		_, err := backend.Rollback(request)
		require.ErrorIs(t, err, errDSTWrite)
		_, err = backend.Snapshot()
		require.ErrorIs(t, err, errCFHeaderRead)
		reply, err := backend.Snapshot()
		require.NoError(t, err)
		synctest.Wait()
		t.Logf(
			"after healing read failure: durable/published "+
				"height=%d; pending=%t; disconnects=%d",
			reply.BlockTip.Height,
			backend.HasPendingRollback(),
			len(events),
		)
		require.Zero(
			t,
			reply.BlockTip.Height,
			"healed snapshot must finish the admitted rollback to "+
				"genesis",
		)
		require.Len(t, events, 3)
	})
}
