package neutrino

import (
	"testing"
	"testing/synctest"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

type reviewRollbackParentRead struct {
	headerfs.BlockHeaderStore
	failHash chainhash.Hash
	failed   bool
}

func (s *reviewRollbackParentRead) FetchHeader(
	hash *chainhash.Hash,
) (*wire.BlockHeader, uint32, error) {

	if *hash == s.failHash && !s.failed {
		s.failed = true
		return nil, 0, errCFHeaderRead
	}
	return s.BlockHeaderStore.FetchHeader(hash)
}

func TestReviewRollbackParentReadRetainsTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, blocks, _, events := reviewStoreFixture(t, 3, 3)
		bm.cfg.BlockHeaders = &reviewRollbackParentRead{
			BlockHeaderStore: blocks,
			failHash:         dstChains().honest[1].BlockHash(),
		}
		backend := &blockManagerChainStore{manager: bm}
		request := chainStoreRollback{
			ExpectedTip: *bm.publishedBlockTip.Load(),
			Height:      0,
		}
		progress, err := backend.rollback(request)
		require.ErrorIs(t, err, errCFHeaderRead)
		synctest.Wait()
		snapshot, snapshotErr := backend.snapshot()
		t.Logf(
			"partial=%d valid=%t pendingNil=%t snapshot=%d "+
				"snapshotErr=%v events=%d",
			progress.BlockTip.Height,
			progress.BlockTipValid,
			backend.pending == nil,
			snapshot.BlockTip.Height,
			snapshotErr,
			len(events),
		)
		require.NoError(t, snapshotErr)
		require.Zero(
			t,
			snapshot.BlockTip.Height,
			"snapshot must finish pending rollback before "+
				"reporting success",
		)
	})
}
