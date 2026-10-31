package neutrino

import (
	"testing"
	"testing/synctest"

	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// TestChainStoreSuccessfulRollbackAndReorgAvoidReadback allows only the two
// reads before rollback begins.
func TestChainStoreSuccessfulRollbackAndReorgAvoidReadback(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "rollback"
		if replace {
			name = "reorg"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				bm, blocks, _, events := reviewStoreFixture(
					t,
					3,
					3,
				)
				reads := &chainStoreTipReadLimit{
					BlockHeaderStore: blocks,
					limit:            2,
				}
				bm.cfg.BlockHeaders = reads
				backend := &blockManagerChainStore{manager: bm}
				var reply chainStoreReply
				var err error
				if replace {
					fixture := dstChains()
					fork := fixture.fork[1]
					headers := []headerfs.BlockHeader{
						{
							BlockHeader: fork[0],
							Height:      2,
						},
						{
							BlockHeader: fork[1],
							Height:      3,
						},
					}
					tip := *bm.publishedBlockTip.Load()
					request := chainStoreReorg{
						ExpectedTip: tip,
						ForkHeight:  1,
						Headers:     headers,
					}
					reply, err = backend.reorg(request)
					require.Equal(
						t,
						fixture.fork[1][1].BlockHash(),
						reply.BlockTip.Hash,
					)
				} else {
					tip := *bm.publishedBlockTip.Load()
					request := chainStoreRollback{
						ExpectedTip: tip,
						Height:      1,
					}
					reply, err = backend.rollback(request)
					require.Equal(
						t,
						int32(1),
						reply.BlockTip.Height,
					)
				}
				require.NoError(t, err)
				require.True(t, reply.BlockTipValid)
				require.Equal(t, 2, reads.reads)
				require.Nil(t, backend.pending)
				synctest.Wait()
				require.Len(t, events, 2)
				require.Equal(t, uint32(3), (<-events).Height())
				require.Equal(t, uint32(2), (<-events).Height())
			})
		})
	}
}
