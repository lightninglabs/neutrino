package neutrino

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

type blockDNValidationStore struct {
	headerfs.BlockHeaderStore
	batches      []int
	failAncestor bool
}

func (s *blockDNValidationStore) WriteHeaders(h ...headerfs.BlockHeader) error {
	if err := s.BlockHeaderStore.WriteHeaders(h...); err != nil {
		return err
	}
	s.batches = append(s.batches, len(h))
	return nil
}

var errBlockDNAncestor = errors.New("ancestor read failed")

func (s *blockDNValidationStore) FetchHeaderByHeight(
	height uint32) (*wire.BlockHeader, error) {

	// The first retarget needs genesis, which has left the second batch's
	// validation cache. Allow startup reads before injecting that failure.
	if height == 0 && len(s.batches) != 0 && s.failAncestor {
		s.failAncestor = false
		return nil, errBlockDNAncestor
	}
	return s.BlockHeaderStore.FetchHeaderByHeight(height)
}

// TestBlockDNValidationAcrossBatches covers the shared header validator's
// store fallback at a retarget, recovery from an ancestor read failure, and
// bounded commits without redownloading a header file for each batch.
func TestBlockDNValidationAcrossBatches(t *testing.T) {
	const tip = 4001
	chain := newHTTPTestChain(t, tip)
	server := httptest.NewServer(chain)
	defer server.Close()
	for _, failAncestor := range []bool{false, true} {
		name := fmt.Sprint("ancestor failure=", failAncestor)
		t.Run(name, func(t *testing.T) {
			s := manualHTTPService(t, server.URL,
				func(cfg *Config) {
					// Keep the fixture's easy target while
					// still exercising retarget ancestors.
					params := &cfg.ChainParams
					params.RetargetAdjustmentFactor = 1
					params.ReduceMinDifficulty = false
				})
			store := &blockDNValidationStore{
				BlockHeaderStore: s.BlockHeaders,
				failAncestor:     failAncestor,
			}
			s.BlockHeaders = store
			chain.mu.Lock()
			clear(chain.requests)
			chain.mu.Unlock()
			err := s.blockDN.syncOnce(t.Context())
			if failAncestor {
				require.ErrorIs(t, err, errBlockDNAncestor)
				best, tipErr := s.BestBlock()
				require.NoError(t, tipErr)
				require.EqualValues(t, 2000, best.Height)
				err = s.blockDN.syncOnce(t.Context())
			}
			require.NoError(t, err)
			require.Equal(t, []int{2000, 2000, 1},
				store.batches)
			best, err := s.BestBlock()
			require.NoError(t, err)
			require.EqualValues(t, tip, best.Height)
			require.Equal(t, chain.blocks[tip].BlockHash(),
				best.Hash)
			for height := uint32(1); height <= tip; height++ {
				select {
				case n := <-s.blockDN.notifications:
					require.IsType(t,
						&blockntfns.Connected{}, n)
					require.Equal(t, height, n.Height())
				default:
					t.Fatalf("missing height %d", height)
				}
			}
			require.Empty(t, s.blockDN.notifications)
			requests := 1
			if failAncestor {
				requests++
			}
			chain.mu.Lock()
			defer chain.mu.Unlock()
			require.Equal(t, requests,
				chain.requests["/headers/0"])
			require.Equal(t, requests,
				chain.requests["/filter-headers/0"])
			require.Len(t, chain.requests, 3)
		})
	}
}
