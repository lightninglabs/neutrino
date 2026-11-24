package neutrino

import (
	"errors"
	"sync"
	"testing"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/filtersync"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
	"github.com/stretchr/testify/require"
)

// TestCheckpointedFetchRefusesReplacedTip models a reorg that lands while a
// filter sync pass is between its cfcheckpt fetch and its checkpointed
// fetch. The checkpoints were served for the old tip, on the losing branch,
// and the block store now holds the winning branch below the first
// checkpoint. Every honest peer answers the checkpointed queries for the
// store's chain, which can't match those checkpoints, so the fetch must fail
// without banning anyone and leave the store as it is.
func TestCheckpointedFetchRefusesReplacedTip(t *testing.T) {
	t.Parallel()

	bm, hdrStore, cfStore, err := setupBlockManager(t)
	require.NoError(t, err)

	genesisBlockHeader, _, err := hdrStore.ChainTip()
	require.NoError(t, err)
	genesisFilterHeader, _, err := cfStore.ChainTip()
	require.NoError(t, err)

	// The winning chain is what the block store holds now.
	headers, err := generateHeaders(
		genesisBlockHeader, genesisFilterHeader, nil,
	)
	require.NoError(t, err)
	require.NoError(t, hdrStore.WriteHeaders(headers.blockHeaders[1:]...))
	require.NoError(t, bm.ResetHeaderState())

	// The pass read a tip on the losing branch, just past the first
	// checkpoint, and the peers served that branch's checkpoint for it.
	staleTip := chainhash.DoubleHashH([]byte("losing branch tip"))
	stale := chainhash.DoubleHashH([]byte("losing branch cfheader"))
	fetch := filtersync.FetchCheckpointed{
		FilterType:  wire.GCSFilterRegular,
		Checkpoints: []*chainhash.Hash{&stale},
		TipHash:     staleTip,
		TipHeight:   wire.CFCheckptInterval + 5,
	}

	var (
		mu      sync.Mutex
		banned  []string
		queried bool
	)
	bm.cfg.BanPeer = func(addr string, _ banman.Reason) error {
		mu.Lock()
		defer mu.Unlock()
		banned = append(banned, addr)
		return nil
	}

	// Each honest peer answers with the winning chain's headers.
	bm.cfg.QueryDispatcher.(*mockDispatcher).query = func(
		requests []*querysyncwm.Request,
		_ ...querysyncwm.QueryOption) chan error {

		mu.Lock()
		queried = true
		mu.Unlock()

		var msgs []wire.Message
		for _, q := range requests {
			msgs = append(msgs, q.Req)
		}
		responses, err := generateResponses(msgs, headers)
		require.NoError(t, err)

		errChan := make(chan error, 1)
		go func() {
			for _, peer := range []string{"honest-a", "honest-b"} {
				for i := range requests {
					resp := *responses[i]
					requests[i].HandleResp(
						msgs[i], &resp, peer,
					)
				}
			}
			errChan <- errors.New("no peers left")
		}()

		return errChan
	}

	err = bm.cfSync.getCheckpointedCFHeaders(fetch, cfStore)
	require.ErrorContains(t, err, "was replaced")

	mu.Lock()
	require.Empty(t, banned, "honest peers banned for serving the "+
		"current chain against checkpoints of a replaced tip")
	require.False(t, queried, "queries sent for a replaced tip")
	mu.Unlock()

	_, height, err := cfStore.ChainTip()
	require.NoError(t, err)
	require.Zero(t, height)
}
