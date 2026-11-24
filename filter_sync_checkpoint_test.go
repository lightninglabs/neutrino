package neutrino

import (
	"errors"
	"sync"
	"testing"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/filtersync"
	"github.com/lightninglabs/neutrino/headerfs"
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

// TestResolveConflictReportsStoreMismatch puts a wrong filter header in the
// store at the first checkpoint height, while every peer serves the honest
// checkpoints. No peer is at fault, so none may be banned or dropped, the
// store must be left as it is, and the error must say that it's the store
// the peers disagree with rather than a conflict among the peers.
func TestResolveConflictReportsStoreMismatch(t *testing.T) {
	t.Parallel()

	bm, hdrStore, cfStore, err := setupBlockManager(t)
	require.NoError(t, err)

	genesisBlockHeader, _, err := hdrStore.ChainTip()
	require.NoError(t, err)
	genesisFilterHeader, _, err := cfStore.ChainTip()
	require.NoError(t, err)
	headers, err := generateHeaders(
		genesisBlockHeader, genesisFilterHeader, nil,
	)
	require.NoError(t, err)
	require.NoError(t, hdrStore.WriteHeaders(headers.blockHeaders[1:]...))

	// The store holds the honest headers up to the first checkpoint, but
	// a wrong one at the checkpoint itself.
	bad := chainhash.Hash{0xba, 0xd}
	cf := make([]headerfs.FilterHeader, wire.CFCheckptInterval)
	copy(cf, headers.cfHeaders[1:wire.CFCheckptInterval+1])
	cf[len(cf)-1].FilterHash = bad
	require.NoError(t, cfStore.WriteHeaders(cf...))
	require.NoError(t, bm.ResetHeaderState())

	var acted []string
	bm.cfg.BanPeer = func(addr string, _ banman.Reason) error {
		acted = append(acted, "ban "+addr)
		return nil
	}
	bm.cfg.DisconnectPeer = func(addr string) error {
		acted = append(acted, "disconnect "+addr)
		return nil
	}
	bm.cfg.Broadcaster = newScriptedBroadcaster(nil)

	cps := headers.checkpoints
	good, err := bm.cfSync.resolveConflict(
		map[string][]*chainhash.Hash{
			"10.0.0.1:8333": cps, "10.0.0.2:8333": cps,
		}, cfStore, wire.GCSFilterRegular,
	)
	require.ErrorIs(t, err, errStoreDisagreesWithPeers)
	require.ErrorContains(t, err, bad.String())
	require.Empty(t, good)
	require.Empty(t, acted)

	tip, height, err := cfStore.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(wire.CFCheckptInterval), height)
	require.Equal(t, bad, *tip)
}

// TestCheckpointedFetchReorgDuringQuery reorgs the block store below the
// first checkpoint after the checkpointed fetch has checked its tip and sent
// its queries. The peers answer for the chain the queries were built from,
// which matches the checkpoints, so nobody may be banned, and the chain store
// must refuse to write headers for blocks it no longer holds. This is the
// window the tip check can't close, and it must fail safe.
func TestCheckpointedFetchReorgDuringQuery(t *testing.T) {
	t.Parallel()

	bm, hdrStore, cfStore, err := setupBlockManager(t)
	require.NoError(t, err)

	genesisBlockHeader, _, err := hdrStore.ChainTip()
	require.NoError(t, err)
	genesisFilterHeader, _, err := cfStore.ChainTip()
	require.NoError(t, err)
	headers, err := generateHeaders(
		genesisBlockHeader, genesisFilterHeader, nil,
	)
	require.NoError(t, err)
	require.NoError(t, hdrStore.WriteHeaders(headers.blockHeaders[1:]...))
	require.NoError(t, bm.ResetHeaderState())

	var (
		mu     sync.Mutex
		banned []string
	)
	bm.cfg.BanPeer = func(addr string, _ banman.Reason) error {
		mu.Lock()
		defer mu.Unlock()
		banned = append(banned, addr)
		return nil
	}

	const forkHeight = wire.CFCheckptInterval / 2
	bm.cfg.QueryDispatcher.(*mockDispatcher).query = func(
		requests []*querysyncwm.Request,
		_ ...querysyncwm.QueryOption) chan error {

		// The reorg lands once the queries are out. The fixture's
		// headers don't link by hash, so we cut the block store back
		// directly rather than through the chain store's rollback.
		_, tip, err := hdrStore.ChainTip()
		require.NoError(t, err)
		_, err = hdrStore.RollbackBlockHeaders(tip - forkHeight)
		require.NoError(t, err)

		var msgs []wire.Message
		for _, q := range requests {
			msgs = append(msgs, q.Req)
		}
		responses, err := generateResponses(msgs, headers)
		require.NoError(t, err)

		errChan := make(chan error, 1)
		go func() {
			for i := range requests {
				resp := *responses[i]
				requests[i].HandleResp(msgs[i], &resp, "honest")
			}
			errChan <- nil
		}()

		return errChan
	}

	bm.wg.Add(1)
	go bm.blockHandler()
	t.Cleanup(func() { require.NoError(t, bm.Stop()) })

	err = fetchCheckpointedAtTip(bm.cfSync, headers.checkpoints, cfStore)
	require.Error(t, err)

	mu.Lock()
	require.Empty(t, banned)
	mu.Unlock()

	_, height, err := cfStore.ChainTip()
	require.NoError(t, err)
	require.LessOrEqual(t, height, uint32(forkHeight))
}
