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
	"pgregory.net/rapid"
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
	require.ErrorContains(t, err, "couldn't write cfheaders")

	mu.Lock()
	require.Empty(t, banned)
	mu.Unlock()

	_, height, err := cfStore.ChainTip()
	require.NoError(t, err)
	require.LessOrEqual(t, height, uint32(forkHeight))
}

// TestResolveConflictBelowBlockTip has two peers disagree at a checkpoint
// above the block header tip, as after a reorg that shortened the chain. The
// follow-up cfheaders query can't be planned then, and that local failure
// must come back as an error, not as peers that didn't answer: dropping them
// would disconnect every peer that served checkpoints.
func TestResolveConflictBelowBlockTip(t *testing.T) {
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

	// The block tip sits between the second and third checkpoints.
	tip := 2*wire.CFCheckptInterval + wire.CFCheckptInterval/2
	require.NoError(t, hdrStore.WriteHeaders(
		headers.blockHeaders[1:tip+1]...,
	))
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

	// The lists agree on the first three checkpoints and differ at the
	// fourth, so the follow-up query starts above the block tip.
	cps := headers.checkpoints
	other := chainhash.Hash{0x0f}
	good, err := bm.cfSync.resolveConflict(
		map[string][]*chainhash.Hash{
			"10.0.0.1:8333": cps[:4],
			"10.0.0.2:8333": {cps[0], cps[1], cps[2], &other},
		}, cfStore, wire.GCSFilterRegular,
	)
	require.ErrorContains(t, err, "below the cfheaders query")
	require.Empty(t, good)
	require.Empty(t, acted)
}

// TestUncheckpointedFetchWrongStoreTip puts a wrong, nonzero filter header at
// the tip of the store while every peer is honest. Each reply's
// PrevFilterHeader then differs from our tip, which is evidence against our
// store and not against any peer, so no peer may be banned, the store must
// stay as it is and the error must say that the store disagrees with the
// peers. When one peer agrees with our tip, the others are still banned.
func TestUncheckpointedFetchWrongStoreTip(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		storeWrong bool
	}{
		{name: "store tip wrong", storeWrong: true},
		{name: "one peer wrong", storeWrong: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			testUncheckpointedFetchWrongStoreTip(
				t, test.storeWrong,
			)
		})
	}
}

func testUncheckpointedFetchWrongStoreTip(t *testing.T, storeWrong bool) {
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

	// Ten blocks, and filter headers up to height five.
	const blockTip, filterTip = 10, 5
	require.NoError(t, hdrStore.WriteHeaders(
		headers.blockHeaders[1:blockTip+1]...,
	))
	cf := make([]headerfs.FilterHeader, filterTip)
	copy(cf, headers.cfHeaders[1:filterTip+1])
	bad := chainhash.Hash{0xba, 0xd}
	if storeWrong {
		cf[filterTip-1].FilterHash = bad
	}
	require.NoError(t, cfStore.WriteHeaders(cf...))
	require.NoError(t, bm.ResetHeaderState())

	// The honest reply covers the blocks above the filter tip.
	honest := &wire.MsgCFHeaders{
		FilterType:       wire.GCSFilterRegular,
		StopHash:         headers.blockHeaders[blockTip].BlockHash(),
		PrevFilterHeader: headers.cfHeaders[filterTip].FilterHash,
	}
	for h := filterTip + 1; h <= blockTip; h++ {
		honest.FilterHashes = append(
			honest.FilterHashes, &headers.filterHashes[h],
		)
	}
	liar := *honest
	liar.PrevFilterHeader = bad
	scripts := map[string][]wire.Message{
		"10.0.0.1:8333": {honest}, "10.0.0.2:8333": {honest},
	}
	if !storeWrong {
		scripts["10.0.0.3:8333"] = []wire.Message{&liar}
	}
	bm.cfg.Broadcaster = newScriptedBroadcaster(scripts)

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

	// The honest headers get written when a peer agrees with our tip,
	// which sends a connected notification for each.
	go func() {
		for {
			select {
			case <-bm.blockNtfnChan:
			case <-bm.quit:
				return
			}
		}
	}()
	bm.wg.Add(1)
	go bm.blockHandler()
	t.Cleanup(func() { require.NoError(t, bm.Stop()) })

	err = bm.cfSync.getUncheckpointedCFHeaders(wire.GCSFilterRegular)

	mu.Lock()
	defer mu.Unlock()
	if !storeWrong {
		require.NoError(t, err)
		require.Equal(t, []string{"10.0.0.3:8333"}, banned)
		return
	}
	require.ErrorIs(t, err, errStoreDisagreesWithPeers)
	require.ErrorContains(t, err, bad.String())
	require.Empty(t, banned)

	tip, height, err := cfStore.ChainTip()
	require.NoError(t, err)
	require.Equal(t, uint32(filterTip), height)
	require.Equal(t, bad, *tip)
}

// TestCheckPrevFilterHeadersBlamesOnEvidence checks, for any set of replies,
// that a peer is only blamed for its PrevFilterHeader when another peer
// agrees with our filter tip, that a peer agreeing with our tip is never
// blamed, and that replies which all agree with each other but not with our
// tip report the store.
func TestCheckPrevFilterHeadersBlamesOnEvidence(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		tip := chainhash.Hash{1}
		headers := make(map[string]*wire.MsgCFHeaders)
		numPeers := rapid.IntRange(1, 6).Draw(t, "peers")
		for i := 0; i < numPeers; i++ {
			// Prev 0 is our tip, the others are other headers.
			prev := rapid.IntRange(0, 2).Draw(t, "prev")
			prevHash := chainhash.Hash{byte(prev + 1)}
			headers[string(rune('a'+i))] = &wire.MsgCFHeaders{
				PrevFilterHeader: prevHash,
			}
		}

		agreeing, served := 0, make(map[chainhash.Hash]bool)
		for _, msg := range headers {
			if msg.PrevFilterHeader == tip {
				agreeing++
			}
			served[msg.PrevFilterHeader] = true
		}

		wrong, err := checkPrevFilterHeaders(headers, tip, 7)
		for _, peer := range wrong {
			require.NotEqual(t, tip, headers[peer].PrevFilterHeader)
		}
		switch {
		case agreeing > 0:
			require.NoError(t, err)
			require.Len(t, wrong, numPeers-agreeing)

		case len(served) == 1:
			require.ErrorIs(t, err, errStoreDisagreesWithPeers)
			require.Empty(t, wrong)

		default:
			require.Error(t, err)
			require.Empty(t, wrong)
		}
	})
}
