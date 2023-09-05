package neutrino

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/gcs"
	"github.com/btcsuite/btcd/btcutil/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/integration/rpctest"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/query"
	"github.com/stretchr/testify/require"
)

const (
	// maxHeight is the height we will generate filter headers up to. We use
	// an odd number of checkpoints to ensure we can test cases where the
	// block manager is only able to fetch filter headers for one checkpoint
	// interval rather than two.
	maxHeight = 21 * uint32(wire.CFCheckptInterval)

	dbOpenTimeout = time.Second * 10
)

// mockDispatcher implements the query.Dispatcher interface and allows us to
// set up a custom Query method during tests.
type mockDispatcher struct {
	query.WorkManager

	query func(requests []*query.Request,
		options ...query.QueryOption) chan error
}

var _ query.Dispatcher = (*mockDispatcher)(nil)

func (m *mockDispatcher) Query(requests []*query.Request,
	options ...query.QueryOption) chan error {

	return m.query(requests, options...)
}

// setupBlockManager initialises a blockManager to be used in tests.
func setupBlockManager(t *testing.T) (*blockManager, headerfs.BlockHeaderStore,
	*headerfs.FilterHeaderStore, error) {

	// Set up the block and filter header stores.
	tempDir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", tempDir+"/weks.db", true, dbOpenTimeout,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error opening DB: %s", err)
	}

	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	hdrStore, err := headerfs.NewBlockHeaderStore(
		tempDir, db, &chaincfg.SimNetParams,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error creating block "+
			"header store: %s", err)
	}

	cfStore, err := headerfs.NewFilterHeaderStore(
		tempDir, db, headerfs.RegularFilter, &chaincfg.SimNetParams,
		nil,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error creating filter "+
			"header store: %s", err)
	}

	// Set up a blockManager with the chain service we defined.
	bm, err := newBlockManager(&blockManagerCfg{
		ChainParams:                  chaincfg.SimNetParams,
		BlockHeaders:                 hdrStore,
		RegFilterHeaders:             cfStore,
		cfHeaderQueryDispatcher:      &mockDispatcher{},
		blkHdrCheckptQueryDispatcher: &mockDispatcher{},
		TimeSource:                   blockchain.NewMedianTime(),
		BanPeer: func(string, banman.Reason) error {
			return nil
		},
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to create "+
			"blockmanager: %v", err)
	}

	return bm, hdrStore, cfStore, nil
}

// headers wraps the different headers and filters used throughout the tests.
type headers struct {
	blockHeaders []headerfs.BlockHeader
	cfHeaders    []headerfs.FilterHeader
	checkpoints  []*chainhash.Hash
	filterHashes []chainhash.Hash
}

// generateHeaders generates block headers, filter header and hashes, and
// checkpoints from the given genesis. The onCheckpoint method will be called
// with the current cf header on each checkpoint to modify the derivation of
// the next interval.
func generateHeaders(genesisBlockHeader *wire.BlockHeader,
	genesisFilterHeader *chainhash.Hash,
	onCheckpoint func(*chainhash.Hash)) (*headers, error) {

	var blockHeaders []headerfs.BlockHeader
	blockHeaders = append(blockHeaders, headerfs.BlockHeader{
		BlockHeader: genesisBlockHeader,
		Height:      0,
	})

	var cfHeaders []headerfs.FilterHeader
	cfHeaders = append(cfHeaders, headerfs.FilterHeader{
		HeaderHash: genesisBlockHeader.BlockHash(),
		FilterHash: *genesisFilterHeader,
		Height:     0,
	})

	// The filter hashes (not the filter headers!) will be sent as
	// part of the CFHeaders response, so we also keep track of
	// them.
	genesisFilter, err := builder.BuildBasicFilter(
		chaincfg.SimNetParams.GenesisBlock, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to build genesis filter: %v",
			err)
	}

	genesisFilterHash, err := builder.GetFilterHash(genesisFilter)
	if err != nil {
		return nil, fmt.Errorf("unable to get genesis filter hash: %v",
			err)
	}

	var filterHashes []chainhash.Hash
	filterHashes = append(filterHashes, genesisFilterHash)

	// Also keep track of the current filter header. We use this to
	// calculate the next filter header, as it commits to the
	// previous.
	currentCFHeader := *genesisFilterHeader

	// checkpoints will be the checkpoints passed to
	// getCheckpointedCFHeaders.
	var checkpoints []*chainhash.Hash

	for height := uint32(1); height <= maxHeight; height++ {
		header := heightToHeader(height)
		blockHeader := headerfs.BlockHeader{
			BlockHeader: header,
			Height:      height,
		}

		blockHeaders = append(blockHeaders, blockHeader)

		// It doesn't really matter what filter the filter
		// header commit to, so just use the height as a nonce
		// for the filters.
		filterHash := chainhash.Hash{}
		binary.BigEndian.PutUint32(filterHash[:], height)
		filterHashes = append(filterHashes, filterHash)

		// Calculate the current filter header, and add to our
		// slice.
		currentCFHeader = chainhash.DoubleHashH(
			append(filterHash[:], currentCFHeader[:]...),
		)
		cfHeaders = append(cfHeaders, headerfs.FilterHeader{
			HeaderHash: header.BlockHash(),
			FilterHash: currentCFHeader,
			Height:     height,
		})

		// Each interval we must record a checkpoint.
		if height%wire.CFCheckptInterval == 0 {
			// We must make a copy of the current header to
			// avoid mutation.
			cfh := currentCFHeader
			checkpoints = append(checkpoints, &cfh)

			if onCheckpoint != nil {
				onCheckpoint(&currentCFHeader)
			}
		}
	}

	return &headers{
		blockHeaders: blockHeaders,
		cfHeaders:    cfHeaders,
		checkpoints:  checkpoints,
		filterHashes: filterHashes,
	}, nil
}

// generateResponses generates the MsgCFHeaders messages from the given queries
// and headers.
func generateResponses(msgs []query.ReqMessage,
	headers *headers) ([]*wire.MsgCFHeaders, error) {

	// Craft a response for each message.
	var responses []*wire.MsgCFHeaders
	for _, msg := range msgs {
		// Only GetCFHeaders expected.
		q, ok := msg.Message().(*wire.MsgGetCFHeaders)
		if !ok {
			return nil, fmt.Errorf("got unexpected message %T",
				msg)
		}

		// The start height must be set to a checkpoint height+1.
		if q.StartHeight%wire.CFCheckptInterval != 1 {
			return nil, fmt.Errorf("unexpexted start height %v",
				q.StartHeight)
		}

		var prevFilterHeader chainhash.Hash
		switch q.StartHeight {
		// If the start height is 1 the prevFilterHeader is set to the
		// genesis header.
		case 1:
			genesisFilterHeader := headers.cfHeaders[0].FilterHash
			prevFilterHeader = genesisFilterHeader

		// Otherwise we use one of the created checkpoints.
		default:
			j := q.StartHeight/wire.CFCheckptInterval - 1
			prevFilterHeader = *headers.checkpoints[j]
		}

		resp := &wire.MsgCFHeaders{
			FilterType:       q.FilterType,
			StopHash:         q.StopHash,
			PrevFilterHeader: prevFilterHeader,
		}

		// Keep adding filter hashes until we reach the stop hash.
		for h := q.StartHeight; ; h++ {
			resp.FilterHashes = append(
				resp.FilterHashes, &headers.filterHashes[h],
			)

			blockHash := headers.blockHeaders[h].BlockHash()
			if blockHash == q.StopHash {
				break
			}
		}

		responses = append(responses, resp)
	}

	return responses, nil
}

// TestBlockManagerInitialInterval tests that the block manager is able to
// handle checkpointed filter header query responses in out of order, and when
// a partial interval is already written to the store.
func TestBlockManagerInitialInterval(t *testing.T) {
	t.Parallel()

	type testCase struct {
		// permute indicates whether responses should be permutated.
		permute bool

		// partialInterval indicates whether we should write parts of
		// the first checkpoint interval to the filter header store
		// before starting the test.
		partialInterval bool

		// repeat indicates whether responses should be repeated.
		repeat bool
	}

	// Generate all combinations of testcases.
	var testCases []testCase
	b := []bool{false, true}
	for _, perm := range b {
		for _, part := range b {
			for _, rep := range b {
				testCases = append(testCases, testCase{
					permute:         perm,
					partialInterval: part,
					repeat:          rep,
				})
			}
		}
	}

	for _, test := range testCases {
		test := test
		testDesc := fmt.Sprintf("permute=%v, partial=%v, repeat=%v",
			test.permute, test.partialInterval, test.repeat)

		bm, hdrStore, cfStore, err := setupBlockManager(t)
		if err != nil {
			t.Fatalf("unable to set up ChainService: %v", err)
		}

		// Keep track of the filter headers and block headers. Since
		// the genesis headers are written automatically when the store
		// is created, we query it to add to the slices.
		genesisBlockHeader, _, err := hdrStore.ChainTip()
		require.NoError(t, err)

		genesisFilterHeader, _, err := cfStore.ChainTip()
		require.NoError(t, err)

		headers, err := generateHeaders(
			genesisBlockHeader, genesisFilterHeader, nil,
		)
		require.NoError(t, err)

		// Write all block headers but the genesis, since it is already
		// in the store.
		err = hdrStore.WriteHeaders(headers.blockHeaders[1:]...)
		require.NoError(t, err)

		// We emulate the case where a few filter headers are already
		// written to the store by writing 1/3 of the first interval.
		if test.partialInterval {
			err = cfStore.WriteHeaders(
				headers.cfHeaders[1 : wire.CFCheckptInterval/3]...,
			)
			require.NoError(t, err)
		}

		// We set up a custom query batch method for this test, as we
		// will use this to feed the blockmanager with our crafted
		// responses.
		bm.cfg.cfHeaderQueryDispatcher.(*mockDispatcher).query = func(
			requests []*query.Request,
			options ...query.QueryOption) chan error {

			var msgs []query.ReqMessage
			for _, q := range requests {
				msgs = append(msgs, q.Req)
			}

			responses, err := generateResponses(msgs, headers)
			require.NoError(t, err)

			// We permute the response order if the test signals
			// that.
			perm := rand.Perm(len(responses))

			errChan := make(chan error, 1)
			go func() {
				for i, v := range perm {
					index := i
					if test.permute {
						index = v
					}

					// Before handling the response we take
					// copies of the message, as we cannot
					// guarantee that it won't be modified.
					resp := *responses[index]
					resp2 := *responses[index]

					// Let the blockmanager handle the
					// message.
					progress := requests[index].HandleResp(
						msgs[index], &resp, nil,
					)

					if progress != query.Finished {
						errChan <- fmt.Errorf("got "+
							" %v on "+
							"send of index %d: %v", progress,
							index, testDesc)
						return
					}

					// If we are not testing repeated
					// responses, go on to the next
					// response.
					if !test.repeat {
						continue
					}

					// Otherwise resend the response we
					// just sent.
					progress = requests[index].HandleResp(
						msgs[index], &resp2, nil,
					)
					if progress != query.Finished {
						errChan <- fmt.Errorf("got "+
							" %v on "+
							"send of index %d: %v", progress,
							index, testDesc)
						return
					}
				}
				errChan <- nil
			}()

			return errChan
		}

		// We should expect to see notifications for each new filter
		// header being connected.
		startHeight := uint32(1)
		if test.partialInterval {
			startHeight = wire.CFCheckptInterval / 3
		}
		go func() {
			for i := startHeight; i <= maxHeight; i++ {
				ntfn := <-bm.blockNtfnChan
				if _, ok := ntfn.(*blockntfns.Connected); !ok {
					t.Error("expected block connected " +
						"notification")
					return
				}
			}
		}()

		// Call the get checkpointed cf headers method with the
		// checkpoints we created to start the test.
		bm.getCheckpointedCFHeaders(
			headers.checkpoints, cfStore, wire.GCSFilterRegular,
		)

		// Finally make sure the filter header tip is what we expect.
		tip, tipHeight, err := cfStore.ChainTip()
		require.NoError(t, err)

		require.Equal(t, maxHeight, tipHeight, "tip height")

		lastCheckpoint := headers.checkpoints[len(headers.checkpoints)-1]
		require.Equal(t, *lastCheckpoint, *tip, "tip")
	}
}

// TestBlockManagerInvalidInterval tests that the block manager is able to
// determine it is receiving corrupt checkpoints and filter headers.
func TestBlockManagerInvalidInterval(t *testing.T) {
	t.Parallel()

	type testCase struct {
		// wrongGenesis indicates whether we should start deriving the
		// filters from a wrong genesis.
		wrongGenesis bool

		// intervalMisaligned indicates whether each interval prev hash
		// should not line up with the previous checkpoint.
		intervalMisaligned bool

		// invalidPrevHash indicates whether the interval responses
		// should have a prev hash that doesn't mathc that interval.
		invalidPrevHash bool

		// partialInterval indicates whether we should write parts of
		// the first checkpoint interval to the filter header store
		// before starting the test.
		partialInterval bool

		// firstInvalid is the first interval response we expect the
		// blockmanager to determine is invalid.
		firstInvalid int
	}

	testCases := []testCase{
		// With a set of checkpoints and filter headers calculated from
		// the wrong genesis, the block manager should be able to
		// determine that the first interval doesn't line up.
		{
			wrongGenesis: true,
			firstInvalid: 0,
		},

		// With checkpoints calculated from the wrong genesis, and a
		// partial set of filter headers already written, the first
		// interval response should be considered invalid.
		{
			wrongGenesis:    true,
			partialInterval: true,
			firstInvalid:    0,
		},

		// With intervals not lining up, the second interval response
		// should be determined invalid.
		{
			intervalMisaligned: true,
			firstInvalid:       0,
		},

		// With misaligned intervals and a partial interval written, the
		// second interval response should be considered invalid.
		{
			intervalMisaligned: true,
			partialInterval:    true,
			firstInvalid:       0,
		},

		// With responses having invalid prev hashes, the second
		// interval response should be deemed invalid.
		{
			invalidPrevHash: true,
			firstInvalid:    1,
		},
	}

	for _, test := range testCases {
		test := test
		bm, hdrStore, cfStore, err := setupBlockManager(t)
		require.NoError(t, err)

		// Create a mock peer to prevent panics when attempting to ban
		// a peer that served an invalid filter header.
		mockPeer := NewServerPeer(&ChainService{}, false)
		mockPeer.Peer, err = peer.NewOutboundPeer(
			NewPeerConfig(mockPeer), "127.0.0.1:8333",
		)
		require.NoError(t, err)

		// Keep track of the filter headers and block headers. Since
		// the genesis headers are written automatically when the store
		// is created, we query it to add to the slices.
		genesisBlockHeader, _, err := hdrStore.ChainTip()
		require.NoError(t, err)

		genesisFilterHeader, _, err := cfStore.ChainTip()
		require.NoError(t, err)

		// To emulate a full node serving us filter headers derived
		// from different genesis than what we have, we flip a bit in
		// the genesis filter header.
		if test.wrongGenesis {
			genesisFilterHeader[0] ^= 1
		}

		headers, err := generateHeaders(
			genesisBlockHeader, genesisFilterHeader,
			func(currentCFHeader *chainhash.Hash) {
				// If we are testing that each interval doesn't
				// line up properly with the previous, we flip
				// a bit in the current header before
				// calculating the next interval checkpoint.
				if test.intervalMisaligned {
					currentCFHeader[0] ^= 1
				}
			},
		)
		require.NoError(t, err)

		// Write all block headers but the genesis, since it is already
		// in the store.
		err = hdrStore.WriteHeaders(headers.blockHeaders[1:]...)
		require.NoError(t, err)

		// We emulate the case where a few filter headers are already
		// written to the store by writing 1/3 of the first interval.
		if test.partialInterval {
			err = cfStore.WriteHeaders(
				headers.cfHeaders[1 : wire.CFCheckptInterval/3]...,
			)
			require.NoError(t, err)
		}

		bm.cfg.cfHeaderQueryDispatcher.(*mockDispatcher).query = func(
			requests []*query.Request,
			options ...query.QueryOption) chan error {

			var msgs []query.ReqMessage
			for _, q := range requests {
				msgs = append(msgs, q.Req)
			}
			responses, err := generateResponses(msgs, headers)
			require.NoError(t, err)

			// Since we used the generated checkpoints when
			// creating the responses, we must flip the
			// PrevFilterHeader bit back before sending them if we
			// are checking for misaligned intervals. This to
			// ensure we don't hit the invalid prev hash case.
			if test.intervalMisaligned {
				for i := range responses {
					if i == 0 {
						continue
					}
					responses[i].PrevFilterHeader[0] ^= 1
				}
			}

			// If we are testing for intervals with invalid prev
			// hashes, we flip a bit to corrupt them, regardless of
			// whether we are testing misaligned intervals.
			if test.invalidPrevHash {
				for i := range responses {
					if i == 0 {
						continue
					}
					responses[i].PrevFilterHeader[1] ^= 1
				}
			}

			errChan := make(chan error, 1)
			go func() {
				// Check that the success of the callback match what we
				// expect.
				for i := range responses {
					progress := requests[i].HandleResp(
						msgs[i], responses[i], nil,
					)
					if i == test.firstInvalid {
						if progress == query.Finished {
							t.Errorf("expected interval "+
								"%d to be invalid", i)
							return
						}
						errChan <- fmt.Errorf("invalid interval")
						break
					}

					if progress != query.Finished {
						t.Errorf("expected interval %d to be "+
							"valid", i)
						return
					}
				}

				errChan <- nil
			}()

			return errChan
		}

		// We should expect to see notifications for each new filter
		// header being connected.
		startHeight := uint32(1)
		if test.partialInterval {
			startHeight = wire.CFCheckptInterval / 3
		}
		go func() {
			for i := startHeight; i <= maxHeight; i++ {
				ntfn := <-bm.blockNtfnChan
				if _, ok := ntfn.(*blockntfns.Connected); !ok {
					t.Error("expected block connected " +
						"notification")
					return
				}
			}
		}()

		// Start the test by calling the get checkpointed cf headers
		// method with the checkpoints we created.
		bm.getCheckpointedCFHeaders(
			headers.checkpoints, cfStore, wire.GCSFilterRegular,
		)
	}
}

// buildNonPushScriptFilter creates a CFilter with all output scripts except all
// OP_RETURNS with push-only scripts.
//
// NOTE: this is not a valid filter, only for tests.
func buildNonPushScriptFilter(block *wire.MsgBlock) (*gcs.Filter, error) {
	blockHash := block.BlockHash()
	b := builder.WithKeyHash(&blockHash)

	for _, tx := range block.Transactions {
		for _, txOut := range tx.TxOut {
			// The old version of BIP-158 skipped OP_RETURNs that
			// had a push-only script.
			if txOut.PkScript[0] == txscript.OP_RETURN &&
				txscript.IsPushOnlyScript(txOut.PkScript[1:]) {

				continue
			}

			b.AddEntry(txOut.PkScript)
		}
	}

	return b.Build()
}

// buildAllPkScriptsFilter creates a CFilter with all output scripts, including
// OP_RETURNS.
//
// NOTE: this is not a valid filter, only for tests.
func buildAllPkScriptsFilter(block *wire.MsgBlock) (*gcs.Filter, error) {
	blockHash := block.BlockHash()
	b := builder.WithKeyHash(&blockHash)

	for _, tx := range block.Transactions {
		for _, txOut := range tx.TxOut {
			// An old version of BIP-158 included all output
			// scripts.
			b.AddEntry(txOut.PkScript)
		}
	}

	return b.Build()
}

func assertBadPeers(expBad map[string]struct{}, badPeers []string) error {
	remBad := make(map[string]struct{})
	for p := range expBad {
		remBad[p] = struct{}{}
	}
	for _, p := range badPeers {
		_, ok := remBad[p]
		if !ok {
			return fmt.Errorf("did not expect %v to be bad", p)
		}
		delete(remBad, p)
	}

	if len(remBad) != 0 {
		return fmt.Errorf("did expect more bad peers")
	}

	return nil
}

// TestBlockManagerDetectBadPeers checks that we detect bad peers, like peers
// not responding to our filter query, serving inconsistent filters etc.
func TestBlockManagerDetectBadPeers(t *testing.T) {
	t.Parallel()

	var (
		stopHash        = chainhash.Hash{}
		prev            = chainhash.Hash{}
		startHeight     = uint32(100)
		badIndex        = uint32(5)
		targetIndex     = startHeight + badIndex
		fType           = wire.GCSFilterRegular
		filterBytes, _  = correctFilter.NBytes()
		filterHash, _   = builder.GetFilterHash(correctFilter)
		blockHeader     = wire.BlockHeader{}
		targetBlockHash = block.BlockHash()

		peers  = []string{"good1:1", "good2:1", "bad:1", "good3:1"}
		expBad = map[string]struct{}{
			"bad:1": {},
		}
	)

	testCases := []struct {
		// filterAnswers is used by each testcase to set the anwers we
		// want each peer to respond with on filter queries.
		filterAnswers func(string, map[string]wire.Message)
	}{
		{
			// We let the "bad" peers not respond to the filter
			// query. They should be marked bad because they are
			// unresponsive. We do this to ensure peers cannot
			// only respond to us with headers, and stall our sync
			// by not responding to filter requests.
			filterAnswers: func(p string,
				answers map[string]wire.Message) {

				if strings.Contains(p, "bad") {
					return
				}

				answers[p] = wire.NewMsgCFilter(
					fType, &targetBlockHash, filterBytes,
				)
			},
		},
		{
			// We let the "bad" peers serve filters that don't hash
			// to the filter headers they have sent.
			filterAnswers: func(p string,
				answers map[string]wire.Message) {

				filterData := filterBytes
				if strings.Contains(p, "bad") {
					filterData, _ = fakeFilter1.NBytes()
				}

				answers[p] = wire.NewMsgCFilter(
					fType, &targetBlockHash, filterData,
				)
			},
		},
	}

	for _, test := range testCases {
		// Create a mock block header store. We only need to be able to
		// serve a header for the target index.
		blockHeaders := newMockBlockHeaderStore()
		blockHeaders.heights[targetIndex] = blockHeader

		// We set up the mock queryAllPeers to only respond according to
		// the active testcase.
		answers := make(map[string]wire.Message)
		queryAllPeers := func(
			queryMsg wire.Message,
			checkResponse func(sp *ServerPeer, resp wire.Message,
				quit chan<- struct{}, peerQuit chan<- struct{}),
			options ...QueryOption) {

			for p, resp := range answers {
				pp, err := peer.NewOutboundPeer(
					&peer.Config{}, p,
				)
				if err != nil {
					panic(err)
				}

				sp := &ServerPeer{
					Peer: pp,
				}
				checkResponse(
					sp, resp, make(chan struct{}),
					make(chan struct{}),
				)
			}
		}

		for _, p := range peers {
			test.filterAnswers(p, answers)
		}

		// For the CFHeaders, we pretend all peers responded with the same
		// filter headers.
		msg := &wire.MsgCFHeaders{
			FilterType:       fType,
			StopHash:         stopHash,
			PrevFilterHeader: prev,
		}

		for i := uint32(0); i < 2*badIndex; i++ {
			_ = msg.AddCFHash(&filterHash)
		}

		headers := make(map[string]*wire.MsgCFHeaders)
		for _, p := range peers {
			headers[p] = msg
		}

		bm := &blockManager{
			cfg: &blockManagerCfg{
				BlockHeaders:  blockHeaders,
				queryAllPeers: queryAllPeers,
			},
		}

		// Now trying to detect which peers are bad, we should detect the
		// bad ones.
		badPeers, err := bm.detectBadPeers(
			headers, targetIndex, badIndex, fType,
		)
		require.NoError(t, err)

		err = assertBadPeers(expBad, badPeers)
		require.NoError(t, err)
	}
}

// TestHandleHeaders checks that we handle headers correctly, and that we
// disconnect peers that serve us bad headers (headers that don't connect to
// each other properly).
func TestHandleHeaders(t *testing.T) {
	t.Parallel()

	// First, we set up a block manager and a fake peer that will act as the
	// test's remote peer.
	bm, _, _, err := setupBlockManager(t)
	require.NoError(t, err)

	fakePeer, err := peer.NewOutboundPeer(&peer.Config{}, "fake:123")
	require.NoError(t, err)

	// We'll want to use actual, real blocks, so we take a miner harness
	// that we can use to generate some.
	harness, err := rpctest.New(
		&chaincfg.SimNetParams, nil, []string{"--txindex"}, "",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, harness.TearDown())
	})

	err = harness.SetUp(false, 0)
	require.NoError(t, err)

	// Generate 200 valid blocks that we then feed to the block manager.
	blockHashes, err := harness.Client.Generate(200)
	require.NoError(t, err)

	hmsg := &headersMsg{
		headers: &wire.MsgHeaders{
			Headers: make([]*wire.BlockHeader, len(blockHashes)),
		},
		peer: &ServerPeer{
			Peer: fakePeer,
		},
	}

	for i := range blockHashes {
		header, err := harness.Client.GetBlockHeader(blockHashes[i])
		require.NoError(t, err)

		hmsg.headers.Headers[i] = header
	}

	// Let's feed in the correct headers. This should work fine and the peer
	// should not be disconnected.
	bm.handleHeadersMsg(hmsg)
	assertPeerDisconnected(false, fakePeer, t)

	// Now scramble the headers and feed them in again. This should cause
	// the peer to be disconnected.
	rand.Shuffle(len(hmsg.headers.Headers), func(i, j int) {
		hmsg.headers.Headers[i], hmsg.headers.Headers[j] =
			hmsg.headers.Headers[j], hmsg.headers.Headers[i]
	})
	bm.handleHeadersMsg(hmsg)
	assertPeerDisconnected(true, fakePeer, t)
}

// assertPeerDisconnected asserts that the peer supplied as an argument is disconnected.
func assertPeerDisconnected(shouldBeDisconnected bool, sp *peer.Peer, t *testing.T) {
	// This is quite hacky but works: We expect the peer to be
	// disconnected, which sets the unexported "disconnected" field
	// to 1.
	refValue := reflect.ValueOf(sp).Elem()
	foo := refValue.FieldByName("disconnect").Int()

	if shouldBeDisconnected {
		require.EqualValues(t, 1, foo)
	} else {
		require.EqualValues(t, 0, foo)
	}
}

// TestBatchCheckpointedBlkHeaders tests the batch checkpointed headers function.
func TestBatchCheckpointedBlkHeaders(t *testing.T) {
	t.Parallel()

	// First, we set up a block manager and a fake peer that will act as the
	// test's remote peer.
	bm, _, _, err := setupBlockManager(t)
	require.NoError(t, err)

	// Created checkpoints for our simulated network.
	checkpoints := []chaincfg.Checkpoint{

		{
			Hash:   &chainhash.Hash{1},
			Height: int32(1),
		},

		{
			Hash:   &chainhash.Hash{2},
			Height: int32(2),
		},

		{
			Hash:   &chainhash.Hash{3},
			Height: int32(3),
		},
	}

	modParams := chaincfg.SimNetParams
	modParams.Checkpoints = append(modParams.Checkpoints, checkpoints...)
	bm.cfg.ChainParams = modParams

	// set checkpoint and header tip.
	bm.nextCheckpoint = &checkpoints[0]

	bm.newHeadersMtx.Lock()
	bm.headerTip = 0
	bm.headerTipHash = chainhash.Hash{0}
	bm.newHeadersMtx.Unlock()

	// This is the query we assert to obtain if the function works accordingly.
	expectedQuery := CheckpointedBlockHeadersQuery{
		blockMgr: bm,
		msgs: []*headerQuery{

			{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: blockchain.BlockLocator([]*chainhash.Hash{{0}}),
					HashStop:           *checkpoints[0].Hash,
				},
				startHeight:   int32(0),
				initialHeight: int32(0),
				startHash:     chainhash.Hash{0},
				endHeight:     checkpoints[0].Height,
				initialHash:   chainhash.Hash{0},
			},

			{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: blockchain.BlockLocator([]*chainhash.Hash{checkpoints[0].Hash}),
					HashStop:           *checkpoints[1].Hash,
				},
				startHeight:   checkpoints[0].Height,
				initialHeight: checkpoints[0].Height,
				startHash:     *checkpoints[0].Hash,
				endHeight:     checkpoints[1].Height,
				initialHash:   *checkpoints[0].Hash,
			},

			{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: blockchain.BlockLocator([]*chainhash.Hash{checkpoints[1].Hash}),
					HashStop:           *checkpoints[2].Hash,
				},
				startHeight:   checkpoints[1].Height,
				initialHeight: checkpoints[1].Height,
				startHash:     *checkpoints[1].Hash,
				endHeight:     checkpoints[2].Height,
				initialHash:   *checkpoints[1].Hash,
			},

			{

				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: blockchain.BlockLocator([]*chainhash.Hash{checkpoints[2].Hash}),
					HashStop:           zeroHash,
				},
				startHeight:   checkpoints[2].Height,
				initialHeight: checkpoints[2].Height,
				startHash:     *checkpoints[2].Hash,
				endHeight:     checkpoints[2].Height + wire.MaxBlockHeadersPerMsg,
				initialHash:   *checkpoints[2].Hash,
			},
		},
	}

	// create request.
	expectedRequest := expectedQuery.requests()

	bm.cfg.blkHdrCheckptQueryDispatcher.(*mockDispatcher).query = func(requests []*query.Request,
		options ...query.QueryOption) chan error {

		// assert that the requests obtained has same length as that of our expected query.
		if len(requests) != len(expectedRequest) {
			t.Fatalf("unequal length")
		}

		for i, req := range requests {
			testEqualReqMessage(req, expectedRequest[i], t)
		}

		// Ensure the query options sent by query is four. This is the number of query option supplied as args while
		// querying the workmanager.
		if len(options) != 3 {
			t.Fatalf("expected five option parameter for query but got, %v\n", len(options))
		}
		return nil
	}

	// call the function that we are testing.
	bm.batchCheckpointedBlkHeaders()
}

// This function tests the ProcessBlKHeaderInCheckPtRegionInOrder function.
func TestProcessBlKHeaderInCheckPtRegionInOrder(t *testing.T) {
	t.Parallel()

	// First, we set up a block manager and a fake peer that will act as the
	// test's remote peer.
	bm, _, _, err := setupBlockManager(t)
	require.NoError(t, err)

	fakePeer, err := peer.NewOutboundPeer(&peer.Config{}, "fake:123")
	require.NoError(t, err)

	// We'll want to use actual, real blocks, so we take a miner harness
	// that we can use to generate some.
	harness, err := rpctest.New(
		&chaincfg.SimNetParams, nil, []string{"--txindex"}, "",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, harness.TearDown())
	})

	err = harness.SetUp(false, 0)
	require.NoError(t, err)

	// Generate 30 valid blocks that we then feed to the block manager.
	blockHashes, err := harness.Client.Generate(30)
	require.NoError(t, err)

	// This is the headerMessage containing 10 headers starting at height 0.
	hmsgTip0 := &headersMsg{
		headers: &wire.MsgHeaders{
			Headers: make([]*wire.BlockHeader, 10),
		},
		peer: &ServerPeer{
			Peer: fakePeer,
		},
	}

	// This is the headerMessage containing 10 headers starting at height 10.
	hmsgTip10 := &headersMsg{
		headers: &wire.MsgHeaders{
			Headers: make([]*wire.BlockHeader, 10),
		},
		peer: &ServerPeer{
			Peer: fakePeer,
		},
	}

	// This is the headerMessage containing 10 headers starting at height 20.
	hmsgTip20 := &headersMsg{
		headers: &wire.MsgHeaders{
			Headers: make([]*wire.BlockHeader, 10),
		},
		peer: &ServerPeer{
			Peer: fakePeer,
		},
	}

	// Loop through the generated blockHashes and add headers to their appropriate slices.
	for i := range blockHashes {
		header, err := harness.Client.GetBlockHeader(blockHashes[i])
		require.NoError(t, err)

		if i < 10 {
			hmsgTip0.headers.Headers[i] = header
		}

		if i >= 10 && i < 20 {
			hmsgTip10.headers.Headers[i-10] = header
		}

		if i >= 20 {
			hmsgTip20.headers.Headers[i-20] = header
		}
	}

	// initialize the hdrTipSlice.
	bm.hdrTipSlice = make([]int32, 0)

	// Create checkpoint for our test chain.
	checkpoint := chaincfg.Checkpoint{
		Hash:   blockHashes[29],
		Height: int32(30),
	}
	bm.cfg.ChainParams.Checkpoints = append(bm.cfg.ChainParams.Checkpoints, []chaincfg.Checkpoint{
		checkpoint,
	}...)
	bm.nextCheckpoint = &checkpoint

	// If ProcessBlKHeaderInCheckPtRegionInOrder loop receives invalid headers assert the query parameters being sent
	// to the workmanager is expected.
	bm.cfg.blkHdrCheckptQueryDispatcher.(*mockDispatcher).query = func(requests []*query.Request,
		options ...query.QueryOption) chan error {

		// The function should send only one request.
		if len(requests) != 1 {
			t.Fatalf("expected only one request")
		}

		finalNode := bm.headerList.Back()
		newHdrTip := finalNode.Height
		newHdrTipHash := finalNode.Header.BlockHash()
		prevCheckPt := bm.findPreviousHeaderCheckpoint(newHdrTip)

		testEqualReqMessage(requests[0], &query.Request{

			Req: &headerQuery{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: []*chainhash.Hash{&newHdrTipHash},
					HashStop:           *bm.nextCheckpoint.Hash,
				},
				startHeight:   newHdrTip,
				initialHeight: prevCheckPt.Height,
				startHash:     newHdrTipHash,
				endHeight:     bm.nextCheckpoint.Height,
				initialHash:   newHdrTipHash,
				index:         0,
			},
		}, t)

		// The function should include only four query options while querying.
		if len(options) != 3 {
			t.Fatalf("expected three option parameter for query but got, %v\n", len(options))
		}
		return nil
	}

	// Call the function in a goroutine.
	go bm.processBlKHeaderInCheckPtRegionInOrder()

	// At this point syncPeer should be nil.
	bm.syncPeerMutex.RLock()
	if bm.syncPeer != nil {
		bm.syncPeerMutex.RUnlock()
		t.Fatalf("syncPeer should be nil initially")
	}
	bm.syncPeerMutex.RUnlock()

	// Set header tip to zero and write a response at height 10, ensure the ProcessBlKHeaderInCheckPtRegionInOrder loop
	// does not handle the response as it does not correspond to the current header tip.
	bm.newHeadersMtx.Lock()
	bm.headerTip = 0
	bm.newHeadersMtx.Unlock()

	bm.writeBatchMtx.Lock()
	newTipWrite := int32(10)
	bm.hdrTipToResponse[newTipWrite] = hmsgTip10
	i := sort.Search(len(bm.hdrTipSlice), func(i int) bool {
		return bm.hdrTipSlice[i] >= newTipWrite
	})

	bm.hdrTipSlice = append(bm.hdrTipSlice[:i], append([]int32{newTipWrite}, bm.hdrTipSlice[i:]...)...)
	bm.writeBatchMtx.Unlock()

	// SyncPeer should still be nil to indicate that the loop did not handle the response.
	bm.syncPeerMutex.RLock()
	if bm.syncPeer != nil {
		bm.syncPeerMutex.RUnlock()
		t.Fatalf("syncPeer should be nil")
	}
	bm.syncPeerMutex.RUnlock()

	// Set header tip to 20 to indicate that even when the chain's tip is higher that the available tips in the
	// hdrTipToResponse map, the loop does not still handle it.
	bm.newHeadersMtx.Lock()
	bm.headerTip = 20
	bm.newHeadersMtx.Unlock()

	// SyncPeer should still be nil to indicate that the loop did not handle the response.
	bm.syncPeerMutex.RLock()
	if bm.syncPeer != nil {
		bm.syncPeerMutex.RUnlock()
		t.Fatalf("syncPeer should be nil")
	}
	bm.syncPeerMutex.RUnlock()

	// Set headerTip to zero and write a response at height 0 to the hdrTipToResponse map. The loop should handle this
	// response now and the following response that would correspond to its new tip after this.
	bm.newHeadersMtx.Lock()
	bm.headerTip = 0
	bm.newHeadersMtx.Unlock()

	bm.writeBatchMtx.Lock()
	newTipWrite = int32(0)
	i = sort.Search(len(bm.hdrTipSlice), func(i int) bool {
		return bm.hdrTipSlice[i] >= newTipWrite
	})

	bm.hdrTipSlice = append(bm.hdrTipSlice[:i], append([]int32{newTipWrite}, bm.hdrTipSlice[i:]...)...)

	bm.hdrTipToResponse[newTipWrite] = hmsgTip0
	bm.writeBatchMtx.Unlock()

	// Allow time for handling the response.
	time.Sleep(1 * time.Second)
	bm.syncPeerMutex.RLock()
	if bm.syncPeer == nil {
		bm.syncPeerMutex.RUnlock()
		t.Fatalf("syncPeer should not be nil")
	}
	bm.syncPeerMutex.RUnlock()

	// Header tip should be 20 as th the loop would handle response at height 0 then the previously written
	// height 10.
	bm.newHeadersMtx.RLock()
	if bm.headerTip != 20 {
		hdrTip := bm.headerTip
		bm.newHeadersMtx.RUnlock()
		t.Fatalf("expected header tip at 10 but got %v\n", hdrTip)
	}
	bm.newHeadersMtx.RUnlock()

	// Now scramble the headers and feed them in again. This should cause
	// the loop to delete this response from the map and re-request for this header from
	// the workmanager.
	rand.Shuffle(len(hmsgTip20.headers.Headers), func(i, j int) {
		hmsgTip20.headers.Headers[i], hmsgTip20.headers.Headers[j] =
			hmsgTip20.headers.Headers[j], hmsgTip20.headers.Headers[i]
	})

	// Write this header at height 20, this would cause the loop to handle it.
	bm.writeBatchMtx.Lock()
	newTipWrite = int32(20)
	bm.hdrTipToResponse[newTipWrite] = hmsgTip20
	i = sort.Search(len(bm.hdrTipSlice), func(i int) bool {
		return bm.hdrTipSlice[i] >= newTipWrite
	})

	bm.hdrTipSlice = append(bm.hdrTipSlice[:i], append([]int32{newTipWrite}, bm.hdrTipSlice[i:]...)...)

	bm.writeBatchMtx.Unlock()

	// Allow time for handling.
	time.Sleep(1 * time.Second)

	// HeadrTip should not advance as headers are invalid.
	bm.newHeadersMtx.RLock()
	if bm.headerTip != 20 {
		hdrTip := bm.headerTip
		bm.newHeadersMtx.RUnlock()
		t.Fatalf("expected header tip at 20 but got %v\n", hdrTip)
	}
	bm.newHeadersMtx.RUnlock()

	// Syncpeer should not be nil as we are still in the loop.
	bm.syncPeerMutex.RLock()
	if bm.syncPeer == nil {
		bm.syncPeerMutex.RUnlock()
		t.Fatalf("syncPeer should not be nil")
	}
	bm.syncPeerMutex.RUnlock()

	// The response at header tip 20 should be deleted.
	bm.writeBatchMtx.RLock()
	_, ok := bm.hdrTipToResponse[int32(20)]
	bm.writeBatchMtx.RUnlock()

	if ok {
		t.Fatalf("expected response to header tip deleted")
	}
}

// TestCheckpointedBlockHeadersQuery_handleResponse tests the handleResponse method
// of the CheckpointedBlockHeadersQuery.
func TestCheckpointedBlockHeadersQuery_handleResponse(t *testing.T) {
	t.Parallel()

	// handleRespTestCase holds all the information required to test different scenarios while
	// using the function.
	type handleRespTestCase struct {

		// name of the testcase.
		name string

		// resp is the response argument to be sent to the handleResp method as an arg.
		resp wire.Message

		// req is the request method to be sent to the handleResp method as an arg.
		req query.ReqMessage

		// progress is the expected progress to be returned by the handleResp method.
		progress query.Progress

		// lastblock is the block with which we obtain its hash to be used as the request's hashStop.
		lastBlock wire.BlockHeader

		// peerDisconnected indicates if the peer would be disconnected after the handleResp method is done.
		peerDisconnected bool
	}

	testCases := []handleRespTestCase{

		{
			// Scenario in which we have a request type that is not the same as the expected headerQuery type.It should
			// return no error and NoProgressNoFinalResp query.Progress.
			name: "invalid request type",
			resp: &wire.MsgHeaders{
				Headers: make([]*wire.BlockHeader, 0, 5),
			},
			req:      &encodedQuery{},
			progress: query.NoResponse,
		},

		{
			// Scenario in which we have a response type that is not same as the expected wire.MsgHeaders. It should
			// return no error and NoProgressNoFinalResp query.Progress.
			name: "invalid response type",
			resp: &wire.MsgCFHeaders{},
			req: &headerQuery{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: []*chainhash.Hash{
						{1},
					},
				},
				startHeight:   1,
				initialHeight: 1,
				startHash:     chainhash.Hash{1},
				endHeight:     6,
				initialHash:   chainhash.Hash{1},
			},
			progress: query.NoResponse,
			lastBlock: wire.BlockHeader{
				PrevBlock: chainhash.Hash{5},
			},
		},

		{
			// Scenario in which we have the response in the hdrTipResponseMap. While calling these testcases, we
			// initialize the hdrTipToResponse map to contain a response at height 0 and 6. Since this request ahs a
			// startheight of 0, its response would be in the map already, aligning with this scenario. This scenario
			// should return query.IgnoreRequest,
			name: "response start Height in hdrTipResponse map",
			resp: &wire.MsgHeaders{
				Headers: make([]*wire.BlockHeader, 0, 4),
			},
			req: &headerQuery{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: []*chainhash.Hash{
						{0},
					},
				},
				startHeight:   0,
				initialHeight: 0,
				startHash:     chainhash.Hash{0},
				endHeight:     5,
				initialHash:   chainhash.Hash{0},
			},
			progress: query.IgnoreRequest,
			lastBlock: wire.BlockHeader{
				PrevBlock: chainhash.Hash{4},
			},
		},

		{
			// Scenario in which the valid response we receive is of length, zero. We should return
			// query.ResponseErr.
			name: "response header length 0",
			resp: &wire.MsgHeaders{
				Headers: make([]*wire.BlockHeader, 0),
			},
			req: &headerQuery{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: []*chainhash.Hash{
						{1},
					},
				},
				startHeight:   1,
				initialHeight: 1,
				startHash:     chainhash.Hash{1},
				endHeight:     5,
				initialHash:   chainhash.Hash{1},
			},
			progress: query.ResponseErr,
			lastBlock: wire.BlockHeader{
				PrevBlock: chainhash.Hash{4},
			},
		},

		{
			// Scenario in which the  response received is at the request's initialHeight (lower bound height in
			// checkpoint request) but its first block's previous hash is not same as the checkpoint hash. It
			// should return query.ResponseErr.
			name: "response at initialHash has disconnected start Hash",
			resp: &wire.MsgHeaders{
				Headers: []*wire.BlockHeader{
					{
						PrevBlock: chainhash.Hash{2},
					},
					{
						PrevBlock: chainhash.Hash{3},
					},
					{
						PrevBlock: chainhash.Hash{4},
					},
				},
			},
			req: &headerQuery{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: []*chainhash.Hash{
						{1},
					},
				},
				startHeight:   1,
				initialHeight: 1,
				startHash:     chainhash.Hash{1},
				endHeight:     5,
				initialHash:   chainhash.Hash{1},
			},
			progress: query.ResponseErr,
			lastBlock: wire.BlockHeader{
				PrevBlock: chainhash.Hash{4},
			},
			peerDisconnected: true,
		},

		{
			// Scenario in which the response is not at the initial Hash (lower bound hash in the
			// checkpoint request)  but the response is complete and valid. It should return query.Finished.
			name: "response not at initialHash, valid complete headers",
			resp: &wire.MsgHeaders{
				Headers: []*wire.BlockHeader{
					{
						PrevBlock: chainhash.Hash{2},
					},
					{
						PrevBlock: chainhash.Hash{3},
					},
					{
						PrevBlock: chainhash.Hash{4},
					},
				},
			},
			req: &headerQuery{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: []*chainhash.Hash{
						{2},
					},
				},
				startHeight:   2,
				initialHeight: 1,
				startHash:     chainhash.Hash{2},
				endHeight:     5,
				initialHash:   chainhash.Hash{2},
			},
			progress: query.Finished,
			lastBlock: wire.BlockHeader{
				PrevBlock: chainhash.Hash{4},
			},
		},

		{
			// Scenario in which the response is not at initial hash (lower bound height in
			// checkpoint request) and the response is unfinished. The jobErr should be nil and return
			// finalRespNoProgress query.progress.
			name: "response not at initial Hash, unfinished response",
			resp: &wire.MsgHeaders{
				Headers: []*wire.BlockHeader{
					{
						PrevBlock: chainhash.Hash{2},
					},
					{
						PrevBlock: chainhash.Hash{3},
					},
					{
						PrevBlock: chainhash.Hash{4},
					},
				},
			},
			req: &headerQuery{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: []*chainhash.Hash{
						{2},
					},
				},
				startHeight:   2,
				initialHeight: 1,
				startHash:     chainhash.Hash{2},
				endHeight:     6,
				initialHash:   chainhash.Hash{2},
			},
			progress: query.UnFinishedRequest,
			lastBlock: wire.BlockHeader{
				PrevBlock: chainhash.Hash{5},
			},
		},

		{
			// Scenario in which the response length is greater than expected. Peer should be
			// disconnected and the method should return query.ResponseErr.
			name: "response header length more than expected",
			resp: &wire.MsgHeaders{
				Headers: []*wire.BlockHeader{
					{
						PrevBlock: chainhash.Hash{1},
					},
					{
						PrevBlock: chainhash.Hash{2},
					},
					{
						PrevBlock: chainhash.Hash{3},
					},
					{
						PrevBlock: chainhash.Hash{4},
					},
					{
						PrevBlock: chainhash.Hash{5},
					},
					{
						PrevBlock: chainhash.Hash{6},
					},
				},
			},
			req: &headerQuery{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: []*chainhash.Hash{
						{1},
					},
				},
				startHeight:   1,
				initialHeight: 1,
				startHash:     chainhash.Hash{1},
				endHeight:     6,
				initialHash:   chainhash.Hash{1},
			},
			progress: query.ResponseErr,
			lastBlock: wire.BlockHeader{
				PrevBlock: chainhash.Hash{5},
			},
			peerDisconnected: true,
		},

		{
			// Scenario in which response is complete and a valid header. Its start height is at the initial height.
			// progress should be query.Finished.
			name: "complete response valid headers",
			resp: &wire.MsgHeaders{
				Headers: []*wire.BlockHeader{
					{
						PrevBlock: chainhash.Hash{1},
					},
					{
						PrevBlock: chainhash.Hash{2},
					},
					{
						PrevBlock: chainhash.Hash{3},
					},
				},
			},
			req: &headerQuery{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: []*chainhash.Hash{
						{1},
					},
				},
				startHeight:   1,
				initialHeight: 1,
				startHash:     chainhash.Hash{1},
				endHeight:     4,
				initialHash:   chainhash.Hash{1},
			},
			progress: query.Finished,
			lastBlock: wire.BlockHeader{
				PrevBlock: chainhash.Hash{3},
			},
		},

		{
			// Scenario in which response is at initialHash and the response is incomplete.
			// It should return query.UnFinishedRequest.
			name: "response at initial hash, incomplete response, valid headers",
			resp: &wire.MsgHeaders{
				Headers: []*wire.BlockHeader{
					{
						PrevBlock: chainhash.Hash{1},
					},
					{
						PrevBlock: chainhash.Hash{2},
					},
					{
						PrevBlock: chainhash.Hash{3},
					},
				},
			},
			req: &headerQuery{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: []*chainhash.Hash{
						{1},
					},
				},
				startHeight:   1,
				initialHeight: 1,
				startHash:     chainhash.Hash{1},
				endHeight:     6,
				initialHash:   chainhash.Hash{1},
			},
			progress: query.UnFinishedRequest,
			lastBlock: wire.BlockHeader{
				PrevBlock: chainhash.Hash{5},
			},
		},

		{
			// Scenario in which response is incomplete but valid. The new response's start height created in this
			// scenario is present in the hdrTipResponseMap. The startHeight is 6 and response at height 6 has been
			// preveiously written in to the hdrTipResponse map for the sake of this test.
			name: "incomplete response, valid headers, new resp in hdrTipToResponse map",
			resp: &wire.MsgHeaders{
				Headers: []*wire.BlockHeader{
					{
						PrevBlock: chainhash.Hash{1},
					},
					{
						PrevBlock: chainhash.Hash{2},
					},
					{
						PrevBlock: chainhash.Hash{3},
					},
					{
						PrevBlock: chainhash.Hash{4},
					},
					{
						PrevBlock: chainhash.Hash{5},
					},
				},
			},
			req: &headerQuery{
				message: &wire.MsgGetHeaders{
					BlockLocatorHashes: []*chainhash.Hash{
						{1},
					},
				},
				startHeight:   1,
				initialHeight: 1,
				startHash:     chainhash.Hash{1},
				endHeight:     10,
				initialHash:   chainhash.Hash{1},
			},
			progress: query.Finished,
			lastBlock: wire.BlockHeader{
				PrevBlock: chainhash.Hash{9},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// set up block manager.
			bm, _, _, err := setupBlockManager(t)
			require.NoError(t, err)

			var oldReqStartHeight int32

			bm.hdrTipToResponse[0] = &headersMsg{
				headers: &wire.MsgHeaders{},
			}
			bm.hdrTipToResponse[6] = &headersMsg{
				headers: &wire.MsgHeaders{},
			}

			fakePeer, err := peer.NewOutboundPeer(&peer.Config{}, "fake:123")
			require.NoError(t, err)

			blkHdrquery := &CheckpointedBlockHeadersQuery{
				blockMgr: bm,
			}
			req := tc.req
			r, ok := tc.req.(*headerQuery)
			if ok {
				reqMessage, ok := req.Message().(*wire.MsgGetHeaders)
				if !ok {
					t.Fatalf("request message not of type wire.MsgGetHeaders")
				}
				reqMessage.HashStop = tc.lastBlock.BlockHash()
				req = r
				oldReqStartHeight = r.startHeight
			}
			actualProgress := blkHdrquery.handleResponse(req, tc.resp, &ServerPeer{
				Peer: fakePeer,
			})

			if tc.progress != actualProgress {
				t.Fatalf("unexpected progress.Expected: %v but got: %v", tc.progress, actualProgress)
			}

			if actualProgress == query.UnFinishedRequest {
				resp := tc.resp.(*wire.MsgHeaders)
				request := req.(*headerQuery)
				if request.startHash != resp.Headers[len(resp.Headers)-1].BlockHash() {
					t.Fatalf("unexpected new startHash")
				}

				if request.startHeight != oldReqStartHeight+int32(len(resp.Headers)) {
					t.Fatalf("unexpected new start height")
				}

				requestMessage := req.Message().(*wire.MsgGetHeaders)

				if *requestMessage.BlockLocatorHashes[0] != request.startHash {
					t.Fatalf("unexpected new blockLocator")
				}
			}

			assertPeerDisconnected(tc.peerDisconnected, fakePeer, t)
		})
	}
}

// testEqualReqMessage tests if two query.Request are same.
func testEqualReqMessage(a, b *query.Request, t *testing.T) {
	aMessage := a.Req.(*headerQuery)
	bMessage := b.Req.(*headerQuery)

	if aMessage.startHeight != bMessage.startHeight {
		t.Fatalf("dissimilar startHeight")
	}
	if aMessage.startHash != bMessage.startHash {
		t.Fatalf("dissimilar startHash")
	}
	if aMessage.endHeight != bMessage.endHeight {
		t.Fatalf("dissimilar endHash")
	}
	if aMessage.initialHash != bMessage.initialHash {
		t.Fatalf("dissimilar initialHash")
	}

	aMessageGetHeaders := aMessage.Message().(*wire.MsgGetHeaders)
	bMessageGetHeaders := bMessage.Message().(*wire.MsgGetHeaders)

	if !reflect.DeepEqual(aMessageGetHeaders.BlockLocatorHashes, bMessageGetHeaders.BlockLocatorHashes) {
		t.Fatalf("dissimilar blocklocator hash")
	}

	if aMessageGetHeaders.HashStop != bMessageGetHeaders.HashStop {
		t.Fatalf("dissimilar hashstop")
	}
	if a.Req.PriorityIndex() != b.Req.PriorityIndex() {
		t.Fatalf("dissimilar priority index")
	}
}
