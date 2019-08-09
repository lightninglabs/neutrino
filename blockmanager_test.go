package neutrino

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil/gcs"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/blockntfns"
	"github.com/lightninglabs/neutrino/headerfs"
)

// maxHeight is the height we will generate filter headers up to. We use an odd
// number of checkpoints to ensure we can test cases where the block manager is
// only able to fetch filter headers for one checkpoint interval rather than
// two.
const maxHeight = 21 * uint32(wire.CFCheckptInterval)

// setupBlockManager initialises a blockManager to be used in tests.
func setupBlockManager() (*blockManager, headerfs.BlockHeaderStore,
	*headerfs.FilterHeaderStore, func(), error) {

	// Set up the block and filter header stores.
	tempDir, err := ioutil.TempDir("", "neutrino")
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("Failed to create "+
			"temporary directory: %s", err)
	}

	db, err := walletdb.Create("bdb", tempDir+"/weks.db")
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, nil, nil, nil, fmt.Errorf("Error opening DB: %s",
			err)
	}

	cleanUp := func() {
		db.Close()
		os.RemoveAll(tempDir)
	}

	hdrStore, err := headerfs.NewBlockHeaderStore(
		tempDir, db, &chaincfg.SimNetParams,
	)
	if err != nil {
		cleanUp()
		return nil, nil, nil, nil, fmt.Errorf("Error creating block "+
			"header store: %s", err)
	}

	cfStore, err := headerfs.NewFilterHeaderStore(
		tempDir, db, headerfs.RegularFilter, &chaincfg.SimNetParams,
		nil,
	)
	if err != nil {
		cleanUp()
		return nil, nil, nil, nil, fmt.Errorf("Error creating filter "+
			"header store: %s", err)
	}

	banStore, err := banman.NewStore(db)
	if err != nil {
		cleanUp()
		return nil, nil, nil, nil, fmt.Errorf("unable to initialize "+
			"ban store: %v", err)
	}

	// Set up a chain service for the block manager. Each test should set
	// custom query methods on this chain service.
	cs := &ChainService{
		chainParams:      chaincfg.SimNetParams,
		BlockHeaders:     hdrStore,
		RegFilterHeaders: cfStore,
		banStore:         banStore,
	}

	// Set up a blockManager with the chain service we defined.
	bm, err := newBlockManager(cs, nil)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("unable to create "+
			"blockmanager: %v", err)
	}

	return bm, hdrStore, cfStore, cleanUp, nil
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
// the next interval
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
func generateResponses(msgs []wire.Message,
	headers *headers) ([]*wire.MsgCFHeaders, error) {

	// Craft a response for each message.
	var responses []*wire.MsgCFHeaders
	for _, msg := range msgs {
		// Only GetCFHeaders expected.
		q, ok := msg.(*wire.MsgGetCFHeaders)
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
		testDesc := fmt.Sprintf("permute=%v, partial=%v, repeat=%v",
			test.permute, test.partialInterval, test.repeat)

		bm, hdrStore, cfStore, cleanUp, err := setupBlockManager()
		if err != nil {
			t.Fatalf("unable to set up ChainService: %v", err)
		}
		defer cleanUp()

		// Keep track of the filter headers and block headers. Since
		// the genesis headers are written automatically when the store
		// is created, we query it to add to the slices.
		genesisBlockHeader, _, err := hdrStore.ChainTip()
		if err != nil {
			t.Fatal(err)
		}

		genesisFilterHeader, _, err := cfStore.ChainTip()
		if err != nil {
			t.Fatal(err)
		}

		headers, err := generateHeaders(genesisBlockHeader,
			genesisFilterHeader, nil)
		if err != nil {
			t.Fatalf("unable to generate headers: %v", err)
		}

		// Write all block headers but the genesis, since it is already
		// in the store.
		if err = hdrStore.WriteHeaders(headers.blockHeaders[1:]...); err != nil {
			t.Fatalf("Error writing batch of headers: %s", err)
		}

		// We emulate the case where a few filter headers are already
		// written to the store by writing 1/3 of the first interval.
		if test.partialInterval {
			err = cfStore.WriteHeaders(
				headers.cfHeaders[1 : wire.CFCheckptInterval/3]...,
			)
			if err != nil {
				t.Fatalf("Error writing batch of headers: %s",
					err)
			}
		}

		// We set up a custom query batch method for this test, as we
		// will use this to feed the blockmanager with our crafted
		// responses.
		bm.server.queryBatch = func(msgs []wire.Message,
			f func(*ServerPeer, wire.Message, wire.Message) bool,
			q <-chan struct{}, qo ...QueryOption) {

			responses, err := generateResponses(msgs, headers)
			if err != nil {
				t.Fatalf("unable to generate responses: %v",
					err)
			}

			// We permute the response order if the test signals
			// that.
			perm := rand.Perm(len(responses))
			for i, v := range perm {
				index := i
				if test.permute {
					index = v
				}

				// Before sending we take a copy of the
				// message, as we cannot guarantee that it
				// won't be modified.
				r := *responses[index]

				// Let the blockmanager handle the message.
				if !f(nil, msgs[index], responses[index]) {
					t.Fatalf("got response false on "+
						"send of index %d: %v",
						index, testDesc)
				}

				// If we are not testing repeated responses, go
				// on to the next response.
				if !test.repeat {
					continue
				}

				// Otherwise resend the response we just sent.
				if !f(nil, msgs[index], &r) {
					t.Fatalf("got response false on "+
						"resend of index %d: %v",
						index, testDesc)
				}

			}
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
					t.Fatal("expected block connected " +
						"notification")
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
		if err != nil {
			t.Fatal(err)
		}

		if tipHeight != maxHeight {
			t.Fatalf("expected tip height to be %v, was %v",
				maxHeight, tipHeight)
		}

		lastCheckpoint := headers.checkpoints[len(headers.checkpoints)-1]
		if *tip != *lastCheckpoint {
			t.Fatalf("expected tip to be %v, was %v",
				lastCheckpoint, tip)
		}
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
		bm, hdrStore, cfStore, cleanUp, err := setupBlockManager()
		if err != nil {
			t.Fatalf("unable to set up ChainService: %v", err)
		}
		defer cleanUp()

		// Create a mock peer to prevent panics when attempting to ban
		// a peer that served an invalid filter header.
		mockPeer := newServerPeer(bm.server, false)
		mockPeer.Peer, err = peer.NewOutboundPeer(
			newPeerConfig(mockPeer), "127.0.0.1:8333",
		)
		if err != nil {
			t.Fatal(err)
		}

		// Keep track of the filter headers and block headers. Since
		// the genesis headers are written automatically when the store
		// is created, we query it to add to the slices.
		genesisBlockHeader, _, err := hdrStore.ChainTip()
		if err != nil {
			t.Fatal(err)
		}

		genesisFilterHeader, _, err := cfStore.ChainTip()
		if err != nil {
			t.Fatal(err)
		}
		// To emulate a full node serving us filter headers derived
		// from different genesis than what we have, we flip a bit in
		// the genesis filter header.
		if test.wrongGenesis {
			genesisFilterHeader[0] ^= 1
		}

		headers, err := generateHeaders(genesisBlockHeader,
			genesisFilterHeader,
			func(currentCFHeader *chainhash.Hash) {
				// If we are testing that each interval doesn't
				// line up properly with the previous, we flip
				// a bit in the current header before
				// calculating the next interval checkpoint.
				if test.intervalMisaligned {
					currentCFHeader[0] ^= 1
				}
			})
		if err != nil {
			t.Fatalf("unable to generate headers: %v", err)
		}

		// Write all block headers but the genesis, since it is already
		// in the store.
		if err = hdrStore.WriteHeaders(headers.blockHeaders[1:]...); err != nil {
			t.Fatalf("Error writing batch of headers: %s", err)
		}

		// We emulate the case where a few filter headers are already
		// written to the store by writing 1/3 of the first interval.
		if test.partialInterval {
			err = cfStore.WriteHeaders(
				headers.cfHeaders[1 : wire.CFCheckptInterval/3]...,
			)
			if err != nil {
				t.Fatalf("Error writing batch of headers: %s",
					err)
			}
		}

		bm.server.queryBatch = func(msgs []wire.Message,
			f func(*ServerPeer, wire.Message, wire.Message) bool,
			q <-chan struct{}, qo ...QueryOption) {

			responses, err := generateResponses(msgs, headers)
			if err != nil {
				t.Fatalf("unable to generate responses: %v",
					err)
			}

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
			// hashes, we flip a bit to corrup them, regardless of
			// whether we are testing misaligned intervals.
			if test.invalidPrevHash {
				for i := range responses {
					if i == 0 {
						continue
					}
					responses[i].PrevFilterHeader[1] ^= 1
				}
			}

			// Check that the success of the callback match what we
			// expect.
			for i := range responses {
				success := f(mockPeer, msgs[i], responses[i])
				if i == test.firstInvalid {
					if success {
						t.Fatalf("expected interval "+
							"%d to be invalid", i)
					}
					break
				}

				if !success {
					t.Fatalf("expected interval %d to be "+
						"valid", i)
				}
			}
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
					t.Fatal("expected block connected " +
						"notification")
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
	for _, peer := range badPeers {
		_, ok := remBad[peer]
		if !ok {
			return fmt.Errorf("did not expect %v to be bad", peer)
		}
		delete(remBad, peer)
	}

	if len(remBad) != 0 {
		return fmt.Errorf("did expect more bad peers")
	}

	return nil
}

type mockQueryAccess struct {
	answers map[string]wire.Message
}

func (m *mockQueryAccess) queryAllPeers(
	queryMsg wire.Message,
	checkResponse func(sp *ServerPeer, resp wire.Message,
		quit chan<- struct{}, peerQuit chan<- struct{}),
	options ...QueryOption) {

	for p, resp := range m.answers {
		pp, err := peer.NewOutboundPeer(&peer.Config{}, p)
		if err != nil {
			panic(err)
		}

		sp := &ServerPeer{
			Peer: pp,
		}
		checkResponse(sp, resp, make(chan struct{}), make(chan struct{}))
	}
}

var _ QueryAccess = (*mockQueryAccess)(nil)

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
			"bad:1": struct{}{},
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
		cs := &ChainService{
			BlockHeaders: blockHeaders,
		}

		// We set up the mock QueryAccess to only respond according to
		// the active testcase.
		mock := &mockQueryAccess{
			answers: make(map[string]wire.Message),
		}
		for _, peer := range peers {
			test.filterAnswers(peer, mock.answers)
		}

		// For the CFHeaders, we pretend all peers responded with the same
		// filter headers.
		msg := &wire.MsgCFHeaders{
			FilterType:       fType,
			StopHash:         stopHash,
			PrevFilterHeader: prev,
		}

		for i := uint32(0); i < 2*badIndex; i++ {
			msg.AddCFHash(&filterHash)
		}

		headers := make(map[string]*wire.MsgCFHeaders)
		for _, peer := range peers {
			headers[peer] = msg
		}

		bm := &blockManager{
			server:  cs,
			queries: mock,
		}

		// Now trying to detect which peers are bad, we should detect the
		// bad ones.
		badPeers, err := bm.detectBadPeers(
			headers, targetIndex, badIndex, fType,
		)
		if err != nil {
			t.Fatalf("failed to detect bad peers: %v", err)
		}

		if err := assertBadPeers(expBad, badPeers); err != nil {
			t.Fatal(err)
		}
	}
}
