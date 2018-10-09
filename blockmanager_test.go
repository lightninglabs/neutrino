package neutrino

import (
	"encoding/binary"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightninglabs/neutrino/headerfs"
)

// TestBlockManagerGetCheckpointedCFHeaders tests that the block manager is
// able to handle multiple checkpointed filter header query responses in
// parallel.
func TestBlockManagerGetCheckpointedCFHeaders(t *testing.T) {
	t.Parallel()

	// Set up the block and filter header stores.
	tempDir, err := ioutil.TempDir("", "neutrino")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %s", err)
	}
	defer os.RemoveAll(tempDir)

	db, err := walletdb.Create("bdb", tempDir+"/weks.db")
	if err != nil {
		t.Fatalf("Error opening DB: %s", err)
	}
	defer db.Close()

	hdrStore, err := headerfs.NewBlockHeaderStore(
		tempDir, db, &chaincfg.SimNetParams,
	)
	if err != nil {
		t.Fatalf("Error creating block header store: %s", err)
	}

	cfStore, err := headerfs.NewFilterHeaderStore(
		tempDir, db, headerfs.RegularFilter, &chaincfg.SimNetParams,
	)
	if err != nil {
		t.Fatalf("Error creating filter header store: %s", err)
	}

	// Keep track of the filter headers and block headers. Since the
	// genesis headers are written automatically when the store is created,
	// we query it to add to the slices.
	genesisBlockHeader, _, err := hdrStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}

	var blockHeaders []headerfs.BlockHeader
	blockHeaders = append(blockHeaders, headerfs.BlockHeader{
		BlockHeader: genesisBlockHeader,
		Height:      0,
	})

	genesisFilterHeader, _, err := cfStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}

	var cfHeaders []chainhash.Hash
	cfHeaders = append(cfHeaders, *genesisFilterHeader)

	// Also keep track of the current filter header. We use this to
	// calculate the next filter header, as it commits to the previous.
	currentCFHeader := *genesisFilterHeader

	// checkpoints will be the checkpoints passed to
	// getCheckpointedCFHeaders.
	var checkpoints []*chainhash.Hash

	maxHeight := 50 * uint32(wire.CFCheckptInterval)
	for height := uint32(1); height <= maxHeight; height++ {
		header := heightToHeader(height)
		blockHeader := headerfs.BlockHeader{
			BlockHeader: header,
			Height:      height,
		}

		blockHeaders = append(blockHeaders, blockHeader)

		// It doesn't really matter what filter the filter header
		// commit to, so just use the height as a nonce.
		filterHash := chainhash.Hash{}
		binary.BigEndian.PutUint32(filterHash[:], height)
		currentCFHeader = chainhash.DoubleHashH(
			append(filterHash[:], currentCFHeader[:]...),
		)
		cfHeaders = append(cfHeaders, filterHash)

		// Each interval we must record a checkpoint.
		if height > 0 && height%wire.CFCheckptInterval == 0 {
			// We must make a copy of the current header.
			cfh := currentCFHeader
			checkpoints = append(checkpoints, &cfh)
		}

	}

	// Write all block headers but the genesis, since it is already in the
	// store.
	if err = hdrStore.WriteHeaders(blockHeaders[1:]...); err != nil {
		t.Fatalf("Error writing batch of headers: %s", err)
	}

	var wg sync.WaitGroup

	// Set up a chain service with a custom query batch method.
	cs := &ChainService{
		BlockHeaders: hdrStore,
	}
	cs.queryBatch = func(msgs []wire.Message, f func(*ServerPeer,
		wire.Message, wire.Message) bool, q <-chan struct{},
		qo ...QueryOption) {

		// Craft response for each message.
		for _, msg := range msgs {
			// Only GetCFHeaders expected.
			q, ok := msg.(*wire.MsgGetCFHeaders)
			if !ok {
				t.Fatalf("got unexpected message %T", msg)
			}

			// The start height must be set to a checkpoint
			// height+1.
			if q.StartHeight%wire.CFCheckptInterval != 1 {
				t.Fatalf("unexpexted start height %v",
					q.StartHeight)
			}

			var prevFilterHeader chainhash.Hash
			switch q.StartHeight {

			// If the start height is 1 the prevFilterHeader is set
			// to the genesis header.
			case 1:
				prevFilterHeader = *genesisFilterHeader

			// Otherwise we use one of the created checkpoints.
			default:
				j := q.StartHeight/wire.CFCheckptInterval - 1
				prevFilterHeader = *checkpoints[j]
			}

			resp := &wire.MsgCFHeaders{
				FilterType:       q.FilterType,
				StopHash:         q.StopHash,
				PrevFilterHeader: prevFilterHeader,
			}

			// Keep adding filter hashes until we reach the stop
			// hash.
			for h := q.StartHeight; ; h++ {
				resp.FilterHashes = append(resp.FilterHashes,
					&cfHeaders[h])

				blockHash := blockHeaders[h].BlockHash()
				if blockHash == q.StopHash {
					break
				}
			}

			// To ensure the block manager can handle responses
			// coming potentially out of order, we launch a
			// goroutine for each, and make it sleep a short while
			// before calling the response handler.
			wg.Add(1)
			go func(query, response wire.Message) {
				defer wg.Done()

				r := rand.Intn(1000)
				time.Sleep(time.Duration(r) * time.Millisecond)

				if !f(nil, query, response) {
					t.Fatalf("got response false")
				}
			}(q, resp)

		}
	}

	// Set up a blockManager with the chain service we defined...
	bm := blockManager{
		server: cs,
		blkHeaderProgressLogger: newBlockProgressLogger(
			"Processed", "block", log,
		),
		fltrHeaderProgessLogger: newBlockProgressLogger(
			"Verified", "filter header", log,
		),
	}
	bm.newHeadersSignal = sync.NewCond(&bm.newHeadersMtx)
	bm.newFilterHeadersSignal = sync.NewCond(&bm.newFilterHeadersMtx)

	// ...and call the get checkpointed cf headers method with the
	// checkpoints we created.
	bm.getCheckpointedCFHeaders(
		checkpoints, cfStore, wire.GCSFilterRegular,
	)

	// Wait for all the responses to be handled.
	wg.Wait()

	// Finally make sure the filter header tip is what we expect.
	tip, tipHeight, err := cfStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}

	if tipHeight != maxHeight {
		t.Fatalf("expected tip height to be %v, was %v", maxHeight,
			tipHeight)
	}

	lastCheckpoint := checkpoints[len(checkpoints)-1]
	if *tip != *lastCheckpoint {
		t.Fatalf("expected tip to be %v, was %v",
			lastCheckpoint, tip)
	}
}
