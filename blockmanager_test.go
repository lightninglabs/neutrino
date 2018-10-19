package neutrino

import (
	"encoding/binary"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightninglabs/neutrino/headerfs"
)

// TestBlockManagerInitialInterval tests that the block manager is able to
// handle checkpointed filter header query responses in out of order, and when
// a partial interval is already written to the store.
func TestBlockManagerInitialInterval(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		// permute indicates whether responses should be permutated.
		permute bool

		// partialInterval indicates whether we should write parts of
		// the first checkpoint interval to the filter header store
		// before starting the test.
		partialInterval bool
	}{
		{
			permute:         false,
			partialInterval: false,
		},
		{
			permute:         false,
			partialInterval: true,
		},
		{
			permute:         true,
			partialInterval: false,
		},
		{
			permute:         true,
			partialInterval: true,
		},
	}

	for _, test := range testCases {

		// Set up the block and filter header stores.
		tempDir, err := ioutil.TempDir("", "neutrino")
		if err != nil {
			t.Fatalf("Failed to create temporary directory: %s",
				err)
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
			tempDir, db, headerfs.RegularFilter,
			&chaincfg.SimNetParams,
		)
		if err != nil {
			t.Fatalf("Error creating filter header store: %s", err)
		}

		// Keep track of the filter headers and block headers. Since
		// the genesis headers are written automatically when the store
		// is created, we query it to add to the slices.
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

		var cfHeaders []headerfs.FilterHeader
		cfHeaders = append(cfHeaders, headerfs.FilterHeader{
			HeaderHash: genesisBlockHeader.BlockHash(),
			FilterHash: *genesisFilterHeader,
			Height:     0,
		})

		// The filter hashes (not the filter headers!) will be sent as
		// part of the CFHeaders response, so we also keep track of
		// them
		genesisFilter, err := builder.BuildBasicFilter(
			chaincfg.SimNetParams.GenesisBlock, nil,
		)
		if err != nil {
			t.Fatalf("unable to build genesis filter: %v", err)
		}

		genesisFilterHash, err := builder.GetFilterHash(genesisFilter)
		if err != nil {
			t.Fatal("fail")
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

		maxHeight := 20 * uint32(wire.CFCheckptInterval)
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
			if height > 0 && height%wire.CFCheckptInterval == 0 {
				// We must make a copy of the current header to
				// avoid mutation.
				cfh := currentCFHeader
				checkpoints = append(checkpoints, &cfh)
			}

		}

		// Write all block headers but the genesis, since it is already
		// in the store.
		if err = hdrStore.WriteHeaders(blockHeaders[1:]...); err != nil {
			t.Fatalf("Error writing batch of headers: %s", err)
		}

		// We emulate the case where a few filter headers are already
		// written to the store by writing 1/3 of the first interval.
		if test.partialInterval {
			err = cfStore.WriteHeaders(
				cfHeaders[1 : wire.CFCheckptInterval/3]...,
			)
			if err != nil {
				t.Fatalf("Error writing batch of headers: %s",
					err)
			}
		}

		// Set up a chain service with a custom query batch method.
		cs := &ChainService{
			BlockHeaders: hdrStore,
		}
		cs.queryBatch = func(msgs []wire.Message, f func(*ServerPeer,
			wire.Message, wire.Message) bool, q <-chan struct{},
			qo ...QueryOption) {

			// Craft response for each message.
			var responses []*wire.MsgCFHeaders
			for _, msg := range msgs {
				// Only GetCFHeaders expected.
				q, ok := msg.(*wire.MsgGetCFHeaders)
				if !ok {
					t.Fatalf("got unexpected message %T",
						msg)
				}

				// The start height must be set to a checkpoint
				// height+1.
				if q.StartHeight%wire.CFCheckptInterval != 1 {
					t.Fatalf("unexpexted start height %v",
						q.StartHeight)
				}

				var prevFilterHeader chainhash.Hash
				switch q.StartHeight {

				// If the start height is 1 the
				// prevFilterHeader is set to the genesis
				// header.
				case 1:
					prevFilterHeader = *genesisFilterHeader

				// Otherwise we use one of the created
				// checkpoints.
				default:
					j := q.StartHeight/wire.CFCheckptInterval - 1
					prevFilterHeader = *checkpoints[j]
				}

				resp := &wire.MsgCFHeaders{
					FilterType:       q.FilterType,
					StopHash:         q.StopHash,
					PrevFilterHeader: prevFilterHeader,
				}

				// Keep adding filter hashes until we reach the
				// stop hash.
				for h := q.StartHeight; ; h++ {
					resp.FilterHashes = append(
						resp.FilterHashes, &filterHashes[h],
					)

					blockHash := blockHeaders[h].BlockHash()
					if blockHash == q.StopHash {
						break
					}
				}

				responses = append(responses, resp)
			}

			// We permute the response order if the test signals
			// that.
			perm := rand.Perm(len(responses))
			for i, v := range perm {
				index := i
				if test.permute {
					index = v
				}
				if !f(nil, msgs[index], responses[index]) {
					t.Fatalf("got response false")
				}
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

		// Finally make sure the filter header tip is what we expect.
		tip, tipHeight, err := cfStore.ChainTip()
		if err != nil {
			t.Fatal(err)
		}

		if tipHeight != maxHeight {
			t.Fatalf("expected tip height to be %v, was %v",
				maxHeight, tipHeight)
		}

		lastCheckpoint := checkpoints[len(checkpoints)-1]
		if *tip != *lastCheckpoint {
			t.Fatalf("expected tip to be %v, was %v",
				lastCheckpoint, tip)
		}
	}
}
