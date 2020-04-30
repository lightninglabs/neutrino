package neutrino

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/gcs"
	"github.com/lightninglabs/neutrino/headerfs"
)

type MockChainClient struct {
	getBlockResponse     map[chainhash.Hash]*btcutil.Block
	getBlockHashResponse map[int64]*chainhash.Hash
	getBestBlockHash     *chainhash.Hash
	getBestBlockHeight   int32
	getCFilterResponse   map[chainhash.Hash]*gcs.Filter
}

func NewMockChainClient() *MockChainClient {
	return &MockChainClient{
		getBlockResponse:     make(map[chainhash.Hash]*btcutil.Block),
		getBlockHashResponse: make(map[int64]*chainhash.Hash),
		getCFilterResponse:   make(map[chainhash.Hash]*gcs.Filter),
	}
}

func (c *MockChainClient) SetBlock(hash *chainhash.Hash, block *btcutil.Block) {
	c.getBlockResponse[*hash] = block
}

func (c *MockChainClient) GetBlockFromNetwork(blockHash chainhash.Hash,
	options ...QueryOption) (*btcutil.Block, error) {
	return c.getBlockResponse[blockHash], nil
}

func (c *MockChainClient) SetBlockHash(height int64, hash *chainhash.Hash) {
	c.getBlockHashResponse[height] = hash
}

func (c *MockChainClient) GetBlockHash(height int64) (*chainhash.Hash, error) {
	return c.getBlockHashResponse[height], nil
}

func (c *MockChainClient) SetBestSnapshot(hash *chainhash.Hash, height int32) {
	c.getBestBlockHash = hash
	c.getBestBlockHeight = height
}

func (c *MockChainClient) BestSnapshot() (*headerfs.BlockStamp, error) {
	return &headerfs.BlockStamp{
		Hash:   *c.getBestBlockHash,
		Height: c.getBestBlockHeight,
	}, nil
}

func (c *MockChainClient) blockFilterMatches(ro *rescanOptions,
	blockHash *chainhash.Hash) (bool, error) {
	return true, nil
}

func makeTestInputWithScript() *InputWithScript {
	hash, _ := chainhash.NewHashFromStr("87a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03")
	pkScript := []byte("76a91471d7dd96d9edda09180fe9d57a477b5acc9cad118")

	return &InputWithScript{
		OutPoint: wire.OutPoint{
			Hash:  *hash,
			Index: 0,
		},
		PkScript: pkScript,
	}

}

// TestFindSpends tests that findSpends properly returns spend reports.
func TestFindSpends(t *testing.T) {
	height := uint32(100000)

	reqs := []*GetUtxoRequest{
		{
			Input:       makeTestInputWithScript(),
			BirthHeight: height,
		},
	}

	// Test that finding spends with an empty outpoints index returns no
	// spends.
	r := newBatchSpendReporter()
	spends := r.notifySpends(&Block100000, height)
	if len(spends) != 0 {
		t.Fatalf("unexpected number of spend reports -- "+
			"want %d, got %d", 0, len(spends))
	}

	// Now, add the test outpoint to the outpoint index.
	r.addNewRequests(reqs)

	// Ensure that a spend report is now returned.
	spends = r.notifySpends(&Block100000, height)
	if len(spends) != 1 {
		t.Fatalf("unexpected number of spend reports -- "+
			"want %d, got %d", 1, len(spends))
	}
}

// TestFindInitialTransactions tests that findInitialTransactions properly
// returns the transaction corresponding to an output if it is found in the
// given block.
func TestFindInitialTransactions(t *testing.T) {
	hash, _ := chainhash.NewHashFromStr("e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d")
	outpoint := &wire.OutPoint{Hash: *hash, Index: 0}
	pkScript := []byte("76a91439aa3d569e06a1d7926dc4be1193c99bf2eb9ee08")
	height := uint32(100000)

	reqs := []*GetUtxoRequest{
		{
			Input: &InputWithScript{
				OutPoint: *outpoint,
				PkScript: pkScript,
			},
			BirthHeight: height,
		},
	}

	// First, try to find the outpoint within the block.
	r := newBatchSpendReporter()
	initialTxns := r.findInitialTransactions(&Block100000, reqs, height)
	if len(initialTxns) != 1 {
		t.Fatalf("unexpected number of spend reports -- "+
			"want %v, got %v", 1, len(initialTxns))
	}

	output := initialTxns[*outpoint]
	if output == nil || output.Output == nil || output.Output.Value != 1000000 {
		t.Fatalf("Expected spend report to contain initial output -- "+
			"instead got: %v", output)
	}

	// Now, modify the output index such that is invalid.
	outpoint.Index = 1

	// Try to find the invalid outpoint in the same block.
	r = newBatchSpendReporter()
	initialTxns = r.findInitialTransactions(&Block100000, reqs, height)
	if len(initialTxns) != 1 {
		t.Fatalf("unexpected number of spend reports -- "+
			"want %v, got %v", 1, len(initialTxns))
	}

	// The spend report should be nil since the output index is invalid.
	output = initialTxns[*outpoint]
	if output != nil {
		t.Fatalf("Expected spend report to be nil since the output index "+
			"is invalid, got %v", output)
	}

	// Finally, restore the valid output index, but modify the txid.
	outpoint.Index = 0
	outpoint.Hash[0] ^= 0x01

	// Try to find the outpoint with an invalid txid in the same block.
	r = newBatchSpendReporter()
	initialTxns = r.findInitialTransactions(&Block100000, reqs, height)
	if len(initialTxns) != 1 {
		t.Fatalf("unexpected number of spend reports -- "+
			"want %v, got %v", 1, len(initialTxns))
	}

	// Again, the spend report should be nil because of the invalid txid.
	output = initialTxns[*outpoint]
	if output != nil {
		t.Fatalf("Expected spend report to be nil since the txid "+
			"is not in block, got %v", output)
	}
}

// TestDequeueAtHeight asserts the correct behavior of various orderings of
// enqueuing requests and dequeueing that could arise. Predominately, this
// ensures that dequeueing heights lower than what has already been dequeued
// will not return requests, as they should be moved internally to the nextBatch
// slice.
func TestDequeueAtHeight(t *testing.T) {
	mockChainClient := NewMockChainClient()
	scanner := NewUtxoScanner(&UtxoScannerConfig{
		GetBlock:           mockChainClient.GetBlockFromNetwork,
		GetBlockHash:       mockChainClient.GetBlockHash,
		BestSnapshot:       mockChainClient.BestSnapshot,
		BlockFilterMatches: mockChainClient.blockFilterMatches,
	})

	// Add the requests in order of their block heights.
	req100000, err := scanner.Enqueue(makeTestInputWithScript(), 100000, func(height uint32) {})
	if err != nil {
		t.Fatalf("unable to enqueue scan request: %v", err)
	}
	req100001, err := scanner.Enqueue(makeTestInputWithScript(), 100001, func(height uint32) {})
	if err != nil {
		t.Fatalf("unable to enqueue scan request: %v", err)
	}

	// Dequeue the heights in the same order, this should return both
	// requests without failure.

	reqs := scanner.dequeueAtHeight(100000)
	if len(reqs) != 1 {
		t.Fatalf("Unexpected number of requests returned -- "+
			"want %v, got %v", 1, len(reqs))
	}
	if !reflect.DeepEqual(reqs[0], req100000) {
		t.Fatalf("Unexpected request returned -- "+
			"want %v, got %v", reqs[0], req100000)
	}

	// We've missed block 100000 by this point so only return 100001.
	reqs = scanner.dequeueAtHeight(100001)
	if len(reqs) != 1 {
		t.Fatalf("Unexpected number of requests returned -- "+
			"want %v, got %v", 1, len(reqs))
	}
	if !reflect.DeepEqual(reqs[0], req100001) {
		t.Fatalf("Unexpected request returned -- "+
			"want %v, got %v", reqs[0], req100001)
	}

	// Now, add the requests in order of their block heights.
	req100000, err = scanner.Enqueue(makeTestInputWithScript(), 100000, func(height uint32) {})
	if err != nil {
		t.Fatalf("unable to enqueue scan request: %v", err)
	}
	req100001, err = scanner.Enqueue(makeTestInputWithScript(), 100001, func(height uint32) {})
	if err != nil {
		t.Fatalf("unable to enqueue scan request: %v", err)
	}

	// We've missed block 100000 by this point so only return 100001.
	reqs = scanner.dequeueAtHeight(100001)
	if len(reqs) != 1 {
		t.Fatalf("Unexpected number of requests returned -- "+
			"want %v, got %v", 1, len(reqs))
	}
	if !reflect.DeepEqual(reqs[0], req100001) {
		t.Fatalf("Unexpected request returned -- "+
			"want %v, got %v", reqs[0], req100001)
	}

	// Try to request requests at height 100000, which should not return a
	// request since we've already passed it.
	reqs = scanner.dequeueAtHeight(100000)
	if len(reqs) != 0 {
		t.Fatalf("Unexpected number of requests returned -- "+
			"want %v, got %v", 0, len(reqs))
	}

	// Now, add the requests out of order wrt. their block heights.
	req100001, err = scanner.Enqueue(makeTestInputWithScript(), 100001, func(height uint32) {})
	if err != nil {
		t.Fatalf("unable to enqueue scan request: %v", err)
	}
	req100000, err = scanner.Enqueue(makeTestInputWithScript(), 100000, func(height uint32) {})
	if err != nil {
		t.Fatalf("unable to enqueue scan request: %v", err)
	}

	// Dequeue the heights in the correct order, this should return both
	// requests without failure.

	reqs = scanner.dequeueAtHeight(100000)
	if len(reqs) != 1 {
		t.Fatalf("Unexpected number of requests returned -- "+
			"want %v, got %v", 1, len(reqs))
	}
	if !reflect.DeepEqual(reqs[0], req100000) {
		t.Fatalf("Unexpected request returned -- "+
			"want %v, got %v", reqs[0], req100000)
	}

	reqs = scanner.dequeueAtHeight(100001)
	if len(reqs) != 1 {
		t.Fatalf("Unexpected number of requests returned -- "+
			"want %v, got %v", 1, len(reqs))
	}
	if !reflect.DeepEqual(reqs[0], req100001) {
		t.Fatalf("Unexpected request returned -- "+
			"want %v, got %v", reqs[0], req100001)
	}

	// Again, add the requests out of order wrt. their block heights.
	req100001, err = scanner.Enqueue(makeTestInputWithScript(), 100001, func(height uint32) {})
	if err != nil {
		t.Fatalf("unable to enqueue scan request: %v", err)
	}
	req100000, err = scanner.Enqueue(makeTestInputWithScript(), 100000, func(height uint32) {})
	if err != nil {
		t.Fatalf("unable to enqueue scan request: %v", err)
	}

	// We've missed block 100000 by this point so only return 100001.
	reqs = scanner.dequeueAtHeight(100001)
	if len(reqs) != 1 {
		t.Fatalf("Unexpected number of requests returned -- "+
			"want %v, got %v", 1, len(reqs))
	}
	if !reflect.DeepEqual(reqs[0], req100001) {
		t.Fatalf("Unexpected request returned -- "+
			"want %v, got %v", reqs[0], req100001)
	}

	// Try to request requests at height 100000, which should not return a
	// request since we've already passed it.
	reqs = scanner.dequeueAtHeight(100000)
	if len(reqs) != 0 {
		t.Fatalf("Unexpected number of requests returned -- "+
			"want %v, got %v", 0, len(reqs))
	}
}

// TestUtxoScannerScanBasic tests that enqueueing a spend request at the height
// of the spend returns a correct spend report.
func TestUtxoScannerScanBasic(t *testing.T) {
	mockChainClient := NewMockChainClient()

	block99999Hash := Block99999.BlockHash()
	mockChainClient.SetBlockHash(99999, &block99999Hash)
	mockChainClient.SetBlock(&block99999Hash, btcutil.NewBlock(&Block99999))

	block100000Hash := Block100000.BlockHash()
	mockChainClient.SetBlockHash(100000, &block100000Hash)
	mockChainClient.SetBlock(&block100000Hash, btcutil.NewBlock(&Block100000))
	mockChainClient.SetBestSnapshot(&block100000Hash, 100000)

	scanner := NewUtxoScanner(&UtxoScannerConfig{
		GetBlock:           mockChainClient.GetBlockFromNetwork,
		GetBlockHash:       mockChainClient.GetBlockHash,
		BestSnapshot:       mockChainClient.BestSnapshot,
		BlockFilterMatches: mockChainClient.blockFilterMatches,
	})
	scanner.Start()
	defer scanner.Stop()

	var (
		spendReport *SpendReport
		scanErr     error
	)

	var progressPoints []uint32
	req, err := scanner.Enqueue(makeTestInputWithScript(), 99999, func(height uint32) {
		progressPoints = append(progressPoints, height)
	})
	if err != nil {
		t.Fatalf("unable to enqueue utxo scan request: %v", err)
	}

	spendReport, scanErr = req.Result(nil)
	if scanErr != nil {
		t.Fatalf("unable to complete scan for utxo: %v", scanErr)
	}

	if spendReport == nil || spendReport.SpendingTx == nil {
		t.Fatalf("Expected scanned output to be spent -- "+
			"scan report: %v", spendReport)
	}

	// We scanned two blocks, we should have only one progress event.
	expectedProgress := []uint32{99999}
	if !reflect.DeepEqual(progressPoints, expectedProgress) {
		t.Fatalf("wrong progress during rescan, expected %v got: %v",
			expectedProgress, progressPoints)
	}
}

// TestUtxoScannerScanAddBlocks tests that adding new blocks to neutrino's view
// of the best snapshot properly dispatches spend reports. Internally, this
// tests that the rescan detects a difference in the original best height and
// the best height after a rescan, and then continues scans up to the new tip.
func TestUtxoScannerScanAddBlocks(t *testing.T) {
	mockChainClient := NewMockChainClient()

	block99999Hash := Block99999.BlockHash()
	mockChainClient.SetBlockHash(99999, &block99999Hash)
	mockChainClient.SetBlock(&block99999Hash, btcutil.NewBlock(&Block99999))
	mockChainClient.SetBestSnapshot(&block99999Hash, 99999)

	block100000Hash := Block100000.BlockHash()
	mockChainClient.SetBlockHash(100000, &block100000Hash)
	mockChainClient.SetBlock(&block100000Hash, btcutil.NewBlock(&Block100000))

	var snapshotLock sync.Mutex
	waitForSnapshot := make(chan struct{})

	scanner := NewUtxoScanner(&UtxoScannerConfig{
		GetBlock:     mockChainClient.GetBlockFromNetwork,
		GetBlockHash: mockChainClient.GetBlockHash,
		BestSnapshot: func() (*headerfs.BlockStamp, error) {
			<-waitForSnapshot
			snapshotLock.Lock()
			defer snapshotLock.Unlock()

			return mockChainClient.BestSnapshot()
		},
		BlockFilterMatches: mockChainClient.blockFilterMatches,
	})
	scanner.Start()
	defer scanner.Stop()

	var (
		spendReport *SpendReport
		scanErr     error
	)

	var progressPoints []uint32
	req, err := scanner.Enqueue(makeTestInputWithScript(), 99999, func(height uint32) {
		progressPoints = append(progressPoints, height)
	})
	if err != nil {
		t.Fatalf("unable to enqueue scan request: %v", err)
	}

	// The utxoscanner should currently be waiting for the block stamp at
	// height 99999. Signaling will cause the initial scan to finish and
	// block while querying again for the updated chain tip.
	waitForSnapshot <- struct{}{}

	// Now, add the successor block at height 100000 and update the best
	// snapshot..
	snapshotLock.Lock()
	mockChainClient.SetBestSnapshot(&block100000Hash, 100000)
	snapshotLock.Unlock()

	// The rescan should now be waiting for stamp 100000, signal to allow
	// the rescan to detect the added block and perform another pass.
	// Signal one more for the final query scan makes before exiting.
	waitForSnapshot <- struct{}{}
	waitForSnapshot <- struct{}{}

	spendReport, scanErr = req.Result(nil)
	if scanErr != nil {
		t.Fatalf("unable to complete scan for utxo: %v", scanErr)
	}

	if spendReport == nil || spendReport.SpendingTx == nil {
		t.Fatalf("Expected scanned output to be spent -- "+
			"scan report: %v", spendReport)
	}

	// We scanned two blocks, we should have only one progress event.
	expectedProgress := []uint32{99999}
	if !reflect.DeepEqual(progressPoints, expectedProgress) {
		t.Fatalf("wrong progress during rescan, expected %v got: %v",
			expectedProgress, progressPoints)
	}
}

// TestUtxoScannerCancelRequest tests the ability to cancel pending GetUtxo
// requests, as well as the scanners ability to exit and cancel request when
// stopped during a batch scan.
func TestUtxoScannerCancelRequest(t *testing.T) {
	mockChainClient := NewMockChainClient()

	block100000Hash := Block100000.BlockHash()
	mockChainClient.SetBlockHash(100000, &block100000Hash)
	mockChainClient.SetBlock(&block100000Hash, btcutil.NewBlock(&Block100000))
	mockChainClient.SetBestSnapshot(&block100000Hash, 100000)

	fetchErr := errors.New("cannot fetch block")

	// Create a mock function that will block when the utxoscanner tries to
	// retrieve a block from the network. It will return fetchErr when it
	// finally returns.
	block := make(chan struct{})
	scanner := NewUtxoScanner(&UtxoScannerConfig{
		GetBlock: func(chainhash.Hash, ...QueryOption,
		) (*btcutil.Block, error) {
			<-block
			return nil, fetchErr
		},
		GetBlockHash:       mockChainClient.GetBlockHash,
		BestSnapshot:       mockChainClient.BestSnapshot,
		BlockFilterMatches: mockChainClient.blockFilterMatches,
	})

	scanner.Start()
	defer scanner.Stop()

	// Add the requests in order of their block heights.
	req100000, err := scanner.Enqueue(makeTestInputWithScript(), 100000, func(height uint32) {})
	if err != nil {
		t.Fatalf("unable to enqueue scan request: %v", err)
	}
	req100001, err := scanner.Enqueue(makeTestInputWithScript(), 100001, func(height uint32) {})
	if err != nil {
		t.Fatalf("unable to enqueue scan request: %v", err)
	}

	// Spawn our first task with a cancel chan, which we'll test to make
	// sure it can break away early.
	cancel100000 := make(chan struct{})
	err100000 := make(chan error, 1)
	go func() {
		_, err := req100000.Result(cancel100000)
		err100000 <- err
	}()

	// Spawn our second task without a cancel chan, we'll be testing it's
	// ability to break if the scanner is stopped.
	err100001 := make(chan error, 1)
	go func() {
		_, err := req100001.Result(nil)
		err100001 <- err
	}()

	// Check that neither succeed without any further action.
	select {
	case <-err100000:
		t.Fatalf("getutxo should not have been cancelled yet")
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case <-err100001:
		t.Fatalf("getutxo should not have been cancelled yet")
	case <-time.After(50 * time.Millisecond):
	}

	// Cancel the first request, which should cause it to return
	// ErrGetUtxoCancelled.
	close(cancel100000)

	select {
	case err := <-err100000:
		if err != ErrGetUtxoCancelled {
			t.Fatalf("unexpected error returned "+
				"from Result, want: %v, got %v",
				ErrGetUtxoCancelled, err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("getutxo should have been cancelled")
	}

	// The second task shouldn't have been started yet, and should deliver a
	// message since it wasn't tied to the same cancel chan.
	select {
	case <-err100001:
		t.Fatalf("getutxo should not have been cancelled yet")
	case <-time.After(50 * time.Millisecond):
	}

	// Spawn a goroutine to stop the scanner, we add a wait group to make
	// sure it cleans up at the end of the test.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner.Stop()
	}()

	// The second request should be cancelled as soon as the utxoscanner
	// begins shut down, returning ErrShuttingDown.
	select {
	case err := <-err100001:
		if err != ErrShuttingDown {
			t.Fatalf("unexpected error returned "+
				"from Result, want: %v, got %v",
				ErrShuttingDown, err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("getutxo should have been cancelled")
	}

	// Ensure that GetBlock gets unblocked so the batchManager can properly
	// exit.
	select {
	case block <- struct{}{}:
	default:
	}

	// Wait for the utxo scanner to exit fully.
	wg.Wait()
}

// Block99999 defines block 99,999 of the main chain. It is used to test a
// rescan consisting of multiple blocks.
var Block99999 = wire.MsgBlock{
	Header: wire.BlockHeader{
		Version: 1,
		PrevBlock: chainhash.Hash([32]byte{
			0x1d, 0x35, 0xce, 0x8c, 0x72, 0x5a, 0x13, 0x56,
			0xc2, 0x34, 0xd0, 0x88, 0x59, 0x0b, 0xf7, 0x86,
			0x69, 0x90, 0x91, 0x76, 0x2d, 0x01, 0x97, 0x36,
			0x30, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		}), // 0000000000002103637910d267190996687fb095880d432c6531a527c8ec53d1
		MerkleRoot: chainhash.Hash([32]byte{
			0x09, 0xa7, 0x45, 0x1a, 0x7d, 0x41, 0xca, 0x75,
			0x8d, 0x4c, 0xc3, 0xc8, 0x5c, 0x5b, 0x07, 0x60,
			0x30, 0xf2, 0x3c, 0x5a, 0xed, 0xd6, 0x79, 0x49,
			0xa3, 0xe1, 0xa8, 0x55, 0xf2, 0x9d, 0xe0, 0x11,
		}), // 110ed92f558a1e3a94976ddea5c32f030670b5c58c3cc4d857ac14d7a1547a90
		Timestamp: time.Unix(1293623731, 0), // 2010-12-29 11:55:31
		Bits:      0x1b04864c,               // 453281356
		Nonce:     0xe80388b2,               // 3892545714
	},
	Transactions: []*wire.MsgTx{
		{
			Version: 1,
			TxIn: []*wire.TxIn{
				{
					PreviousOutPoint: wire.OutPoint{
						Hash:  chainhash.Hash{},
						Index: 0xffffffff,
					},
					SignatureScript: []byte{
						0x04, 0x4c, 0x86, 0x04, 0x1b, 0x01, 0x3e,
					},
					Sequence: 0xffffffff,
				},
			},
			TxOut: []*wire.TxOut{
				{
					Value: 0x12a05f200, // 5000000000
					PkScript: []byte{
						0x41, // OP_DATA_65
						0x04, 0xd1, 0x90, 0x84, 0x0c, 0xfd, 0xae, 0x05,
						0xaf, 0x3d, 0x2f, 0xeb, 0xca, 0x52, 0xb0, 0x46,
						0x6b, 0x6e, 0xfb, 0x02, 0xb4, 0x40, 0x36, 0xa5,
						0xd0, 0xd7, 0x06, 0x59, 0xa5, 0x3f, 0x7b, 0x84,
						0xb7, 0x36, 0xc5, 0xa0, 0x5e, 0xd8, 0x1e, 0x90,
						0xaf, 0x70, 0x98, 0x5d, 0x59, 0xff, 0xb3, 0xd1,
						0xb9, 0x13, 0x64, 0xf7, 0x0b, 0x4d, 0x2b, 0x3b,
						0x75, 0x53, 0xe1, 0x77, 0xb1, 0xce, 0xaf, 0xf3,
						0x22, // 65-byte signature
						0xac, // OP_CHECKSIG
					},
				},
			},
			LockTime: 0,
		},
	},
}

// The following is taken from the btcsuite/btcutil project.
// Block100000 defines block 100,000 of the block chain.  It is used to test
// Block operations.
var Block100000 = wire.MsgBlock{
	Header: wire.BlockHeader{
		Version: 1,
		PrevBlock: chainhash.Hash([32]byte{ // Make go vet happy.
			0x50, 0x12, 0x01, 0x19, 0x17, 0x2a, 0x61, 0x04,
			0x21, 0xa6, 0xc3, 0x01, 0x1d, 0xd3, 0x30, 0xd9,
			0xdf, 0x07, 0xb6, 0x36, 0x16, 0xc2, 0xcc, 0x1f,
			0x1c, 0xd0, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
		}), // 000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250
		MerkleRoot: chainhash.Hash([32]byte{ // Make go vet happy.
			0x66, 0x57, 0xa9, 0x25, 0x2a, 0xac, 0xd5, 0xc0,
			0xb2, 0x94, 0x09, 0x96, 0xec, 0xff, 0x95, 0x22,
			0x28, 0xc3, 0x06, 0x7c, 0xc3, 0x8d, 0x48, 0x85,
			0xef, 0xb5, 0xa4, 0xac, 0x42, 0x47, 0xe9, 0xf3,
		}), // f3e94742aca4b5ef85488dc37c06c3282295ffec960994b2c0d5ac2a25a95766
		Timestamp: time.Unix(1293623863, 0), // 2010-12-29 11:57:43 +0000 UTC
		Bits:      0x1b04864c,               // 453281356
		Nonce:     0x10572b0f,               // 274148111
	},
	Transactions: []*wire.MsgTx{
		{
			Version: 1,
			TxIn: []*wire.TxIn{
				{
					PreviousOutPoint: wire.OutPoint{
						Hash:  chainhash.Hash{},
						Index: 0xffffffff,
					},
					SignatureScript: []byte{
						0x04, 0x4c, 0x86, 0x04, 0x1b, 0x02, 0x06, 0x02,
					},
					Sequence: 0xffffffff,
				},
			},
			TxOut: []*wire.TxOut{
				{
					Value: 0x12a05f200, // 5000000000
					PkScript: []byte{
						0x41, // OP_DATA_65
						0x04, 0x1b, 0x0e, 0x8c, 0x25, 0x67, 0xc1, 0x25,
						0x36, 0xaa, 0x13, 0x35, 0x7b, 0x79, 0xa0, 0x73,
						0xdc, 0x44, 0x44, 0xac, 0xb8, 0x3c, 0x4e, 0xc7,
						0xa0, 0xe2, 0xf9, 0x9d, 0xd7, 0x45, 0x75, 0x16,
						0xc5, 0x81, 0x72, 0x42, 0xda, 0x79, 0x69, 0x24,
						0xca, 0x4e, 0x99, 0x94, 0x7d, 0x08, 0x7f, 0xed,
						0xf9, 0xce, 0x46, 0x7c, 0xb9, 0xf7, 0xc6, 0x28,
						0x70, 0x78, 0xf8, 0x01, 0xdf, 0x27, 0x6f, 0xdf,
						0x84, // 65-byte signature
						0xac, // OP_CHECKSIG
					},
				},
			},
			LockTime: 0,
		},
		{
			Version: 1,
			TxIn: []*wire.TxIn{
				{
					PreviousOutPoint: wire.OutPoint{
						Hash: chainhash.Hash([32]byte{ // Make go vet happy.
							0x03, 0x2e, 0x38, 0xe9, 0xc0, 0xa8, 0x4c, 0x60,
							0x46, 0xd6, 0x87, 0xd1, 0x05, 0x56, 0xdc, 0xac,
							0xc4, 0x1d, 0x27, 0x5e, 0xc5, 0x5f, 0xc0, 0x07,
							0x79, 0xac, 0x88, 0xfd, 0xf3, 0x57, 0xa1, 0x87,
						}), // 87a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03
						Index: 0,
					},
					SignatureScript: []byte{
						0x49, // OP_DATA_73
						0x30, 0x46, 0x02, 0x21, 0x00, 0xc3, 0x52, 0xd3,
						0xdd, 0x99, 0x3a, 0x98, 0x1b, 0xeb, 0xa4, 0xa6,
						0x3a, 0xd1, 0x5c, 0x20, 0x92, 0x75, 0xca, 0x94,
						0x70, 0xab, 0xfc, 0xd5, 0x7d, 0xa9, 0x3b, 0x58,
						0xe4, 0xeb, 0x5d, 0xce, 0x82, 0x02, 0x21, 0x00,
						0x84, 0x07, 0x92, 0xbc, 0x1f, 0x45, 0x60, 0x62,
						0x81, 0x9f, 0x15, 0xd3, 0x3e, 0xe7, 0x05, 0x5c,
						0xf7, 0xb5, 0xee, 0x1a, 0xf1, 0xeb, 0xcc, 0x60,
						0x28, 0xd9, 0xcd, 0xb1, 0xc3, 0xaf, 0x77, 0x48,
						0x01, // 73-byte signature
						0x41, // OP_DATA_65
						0x04, 0xf4, 0x6d, 0xb5, 0xe9, 0xd6, 0x1a, 0x9d,
						0xc2, 0x7b, 0x8d, 0x64, 0xad, 0x23, 0xe7, 0x38,
						0x3a, 0x4e, 0x6c, 0xa1, 0x64, 0x59, 0x3c, 0x25,
						0x27, 0xc0, 0x38, 0xc0, 0x85, 0x7e, 0xb6, 0x7e,
						0xe8, 0xe8, 0x25, 0xdc, 0xa6, 0x50, 0x46, 0xb8,
						0x2c, 0x93, 0x31, 0x58, 0x6c, 0x82, 0xe0, 0xfd,
						0x1f, 0x63, 0x3f, 0x25, 0xf8, 0x7c, 0x16, 0x1b,
						0xc6, 0xf8, 0xa6, 0x30, 0x12, 0x1d, 0xf2, 0xb3,
						0xd3, // 65-byte pubkey
					},
					Sequence: 0xffffffff,
				},
			},
			TxOut: []*wire.TxOut{
				{
					Value: 0x2123e300, // 556000000
					PkScript: []byte{
						0x76, // OP_DUP
						0xa9, // OP_HASH160
						0x14, // OP_DATA_20
						0xc3, 0x98, 0xef, 0xa9, 0xc3, 0x92, 0xba, 0x60,
						0x13, 0xc5, 0xe0, 0x4e, 0xe7, 0x29, 0x75, 0x5e,
						0xf7, 0xf5, 0x8b, 0x32,
						0x88, // OP_EQUALVERIFY
						0xac, // OP_CHECKSIG
					},
				},
				{
					Value: 0x108e20f00, // 4444000000
					PkScript: []byte{
						0x76, // OP_DUP
						0xa9, // OP_HASH160
						0x14, // OP_DATA_20
						0x94, 0x8c, 0x76, 0x5a, 0x69, 0x14, 0xd4, 0x3f,
						0x2a, 0x7a, 0xc1, 0x77, 0xda, 0x2c, 0x2f, 0x6b,
						0x52, 0xde, 0x3d, 0x7c,
						0x88, // OP_EQUALVERIFY
						0xac, // OP_CHECKSIG
					},
				},
			},
			LockTime: 0,
		},
		{
			Version: 1,
			TxIn: []*wire.TxIn{
				{
					PreviousOutPoint: wire.OutPoint{
						Hash: chainhash.Hash([32]byte{ // Make go vet happy.
							0xc3, 0x3e, 0xbf, 0xf2, 0xa7, 0x09, 0xf1, 0x3d,
							0x9f, 0x9a, 0x75, 0x69, 0xab, 0x16, 0xa3, 0x27,
							0x86, 0xaf, 0x7d, 0x7e, 0x2d, 0xe0, 0x92, 0x65,
							0xe4, 0x1c, 0x61, 0xd0, 0x78, 0x29, 0x4e, 0xcf,
						}), // cf4e2978d0611ce46592e02d7e7daf8627a316ab69759a9f3df109a7f2bf3ec3
						Index: 1,
					},
					SignatureScript: []byte{
						0x47, // OP_DATA_71
						0x30, 0x44, 0x02, 0x20, 0x03, 0x2d, 0x30, 0xdf,
						0x5e, 0xe6, 0xf5, 0x7f, 0xa4, 0x6c, 0xdd, 0xb5,
						0xeb, 0x8d, 0x0d, 0x9f, 0xe8, 0xde, 0x6b, 0x34,
						0x2d, 0x27, 0x94, 0x2a, 0xe9, 0x0a, 0x32, 0x31,
						0xe0, 0xba, 0x33, 0x3e, 0x02, 0x20, 0x3d, 0xee,
						0xe8, 0x06, 0x0f, 0xdc, 0x70, 0x23, 0x0a, 0x7f,
						0x5b, 0x4a, 0xd7, 0xd7, 0xbc, 0x3e, 0x62, 0x8c,
						0xbe, 0x21, 0x9a, 0x88, 0x6b, 0x84, 0x26, 0x9e,
						0xae, 0xb8, 0x1e, 0x26, 0xb4, 0xfe, 0x01,
						0x41, // OP_DATA_65
						0x04, 0xae, 0x31, 0xc3, 0x1b, 0xf9, 0x12, 0x78,
						0xd9, 0x9b, 0x83, 0x77, 0xa3, 0x5b, 0xbc, 0xe5,
						0xb2, 0x7d, 0x9f, 0xff, 0x15, 0x45, 0x68, 0x39,
						0xe9, 0x19, 0x45, 0x3f, 0xc7, 0xb3, 0xf7, 0x21,
						0xf0, 0xba, 0x40, 0x3f, 0xf9, 0x6c, 0x9d, 0xee,
						0xb6, 0x80, 0xe5, 0xfd, 0x34, 0x1c, 0x0f, 0xc3,
						0xa7, 0xb9, 0x0d, 0xa4, 0x63, 0x1e, 0xe3, 0x95,
						0x60, 0x63, 0x9d, 0xb4, 0x62, 0xe9, 0xcb, 0x85,
						0x0f, // 65-byte pubkey
					},
					Sequence: 0xffffffff,
				},
			},
			TxOut: []*wire.TxOut{
				{
					Value: 0xf4240, // 1000000
					PkScript: []byte{
						0x76, // OP_DUP
						0xa9, // OP_HASH160
						0x14, // OP_DATA_20
						0xb0, 0xdc, 0xbf, 0x97, 0xea, 0xbf, 0x44, 0x04,
						0xe3, 0x1d, 0x95, 0x24, 0x77, 0xce, 0x82, 0x2d,
						0xad, 0xbe, 0x7e, 0x10,
						0x88, // OP_EQUALVERIFY
						0xac, // OP_CHECKSIG
					},
				},
				{
					Value: 0x11d260c0, // 299000000
					PkScript: []byte{
						0x76, // OP_DUP
						0xa9, // OP_HASH160
						0x14, // OP_DATA_20
						0x6b, 0x12, 0x81, 0xee, 0xc2, 0x5a, 0xb4, 0xe1,
						0xe0, 0x79, 0x3f, 0xf4, 0xe0, 0x8a, 0xb1, 0xab,
						0xb3, 0x40, 0x9c, 0xd9,
						0x88, // OP_EQUALVERIFY
						0xac, // OP_CHECKSIG
					},
				},
			},
			LockTime: 0,
		},
		{
			Version: 1,
			TxIn: []*wire.TxIn{
				{
					PreviousOutPoint: wire.OutPoint{
						Hash: chainhash.Hash([32]byte{ // Make go vet happy.
							0x0b, 0x60, 0x72, 0xb3, 0x86, 0xd4, 0xa7, 0x73,
							0x23, 0x52, 0x37, 0xf6, 0x4c, 0x11, 0x26, 0xac,
							0x3b, 0x24, 0x0c, 0x84, 0xb9, 0x17, 0xa3, 0x90,
							0x9b, 0xa1, 0xc4, 0x3d, 0xed, 0x5f, 0x51, 0xf4,
						}), // f4515fed3dc4a19b90a317b9840c243bac26114cf637522373a7d486b372600b
						Index: 0,
					},
					SignatureScript: []byte{
						0x49, // OP_DATA_73
						0x30, 0x46, 0x02, 0x21, 0x00, 0xbb, 0x1a, 0xd2,
						0x6d, 0xf9, 0x30, 0xa5, 0x1c, 0xce, 0x11, 0x0c,
						0xf4, 0x4f, 0x7a, 0x48, 0xc3, 0xc5, 0x61, 0xfd,
						0x97, 0x75, 0x00, 0xb1, 0xae, 0x5d, 0x6b, 0x6f,
						0xd1, 0x3d, 0x0b, 0x3f, 0x4a, 0x02, 0x21, 0x00,
						0xc5, 0xb4, 0x29, 0x51, 0xac, 0xed, 0xff, 0x14,
						0xab, 0xba, 0x27, 0x36, 0xfd, 0x57, 0x4b, 0xdb,
						0x46, 0x5f, 0x3e, 0x6f, 0x8d, 0xa1, 0x2e, 0x2c,
						0x53, 0x03, 0x95, 0x4a, 0xca, 0x7f, 0x78, 0xf3,
						0x01, // 73-byte signature
						0x41, // OP_DATA_65
						0x04, 0xa7, 0x13, 0x5b, 0xfe, 0x82, 0x4c, 0x97,
						0xec, 0xc0, 0x1e, 0xc7, 0xd7, 0xe3, 0x36, 0x18,
						0x5c, 0x81, 0xe2, 0xaa, 0x2c, 0x41, 0xab, 0x17,
						0x54, 0x07, 0xc0, 0x94, 0x84, 0xce, 0x96, 0x94,
						0xb4, 0x49, 0x53, 0xfc, 0xb7, 0x51, 0x20, 0x65,
						0x64, 0xa9, 0xc2, 0x4d, 0xd0, 0x94, 0xd4, 0x2f,
						0xdb, 0xfd, 0xd5, 0xaa, 0xd3, 0xe0, 0x63, 0xce,
						0x6a, 0xf4, 0xcf, 0xaa, 0xea, 0x4e, 0xa1, 0x4f,
						0xbb, // 65-byte pubkey
					},
					Sequence: 0xffffffff,
				},
			},
			TxOut: []*wire.TxOut{
				{
					Value: 0xf4240, // 1000000
					PkScript: []byte{
						0x76, // OP_DUP
						0xa9, // OP_HASH160
						0x14, // OP_DATA_20
						0x39, 0xaa, 0x3d, 0x56, 0x9e, 0x06, 0xa1, 0xd7,
						0x92, 0x6d, 0xc4, 0xbe, 0x11, 0x93, 0xc9, 0x9b,
						0xf2, 0xeb, 0x9e, 0xe0,
						0x88, // OP_EQUALVERIFY
						0xac, // OP_CHECKSIG
					},
				},
			},
			LockTime: 0,
		},
	},
}
