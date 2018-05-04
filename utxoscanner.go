package neutrino

import (
	"container/heap"
	"sync"
	"sync/atomic"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/btcsuite/btcwallet/waddrmgr"
	"github.com/davecgh/go-spew/spew"
)

// reportAndError is a simple pair type holding a spend report and error.
type reportAndError struct {
	report *SpendReport
	err    error
}

// GetUtxoRequest is a request to scan for OutPoint from the height StartHeight.
type GetUtxoRequest struct {
	// OutPoint is the target outpoint to watch for spentness.
	OutPoint *wire.OutPoint

	// StartHeight is the height we should begin scanning for spends.
	StartHeight uint32

	resultErrChan chan reportAndError

	quit chan struct{}
}

func (r *GetUtxoRequest) deliver(report *SpendReport, err error) {
	select {
	case r.resultErrChan <- reportAndError{report, err}:
	default:
	}
}

// Result is callback returning either a spend report or an error.
func (r *GetUtxoRequest) Result() (*SpendReport, error) {
	select {
	case resultErr := <-r.resultErrChan:
		return resultErr.report, resultErr.err
	case <-r.quit:
		return nil, ErrShuttingDown
	}
}

// UtxoScannerConfig exposes configurable methods for interacting with the blockchain.
type UtxoScannerConfig struct {
	// BestSnapshot returns the block stamp of the current chain tip.
	BestSnapshot func() (*waddrmgr.BlockStamp, error)

	// GetBlockHash returns the block hash at given height in main chain.
	GetBlockHash func(height int64) (*chainhash.Hash, error)

	// BlockFilterMatches checks the cfilter for the block hash for matches
	// against the rescan options.
	BlockFilterMatches func(ro *rescanOptions, blockHash *chainhash.Hash) (bool, error)

	// GetBlock fetches a block from the p2p network.
	GetBlock func(chainhash.Hash, ...QueryOption) (*btcutil.Block, error)
}

// A PriorityQueue implements heap.Interface and holds GetUtxoRequests. The
// queue maintains that heap.Pop() will always return the GetUtxo request with
// the least starting height. This allows us to add new GetUtxo requests to
// an already running batch.
type PriorityQueue []*GetUtxoRequest

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the least StartHeight.
	return pq[i].StartHeight < pq[j].StartHeight
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push is called by the heap.Interface implementation to add an element to the
// end of the backing store. The heap library will then maintain the heap
// invariant.
func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*GetUtxoRequest)
	*pq = append(*pq, item)
}

// Peek returns the least height element in the queue without removing it.
func (pq *PriorityQueue) Peek() *GetUtxoRequest {
	return (*pq)[0]
}

// Pop is called by the heap.Interface implementation to remove an element from
// the end of the backing store. The heap library will then maintain the heap
// invariant.
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// IsEmpty returns true if the queue has no elements.
func (pq *PriorityQueue) IsEmpty() bool {
	return pq.Len() == 0
}

// UtxoScanner batches calls to GetUtxo so that a single scan can search for
// multiple outpoints. If a scan is in progress when a new element is added, we
// check whether it can safely be added to the current batch, if not it will be
// included in the next batch.
type UtxoScanner struct {
	started uint32
	stopped uint32

	cfg *UtxoScannerConfig

	pq        PriorityQueue
	stopQueue bool

	nextBatch []*GetUtxoRequest

	mu sync.Mutex
	cv *sync.Cond

	wg   sync.WaitGroup
	quit chan struct{}
}

// NewUtxoScanner creates a new instance of UtxoScanner using the given chain
// interface.
func NewUtxoScanner(cfg *UtxoScannerConfig) *UtxoScanner {
	scanner := &UtxoScanner{
		cfg:  cfg,
		cv:   sync.NewCond(&sync.Mutex{}),
		quit: make(chan struct{}),
	}
	scanner.cv = sync.NewCond(&scanner.mu)

	return scanner
}

// Start begins running scan batches.
func (s *UtxoScanner) Start() {
	if !atomic.CompareAndSwapUint32(&s.started, 0, 1) {
		return
	}

	s.wg.Add(1)
	go s.batchManager()
}

// Stop any in-progress scan.
func (s *UtxoScanner) Stop() {
	if !atomic.CompareAndSwapUint32(&s.stopped, 0, 1) {
		return
	}

	s.cv.L.Lock()
	s.stopQueue = true
	s.cv.L.Unlock()

	s.cv.Signal()

	close(s.quit)
	s.wg.Wait()
}

// Enqueue takes a GetUtxoRequest and adds it to the next applicable batch.
func (s *UtxoScanner) Enqueue(outPoint *wire.OutPoint,
	startHeight uint32) (*GetUtxoRequest, error) {

	log.Debugf("Enqueuing request for %s with start height %d",
		outPoint.String(), startHeight)

	req := &GetUtxoRequest{
		OutPoint:      outPoint,
		StartHeight:   startHeight,
		resultErrChan: make(chan reportAndError, 1),
		quit:          s.quit,
	}

	s.cv.L.Lock()
	select {
	case <-s.quit:
		s.cv.L.Unlock()
		return nil, ErrShuttingDown
	default:
	}

	// Insert the request into the queue and signal any threads that might be
	// waiting for new elements.
	heap.Push(&s.pq, req)

	s.cv.L.Unlock()
	s.cv.Signal()

	return req, nil
}

// batchManager is responsible for scheduling batches of UTXOs to scan. Any
// incoming requests whose start height has already been passed will be added to
// the next batch, which gets scheduled after the current batch finishes.
//
// NOTE: This method MUST be spawned as a goroutine.
func (s *UtxoScanner) batchManager() {
	defer s.wg.Done()

	for {
		s.cv.L.Lock()
		if s.stopQueue {
			s.cv.L.Unlock()
			return
		}

		// Re-queue previously skipped requests for next batch.
		for _, request := range s.nextBatch {
			heap.Push(&s.pq, request)
		}

		s.nextBatch = nil

		s.cv.L.Unlock()

		failedRequests, err := s.scan()

		// If there was an error, then notify the currently outstanding
		// requests.
		if err != nil {
			for _, request := range failedRequests {
				request.deliver(nil, err)
			}
		}
	}
}

// Returns the GetUtxoRequest with the lowest block height. If no elements are
// available, then block until one is added.
func (s *UtxoScanner) peek() *GetUtxoRequest {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	// Block until the queue is no longer empty.
	for !s.stopQueue && s.pq.IsEmpty() {
		s.cv.Wait()
	}

	// We return nil only in the case that we've been interrupted, so callers
	// can use this to determine that the UtxoScanner is shutting down.
	if s.stopQueue {
		return nil
	}

	return s.pq.Peek()
}

// dequeueAtHeight returns all GetUtxo requests that have starting height of the
// given height.
func (s *UtxoScanner) dequeueAtHeight(height uint32) []*GetUtxoRequest {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	// Take any requests that are too old to go in this batch and keep them for
	// the next batch.
	for !s.pq.IsEmpty() && s.pq.Peek().StartHeight < height {
		item := heap.Pop(&s.pq).(*GetUtxoRequest)
		s.nextBatch = append(s.nextBatch, item)
	}

	var requests []*GetUtxoRequest
	for !s.pq.IsEmpty() && s.pq.Peek().StartHeight == height {
		item := heap.Pop(&s.pq).(*GetUtxoRequest)
		requests = append(requests, item)
	}

	return requests
}

// findInitialTransactions returns a SpendReport for the UTXO, or nil if it does
// not exist in this block.
func findInitialTransactions(block *wire.MsgBlock,
	newReqs []*GetUtxoRequest) map[wire.OutPoint]*SpendReport {

	// Construct a reverse index from txid to all a list of requests whose
	// outputs share the same txid.
	txidReverseIndex := make(map[chainhash.Hash][]*GetUtxoRequest)
	for _, req := range newReqs {
		txidReverseIndex[req.OutPoint.Hash] = append(
			txidReverseIndex[req.OutPoint.Hash], req,
		)
	}

	// Iterate over the transactions in this block, hashing each and
	// querying our reverse index to see if any requests depend on the txn.
	initialTxns := make(map[wire.OutPoint]*SpendReport)
	for _, tx := range block.Transactions {
		txidReqs, ok := txidReverseIndex[tx.TxHash()]
		if !ok {
			continue
		}

		// For all requests that are watching this txid, use the output
		// index of each to grab the initial output.
		txOuts := tx.TxOut
		for _, req := range txidReqs {
			op := req.OutPoint

			// Ensure that the outpoint's index references an actual
			// output on the transaction. If not, we will be unable
			// to find the initial output.
			if op.Index >= uint32(len(txOuts)) {
				log.Errorf("Failed to find outpoint %s -- "+
					"invalid output index", req.OutPoint)
				initialTxns[*op] = nil
				continue
			}

			initialTxns[*op] = &SpendReport{
				Output: txOuts[op.Index],
			}
		}

		// If our set of initial txns is the same size as the requests
		// we were asked to find, we are done.
		if len(initialTxns) == len(newReqs) {
			return initialTxns
		}
	}

	// Otherwise, we must reconcile any requests for which the txid did not
	// exist in this block.
	for _, req := range newReqs {
		_, ok := initialTxns[*req.OutPoint]
		if !ok {
			log.Errorf("Failed to find outpoint %s -- "+
				"txid not found in block", req.OutPoint)
			initialTxns[*req.OutPoint] = nil
		}
	}

	return initialTxns
}

// findSpends finds any transactions in the block that spend the given
// outpoints.
func findSpends(block *wire.MsgBlock, height uint32,
	outpoints map[wire.OutPoint][]byte) map[wire.OutPoint]*SpendReport {

	spends := make(map[wire.OutPoint]*SpendReport)

	// If we've spent the output in this block, return an error stating that
	// the output is spent.
	for _, tx := range block.Transactions {
		// Check each input to see if this transaction spends one of our
		// watched outpoints.
		for i, ti := range tx.TxIn {
			if _, ok := outpoints[ti.PreviousOutPoint]; ok {
				log.Debugf("Transaction %s spends outpoint %s", tx.TxHash(),
					ti.PreviousOutPoint)

				spends[ti.PreviousOutPoint] = &SpendReport{
					SpendingTx:         tx,
					SpendingInputIndex: uint32(i),
					SpendingTxHeight:   height,
				}
			}
		}
	}

	return spends
}

// scan runs a single batch, pulling in any requests that get added above the
// batch's last processed height. If there was an error, then return the
// outstanding requests.
func (s *UtxoScanner) scan() ([]*GetUtxoRequest, error) {
	requests := make(map[wire.OutPoint]*GetUtxoRequest)

	// Construct a closure that will spill the contents of the request map
	// in the event we experience a critical rescan error.
	spillRequests := func() []*GetUtxoRequest {
		requestList := make([]*GetUtxoRequest, 0, len(requests))
		for _, req := range requests {
			requestList = append(requestList, req)
		}
		return requestList
	}

	// Take the request with the lowest block height so we can begin the scan
	// from there.
	req := s.peek()

	// Check if we were interrupted while waiting.
	if req == nil {
		return nil, nil
	}

	// While scanning through the blockchain, take note of the transactions that
	// create the outpoints. If the outpoint isn't spent then return this
	// transaction.
	initialTx := make(map[wire.OutPoint]*SpendReport)
	outpoints := make(map[wire.OutPoint][]byte)
	var filterEntries [][]byte

	bestStamp, err := s.cfg.BestSnapshot()
	if err != nil {
		return nil, err
	}

	var (
		startHeight = req.StartHeight
		endHeight   = uint32(bestStamp.Height)
	)

scanToEnd:
	// Scan forward through the blockchain and look for any transactions that
	// might spend the given UTXOs.
	for height := startHeight; height <= endHeight; height++ {
		// Check to see if the utxoscanner has been signaled to exit.
		select {
		case <-s.quit:
			return spillRequests(), ErrShuttingDown
		default:
		}

		// If there are any new requests that can safely be added to this batch,
		// then try and fetch them.
		reqs := s.dequeueAtHeight(height)
		for _, req := range reqs {
			log.Debugf("Adding outpoint=%s height=%d to watchlist",
				req.OutPoint, req.StartHeight)

			entry := builder.OutPointToFilterEntry(*req.OutPoint)
			filterEntries = append(filterEntries, entry)

			outpoints[*req.OutPoint] = entry
			requests[*req.OutPoint] = req
		}

		hash, err := s.cfg.GetBlockHash(int64(height))
		if err != nil {
			return spillRequests(), err
		}

		// If an outpoint is created in this block, then fetch it regardless.
		// Otherwise check to see if the filter matches any of our watched
		// outpoints.
		fetch := len(reqs) > 0
		if !fetch {
			options := rescanOptions{
				watchList: filterEntries,
			}

			match, err := s.cfg.BlockFilterMatches(&options, hash)
			if err != nil {
				return spillRequests(), err
			}

			// If still no match is found, we have no reason to
			// fetch this block, and can continue to next height.
			if !match {
				continue
			}
		}

		log.Debugf("Fetching block height=%d hash=%s", height, hash)

		block, err := s.cfg.GetBlock(*hash)
		if err != nil {
			return spillRequests(), err
		}

		// Check to see if the utxoscanner has been signaled to exit.
		select {
		case <-s.quit:
			return spillRequests(), ErrShuttingDown
		default:
		}

		log.Debugf("Got block height=%d hash=%s", height, hash)

		// Grab the txns that created the outputs requested at this
		// block height.
		txnsInBlock := findInitialTransactions(block.MsgBlock(), reqs)
		for outPoint, tx := range txnsInBlock {
			if tx != nil {
				log.Debugf("Block %d creates output %s",
					height, outPoint)
			}
			initialTx[outPoint] = tx
		}

		// Filter the block for any spends using our set of outpoints.
		spends := findSpends(block.MsgBlock(), height, outpoints)
		for outPoint, spend := range spends {
			// Find the request this spend relates to.
			request, ok := requests[outPoint]
			if !ok {
				log.Debugf("Utxo scan request for scanned "+
					"outpoint=%v was not found", outPoint)
				continue
			}

			// With the request located, we remove this outpoint
			// from both the requests, outpoints, and initial txns
			// map. This will ensures we don't continue watching
			// this outpoint.
			delete(requests, outPoint)
			delete(outpoints, outPoint)
			delete(initialTx, outPoint)

			log.Debugf("Outpoint %s is spent in tx %s",
				outPoint.String(), spew.Sprint(spend))

			// Deliver the spend report to the request.
			request.deliver(spend, nil)
		}

		// Rebuild filter entries from cached entries remaining in
		// outpoints map.
		filterEntries = filterEntries[:0]
		for _, entry := range outpoints {
			filterEntries = append(filterEntries, entry)
		}
	}

	// We've scanned up to the end height, now perform a check to see if we
	// still have any new blocks to process. If this is the first time
	// through, we might have a few blocks that were added since the
	// scan started.
	currStamp, err := s.cfg.BestSnapshot()
	if err != nil {
		return spillRequests(), err
	}

	// If the returned height is higher, we still have more blocks to go.
	// Shift the start and end heights and continue scanning.
	if uint32(currStamp.Height) > endHeight {
		startHeight = endHeight + 1
		endHeight = uint32(currStamp.Height)
		goto scanToEnd
	}

	log.Debugf("Finished batch, %d unspent outpoints", len(requests))

	for _, request := range requests {
		tx, ok := initialTx[*request.OutPoint]
		if ok {
			request.deliver(tx, nil)
		} else {
			// A nil SpendReport indicates the output was not found.
			request.deliver(nil, nil)
		}
	}

	return nil, nil
}
