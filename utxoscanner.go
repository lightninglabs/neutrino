package neutrino

import (
	"container/heap"
	"github.com/davecgh/go-spew/spew"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcd/wire"
	"github.com/roasbeef/btcutil"
	"github.com/roasbeef/btcutil/gcs"
	"github.com/roasbeef/btcutil/gcs/builder"
	"github.com/roasbeef/btcwallet/waddrmgr"
	"sync"
)

// GetUtxoRequest is a request to scan for OutPoint from the height StartHeight.
type GetUtxoRequest struct {
	OutPoint    *wire.OutPoint
	StartHeight uint32
	Result      func(*SpendReport, error)
}

// Interface exposes the necessary methods for interacting with the blockchain.
type Interface interface {
	GetBlockFromNetwork(chainhash.Hash, ...QueryOption) (*btcutil.Block, error)
	GetBlockHash(int64) (*chainhash.Hash, error)
	BestSnapshot() (*waddrmgr.BlockStamp, error)
	GetCFilter(blockHash chainhash.Hash, filterType wire.FilterType,
		options ...QueryOption) (*gcs.Filter, error)
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
	pq      PriorityQueue
	stopped bool

	chainClient Interface

	nextBatch []*GetUtxoRequest

	cv *sync.Cond
}

// NewUtxoScanner creates a new instance of UtxoScanner using the given chain
// interface.
func NewUtxoScanner(chainClient Interface) UtxoScanner {
	return UtxoScanner{
		pq:          make(PriorityQueue, 0),
		cv:          sync.NewCond(&sync.Mutex{}),
		stopped:     false,
		chainClient: chainClient,
	}
}

// Start begins running scan batches.
func (s *UtxoScanner) Start() {
	go func() {
		for {
			s.cv.L.Lock()
			if s.stopped {
				s.cv.L.Unlock()
				return
			}

			// Re-queue previously skipped requests for next batch.
			for _, request := range s.nextBatch {
				heap.Push(&s.pq, request)
			}

			s.nextBatch = nil

			s.cv.L.Unlock()

			requests, err := s.runBatch()

			// If there was an error, then notify the currently outstanding
			// requests.
			if err != nil {
				for _, request := range requests {
					request.Result(nil, err)
				}
			}
		}
	}()
}

// getAtHeight returns all GetUtxo requests that have starting height of the
// given height.
func (s *UtxoScanner) getAtHeight(height uint32) []*GetUtxoRequest {
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

// CheckTransactions finds any transactions in the block that spend the given
// outpoints.
func (s *UtxoScanner) CheckTransactions(block *wire.MsgBlock, height uint32,
	outpoints map[wire.OutPoint]struct{}) (map[wire.OutPoint]SpendReport,
	error) {
	spends := make(map[wire.OutPoint]SpendReport)

	// If we've spent the output in this block, return an
	// error stating that the output is spent.
	for _, tx := range block.Transactions {
		// Check each input to see if this transaction spends one of our
		// watched outpoints.
		for i, ti := range tx.TxIn {
			if _, ok := outpoints[ti.PreviousOutPoint]; ok {
				log.Debugf("Transaction %s spends outpoint %s", tx.TxHash(),
					ti.PreviousOutPoint)
				spends[ti.PreviousOutPoint] = SpendReport{
					SpendingTx:         tx,
					SpendingInputIndex: uint32(i),
					SpendingTxHeight:   height,
				}
			}
		}
	}

	return spends, nil
}

// filterMatches checks whether any of the filterEntries match for the given
// block.
func (s *UtxoScanner) filterMatches(hash chainhash.Hash,
	filterEntries [][]byte) (bool, error) {
	filter, err := s.chainClient.GetCFilter(hash,
		wire.GCSFilterRegular)
	if err != nil {
		return false, err
	}

	if filter != nil {
		filterKey := builder.DeriveKey(&hash)
		return filter.MatchAny(filterKey, filterEntries)
	}

	return false, nil
}

func buildFilterEntries(requests []*GetUtxoRequest) ([][]byte,
	map[wire.OutPoint]struct{}) {
	var filterEntries [][]byte
	outpoints := make(map[wire.OutPoint]struct{})

	for _, request := range requests {
		op := *request.OutPoint
		outpoints[op] = struct{}{}
		filterEntries = append(filterEntries, builder.OutPointToFilterEntry(op))
	}

	return filterEntries, outpoints
}

// runBatch runs a single batch. If there was an error, then return the
// outstanding requests.
func (s *UtxoScanner) runBatch() ([]*GetUtxoRequest, error) {
	var requests []*GetUtxoRequest

	// Take the request with the lowest block height so we can begin the scan
	// from there.
	req := s.peek()

	// Check if we were interrupted while waiting.
	if req == nil {
		return nil, nil
	}

	startHeight := req.StartHeight
	best, err := s.chainClient.BestSnapshot()
	if err != nil {
		return requests, err
	}

	// While scanning through the blockchain, take note of the transactions that
	// create the outpoints. If the outpoint isn't spent then return this
	// transaction.
	initialTx := make(map[wire.OutPoint]*SpendReport)

	filterEntries, outpoints := buildFilterEntries(requests)

	// Scan forward through the blockchain and look for any transactions that
	// might spend the given UTXOs.
	for height := startHeight; height <= uint32(best.Height); height++ {
		// If there are any new requests that can safely be added to this batch,
		// then try and fetch them.
		reqs := s.getAtHeight(height)
		if len(reqs) > 0 {
			for _, req := range reqs {
				log.Debugf("Adding %s (%d) to watchlist", req.OutPoint.String(),
					req.StartHeight)
				requests = append(requests, req)
				outpoints[*req.OutPoint] = struct{}{}
				filterEntries = append(filterEntries,
					builder.OutPointToFilterEntry(*req.OutPoint))
			}
		}

		hash, err := s.chainClient.GetBlockHash(int64(height))
		if err != nil {
			return requests, err
		}

		// If an outpoint is created in this block, then fetch it regardless.
		// Otherwise check to see if the filter matches any of our watched
		// outpoints.
		fetch := len(reqs) > 0
		if !fetch {
			match, err := s.filterMatches(*hash, filterEntries)
			if err != nil {
				return requests, err
			}
			fetch = match
		}

		if fetch {
			log.Debugf("Fetching block at height %d (%s)", height,
				hash.String())

			// FIXME(simon): Find out why this takes three minutes.
			// Fetch the block from the network.
			block, err := s.chainClient.GetBlockFromNetwork(*hash)
			if err != nil {
				return requests, err
			}

			log.Debugf("Got block %d (%s)", height, hash.String())

			if fetch {
				for _, request := range requests {
					if request.StartHeight == height {
						tx := findTransaction(block.MsgBlock(),
							request.OutPoint)
						// Grab the tx that created this output.
						initialTx[*request.OutPoint] = tx

						log.Debugf("Block %d creates output %s", height,
							request.OutPoint.String())
					}
				}
			}

			spends, err := s.CheckTransactions(block.MsgBlock(), height,
				outpoints)
			if err != nil {
				return requests, err
			}

			for outPoint, spend := range spends {
				log.Debugf("Outpoint %s is spent in tx %s", outPoint.String(),
					spew.Sprint(spend))

				// Find the request this spend relates to.
				var filteredRequests []*GetUtxoRequest
				for _, request := range requests {
					if *request.OutPoint == outPoint {
						request.Result(&spend, nil)
					} else {
						filteredRequests = append(filteredRequests, request)
					}
				}
				requests = filteredRequests

				// Remove the filter from filterEntries.
				filterEntries, outpoints = buildFilterEntries(requests)
			}
		}
	}

	log.Debugf("Finished batch, %d unspent outpoints", len(requests))

	for _, request := range requests {
		tx, ok := initialTx[*request.OutPoint]
		if ok {
			request.Result(tx, nil)
		} else {
			// A nil SpendReport indicates the output was not found.
			request.Result(nil, nil)
		}
	}

	return nil, nil
}

// Stop any in-progress scan.
func (s *UtxoScanner) Stop() {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	s.stopped = true
}

// Returns the GetUtxoRequest with the lowest block height. If no elements are
// available, then block until one is added.
func (s *UtxoScanner) peek() *GetUtxoRequest {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	// Block until the queue is no longer empty.
	for !s.stopped && s.pq.IsEmpty() {
		s.cv.Wait()
	}

	// We return nil only in the case that we've been interrupted, so callers
	// can use this to determine that the UtxoScanner is shutting down.
	if s.stopped {
		return nil
	}

	return s.pq.Peek()
}

// Enqueue takes a GetUtxoRequest and adds it to the next applicable batch.
func (s *UtxoScanner) Enqueue(req *GetUtxoRequest) {
	log.Debugf("Enqueuing request for %s with start height %d",
		req.OutPoint.String(), req.StartHeight)
	s.cv.L.Lock()

	// Insert the request into the queue and signal any threads that might be
	// waiting for new elements.
	heap.Push(&s.pq, req)

	s.cv.L.Unlock()
	s.cv.Signal()
}

// findTransaction returns a SpendReport for the UTXO, or nil if it does not
// exist in this block.
func findTransaction(block *wire.MsgBlock,
	point *wire.OutPoint) *SpendReport {
	for _, tx := range block.Transactions {
		if tx.TxHash() == point.Hash {
			outputs := tx.TxOut
			return &SpendReport{
				Output: outputs[point.Index],
			}
		}
	}

	log.Errorf("Failed to find tx %s", point.Hash.String())

	return nil
}
