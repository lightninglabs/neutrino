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
// the greatest starting height. This allows us to add new GetUtxo requests to
// an already running batch that's still in the process of catching up to the
// start height of the request.
type PriorityQueue []*GetUtxoRequest

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest height so we use greater than here.
	return pq[i].StartHeight > pq[j].StartHeight
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

// Peek returns the greatest height element in the queue without removing it.
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

// drain removes all items currently in the queue and returns a slice. If there
// are no items in the queue, this method blocks until one is available. The
// requests are returned in descending height order.
func (s *UtxoScanner) drain() []GetUtxoRequest {
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

	// Empty the queue.
	var requests []GetUtxoRequest
	for !s.pq.IsEmpty() {
		item := heap.Pop(&s.pq).(*GetUtxoRequest)
		requests = append(requests, *item)
	}

	return requests
}

// getAfterHeight returns all GetUtxo requests that have starting height of at
// least the given height.
func (s *UtxoScanner) getAfterHeight(height uint32) []GetUtxoRequest {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	var requests []GetUtxoRequest
	for !s.pq.IsEmpty() && s.pq.Peek().StartHeight >= height {
		item := heap.Pop(&s.pq).(*GetUtxoRequest)
		requests = append(requests, *item)
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

func buildFilterEntries(requests []GetUtxoRequest) ([][]byte,
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
func (s *UtxoScanner) runBatch() ([]GetUtxoRequest, error) {
	// Fetch all queued requests, blocking if the queue is empty.
	requests := s.drain()

	// If the queue was empty then we were interrupted while waiting.
	if requests == nil {
		return nil, nil
	}

	log.Debugf("Running batch, looking for %d outpoints", len(requests))

	filterEntries, outpoints := buildFilterEntries(requests)

	startHeight := requests[len(requests)-1].StartHeight
	best, err := s.chainClient.BestSnapshot()
	if err != nil {
		return requests, err
	}

	// While scanning through the blockchain, take note of the transactions that
	// create the outpoints. If the outpoint isn't spent then return this
	// transaction.
	initialTx := make(map[wire.OutPoint]*SpendReport)

	// Scan forward through the blockchain and look for any transactions that
	// might spend the given UTXOs.
	for height := startHeight; height <= uint32(best.Height); height++ {

		// TODO(simon): Don't search for outpoints before their birthday.

		s.cv.L.Lock()
		size := s.pq.Len()
		s.cv.L.Unlock()

		// If there are any new requests that can safely be added to this batch,
		// then try and fetch them.
		reqs := s.getAfterHeight(height)
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

		log.Debugf("Checking for spends of %d/%d outpoints at height %d",
			len(requests), size, height)

		hash, err := s.chainClient.GetBlockHash(int64(height))
		if err != nil {
			return requests, err
		}

		fetch := false
		for _, request := range requests {
			if request.StartHeight == height {
				// Grab the tx that created this output.
				fetch = true
			}
		}

		match, err := s.filterMatches(*hash, filterEntries)
		if err != nil {
			return requests, err
		}

		if match || fetch {
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
						tx := s.GetOriginalTx(block.MsgBlock(),
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
				var filteredRequests []GetUtxoRequest
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
			log.Debugf("Returning unspent output %s", spew.Sdump(tx))
			request.Result(tx, nil)
		} else {
			log.Debugf("Failed to find output %s", request.OutPoint)

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

// Enqueue takes a GetUtxoRequest and adds it to the next applicable batch.
func (s *UtxoScanner) Enqueue(req GetUtxoRequest) {
	log.Debugf("Enqueuing request for %s with start height %d",
		req.OutPoint.String(), req.StartHeight)
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	// Insert the request into the queue and signal any threads that might be
	// waiting for new elements.
	heap.Push(&s.pq, &req)
	s.cv.Signal()
}

// GetOriginalTx returns a SpendReport for the UTXO, or nil if it does not exist
// in this block.
func (s *UtxoScanner) GetOriginalTx(block *wire.MsgBlock,
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
