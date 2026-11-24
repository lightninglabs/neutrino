package neutrino

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/lightninglabs/neutrino/headerfs"
)

// getUtxoResult is a simple pair type holding a spend report and error.
type getUtxoResult struct {
	report *SpendReport
	err    error
}

// GetUtxoRequest is a request to scan for InputWithScript from the height
// BirthHeight.
type GetUtxoRequest struct {
	// Input is the target outpoint with script to watch for spentness.
	Input *InputWithScript

	// BirthHeight is the height at which we expect to find the original
	// unspent outpoint. This is also the height used when starting the
	// search for spends.
	BirthHeight uint32

	// resultChan either the spend report or error for this request.
	resultChan chan *getUtxoResult

	// result caches the first spend report or error returned for this
	// request.
	result *getUtxoResult

	// onProgress is the method to be used by the scanner to report
	// its progress. It can be nil if not specified by the caller.
	onProgress ScanProgressHandler

	// mu ensures the first response delivered via resultChan is in fact
	// what gets cached in result.
	mu sync.Mutex

	quit chan struct{}
}

// deliver tries to deliver the report or error to any subscribers. If
// resultChan cannot accept a new update, this method will not block.
func (r *GetUtxoRequest) deliver(report *SpendReport, err error) {
	select {
	case r.resultChan <- &getUtxoResult{report, err}:
	default:
		log.Warnf("duplicate getutxo result delivered for "+
			"outpoint=%v, spend=%v, err=%v",
			r.Input.OutPoint, report, err)
	}
}

// Result is callback returning either a spend report or an error.
func (r *GetUtxoRequest) Result(cancel <-chan struct{}) (*SpendReport, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case result := <-r.resultChan:
		// Cache the first result returned, in case we have multiple
		// readers calling Result.
		if r.result == nil {
			r.result = result
		}

		return r.result.report, r.result.err

	case <-cancel:
		return nil, ErrGetUtxoCancelled

	case <-r.quit:
		return nil, ErrShuttingDown
	}
}

// UtxoScannerConfig exposes configurable methods for interacting with the blockchain.
type UtxoScannerConfig struct {
	// BestSnapshot returns the block stamp of the current chain tip.
	BestSnapshot func() (*headerfs.BlockStamp, error)

	// GetBlockHash returns the block hash at given height in main chain.
	GetBlockHash func(height int64) (*chainhash.Hash, error)

	// BlockFilterMatches checks the cfilter for the block hash for matches
	// against the rescan options.
	BlockFilterMatches func(ro *rescanOptions, blockHash *chainhash.Hash) (bool, error)

	// GetBlock fetches a block from the p2p network.
	GetBlock func(chainhash.Hash, ...QueryOption) (*btcutil.Block, error)
}

// UtxoScanner batches calls to GetUtxo so that a single scan can search for
// multiple outpoints. If a scan is in progress when a new element is added, we
// check whether it can safely be added to the current batch, if not it will be
// included in the next batch.
type UtxoScanner struct {
	started uint32
	stopped uint32

	cfg *UtxoScannerConfig

	pq        GetUtxoRequestPQ
	nextBatch []*GetUtxoRequest

	mu sync.Mutex
	cv *sync.Cond

	wg       sync.WaitGroup
	quit     chan struct{}
	shutdown chan struct{}
}

// NewUtxoScanner creates a new instance of UtxoScanner using the given chain
// interface.
func NewUtxoScanner(cfg *UtxoScannerConfig) *UtxoScanner {
	scanner := &UtxoScanner{
		cfg:      cfg,
		quit:     make(chan struct{}),
		shutdown: make(chan struct{}),
	}
	scanner.cv = sync.NewCond(&scanner.mu)

	return scanner
}

// Start begins running scan batches.
func (s *UtxoScanner) Start() error {
	if !atomic.CompareAndSwapUint32(&s.started, 0, 1) {
		return nil
	}

	s.wg.Add(1)
	go s.batchManager()

	return nil
}

// Stop any in-progress scan.
func (s *UtxoScanner) Stop() error {
	if !atomic.CompareAndSwapUint32(&s.stopped, 0, 1) {
		return nil
	}

	close(s.quit)

batchShutdown:
	for {
		select {
		case <-s.shutdown:
			break batchShutdown
		case <-time.After(50 * time.Millisecond):
			s.cv.Signal()
		}
	}

	// Cancel all pending get utxo requests that were not pulled into the
	// batchManager's main goroutine.
	for !s.pq.IsEmpty() {
		pendingReq := heap.Pop(&s.pq).(*GetUtxoRequest)
		pendingReq.deliver(nil, ErrShuttingDown)
	}

	return nil
}

// Enqueue takes a GetUtxoRequest and adds it to the next applicable batch.
func (s *UtxoScanner) Enqueue(input *InputWithScript,
	birthHeight uint32,
	progressHandler ScanProgressHandler) (*GetUtxoRequest, error) {

	log.Debugf("Enqueuing request for %s with birth height %d",
		input.OutPoint.String(), birthHeight)

	req := &GetUtxoRequest{
		Input:       input,
		BirthHeight: birthHeight,
		resultChan:  make(chan *getUtxoResult, 1),
		onProgress:  progressHandler,
		quit:        s.quit,
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
	defer close(s.shutdown)

	for {
		s.cv.L.Lock()
		// Re-queue previously skipped requests for next batch.
		for _, request := range s.nextBatch {
			heap.Push(&s.pq, request)
		}
		s.nextBatch = nil

		// Wait for the queue to be non-empty.
		for s.pq.IsEmpty() {
			s.cv.Wait()

			select {
			case <-s.quit:
				s.cv.L.Unlock()
				return
			default:
			}
		}

		req := s.pq.Peek()
		s.cv.L.Unlock()

		// Break out now before starting a scan if a shutdown was
		// requested.
		select {
		case <-s.quit:
			return
		default:
		}

		// Initiate a scan, starting from the birth height of the
		// least-height request currently in the queue.
		err := s.scanFromHeight(req.BirthHeight)
		if err != nil {
			log.Errorf("utxo scan failed: %v", err)
		}
	}
}

// dequeueAtHeight returns all GetUtxoRequests that have starting height of the
// given height.
func (s *UtxoScanner) dequeueAtHeight(height uint32) []*GetUtxoRequest {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	// Take any requests that are too old to go in this batch and keep them for
	// the next batch.
	for !s.pq.IsEmpty() && s.pq.Peek().BirthHeight < height {
		item := heap.Pop(&s.pq).(*GetUtxoRequest)
		s.nextBatch = append(s.nextBatch, item)
	}

	var requests []*GetUtxoRequest
	for !s.pq.IsEmpty() && s.pq.Peek().BirthHeight == height {
		item := heap.Pop(&s.pq).(*GetUtxoRequest)
		requests = append(requests, item)
	}

	return requests
}

// scanFromHeight runs a single batch, pulling in any requests that get added
// above the batch's last processed height. If there was an error, then return
// the outstanding requests.
func (s *UtxoScanner) scanFromHeight(initHeight uint32) error {
	// Before beginning the scan, grab the best block stamp we know of,
	// which will serve as an initial estimate for the end height of the
	// scan.
	bestStamp, err := s.cfg.BestSnapshot()
	if err != nil {
		s.pause()
		return err
	}

	// Nothing can be scanned for a request that starts above the tip
	// until the tip reaches it, which happens while the filter headers
	// catch up, and after a reorg sends a batch back to a birth height
	// above the fork. The request stays queued, so without a pause the
	// batch manager would come straight back here.
	if initHeight > uint32(bestStamp.Height) {
		s.pause()
		return nil
	}

	var (
		// startHeight and endHeight bound the range of the current
		// scan. If more blocks are found while a scan is running,
		// these values will be updated afterwards to scan for the new
		// blocks.
		startHeight = initHeight
		endHeight   = uint32(bestStamp.Height)
	)

	reporter := newBatchSpendReporter()
	options := defaultRescanOptions()

	// prev is the block the scan settled on at the height below the one
	// being scanned. It's unset until the first height of the batch is
	// done.
	var prev *chainhash.Hash

scanToEnd:
	// Scan forward through the blockchain and look for any transactions that
	// might spend the given UTXOs.
	for height := startHeight; height <= endHeight; height++ {
		// Before beginning to scan this height, check to see if the
		// utxoscanner has been signaled to exit.
		select {
		case <-s.quit:
			return reporter.FailRemaining(ErrShuttingDown)
		default:
		}

		// If there are any new requests that can safely be added to this batch,
		// then try and fetch them.
		newReqs := s.dequeueAtHeight(height)

		hash, block, err := s.fetchActiveBlock(
			reporter, options, height, newReqs,
		)
		if err != nil {
			// The requests taken for this height aren't in the
			// reporter yet, so FailRemaining wouldn't reach them.
			for _, req := range newReqs {
				req.deliver(nil, err)
			}

			return reporter.FailRemaining(err)
		}

		// fetchActiveBlock only checks this height. A reorg more
		// than one block deep can also have replaced blocks we
		// already scanned or skipped, whose replacements could
		// spend, or not create, what we watch. Every reorg that
		// reaches below this height replaces the block right below
		// it, so we check that one is still the block we settled on
		// there. This checks each block is still active, not that
		// it links to the next: a sequence of reorgs that swaps
		// back and forth between two branches within one height
		// could pass, which we don't guard against.
		if prev != nil {
			replaced, err := s.replaced(height-1, prev)
			if err != nil {
				for _, req := range newReqs {
					req.deliver(nil, err)
				}

				return reporter.FailRemaining(err)
			}
			if replaced {
				s.requeue(append(reporter.takeRemaining(),
					newReqs...))

				return nil
			}
		}
		prev = hash

		if block == nil {
			reporter.NotifyProgress(height)
			continue
		}

		reporter.ProcessBlock(block.MsgBlock(), newReqs, height)
		reporter.NotifyProgress(height)
	}

	// We've scanned up to the end height, now perform a check to see if we
	// still have any new blocks to process. If this is the first time
	// through, we might have a few blocks that were added since the
	// scan started.
	currStamp, err := s.cfg.BestSnapshot()
	if err != nil {
		return reporter.FailRemaining(err)
	}

	// If the returned height is higher, we still have more blocks to go.
	// Shift the start and end heights and continue scanning.
	if uint32(currStamp.Height) > endHeight {
		startHeight = endHeight + 1
		endHeight = uint32(currStamp.Height)
		goto scanToEnd
	}

	reporter.NotifyUnspentAndUnfound()

	return nil
}

// A GetUtxoRequestPQ implements heap.Interface and holds GetUtxoRequests. The
// queue maintains that heap.Pop() will always return the GetUtxo request with
// the least starting height. This allows us to add new GetUtxo requests to
// an already running batch.
type GetUtxoRequestPQ []*GetUtxoRequest

func (pq GetUtxoRequestPQ) Len() int { return len(pq) }

func (pq GetUtxoRequestPQ) Less(i, j int) bool {
	// We want Pop to give us the least BirthHeight.
	return pq[i].BirthHeight < pq[j].BirthHeight
}

func (pq GetUtxoRequestPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push is called by the heap.Interface implementation to add an element to the
// end of the backing store. The heap library will then maintain the heap
// invariant.
func (pq *GetUtxoRequestPQ) Push(x interface{}) {
	item := x.(*GetUtxoRequest)
	*pq = append(*pq, item)
}

// Peek returns the least height element in the queue without removing it.
func (pq *GetUtxoRequestPQ) Peek() *GetUtxoRequest {
	return (*pq)[0]
}

// Pop is called by the heap.Interface implementation to remove an element from
// the end of the backing store. The heap library will then maintain the heap
// invariant.
func (pq *GetUtxoRequestPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// IsEmpty returns true if the queue has no elements.
func (pq *GetUtxoRequestPQ) IsEmpty() bool {
	return pq.Len() == 0
}

// fetchActiveBlock returns the hash of the active block at height, together
// with the block if the scan must process it, or a nil block if it can be
// skipped: no request starts in it and its filter matches nothing we watch.
//
// The block is fetched, or its filter matched, by the hash the height
// resolved to beforehand, so a reorg in between can leave us looking at a
// block that is no longer on the active chain. Processing it would report
// spends that only exist on the abandoned branch, and skipping it could miss
// a spend in the block that replaced it. We check the height again once we
// have decided, and if the active block changed, start over at the same
// height with the new one.
func (s *UtxoScanner) fetchActiveBlock(reporter *batchSpendReporter,
	options *rescanOptions, height uint32,
	newReqs []*GetUtxoRequest) (*chainhash.Hash, *btcutil.Block, error) {

	for {
		hash, err := s.cfg.GetBlockHash(int64(height))
		if err != nil {
			return nil, nil, err
		}

		// If an outpoint is created in this block, then fetch it
		// regardless. Otherwise check to see if the filter matches
		// any of our watched outpoints.
		if len(newReqs) == 0 {
			options.watchList = reporter.filterEntries

			options.queryOptions = []QueryOption{
				CancelQuery(s.quit),
			}
			match, err := s.cfg.BlockFilterMatches(options, hash)
			if err != nil {
				return nil, nil, s.quitOr(err)
			}

			// If still no match is found, we have no reason to
			// fetch this block, as long as it is still the active
			// one: a block that replaced it may spend what we
			// watch.
			if !match {
				active, err := s.cfg.GetBlockHash(
					int64(height),
				)
				if err != nil {
					return nil, nil, err
				}
				if *active == *hash {
					return hash, nil, nil
				}

				continue
			}
		}

		// Before fetching the block, check to see if the utxoscanner
		// has been signaled to exit so that we can exit the rescan
		// before performing an expensive operation.
		select {
		case <-s.quit:
			return nil, nil, ErrShuttingDown
		default:
		}

		log.Debugf("Fetching block height=%d hash=%s", height, hash)

		// The fetch is bound to the scanner's quit, so Stop doesn't
		// wait for a query that can take until its timeout.
		block, err := s.cfg.GetBlock(*hash, CancelQuery(s.quit))
		if err != nil {
			return nil, nil, s.quitOr(err)
		}

		// Check again to see if the utxoscanner has been signaled to
		// exit.
		select {
		case <-s.quit:
			return nil, nil, ErrShuttingDown
		default:
		}

		active, err := s.cfg.GetBlockHash(int64(height))
		if err != nil {
			return nil, nil, err
		}
		if *active == *hash {
			log.Debugf("Processing block height=%d hash=%s",
				height, hash)

			return hash, block, nil
		}

		log.Infof("Block %s at height %d was replaced by %s while "+
			"it was fetched, scanning the new one", hash, height,
			active)
	}
}

// tipPollInterval is how long the scanner waits before it looks at the tip
// again, when the lowest queued request starts above it or the tip can't be
// read.
const tipPollInterval = time.Second

// pause waits tipPollInterval, or until the scanner is stopped.
func (s *UtxoScanner) pause() {
	select {
	case <-time.After(tipPollInterval):
	case <-s.quit:
	}
}

// replaced reports whether the active block at height is no longer the one
// the scan settled on there.
func (s *UtxoScanner) replaced(height uint32,
	scanned *chainhash.Hash) (bool, error) {

	active, err := s.cfg.GetBlockHash(int64(height))
	if err != nil {
		return false, err
	}
	if *active == *scanned {
		return false, nil
	}

	log.Infof("Block %s at height %d was replaced by %s during a utxo "+
		"scan, scanning again from the lowest birth height", scanned,
		height, active)

	return true, nil
}

// requeue puts requests that haven't been answered back in the queue, so the
// next batch scans them again from their birth heights.
func (s *UtxoScanner) requeue(reqs []*GetUtxoRequest) {
	s.cv.L.Lock()
	for _, req := range reqs {
		heap.Push(&s.pq, req)
	}
	s.cv.L.Unlock()
}

// quitOr returns ErrShuttingDown if the scanner is stopping, since a fetch
// that failed then was most likely cancelled by the stop, and err otherwise.
func (s *UtxoScanner) quitOr(err error) error {
	select {
	case <-s.quit:
		return ErrShuttingDown
	default:
		return err
	}
}
