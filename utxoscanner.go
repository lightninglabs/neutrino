package neutrino

import (
	"container/heap"
	"github.com/roasbeef/btcd/wire"
	"sync"
)

type GetUtxoRequest struct {
	OutPoint    *wire.OutPoint
	StartHeight uint32
	Result      func(SpendReport)
}

type Item struct {
	value  GetUtxoRequest
	height uint32
}

// A PriorityQueue implements heap.Interface and holds Items. The queue
// maintains that Pop() will always return the GetUtxo request with the greatest
// starting height. This allows us to add new GetUtxo requests to an already
// running batch that's still in the process of catching up to the start height
// of the request.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].height > pq[j].height
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*Item)
	item.height = item.value.StartHeight
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Peek() *Item {
	return (*pq)[len(*pq)-1]
}

// Pop returns the greatest height GetUtxo request from the queue, removing it
// from the queue in the process.
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) IsEmpty() bool {
	return pq.Len() == 0
}

type UtxoScanner struct {
	pq      PriorityQueue
	stopped bool

	cv *sync.Cond
}

func NewUtxoScanner() UtxoScanner {
	return UtxoScanner{
		pq:      make(PriorityQueue, 0),
		cv:      sync.NewCond(&sync.Mutex{}),
		stopped: false,
	}
}

func (s *UtxoScanner) Start() {
	go func() {
		for {
			s.cv.L.Lock()
			if s.stopped {
				s.cv.L.Unlock()
				return
			}
			s.cv.L.Unlock()

			s.runBatch()
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
		item := s.pq.Pop().(*Item)
		requests = append(requests, item.value)
	}

	return requests
}

// Returns all GetUtxo requests that have starting height greater than the given
// height.
func (s *UtxoScanner) getAfterHeight(height uint32) []GetUtxoRequest {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	var requests []GetUtxoRequest
	for !s.pq.IsEmpty() && s.pq.Peek().height > height {
		item := s.pq.Pop().(*Item)
		requests = append(requests, item.value)
	}
	return requests
}

// Runs a single batch.
func (s *UtxoScanner) runBatch() {
	// Fetch all queued requests, blocking if the queue is empty.
	requests := s.drain()

	if requests == nil {
		return
	}

	startHeight := requests[len(requests)-1].StartHeight
	bestHeight := uint32(500000)

	for height := startHeight; startHeight < bestHeight; height++ {
		// If there are any new requests that can safely be added to this batch,
		// then try and fetch them.
		reqs := s.getAfterHeight(height)

		if len(reqs) > 0 {
			for _, req := range reqs {
				requests = append(requests, req)
			}
			// TODO(simon): Re-calculate filters.
		}

		// TODO(simon): Actual scanning here.
	}
}

// Stop any in-progress scan.
func (s *UtxoScanner) Stop() {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	s.stopped = true
}

func (s *UtxoScanner) Enqueue(req GetUtxoRequest) {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	// Insert the request into the queue and signal any threads that might be
	// waiting for new elements.
	heap.Push(&s.pq, req)
	s.cv.Signal()
}
