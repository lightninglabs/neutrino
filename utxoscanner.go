package neutrino

import (
	"container/heap"
	"github.com/roasbeef/btcd/wire"
	"sync"
)

type OutPointHint struct {
	outPoint   *wire.OutPoint
	heightHint uint32
}

func NewOutPointHint(outPoint *wire.OutPoint, heightHint uint32) OutPointHint {
	return OutPointHint{
		outPoint:   outPoint,
		heightHint: heightHint,
	}
}

type GetUtxoRequest struct {
	OutPoint OutPointHint
	Result   func(SpendReport)
}

type Item struct {
	value    GetUtxoRequest
	priority int
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

type UtxoScanner struct {
	pq PriorityQueue

	cv       *sync.Cond
	queueMut sync.Mutex
}

func NewUtxoScanner() UtxoScanner {
	var queueMut sync.Mutex
	return UtxoScanner{
		pq:       make(PriorityQueue, 0),
		cv:       sync.NewCond(&queueMut),
		queueMut: queueMut,
	}
}

func (s *UtxoScanner) Start() {
	go func() {
		// TODO(simon): Block until an item is added to the queue
	}()
}

// Schedules a new batch if one isn't already running.
func (s *UtxoScanner) schedule() {
}

func (s *UtxoScanner) Stop() {
	// TODO(simon): Stop any in-progress scan.
}

func (s *UtxoScanner) Enqueue(req GetUtxoRequest) {
	s.queueMut.Lock()
	defer s.queueMut.Unlock()
	// TODO(simon): Insert the request into the queue.
	 heap.Push(&s.pq, req)
	// TODO(simon): Signal CV
}
