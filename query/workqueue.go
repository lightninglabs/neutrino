package query

type workQueue struct {
	tasks []*queryTask
}

// Len returns the number of nodes in the priority queue.
//
// NOTE: This is part of the heap.Interface implementation.
func (w *workQueue) Len() int { return len(w.tasks) }

// Less returns whether the item in the priority queue with index i should sort
// before the item with index j.
//
// NOTE: This is part of the heap.Interface implementation.
func (w *workQueue) Less(i, j int) bool {
	return w.tasks[i].Priority() < w.tasks[j].Priority()
}

// Swap swaps the nodes at the passed indices in the priority queue.
//
// NOTE: This is part of the heap.Interface implementation.
func (w *workQueue) Swap(i, j int) {
	w.tasks[i], w.tasks[j] = w.tasks[j], w.tasks[i]
}

// Push pushes the passed item onto the priority queue.
//
// NOTE: This is part of the heap.Interface implementation.
func (w *workQueue) Push(x interface{}) {
	w.tasks = append(w.tasks, x.(*queryTask))
}

// Pop removes the highest priority item (according to Less) from the priority
// queue and returns it.
//
// NOTE: This is part of the heap.Interface implementation.
func (w *workQueue) Pop() interface{} {
	n := len(w.tasks)
	x := w.tasks[n-1]
	w.tasks = w.tasks[0 : n-1]
	return x
}

func (w *workQueue) Peek() interface{} {
	return w.tasks[0]
}
