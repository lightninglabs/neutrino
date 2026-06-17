package workmanager

// Task is an interface that has a method for returning their index in the
// work queue.
type Task interface {
	// Index returns this Task's index in the work queue.
	Index() uint64
}

// workQueue is a generic min-heap used to keep a list of remaining queryTasks
// in order.
type workQueue[T Task] struct {
	tasks []T
}

// Len returns the number of nodes in the priority queue.
func (w *workQueue[T]) Len() int { return len(w.tasks) }

// Push adds the passed task to the work queue.
func (w *workQueue[T]) Push(task T) {
	w.tasks = append(w.tasks, task)
	w.up(w.Len() - 1)
}

// Pop removes and returns the lowest-indexed task in the queue.
func (w *workQueue[T]) Pop() (T, bool) {
	var zero T
	if w.Len() == 0 {
		return zero, false
	}

	task := w.tasks[0]
	last := w.Len() - 1
	w.tasks[0] = w.tasks[last]
	w.tasks[last] = zero
	w.tasks = w.tasks[:last]

	if w.Len() > 0 {
		w.down(0)
	}

	return task, true
}

// Peek returns the first item in the queue.
func (w *workQueue[T]) Peek() (T, bool) {
	var zero T
	if w.Len() == 0 {
		return zero, false
	}

	return w.tasks[0], true
}

func (w *workQueue[T]) less(i, j int) bool {
	return w.tasks[i].Index() < w.tasks[j].Index()
}

func (w *workQueue[T]) swap(i, j int) {
	w.tasks[i], w.tasks[j] = w.tasks[j], w.tasks[i]
}

func (w *workQueue[T]) up(child int) {
	for {
		parent := (child - 1) / 2
		if child == 0 || !w.less(child, parent) {
			return
		}

		w.swap(parent, child)
		child = parent
	}
}

func (w *workQueue[T]) down(parent int) {
	for {
		left := 2*parent + 1
		if left >= w.Len() {
			return
		}

		child := left
		if right := left + 1; right < w.Len() && w.less(right, left) {
			child = right
		}

		if !w.less(child, parent) {
			return
		}

		w.swap(parent, child)
		parent = child
	}
}
