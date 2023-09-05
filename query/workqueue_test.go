package query

import (
	"container/heap"
	"testing"
)

type task struct {
	index float64
}

var _ Task = (*task)(nil)

func (t *task) Index() float64 {
	return t.index
}

// TestWorkQueue makes sure workQueue implements the desired behaviour.
func TestWorkQueue(t *testing.T) {
	t.Parallel()

	const numTasks = 20

	// Create a workQueue.
	q := &workQueue{}
	heap.Init(q)

	// Create a simple list of tasks and add them all to the queue.
	var tasks []*task
	for i := float64(0); i < numTasks; i++ {
		tasks = append(tasks, &task{
			index: i,
		})
	}

	for _, t := range tasks {
		heap.Push(q, t)
	}

	// Check that it reports the expected number of elements.
	l := q.Len()
	if l != numTasks {
		t.Fatalf("expected %v length, was %v", numTasks, l)
	}

	// Pop half, and make sure they arrive in the right order.
	for i := float64(0); i < numTasks/2; i++ {
		peek := q.Peek().(*task)
		pop := heap.Pop(q)

		// We expect the peeked and popped element to be the same.
		if peek != pop {
			t.Fatalf("peek and pop mismatch")
		}

		if peek.index != i {
			t.Fatalf("wrong index: %v", peek.index)
		}
	}

	// Insert 3 elements with index 0.
	for j := 0; j < 3; j++ {
		heap.Push(q, tasks[0])
	}

	for i := float64(numTasks/2 - 3); i < numTasks; i++ {
		peek := q.Peek().(*task)
		pop := heap.Pop(q)

		// We expect the peeked and popped element to be the same.
		if peek != pop {
			t.Fatalf("peek and pop mismatch")
		}

		// First three element should have index 0, rest should have
		// index i.
		exp := i
		if i < numTasks/2 {
			exp = 0
		}

		if peek.index != exp {
			t.Fatalf("wrong index: %v", peek.index)
		}
	}

	// Finally, the queue should be empty.
	l = q.Len()
	if l != 0 {
		t.Fatalf("expected %v length, was %v", 0, l)
	}
}
