package headerlist

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
)

// TestBoundedMemoryChainEmptyList tests the expected functionality of an empty
// list w.r.t which methods return a nil pointer and which do not.
func TestBoundedMemoryChainEmptyList(t *testing.T) {
	t.Parallel()

	memChain := NewBoundedMemoryChain(5)

	// An empty list should have a nil Back() pointer.
	if memChain.Back() != nil {
		t.Fatalf("back of chain should be nil but isn't")
	}

	// An empty list should have a nil Front() pointer.
	if memChain.Front() != nil {
		t.Fatalf("front of chain should be nil but isn't")
	}

	// The length of the chain at this point should be zero.
	if memChain.len != 0 {
		t.Fatalf("length of chain should be zero, is instead: %v",
			memChain.len)
	}

	// After we push back a single element to the empty list, the Front()
	// and Back() pointers should be identical.
	memChain.PushBack(Node{
		Height: 1,
	})

	if memChain.Front() != memChain.Back() {
		t.Fatalf("back and front of chain of length 1 should be " +
			"identical")
	}
}

// TestBoundedMemoryChainResetHeaderState tests that if we insert a number of
// elements, then reset the chain to nothing, it is identical to a newly
// created chain with only that element.
func TestBoundedMemoryChainResetHeaderState(t *testing.T) {
	t.Parallel()

	memChain := NewBoundedMemoryChain(5)

	// We'll start out by inserting 3 elements into the chain.
	const numElements = 3
	for i := 0; i < numElements; i++ {
		memChain.PushBack(Node{
			Height: int32(i),
		})
	}

	// With the set of elements inserted, we'll now pick a new element to
	// serve as the very head of the chain, with all other items removed.
	newNode := Node{
		Height: 4,
	}
	memChain.ResetHeaderState(newNode)

	// At this point, the front and back of the chain should be identical.
	if memChain.Front() != memChain.Back() {
		t.Fatalf("back and front of chain of length 1 should be " +
			"identical")
	}

	// Additionally, both the front and back of the chain should be
	// identical to the node above.
	if *memChain.Front() != newNode {
		t.Fatalf("wrong node, expected %v, got %v", newNode,
			memChain.Front())
	}
	if *memChain.Back() != newNode {
		t.Fatalf("wrong node, expected %v, got %v", newNode,
			memChain.Back())
	}
}

// TestBoundedMemoryChainSizeLimit tests that if we add elements until the size
// of the list if exceeded, then the list is properly bounded.
func TestBoundedMemoryChainSizeLimit(t *testing.T) {
	t.Parallel()

	memChain := NewBoundedMemoryChain(5)

	// We'll start out by inserting 20 elements into the memChain. As this
	// is greater than the total number of elements, we should end up with
	// the chain bounded at the end of the set of insertions.
	const numElements = 20
	var totalElems []Node
	for i := 0; i < numElements; i++ {
		node := Node{
			Height: int32(i),
		}
		memChain.PushBack(node)

		totalElems = append(totalElems, node)
	}

	// At this point, the length of the chain should still be 5, the total
	// number of elements.
	if memChain.len != 5 {
		t.Fatalf("wrong length, expected %v, got %v", 5, memChain.len)
	}

	// If we attempt to get the prev element front of the chain, we should
	// get a nil value.
	if memChain.Front().Prev() != nil {
		t.Fatalf("expected prev of tail to be nil, is instead: %v",
			spew.Sdump(memChain.Front().Prev()))
	}

	// The prev element to the back of the chain, should be the element
	// directly following it.
	expectedPrev := totalElems[len(totalElems)-2]
	if memChain.Back().Prev().Height != expectedPrev.Height {
		t.Fatalf("wrong node, expected %v, got %v", expectedPrev,
			memChain.Back().Prev())
	}

	// We'll now confirm that the remaining elements within the chain are
	// the as we expect, and that they have the proper prev element.
	for i, node := range memChain.chain {
		if node.Height != totalElems[15+i].Height {
			t.Fatalf("wrong node: expected %v, got %v",
				spew.Sdump(node),
				spew.Sdump(totalElems[15+i]))
		}

		if i == 0 {
			if node.Prev() != nil {
				t.Fatalf("prev of first elem should be nil")
			}
		} else {
			expectedPrevElem := memChain.chain[i-1]
			if node.Prev().Height != expectedPrevElem.Height {
				t.Fatalf("wrong node: expected %v, got %v",
					spew.Sdump(expectedPrevElem),
					spew.Sdump(node.Prev()))
			}
		}
	}
}

// TestBoundedMemoryChainPrevIteration tests that once we insert elements, we
// can properly traverse the entire chain backwards, starting from the final
// element.
func TestBoundedMemoryChainPrevIteration(t *testing.T) {
	t.Parallel()

	memChain := NewBoundedMemoryChain(5)

	// We'll start out by inserting 3 elements into the chain.
	const numElements = 3
	for i := 0; i < numElements; i++ {
		memChain.PushBack(Node{
			Height: int32(i),
		})
	}

	// We'll now add an additional element to the chain.
	iterNode := memChain.PushBack(Node{
		Height: 99,
	})

	// We'll now walk backwards with the iterNode until we run into the nil
	// pointer.
	for iterNode != nil {
		nextNode := iterNode
		iterNode = iterNode.Prev()

		if iterNode != nil && nextNode.Prev().Height != iterNode.Height {
			t.Fatalf("expected %v, got %v",
				spew.Sdump(nextNode.Prev()),
				spew.Sdump(iterNode))
		}
	}
}
