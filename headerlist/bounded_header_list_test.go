package headerlist

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"pgregory.net/rapid"
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

// TestBoundedMemoryChainAncestor ensures that skip-list ancestor lookups return
// the expected nodes, including after the bounded chain wraps.
func TestBoundedMemoryChainAncestor(t *testing.T) {
	t.Parallel()

	memChain := NewBoundedMemoryChain(100)
	for i := 0; i < 100; i++ {
		memChain.PushBack(Node{
			Height: int32(i),
		})
	}

	tip := memChain.Back()
	for i := 0; i < 100; i++ {
		ancestor := tip.Ancestor(int32(i))
		if ancestor == nil {
			t.Fatalf("expected ancestor at height %v", i)
		}
		if ancestor.Height != int32(i) {
			t.Fatalf("expected ancestor height %v, got %v",
				i, ancestor.Height)
		}
	}

	wrappedChain := NewBoundedMemoryChain(5)
	for i := 0; i < 20; i++ {
		wrappedChain.PushBack(Node{
			Height: int32(i),
		})
	}

	tip = wrappedChain.Back()
	for i := 15; i < 20; i++ {
		ancestor := tip.Ancestor(int32(i))
		if ancestor == nil {
			t.Fatalf("expected ancestor at height %v", i)
		}
		if ancestor.Height != int32(i) {
			t.Fatalf("expected ancestor height %v, got %v",
				i, ancestor.Height)
		}
	}

	if ancestor := tip.Ancestor(14); ancestor != nil {
		t.Fatalf("unexpected pruned ancestor: %v", ancestor.Height)
	}
}

// TestBoundedMemoryChainAncestorRapid uses property-based testing to confirm
// that skip-list ancestor lookups agree with a naive linear walk across many
// random chain shapes, including wrap-around configurations.
func TestBoundedMemoryChainAncestorRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapid.IntRange(1, 512).Draw(t, "capacity")
		startHeight := rapid.IntRange(0, 100000).Draw(t, "start_height")
		appendCount := rapid.IntRange(1, 2048).Draw(t, "append_count")

		memChain := NewBoundedMemoryChain(uint32(capacity))
		for i := 0; i < appendCount; i++ {
			memChain.PushBack(Node{
				Height: int32(startHeight + i),
			})
		}

		assertAncestorsMatchLinearWalk(t, memChain)
	})
}

// TestBoundedMemoryChainAncestorResetRapid is a property-based test that
// covers the post-reset path: after ResetHeaderState the chain seeds a new
// genesis at an arbitrary height, and subsequent PushBacks must still produce
// ancestor pointers that agree with a linear prev walk.
func TestBoundedMemoryChainAncestorResetRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapid.IntRange(1, 512).Draw(t, "capacity")
		beforeReset := rapid.IntRange(1, 2048).Draw(t, "before_reset")
		resetHeight := rapid.IntRange(0, 100000).Draw(t, "reset_height")
		afterReset := rapid.IntRange(0, 2048).Draw(t, "after_reset")

		memChain := NewBoundedMemoryChain(uint32(capacity))
		for i := 0; i < beforeReset; i++ {
			memChain.PushBack(Node{
				Height: int32(i),
			})
		}

		memChain.ResetHeaderState(Node{
			Height: int32(resetHeight),
		})
		for i := 1; i <= afterReset; i++ {
			memChain.PushBack(Node{
				Height: int32(resetHeight + i),
			})
		}

		assertAncestorsMatchLinearWalk(t, memChain)
	})
}

// assertAncestorsMatchLinearWalk fails the test if Node.Ancestor disagrees
// with a linear walk along prev pointers for any target height in or near the
// current chain bounds.
func assertAncestorsMatchLinearWalk(t rapid.TB, chain *BoundedMemoryChain) {
	t.Helper()

	tip := chain.Back()
	if tip == nil {
		return
	}

	front := chain.Front()
	minHeight := front.Height - 2
	maxHeight := tip.Height + 2
	for h := minHeight; h <= maxHeight; h++ {
		targetHeight := h
		expected := linearAncestor(tip, targetHeight)
		actual := tip.Ancestor(targetHeight)

		if expected == nil {
			if actual != nil {
				t.Fatalf("expected no ancestor at height %v, "+
					"got %v", targetHeight, actual.Height)
			}

			continue
		}

		if actual == nil {
			t.Fatalf("expected ancestor at height %v", targetHeight)
		}
		if actual.Height != expected.Height {
			t.Fatalf("expected ancestor height %v, got %v",
				expected.Height, actual.Height)
		}
	}
}

// linearAncestor returns the ancestor at the given height by walking prev
// pointers from tip one node at a time. It serves as the reference oracle
// against which the skip-list implementation is compared.
func linearAncestor(tip *Node, height int32) *Node {
	for tip != nil && tip.Height != height {
		tip = tip.Prev()
	}

	return tip
}

// BenchmarkBoundedMemoryChainAncestor measures skip-list ancestor lookup cost
// over a fixed spread of target heights, including small, mid-range, and tip
// targets that exercise different skip patterns.
func BenchmarkBoundedMemoryChainAncestor(b *testing.B) {
	memChain := NewBoundedMemoryChain(10000)
	for i := 0; i < 10000; i++ {
		memChain.PushBack(Node{
			Height: int32(i),
		})
	}

	tip := memChain.Back()
	targets := []int32{0, 1, 17, 499, 999, 2016, 4096, 8191, 9999}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, target := range targets {
			ancestor := tip.Ancestor(target)
			if ancestor == nil || ancestor.Height != target {
				b.Fatalf("unexpected ancestor at "+
					"height %v", target)
			}
		}
	}
}

// BenchmarkBoundedMemoryChainPrevWalk is the baseline counterpart to
// BenchmarkBoundedMemoryChainAncestor: it answers the same lookups using only
// the prev pointer so the two can be compared head-to-head.
func BenchmarkBoundedMemoryChainPrevWalk(b *testing.B) {
	memChain := NewBoundedMemoryChain(10000)
	for i := 0; i < 10000; i++ {
		memChain.PushBack(Node{
			Height: int32(i),
		})
	}

	tip := memChain.Back()
	targets := []int32{0, 1, 17, 499, 999, 2016, 4096, 8191, 9999}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, target := range targets {
			iter := tip
			for iter != nil && iter.Height != target {
				iter = iter.Prev()
			}
			if iter == nil || iter.Height != target {
				b.Fatalf("unexpected ancestor at "+
					"height %v", target)
			}
		}
	}
}
