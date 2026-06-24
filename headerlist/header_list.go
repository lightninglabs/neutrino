package headerlist

import "github.com/btcsuite/btcd/wire/v2"

// Chain is an interface that stores a list of Nodes. Each node represents a
// header in the main chain and also includes a height along with it. This is
// meant to serve as a replacement to list.List which provides similar
// functionality, but allows implementations to use custom storage backends and
// semantics.
type Chain interface {
	// ResetHeaderState resets the state of all nodes. After this method, it will
	// be as if the chain was just newly created.
	ResetHeaderState(Node)

	// Back returns the end of the chain. If the chain is empty, then this
	// return a pointer to a nil node.
	Back() *Node

	// Front returns the head of the chain. If the chain is empty, then
	// this returns a  pointer to a nil node.
	Front() *Node

	// PushBack will push a new entry to the end of the chain. The entry
	// added to the chain is also returned in place.
	PushBack(Node) *Node
}

// Node is a node within the Chain. Each node stores a header as well as a
// height. Nodes can also be used to traverse the chain backwards via their
// Prev() method.
type Node struct {
	// Height is the height of this node within the main chain.
	Height int32

	// Header is the header that this node represents.
	Header wire.BlockHeader

	// prev is the node immediately before this one within the chain. The
	// genesis or "head" node has a nil prev pointer.
	prev *Node

	// ancestor is a skip-list pointer that may jump several heights back
	// in a single step, enabling O(log n) ancestor lookups. Skipping is
	// optional: callers may always fall back to walking prev.
	ancestor *Node
}

// Prev attempts to access the prior node within the header chain relative to
// this node. If this is the start of the chain, then this method will return
// nil.
func (n *Node) Prev() *Node {
	return n.prev
}

// invertLowestOne clears the lowest set bit in the binary representation of n.
// It is a small helper used to derive skip-list ancestor heights.
func invertLowestOne(n int32) int32 {
	return n & (n - 1)
}

// getAncestorHeight returns the skip-list ancestor height for a node at the
// given height. The result is chosen so that following ancestor pointers
// produces an O(log n) traversal, in the spirit of Bitcoin Core's CBlockIndex
// skip-list. Heights at or below zero have no meaningful ancestor and are
// clamped to zero.
func getAncestorHeight(height int32) int32 {
	if height <= 0 {
		return 0
	}

	return invertLowestOne(invertLowestOne(height))
}

// buildAncestor populates the skip-list ancestor pointer for this node by
// hopping back through the prior node's own skip-list. This must be called
// once, immediately after the prev pointer is set, so the skip-list stays
// consistent with the chain.
func (n *Node) buildAncestor() {
	if n.prev == nil {
		return
	}

	n.ancestor = n.prev.Ancestor(getAncestorHeight(n.Height))
}

// Ancestor returns the ancestor node at the target height. It uses the
// skip-list pointer to jump multiple heights when that pointer still lands at
// or above the target, and falls back to walking the prev pointer one node at
// a time when the skip would overshoot. The skip pointer is treated as an
// optimization: callers always get the same result they would have obtained by
// repeatedly calling Prev, just in fewer steps.
//
// If height is greater than n.Height, or the requested height is no longer in
// the chain (e.g. it was pruned out of a BoundedMemoryChain), nil is returned.
func (n *Node) Ancestor(height int32) *Node {
	if n == nil || height > n.Height {
		return nil
	}

	for n != nil && n.Height != height {
		// Take the skip-list shortcut only when it still lands at or
		// above the target and actually makes progress (the ancestor
		// height must be strictly less than the current height). The
		// height-vs-target check on the ancestor itself guards against
		// stale skip pointers in a wrapped BoundedMemoryChain where the
		// pointed-to slot may have been overwritten.
		ancestorHeight := getAncestorHeight(n.Height)
		if n.ancestor != nil && ancestorHeight >= height &&
			n.ancestor.Height >= height &&
			n.ancestor.Height < n.Height {

			n = n.ancestor
			continue
		}

		n = n.prev
	}

	return n
}
