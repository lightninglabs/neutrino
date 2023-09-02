package query

import (
	"fmt"
	"testing"
)

// TestPeerRank checks that the peerRanking correctly orders peers according to
// how they are rewarded and punished.
func TestPeerRank(t *testing.T) {
	const numPeers = 8

	ranking := NewPeerRanking()
	var peers []string
	for i := 0; i < numPeers; i++ {
		p := fmt.Sprintf("peer%d", i)
		peers = append(peers, p)
		ranking.AddPeer(p)
	}

	// We'll try to order half of the peers.
	peers = peers[:numPeers/2]
	ranking.Order(peers)

	// Since no peer was rewarded or punished, their order
	// should be unchanged.
	for i := 0; i < numPeers/2; i++ {
		p := fmt.Sprintf("peer%d", i)
		if peers[i] != p {
			t.Fatalf("expected %v, got %v", p, peers[i])
		}
	}

	// Punish the first ones more, which should flip the order.
	for i := 0; i < numPeers/2; i++ {
		for j := 0; j <= i; j++ {
			ranking.Punish(peers[j])
		}
	}

	ranking.Order(peers)
	for i := 0; i < numPeers/2; i++ {
		p := fmt.Sprintf("peer%d", numPeers/2-i-1)
		if peers[i] != p {
			t.Fatalf("expected %v, got %v", p, peers[i])
		}
	}

	// This is the lowest scored peer after punishment.
	const lowestScoredPeer = "peer0"

	// Reward the lowest scored one a bunch, which should move it
	// to the front.
	for i := 0; i < 10; i++ {
		ranking.Reward(lowestScoredPeer)
	}

	ranking.Order(peers)
	if peers[0] != lowestScoredPeer {
		t.Fatalf("peer0 was not first")
	}

	// Punish the peer a bunch to make it the lowest scored one.
	for i := 0; i < 10; i++ {
		ranking.Punish(lowestScoredPeer)
	}

	ranking.Order(peers)
	if peers[len(peers)-1] != lowestScoredPeer {
		t.Fatalf("peer0 should be last")
	}

	// Reset its ranking. It should have the default score now
	// and should not be the lowest ranked peer.
	ranking.ResetRanking(lowestScoredPeer)
	ranking.Order(peers)
	if peers[len(peers)-1] == lowestScoredPeer {
		t.Fatalf("peer0 should not be last.")
	}
}
