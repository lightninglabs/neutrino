package query

import (
	"sort"
)

const (
	bestScore    = 0
	defaultScore = 4
	worstScore   = 8
)

// peerRanking is a struct that keeps history of peer's previous query success
// rate, and uses that to prioritise which peers to give the next queries to.
type peerRanking struct {
	// rank keeps track of the current set of peers and their score. A
	// lower score is better.
	rank map[string]uint64
}

// NewPeerRanking returns a new, empty ranking.
func NewPeerRanking() *peerRanking {
	return &peerRanking{
		rank: make(map[string]uint64),
	}
}

// Order sorts the given slice of peers based on their current score. If a
// peer has no current score given, the default will be used.
func (p *peerRanking) Order(peers []string) {
	sort.Slice(peers, func(i, j int) bool {
		score1, ok := p.rank[peers[i]]
		if !ok {
			score1 = defaultScore
		}

		score2, ok := p.rank[peers[j]]
		if !ok {
			score2 = defaultScore
		}
		return score1 < score2
	})
}

// AddPeer adds a new peer to the ranking, starting out with the default score.
func (p *peerRanking) AddPeer(peer string) {
	if _, ok := p.rank[peer]; ok {
		return
	}
	p.rank[peer] = defaultScore
}

// Punish increases the score of the given peer.
func (p *peerRanking) Punish(peer string) {
	score, ok := p.rank[peer]
	if !ok {
		return
	}

	// Cannot punish more.
	if score == worstScore {
		return
	}

	p.rank[peer] = score + 1
}

// Reward decreases the score of the given peer.
// TODO(halseth): use actual response time when ranking peers.
func (p *peerRanking) Reward(peer string) {
	score, ok := p.rank[peer]
	if !ok {
		return
	}

	// Cannot reward more.
	if score == bestScore {
		return
	}

	p.rank[peer] = score - 1
}
