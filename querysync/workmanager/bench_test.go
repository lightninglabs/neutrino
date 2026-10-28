package workmanager

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	// benchNumPeers is the number of mock peers the dispatcher benchmark
	// spreads each batch across.
	benchNumPeers = 8

	// benchBaseJobs is the smallest batch size the benchmark runs.
	benchBaseJobs = 64
)

// autoWorker is a mock Worker that immediately answers every job it is given
// with a successful result, so a benchmark measures dispatcher overhead rather
// than peer latency.
type autoWorker struct {
	peer    Peer
	nextJob chan *queryJob
}

// A compile-time check to ensure autoWorker satisfies the Worker interface.
var _ Worker = (*autoWorker)(nil)

// NewJob returns the channel the dispatcher sends jobs on.
func (a *autoWorker) NewJob() chan<- *queryJob {
	return a.nextJob
}

// Run answers each received job with an empty result until quit is closed.
func (a *autoWorker) Run(results chan<- *jobResult, quit <-chan struct{}) {
	for {
		select {
		case job := <-a.nextJob:
			select {
			case results <- &jobResult{job: job, peer: a.peer}:
			case <-quit:
				return
			}

		case <-quit:
			return
		}
	}
}

// BenchmarkDispatchBatch measures the time and allocations needed to dispatch
// a batch of jobs over a fixed set of instantly responding mock peers.
func BenchmarkDispatchBatch(b *testing.B) {
	for _, numJobs := range []int{benchBaseJobs, 4 * benchBaseJobs} {
		b.Run(fmt.Sprintf("jobs=%d", numJobs), func(b *testing.B) {
			benchDispatchBatch(b, numJobs)
		})
	}
}

func benchDispatchBatch(b *testing.B, numJobs int) {
	peerChan := make(chan Peer)
	ranking := &mockPeerRanking{}
	ranking.setLess(func(i, j string) bool {
		return i < j
	})

	wm := NewWorkManager(&Config{
		ConnectedPeers: func() (<-chan Peer, func(), error) {
			return peerChan, func() {}, nil
		},
		NewWorker: func(peer Peer) Worker {
			return &autoWorker{
				peer:    peer,
				nextJob: make(chan *queryJob, 1),
			}
		},
		Ranking: ranking,
	})
	require.NoError(b, wm.Start())
	defer func() {
		require.NoError(b, wm.Stop())
	}()

	for i := 0; i < benchNumPeers; i++ {
		peerChan <- &mockPeer{
			addr: fmt.Sprintf("mock%d", i),
			quit: make(chan struct{}),
		}
	}

	requests := make([]*Request, numJobs)
	for i := range requests {
		requests[i] = &Request{}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		errChan := wm.Query(requests, Timeout(time.Minute))
		require.NoError(b, <-errChan)
	}
}
