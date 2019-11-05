package query

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

type mockWorker struct {
	peer    Peer
	nextJob chan *queryJob
	results chan *jobResult
}

var _ Worker = (*mockWorker)(nil)

func (m *mockWorker) exited() <-chan struct{} {
	return nil
}

func (m *mockWorker) NewJob() chan<- *queryJob {
	return m.nextJob
}

func (m *mockWorker) Run(results chan<- *jobResult,
	quit <-chan struct{}) {

	// We'll forward the mocked responses on the result channel.
	for {
		select {
		case r := <-m.results:
			// Set the peer before forwarding it.
			r.peer = m.peer

			results <- r
		case <-quit:
			return
		}
	}
}

type mockPeerRanking struct {
	less func(i, j string) bool
}

var _ PeerRanking = (*mockPeerRanking)(nil)

func (p *mockPeerRanking) AddPeer(peer string) {
}

func (p *mockPeerRanking) Order(peers []string) {
	if p.less == nil {
		return
	}

	sort.Slice(peers, func(i, j int) bool {
		return p.less(peers[i], peers[j])
	})
}

func (p *mockPeerRanking) addPeer(peer string) {
}

func (p *mockPeerRanking) Punish(peer string) {
}

func (p *mockPeerRanking) Reward(peer string) {
}

// startWorkManager starts a new workmanager with the given number of mock
// workers.
func startWorkManager(t *testing.T, numWorkers int) (*WorkManager,
	[]*mockWorker) {

	// We set up a custom NewWorker closure for the WorkManager, such that
	// we can start mockWorkers when it is called.
	workerChan := make(chan *mockWorker)

	peerChan := make(chan Peer)
	wm := New(&Config{
		ConnectedPeers: func() (<-chan Peer, func(), error) {
			return peerChan, func() {}, nil
		},
		NewWorker: func(peer Peer) Worker {
			m := &mockWorker{
				peer:    peer,
				nextJob: make(chan *queryJob),
				results: make(chan *jobResult),
			}
			workerChan <- m
			return m
		},
		Ranking: &mockPeerRanking{},
	})

	// Start the work manager.
	wm.Start()

	// We'll notify about a set of connected peers, and expect it to start
	// a new worker for each.
	workers := make([]*mockWorker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		peer := &mockPeer{
			addr: fmt.Sprintf("mock%v", i),
		}
		select {
		case peerChan <- peer:
		case <-time.After(time.Second):
			t.Fatal("work manager did not receive peer")
		}

		// Wait for the worker to be started.
		var w *mockWorker
		select {
		case w = <-workerChan:
		case <-time.After(time.Second):
			t.Fatalf("no worker")
		}

		workers[i] = w
	}

	return wm, workers
}

// TestWorkManagerWorkDispatcherSingleWorker tests that the workDispatcher
// goroutine properly schedules the incoming queries in the order of their batch
// and sends them to the worker.
func TestWorkManagerWorkDispatcherSingleWorker(t *testing.T) {
	const numQueries = 100

	// Start work manager with a sinlge worker.
	wm, workers := startWorkManager(t, 1)

	// Schedule a batch of queries.
	var queries []*Request
	for i := 0; i < numQueries; i++ {
		q := &Request{}
		queries = append(queries, q)
	}

	errChan := wm.Query(queries)

	wk := workers[0]

	// Each query should be sent on the nextJob queue, in the order they
	// had in their batch.
	for i := uint64(0); i < numQueries; i++ {
		var job *queryJob
		select {
		case job = <-wk.nextJob:
			if job.index != i {
				t.Fatalf("wrong index")
			}
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}

		// Respond with a success result.
		select {
		case wk.results <- &jobResult{
			job: job,
			err: nil,
		}:
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}

	// The query should exit with a non-error.
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("got error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("nothing received on errChan")
	}
}

// TestWorkManagerDispatcherFailure tests that queries that fail gets resent to
// workers.
func TestWorkManagerWorkDispatcherFailures(t *testing.T) {
	const numQueries = 100

	// Start work manager with as many workers as queries. This is not very
	// realistic, but makes the work manager able to schedule all queries
	// concurrently.
	wm, workers := startWorkManager(t, numQueries)

	// When the jobs gets scheduled, keep track of which worker was
	// assigned the job.
	type sched struct {
		wk  *mockWorker
		job *queryJob
	}

	// Schedule a batch of queries.
	var scheduledJobs [numQueries]chan sched
	var queries [numQueries]*Request
	for i := 0; i < numQueries; i++ {
		q := &Request{}
		queries[i] = q
		scheduledJobs[i] = make(chan sched)
	}

	// Fot each wotker, spin up a goroutine that will forward the job it
	// got to our slice of sheduled jobs, such that we can handle them in
	// order.
	for i := 0; i < len(workers); i++ {
		wk := workers[i]
		go func() {
			for {
				select {
				case job := <-wk.nextJob:
					scheduledJobs[job.index] <- sched{
						wk:  wk,
						job: job,
					}
				}
			}
		}()
	}

	// Send the batch, and Retrieve all jobs immediately.
	errChan := wm.Query(queries[:])

	var jobs [numQueries]sched
	for i := uint64(0); i < numQueries; i++ {
		var s sched
		select {
		case s = <-scheduledJobs[i]:
			if s.job.index != i {
				t.Fatalf("wrong index")
			}

		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}

		jobs[s.job.index] = s
	}

	// Go backwards, and fail half of them.
	for i := numQueries - 1; i >= 0; i-- {
		var err error
		if i%2 == 0 {
			err = fmt.Errorf("failed job")
		}

		select {
		case jobs[i].wk.results <- &jobResult{
			job: jobs[i].job,
			err: err,
		}:
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}

	// Finally, make sure the failed jobs are being retried, in the same
	// order as they were originally scheduled.
	for i := uint64(0); i < numQueries; i += 2 {
		var s sched
		select {
		case s = <-scheduledJobs[i]:
			if s.job.index != i {
				t.Fatalf("wrong index")
			}
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}
		select {
		case s.wk.results <- &jobResult{
			job: s.job,
			err: nil,
		}:
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}

	// The query should ultimately succeed.
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("got error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("nothing received on errChan")
	}
}

// TestWorkManagerCancelBatch checks that we can cancel a batch query midway,
// and that the jobs it contains are canceled.
func TestWorkManagerCancelBatch(t *testing.T) {
	const numQueries = 100

	// Start the workDispatcher goroutine.
	wm, workers := startWorkManager(t, 1)
	wk := workers[0]

	// Schedule a batch of queries.
	var queries []*Request
	for i := 0; i < numQueries; i++ {
		q := &Request{}
		queries = append(queries, q)
	}

	// Send the query, and include a channel to cancel the batch.
	cancelChan := make(chan struct{})
	errChan := wm.Query(queries, Cancel(cancelChan))

	// Respond with a result to half of the queries.
	for i := 0; i < numQueries/2; i++ {
		var job *queryJob
		select {
		case job = <-wk.nextJob:
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}

		// Respond with a success result.
		select {
		case wk.results <- &jobResult{
			job: job,
			err: nil,
		}:
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}

	// Cancel the batch.
	close(cancelChan)

	// All remaining queries should be canceled.
	for i := 0; i < numQueries/2; i++ {
		var job *queryJob
		select {
		case job = <-wk.nextJob:
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}

		select {
		case <-job.cancelChan:
		case <-time.After(time.Second):
			t.Fatalf("job not canceled")
		}

		select {
		case wk.results <- &jobResult{
			job: job,
			err: ErrJobCanceled,
		}:
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}

	}

	// The query should exit with an error.
	select {
	case err := <-errChan:
		if err != ErrJobCanceled {
			t.Fatalf("expected ErrJobCanceled, got : %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("nothing received on errChan")
	}
}

// TestWorkManaferWorkRankingScheduling checks that the work manager schedules
// jobs among workers according to the peer ranking.
func TestWorkManagerWorkRankingScheduling(t *testing.T) {
	const numQueries = 4
	const numWorkers = 8

	wm, workers := startWorkManager(t, numWorkers)

	// Set up the ranking to prioritize lower numbered workers.
	wm.cfg.Ranking.(*mockPeerRanking).less = func(i, j string) bool {
		return i < j
	}

	// Schedule a batch of queries.
	var queries []*Request
	for i := 0; i < numQueries; i++ {
		q := &Request{}
		queries = append(queries, q)
	}

	// Send the batch, and Retrieve all jobs immediately.
	errChan := wm.Query(queries)

	// The 4 first workers should get the job.
	var jobs []*queryJob
	for i := 0; i < numQueries; i++ {
		select {
		case job := <-workers[i].nextJob:
			if job.index != uint64(i) {
				t.Fatalf("unexpected job")
			}
			jobs = append(jobs, job)

		case <-time.After(time.Second):
			t.Fatalf("job not scheduled")
		}
	}

	// Alter the priority to prioritize even mock workers.
	wm.cfg.Ranking.(*mockPeerRanking).less = func(i, j string) bool {
		even := func(p string) bool {
			if p == "mock0" || p == "mock2" || p == "mock4" ||
				p == "mock6" {
				return true
			}
			return false
		}

		if even(i) && !even(j) {
			return true
		}

		if even(j) && !even(i) {
			return false
		}

		return i < j

	}
	// Go backwards, and succeed the queries.
	for i := numQueries - 1; i >= 0; i-- {
		select {
		case workers[i].results <- &jobResult{
			job: jobs[i],
			err: nil,
		}:
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}

	// Sleep to make sure all results are forwarded to the workmanager.
	time.Sleep(50 * time.Millisecond)

	// Send a new set of queries.
	queries = nil
	for i := 0; i < numQueries; i++ {
		q := &Request{}
		queries = append(queries, q)
	}
	errChan = wm.Query(queries)

	// The new jobs should be scheduled on the even numbered workers.
	for i := 0; i < len(workers); i += 2 {
		select {
		case <-workers[i].nextJob:
		case <-time.After(time.Second):
			t.Fatalf("job not scheduled")
		}
	}
}
