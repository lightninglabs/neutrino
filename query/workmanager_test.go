package query

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockWorker struct {
	peer    Peer
	nextJob chan *queryJob
	results chan *jobResult
}

var _ Worker = (*mockWorker)(nil)

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

func (p *mockPeerRanking) Punish(peer string) {
}

func (p *mockPeerRanking) Reward(peer string) {
}

func (p *mockPeerRanking) ResetRanking(peer string) {
}

// startWorkManager starts a new workmanager with the given number of mock
// workers.
func startWorkManager(t *testing.T, numWorkers int) (WorkManager,
	[]*mockWorker) {

	// We set up a custom NewWorker closure for the WorkManager, such that
	// we can start mockWorkers when it is called.
	workerChan := make(chan *mockWorker)

	peerChan := make(chan Peer)
	wm := NewWorkManager(&Config{
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

	// Start work manager with a single worker.
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

	// For each worker, spin up a goroutine that will forward the job it
	// got to our slice of scheduled jobs, such that we can handle them in
	// order.
	for i := 0; i < len(workers); i++ {
		wk := workers[i]
		go func() {
			for {
				job := <-wk.nextJob
				scheduledJobs[job.index] <- sched{
					wk:  wk,
					job: job,
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

// TestWorkManagerWorkRankingScheduling checks that the work manager schedules
// jobs among workers according to the peer ranking.
func TestWorkManagerWorkRankingScheduling(t *testing.T) {
	const numQueries = 4
	const numWorkers = 8

	workMgr, workers := startWorkManager(t, numWorkers)

	require.IsType(t, &peerWorkManager{}, workMgr)
	wm := workMgr.(*peerWorkManager) //nolint:forcetypeassert

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
	_ = wm.Query(queries)

	// The new jobs should be scheduled on the even numbered workers.
	for i := 0; i < len(workers); i += 2 {
		select {
		case <-workers[i].nextJob:
		case <-time.After(time.Second):
			t.Fatalf("job not scheduled")
		}
	}
}

// queryJobWithWorkerIndex is used to know which worker was used for the
// corresponding job request to signal the result back to the result channel.
type queryJobWithWorkerIndex struct {
	worker int
	job    *queryJob
}

// mergeWorkChannels is used to merge the channels of all the workers into a one
// single one for better control of the concurrency during testing.
func mergeWorkChannels(workers []*mockWorker) <-chan queryJobWithWorkerIndex {
	var wg sync.WaitGroup
	merged := make(chan queryJobWithWorkerIndex)

	// Function to copy data from each worker channel to the merged channel
	readFromWorker := func(input <-chan *queryJob, worker int) {
		defer wg.Done()
		for {
			value, ok := <-input
			if !ok {
				// Channel is closed, exit the loop
				return
			}
			merged <- queryJobWithWorkerIndex{
				worker: worker,
				job:    value,
			}
		}
	}

	// Start a goroutine for each worker channel.
	wg.Add(len(workers))
	for i, work := range workers {
		go readFromWorker(work.nextJob, i)
	}

	// Wait for all copying to be done, then close the merged channel
	go func() {
		wg.Wait()
		close(merged)
	}()

	return merged
}

// TestWorkManagerTimeOutBatch tests that as soon as a batch times-out all the
// ongoing queries already registered with workers and also the queued up ones
// are canceled.
func TestWorkManagerTimeOutBatch(t *testing.T) {
	const numQueries = 100
	const numWorkers = 10

	// Start the workDispatcher goroutine.
	wm, workers := startWorkManager(t, numWorkers)

	// mergeChan is the channel which receives all the jobQueries
	// sequentially which are sent to the registered workers.
	mergeChan := mergeWorkChannels(workers)

	// activeQueries are the queries currently registered with the workers.
	var activeQueries []queryJobWithWorkerIndex

	// Schedule a batch of queries.
	var queries []*Request
	for i := 0; i < numQueries; i++ {
		q := &Request{}
		queries = append(queries, q)
	}

	// Send the batch query (including numQueries), and include a channel
	// to cancel the batch.
	//
	// NOTE: We will timeout the batch to simulate a slow peer connection
	// and make sure we cancel all ongoing queries including the ones which
	// are still queued up.
	errChan := wm.Query(queries, Timeout(1*time.Second))

	// Send a query to every active worker.
	for i := 0; i < numWorkers; i++ {
		select {
		case jobQuery := <-mergeChan:
			activeQueries = append(activeQueries, jobQuery)
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(5 * time.Second):
			t.Fatalf("next job not received")
		}
	}

	// We wait before we send the result for one query to exceed the timeout
	// of the batch.
	time.Sleep(2 * time.Second)

	// We need to signal a result for one of the active workers so that
	// the batch timeout is triggered.
	workerIndex := activeQueries[0].worker
	workers[workerIndex].results <- &jobResult{
		job: activeQueries[0].job,
		err: nil,
	}

	// As soon as the batch times-out an error is sent via the errChan.
	select {
	case err := <-errChan:
		require.ErrorIs(t, err, ErrQueryTimeout)
	case <-time.After(time.Second):
		t.Fatalf("expected for the errChan to signal")
	}

	// The cancelChan got closed, this happens when the batch times-out.
	// So all the ongoing queries are canceled as well.
	for i := 1; i < numWorkers; i++ {
		job := activeQueries[i].job
		select {
		case <-job.internalCancelChan:
			workers[i].results <- &jobResult{
				job: job,
				err: nil,
			}
		case <-time.After(time.Second):
			t.Fatalf("expected for the cancelChan to close")
		}
	}

	// Make also sure that all the queued queries for this batch are
	// canceled as well.
	for i := numWorkers; i < numQueries; i++ {
		select {
		case res := <-mergeChan:
			job := res.job
			workerIndex := res.worker
			select {
			case <-job.internalCancelChan:
				workers[workerIndex].results <- &jobResult{
					job: job,
					err: nil,
				}
			case <-time.After(time.Second):
				t.Fatalf("expected for the cancelChan to close")
			}
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}
	}
}

// TestWorkManagerProgressTimeoutResets verifies that a batch configured with
// ProgressTimeout keeps running as long as queries continue to complete
// successfully within the idle window — even when the batch's total runtime
// far exceeds the idle window.
func TestWorkManagerProgressTimeoutResets(t *testing.T) {
	const numQueries = 10
	const progressTimeout = 150 * time.Millisecond
	const stepInterval = 50 * time.Millisecond

	wm, workers := startWorkManager(t, 1)
	wk := workers[0]

	var queries []*Request
	for i := 0; i < numQueries; i++ {
		queries = append(queries, &Request{})
	}

	// Use a generous hard deadline so the idle timer is the effective
	// bound — Timeout normalizes non-positive values to fire-fast, so
	// "effectively unbounded" must be expressed as a value well past
	// the test's runtime budget.
	errChan := wm.Query(
		queries, Timeout(time.Hour),
		ProgressTimeout(progressTimeout),
	)

	// Feed one successful result every stepInterval. Total runtime is
	// numQueries*stepInterval = 500ms, well over the 150ms idle window —
	// any failure here means the idle timer was not being reset on
	// successful progress.
	for i := 0; i < numQueries; i++ {
		var job *queryJob
		select {
		case job = <-wk.nextJob:
		case err := <-errChan:
			t.Fatalf("did not expect on errChan, got: %v", err)
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}

		time.Sleep(stepInterval)

		select {
		case wk.results <- &jobResult{job: job, err: nil}:
		case err := <-errChan:
			t.Fatalf("did not expect on errChan, got: %v", err)
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}

	select {
	case err := <-errChan:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatalf("nothing received on errChan")
	}
}

// TestWorkManagerProgressTimeoutFires verifies that a batch configured with
// ProgressTimeout is canceled when no successful query completes within the
// idle window, and that all in-flight and queued queries are signaled via
// internalCancelChan.
func TestWorkManagerProgressTimeoutFires(t *testing.T) {
	const numQueries = 50
	const numWorkers = 10
	const progressTimeout = 200 * time.Millisecond

	wm, workers := startWorkManager(t, numWorkers)
	mergeChan := mergeWorkChannels(workers)

	var queries []*Request
	for i := 0; i < numQueries; i++ {
		queries = append(queries, &Request{})
	}

	errChan := wm.Query(
		queries, Timeout(time.Hour),
		ProgressTimeout(progressTimeout),
	)

	// Collect the jobs dispatched to the active workers. We never feed
	// a result, so the idle timer should fire after progressTimeout.
	activeQueries := make([]queryJobWithWorkerIndex, 0, numWorkers)
	for i := 0; i < numWorkers; i++ {
		select {
		case jq := <-mergeChan:
			activeQueries = append(activeQueries, jq)
		case <-errChan:
			t.Fatalf("did not expect on errChan yet")
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}
	}

	// The batch cancel is delivered when the workmanager processes the
	// next result, so feed one to nudge it after the idle window
	// elapses. That result is discarded with the "batch already
	// canceled" log line but triggers the timeout select.
	time.Sleep(progressTimeout + 50*time.Millisecond)

	workerIndex := activeQueries[0].worker
	workers[workerIndex].results <- &jobResult{
		job: activeQueries[0].job,
		err: nil,
	}

	select {
	case err := <-errChan:
		require.ErrorIs(t, err, ErrQueryTimeout)
	case <-time.After(time.Second):
		t.Fatalf("expected errChan to signal idle timeout")
	}

	// All remaining in-flight queries should observe their
	// internalCancelChan close.
	for i := 1; i < numWorkers; i++ {
		job := activeQueries[i].job
		select {
		case <-job.internalCancelChan:
			workers[i].results <- &jobResult{job: job, err: nil}
		case <-time.After(time.Second):
			t.Fatalf("expected internalCancelChan to close")
		}
	}

	// Any still-queued jobs should also see internalCancelChan close.
	for i := numWorkers; i < numQueries; i++ {
		select {
		case res := <-mergeChan:
			select {
			case <-res.job.internalCancelChan:
				workers[res.worker].results <- &jobResult{
					job: res.job,
					err: nil,
				}
			case <-time.After(time.Second):
				t.Fatalf("expected internalCancelChan " +
					"close on queued job")
			}
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}
	}
}

// TestWorkManagerBothTimeouts verifies that when both Timeout and
// ProgressTimeout are set, the hard wall-clock deadline still fires even when
// the idle timer is constantly being reset by steady successes.
func TestWorkManagerBothTimeouts(t *testing.T) {
	const numQueries = 1000
	const hardTimeout = 300 * time.Millisecond
	const idleTimeout = 5 * time.Second // Effectively never fires.

	wm, workers := startWorkManager(t, 1)
	wk := workers[0]

	var queries []*Request
	for i := 0; i < numQueries; i++ {
		queries = append(queries, &Request{})
	}

	errChan := wm.Query(
		queries, Timeout(hardTimeout), ProgressTimeout(idleTimeout),
	)

	start := time.Now()

	// Feed successes one per ~10ms — total natural runtime is ~10s,
	// far longer than the 300ms hard deadline. The hard timeout must
	// fire despite the idle timer being constantly reset.
	stop := make(chan struct{})
	go func() {
		for {
			var job *queryJob
			select {
			case job = <-wk.nextJob:
			case <-stop:
				return
			}
			select {
			case wk.results <- &jobResult{job: job, err: nil}:
			case <-stop:
				return
			}
			select {
			case <-time.After(10 * time.Millisecond):
			case <-stop:
				return
			}
		}
	}()
	defer close(stop)

	select {
	case err := <-errChan:
		elapsed := time.Since(start)
		require.ErrorIs(t, err, ErrQueryTimeout)
		require.GreaterOrEqual(t, elapsed, hardTimeout)
		require.Less(t, elapsed, hardTimeout+500*time.Millisecond,
			"hard timeout fired far too late")
	case <-time.After(2 * time.Second):
		t.Fatalf("hard timeout did not fire")
	}
}

// TestWorkManagerProgressTimeoutZeroPeers is the regression test for the
// "progress timer never observed" bug. It dispatches a batch when no workers
// are connected. With the timer wired into the dispatcher's outer select via
// w.progressWakes, the batch must still be canceled with ErrQueryTimeout when
// the idle window elapses, even though no jobResult will ever be produced
// (there are no workers to produce one). Without that wake path, the batch
// would hang until the workmanager itself is torn down.
func TestWorkManagerProgressTimeoutZeroPeers(t *testing.T) {
	const progressTimeout = 200 * time.Millisecond

	// Build the workmanager directly without announcing any peers, so
	// there are zero workers. This is the configuration that exposes
	// the bug.
	peerChan := make(chan Peer)
	workerChan := make(chan *mockWorker, 1)
	wm := NewWorkManager(&Config{
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
	require.NoError(t, wm.Start())
	defer func() {
		require.NoError(t, wm.Stop())
	}()

	// Schedule a one-shot batch with a hard deadline well past the
	// idle window; the idle timer is the effective bound. Crucially,
	// no peer is announced, so no worker is created, so no jobResult
	// will ever arrive.
	queries := []*Request{{}}
	start := time.Now()
	errChan := wm.Query(
		queries, Timeout(time.Hour),
		ProgressTimeout(progressTimeout),
	)

	select {
	case err := <-errChan:
		require.ErrorIs(t, err, ErrQueryTimeout)
		elapsed := time.Since(start)
		require.GreaterOrEqual(t, elapsed, progressTimeout)
		require.Less(t, elapsed, progressTimeout+time.Second,
			"idle timer fired far too late under zero-peer "+
				"conditions")
	case <-time.After(2 * time.Second):
		t.Fatalf("batch hung past idle window with no workers; " +
			"progress timer is not being observed by the " +
			"dispatcher's outer select")
	}
}

// TestWorkManagerProgressTimeoutFailuresDontReset verifies that failed
// results do not reset the idle timer — only successful completions count as
// progress. A batch where every result is a failure (rescheduled forever via
// NoRetryMax) must still be canceled by the idle timer.
func TestWorkManagerProgressTimeoutFailuresDontReset(t *testing.T) {
	const numWorkers = 4
	const progressTimeout = 250 * time.Millisecond

	wm, workers := startWorkManager(t, numWorkers)
	mergeChan := mergeWorkChannels(workers)

	var queries []*Request
	for i := 0; i < numWorkers; i++ {
		queries = append(queries, &Request{})
	}

	errChan := wm.Query(
		queries, Timeout(time.Hour), NoRetryMax(),
		ProgressTimeout(progressTimeout),
	)

	// Continuously feed failures back. NoRetryMax means jobs are
	// rescheduled indefinitely; failures must not decrement batch.rem
	// and must not reset the idle timer.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case res := <-mergeChan:
				select {
				case workers[res.worker].results <- &jobResult{
					job: res.job,
					err: ErrQueryTimeout,
				}:
				case <-stop:
					return
				}
			}
		}
	}()
	defer close(stop)

	start := time.Now()
	select {
	case err := <-errChan:
		require.ErrorIs(t, err, ErrQueryTimeout)
		elapsed := time.Since(start)
		require.GreaterOrEqual(t, elapsed, progressTimeout)
		require.Less(t, elapsed, progressTimeout+time.Second,
			"idle timeout fired far too late")
	case <-time.After(2 * time.Second):
		t.Fatalf("idle timeout did not fire despite no progress")
	}
}

// TestWorkManagerTimeoutNonPositiveNormalized verifies that Timeout(0) and
// Timeout(d<0) are normalized to a fire-fast deadline. This preserves the
// pre-PR semantics of time.After(d) for d<=0 (fires immediately) so external
// consumers passing an uninitialized or computed-negative time.Duration get
// the historical fail-fast behaviour, not a silently unbounded batch.
func TestWorkManagerTimeoutNonPositiveNormalized(t *testing.T) {
	cases := []struct {
		name string
		d    time.Duration
	}{
		{"zero", 0},
		{"negative", -1 * time.Second},
		{"min int64", time.Duration(math.MinInt64)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wm, workers := startWorkManager(t, 1)
			wk := workers[0]

			// Submit a 2-query batch with a non-positive Timeout.
			// After normalization the deadline is 1ns, already
			// expired by the time the first jobResult is
			// delivered. Two queries (not one) is important: a
			// single-query batch completes on the success branch
			// before the hard-timeout select is reached, so we
			// need at least one query still outstanding when the
			// inner select runs.
			errChan := wm.Query(
				[]*Request{{}, {}}, Timeout(tc.d),
			)

			var job *queryJob
			select {
			case job = <-wk.nextJob:
			case <-time.After(2 * time.Second):
				t.Fatalf("worker did not receive job")
			}
			select {
			case wk.results <- &jobResult{job: job, err: nil}:
			case <-time.After(2 * time.Second):
				t.Fatalf("result not consumed")
			}

			select {
			case err := <-errChan:
				require.ErrorIs(t, err, ErrQueryTimeout,
					"non-positive Timeout was not "+
						"normalized to a fire-fast "+
						"deadline")
			case <-time.After(2 * time.Second):
				t.Fatalf("Timeout(%v) did not fire", tc.d)
			}
		})
	}
}
