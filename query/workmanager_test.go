package query

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockWorker struct {
	peer    Peer
	nextJob chan *queryJob
	results chan *jobResult
}

func (m *mockWorker) IsPeerBehindStartHeight(req ReqMessage) bool {
	return m.peer.IsPeerBehindStartHeight(req)
}

func (m *mockWorker) IsSyncCandidate() bool {
	return m.peer.IsSyncCandidate()
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

type ctx struct {
	wm       WorkManager
	peerChan chan Peer
}

// startWorkManager starts a new workmanager with the given number of mock
// workers.
func startWorkManager(t *testing.T, numWorkers int) (ctx,
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

	return ctx{
		wm:       wm,
		peerChan: peerChan,
	}, workers
}

// TestWorkManagerWorkDispatcherSingleWorker tests that the workDispatcher
// goroutine properly schedules the incoming queries in the order of their batch
// and sends them to the worker.
func TestWorkManagerWorkDispatcherSingleWorker(t *testing.T) {
	const numQueries = 100

	// Start work manager with a sinlge worker.
	c, workers := startWorkManager(t, 1)
	wm := c.wm
	// Schedule a batch of queries.
	var queries []*Request
	for i := 0; i < numQueries; i++ {
		q := &Request{
			Req: &mockQueryEncoded{},
		}

		queries = append(queries, q)
	}

	errChan := wm.Query(queries, ErrChan(make(chan error, 1)))

	wk := workers[0]

	// Each query should be sent on the nextJob queue, in the order they
	// had in their batch.
	for i := float64(0); i < numQueries; i++ {
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
	c, workers := startWorkManager(t, numQueries)
	wm := c.wm

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
		q := &Request{
			Req: &mockQueryEncoded{},
		}
		queries[i] = q
		scheduledJobs[i] = make(chan sched)
	}

	// Fot each worker, spin up a goroutine that will forward the job it
	// got to our slice of scheduled jobs, such that we can handle them in
	// order.
	for i := 0; i < len(workers); i++ {
		wk := workers[i]
		go func() {
			for {
				job := <-wk.nextJob
				scheduledJobs[int(job.index)] <- sched{
					wk:  wk,
					job: job,
				}
			}
		}()
	}

	// Send the batch, and Retrieve all jobs immediately.
	errChan := wm.Query(queries[:], ErrChan(make(chan error, 1)))

	var jobs [numQueries]sched
	for i := 0; i < numQueries; i++ {
		var s sched
		select {
		case s = <-scheduledJobs[i]:
			if s.job.index != float64(i) {
				t.Fatalf("wrong index")
			}

		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}

		jobs[int(s.job.index)] = s
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
	for i := float64(0); i < numQueries; i += 2 {
		var s sched
		select {
		case s = <-scheduledJobs[int(i)]:
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

// TestWorkManagerErrQueryTimeout tests that the workers that return query
// timeout are not sent jobs until they return a different error.
func TestWorkManagerErrQueryTimeout(t *testing.T) {
	const numQueries = 1
	const numWorkers = 1

	// Start work manager.
	c, workers := startWorkManager(t, numWorkers)
	wm := c.wm

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
		q := &Request{
			Req: &mockQueryEncoded{},
		}
		queries[i] = q
		scheduledJobs[i] = make(chan sched)
	}

	// Spin up goroutine for only one worker. Forward gotten jobs
	// to our slice of scheduled jobs, such that we can handle them in
	// order.
	wk := workers[0]
	go func() {
		for {
			job := <-wk.nextJob
			scheduledJobs[int(job.index)] <- sched{
				wk:  wk,
				job: job,
			}
		}
	}()

	// Send the batch, and Retrieve all jobs immediately.
	errChan := wm.Query(queries[:], ErrChan(make(chan error, 1)))

	var s sched

	// Ensure job is sent to the worker.
	select {
	case s = <-scheduledJobs[0]:
		if s.job.index != float64(0) {
			t.Fatalf("wrong index")
		}
	case <-errChan:
		t.Fatalf("did not expect on errChan")
	case <-time.After(time.Second):
		t.Fatalf("next job not received")
	}

	// Return jobResult with an ErrQueryTimeout.
	select {
	case s.wk.results <- &jobResult{
		job: s.job,
		err: ErrQueryTimeout,
	}:
	case <-errChan:
		t.Fatalf("did not expect on errChan")
	case <-time.After(time.Second):
		t.Fatalf("result not handled")
	}

	// Make sure the job is not retried as there are no available
	// peer to retry it. The only available worker should be waiting in the
	// worker feedback loop.
	select {
	case <-scheduledJobs[0]:
		t.Fatalf("did not expect job rescheduled")
	case <-errChan:
		t.Fatalf("did not expect on errChan")
	case <-time.After(time.Second):
	}

	// There should be no errChan message as query is still incomplete.
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("got error: %v", err)
		}
		t.Fatalf("expected no errChan message")
	case <-time.After(time.Second):
	}
}

// TestWorkManagerCancelBatch checks that we can cancel a batch query midway,
// and that the jobs it contains are canceled.
func TestWorkManagerCancelBatch(t *testing.T) {
	const numQueries = 100

	// Start the workDispatcher goroutine.
	c, workers := startWorkManager(t, 1)
	wm := c.wm
	wk := workers[0]

	// Schedule a batch of queries.
	var queries []*Request
	for i := 0; i < numQueries; i++ {
		q := &Request{
			Req: &mockQueryEncoded{},
		}
		queries = append(queries, q)
	}

	// Send the query, and include a channel to cancel the batch.
	cancelChan := make(chan struct{})
	errChan := wm.Query(queries, Cancel(cancelChan), ErrChan(make(chan error, 1)))

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

	c, workers := startWorkManager(t, numWorkers)
	workMgr := c.wm

	require.IsType(t, workMgr, &peerWorkManager{})
	wm := workMgr.(*peerWorkManager) //nolint:forcetypeassert

	// Set up the ranking to prioritize lower numbered workers.
	wm.cfg.Ranking.(*mockPeerRanking).less = func(i, j string) bool {
		return i < j
	}

	// Schedule a batch of queries.
	var queries []*Request
	for i := 0; i < numQueries; i++ {
		q := &Request{
			Req: &mockQueryEncoded{},
		}
		queries = append(queries, q)
	}

	// Send the batch, and Retrieve all jobs immediately.
	errChan := wm.Query(queries, ErrChan(make(chan error, 1)))

	// The 4 first workers should get the job.
	var jobs []*queryJob
	for i := 0; i < numQueries; i++ {
		select {
		case job := <-workers[i].nextJob:
			if job.index != float64(i) {
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
		q := &Request{
			Req: &mockQueryEncoded{},
		}
		queries = append(queries, q)
	}
	_ = wm.Query(queries, ErrChan(make(chan error, 1)))

	// The new jobs should be scheduled on the even numbered workers.
	for i := 0; i < len(workers); i += 2 {
		select {
		case <-workers[i].nextJob:
		case <-time.After(time.Second):
			t.Fatalf("job not scheduled")
		}
	}
}

// TestWorkManagerSchedulePriorityIndex tests that the workmanager acknowledges
// priority index.
func TestWorkManagerSchedulePriorityIndex(t *testing.T) {
	const numQueries = 3

	// Start work manager with as many workers as queries. This is not very
	// realistic, but makes the work manager able to schedule all queries
	// concurrently.
	c, workers := startWorkManager(t, numQueries)
	wm := c.wm

	// When the jobs gets scheduled, keep track of which worker was
	// assigned the job.
	type sched struct {
		wk  *mockWorker
		job *queryJob
	}

	// Schedule a batch of queries.
	var scheduledJobs [5]chan sched
	var queries [numQueries]*Request
	for i := 0; i < numQueries; i++ {
		var q *Request
		idx := i
		if i == 0 {
			q = &Request{
				Req: &mockQueryEncoded{},
			}
		} else {
			// Assign priority index.
			idx = i + 2
			q = &Request{
				Req: &mockQueryEncoded{
					index: float64(idx),
				},
			}
		}
		queries[i] = q
		scheduledJobs[idx] = make(chan sched)
	}

	// Fot each worker, spin up a goroutine that will forward the job it
	// got to our slice of scheduled jobs, such that we can handle them in
	// order.
	for i := 0; i < len(workers); i++ {
		wk := workers[i]
		go func() {
			for {
				job := <-wk.nextJob
				scheduledJobs[int(job.index)] <- sched{
					wk:  wk,
					job: job,
				}
			}
		}()
	}

	// Send the batch, and Retrieve all jobs immediately.
	errChan := wm.Query(queries[:], ErrChan(make(chan error, 1)))

	var jobs [numQueries]sched
	for i := uint64(0); i < numQueries; i++ {
		var expectedIndex float64

		if i == 0 {
			expectedIndex = float64(0)
		} else {
			expectedIndex = float64(i + 2)
		}
		var s sched
		select {
		case s = <-scheduledJobs[int(expectedIndex)]:

			if s.job.index != expectedIndex {
				t.Fatalf("wrong index: Got %v but expected %v", s.job.index,
					expectedIndex)
			}
		case <-errChan:
			t.Fatalf("did not expect an errChan")
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}

		jobs[i] = s
	}

	// Go backwards send results for job.
	for i := numQueries - 1; i >= 0; i-- {
		select {
		case jobs[i].wk.results <- &jobResult{
			job: jobs[i].job,
		}:
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}

	// Finally, make sure no jobs are retried.
	for i := uint64(0); i < numQueries; i++ {
		select {
		case <-scheduledJobs[i]:
			t.Fatalf("did not expect a retried job")
		case <-time.After(time.Second):
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

// TestPeerWorkManager_Stop tests the workmanager shutdown.
func TestPeerWorkManager_Stop(t *testing.T) {
	const numQueries = 5

	c, _ := startWorkManager(t, 0)
	wm := c.wm

	createRequest := func(numQuery int) []*Request {
		var queries []*Request
		for i := 0; i < numQuery; i++ {
			q := &Request{
				Req: &mockQueryEncoded{},
			}
			queries = append(queries, q)
		}

		return queries
	}

	// Send the batch, and Retrieve all jobs immediately.
	errChan := wm.Query(createRequest(numQueries), ErrChan(make(chan error, 1)))
	errChan2 := wm.Query(createRequest(numQueries))

	if errChan2 != nil {
		t.Fatalf("expected Query call without ErrChan option func to return" +
			"niil errChan")
	}

	errChan3 := make(chan error, 1)
	go func() {
		err := wm.Stop()

		errChan3 <- err
	}()

	select {
	case <-errChan:
	case <-time.After(time.Second):
		t.Fatalf("expected error workmanager shutting down")
	}

	select {
	case err := <-errChan3:
		if err != nil {
			t.Fatalf("unexpected error while stopping workmanager: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("workmanager stop functunction should return error")
	}
}

// TestWorkManagerErrResponseExistForQuery tests a scenario in which a workmanager handles
// an ErrIgnoreRequest.
func TestWorkManagerErrResponseExistForQuery(t *testing.T) {
	const numQueries = 5

	// Start work manager with as many workers as queries. This is not very
	// realistic, but makes the work manager able to schedule all queries
	// concurrently.
	c, workers := startWorkManager(t, numQueries)
	wm := c.wm

	// When the jobs gets scheduled, keep track of which worker was
	// assigned the job.
	type sched struct {
		wk  *mockWorker
		job *queryJob
	}

	// Schedule a batch of queries.
	var (
		queries       [numQueries]*Request
		scheduledJobs [numQueries]chan sched
	)
	for i := 0; i < numQueries; i++ {
		q := &Request{
			Req: &mockQueryEncoded{},
		}
		queries[i] = q
		scheduledJobs[i] = make(chan sched)
	}

	// Fot each worker, spin up a goroutine that will forward the job it
	// got to our slice of scheduled jobs, such that we can handle them in
	// order.
	for i := 0; i < len(workers); i++ {
		wk := workers[i]
		go func() {
			for {
				job := <-wk.nextJob
				scheduledJobs[int(job.index)] <- sched{
					wk:  wk,
					job: job,
				}
			}
		}()
	}

	// Send the batch, and Retrieve all jobs immediately.
	errChan := wm.Query(queries[:], ErrChan(make(chan error, 1)))
	var jobs [numQueries]sched
	for i := 0; i < numQueries; i++ {
		var s sched
		select {
		case s = <-scheduledJobs[i]:
			if s.job.index != float64(i) {
				t.Fatalf("wrong index")
			}

		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}

		jobs[int(s.job.index)] = s
	}

	// Go backwards, and make half of it return with an ErrIgnoreRequest.
	for i := numQueries - 1; i >= 0; i-- {
		select {
		case jobs[i].wk.results <- &jobResult{
			job: jobs[i].job,
			err: ErrIgnoreRequest,
		}:
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}

	// Finally, make sure the failed jobs are not retried.
	for i := 0; i < numQueries; i++ {
		var s sched
		select {
		case s = <-scheduledJobs[i]:
			t.Fatalf("did not expect any retried job but job"+
				"%v\n retried", s.job.index)
		case <-errChan:
			t.Fatalf("did not expect an errChan")
		case <-time.After(time.Second):
		}
	}
}

// TestWorkManagerResultUnfinished tests the workmanager handling a result with an unfinished boolean set
// to true.
func TestWorkManagerResultUnfinished(t *testing.T) {
	const numQueries = 10

	// Start work manager with as many workers as queries. This is not very
	// realistic, but makes the work manager able to schedule all queries
	// concurrently.
	c, workers := startWorkManager(t, numQueries)
	wm := c.wm

	// When the jobs gets scheduled, keep track of which worker was
	// assigned the job.
	type sched struct {
		wk  *mockWorker
		job *queryJob
	}

	// Schedule a batch of queries.
	var (
		queries       [numQueries]*Request
		scheduledJobs [numQueries]chan sched
	)
	for i := 0; i < numQueries; i++ {
		q := &Request{
			Req: &mockQueryEncoded{},
		}
		queries[i] = q
		scheduledJobs[i] = make(chan sched)
	}

	// Fot each worker, spin up a goroutine that will forward the job it
	// got to our slice of scheduled jobs, such that we can handle them in
	// order.
	for i := 0; i < len(workers); i++ {
		wk := workers[i]
		go func() {
			for {
				job := <-wk.nextJob
				scheduledJobs[int(job.index)] <- sched{
					wk:  wk,
					job: job,
				}
			}
		}()
	}

	// Send the batch, and Retrieve all jobs immediately.
	errChan := wm.Query(queries[:], ErrChan(make(chan error, 1)))
	var jobs [numQueries]sched
	for i := 0; i < numQueries; i++ {
		var s sched
		select {
		case s = <-scheduledJobs[i]:
			if s.job.index != float64(i) {
				t.Fatalf("wrong index")
			}

		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}

		jobs[int(s.job.index)] = s
	}

	// Go backwards, and make half of it unfinished.
	for i := numQueries - 1; i >= 0; i-- {
		var (
			unfinished bool
		)
		if i%2 == 0 {
			unfinished = true
		}

		select {
		case jobs[i].wk.results <- &jobResult{
			job:        jobs[i].job,
			unfinished: unfinished,
		}:
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}

	// Finally, make sure the failed jobs are being retried, in the same
	// order as they were originally scheduled.
	for i := 0; i < numQueries; i += 2 {
		var s sched
		select {
		case s = <-scheduledJobs[i]:

			// The new tindex the job should have.
			idx := float64(i) + 0.0005
			if idx != s.job.index {
				t.Fatalf("expected index %v for job"+
					"but got, %v\n", idx, s.job.index)
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

// TestIsWorkerEligibleForBlkHdrFetch tests the IsWorkerEligibleForBlkHdrFetch function.
func TestIsWorkerEligibleForBlkHdrFetch(t *testing.T) {
	type testArgs struct {
		name                string
		activeWorker        *activeWorker
		job                 *queryJob
		expectedEligibility bool
	}

	testCases := []testArgs{
		{
			name: "peer sync candidate, best height behind job start Height",
			activeWorker: &activeWorker{
				w: &mockWorker{
					peer: &mockPeer{
						bestHeight: 5,
						fullNode:   true,
					},
				},
			},
			job: &queryJob{
				Request: &Request{
					Req: &mockQueryEncoded{
						startHeight: 10,
					},
				},
			},
			expectedEligibility: false,
		},

		{
			name: "peer sync candidate, best height ahead job start Height",
			activeWorker: &activeWorker{
				w: &mockWorker{
					peer: &mockPeer{
						bestHeight: 10,
						fullNode:   true,
					},
				},
			},
			job: &queryJob{
				Request: &Request{
					Req: &mockQueryEncoded{
						startHeight: 5,
					},
				},
			},
			expectedEligibility: true,
		},

		{
			name: "peer not sync candidate, best height behind job start Height",
			activeWorker: &activeWorker{
				w: &mockWorker{
					peer: &mockPeer{
						bestHeight: 5,
						fullNode:   false,
					},
				},
			},
			job: &queryJob{
				Request: &Request{
					Req: &mockQueryEncoded{
						startHeight: 10,
					},
				},
			},
			expectedEligibility: false,
		},

		{
			name: "peer not sync candidate, best height ahead job start Height",
			activeWorker: &activeWorker{
				w: &mockWorker{
					peer: &mockPeer{
						bestHeight: 10,
						fullNode:   false,
					},
				},
			},
			job: &queryJob{
				Request: &Request{
					Req: &mockQueryEncoded{
						startHeight: 5,
					},
				},
			},
			expectedEligibility: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			isEligible := IsWorkerEligibleForBlkHdrFetch(test.activeWorker, test.job)
			if isEligible != test.expectedEligibility {
				t.Fatalf("Expected '%v'for eligibility check but got"+
					"'%v'\n", test.expectedEligibility, isEligible)
			}
		})
	}
}
