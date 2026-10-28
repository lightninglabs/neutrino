package workmanager

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/querysync"
	"github.com/stretchr/testify/require"
)

const mockPeerZero = "mock0"

type mockPeer struct {
	addr           string
	lastPingMicros int64
	quit           chan struct{}
}

var _ Peer = (*mockPeer)(nil)

func (m *mockPeer) QueueMessageWithEncoding(wire.Message,
	chan<- struct{}, wire.MessageEncoding) {
}

func (m *mockPeer) SubscribeRecvMsg() (<-chan wire.Message, func()) {
	return make(chan wire.Message), func() {}
}

func (m *mockPeer) Addr() string {
	return m.addr
}

func (m *mockPeer) LastPingMicros() int64 {
	return m.lastPingMicros
}

func (m *mockPeer) OnDisconnect() <-chan struct{} {
	return m.quit
}

type mockWorker struct {
	peer    Peer
	nextJob chan *queryJob
	results chan *jobResult

	// exit, if closed, makes Run return as if the peer had disconnected.
	exit chan struct{}
}

var _ Worker = (*mockWorker)(nil)

func (m *mockWorker) NewJob() chan<- *queryJob {
	return m.nextJob
}

func (m *mockWorker) Run(results chan<- *jobResult,
	quit <-chan struct{}) {

	for {
		select {
		case r := <-m.results:
			r.peer = m.peer
			results <- r

		case <-m.exit:
			return

		case <-quit:
			return
		}
	}
}

type mockPeerRanking struct {
	mu   sync.RWMutex
	less func(i, j string) bool
}

var _ PeerRanking = (*mockPeerRanking)(nil)

func (p *mockPeerRanking) AddPeer(string) {
}

func (p *mockPeerRanking) Order(peers []string) {
	p.mu.RLock()
	less := p.less
	p.mu.RUnlock()

	if less == nil {
		sort.Strings(peers)
		return
	}

	sort.Slice(peers, func(i, j int) bool {
		return less(peers[i], peers[j])
	})
}

func (p *mockPeerRanking) Punish(string) {
}

func (p *mockPeerRanking) Reward(string) {
}

func (p *mockPeerRanking) ResetRanking(string) {
}

func (p *mockPeerRanking) setLess(less func(i, j string) bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.less = less
}

func startWorkManager(
	t *testing.T,
	numWorkers int,
	trace *querysync.TraceRecorder,
	events querysync.SchedulerEventSink,
) (WorkManager, []*mockWorker) {

	t.Helper()

	workerChan := make(chan *mockWorker)
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
			m := &mockWorker{
				peer:    peer,
				nextJob: make(chan *queryJob, 1),
				results: make(chan *jobResult),
				exit:    make(chan struct{}),
			}
			workerChan <- m
			return m
		},
		Ranking:         ranking,
		SchedulerTrace:  trace,
		SchedulerEvents: events,
	})

	require.NoError(t, wm.Start())

	workers := make([]*mockWorker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		peer := &mockPeer{
			addr: fmt.Sprintf("mock%v", i),
			quit: make(chan struct{}),
		}

		select {
		case peerChan <- peer:
		case <-time.After(time.Second):
			t.Fatal("work manager did not receive peer")
		}

		select {
		case workers[i] = <-workerChan:
		case <-time.After(time.Second):
			t.Fatal("worker not started")
		}
	}

	return wm, workers
}

func recvJob(t *testing.T, worker *mockWorker, errChan <-chan error) *queryJob {
	t.Helper()

	select {
	case job := <-worker.nextJob:
		return job

	case err := <-errChan:
		t.Fatalf("did not expect query result: %v", err)

	case <-time.After(time.Second):
		t.Fatal("job not scheduled")
	}

	return nil
}

func recvJobOnlyFrom(t *testing.T, worker, forbidden *mockWorker,
	errChan <-chan error) *queryJob {

	t.Helper()

	select {
	case job := <-worker.nextJob:
		return job

	case job := <-forbidden.nextJob:
		t.Fatalf("unexpected job %v scheduled to peer %v", job.index,
			forbidden.peer.Addr())

	case err := <-errChan:
		t.Fatalf("did not expect query result: %v", err)

	case <-time.After(time.Second):
		t.Fatalf("job not scheduled to peer %v", worker.peer.Addr())
	}

	return nil
}

func requireNoJob(t *testing.T, worker *mockWorker) {
	t.Helper()

	select {
	case job := <-worker.nextJob:
		t.Fatalf("unexpected job %v scheduled to peer %v", job.index,
			worker.peer.Addr())

	case <-time.After(100 * time.Millisecond):
	}
}

func sendResult(t *testing.T, worker *mockWorker, job *queryJob, err error) {
	t.Helper()

	select {
	case worker.results <- &jobResult{
		job: job,
		err: err,
	}:
	case <-time.After(time.Second):
		t.Fatal("result not handled")
	}
}

func requireQueryDone(t *testing.T, errChan <-chan error) {
	t.Helper()

	select {
	case err := <-errChan:
		require.NoError(t, err)

	case <-time.After(time.Second):
		t.Fatal("query did not finish")
	}
}

func TestWorkManagerRecordsReplayableSchedulerTraceAndEvents(t *testing.T) {
	trace := &querysync.TraceRecorder{}
	events := &querysync.SchedulerEventRecorder{}
	wm, workers := startWorkManager(t, 2, trace, events)
	defer func() {
		require.NoError(t, wm.Stop())
	}()

	errChan := wm.Query([]*Request{{}})
	job := recvJob(t, workers[0], errChan)
	sendResult(t, workers[0], job, nil)
	requireQueryDone(t, errChan)

	replay, err := querysync.ReplayTrace(
		t.Context(), querysync.DefaultConfig(), trace.Events(),
	)
	require.NoError(t, err)
	require.NotEmpty(t, replay.Dispatches)
	require.Equal(t, querysync.BridgeDispatchWant{
		OK:            true,
		PeerID:        mockPeerZero,
		TaskID:        0,
		TimeoutMillis: int(minQueryTimeout / time.Millisecond),
	}, replay.Dispatches[0])

	var owned bool
	var sent, resultEvent bool
	for _, event := range events.Events() {
		if event.Type == querysync.SchedulerEventTaskOwned &&
			event.PeerID == mockPeerZero && event.OK {

			owned = true
		}
		if event.Type == querysync.SchedulerEventTaskSent &&
			event.PeerID == mockPeerZero && event.OK {

			require.EqualValues(t, 0, event.TaskID)
			require.Equal(t, minQueryTimeout, event.Timeout)
			sent = true
		}
		if event.Type == querysync.SchedulerEventTaskResult &&
			event.PeerID == mockPeerZero && event.OK {

			require.EqualValues(t, 0, event.TaskID)
			require.NotZero(t, event.ActiveDuration)
			resultEvent = true
		}
	}
	require.True(t, owned)
	require.True(t, sent)
	require.True(t, resultEvent)
}

func TestWorkManagerAvoidsAllFailedPeersForRetry(t *testing.T) {
	wm, workers := startWorkManager(t, 3, nil, nil)
	defer func() {
		require.NoError(t, wm.Stop())
	}()

	errChan := wm.Query(
		[]*Request{{}}, NumRetries(4),
	)
	job := recvJob(t, workers[0], errChan)

	sendResult(t, workers[0], job, ErrPeerNotFound)
	retry := recvJob(t, workers[1], errChan)
	require.Same(t, job, retry)

	sendResult(t, workers[1], retry, ErrQueryTimeout)
	retry = recvJob(t, workers[2], errChan)
	require.Same(t, job, retry)

	sendResult(t, workers[2], retry, nil)
	requireQueryDone(t, errChan)
}

func TestWorkManagerHardCoolsTimeoutPeerAndReassignsLocalWork(t *testing.T) {
	wm, workers := startWorkManager(t, 2, nil, nil)
	defer func() {
		require.NoError(t, wm.Stop())
	}()

	errChan := wm.Query([]*Request{
		{}, {}, {}, {}, {},
	})

	job0 := recvJob(t, workers[0], errChan)
	job1 := recvJob(t, workers[1], errChan)

	sendResult(t, workers[0], job0, ErrQueryTimeout)
	requireNoJob(t, workers[0])

	sendResult(t, workers[1], job1, nil)
	for i := 0; i < 4; i++ {
		job := recvJobOnlyFrom(t, workers[1], workers[0], errChan)
		sendResult(t, workers[1], job, nil)
	}

	requireQueryDone(t, errChan)
}

func TestSchedulerDoesNotRelaxHardCooldown(t *testing.T) {
	now := time.Now()
	ranking := &mockPeerRanking{}
	ranking.setLess(func(i, j string) bool {
		return i < j
	})

	d := newDispatcher(&peerWorkManager{
		cfg: &Config{Ranking: ranking},
	}, nil)
	d.workers = map[string]*activeWorker{
		"hard": {
			peer: &mockPeer{
				addr: "hard",
				quit: make(chan struct{}),
			},
			hardCooldown: now.Add(time.Minute),
		},
		"soft": {
			peer: &mockPeer{
				addr: "soft",
				quit: make(chan struct{}),
			},
			cooldown: now.Add(time.Minute),
		},
	}

	avoid := d.syncPeers(syncOpts{ignoreCooldown: true})
	require.Equal(t, []string{"hard"}, avoid)

	assignment, ok := d.sched.AssignLocalTask(querysync.Task{
		ID:         1,
		Timeout:    minQueryTimeout,
		AvoidPeers: avoid,
	})
	require.True(t, ok)
	require.Equal(t, "soft", assignment.PeerID)
}

func TestWorkManagerStealsOwnedWorkFromBusyPeer(t *testing.T) {
	events := &querysync.SchedulerEventRecorder{}
	wm, workers := startWorkManager(t, 2, nil, events)
	defer func() {
		require.NoError(t, wm.Stop())
	}()

	errChan := wm.Query([]*Request{
		{}, {}, {}, {}, {},
	})

	job0 := recvJob(t, workers[0], errChan)
	job1 := recvJob(t, workers[1], errChan)

	sendResult(t, workers[1], job1, nil)
	local := recvJob(t, workers[1], errChan)
	require.EqualValues(t, 3, local.index)

	sendResult(t, workers[1], local, nil)
	stolen := recvJob(t, workers[1], errChan)
	require.EqualValues(t, 2, stolen.index)

	sendResult(t, workers[0], job0, nil)
	remaining := recvJob(t, workers[0], errChan)
	require.EqualValues(t, 4, remaining.index)

	sendResult(t, workers[1], stolen, nil)
	sendResult(t, workers[0], remaining, nil)
	requireQueryDone(t, errChan)

	var steal *querysync.SchedulerEvent
	var applied *querysync.SchedulerEvent
	for _, event := range events.Events() {
		if event.Type == querysync.SchedulerEventWorkStolen &&
			event.OK {

			steal = &event
		}
		if event.Type == querysync.SchedulerEventWorkStealApplied &&
			event.OK {

			applied = &event
		}
	}
	require.NotNil(t, steal)
	require.Equal(t, "mock1", steal.ThiefID)
	require.Equal(t, mockPeerZero, steal.DonorID)
	require.EqualValues(t, 2, steal.TaskID)

	require.NotNil(t, applied)
	require.Equal(t, "mock1", applied.ThiefID)
	require.Equal(t, mockPeerZero, applied.DonorID)
	require.EqualValues(t, 2, applied.TaskID)
	require.Equal(t, []querysync.TaskID{2}, applied.TaskIDs)
	require.Equal(t, 1, applied.StolenTaskCount)
	require.Equal(t, 1, applied.ThiefLocalLen)
	require.Equal(t, 1, applied.DonorLocalLen)
	require.EqualValues(t, 1, applied.StolenCount)
}

// TestWorkManagerReplacesStaleWorkerOnReconnect checks that a peer connecting
// under the address of an existing worker takes its place, that queued work
// follows the new worker, and that a connection notification for a peer that
// has already disconnected never starts a worker.
func TestWorkManagerReplacesStaleWorkerOnReconnect(t *testing.T) {
	t.Parallel()

	var (
		workerChan = make(chan *mockWorker, 4)
		peerChan   = make(chan Peer)
	)
	wm := NewWorkManager(&Config{
		ConnectedPeers: func() (<-chan Peer, func(), error) {
			return peerChan, func() {}, nil
		},
		NewWorker: func(peer Peer) Worker {
			m := &mockWorker{
				peer:    peer,
				nextJob: make(chan *queryJob, 1),
				results: make(chan *jobResult),
				exit:    make(chan struct{}),
			}
			workerChan <- m

			return m
		},
	})
	require.NoError(t, wm.Start())
	defer func() {
		require.NoError(t, wm.Stop())
	}()

	connect := func(p *mockPeer) {
		t.Helper()

		select {
		case peerChan <- p:
		case <-time.After(time.Second):
			t.Fatal("work manager did not receive peer")
		}
	}

	// The first connection starts a worker, which gets the first query.
	first := &mockPeer{addr: mockPeerZero, quit: make(chan struct{})}
	connect(first)
	oldWorker := <-workerChan

	errChan := wm.Query([]*Request{{}})
	job := recvJob(t, oldWorker, errChan)

	// The peer reconnects while the old worker still holds the job. Its
	// goroutines exit with the old connection.
	close(first.quit)
	second := &mockPeer{addr: mockPeerZero, quit: make(chan struct{})}
	connect(second)

	var newWorker *mockWorker
	select {
	case newWorker = <-workerChan:
	case <-time.After(time.Second):
		t.Fatal("replacement worker not started")
	}
	require.NotSame(t, oldWorker, newWorker)

	// The old worker's late result must not wedge the manager, and the
	// next query has to be served by the replacement worker.
	sendResult(t, oldWorker, job, nil)
	requireQueryDone(t, errChan)

	errChan = wm.Query([]*Request{{}})
	job = recvJob(t, newWorker, errChan)
	sendResult(t, newWorker, job, nil)
	requireQueryDone(t, errChan)

	// A notification for a peer that is already gone is dropped.
	gone := &mockPeer{addr: "gone", quit: make(chan struct{})}
	close(gone.quit)
	connect(gone)
	select {
	case <-workerChan:
		t.Fatal("worker started for disconnected peer")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestWorkManagerReapsExitedWorkerEagerly checks that the jobs queued on a
// worker that exits are reassigned right away, without waiting for anything
// to be sent to the dead worker.
func TestWorkManagerReapsExitedWorkerEagerly(t *testing.T) {
	wm, workers := startWorkManager(t, 2, nil, nil)
	defer func() {
		require.NoError(t, wm.Stop())
	}()

	errChan := wm.Query([]*Request{{}, {}, {}})

	// Both workers get a job, and the last one waits in the local queue
	// of the first, which has no way of starting it while busy.
	_ = recvJob(t, workers[0], errChan)
	job1 := recvJob(t, workers[1], errChan)

	// The first peer disconnects. Its queued job must make its way to the
	// remaining worker once that is free.
	close(workers[0].exit)
	sendResult(t, workers[1], job1, nil)

	job2 := recvJob(t, workers[1], errChan)
	require.EqualValues(t, 2, job2.index)
}
