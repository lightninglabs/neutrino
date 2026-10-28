package workmanager

import (
	"sync"
	"time"

	"github.com/lightninglabs/neutrino/querysync"
)

const (
	// minQueryTimeout is the timeout a query will be initially given. If
	// the peer given the query fails to respond within the timeout, it
	// will be given to the next peer with an increased timeout.
	minQueryTimeout = 2 * time.Second

	// maxQueryTimeout is the maximum timeout given to a single query.
	maxQueryTimeout = 32 * time.Second

	// queryTimeoutRTTMultiplier is the multiplier applied to a peer's
	// latest ping RTT sample when deriving a per-peer query timeout floor.
	queryTimeoutRTTMultiplier = 8

	// scheduleRetryInterval is the delay used when work is available but
	// all nominally free workers decline a non-blocking job send.
	scheduleRetryInterval = 10 * time.Millisecond

	// failedPeerCooldown is the period after a failed query attempt during
	// which the scheduler will prefer other ready peers for new work. If
	// all peers are only soft-cooling down, the scheduler falls back to
	// them rather than stalling progress. Non-responsive peers that still
	// appear connected are hard-cooled for this same period so their
	// unstarted local work can be reassigned instead of waiting behind a
	// peer that just failed to answer.
	failedPeerCooldown = 30 * time.Second

	// nonResponsivePeerFailures is the number of consecutive query
	// non-responses that escalates a peer from normal retry/cooldown into
	// a local quarantine and optional external ban.
	nonResponsivePeerFailures = 3

	// nonResponsivePeerCooldown is the local quarantine duration used when
	// the caller does not disconnect the peer through OnPeerUnresponsive.
	nonResponsivePeerCooldown = 10 * time.Minute
)

type batch struct {
	requests []*Request
	options  Options
	errChan  chan error
}

// progressWake identifies the batch and timer generation whose idle period
// ended.
type progressWake struct {
	batchNum uint64
	gen      uint64
}

// Worker is the interface that must be satisfied by workers managed by this
// package's WorkManager implementation.
type Worker interface {
	// Run starts the worker. The worker will supply its peer with queries,
	// and handle responses from it. Results for any query handled by this
	// worker will be delivered on the results channel. quit can be closed
	// to immediately make the worker exit.
	//
	// The method is blocking, and should be started in a goroutine. It
	// will run until the peer disconnects or the worker is told to quit.
	Run(results chan<- *jobResult, quit <-chan struct{})

	// NewJob returns a channel where work that is to be handled by the
	// worker can be sent. If the worker reads a queryJob from this
	// channel, it is guaranteed that a response will eventually be
	// delivered on the results channel (except when the quit channel has
	// been closed).
	NewJob() chan<- *queryJob
}

// PeerRanking is an interface that must be satisfied by the underlying module
// that is used to determine which peers to prioritize querios on.
type PeerRanking interface {
	// AddPeer adds a peer to the ranking.
	AddPeer(peer string)

	// Reward should be called when the peer has succeeded in a query,
	// increasing the likelihood that it will be picked for subsequent
	// queries.
	Reward(peer string)

	// Punish should be called when the peer has failed in a query,
	// decreasing the likelihood that it will be picked for subsequent
	// queries.
	Punish(peer string)

	// Order sorts the slice of peers according to their ranking.
	Order(peers []string)

	// ResetRanking sets the score of the passed peer to the defaultScore.
	ResetRanking(peerAddr string)
}

type peerScorer interface {
	// Score returns the current rank score for a peer. A lower score is
	// better.
	Score(peer string) uint64
}

// activeWorker wraps a Worker that is currently running, together with the job
// we have given to it.
// TODO(halseth): support more than one active job at a time.
type activeWorker struct {
	w            Worker
	peer         Peer
	activeJob    *queryJob
	localWork    []*queryJob
	cooldown     time.Time
	hardCooldown time.Time
	quarantine   time.Time
}

// Config holds the configuration options for a new sync work manager.
type Config struct {
	// ConnectedPeers is a function that returns a channel where all
	// connected peers will be sent. It is assumed that all current peers
	// will be sent imemdiately, and new peers as they connect.
	//
	// The returned function closure is called to cancel the subscription.
	ConnectedPeers func() (<-chan Peer, func(), error)

	// Peers returns a snapshot of the connected peers that are eligible to
	// receive work. It is used to pick the recipients of a Broadcast.
	Peers func() []Peer

	// NewWorker is function closure that should start a new worker. We
	// make this configurable to easily mock the worker used during tests.
	NewWorker func(Peer) Worker

	// OnMaxTries gives the caller access to the peer once a maximum number
	// of retries have been attempted. The caller can then access the
	// peer's address and can choose to punish the peer accordingly.
	OnMaxTries func(Peer)

	// OnPeerUnresponsive gives the caller access to a peer that repeatedly
	// failed to respond to assigned queries. Callers can use this to
	// extend the normal peer ban system; the work manager also quarantines
	// the peer locally so it is not selected while the callback
	// disconnects it.
	OnPeerUnresponsive func(Peer)

	// Ranking is used to rank the connected peers when determining who to
	// give work to.
	Ranking PeerRanking

	// SchedulerTrace, if non-nil, records the scheduler actor events used
	// for dispatch decisions. The resulting trace can be replayed with
	// querysync.ReplayTrace to compare production behavior with model
	// invariants.
	SchedulerTrace *querysync.TraceRecorder

	// SchedulerEvents, if non-nil, records structured scheduler events for
	// metrics, validation summaries, and live sync diagnostics.
	SchedulerEvents querysync.SchedulerEventSink
}

// peerWorkManager is the main access point for outside callers, and satisfies
// the QueryAccess API. It receives queries to pass to peers, and schedules
// them among available workers, orchestrating where to send them. It
// implements the WorkManager interface.
type peerWorkManager struct {
	cfg *Config

	// newBatches is a channel where new batches of queries will be sent to
	// the workDispatcher.
	newBatches chan *batch

	// jobResults is the common channel where results from queries from all
	// workers will be sent.
	jobResults chan *jobResult

	quit chan struct{}
	wg   sync.WaitGroup
}

// Compile time check to ensure peerWorkManager satisfies the WorkManager
// interface.
var _ WorkManager = (*peerWorkManager)(nil)

// NewWorkManager returns a new WorkManager with the regular worker
// implementation.
func NewWorkManager(cfg *Config) WorkManager {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.NewWorker == nil {
		cfg.NewWorker = NewWorker
	}
	if cfg.Ranking == nil {
		cfg.Ranking = NewPeerRanking()
	}

	return &peerWorkManager{
		cfg:        cfg,
		newBatches: make(chan *batch),
		jobResults: make(chan *jobResult),
		quit:       make(chan struct{}),
	}
}

// queryTimeoutForPeer returns the query timeout to use for the next attempt on
// this peer. The job's current timeout is preserved as a lower bound so retry
// backoff still works across peers.
func queryTimeoutForPeer(
	baseTimeout time.Duration,
	peer Peer,
) time.Duration {

	return querysync.QueryTimeoutForPeer(
		baseTimeout, peerSnapshot(peer, 0, querysync.PeerReady),
		querysync.Config{
			BaseTimeout:   baseTimeout,
			MinTimeout:    minQueryTimeout,
			MaxTimeout:    maxQueryTimeout,
			RTTMultiplier: queryTimeoutRTTMultiplier,
		},
	)
}

func peerSnapshot(
	peer Peer,
	rank int,
	state querysync.PeerState,
) querysync.PeerSnapshot {

	return querysync.PeerSnapshot{
		ID:    peer.Addr(),
		Rank:  rank,
		RTT:   time.Duration(peer.LastPingMicros()) * time.Microsecond,
		State: state,
	}
}

func isNonResponseErr(err error) bool {
	return err == ErrQueryTimeout || err == ErrPeerDisconnected
}

func isHardCooldownErr(err error) bool {
	return err == ErrQueryTimeout || err == ErrPeerNotFound
}

func recordSchedulerEvent(sink querysync.SchedulerEventSink,
	event querysync.SchedulerEvent) {

	if sink == nil {
		return
	}
	if event.At.IsZero() {
		event.At = time.Now()
	}

	sink.RecordSchedulerEvent(event)
}

func durationSince(now, then time.Time) time.Duration {
	if then.IsZero() {
		return 0
	}

	return now.Sub(then)
}

func errString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

func oldestLocalQueueAge(now time.Time, worker *activeWorker) time.Duration {
	var oldest time.Duration
	for _, job := range worker.localWork {
		age := durationSince(now, job.ownedAt)
		if age == 0 {
			continue
		}
		if oldest == 0 || age > oldest {
			oldest = age
		}
	}

	return oldest
}

func removeWorkerLocalWork(worker *activeWorker,
	taskID querysync.TaskID) (*queryJob, bool) {

	for i, job := range worker.localWork {
		if querysync.TaskID(job.index) != taskID {
			continue
		}

		worker.localWork = append(
			worker.localWork[:i], worker.localWork[i+1:]...,
		)

		return job, true
	}

	return nil, false
}

// Start starts the peerWorkManager.
//
// NOTE: this is part of the WorkManager interface.
func (w *peerWorkManager) Start() error {
	w.wg.Add(1)
	go w.workDispatcher()

	return nil
}

// Stop stops the peerWorkManager and all underlying goroutines.
//
// NOTE: this is part of the WorkManager interface.
func (w *peerWorkManager) Stop() error {
	close(w.quit)
	w.wg.Wait()

	return nil
}

// workDispatcher receives batches of queries to be performed from external
// callers, and dispatches these to active workers. It makes sure to prioritize
// the queries in the order they come in, such that early queries will be
// attempted completed first.
//
// NOTE: MUST be run as a goroutine.
func (w *peerWorkManager) workDispatcher() {
	defer w.wg.Done()

	// Get a peer subscription. We do it in this goroutine rather than
	// Start to avoid a deadlock when starting the WorkManager fetches the
	// peers from the server.
	peersConnected, cancel, err := w.cfg.ConnectedPeers()
	if err != nil {
		log.Errorf("Unable to get connected peers: %v", err)
		return
	}
	defer cancel()

	// When the dispatcher exits, the remaining batches get told so.
	d := newDispatcher(w, peersConnected)
	defer d.shutdown()

	d.run()
}

// Query distributes the slice of requests to the set of connected peers.
//
// NOTE: this is part of the WorkManager interface.
func (w *peerWorkManager) Query(requests []*Request,
	options ...QueryOption) chan error {

	qo := ResolveOptions(options...)

	errChan := make(chan error, 1)

	// Add query messages to the queue of batches to handle.
	select {
	case w.newBatches <- &batch{
		requests: requests,
		options:  qo,
		errChan:  errChan,
	}:
	case <-w.quit:
		errChan <- ErrWorkManagerShuttingDown
	}

	return errChan
}
