package workmanager

import (
	"sort"
	"time"

	"github.com/lightninglabs/neutrino/querysync"
)

const (
	// workerExitBufferSize is the capacity of the channel worker goroutines
	// use to tell the dispatcher they have exited. A small buffer lets a
	// burst of simultaneously disconnecting peers return without waiting on
	// the dispatcher.
	workerExitBufferSize = 10
)

// batchProgress tracks a batch of queries that still has jobs outstanding.
type batchProgress struct {
	// noRetryMax is true if jobs in the batch may be retried forever.
	noRetryMax bool

	// maxRetries is the number of attempts each job gets, unless
	// noRetryMax is set.
	maxRetries uint8

	// timer fires when the whole batch has run past its deadline.
	timer *time.Timer

	// rem is the number of jobs in the batch that have not yet succeeded.
	rem int

	// errChan receives the batch's final result.
	errChan chan error

	// cancelChan is closed to abort every job in the batch.
	cancelChan chan struct{}

	// progressTimer fires if the batch goes progressTimeout without a job
	// completing.
	progressTimer *time.Timer

	// progressTimeout is the idle period armed on progressTimer. Zero
	// disables idle detection.
	progressTimeout time.Duration

	// progressGen is the generation of progressTimer. It lets the
	// dispatcher reject wakes that raced a successful completion.
	progressGen uint64
}

// resultInfo bundles a job result with the context every result handler needs,
// so the handlers do not recompute it.
type resultInfo struct {
	// result is the result delivered by the worker.
	result *jobResult

	// at is the time the dispatcher received the result.
	at time.Time

	// activeDuration is how long the job was active on the peer.
	activeDuration time.Duration

	// taskAge is how long ago the job was first created.
	taskAge time.Duration

	// batchNum is the batch the job belongs to.
	batchNum uint64

	// batch is the batch's progress tracker.
	batch *batchProgress

	// worker is the peer's current worker, or nil if the peer is gone or
	// has since been replaced by a reconnected worker.
	worker *activeWorker
}

// peerAddr returns the address of the peer that produced the result.
func (r *resultInfo) peerAddr() string {
	return r.result.peer.Addr()
}

// syncOpts controls how worker state is translated into the peer states the
// scheduler sees.
type syncOpts struct {
	// blocked lists additional peers that must be reported as blocked.
	blocked map[string]struct{}

	// ignoreCooldown reports peers that are only in soft cooldown as
	// ready.
	ignoreCooldown bool

	// ready, if non-empty, names a peer that is reported ready no matter
	// what, because it is the one asking for work.
	ready string
}

// dispatcher is the state owned by the work dispatcher goroutine. Every field
// is only touched from that goroutine, so none of it needs locking: workers
// and the scheduler are the single source of truth for peer state, and the
// scheduler is brought up to date from the workers right before each decision.
type dispatcher struct {
	// w is the work manager that owns the dispatcher.
	w *peerWorkManager

	// cfg is the work manager's configuration.
	cfg *Config

	// peersConnected delivers peers as they connect.
	peersConnected <-chan Peer

	// sched decides which peer owns a job, and which peer steals what.
	sched *querysync.Scheduler

	// work is the global queue of jobs not yet owned by a peer.
	work workQueue[*queryJob]

	// batches tracks batches that still have jobs outstanding.
	batches map[uint64]*batchProgress

	// queries maps each tracked job to its batch.
	queries map[uint64]uint64

	// nextBatch is the index handed to the next batch.
	nextBatch uint64

	// nextQuery is the index, and thus priority, of the next job.
	nextQuery uint64

	// workers holds the running worker of every connected peer.
	workers map[string]*activeWorker

	// nonResponseFailures counts consecutive non-responses per peer.
	nonResponseFailures map[string]uint8

	// quarantinedPeers remembers quarantines across reconnects.
	quarantinedPeers map[string]time.Time

	// sendBlocked holds workers that declined a non-blocking job send.
	sendBlocked map[string]struct{}

	// scheduleRetry fires when sendBlocked should be retried. It is nil
	// when no retry is pending.
	scheduleRetry <-chan time.Time

	// batchTimeouts delivers batches whose deadline passed.
	batchTimeouts chan uint64

	// progressWakes delivers batches that went idle.
	progressWakes chan progressWake

	// workerExits delivers workers whose Run method returned.
	workerExits chan *activeWorker
}

// newDispatcher creates a dispatcher for the work manager.
func newDispatcher(w *peerWorkManager,
	peersConnected <-chan Peer) *dispatcher {

	sched := querysync.NewScheduler(
		querysync.Config{
			BaseTimeout:   minQueryTimeout,
			MinTimeout:    minQueryTimeout,
			MaxTimeout:    maxQueryTimeout,
			RTTMultiplier: queryTimeoutRTTMultiplier,
		}, w.cfg.SchedulerTrace, w.cfg.SchedulerEvents,
	)

	return &dispatcher{
		w:                   w,
		cfg:                 w.cfg,
		peersConnected:      peersConnected,
		sched:               sched,
		batches:             make(map[uint64]*batchProgress),
		queries:             make(map[uint64]uint64),
		workers:             make(map[string]*activeWorker),
		nonResponseFailures: make(map[string]uint8),
		quarantinedPeers:    make(map[string]time.Time),
		sendBlocked:         make(map[string]struct{}),
		batchTimeouts:       make(chan uint64),
		progressWakes:       make(chan progressWake),
		workerExits: make(
			chan *activeWorker, workerExitBufferSize,
		),
	}
}

// run is the dispatcher's event loop. Each iteration first hands out as much
// work as it can, and only then blocks until something new happens.
func (d *dispatcher) run() {
	for {
		// Start jobs that peers already own, then move jobs from the
		// global queue into peer-local queues. Either one making
		// progress may unblock the other, so go around again.
		if d.dispatchOwnedWork() {
			continue
		}
		if d.assignQueuedWork() {
			continue
		}

		// There is nothing left to hand out, so we wait for a peer to
		// connect or exit, a result to come back, a new batch to be
		// scheduled, or one of our timers to fire.
		select {
		case peer := <-d.peersConnected:
			d.handleNewPeer(peer)

		case exited := <-d.workerExits:
			d.handleWorkerExit(exited)

		case result := <-d.w.jobResults:
			d.handleResult(result)

		case batch := <-d.w.newBatches:
			d.handleNewBatch(batch)

		case wake := <-d.progressWakes:
			d.handleProgressWake(wake)

		case batchNum := <-d.batchTimeouts:
			d.handleBatchTimeout(batchNum)

		case <-d.scheduleRetry:
			d.handleRetryTick()

		case <-d.w.quit:
			return
		}
	}
}

// shutdown fails every batch that is still outstanding. It must be called once
// the event loop has returned.
func (d *dispatcher) shutdown() {
	for _, b := range d.batches {
		stopBatchTimers(b)
		b.errChan <- ErrWorkManagerShuttingDown
	}
}

// handleNewPeer starts a worker for a newly connected peer.
func (d *dispatcher) handleNewPeer(peer Peer) {
	// Connection notifications are delivered asynchronously, so one can
	// arrive after its peer has already gone away. Don't start a worker
	// for it.
	select {
	case <-peer.OnDisconnect():
		log.Debugf("Ignoring already disconnected peer %v", peer.Addr())
		return
	default:
	}

	// A peer reconnecting under the same address replaces its stale
	// worker. Remove that one first, so its queued jobs go back to the
	// common queue instead of being orphaned.
	if stale, ok := d.workers[peer.Addr()]; ok {
		log.Debugf("Replacing stale worker for peer %v", peer.Addr())
		d.removeWorker(stale)
	}

	log.Debugf("Starting worker for peer %v", peer.Addr())

	worker := &activeWorker{
		w:          d.cfg.NewWorker(peer),
		peer:       peer,
		quarantine: d.quarantinedPeers[peer.Addr()],
	}
	d.workers[peer.Addr()] = worker
	d.cfg.Ranking.AddPeer(peer.Addr())

	// The worker tells the dispatcher when it exits, so it can be removed
	// right away rather than when a job send happens to notice.
	d.w.wg.Add(1)
	go func() {
		defer d.w.wg.Done()

		worker.w.Run(d.w.jobResults, d.w.quit)

		select {
		case d.workerExits <- worker:
		case <-d.w.quit:
		}
	}()
}

// handleWorkerExit removes a worker whose Run method returned.
func (d *dispatcher) handleWorkerExit(worker *activeWorker) {
	// The exit of a worker that was already replaced by a reconnect must
	// not take down the worker that replaced it.
	if d.workers[worker.peer.Addr()] != worker {
		return
	}

	log.Debugf("Worker for peer %v exited", worker.peer.Addr())
	d.removeWorker(worker)
}

// removeWorker forgets a worker, and returns the jobs it had not started to the
// global queue.
func (d *dispatcher) removeWorker(worker *activeWorker) {
	peerID := worker.peer.Addr()

	d.releaseLocalWork(worker)
	delete(d.workers, peerID)
	delete(d.sendBlocked, peerID)
	d.sched.RemovePeer(peerID)
}

// releaseLocalWork returns the jobs a worker has been given but not started to
// the global queue, where any healthy peer can pick them up.
func (d *dispatcher) releaseLocalWork(worker *activeWorker) {
	for _, job := range worker.localWork {
		d.work.Push(job)
	}
	worker.localWork = nil
}

// handleNewBatch adds all queries of a new batch to the work queue, with
// priority given by the order they were scheduled.
func (d *dispatcher) handleNewBatch(b *batch) {
	batchNum := d.nextBatch
	d.nextBatch++

	log.Debugf("Adding new batch(%d) of %d queries to work queue",
		batchNum, len(b.requests))

	// Internal cancel channel of the batch.
	cancelChan := make(chan struct{})
	createdAt := time.Now()

	for _, q := range b.requests {
		d.work.Push(&queryJob{
			index:              d.nextQuery,
			timeout:            minQueryTimeout,
			encoding:           b.options.Encoding,
			cancelChan:         b.options.CancelChan,
			internalCancelChan: cancelChan,
			createdAt:          createdAt,
			Request:            q,
		})
		d.queries[d.nextQuery] = batchNum
		d.nextQuery++
	}

	timer := time.AfterFunc(b.options.Timeout, func() {
		select {
		case d.batchTimeouts <- batchNum:
		case <-d.w.quit:
		}
	})

	progress := &batchProgress{
		noRetryMax:      b.options.NoRetryMax,
		maxRetries:      b.options.NumRetries,
		timer:           timer,
		rem:             len(b.requests),
		errChan:         b.errChan,
		cancelChan:      cancelChan,
		progressTimeout: b.options.ProgressTimeout,
	}
	d.batches[batchNum] = progress
	d.armProgressTimer(progress, batchNum)
}

// handleProgressWake fails a batch that went idle for too long.
func (d *dispatcher) handleProgressWake(wake progressWake) {
	// Timer generations reject wakes that raced a successful completion.
	batch, ok := d.batches[wake.batchNum]
	if !ok || batch.progressGen != wake.gen {
		return
	}

	d.cancelBatch(wake.batchNum, ErrQueryTimeout)
	log.Warnf("Batch %v idle timeout after %v without progress",
		wake.batchNum, batch.progressTimeout)
}

// handleBatchTimeout fails a batch that ran past its deadline.
func (d *dispatcher) handleBatchTimeout(batchNum uint64) {
	if !d.cancelBatch(batchNum, ErrQueryTimeout) {
		return
	}

	log.Warnf("Batch %v timed out", batchNum)
}

// handleRetryTick gives workers that declined a job send another chance.
func (d *dispatcher) handleRetryTick() {
	d.scheduleRetry = nil
	d.sendBlocked = make(map[string]struct{})
}

// armScheduleRetry makes sure the dispatcher wakes up again shortly, to retry
// workers that are not ready for work yet.
func (d *dispatcher) armScheduleRetry() {
	if d.scheduleRetry == nil {
		d.scheduleRetry = time.After(scheduleRetryInterval)
	}
}

// armProgressTimer (re)starts the idle timer of a batch. Timer generations
// reject callbacks that raced a successful completion.
func (d *dispatcher) armProgressTimer(b *batchProgress, batchNum uint64) {
	if b.progressTimeout == 0 {
		return
	}
	if b.progressTimer != nil {
		b.progressTimer.Stop()
	}

	b.progressGen++
	gen := b.progressGen
	b.progressTimer = time.AfterFunc(b.progressTimeout, func() {
		select {
		case d.progressWakes <- progressWake{
			batchNum: batchNum,
			gen:      gen,
		}:

		case <-d.w.quit:
		}
	})
}

// stopBatchTimers stops both timers of a batch.
func stopBatchTimers(b *batchProgress) {
	if b.timer != nil {
		b.timer.Stop()
	}
	if b.progressTimer != nil {
		b.progressTimer.Stop()
	}
}

// completeBatch delivers the batch's final result and stops tracking it.
func (d *dispatcher) completeBatch(batchNum uint64, batch *batchProgress,
	err error) {

	stopBatchTimers(batch)
	batch.errChan <- err
	delete(d.batches, batchNum)
}

// cancelBatch aborts a batch with the given error. Its internal cancel channel
// is closed, so any of its jobs that are queued or still unstarted on a peer
// are dropped as soon as they are looked at. Jobs already on the wire finish
// on their own and their results are discarded. It reports whether the batch
// was still being tracked.
//
// This is the single place batches are aborted, whether from a timeout, an
// exhausted retry budget, a caller cancel, or an explicit cancel request.
func (d *dispatcher) cancelBatch(batchNum uint64, err error) bool {
	batch, ok := d.batches[batchNum]
	if !ok {
		return false
	}

	d.completeBatch(batchNum, batch, err)
	if batch.cancelChan != nil {
		close(batch.cancelChan)
	}

	return true
}

// dropCanceledJob stops tracking a job found to be canceled, and aborts the
// batch it belongs to.
func (d *dispatcher) dropCanceledJob(job *queryJob) {
	batchNum, ok := d.queries[job.index]
	delete(d.queries, job.index)
	if !ok {
		return
	}

	if d.cancelBatch(batchNum, ErrJobCanceled) {
		log.Debugf("Canceled batch %v", batchNum)
	}
}

// snapshot returns the scheduler's view of a worker in the given state.
func (d *dispatcher) snapshot(worker *activeWorker, rank int,
	state querysync.PeerState) querysync.PeerSnapshot {

	snapshot := peerSnapshot(worker.peer, rank, state)

	// The local work must be non-nil so that the scheduler replaces, and
	// does not keep, whatever it knew before.
	snapshot.LocalWork = make([]querysync.TaskID, 0, len(worker.localWork))
	for _, job := range worker.localWork {
		snapshot.LocalWork = append(
			snapshot.LocalWork, querysync.TaskID(job.index),
		)
	}

	return snapshot
}

// workerState returns the scheduler state of a worker at the given time. The
// second return value is true if the scheduler should avoid giving the peer
// new work.
func (d *dispatcher) workerState(peerID string, worker *activeWorker,
	now time.Time, opts syncOpts) (querysync.PeerState, bool) {

	switch {
	case peerID == opts.ready:
		return querysync.PeerReady, false

	case worker.activeJob != nil:
		return querysync.PeerBusy, false

	case now.Before(worker.quarantine):
		return querysync.PeerQuarantined, true

	case now.Before(worker.hardCooldown):
		return querysync.PeerBlocked, true

	case !opts.ignoreCooldown && now.Before(worker.cooldown):
		return querysync.PeerBlocked, true
	}

	if _, ok := opts.blocked[peerID]; ok {
		return querysync.PeerBlocked, true
	}

	return querysync.PeerReady, false
}

// syncPeers brings the scheduler up to date with the workers, which are the
// single source of truth for peer state. It returns the sorted peers a new job
// must avoid.
func (d *dispatcher) syncPeers(opts syncOpts) []string {
	ranks := d.workerRanks()
	now := time.Now()

	var avoid []string
	for peerID, worker := range d.workers {
		state, avoidPeer := d.workerState(peerID, worker, now, opts)
		if avoidPeer {
			avoid = append(avoid, peerID)
		}

		err := d.sched.AddPeer(d.snapshot(worker, ranks[peerID], state))
		if err != nil {
			log.Warnf("Unable to update peer %v in query "+
				"scheduler: %v", peerID, err)
		}
	}
	sort.Strings(avoid)

	return avoid
}

// rankedWorkerIDs returns the connected peers, best first.
func (d *dispatcher) rankedWorkerIDs() []string {
	ids := make([]string, 0, len(d.workers))
	for id := range d.workers {
		ids = append(ids, id)
	}
	d.cfg.Ranking.Order(ids)

	return ids
}

// workerRanks returns the scheduler rank of every connected peer. A lower rank
// is better.
func (d *dispatcher) workerRanks() map[string]int {
	peers := d.rankedWorkerIDs()
	ranks := make(map[string]int, len(peers))

	if scorer, ok := d.cfg.Ranking.(peerScorer); ok {
		for _, peer := range peers {
			ranks[peer] = int(scorer.Score(peer))
		}

		return ranks
	}

	for rank, peer := range peers {
		ranks[peer] = rank
	}

	return ranks
}

// emit records a structured scheduler event, if a sink is configured.
func (d *dispatcher) emit(event querysync.SchedulerEvent) {
	recordSchedulerEvent(d.cfg.SchedulerEvents, event)
}
