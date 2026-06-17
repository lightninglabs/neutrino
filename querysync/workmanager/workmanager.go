package workmanager

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/lightninglabs/neutrino/query"
	"github.com/lightninglabs/neutrino/querysync"
)

const (
	// minQueryTimeout is the timeout a query will be initially given. If
	// the peer given the query fails to respond within the timeout, it
	// will be given to the next peer with an increased timeout.
	minQueryTimeout = 2 * time.Second

	// maxQueryTimeout is the maximum timeout given to a single query.
	maxQueryTimeout = 32 * time.Second

	// queryTimeoutRTTMultiplier is the multiplier applied to a peer's latest
	// ping RTT sample when deriving a per-peer query timeout floor.
	queryTimeoutRTTMultiplier = 8

	// scheduleRetryInterval is the delay used when work is available but all
	// nominally free workers decline a non-blocking job send.
	scheduleRetryInterval = 10 * time.Millisecond

	// failedPeerCooldown is the period after a failed query attempt during
	// which the scheduler will prefer other ready peers for new work. If all
	// peers are only soft-cooling down, the scheduler falls back to them
	// rather than stalling progress. Non-responsive peers that still appear
	// connected are hard-cooled for this same period so their unstarted local
	// work can be reassigned instead of waiting behind a peer that just
	// failed to answer.
	failedPeerCooldown = 30 * time.Second

	// nonResponsivePeerFailures is the number of consecutive query
	// non-responses that escalates a peer from normal retry/cooldown into a
	// local quarantine and optional external ban.
	nonResponsivePeerFailures = 3

	// nonResponsivePeerCooldown is the local quarantine duration used when
	// the caller does not disconnect the peer through OnPeerUnresponsive.
	nonResponsivePeerCooldown = 10 * time.Minute
)

type batch struct {
	requests []*query.Request
	options  query.Options
	errChan  chan error
}

// Worker is the interface that must be satisfied by workers managed by this
// package's query.WorkManager implementation.
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
	peer         query.Peer
	actor        *querysync.PeerActor
	activeJob    *queryJob
	localWork    []*queryJob
	cooldown     time.Time
	hardCooldown time.Time
	quarantine   time.Time
	onExit       chan struct{}
}

// Config holds the configuration options for a new sync work manager.
type Config struct {
	// ConnectedPeers is a function that returns a channel where all
	// connected peers will be sent. It is assumed that all current peers
	// will be sent imemdiately, and new peers as they connect.
	//
	// The returned function closure is called to cancel the subscription.
	ConnectedPeers func() (<-chan query.Peer, func(), error)

	// NewWorker is function closure that should start a new worker. We
	// make this configurable to easily mock the worker used during tests.
	NewWorker func(query.Peer) Worker

	// OnMaxTries gives the caller access to the peer once a maximum number
	// of retries have been attempted. The caller can then access the peer's
	// address and can choose to punish the peer accordingly.
	OnMaxTries func(query.Peer)

	// OnPeerUnresponsive gives the caller access to a peer that repeatedly
	// failed to respond to assigned queries. Callers can use this to extend
	// the normal peer ban system; the work manager also quarantines the peer
	// locally so it is not selected while the callback disconnects it.
	OnPeerUnresponsive func(query.Peer)

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
// the QueryAccess API. It receives queries to pass to peers, and schedules them
// among available workers, orchestrating where to send them. It implements the
// query.WorkManager interface.
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

// Compile time check to ensure peerWorkManager satisfies the query.WorkManager
// interface.
var _ query.WorkManager = (*peerWorkManager)(nil)

// NewWorkManager returns a new query.WorkManager with the regular worker
// implementation.
func NewWorkManager(cfg *Config) query.WorkManager {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.NewWorker == nil {
		cfg.NewWorker = NewWorker
	}
	if cfg.Ranking == nil {
		cfg.Ranking = query.NewPeerRanking()
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
func queryTimeoutForPeer(baseTimeout time.Duration, peer query.Peer) time.Duration {
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

func peerSnapshot(peer query.Peer, rank int, state querysync.PeerState) querysync.PeerSnapshot {
	return querysync.PeerSnapshot{
		ID:    peer.Addr(),
		Rank:  rank,
		RTT:   time.Duration(peer.LastPingMicros()) * time.Microsecond,
		State: state,
	}
}

func workerRanks(workers map[string]*activeWorker,
	ranking PeerRanking) map[string]int {

	peers := make([]string, 0, len(workers))
	for peer := range workers {
		peers = append(peers, peer)
	}
	ranking.Order(peers)

	ranks := make(map[string]int, len(peers))
	if scorer, ok := ranking.(peerScorer); ok {
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

func newQuerySchedulerManager(baseTimeout time.Duration) *querysync.ManagerActor {
	return querysync.NewManagerActor(
		querysync.NewSchedulerFSM(querysync.Config{
			BaseTimeout:   baseTimeout,
			MinTimeout:    minQueryTimeout,
			MaxTimeout:    maxQueryTimeout,
			RTTMultiplier: queryTimeoutRTTMultiplier,
		}), 128,
	)
}

func sortedPeerSet(peers map[string]struct{}) []string {
	peerIDs := make([]string, 0, len(peers))
	for peerID := range peers {
		peerIDs = append(peerIDs, peerID)
	}
	sort.Strings(peerIDs)

	return peerIDs
}

func isNonResponseErr(err error) bool {
	return err == query.ErrQueryTimeout || err == query.ErrPeerDisconnected
}

func isHardCooldownErr(err error) bool {
	return err == query.ErrQueryTimeout || err == query.ErrPeerNotFound
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

func peerStateEvent(state querysync.PeerState) querysync.PeerEvent {
	switch state {
	case querysync.PeerReady:
		return querysync.PeerReadyEvent{}

	case querysync.PeerBlocked:
		return querysync.PeerBlockedEvent{}

	case querysync.PeerQuarantined:
		return querysync.PeerQuarantinedEvent{}

	case querysync.PeerDown:
		return querysync.PeerDownEvent{}

	default:
		return nil
	}
}

func updateWorkerPeerState(ctx context.Context,
	scheduler *querysync.ManagerActor, worker *activeWorker, rank int,
	state querysync.PeerState) error {

	snapshot := peerSnapshot(worker.peer, rank, state)
	if worker.actor == nil {
		return scheduler.AddPeer(ctx, snapshot)
	}

	if err := worker.actor.Send(ctx, querysync.PeerRankEvent{
		Rank: rank,
	}); err != nil {
		return err
	}

	if err := worker.actor.Send(ctx, querysync.PeerRTTEvent{
		RTT: snapshot.RTT,
	}); err != nil {
		return err
	}

	// Busy is entered only when a concrete assignment is accepted by the
	// worker. Re-sending PeerAssignedEvent during ordinary scheduler
	// refreshes would incorrectly look like a second concurrent assignment.
	if state == querysync.PeerBusy {
		return nil
	}

	event := peerStateEvent(state)
	if event == nil {
		return nil
	}

	return worker.actor.Send(ctx, event)
}

func setWorkerPeerState(ctx context.Context,
	scheduler *querysync.ManagerActor, worker *activeWorker,
	state querysync.PeerState) error {

	if worker.actor != nil {
		event := peerStateEvent(state)
		if event == nil {
			return nil
		}

		return worker.actor.Send(ctx, event)
	}

	return scheduler.UpdatePeerState(ctx, worker.peer.Addr(), state)
}

func assignWorkerJob(ctx context.Context,
	scheduler *querysync.ManagerActor, worker *activeWorker,
	job *queryJob) error {

	if worker.actor != nil {
		return worker.actor.Send(ctx, querysync.PeerAssignedEvent{
			TaskID: querysync.TaskID(job.index),
		})
	}

	return scheduler.UpdatePeerState(
		ctx, worker.peer.Addr(), querysync.PeerBusy,
	)
}

func workerLocalWorkIDs(worker *activeWorker) []querysync.TaskID {
	ids := make([]querysync.TaskID, 0, len(worker.localWork))
	for _, job := range worker.localWork {
		ids = append(ids, querysync.TaskID(job.index))
	}

	return ids
}

func setWorkerLocalWork(ctx context.Context,
	scheduler *querysync.ManagerActor, worker *activeWorker) error {

	return scheduler.SetPeerLocalWork(
		ctx, worker.peer.Addr(), workerLocalWorkIDs(worker),
	)
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

func scheduleQueryJob(workers map[string]*activeWorker, ranking PeerRanking,
	job *queryJob, batchNum uint64,
	blocked map[string]struct{}, ignoreCooldown bool) (
	querysync.Assignment, bool) {

	ctx := context.Background()
	scheduler := newQuerySchedulerManager(job.timeout)
	scheduler.Start(ctx)
	defer scheduler.Stop()

	return scheduleQueryJobWithScheduler(
		ctx, scheduler, workers, ranking, job, batchNum, blocked,
		ignoreCooldown,
	)
}

func scheduleQueryJobWithScheduler(ctx context.Context,
	scheduler *querysync.ManagerActor, workers map[string]*activeWorker,
	ranking PeerRanking, job *queryJob, batchNum uint64,
	blocked map[string]struct{}, ignoreCooldown bool) (
	querysync.Assignment, bool) {

	ranks := workerRanks(workers, ranking)
	now := time.Now()
	avoidPeers := make(map[string]struct{})

	for peerAddr, worker := range workers {
		state := querysync.PeerReady
		switch {
		case worker.activeJob != nil:
			state = querysync.PeerBusy
		case now.Before(worker.quarantine):
			state = querysync.PeerQuarantined
			avoidPeers[peerAddr] = struct{}{}
		case now.Before(worker.hardCooldown):
			state = querysync.PeerBlocked
			avoidPeers[peerAddr] = struct{}{}
		case !ignoreCooldown && now.Before(worker.cooldown):
			state = querysync.PeerBlocked
			avoidPeers[peerAddr] = struct{}{}
		case blocked != nil:
			if _, ok := blocked[peerAddr]; ok {
				state = querysync.PeerBlocked
				avoidPeers[peerAddr] = struct{}{}
			}
		}

		err := updateWorkerPeerState(
			ctx, scheduler, worker, ranks[peerAddr], state,
		)
		if err != nil {
			log.Warnf("Unable to update peer %v in query scheduler: %v",
				peerAddr, err)
		}
	}

	assignment, ok, err := scheduler.AskDispatchTask(ctx, querysync.Task{
		ID:         querysync.TaskID(job.index),
		BatchID:    batchNum,
		Canceled:   job.canceled(),
		Timeout:    job.timeout,
		AvoidPeers: sortedPeerSet(avoidPeers),
	})
	if err != nil {
		log.Warnf("Unable to dispatch query job %v: %v",
			job.index, err)

		return querysync.Assignment{}, false
	}

	return assignment, ok
}

func assignQueryJobToLocalWorkWithScheduler(ctx context.Context,
	scheduler *querysync.ManagerActor, workers map[string]*activeWorker,
	ranking PeerRanking, job *queryJob, batchNum uint64,
	blocked map[string]struct{}, ignoreCooldown bool) (
	querysync.Assignment, bool) {

	ranks := workerRanks(workers, ranking)
	now := time.Now()
	avoidPeers := make(map[string]struct{})

	for peerAddr, worker := range workers {
		state := querysync.PeerReady
		switch {
		case worker.activeJob != nil:
			state = querysync.PeerBusy
		case now.Before(worker.quarantine):
			state = querysync.PeerQuarantined
			avoidPeers[peerAddr] = struct{}{}
		case now.Before(worker.hardCooldown):
			state = querysync.PeerBlocked
			avoidPeers[peerAddr] = struct{}{}
		case !ignoreCooldown && now.Before(worker.cooldown):
			state = querysync.PeerBlocked
			avoidPeers[peerAddr] = struct{}{}
		case blocked != nil:
			if _, ok := blocked[peerAddr]; ok {
				state = querysync.PeerBlocked
				avoidPeers[peerAddr] = struct{}{}
			}
		}

		err := updateWorkerPeerState(
			ctx, scheduler, worker, ranks[peerAddr], state,
		)
		if err != nil {
			log.Warnf("Unable to update peer %v in query scheduler: %v",
				peerAddr, err)
		}

		if err := setWorkerLocalWork(ctx, scheduler, worker); err != nil {
			log.Warnf("Unable to update peer %v local work in "+
				"query scheduler: %v", peerAddr, err)
		}
	}

	assignment, ok, err := scheduler.AskAssignLocalTask(ctx, querysync.Task{
		ID:         querysync.TaskID(job.index),
		BatchID:    batchNum,
		Canceled:   job.canceled(),
		Timeout:    job.timeout,
		AvoidPeers: sortedPeerSet(avoidPeers),
	})
	if err != nil {
		log.Warnf("Unable to assign query job %v: %v", job.index, err)

		return querysync.Assignment{}, false
	}

	return assignment, ok
}

// Start starts the peerWorkManager.
//
// NOTE: this is part of the query.WorkManager interface.
func (w *peerWorkManager) Start() error {
	w.wg.Add(1)
	go w.workDispatcher()

	return nil
}

// Stop stops the peerWorkManager and all underlying goroutines.
//
// NOTE: this is part of the query.WorkManager interface.
func (w *peerWorkManager) Stop() error {
	close(w.quit)
	w.wg.Wait()

	return nil
}

// workDispatcher receives batches of queries to be performed from external
// callers, and dispatches these to active workers.  It makes sure to
// prioritize the queries in the order they come in, such that early queries
// will be attempted completed first.
//
// NOTE: MUST be run as a goroutine.
func (w *peerWorkManager) workDispatcher() {
	defer w.wg.Done()

	// Get a peer subscription. We do it in this goroutine rather than
	// Start to avoid a deadlock when starting the query.WorkManager fetches
	// the peers from the server.
	peersConnected, cancel, err := w.cfg.ConnectedPeers()
	if err != nil {
		log.Errorf("Unable to get connected peers: %v", err)
		return
	}
	defer cancel()

	// Init a work queue which will be used to sort the incoming queries in
	// a first come first served fashion.
	work := &workQueue[*queryJob]{}

	type batchProgress struct {
		noRetryMax bool
		maxRetries uint8
		timer      *time.Timer
		rem        int
		errChan    chan error
		cancelChan chan struct{}
	}

	// We set up a batch index counter to keep track of batches that still
	// have queries in flight. This lets us track when all queries for a
	// batch have been finished, and return an (non-)error to the caller.
	batchIndex := uint64(0)
	currentBatches := make(map[uint64]*batchProgress)
	batchTimeouts := make(chan uint64)

	// When the work dispatcher exits, we'll loop through the remaining
	// batches and send on their error channel.
	defer func() {
		for _, b := range currentBatches {
			if b.timer != nil {
				b.timer.Stop()
			}
			b.errChan <- query.ErrWorkManagerShuttingDown
		}
	}()

	// We set up a counter that we'll increase with each incoming query,
	// and will serve as the priority of each. In addition we map each
	// query to the batch they are part of.
	queryIndex := uint64(0)
	currentQueries := make(map[uint64]uint64)

	workers := make(map[string]*activeWorker)
	nonResponseFailures := make(map[string]uint8)
	quarantinedPeers := make(map[string]time.Time)
	sendBlockedWorkers := make(map[string]struct{})
	var scheduleRetry <-chan time.Time
	defer func() {
		for _, worker := range workers {
			if worker.actor != nil {
				worker.actor.Stop()
			}
		}
	}()

	schedulerCtx, stopScheduler := context.WithCancel(context.Background())
	defer stopScheduler()

	scheduler := newQuerySchedulerManager(minQueryTimeout)
	scheduler.SetTraceRecorder(w.cfg.SchedulerTrace)
	scheduler.SetEventSink(w.cfg.SchedulerEvents)
	scheduler.Start(schedulerCtx)
	defer scheduler.Stop()

	armScheduleRetry := func() {
		if scheduleRetry == nil {
			scheduleRetry = time.After(scheduleRetryInterval)
		}
	}

	finishBatch := func(batchNum uint64, batch *batchProgress,
		err error, cancel bool) {

		if batch.timer != nil {
			batch.timer.Stop()
		}

		batch.errChan <- err
		delete(currentBatches, batchNum)

		if cancel && batch.cancelChan != nil {
			close(batch.cancelChan)
		}
	}

	dropCanceledJob := func(job *queryJob) {
		batchNum, ok := currentQueries[job.index]
		delete(currentQueries, job.index)
		if !ok {
			return
		}

		batch, ok := currentBatches[batchNum]
		if ok {
			finishBatch(batchNum, batch, query.ErrJobCanceled, true)
			log.Debugf("Canceled batch %v", batchNum)
		}
	}

	rankedWorkerIDs := func() []string {
		ids := make([]string, 0, len(workers))
		for id := range workers {
			ids = append(ids, id)
		}
		w.cfg.Ranking.Order(ids)

		return ids
	}

	removeWorker := func(peerID string, worker *activeWorker) {
		for _, job := range worker.localWork {
			work.Push(job)
		}
		worker.localWork = nil

		delete(workers, peerID)
		delete(sendBlockedWorkers, peerID)
		if worker.actor != nil {
			if err := worker.actor.Send(
				schedulerCtx, querysync.PeerDownEvent{},
			); err != nil {
				log.Warnf("Unable to mark peer %v down in "+
					"query scheduler: %v", peerID, err)
			}
			worker.actor.Stop()
		}

		if err := scheduler.RemovePeer(schedulerCtx, peerID); err != nil {
			log.Warnf("Unable to remove peer %v from query "+
				"scheduler: %v", peerID, err)
		}
	}

	moveStolenWork := func(result querysync.StealResult) bool {
		donor := workers[result.DonorID]
		thief := workers[result.ThiefID]
		if donor == nil || thief == nil {
			return false
		}

		stolenTasks := result.TaskIDs
		if len(stolenTasks) == 0 {
			stolenTasks = []querysync.TaskID{result.TaskID}
		}

		now := time.Now()
		moved := make([]*queryJob, 0, len(stolenTasks))
		var stealAge time.Duration
		var maxStolenCount uint16
		for _, taskID := range stolenTasks {
			job, ok := removeWorkerLocalWork(donor, taskID)
			if !ok {
				restored := append([]*queryJob(nil), moved...)
				donor.localWork = append(restored, donor.localWork...)

				log.Warnf("Peer %v stole job %v from peer %v, "+
					"but donor no longer owns it",
					result.ThiefID, taskID, result.DonorID)

				return false
			}

			age := durationSince(now, job.ownedAt)
			if age > stealAge {
				stealAge = age
			}

			moved = append(moved, job)
		}

		for _, job := range moved {
			job.ownedAt = now
			job.lastStolenAt = now
			job.stolenCount++
			if job.stolenCount > maxStolenCount {
				maxStolenCount = job.stolenCount
			}
		}

		thief.localWork = append(thief.localWork, moved...)
		donorAge := oldestLocalQueueAge(now, donor)
		thiefAge := oldestLocalQueueAge(now, thief)

		recordSchedulerEvent(w.cfg.SchedulerEvents, querysync.SchedulerEvent{
			Type:             querysync.SchedulerEventWorkStealApplied,
			At:               now,
			PeerID:           result.ThiefID,
			ThiefID:          result.ThiefID,
			DonorID:          result.DonorID,
			TaskID:           result.TaskID,
			TaskIDs:          stolenTasks,
			Steal:            &result,
			OK:               true,
			StealAge:         stealAge,
			DonorLocalAge:    donorAge,
			ThiefLocalAge:    thiefAge,
			DonorLocalLen:    len(donor.localWork),
			ThiefLocalLen:    len(thief.localWork),
			StolenTaskCount:  len(moved),
			StolenCount:      maxStolenCount,
			LocalWorkLen:     len(thief.localWork),
			LocalQueueAge:    thiefAge,
			CooldownDuration: 0,
		})

		log.Debugf("Peer %v stole %v job(s) %v from peer %v "+
			"(donor_remaining=%v, steal_age=%v, donor_queue=%v, "+
			"thief_queue=%v)", result.ThiefID, len(moved),
			stolenTasks, result.DonorID, result.DonorRemaining,
			stealAge, len(donor.localWork), len(thief.localWork))

		return true
	}

	dropCanceledLocalWork := func(peerID string, worker *activeWorker) {
		for len(worker.localWork) > 0 &&
			worker.localWork[0].canceled() {

			job := worker.localWork[0]
			worker.localWork = worker.localWork[1:]
			if err := setWorkerLocalWork(
				schedulerCtx, scheduler, worker,
			); err != nil {
				log.Warnf("Unable to drop canceled local work "+
					"for peer %v in query scheduler: %v",
					peerID, err)
			}
			dropCanceledJob(job)
		}
	}

	dispatchOwnedWork := func() bool {
		now := time.Now()
		for _, peerID := range rankedWorkerIDs() {
			worker := workers[peerID]
			if worker == nil || worker.activeJob != nil {
				continue
			}
			if now.Before(worker.quarantine) {

				continue
			}
			if now.Before(worker.hardCooldown) {
				for _, job := range worker.localWork {
					work.Push(job)
				}
				worker.localWork = nil
				if err := setWorkerLocalWork(
					schedulerCtx, scheduler, worker,
				); err != nil {
					log.Warnf("Unable to clear hard-cooldown "+
						"local work for peer %v in query "+
						"scheduler: %v", peerID, err)
				}

				continue
			}
			if now.Before(worker.cooldown) &&
				len(worker.localWork) == 0 {

				continue
			}

			dropCanceledLocalWork(peerID, worker)
			if len(worker.localWork) == 0 && worker.actor != nil {
				steal, ok, err := worker.actor.RequestStolenWork(
					schedulerCtx,
				)
				if err != nil {
					log.Warnf("Peer %v failed to request "+
						"stolen work: %v", peerID, err)
				} else if ok {
					moveStolenWork(steal)
				}
			}

			dropCanceledLocalWork(peerID, worker)
			if len(worker.localWork) == 0 {
				continue
			}

			if err := setWorkerPeerState(
				schedulerCtx, scheduler, worker,
				querysync.PeerReady,
			); err != nil {
				log.Warnf("Unable to mark peer %v ready in "+
					"query scheduler: %v", peerID, err)
			}

			job := worker.localWork[0]
			originalTimeout := job.timeout
			job.timeout = queryTimeoutForPeer(job.timeout, worker.peer)
			sentAt := time.Now()
			localQueueAge := durationSince(sentAt, job.ownedAt)
			taskAge := durationSince(sentAt, job.createdAt)

			select {
			case worker.w.NewJob() <- job:
				delete(sendBlockedWorkers, peerID)
				worker.localWork = worker.localWork[1:]
				if err := setWorkerLocalWork(
					schedulerCtx, scheduler, worker,
				); err != nil {
					log.Warnf("Unable to update peer %v "+
						"local work in query scheduler: %v",
						peerID, err)
				}

				worker.activeJob = job
				job.activeAt = sentAt
				if err := assignWorkerJob(
					schedulerCtx, scheduler, worker, job,
				); err != nil {
					log.Warnf("Unable to mark peer %v busy "+
						"in query scheduler: %v", peerID,
						err)
				}

				recordSchedulerEvent(
					w.cfg.SchedulerEvents,
					querysync.SchedulerEvent{
						Type:          querysync.SchedulerEventTaskSent,
						At:            sentAt,
						PeerID:        peerID,
						TaskID:        querysync.TaskID(job.index),
						Timeout:       job.timeout,
						TaskAge:       taskAge,
						LocalQueueAge: localQueueAge,
						LocalWorkLen:  len(worker.localWork),
						StolenCount:   job.stolenCount,
						Attempts:      job.tries,
						OK:            true,
					},
				)

				log.Tracef("Sent owned job %v to worker %v "+
					"(queue_age=%v, task_age=%v, timeout=%v, "+
					"local_remaining=%v)", job.Index(), peerID,
					localQueueAge, taskAge, job.timeout,
					len(worker.localWork))
				scheduleRetry = nil

				return true

			case <-worker.onExit:
				job.timeout = originalTimeout
				removeWorker(peerID, worker)

				return true

			case <-w.quit:
				return false

			default:
				job.timeout = originalTimeout
				sendBlockedWorkers[peerID] = struct{}{}
				if err := setWorkerPeerState(
					schedulerCtx, scheduler, worker,
					querysync.PeerBlocked,
				); err != nil {
					log.Warnf("Unable to mark peer %v "+
						"blocked in query scheduler: %v",
						peerID, err)
				}
				armScheduleRetry()
			}
		}

		return false
	}

Loop:
	for {
		if dispatchOwnedWork() {
			continue Loop
		}

		// If the work queue is non-empty, we'll take out the first
		// element in order to distribute it to a worker.
		if work.Len() > 0 {
			next, _ := work.Peek()
			if next.canceled() {
				work.Pop()
				batchNum, ok := currentQueries[next.index]
				delete(currentQueries, next.index)

				if ok {
					batch, ok := currentBatches[batchNum]
					if ok {
						finishBatch(
							batchNum, batch, query.ErrJobCanceled,
							true,
						)
						log.Debugf(
							"Canceled batch %v", batchNum,
						)
					}
				}

				continue Loop
			}

			batchNum, ok := currentQueries[next.index]
			if !ok {
				log.Warnf("Query(%d) found in work queue "+
					"without batch tracking", next.index)
				work.Pop()
				continue Loop
			}

			blockedWorkers := make(map[string]struct{})
			hardAvoidWorkers := make(map[string]struct{})
			currentFailedPeers := 0
			for peer := range next.failedPeers {
				if _, ok := workers[peer]; ok {
					currentFailedPeers++
				}
			}
			if currentFailedPeers < len(workers) {
				for peer := range next.failedPeers {
					if _, ok := workers[peer]; ok {
						hardAvoidWorkers[peer] = struct{}{}
					}
				}
			}
			for peer := range sendBlockedWorkers {
				blockedWorkers[peer] = struct{}{}
			}

			now := time.Now()
			softAvoidWorkers := make(map[string]struct{})
			for peer, worker := range workers {
				if now.Before(worker.quarantine) {
					blockedWorkers[peer] = struct{}{}
					continue
				}
				if now.Before(worker.hardCooldown) {
					blockedWorkers[peer] = struct{}{}
					continue
				}
				if now.Before(worker.cooldown) {
					softAvoidWorkers[peer] = struct{}{}
				}
			}

			for peer := range hardAvoidWorkers {
				blockedWorkers[peer] = struct{}{}
			}
			for peer := range softAvoidWorkers {
				blockedWorkers[peer] = struct{}{}
			}

			avoidingCooldown := len(softAvoidWorkers) > 0
			ignoreCooldown := false
			for {
				assignment, ok := assignQueryJobToLocalWorkWithScheduler(
					schedulerCtx, scheduler, workers,
					w.cfg.Ranking, next, batchNum, blockedWorkers,
					ignoreCooldown,
				)
				if !ok {
					if avoidingCooldown {
						for peer := range softAvoidWorkers {
							delete(blockedWorkers, peer)
						}
						avoidingCooldown = false
						ignoreCooldown = true
						continue
					}

					break
				}

				p := assignment.PeerID
				r, ok := workers[p]
				if !ok {
					blockedWorkers[p] = struct{}{}
					continue
				}

				next.timeout = assignment.Timeout
				ownedAt := time.Now()
				if next.createdAt.IsZero() {
					next.createdAt = ownedAt
				}
				next.ownedAt = ownedAt
				r.localWork = append(r.localWork, next)
				work.Pop()
				recordSchedulerEvent(
					w.cfg.SchedulerEvents,
					querysync.SchedulerEvent{
						Type:         querysync.SchedulerEventTaskLocalQueued,
						At:           ownedAt,
						PeerID:       p,
						TaskID:       querysync.TaskID(next.index),
						BatchID:      batchNum,
						Timeout:      next.timeout,
						TaskAge:      durationSince(ownedAt, next.createdAt),
						LocalWorkLen: len(r.localWork),
						Attempts:     next.tries,
						OK:           true,
					},
				)
				log.Tracef("Assigned job %v to peer %v local "+
					"work queue (task_age=%v, local_len=%v)",
					next.Index(), p,
					durationSince(ownedAt, next.createdAt),
					len(r.localWork))

				continue Loop
			}

			if len(blockedWorkers) > 0 {
				armScheduleRetry()
			}
		}

		// Otherwise the work queue is empty, or there are no workers
		// to distribute work to, so we'll just wait for a result of a
		// previous query to come back, a new peer to connect, or for a
		// new batch of queries to be scheduled.
		select {
		// Spin up a goroutine that runs a worker each time a peer
		// connects.
		case peer := <-peersConnected:
			log.Debugf("Starting worker for peer %v",
				peer.Addr())

			r := w.cfg.NewWorker(peer)
			peerActor := querysync.NewPeerActor(
				peer.Addr(), 0, scheduler, 128,
			)
			if err := peerActor.Start(schedulerCtx); err != nil {
				log.Warnf("Unable to start query peer actor for "+
					"%v: %v", peer.Addr(), err)
				peerActor = nil
			}

			// We'll create a channel that will close after the
			// worker's Run method returns, to know when we can
			// remove it from our set of active workers.
			onExit := make(chan struct{})
			workers[peer.Addr()] = &activeWorker{
				w:          r,
				peer:       peer,
				actor:      peerActor,
				activeJob:  nil,
				quarantine: quarantinedPeers[peer.Addr()],
				onExit:     onExit,
			}

			w.cfg.Ranking.AddPeer(peer.Addr())
			if peerActor == nil {
				err := scheduler.AddPeer(schedulerCtx,
					querysync.PeerSnapshot{
						ID:    peer.Addr(),
						State: querysync.PeerReady,
					},
				)
				if err != nil {
					log.Warnf("Unable to add peer %v to "+
						"query scheduler: %v",
						peer.Addr(), err)
				}
			}

			w.wg.Add(1)
			go func() {
				defer w.wg.Done()
				defer close(onExit)

				r.Run(w.jobResults, w.quit)
			}()

		// A new result came back.
		case result := <-w.jobResults:
			resultAt := time.Now()
			activeDuration := durationSince(
				resultAt, result.job.activeAt,
			)
			taskAge := durationSince(resultAt, result.job.createdAt)

			log.Tracef("Result for job %v received from peer %v "+
				"(err=%v)", result.job.index,
				result.peer.Addr(), result.err)

			// Delete the job from the worker's active job, such
			// that the slot gets opened for more work.
			r := workers[result.peer.Addr()]
			if r != nil {
				r.activeJob = nil
				err := setWorkerPeerState(
					schedulerCtx, scheduler, r,
					querysync.PeerReady,
				)
				if err != nil {
					log.Warnf("Unable to complete peer %v "+
						"work in query scheduler: %v",
						result.peer.Addr(), err)
				}
			}

			// Get the index of this query's batch, and delete it
			// from the map of current queries, since we don't have
			// to track it anymore. We'll add it back if the result
			// turns out to be an error.
			batchNum, ok := currentQueries[result.job.index]
			if !ok {
				log.Warnf("Query(%d) result from peer %v "+
					"discarded because query is no "+
					"longer tracked: %v",
					result.job.index,
					result.peer.Addr(), result.err)

				continue Loop
			}

			delete(currentQueries, result.job.index)

			// In case the batch is already canceled we return
			// early.
			batch, ok := currentBatches[batchNum]
			if !ok {
				log.Warnf("Query(%d) result from peer %v "+
					"discarded with retries %d, because "+
					"batch already canceled: %v",
					result.job.index,
					result.peer.Addr(),
					result.job.tries, result.err)

				continue Loop
			}

			recordSchedulerEvent(
				w.cfg.SchedulerEvents,
				querysync.SchedulerEvent{
					Type:           querysync.SchedulerEventTaskResult,
					At:             resultAt,
					PeerID:         result.peer.Addr(),
					TaskID:         querysync.TaskID(result.job.index),
					BatchID:        batchNum,
					Err:            errString(result.err),
					TaskAge:        taskAge,
					ActiveDuration: activeDuration,
					Timeout:        result.job.timeout,
					StolenCount:    result.job.stolenCount,
					Attempts:       result.job.tries,
					OK:             result.err == nil,
				},
			)

			switch {
			// If the query ended because it was canceled, drop it.
			case result.err == query.ErrJobCanceled:
				log.Tracef("Query(%d) was canceled before "+
					"result was available from peer %v",
					result.job.index, result.peer.Addr())

				// If this is the first job in this batch that
				// was canceled, forward the error on the
				// batch's error channel.  We do this since a
				// cancellation applies to the whole batch.
				finishBatch(
					batchNum, batch, result.err, true,
				)

				log.Debugf("Canceled batch %v", batchNum)
				continue Loop

				// If the query ended with any other error, put it back
				// into the work queue if it has not reached the
			// maximum number of retries.
			case result.err != nil:
				quarantined := false
				var failureCount uint8
				if isNonResponseErr(result.err) {
					failures := nonResponseFailures[result.peer.Addr()] + 1
					nonResponseFailures[result.peer.Addr()] =
						failures
					failureCount = failures

					if failures >= nonResponsivePeerFailures {
						quarantineUntil := resultAt.Add(
							nonResponsivePeerCooldown,
						)
						quarantinedPeers[result.peer.Addr()] =
							quarantineUntil
						delete(
							nonResponseFailures,
							result.peer.Addr(),
						)

						if r != nil {
							r.quarantine = quarantineUntil
							if err := setWorkerPeerState(
								schedulerCtx,
								scheduler, r,
								querysync.PeerQuarantined,
							); err != nil {
								log.Warnf("Unable to "+
									"quarantine "+
									"peer %v in "+
									"query "+
									"scheduler: %v",
									result.peer.Addr(),
									err)
							}
						}
						quarantined = true

						log.Warnf("Peer %v failed to "+
							"respond to %v queries; "+
							"quarantining until %v "+
							"(job=%v, active_for=%v, "+
							"task_age=%v)",
							result.peer.Addr(),
							nonResponsivePeerFailures,
							quarantineUntil,
							result.job.index,
							activeDuration, taskAge)

						recordSchedulerEvent(
							w.cfg.SchedulerEvents,
							querysync.SchedulerEvent{
								Type:           querysync.SchedulerEventPeerQuarantined,
								At:             resultAt,
								PeerID:         result.peer.Addr(),
								State:          querysync.PeerQuarantined,
								TaskID:         querysync.TaskID(result.job.index),
								BatchID:        batchNum,
								Err:            errString(result.err),
								TaskAge:        taskAge,
								ActiveDuration: activeDuration,
								FailureCount:   failureCount,
								Until:          quarantineUntil,
								OK:             true,
							},
						)

						if w.cfg.OnPeerUnresponsive != nil {
							w.cfg.OnPeerUnresponsive(
								result.peer,
							)
						}
					}
				}

				if r != nil {
					cooldownUntil := resultAt.Add(failedPeerCooldown)
					r.cooldown = cooldownUntil
					if isHardCooldownErr(result.err) {
						r.hardCooldown = cooldownUntil
					}
					if !quarantined {
						err := setWorkerPeerState(
							schedulerCtx, scheduler, r,
							querysync.PeerBlocked,
						)
						if err != nil {
							log.Warnf("Unable to cool "+
								"down peer %v in "+
								"query scheduler: %v",
								result.peer.Addr(),
								err)
						}
					}

					recordSchedulerEvent(
						w.cfg.SchedulerEvents,
						querysync.SchedulerEvent{
							Type:             querysync.SchedulerEventPeerCooldown,
							At:               resultAt,
							PeerID:           result.peer.Addr(),
							State:            querysync.PeerBlocked,
							TaskID:           querysync.TaskID(result.job.index),
							BatchID:          batchNum,
							Err:              errString(result.err),
							TaskAge:          taskAge,
							ActiveDuration:   activeDuration,
							FailureCount:     failureCount,
							CooldownDuration: failedPeerCooldown,
							Until:            cooldownUntil,
							OK:               true,
						},
					)
				}

				// Refresh peer rank on disconnect.
				if result.err == query.ErrPeerDisconnected {
					w.cfg.Ranking.ResetRanking(
						result.peer.Addr(),
					)
				} else {
					// Punish the peer for the failed query.
					w.cfg.Ranking.Punish(result.peer.Addr())
				}

				if !batch.noRetryMax {
					result.job.tries++
				}

				// Check if this query has reached its maximum
				// number of retries. If so, remove it from the
				// batch and don't reschedule it.
				if !batch.noRetryMax &&
					result.job.tries >= batch.maxRetries {

					log.Warnf("Query(%d) from peer %v "+
						"failed and reached maximum "+
						"number of retries, not "+
						"rescheduling: %v "+
						"(active_for=%v, task_age=%v, "+
						"tries=%v)",
						result.job.index,
						result.peer.Addr(), result.err,
						activeDuration, taskAge,
						result.job.tries)

					// Return the error and cancel the
					// batch.
					finishBatch(
						batchNum, batch, result.err, true,
					)

					log.Debugf("Canceled batch %v",
						batchNum)

					// Since we've reached this query's
					// maximum number of retries, now is the
					// time to call the OnMaxTries callback
					// function if it isn't nil.
					if w.cfg.OnMaxTries != nil {
						w.cfg.OnMaxTries(result.peer)
					}

					continue Loop
				}

				log.Warnf("Query(%d) from peer %v failed, "+
					"rescheduling: %v (active_for=%v, "+
					"task_age=%v, tries=%v, timeout=%v)",
					result.job.index, result.peer.Addr(),
					result.err, activeDuration, taskAge,
					result.job.tries, result.job.timeout)

				if result.job.failedPeers == nil {
					result.job.failedPeers = make(map[string]struct{})
				}
				result.job.failedPeers[result.peer.Addr()] = struct{}{}

				// If it was a timeout, we dynamically increase
				// it for the next attempt.
				if result.err == query.ErrQueryTimeout {
					newTimeout := result.job.timeout * 2
					if newTimeout > maxQueryTimeout {
						newTimeout = maxQueryTimeout
					}
					result.job.timeout = newTimeout
				}

				work.Push(result.job)
				currentQueries[result.job.index] = batchNum

			// Otherwise, we got a successful result and update the
			// status of the batch this query is a part of.
			default:
				delete(nonResponseFailures, result.peer.Addr())
				delete(quarantinedPeers, result.peer.Addr())
				if r != nil {
					r.quarantine = time.Time{}
					r.hardCooldown = time.Time{}
				}

				// Reward the peer for the successful query.
				w.cfg.Ranking.Reward(result.peer.Addr())

				// Decrement the number of queries remaining in
				// the batch.
				batch.rem--
				log.Tracef("Remaining jobs for batch "+
					"%v: %v ", batchNum, batch.rem)

				// If this was the last query in flight
				// for this batch, we can notify that
				// it finished, and delete it.
				if batch.rem == 0 {
					finishBatch(
						batchNum, batch, nil, false,
					)

					log.Tracef("Batch %v done",
						batchNum)
					continue Loop
				}
			}

		// A new batch of queries where scheduled.
		case batch := <-w.newBatches:
			// Add all new queries in the batch to our work queue,
			// with priority given by the order they were
			// scheduled.
			log.Debugf("Adding new batch(%d) of %d queries to "+
				"work queue", batchIndex, len(batch.requests))

			// Internal cancel channel of a batch request.
			cancelChan := make(chan struct{})
			thisBatchIndex := batchIndex
			createdAt := time.Now()

			for _, q := range batch.requests {
				work.Push(&queryJob{
					index:              queryIndex,
					timeout:            minQueryTimeout,
					encoding:           batch.options.Encoding,
					cancelChan:         batch.options.CancelChan,
					internalCancelChan: cancelChan,
					createdAt:          createdAt,
					Request:            q,
				})
				currentQueries[queryIndex] = batchIndex
				queryIndex++
			}

			timer := time.AfterFunc(batch.options.Timeout, func() {
				select {
				case batchTimeouts <- thisBatchIndex:
				case <-w.quit:
				}
			})

			currentBatches[batchIndex] = &batchProgress{
				noRetryMax: batch.options.NoRetryMax,
				maxRetries: batch.options.NumRetries,
				timer:      timer,
				rem:        len(batch.requests),
				errChan:    batch.errChan,
				cancelChan: cancelChan,
			}
			batchIndex++

		case batchNum := <-batchTimeouts:
			batch, ok := currentBatches[batchNum]
			if !ok {
				continue Loop
			}

			finishBatch(
				batchNum, batch, query.ErrQueryTimeout, true,
			)

			log.Warnf("Batch %v timed out", batchNum)

		case <-scheduleRetry:
			scheduleRetry = nil
			sendBlockedWorkers = make(map[string]struct{})

		case <-w.quit:
			return
		}
	}
}

// Query distributes the slice of requests to the set of connected peers.
//
// NOTE: this is part of the query.WorkManager interface.
func (w *peerWorkManager) Query(requests []*query.Request,
	options ...query.QueryOption) chan error {

	qo := query.ResolveOptions(options...)

	errChan := make(chan error, 1)

	// Add query messages to the queue of batches to handle.
	select {
	case w.newBatches <- &batch{
		requests: requests,
		options:  qo,
		errChan:  errChan,
	}:
	case <-w.quit:
		errChan <- query.ErrWorkManagerShuttingDown
	}

	return errChan
}
