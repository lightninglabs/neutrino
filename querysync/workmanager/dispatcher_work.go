package workmanager

import (
	"time"

	"github.com/lightninglabs/neutrino/querysync"
)

// dispatchOwnedWork tries to start the next job in the local queue of the best
// ranked idle peer. It reports whether a job was sent.
func (d *dispatcher) dispatchOwnedWork() bool {
	now := time.Now()
	for _, peerID := range d.rankedWorkerIDs() {
		worker := d.workers[peerID]
		if worker == nil || worker.activeJob != nil {
			continue
		}

		// Quarantined and hard cooled down peers are skipped. Their
		// unstarted work was already returned to the global queue the
		// moment they got into that state.
		if now.Before(worker.quarantine) ||
			now.Before(worker.hardCooldown) {

			continue
		}

		// A peer in soft cooldown may only finish the work it already
		// owns, and must not go looking for more.
		if now.Before(worker.cooldown) && len(worker.localWork) == 0 {
			continue
		}

		// An idle peer with nothing queued tries to steal from the
		// most loaded one.
		d.dropCanceledLocalWork(worker)
		if len(worker.localWork) == 0 {
			d.stealFor(peerID)
		}

		d.dropCanceledLocalWork(worker)
		if len(worker.localWork) == 0 {
			continue
		}

		if d.sendOwnedJob(peerID, worker) {
			return true
		}
	}

	return false
}

// sendOwnedJob hands the first job in the worker's local queue to the worker
// without blocking. A worker that is not ready to take it is marked as send
// blocked, and retried shortly. It reports whether the job was sent.
func (d *dispatcher) sendOwnedJob(peerID string, worker *activeWorker) bool {
	job := worker.localWork[0]

	// The timeout is adapted to the peer, but only sticks if the job
	// is actually sent.
	originalTimeout := job.timeout
	job.timeout = queryTimeoutForPeer(job.timeout, worker.peer)
	sentAt := time.Now()
	localQueueAge := durationSince(sentAt, job.ownedAt)
	taskAge := durationSince(sentAt, job.createdAt)

	select {
	case worker.w.NewJob() <- job:

	case <-d.w.quit:
		job.timeout = originalTimeout
		return false

	default:
		job.timeout = originalTimeout
		d.sendBlocked[peerID] = struct{}{}
		d.armScheduleRetry()

		return false
	}

	delete(d.sendBlocked, peerID)
	worker.localWork = worker.localWork[1:]
	worker.activeJob = job
	job.activeAt = sentAt

	d.emit(querysync.SchedulerEvent{
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
	})

	log.Tracef("Sent owned job %v to worker %v (queue_age=%v, "+
		"task_age=%v, timeout=%v, local_remaining=%v)", job.Index(),
		peerID, localQueueAge, taskAge, job.timeout,
		len(worker.localWork))

	d.scheduleRetry = nil

	return true
}

// dropCanceledLocalWork discards canceled jobs at the head of a worker's local
// queue, and aborts the batches they belong to.
func (d *dispatcher) dropCanceledLocalWork(worker *activeWorker) {
	for len(worker.localWork) > 0 && worker.localWork[0].canceled() {
		job := worker.localWork[0]
		worker.localWork = worker.localWork[1:]
		d.dropCanceledJob(job)
	}
}

// stealFor asks the scheduler to move unstarted work from the most loaded peer
// to the idle thief.
func (d *dispatcher) stealFor(thiefID string) {
	// The scheduler decides on the basis of what it knows about all peers,
	// so bring it up to date first. Peers that declined a send are
	// reported as blocked, which makes their queues fair game.
	d.syncPeers(syncOpts{blocked: d.sendBlocked, ready: thiefID})

	steal, ok := d.sched.StealOne(thiefID)
	if !ok {
		return
	}

	d.moveStolenWork(steal)
}

// moveStolenWork applies a steal decision to the workers' local queues. It
// reports whether the jobs were moved.
func (d *dispatcher) moveStolenWork(result querysync.StealResult) bool {
	donor := d.workers[result.DonorID]
	thief := d.workers[result.ThiefID]
	if donor == nil || thief == nil {
		return false
	}

	stolenTasks := result.TaskIDs
	if len(stolenTasks) == 0 {
		stolenTasks = []querysync.TaskID{result.TaskID}
	}

	// Take every stolen job off the donor's queue. If the donor no longer
	// owns one of them, put back what was taken and give up.
	now := time.Now()
	moved := make([]*queryJob, 0, len(stolenTasks))
	var stealAge time.Duration
	for _, taskID := range stolenTasks {
		job, ok := removeWorkerLocalWork(donor, taskID)
		if !ok {
			restored := append([]*queryJob(nil), moved...)
			donor.localWork = append(restored, donor.localWork...)

			log.Warnf("Peer %v stole job %v from peer %v, but "+
				"donor no longer owns it", result.ThiefID,
				taskID, result.DonorID)

			return false
		}

		stealAge = max(stealAge, durationSince(now, job.ownedAt))
		moved = append(moved, job)
	}

	// The jobs now belong to the thief.
	var maxStolenCount uint16
	for _, job := range moved {
		job.ownedAt = now
		job.lastStolenAt = now
		job.stolenCount++
		maxStolenCount = max(maxStolenCount, job.stolenCount)
	}

	thief.localWork = append(thief.localWork, moved...)
	donorAge := oldestLocalQueueAge(now, donor)
	thiefAge := oldestLocalQueueAge(now, thief)

	d.emit(querysync.SchedulerEvent{
		Type:            querysync.SchedulerEventWorkStealApplied,
		At:              now,
		PeerID:          result.ThiefID,
		ThiefID:         result.ThiefID,
		DonorID:         result.DonorID,
		TaskID:          result.TaskID,
		TaskIDs:         stolenTasks,
		Steal:           &result,
		OK:              true,
		StealAge:        stealAge,
		DonorLocalAge:   donorAge,
		ThiefLocalAge:   thiefAge,
		DonorLocalLen:   len(donor.localWork),
		ThiefLocalLen:   len(thief.localWork),
		StolenTaskCount: len(moved),
		StolenCount:     maxStolenCount,
		LocalWorkLen:    len(thief.localWork),
		LocalQueueAge:   thiefAge,
	})

	log.Debugf("Peer %v stole %v job(s) %v from peer %v "+
		"(donor_remaining=%v, steal_age=%v, donor_queue=%v, "+
		"thief_queue=%v)", result.ThiefID, len(moved), stolenTasks,
		result.DonorID, result.DonorRemaining, stealAge,
		len(donor.localWork), len(thief.localWork))

	return true
}

// assignQueuedWork moves the job at the head of the global queue into the
// local queue of a peer. It reports whether it made progress, which is also
// the case if the job was found to be canceled or untracked and dropped.
func (d *dispatcher) assignQueuedWork() bool {
	next, ok := d.work.Peek()
	if !ok {
		return false
	}

	// Canceled jobs are dropped, along with the batch they belong to.
	if next.canceled() {
		d.work.Pop()
		d.dropCanceledJob(next)

		return true
	}

	batchNum, ok := d.queries[next.index]
	if !ok {
		log.Warnf("Query(%d) found in work queue without batch "+
			"tracking", next.index)
		d.work.Pop()

		return true
	}

	blocked, softAvoid := d.blockedPeersFor(next)

	return d.assignToPeer(next, batchNum, blocked, softAvoid)
}

// blockedPeersFor returns the peers that must not be given the job right now,
// and the subset of them that are only avoided because they are in soft
// cooldown.
func (d *dispatcher) blockedPeersFor(job *queryJob) (map[string]struct{},
	map[string]struct{}) {

	blocked := make(map[string]struct{})
	softAvoid := make(map[string]struct{})

	// Peers that already failed this job are only avoided as long as some
	// other peer is left to try.
	failedPeers := 0
	for peer := range job.failedPeers {
		if _, ok := d.workers[peer]; ok {
			failedPeers++
		}
	}
	if failedPeers < len(d.workers) {
		for peer := range job.failedPeers {
			if _, ok := d.workers[peer]; ok {
				blocked[peer] = struct{}{}
			}
		}
	}

	for peer := range d.sendBlocked {
		blocked[peer] = struct{}{}
	}

	// Quarantined and hard cooled down peers are never used, while peers
	// in soft cooldown are used as a last resort.
	now := time.Now()
	for peer, worker := range d.workers {
		switch {
		case now.Before(worker.quarantine),
			now.Before(worker.hardCooldown):

			blocked[peer] = struct{}{}

		case now.Before(worker.cooldown):
			softAvoid[peer] = struct{}{}
			blocked[peer] = struct{}{}
		}
	}

	return blocked, softAvoid
}

// assignToPeer asks the scheduler which peer should own the job, and queues it
// there. If no peer qualifies, peers in soft cooldown are tried before giving
// up. It reports whether the job was assigned.
func (d *dispatcher) assignToPeer(job *queryJob, batchNum uint64,
	blocked, softAvoid map[string]struct{}) bool {

	avoidingCooldown := len(softAvoid) > 0
	ignoreCooldown := false
	for {
		avoid := d.syncPeers(syncOpts{
			blocked:        blocked,
			ignoreCooldown: ignoreCooldown,
		})
		assignment, ok := d.sched.AssignLocalTask(querysync.Task{
			ID:         querysync.TaskID(job.index),
			BatchID:    batchNum,
			Canceled:   job.canceled(),
			Timeout:    job.timeout,
			AvoidPeers: avoid,
		})
		if !ok {
			if avoidingCooldown {
				for peer := range softAvoid {
					delete(blocked, peer)
				}
				avoidingCooldown = false
				ignoreCooldown = true

				continue
			}

			if len(blocked) > 0 {
				d.armScheduleRetry()
			}

			return false
		}

		peerID := assignment.PeerID
		worker, ok := d.workers[peerID]
		if !ok {
			blocked[peerID] = struct{}{}
			continue
		}

		d.queueLocally(job, batchNum, worker, assignment)

		return true
	}
}

// queueLocally moves a job from the global queue to the end of a worker's
// local queue.
func (d *dispatcher) queueLocally(job *queryJob, batchNum uint64,
	worker *activeWorker, assignment querysync.Assignment) {

	peerID := assignment.PeerID

	job.timeout = assignment.Timeout
	ownedAt := time.Now()
	if job.createdAt.IsZero() {
		job.createdAt = ownedAt
	}
	job.ownedAt = ownedAt
	worker.localWork = append(worker.localWork, job)
	d.work.Pop()

	taskAge := durationSince(ownedAt, job.createdAt)
	d.emit(querysync.SchedulerEvent{
		Type:         querysync.SchedulerEventTaskLocalQueued,
		At:           ownedAt,
		PeerID:       peerID,
		TaskID:       querysync.TaskID(job.index),
		BatchID:      batchNum,
		Timeout:      job.timeout,
		TaskAge:      taskAge,
		LocalWorkLen: len(worker.localWork),
		Attempts:     job.tries,
		OK:           true,
	})

	log.Tracef("Assigned job %v to peer %v local work queue "+
		"(task_age=%v, local_len=%v)", job.Index(), peerID, taskAge,
		len(worker.localWork))
}
