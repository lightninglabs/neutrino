package workmanager

import (
	"time"

	"github.com/lightninglabs/neutrino/querysync"
)

// handleResult processes the outcome of a job delivered by a worker.
func (d *dispatcher) handleResult(result *jobResult) {
	info := &resultInfo{
		result: result,
		at:     time.Now(),
	}
	info.activeDuration = durationSince(info.at, result.job.activeAt)
	info.taskAge = durationSince(info.at, result.job.createdAt)

	log.Tracef("Result for job %v received from peer %v (err=%v)",
		result.job.index, info.peerAddr(), result.err)

	// Clear the worker's active job, such that the slot gets opened for
	// more work. A late result from a replaced worker must not touch the
	// worker that took its place after a reconnect.
	worker := d.workers[info.peerAddr()]
	if worker != nil && worker.peer == result.peer {
		worker.activeJob = nil
		info.worker = worker
	}

	// Get the index of this query's batch, and delete it from the map of
	// current queries, since we don't have to track it anymore. We'll add
	// it back if the result turns out to be an error.
	batchNum, ok := d.queries[result.job.index]
	if !ok {
		log.Warnf("Query(%d) result from peer %v discarded because "+
			"query is no longer tracked: %v", result.job.index,
			info.peerAddr(), result.err)

		return
	}
	delete(d.queries, result.job.index)

	// In case the batch is already canceled we return early.
	batch, ok := d.batches[batchNum]
	if !ok {
		log.Warnf("Query(%d) result from peer %v discarded with "+
			"retries %d, because batch already canceled: %v",
			result.job.index, info.peerAddr(), result.job.tries,
			result.err)

		return
	}
	info.batchNum = batchNum
	info.batch = batch

	d.emit(querysync.SchedulerEvent{
		Type:           querysync.SchedulerEventTaskResult,
		At:             info.at,
		PeerID:         info.peerAddr(),
		TaskID:         querysync.TaskID(result.job.index),
		BatchID:        batchNum,
		Err:            errString(result.err),
		TaskAge:        info.taskAge,
		ActiveDuration: info.activeDuration,
		Timeout:        result.job.timeout,
		StolenCount:    result.job.stolenCount,
		Attempts:       result.job.tries,
		OK:             result.err == nil,
	})

	switch {
	// If the query ended because it was canceled, drop it. Since a
	// cancellation applies to the whole batch, the batch's error channel
	// gets the error.
	case result.err == ErrJobCanceled:
		log.Tracef("Query(%d) was canceled before result was "+
			"available from peer %v", result.job.index,
			info.peerAddr())

		d.cancelBatch(batchNum, result.err)
		log.Debugf("Canceled batch %v", batchNum)

	// If the query ended with any other error, put it back into the work
	// queue if it has not reached the maximum number of retries.
	case result.err != nil:
		d.handleJobFailure(info)

	// Otherwise, we got a successful result and update the status of the
	// batch this query is a part of.
	default:
		d.handleJobSuccess(info)
	}
}

// handleJobSuccess rewards the peer, and completes the batch if this was its
// last outstanding job.
func (d *dispatcher) handleJobSuccess(info *resultInfo) {
	peerID := info.peerAddr()

	// A peer that answers is no longer suspect.
	delete(d.nonResponseFailures, peerID)
	delete(d.quarantinedPeers, peerID)
	if info.worker != nil {
		info.worker.quarantine = time.Time{}
		info.worker.hardCooldown = time.Time{}
	}

	d.cfg.Ranking.Reward(peerID)

	// Decrement the number of queries remaining in the batch.
	info.batch.rem--
	log.Tracef("Remaining jobs for batch %v: %v ", info.batchNum,
		info.batch.rem)

	// If this was the last query in flight for this batch, we can notify
	// that it finished, and delete it.
	if info.batch.rem == 0 {
		d.completeBatch(info.batchNum, info.batch, nil)
		log.Tracef("Batch %v done", info.batchNum)

		return
	}

	d.armProgressTimer(info.batch, info.batchNum)
}

// handleJobFailure penalizes the peer that failed a job, and either puts the
// job back in the global queue or, if it ran out of retries, fails the batch.
func (d *dispatcher) handleJobFailure(info *resultInfo) {
	job := info.result.job
	peerID := info.peerAddr()

	quarantined, failureCount := d.trackNonResponse(info)
	d.coolDownPeer(info, quarantined, failureCount)

	// Refresh the peer's rank on disconnect, and punish it for any other
	// failed query.
	if info.result.err == ErrPeerDisconnected {
		d.cfg.Ranking.ResetRanking(peerID)
	} else {
		d.cfg.Ranking.Punish(peerID)
	}

	if !info.batch.noRetryMax {
		job.tries++
	}

	// Check if this query has reached its maximum number of retries. If
	// so, fail the batch and don't reschedule the query.
	if !info.batch.noRetryMax && job.tries >= info.batch.maxRetries {
		log.Warnf("Query(%d) from peer %v failed and reached maximum "+
			"number of retries, not rescheduling: %v "+
			"(active_for=%v, task_age=%v, tries=%v)", job.index,
			peerID, info.result.err, info.activeDuration,
			info.taskAge, job.tries)

		d.cancelBatch(info.batchNum, info.result.err)
		log.Debugf("Canceled batch %v", info.batchNum)

		// Since we've reached this query's maximum number of retries,
		// now is the time to call the OnMaxTries callback function if
		// it isn't nil.
		if d.cfg.OnMaxTries != nil {
			d.cfg.OnMaxTries(info.result.peer)
		}

		return
	}

	log.Warnf("Query(%d) from peer %v failed, rescheduling: %v "+
		"(active_for=%v, task_age=%v, tries=%v, timeout=%v)",
		job.index, peerID, info.result.err, info.activeDuration,
		info.taskAge, job.tries, job.timeout)

	if job.failedPeers == nil {
		job.failedPeers = make(map[string]struct{})
	}
	job.failedPeers[peerID] = struct{}{}

	// If it was a timeout, we dynamically increase it for the next
	// attempt.
	if info.result.err == ErrQueryTimeout {
		job.timeout = min(job.timeout*2, maxQueryTimeout)
	}

	d.work.Push(job)
	d.queries[job.index] = info.batchNum
}

// trackNonResponse counts a failure that looks like the peer not answering.
// After enough consecutive ones the peer is quarantined, and the caller is
// told so it can disconnect the peer. It reports whether the peer was
// quarantined by this failure, and its consecutive failure count.
func (d *dispatcher) trackNonResponse(info *resultInfo) (bool, uint8) {
	if !isNonResponseErr(info.result.err) {
		return false, 0
	}

	peerID := info.peerAddr()
	failures := d.nonResponseFailures[peerID] + 1
	d.nonResponseFailures[peerID] = failures

	if failures < nonResponsivePeerFailures {
		return false, failures
	}

	quarantineUntil := info.at.Add(nonResponsivePeerCooldown)
	d.quarantinedPeers[peerID] = quarantineUntil
	delete(d.nonResponseFailures, peerID)

	// The peer's unstarted work must not wait behind a peer we will not
	// use again for a while.
	if info.worker != nil {
		info.worker.quarantine = quarantineUntil
		d.releaseLocalWork(info.worker)
	}

	log.Warnf("Peer %v failed to respond to %v queries; quarantining "+
		"until %v (job=%v, active_for=%v, task_age=%v)", peerID,
		nonResponsivePeerFailures, quarantineUntil,
		info.result.job.index, info.activeDuration, info.taskAge)

	d.emit(querysync.SchedulerEvent{
		Type:           querysync.SchedulerEventPeerQuarantined,
		At:             info.at,
		PeerID:         peerID,
		State:          querysync.PeerQuarantined,
		TaskID:         querysync.TaskID(info.result.job.index),
		BatchID:        info.batchNum,
		Err:            errString(info.result.err),
		TaskAge:        info.taskAge,
		ActiveDuration: info.activeDuration,
		FailureCount:   failures,
		Until:          quarantineUntil,
		OK:             true,
	})

	if d.cfg.OnPeerUnresponsive != nil {
		d.cfg.OnPeerUnresponsive(info.result.peer)
	}

	return true, failures
}

// coolDownPeer puts the peer that failed a job into cooldown, so other peers
// are preferred for a while. For failures that mean the peer is not answering,
// the cooldown is hard, and the peer's unstarted work moves back to the global
// queue immediately.
func (d *dispatcher) coolDownPeer(info *resultInfo, quarantined bool,
	failureCount uint8) {

	worker := info.worker
	if worker == nil {
		return
	}

	cooldownUntil := info.at.Add(failedPeerCooldown)
	worker.cooldown = cooldownUntil
	if isHardCooldownErr(info.result.err) {
		worker.hardCooldown = cooldownUntil
	}

	if !quarantined && isHardCooldownErr(info.result.err) {
		d.releaseLocalWork(worker)
	}

	d.emit(querysync.SchedulerEvent{
		Type:             querysync.SchedulerEventPeerCooldown,
		At:               info.at,
		PeerID:           info.peerAddr(),
		State:            querysync.PeerBlocked,
		TaskID:           querysync.TaskID(info.result.job.index),
		BatchID:          info.batchNum,
		Err:              errString(info.result.err),
		TaskAge:          info.taskAge,
		ActiveDuration:   info.activeDuration,
		FailureCount:     failureCount,
		CooldownDuration: failedPeerCooldown,
		Until:            cooldownUntil,
		OK:               true,
	})
}
