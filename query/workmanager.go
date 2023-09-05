package query

import (
	"container/heap"
	"errors"
	"sync"
	"time"
)

const (
	// minQueryTimeout is the timeout a query will be initially given. If
	// the peer given the query fails to respond within the timeout, it
	// will be given to the next peer with an increased timeout.
	minQueryTimeout = 2 * time.Second

	// maxQueryTimeout is the maximum timeout given to a single query.
	maxQueryTimeout = 32 * time.Second
)

var (
	// ErrWorkManagerShuttingDown will be returned in case the WorkManager
	// is in the process of exiting.
	ErrWorkManagerShuttingDown = errors.New("WorkManager shutting down")
)

type batch struct {
	requests []*Request
	options  *queryOptions
}

// Worker is the interface that must be satisfied by workers managed by the
// WorkManager.
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

	// IsPeerBehindStartHeight returns a boolean indicating if the peer's known last height is behind
	// the request's start Height which it receives as an argument.
	IsPeerBehindStartHeight(req ReqMessage) bool

	// IsSyncCandidate returns if the peer is a sync candidate.
	IsSyncCandidate() bool
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

	// Order sorst the slice of peers according to their ranking.
	Order(peers []string)
}

// activeWorker wraps a Worker that is currently running, together with the job
// we have given to it.
// TODO(halseth): support more than one active job at a time.
type activeWorker struct {
	w         Worker
	activeJob *queryJob
	onExit    chan struct{}
}

// Config holds the configuration options for a new WorkManager.
type Config struct {
	// ConnectedPeers is a function that returns a channel where all
	// connected peers will be sent. It is assumed that all current peers
	// will be sent imemdiately, and new peers as they connect.
	//
	// The returned function closure is called to cancel the subscription.
	ConnectedPeers func() (<-chan Peer, func(), error)

	// NewWorker is function closure that should start a new worker. We
	// make this configurable to easily mock the worker used during tests.
	NewWorker func(Peer) Worker

	// Ranking is used to rank the connected peers when determining who to
	// give work to.
	Ranking PeerRanking

	// IsEligibleWorkerFunc determines which peer is eligible to receive a job.
	IsEligibleWorkerFunc func(r *activeWorker, next *queryJob) bool
}

// peerWorkManager is the main access point for outside callers, and satisfies
// the QueryAccess API. It receives queries to pass to peers, and schedules them
// among available workers, orchestrating where to send them. It implements the
// WorkManager interface.
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

// Compile time check to ensure peerWorkManager satisfies the WorkManager interface.
var _ WorkManager = (*peerWorkManager)(nil)

// NewWorkManager returns a new WorkManager with the regular worker
// implementation.
func NewWorkManager(cfg *Config) WorkManager {
	return &peerWorkManager{
		cfg:        cfg,
		newBatches: make(chan *batch),
		jobResults: make(chan *jobResult),
		quit:       make(chan struct{}),
	}
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
// callers, and dispatches these to active workers.  It makes sure to
// prioritize the queries in the order they come in, such that early queries
// will be attempted completed first.
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

	// Init a work queue which will be used to sort the incoming queries in
	// a first come first served fashion. We use a heap structure such
	// that we can efficiently put failed queries back in the queue.
	work := &workQueue{}
	heap.Init(work)

	type batchProgress struct {
		noRetryMax bool
		maxRetries uint8
		timeout    <-chan time.Time
		rem        int
		errChan    chan error
	}

	// We set up a batch index counter to keep track of batches that still
	// have queries in flight. This lets us track when all queries for a
	// batch have been finished, and return an (non-)error to the caller.
	batchIndex := uint64(0)
	currentBatches := make(map[uint64]*batchProgress)

	// When the work dispatcher exits, we'll loop through the remaining
	// batches and send on their error channel.
	defer func() {
		for _, b := range currentBatches {
			if b.errChan != nil {
				b.errChan <- ErrWorkManagerShuttingDown
			}
		}
	}()

	// We set up a counter that we'll increase with each incoming query,
	// and will serve as the priority of each. In addition we map each
	// query to the batch they are part of.
	queryIndex := float64(0)
	currentQueries := make(map[float64]uint64)

	workers := make(map[string]*activeWorker)

Loop:
	for {
		// If the work queue is non-empty, we'll take out the first
		// element in order to distribute it to a worker.
		if work.Len() > 0 {
			next := work.Peek().(*queryJob)

			// Find the peers with free work slots available.
			var freeEligibleWorkers []string
			for p, r := range workers {
				// Only one active job at a time is currently
				// supported.
				if r.activeJob != nil {
					continue
				}

				// If there is a specified eligibilty function for
				// the peer, use it to determine which peers we can
				// send jobs to.
				if w.cfg.IsEligibleWorkerFunc != nil {
					if !w.cfg.IsEligibleWorkerFunc(r, next) {
						continue
					}
				}

				freeEligibleWorkers = append(freeEligibleWorkers, p)
			}

			// Use the historical data to rank them.
			w.cfg.Ranking.Order(freeEligibleWorkers)

			// Give the job to the highest ranked peer with free
			// slots available.
			for _, p := range freeEligibleWorkers {
				r := workers[p]

				// The worker has free work slots, it should
				// pick up the query.
				select {
				case r.w.NewJob() <- next:
					log.Tracef("Sent job %v to worker %v",
						next.Index(), p)
					heap.Pop(work)
					r.activeJob = next

					// Go back to start of loop, to check
					// if there are more jobs to
					// distribute.
					continue Loop

				// Remove workers no longer active.
				case <-r.onExit:
					delete(workers, p)
					continue

				case <-w.quit:
					return
				}
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

			// We'll create a channel that will close after the
			// worker's Run method returns, to know when we can
			// remove it from our set of active workers.
			onExit := make(chan struct{})
			workers[peer.Addr()] = &activeWorker{
				w:         r,
				activeJob: nil,
				onExit:    onExit,
			}

			w.cfg.Ranking.AddPeer(peer.Addr())

			w.wg.Add(1)
			go func() {
				defer w.wg.Done()
				defer close(onExit)

				r.Run(w.jobResults, w.quit)
			}()

		// A new result came back.
		case result := <-w.jobResults:
			log.Tracef("Result for job %v received from peer %v "+
				"(err=%v)", result.job.index,
				result.peer.Addr(), result.err)

			// Delete the job from the worker's active job, such
			// that the slot gets opened for more work.
			r := workers[result.peer.Addr()]
			if result.err != ErrQueryTimeout {
				r.activeJob = nil
			}

			// Get the index of this query's batch, and delete it
			// from the map of current queries, since we don't have
			// to track it anymore. We'll add it back if the result
			// turns out to be an error.
			batchNum := currentQueries[result.job.index]
			delete(currentQueries, result.job.index)
			batch := currentBatches[batchNum]

			switch {
			// If the query ended because it was canceled, drop it.
			case result.err == ErrJobCanceled:
				log.Tracef("Query(%d) was canceled before "+
					"result was available from peer %v",
					result.job.index, result.peer.Addr())

				// If this is the first job in this batch that
				// was canceled, forward the error on the
				// batch's error channel.  We do this since a
				// cancellation applies to the whole batch.
				if batch != nil {
					if batch.errChan != nil {
						batch.errChan <- result.err
					}
					delete(currentBatches, batchNum)

					log.Debugf("Canceled batch %v",
						batchNum)
					continue Loop
				}
				//	Take no action if we are to ignore request.
			case result.err == ErrIgnoreRequest:
				log.Debugf("received ignore request")
				continue Loop

			// If the query ended with any other error, put it back
			// into the work queue if it has not reached the
			// maximum number of retries.
			case result.err != nil:
				// Punish the peer for the failed query.
				w.cfg.Ranking.Punish(result.peer.Addr())

				if batch != nil && !batch.noRetryMax {
					result.job.tries++
				}

				// Check if this query has reached its maximum
				// number of retries. If so, remove it from the
				// batch and don't reschedule it.
				if batch != nil && !batch.noRetryMax &&
					result.job.tries >= batch.maxRetries {

					log.Warnf("Query(%d) from peer %v "+
						"failed and reached maximum "+
						"number of retries, not "+
						"rescheduling: %v",
						result.job.index,
						result.peer.Addr(), result.err)

					// Return the error and cancel the
					// batch.
					if batch.errChan != nil {
						batch.errChan <- result.err
					}
					delete(currentBatches, batchNum)

					log.Debugf("Canceled batch %v",
						batchNum)

					continue Loop
				}

				log.Warnf("Query(%d) from peer %v failed, "+
					"rescheduling: %v", result.job.index,
					result.peer.Addr(), result.err)

				// If it was a timeout, we dynamically increase
				// it for the next attempt.
				if result.err == ErrQueryTimeout {
					newTimeout := result.job.timeout * 2
					if newTimeout > maxQueryTimeout {
						newTimeout = maxQueryTimeout
					}
					result.job.timeout = newTimeout
				}

				heap.Push(work, result.job)
				currentQueries[result.job.index] = batchNum

			// Otherwise, we got a successful result and update the
			// status of the batch this query is a part of.
			default:
				// Reward the peer for the successful query.
				w.cfg.Ranking.Reward(result.peer.Addr())

				// If the result is unfinished add 0.0005 to the job index to maintain the
				// required priority then push to work queue
				if result.unfinished {
					result.job.index = result.job.Index() + 0.0005
					log.Debugf("job %v is unfinished, creating new index", result.job.Index())

					heap.Push(work, result.job)
					batch.rem++
					currentQueries[result.job.Index()] = batchNum
				} else {
					log.Debugf("job %v is Finished", result.job.Index())
				}

				// Decrement the number of queries remaining in
				// the batch.
				if batch != nil {
					batch.rem--
					log.Tracef("Remaining jobs for batch "+
						"%v: %v ", batchNum, batch.rem)

					// If this was the last query in flight
					// for this batch, we can notify that
					// it finished, and delete it.
					if batch.rem == 0 {
						if batch.errChan != nil {
							batch.errChan <- nil
						}
						delete(currentBatches, batchNum)

						log.Tracef("Batch %v done",
							batchNum)
						continue Loop
					}
				}
			}

			// If the total timeout for this batch has passed,
			// return an error.
			if batch != nil {
				select {
				case <-batch.timeout:
					if batch.errChan != nil {
						batch.errChan <- ErrQueryTimeout
					}
					delete(currentBatches, batchNum)

					log.Warnf("Query(%d) failed with "+
						"error: %v. Timing out.",
						result.job.index, result.err)

					log.Debugf("Batch %v timed out",
						batchNum)

				default:
				}
			}

		// A new batch of queries where scheduled.
		case batch := <-w.newBatches:
			// Add all new queries in the batch to our work queue,
			// with priority given by the order they were
			// scheduled.
			log.Debugf("Adding new batch(%d) of %d queries to "+
				"work queue", batchIndex, len(batch.requests))

			for _, q := range batch.requests {
				idx := queryIndex

				// If priority index is set, use that index.
				if q.Req.PriorityIndex() != 0 {
					idx = q.Req.PriorityIndex()
				}
				heap.Push(work, &queryJob{
					index:      idx,
					timeout:    minQueryTimeout,
					cancelChan: batch.options.cancelChan,
					Request:    q,
				})
				currentQueries[idx] = batchIndex

				// Only increment queryIndex if it was
				// assigned to this job.
				if q.Req.PriorityIndex() == 0 {
					queryIndex++
				}
			}

			currentBatches[batchIndex] = &batchProgress{
				noRetryMax: batch.options.noRetryMax,
				maxRetries: batch.options.numRetries,
				timeout:    time.After(batch.options.timeout),
				rem:        len(batch.requests),
				errChan:    batch.options.errChan,
			}
			batchIndex++

		case <-w.quit:
			return
		}
	}
}

// Query distributes the slice of requests to the set of connected peers.
//
// NOTE: this is part of the WorkManager interface.
func (w *peerWorkManager) Query(requests []*Request,
	options ...QueryOption) chan error {

	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)

	newBatch := &batch{
		requests: requests,
		options:  qo,
	}

	// Add query messages to the queue of batches to handle.
	select {
	case w.newBatches <- newBatch:
	case <-w.quit:
		if newBatch.options.errChan != nil {
			newBatch.options.errChan <- ErrWorkManagerShuttingDown
		}
	}

	return newBatch.options.errChan
}
