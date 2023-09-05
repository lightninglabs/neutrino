package query

import (
	"errors"
	"time"
)

var (
	// ErrQueryTimeout is an error returned if the worker doesn't respond
	// with a valid response to the request within the timeout.
	ErrQueryTimeout = errors.New("did not get response before timeout")

	// ErrPeerDisconnected is returned if the worker's peer disconnect
	// before the query has been answered.
	ErrPeerDisconnected = errors.New("peer disconnected")

	// ErrJobCanceled is returned if the job is canceled before the query
	// has been answered.
	ErrJobCanceled = errors.New("job canceled")

	// ErrIgnoreRequest is returned if we want to ignore the request after getting
	// a response.
	ErrIgnoreRequest = errors.New("ignore request")

	// ErrResponseErr is returned if we received a compatible response for the query but, it did not pass
	// preliminary verification.
	ErrResponseErr = errors.New("received response with error")
)

// queryJob is the internal struct that wraps the Query to work on, in
// addition to some information about the query.
type queryJob struct {
	tries      uint8
	index      float64
	timeout    time.Duration
	cancelChan <-chan struct{}
	*Request
}

// queryJob should satisfy the Task interface in order to be sorted by the
// workQueue.
var _ Task = (*queryJob)(nil)

// Index returns the queryJob's index within the work queue.
//
// NOTE: Part of the Task interface.
func (q *queryJob) Index() float64 {
	return q.index
}

// jobResult is the final result of the worker's handling of the queryJob.
type jobResult struct {
	job        *queryJob
	peer       Peer
	err        error
	unfinished bool
}

// worker is responsible for polling work from its work queue, and handing it
// to the associated peer. It validates incoming responses with the current
// query's response handler, and polls more work for the peer when it has
// successfully received a response to the request.
type worker struct {
	peer Peer

	// nextJob is a channel of queries to be distributed, where the worker
	// will poll new work from.
	nextJob chan *queryJob
}

// A compile-time check to ensure worker satisfies the Worker interface.
var _ Worker = (*worker)(nil)

// NewWorker creates a new worker associated with the given peer.
func NewWorker(peer Peer) Worker {
	return &worker{
		peer:    peer,
		nextJob: make(chan *queryJob),
	}
}

// Run starts the worker. The worker will supply its peer with queries, and
// handle responses from it. Results for any query handled by this worker will
// be delivered on the results channel. quit can be closed to immediately make
// the worker exit.
//
// The method is blocking, and should be started in a goroutine. It will run
// until the peer disconnects or the worker is told to quit.
//
// NOTE: Part of the Worker interface.
func (w *worker) Run(results chan<- *jobResult, quit <-chan struct{}) {
	peer := w.peer

	// Subscribe to messages from the peer.
	msgChan, cancel := peer.SubscribeRecvMsg()
	defer cancel()

nextJobLoop:
	for {
		log.Tracef("Worker %v waiting for more work", peer.Addr())

		var job *queryJob
		select {
		// Poll a new job from the nextJob channel.
		case job = <-w.nextJob:
			log.Tracef("Worker %v picked up job with index %v",
				peer.Addr(), job.Index())

		// Ignore any message received while not working on anything.
		case msg := <-msgChan:
			log.Tracef("Worker %v ignoring received msg %T "+
				"since no job active", peer.Addr(), msg)
			continue

		// If the peer disconnected, we can exit immediately, as we
		// weren't working on a query.
		case <-peer.OnDisconnect():
			log.Debugf("Peer %v for worker disconnected",
				peer.Addr())
			return

		case <-quit:
			return
		}

		select {
		// There is no point in queueing the request if the job already
		// is canceled, so we check this quickly.
		case <-job.cancelChan:
			log.Tracef("Worker %v found job with index %v "+
				"already canceled", peer.Addr(), job.Index())

			// We break to the below loop, where we'll check the
			// cancel channel again and the ErrJobCanceled
			// result will be sent back.
			break

		// We received a non-canceled query job, send it to the peer.
		default:
			log.Tracef("Worker %v queuing job %T with index %v",
				peer.Addr(), job.Req, job.Index())

			err := job.SendQuery(peer, job.Req)

			// If any error occurs while sending query, quickly send the result
			// containing the error to the workmanager.
			if err != nil {
				select {
				case results <- &jobResult{
					job:  job,
					peer: peer,
					err:  err,
				}:
				case <-quit:
					return
				}
				goto nextJobLoop
			}
		}

		// Wait for the correct response to be received from the peer,
		// or an error happening.
		var (
			jobErr        error
			jobUnfinished bool
			timeout       = time.NewTimer(job.timeout)
		)

	feedbackLoop:
		for {
			select {
			// A message was received from the peer, use the
			// response handler to check whether it was answering
			// our request.
			case resp := <-msgChan:
				progress := job.HandleResp(
					job.Req, resp, peer,
				)

				log.Tracef("Worker %v handled msg %T while "+
					"waiting for response to %T (job=%v). ",
					peer.Addr(), resp, job.Req, job.Index())

				switch {
				case progress == Finished:

				//	Wait for valid response if we have not gotten any one yet.
				case progress == NoResponse:

					continue feedbackLoop

				//	Increase job's timeout if valid response has been received, and we
				// are awaiting more to prevent premature timeout.
				case progress == Progressed:

					timeout.Stop()
					timeout = time.NewTimer(
						job.timeout,
					)

					continue feedbackLoop

				//	Assign true to jobUnfinished to indicate that we need to reschedule job to complete request.
				case progress == UnFinishedRequest:

					jobUnfinished = true

				//	Assign ErrIgnoreRequest to indicate that workmanager should take no action on receipt of
				// this request.
				case progress == IgnoreRequest:

					jobErr = ErrIgnoreRequest

				//	Assign ErrResponseErr to jobErr if we received a valid response that did not pass checks.
				case progress == ResponseErr:

					jobErr = ErrResponseErr
				}

				break feedbackLoop

			// If the timeout is reached before a valid response
			// has been received, we exit with an error.
			case <-timeout.C:
				// The query did experience a timeout and will
				// be given to someone else.
				jobErr = ErrQueryTimeout
				log.Tracef("Worker %v timeout for request %T "+
					"with job index %v", peer.Addr(),
					job.Req, job.Index())

				break feedbackLoop

			// If the peer disconnects before giving us a valid
			// answer, we'll also exit with an error.
			case <-peer.OnDisconnect():
				log.Debugf("Peer %v for worker disconnected, "+
					"cancelling job %v", peer.Addr(),
					job.Index())

				jobErr = ErrPeerDisconnected
				break feedbackLoop

			// If the job was canceled, we report this back to the
			// work manager.
			case <-job.cancelChan:
				log.Tracef("Worker %v job %v canceled",
					peer.Addr(), job.Index())

				jobErr = ErrJobCanceled
				break feedbackLoop

			case <-quit:
				return
			}
		}

		// Stop to allow garbage collection.
		timeout.Stop()

		// This is necessary to avoid a situation where future changes to the job's request affect the current job.
		// For example: suppose we want to fetch headers between checkpoints 0 and 20,000. The maximum number of headers
		// that a peer can send in one message is 2000. When we receive 2000 headers for one request,
		// we update the job's request, changing its startheight and blocklocator to match the next batch of headers
		// that we want to fetch. Since we are not done with fetching our target of 20,000 headers,
		// we will have to make more changes to the job's request in the future. This could alter previous requests,
		// resulting in unwanted behaviour.
		resultJob := &queryJob{
			index: job.Index(),
			Request: &Request{
				Req:        job.CloneReq(job.Req),
				HandleResp: job.Request.HandleResp,
				CloneReq:   job.Request.CloneReq,
				SendQuery:  job.Request.SendQuery,
			},
			cancelChan: job.cancelChan,
			tries:      job.tries,
			timeout:    job.timeout,
		}

		// We have a result ready for the query, hand it off before
		// getting a new job.
		select {
		case results <- &jobResult{
			job:        resultJob,
			peer:       peer,
			err:        jobErr,
			unfinished: jobUnfinished,
		}:
		case <-quit:
			return
		}

		// If the error is a timeout still wait for the response as we are assured a response as long as there was a
		// request but reschedule on another worker to quickly fetch a response so as not to be slowed down by this
		// worker. We either get a response or the peer stalls (i.e. disconnects due to an elongated time without
		// a response)
		if jobErr == ErrQueryTimeout {
			jobErr = nil

			goto feedbackLoop
		}

		// If the peer disconnected, we can exit immediately.
		if jobErr == ErrPeerDisconnected {
			return
		}
	}
}

func (w *worker) IsSyncCandidate() bool {
	return w.peer.IsSyncCandidate()
}

func (w *worker) IsPeerBehindStartHeight(req ReqMessage) bool {
	return w.peer.IsPeerBehindStartHeight(req)
}

// IsWorkerEligibleForBlkHdrFetch is the eligibility function used for the BlockHdrWorkManager to determine workers
// eligible to receive jobs (the job is to fetch headers). If the peer is not a sync candidate or if its last known
// block height is behind the job query's start height, it returns false. Otherwise, it returns true.
func IsWorkerEligibleForBlkHdrFetch(r *activeWorker, next *queryJob) bool {
	if !r.w.IsSyncCandidate() {
		return false
	}

	if r.w.IsPeerBehindStartHeight(next.Req) {
		return false
	}
	return true
}

// NewJob returns a channel where work that is to be handled by the worker can
// be sent. If the worker reads a queryJob from this channel, it is guaranteed
// that a response will eventually be deliverd on the results channel (except
// when the quit channel has been closed).
//
// NOTE: Part of the Worker interface.
func (w *worker) NewJob() chan<- *queryJob {
	return w.nextJob
}
