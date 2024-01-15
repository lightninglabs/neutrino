package query

import (
	"fmt"
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire"
)

var (
	req          = &wire.MsgGetData{}
	progressResp = &wire.MsgTx{
		Version: 111,
	}
	finalResp = &wire.MsgTx{
		Version: 222,
	}
)

type mockPeer struct {
	addr          string
	requests      chan wire.Message
	responses     chan<- wire.Message
	subscriptions chan chan wire.Message
	quit          chan struct{}
}

var _ Peer = (*mockPeer)(nil)

func (m *mockPeer) QueueMessageWithEncoding(msg wire.Message,
	doneChan chan<- struct{}, encoding wire.MessageEncoding) {

	m.requests <- msg
}

func (m *mockPeer) SubscribeRecvMsg() (<-chan wire.Message, func()) {
	msgChan := make(chan wire.Message)
	m.subscriptions <- msgChan

	return msgChan, func() {}
}

func (m *mockPeer) OnDisconnect() <-chan struct{} {
	return m.quit
}

func (m *mockPeer) Addr() string {
	return m.addr
}

// makeJob returns a new query job that will be done when it is given the
// finalResp message. Similarly it will progress on being given the
// progressResp message, while any other message will be ignored.
func makeJob() *queryJob {
	q := &Request{
		Req: req,
		HandleResp: func(req, resp wire.Message, _ string) Progress {
			if resp == finalResp {
				return Progress{
					Finished:   true,
					Progressed: true,
				}
			}

			if resp == progressResp {
				return Progress{
					Finished:   false,
					Progressed: true,
				}
			}

			return Progress{
				Finished:   false,
				Progressed: false,
			}
		},
	}
	return &queryJob{
		index:      123,
		timeout:    30 * time.Second,
		encoding:   defaultQueryEncoding,
		cancelChan: nil,
		Request:    q,
	}
}

type testCtx struct {
	nextJob    chan<- *queryJob
	jobResults chan *jobResult
	peer       *mockPeer
	workerDone chan struct{}
}

// startWorker creates and starts a worker for a new mockPeer. A test context
// containing channels to hand the worker new jobs and receiving the job
// results, in addition to the mockPeer, is returned.
func startWorker() (*testCtx, error) {
	peer := &mockPeer{
		requests:      make(chan wire.Message),
		subscriptions: make(chan chan wire.Message),
		quit:          make(chan struct{}),
	}
	results := make(chan *jobResult)
	quit := make(chan struct{})

	wk := NewWorker(peer)

	// Start worker.
	done := make(chan struct{})
	go func() {
		defer close(done)
		wk.Run(results, quit)
	}()

	// Wait for it to subscribe to peer messages.
	var sub chan wire.Message
	select {
	case sub = <-peer.subscriptions:
	case <-time.After(1 * time.Second):
		return nil, fmt.Errorf("did not subscribe to msgs")
	}
	peer.responses = sub

	return &testCtx{
		nextJob:    wk.NewJob(),
		jobResults: results,
		peer:       peer,
		workerDone: done,
	}, nil
}

// TestWorkerIgnoreMsgs tests that the worker handles being given the response
// to its query after first receiving some non-matching messages.
func TestWorkerIgnoreMsgs(t *testing.T) {
	t.Parallel()

	ctx, err := startWorker()
	if err != nil {
		t.Fatalf("unable to start worker: %v", err)
	}

	// Create a new task and give it to the worker.
	task := makeJob()

	select {
	case ctx.nextJob <- task:
	case <-time.After(1 * time.Second):
		t.Fatalf("did not pick up job")
	}

	// The request should be sent to the peer.
	select {
	case <-ctx.peer.requests:
	case <-time.After(time.Second):
		t.Fatalf("request not sent")
	}

	// First give the worker a few random responses. These will all be
	// ignored.
	for i := 0; i < 5; i++ {
		select {
		case ctx.peer.responses <- &wire.MsgTx{}:
		case <-time.After(time.Second):
			t.Fatalf("resp not received")
		}
	}

	// Answer the query with the correct response.
	select {
	case ctx.peer.responses <- finalResp:
	case <-time.After(time.Second):
		t.Fatalf("resp not received")
	}

	// The worker should respond with a job finished.
	var result *jobResult
	select {
	case result = <-ctx.jobResults:
	case <-time.After(time.Second):
		t.Fatalf("response not received")
	}

	if result.err != nil {
		t.Fatalf("response error: %v", result.err)
	}

	// Make sure the result was given for the intended job.
	if result.job != task {
		t.Fatalf("got result for unexpected job")
	}

	// And the correct peer.
	if result.peer != ctx.peer {
		t.Fatalf("expected peer to be %v, was %v",
			ctx.peer.Addr(), result.peer)
	}
}

// TestWorkerTimeout tests that the worker will eventually return a Timeout
// error if the query is not answered before the time limit.
func TestWorkerTimeout(t *testing.T) {
	t.Parallel()

	const timeout = 50 * time.Millisecond

	ctx, err := startWorker()
	if err != nil {
		t.Fatalf("unable to start worker: %v", err)
	}

	// Create a task with a small timeout.
	task := makeJob()
	task.timeout = timeout

	// Give the worker the new job.
	select {
	case ctx.nextJob <- task:
	case <-time.After(1 * time.Second):
		t.Fatalf("did not pick up job")
	}

	// The request should be given to the peer.
	select {
	case <-ctx.peer.requests:
	case <-time.After(time.Second):
		t.Fatalf("request not sent")
	}

	// Don't anwer the query. This should trigger a timeout, and the worker
	// should respond with an error result.
	var result *jobResult
	select {
	case result = <-ctx.jobResults:
	case <-time.After(time.Second):
		t.Fatalf("response not received")
	}

	if result.err != ErrQueryTimeout {
		t.Fatalf("expected timeout, got: %v", result.err)
	}

	// Make sure the result was given for the intended job.
	if result.job != task {
		t.Fatalf("got result for unexpected job")
	}

	// And the correct peer.
	if result.peer != ctx.peer {
		t.Fatalf("expected peer to be %v, was %v",
			ctx.peer.Addr(), result.peer)
	}

	// It will immediately attempt to fetch another task.
	select {
	case ctx.nextJob <- task:
	case <-time.After(1 * time.Second):
		t.Fatalf("did not pick up job")
	}
}

// TestWorkerDisconnect tests that the worker will return an error if the peer
// disconnects, and that the worker itself is then shut down.
func TestWorkerDisconnect(t *testing.T) {
	t.Parallel()

	ctx, err := startWorker()
	if err != nil {
		t.Fatalf("unable to start worker: %v", err)
	}

	// Give the worker a new job.
	task := makeJob()
	select {
	case ctx.nextJob <- task:
	case <-time.After(1 * time.Second):
		t.Fatalf("did not pick up job")
	}

	// The request should be given to the peer.
	select {
	case <-ctx.peer.requests:
	case <-time.After(time.Second):
		t.Fatalf("request not sent")
	}

	// Disconnect the peer.
	close(ctx.peer.quit)

	// The worker should respond with a job failure.
	var result *jobResult
	select {
	case result = <-ctx.jobResults:
	case <-time.After(time.Second):
		t.Fatalf("response not received")
	}

	if result.err != ErrPeerDisconnected {
		t.Fatalf("expected peer disconnect, got: %v", result.err)
	}

	// Make sure the result was given for the intended job.
	if result.job != task {
		t.Fatalf("got result for unexpected job")
	}

	// And the correct peer.
	if result.peer != ctx.peer {
		t.Fatalf("expected peer to be %v, was %v",
			ctx.peer.Addr(), result.peer)
	}

	// No more jobs should be accepted by the worker after it has exited.
	select {
	case ctx.nextJob <- task:
		t.Fatalf("exited worker did pick up job")
	default:
	}

	// Finally, make sure the worker go routine exits.
	select {
	case <-ctx.workerDone:
	case <-time.After(time.Second):
		t.Fatalf("worker did not exit")
	}
}

// TestWorkerProgress tests that the query won't timeout as long as it is
// making progress.
func TestWorkerProgress(t *testing.T) {
	t.Parallel()

	const taskTimeout = 50 * time.Millisecond

	ctx, err := startWorker()
	if err != nil {
		t.Fatalf("unable to start worker: %v", err)
	}

	// Create a task with a small timeout, and give it to the worker.
	task := makeJob()
	task.timeout = taskTimeout

	select {
	case ctx.nextJob <- task:
	case <-time.After(1 * time.Second):
		t.Fatalf("did not pick up job")
	}

	// The request should be given to the peer.
	select {
	case <-ctx.peer.requests:
	case <-time.After(time.Second):
		t.Fatalf("request not sent")
	}

	// Send a few other responses that indicates progress, but not success.
	// We add a small delay between each time we send a response. In total
	// the delay will be larger than the query timeout, but since we are
	// making progress, the timeout won't trigger.
	for i := 0; i < 5; i++ {
		select {
		case ctx.peer.responses <- progressResp:
		case <-time.After(time.Second):
			t.Fatalf("resp not received")
		}

		time.Sleep(taskTimeout / 2)
	}

	// Finally send the final response.
	select {
	case ctx.peer.responses <- finalResp:
	case <-time.After(time.Second):
		t.Fatalf("resp not received")
	}

	// The worker should respond with a job finished.
	var result *jobResult
	select {
	case result = <-ctx.jobResults:
	case <-time.After(time.Second):
		t.Fatalf("response not received")
	}

	if result.err != nil {
		t.Fatalf("expected no error, got: %v", result.err)
	}

	// Make sure the result was given for the intended task.
	if result.job != task {
		t.Fatalf("got result for unexpected job")
	}

	// And the correct peer.
	if result.peer != ctx.peer {
		t.Fatalf("expected peer to be %v, was %v",
			ctx.peer.Addr(), result.peer)
	}
}

// TestWorkerJobCanceled tests that the worker will return an error if the job is
// canceled while the worker is handling it.
func TestWorkerJobCanceled(t *testing.T) {
	t.Parallel()

	ctx, err := startWorker()
	if err != nil {
		t.Fatalf("unable to start worker: %v", err)
	}

	cancelChan := make(chan struct{})

	// Give the worker a new job.
	task := makeJob()
	task.cancelChan = cancelChan

	// We'll do two checks: first cancelling the job after it has been
	// given to the peer, then handing the already canceled job to the
	// worker.
	canceled := false
	for i := 0; i < 2; i++ {
		select {
		case ctx.nextJob <- task:
		case <-time.After(1 * time.Second):
			t.Fatalf("did not pick up job")
		}

		// If the request was not already canceled, it should be given
		// to the peer.
		if !canceled {
			select {
			case <-ctx.peer.requests:
			case <-time.After(time.Second):
				t.Fatalf("request not sent")
			}

			// Cancel the job.
			close(cancelChan)
			canceled = true
		} else {
			// If it was already canceled, it should not be given
			// to the peer.
			select {
			case <-ctx.peer.requests:
				t.Fatalf("canceled job was given to peer")
			case <-time.After(10 * time.Millisecond):
			}
		}

		// The worker should respond with a job failure.
		var result *jobResult
		select {
		case result = <-ctx.jobResults:
		case <-time.After(time.Second):
			t.Fatalf("response not received")
		}

		if result.err != ErrJobCanceled {
			t.Fatalf("expected job canceled, got: %v", result.err)
		}

		// Make sure the result was given for the intended task.
		if result.job != task {
			t.Fatalf("got result for unexpected job")
		}

		// And the correct peer.
		if result.peer != ctx.peer {
			t.Fatalf("expected peer to be %v, was %v",
				ctx.peer.Addr(), result.peer)
		}
	}
}
