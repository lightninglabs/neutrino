package query

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire"
)

type mockQueryEncoded struct {
	message     *wire.MsgGetData
	encoding    wire.MessageEncoding
	index       float64
	startHeight int
}

func (m *mockQueryEncoded) Message() wire.Message {
	return m.message
}

func (m *mockQueryEncoded) PriorityIndex() float64 {
	return m.index
}

var (
	msg = &wire.MsgGetData{}
	req = &mockQueryEncoded{
		message:  msg,
		encoding: wire.WitnessEncoding,
	}
	progressResp = &wire.MsgTx{
		Version: 111,
	}
	finalResp = &wire.MsgTx{
		Version: 222,
	}
	UnfinishedRequestResp = &wire.MsgTx{
		Version: 333,
	}
	finalRespWithErr = &wire.MsgTx{
		Version: 444,
	}
	IgnoreRequestResp = &wire.MsgTx{
		Version: 444,
	}
)

type mockPeer struct {
	addr          string
	requests      chan wire.Message
	responses     chan<- wire.Message
	subscriptions chan chan wire.Message
	quit          chan struct{}
	bestHeight    int
	fullNode      bool
	err           error
}

var _ Peer = (*mockPeer)(nil)

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

func (m *mockPeer) IsPeerBehindStartHeight(request ReqMessage) bool {
	r := request.(*mockQueryEncoded)
	return m.bestHeight < r.startHeight
}

func (m *mockPeer) IsSyncCandidate() bool {
	return m.fullNode
}

// makeJob returns a new query job that will be done when it is given the
// finalResp message. Similarly ot will progress on being given the
// progressResp message, while any other message will be ignored.
func makeJob() *queryJob {
	q := &Request{
		Req: req,
		HandleResp: func(req ReqMessage, resp wire.Message, peer Peer) Progress {
			if resp == finalResp {
				return Finished
			}

			if resp == progressResp {
				return Progressed
			}

			if resp == UnfinishedRequestResp {
				return UnFinishedRequest
			}

			if resp == finalRespWithErr {
				return ResponseErr
			}
			if resp == IgnoreRequestResp {
				return IgnoreRequest
			}
			return NoResponse
		},
		SendQuery: func(peer Peer, req ReqMessage) error {
			m := peer.(*mockPeer)

			if m.err != nil {
				return m.err
			}

			m.requests <- req.Message()
			return nil
		},
		CloneReq: func(req ReqMessage) ReqMessage {
			oldReq := req.(*mockQueryEncoded)

			newMsg := &wire.MsgGetData{
				InvList: oldReq.message.InvList,
			}

			clone := &mockQueryEncoded{
				message: newMsg,
			}

			return clone
		},
	}

	return &queryJob{
		index:      123,
		timeout:    30 * time.Second,
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

	// Make sure the QueryJob instance in the result is different from the initial one
	// supplied to the worker
	if result.job == task {
		t.Fatalf("result's job should be different from the task's")
	}

	// Make sure we are receiving the corresponding result for the given task.
	if result.job.Index() != task.Index() {
		t.Fatalf("result's job index should not be different from task's")
	}

	// Make sure job does not return as unfinished.
	if result.unfinished {
		t.Fatalf("got unfinished job")
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

	// Make sure the QueryJob instance in the result is different from the initial one
	// supplied to the worker
	if result.job == task {
		t.Fatalf("result's job should be different from the task's")
	}

	// Make sure we are receiving the corresponding result for the given task.
	if result.job.Index() != task.Index() {
		t.Fatalf("result's job index should not be different from task's")
	}

	// And the correct peer.
	if result.peer != ctx.peer {
		t.Fatalf("expected peer to be %v, was %v",
			ctx.peer.Addr(), result.peer)
	}

	// Make sure job does not return as unfinished.
	if result.unfinished {
		t.Fatalf("got unfinished job")
	}

	// It will immediately attempt to fetch another task.
	select {
	case ctx.nextJob <- task:
		t.Fatalf("worker still in feedback loop picked up job")
	case <-time.After(1 * time.Second):
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

	// Make sure the QueryJob instance in the result is different from the initial one
	// supplied to the worker
	if result.job == task {
		t.Fatalf("result's job should be different from the task's")
	}

	// Make sure we are receiving the corresponding result for the given task.
	if result.job.Index() != task.Index() {
		t.Fatalf("result's job index should not be different from task's")
	}

	// And the correct peer.
	if result.peer != ctx.peer {
		t.Fatalf("expected peer to be %v, was %v",
			ctx.peer.Addr(), result.peer)
	}

	// Make sure job does not return as unfinished.
	if result.unfinished {
		t.Fatalf("got unfinished job")
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
	type testResp struct {
		name       string
		response   *wire.MsgTx
		err        *error
		unfinished bool
	}

	testCases := []testResp{

		{
			name:     "final response.",
			response: finalResp,
		},

		{
			name:       "Unfinished request response.",
			response:   UnfinishedRequestResp,
			unfinished: true,
		},

		{
			name:     "ignore request",
			response: IgnoreRequestResp,
			err:      &ErrIgnoreRequest,
		},

		{
			name:     "final response, with err",
			response: finalRespWithErr,
			err:      &ErrResponseErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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
			case ctx.peer.responses <- tc.response:
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

			if tc.err == nil && result.err != nil {
				t.Fatalf("expected no error, got: %v", result.err)
			}

			if tc.err != nil && result.err != *tc.err {
				t.Fatalf("expected error, %v but got: %v", *tc.err,
					result.err)
			}

			// Make sure the QueryJob instance in the result is different from the initial one
			// supplied to the worker
			if result.job == task {
				t.Fatalf("result's job should be different from task's")
			}

			// Make sure we are receiving the corresponding result for the given task.
			if result.job.Index() != task.Index() {
				t.Fatalf("result's job index should not be different from task's")
			}

			// And the correct peer.
			if result.peer != ctx.peer {
				t.Fatalf("expected peer to be %v, was %v",
					ctx.peer.Addr(), result.peer)
			}

			// Make sure job does not return as unfinished.
			if tc.unfinished && !result.unfinished {
				t.Fatalf("expected job unfinished but got job finished")
			}

			if !tc.unfinished && result.unfinished {
				t.Fatalf("expected job finished but got unfinished job")
			}
		})
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

		// Make sure the QueryJob instance in the result is different from the initial one
		// supplied to the worker
		if result.job == task {
			t.Fatalf("result's job should be different from the task's")
		}

		// Make sure we are receiving the corresponding result for the given task.
		if result.job.Index() != task.Index() {
			t.Fatalf("result's job index should not be different from task's")
		}

		// Make sure job does not return as unfinished.
		if result.unfinished {
			t.Fatalf("got unfinished job")
		}

		// And the correct peer.
		if result.peer != ctx.peer {
			t.Fatalf("expected peer to be %v, was %v",
				ctx.peer.Addr(), result.peer)
		}
	}
}

// TestWorkerSendQueryErr will test if the result would return an error
// that would be handled by the worker if there is an error returned while
// sending a query.
func TestWorkerSendQueryErr(t *testing.T) {
	t.Parallel()

	ctx, err := startWorker()
	if err != nil {
		t.Fatalf("unable to start worker: %v", err)
	}

	cancelChan := make(chan struct{})

	// Give the worker a new job.
	taskJob := makeJob()
	taskJob.cancelChan = cancelChan

	// Assign error to be returned while sending query.
	ctx.peer.err = errors.New("query error")

	// Send job to worker
	select {
	case ctx.nextJob <- taskJob:
	case <-time.After(1 * time.Second):
		t.Fatalf("did not pick up job")
	}

	// Request should not be sent as there should be an error while
	// querying.
	select {
	case <-ctx.peer.requests:
		t.Fatalf("request sent when query failed")
	case <-time.After(time.Second):
	}

	// jobResult should be sent by worker at this point.
	var result *jobResult
	select {
	case result = <-ctx.jobResults:
	case <-time.After(time.Second):
		t.Fatalf("response not received")
	}

	// jobResult should contain error.
	if result.err != ctx.peer.err {
		t.Fatalf("expected result's error to be %v, was %v",
			ctx.peer.err, result.err)
	}

	// Make sure the QueryJob instance in the result is same as the taskJob's.
	if result.job != taskJob {
		t.Fatalf("result's job should be same as the taskJob's")
	}

	// And the correct peer.
	if result.peer != ctx.peer {
		t.Fatalf("expected peer to be %v, was %v",
			ctx.peer.Addr(), result.peer)
	}

	// The worker should be in the nextJob Loop.
	select {
	case ctx.nextJob <- taskJob:
	case <-time.After(1 * time.Second):
		t.Fatalf("did not pick up job")
	}
}
