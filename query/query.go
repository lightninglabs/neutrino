package query

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/wire"
)

type SpMsg struct {
	Peer string
	Msg  wire.Message
}

type MsgSubscription struct {
	MsgChan  chan<- SpMsg
	QuitChan <-chan struct{}
}

type Peer interface {
	QueueMessage(msg wire.Message, doneChan chan<- struct{})

	SubscribeRecvMsg(subscription MsgSubscription)
}

type Query struct {
	Req       wire.Message
	CheckResp func(req, resp wire.Message) bool
}

type queryTask struct {
	i int
	*Query
}

func (q *queryTask) Priority() int {
	return q.i
}

type jobResult struct {
	task *queryTask
	err  error
}

func QueryBatch(
	// s is the ChainService to use.
	//s *ChainService,
	peers []Peer,

	// queryMsgs is a slice of queries for which the caller wants responses.
	queries []*Query,

	// quit forces the query to end before it's complete.
	quit <-chan struct{},

	// options takes functional options for executing the query.
	//	options ...QueryOption
) chan error {

	// Add query messages to the task queue.
	//work
	work := &workQueue{}
	heap.Init(work)
	for i, q := range queries {
		heap.Push(work, &queryTask{
			i:     i,
			Query: q,
		})
	}

	// buffer.
	nextJob := make(chan *queryTask)
	responses := make(chan *jobResult)

	var peerWg sync.WaitGroup

	// Spin up a goroutine for each peer.
	for _, peer := range peers {

		w := &worker{
			peer:    peer,
			nextJob: nextJob,
			results: responses,
			quit:    quit,
		}

		peerWg.Add(1)
		go func() {
			defer peerWg.Done()
			w.start()
		}()
	}

	// Send tasks.
	log.Infof("johan starting work for %d queries", len(queries))

	// Gather responses.
	errChan := make(chan error)

	go func() {

		var inProgress int
		for {

			var (
				result *jobResult
			)

			switch {

			case work.Len() > 0:
				next := work.Peek().(*queryTask)

				select {
				case nextJob <- next:
					log.Infof("johan gave job %d", next.i)
					heap.Pop(work)
					inProgress++

				case result = <-responses:
				case <-quit:
					return
				}

			case inProgress > 0:
				select {
				case result = <-responses:
				case <-quit:
					return
				}

			// We are done.
			default:
				return
			}

			if result != nil {
				inProgress--
				if result.err != nil {
					log.Infof("johan received failure %d", result.task.i)
					heap.Push(work, result.task)
				}
				log.Infof("johan received result %d", result.task.i)

			}
		}
	}()

	return errChan
}

type worker struct {
	peer    Peer
	nextJob <-chan *queryTask

	results chan<- *jobResult
	quit    <-chan struct{}
}

func (w *worker) start() {

	// Subscribe to messages from the peer.
	msgChan := make(chan SpMsg) //, len(queryMsgs))

	subQuit := make(chan struct{})
	subscription := MsgSubscription{
		MsgChan:  msgChan,
		QuitChan: subQuit,
	}
	defer close(subQuit)

	// TODO: error out if peer disconnects.
	w.peer.SubscribeRecvMsg(subscription)

	for {
		var job *queryTask
		select {
		case job = <-w.nextJob:
		case <-w.quit:
			return
		}
		log.Infof("johan peer got job %d", job.i)

		// send job to peer
		// TODO: need encoding?
		w.peer.QueueMessage(job.Req, nil)

		// Wait for correct response.
		var resp SpMsg
		timeout := time.After(10 * time.Second)
		var jobErr error

	Loop:
		for {

			select {
			case resp = <-msgChan:
				log.Infof("johan peer got response, checking %d", job.i)
				if !job.CheckResp(job.Req, resp.Msg) {
					log.Infof("johan response not valid for %d", job.i)
					continue Loop
				}
				log.Infof("johan response valid for %d", job.i)
				break Loop

			case <-timeout:
				log.Infof("johan peer timeout for %d", job.i)
				// Peer timeout, give task to someone else.
				jobErr = fmt.Errorf("timeout")
				break Loop

			case <-w.quit:
				return
			}
		}
		log.Infof("johan response to %d received!", job.i)

		// We have response to task!
		select {
		case w.results <- &jobResult{
			task: job,
			err:  jobErr,
		}:
		case <-w.quit:
			return
		}

	}

}
