package query

import (
	"time"

	"github.com/btcsuite/btcd/wire/v2"
)

const (
	// defaultQueryTimeout specifies the default total time a query is
	// allowed to be retried before it will fail.
	defaultQueryTimeout = time.Second * 30

	// defaultQueryEncoding specifies the default encoding (witness or not)
	// for `getdata` and other similar messages.
	defaultQueryEncoding = wire.WitnessEncoding

	// defaultNumRetries is the default number of times that a query job
	// will be retried.
	defaultNumRetries = 2
)

// queries are a set of options that can be modified per-query, unlike global
// options.
type queryOptions struct {
	// timeout specifies the total wall-clock time a batch is allowed to
	// run before it is canceled. This is a hard deadline measured from
	// batch submission. A value of zero means no hard deadline is
	// enforced and the batch must rely on progressTimeout (or external
	// cancellation) to terminate.
	timeout time.Duration

	// progressTimeout, when non-zero, enables a per-batch idle timer that
	// is reset every time a query in the batch completes successfully.
	// The batch is canceled if no successful query completes within this
	// duration. This is intended for long batches (e.g. cfheader sync)
	// where the total runtime cannot be bounded ahead of time but a
	// stalled peer pool should still abort the batch promptly.
	//
	// progressTimeout composes with timeout: if both are set, whichever
	// fires first cancels the batch.
	progressTimeout time.Duration

	// encoding lets the query know which encoding to use when queueing
	// messages to a peer.
	encoding wire.MessageEncoding

	// cancelChan is an optional channel that can be closed to indicate
	// that the query should be canceled.
	cancelChan chan struct{}

	// numRetries is the number of times that a query should be retried
	// before failing.
	numRetries uint8

	// noRetryMax is set if no cap should be applied to the number of times
	// that a query can be retried. If this is set then numRetries has no
	// effect.
	noRetryMax bool
}

// QueryOption is a functional option argument to any of the network query
// methods, such as GetBlock and GetCFilter (when that resorts to a network
// query). These are always processed in order, with later options overriding
// earlier ones.
type QueryOption func(*queryOptions) // nolint

// defaultQueryOptions returns a queryOptions set to package-level defaults.
func defaultQueryOptions() *queryOptions {
	return &queryOptions{
		timeout:    defaultQueryTimeout,
		encoding:   defaultQueryEncoding,
		numRetries: defaultNumRetries,
	}
}

// applyQueryOptions updates a queryOptions set with functional options.
func (qo *queryOptions) applyQueryOptions(options ...QueryOption) {
	for _, option := range options {
		option(qo)
	}
}

// NumRetries is a query option that specifies the number of times a query
// should be retried.
func NumRetries(num uint8) QueryOption {
	return func(qo *queryOptions) {
		qo.numRetries = num
	}
}

// NoRetryMax is a query option that can be used to disable the cap on the
// number of retries. If this is set then NumRetries has no effect.
func NoRetryMax() QueryOption {
	return func(qo *queryOptions) {
		qo.noRetryMax = true
	}
}

// Timeout is a query option that specifies the total wall-clock time a batch
// is allowed to run before it is canceled. A negative or zero value preserves
// the historical "fire immediately" semantics of time.After(d) for d <= 0: the
// batch is canceled on the next dispatch tick. To run a batch without a hard
// wall-clock bound, do not call Timeout at all — the package default
// (defaultQueryTimeout) is used — or rely on ProgressTimeout combined with
// external cancellation.
func Timeout(timeout time.Duration) QueryOption {
	return func(qo *queryOptions) {
		// Normalize non-positive values to a 1ns deadline. Pre-PR,
		// Timeout(0) produced time.After(0), which fires immediately;
		// any external consumer passing an uninitialized
		// time.Duration relied on that fail-fast behaviour. We
		// preserve it explicitly here so that "no deadline" can never
		// be requested by accident — it must be requested by simply
		// not calling Timeout.
		if timeout <= 0 {
			qo.timeout = 1
			return
		}
		qo.timeout = timeout
	}
}

// ProgressTimeout is a query option that enables a per-batch idle timer. The
// timer is reset every time a query in the batch completes successfully; if
// no successful query completes within the given duration, the batch is
// canceled with ErrQueryTimeout. A value of zero disables the idle timer
// (the default).
//
// ProgressTimeout composes with Timeout: if both are set, whichever fires
// first cancels the batch.
func ProgressTimeout(timeout time.Duration) QueryOption {
	return func(qo *queryOptions) {
		qo.progressTimeout = timeout
	}
}

// Encoding is a query option that allows the caller to set a message encoding
// for the query messages.
func Encoding(encoding wire.MessageEncoding) QueryOption {
	return func(qo *queryOptions) {
		qo.encoding = encoding
	}
}

// Cancel takes a channel that can be closed to indicate that the query should
// be canceled.
func Cancel(cancel chan struct{}) QueryOption {
	return func(qo *queryOptions) {
		qo.cancelChan = cancel
	}
}

// Progress encloses the result of handling a response for a given Request,
// determining whether the response did progress the query.
type Progress struct {
	// Finished is true if the query was finished as a result of the
	// received response.
	Finished bool

	// Progressed is true if the query made progress towards fully
	// answering the request as a result of the received response. This is
	// used for the requests types where more than one response is
	// expected.
	Progressed bool
}

// Request is the main struct that defines a bitcoin network query to be sent to
// connected peers.
type Request struct {
	// Req is the message request to send.
	Req wire.Message

	// HandleResp is a response handler that will be called for every
	// message received from the peer that the request was made to. It
	// should validate the response against the request made, and return a
	// Progress indicating whether the request was answered by this
	// particular response.
	//
	// NOTE: Since the worker's job queue will be stalled while this method
	// is running, it should not be doing any expensive operations. It
	// should validate the response and immediately return the progress.
	// The response should be handed off to another goroutine for
	// processing.
	HandleResp func(req, resp wire.Message, peer string) Progress
}

// WorkManager defines an API for a manager that dispatches queries to bitcoin
// peers that must be started and stopped in order to perform these queries.
type WorkManager interface {
	Dispatcher

	// Start sets up any resources that the WorkManager requires. It must
	// be called before any of the Dispatcher calls can be made.
	Start() error

	// Stop cleans up the resources held by the WorkManager.
	Stop() error
}

// Dispatcher is an interface defining the API for dispatching queries to
// bitcoin peers.
type Dispatcher interface {
	// Query distributes the slice of requests to the set of connected
	// peers. It returns an error channel where the final result of the
	// batch of queries will be sent. Responses for the individual queries
	// should be handled by the response handler of each Request.
	Query(reqs []*Request, options ...QueryOption) chan error
}

// Peer is the interface that defines the methods needed by the query package
// to be able to make requests and receive responses from a network peer.
type Peer interface {
	// QueueMessageWithEncoding adds the passed bitcoin message to the peer
	// send queue.
	QueueMessageWithEncoding(msg wire.Message, doneChan chan<- struct{},
		encoding wire.MessageEncoding)

	// SubscribeRecvMsg adds a OnRead subscription to the peer. All bitcoin
	// messages received from this peer will be sent on the returned
	// channel. A closure is also returned, that should be called to cancel
	// the subscription.
	SubscribeRecvMsg() (<-chan wire.Message, func())

	// Addr returns the address of this peer.
	Addr() string

	// OnDisconnect returns a channel that will be closed when this peer is
	// disconnected.
	OnDisconnect() <-chan struct{}
}
