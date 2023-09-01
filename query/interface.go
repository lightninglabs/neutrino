package query

import (
	"time"

	"github.com/btcsuite/btcd/wire"
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
	// timeout specifies the total time a query is allowed to
	// be retried before it will fail.
	timeout time.Duration

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

	// errChan error channel with which the workmananger sends error.
	errChan chan error
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

// ErrChan is a query option that specifies the error channel which the workmanager
// sends any error to.
func ErrChan(err chan error) QueryOption {
	return func(qo *queryOptions) {
		qo.errChan = err
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

// Timeout is a query option that specifies the total time a query is allowed
// to be tried before it is failed.
func Timeout(timeout time.Duration) QueryOption {
	return func(qo *queryOptions) {
		qo.timeout = timeout
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

// Progress encloses the result of handling a response for a given Request.
type Progress string

var (

	// Finished indicates we have received the complete, valid response for this request,
	// and so we are done with it.
	Finished Progress = "Received complete  and valid response for request."

	// Progressed indicates that we have received a valid response, but we are expecting more.
	Progressed Progress = "Received valid response, expecting more response for query."

	// UnFinishedRequest indicates that we have received some response, but we need to rescheule the job
	// to completely fetch all the response required for this request.
	UnFinishedRequest Progress = "Received valid response, reschedule to complete request"

	// ResponseErr indicates we obtained a valid response but response fails checks and needs to
	// be rescheduled.
	ResponseErr Progress = "Received valid response but fails checks "

	// IgnoreRequest indicates that we have received a valid response but the workmanager need take
	// no action on the result of this job.
	IgnoreRequest Progress = "Received response but ignoring"

	// NoResponse indicates that we have received an invalid response for this request, and we need
	// to wait for a valid one.
	NoResponse Progress = "Received invalid response"
)

// Request is the main struct that defines a bitcoin network query to be sent to
// connected peers.
type Request struct {
	// Req contains the message request to send.
	Req ReqMessage

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
	HandleResp func(req ReqMessage, resp wire.Message, peer Peer) Progress

	// SendQuery handles sending request to the worker's peer. It returns an error,
	// if one is encountered while sending the request.
	SendQuery func(peer Peer, request ReqMessage) error

	// CloneReq clones the message.
	CloneReq func(message ReqMessage) ReqMessage
}

// ReqMessage is an interface which all structs containing information
// required to process a message request must implement.
type ReqMessage interface {

	// Message returns the message request.
	Message() wire.Message

	// PriorityIndex returns the priority the caller prefers the request
	// would take.
	PriorityIndex() float64
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

	// IsPeerBehindStartHeight returns a boolean indicating if the peer's known last height is behind
	// the request's start Height which it receives as an argument.
	IsPeerBehindStartHeight(req ReqMessage) bool

	// IsSyncCandidate returns true if the peer is a sync candidate.
	IsSyncCandidate() bool
}
