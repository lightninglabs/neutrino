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
}

// QueryOption is a functional option argument to any of the network query
// methods, such as GetBlock and GetCFilter (when that resorts to a network
// query). These are always processed in order, with later options overriding
// earlier ones.
type QueryOption func(*queryOptions)

// defaultQueryOptions returns a queryOptions set to package-level defaults.
func defaultQueryOptions() *queryOptions {
	return &queryOptions{
		timeout:    defaultQueryTimeout,
		encoding:   defaultQueryEncoding,
		cancelChan: nil,
	}
}

// applyQueryOptions updates a queryOptions set with functional options.
func (qo *queryOptions) applyQueryOptions(options ...QueryOption) {
	for _, option := range options {
		option(qo)
	}
}

// Timeout is a query option that specifies the total time a query is allowed
// to be retried before it is failed.
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

// Query is the main struct that defines a bitcoin network query to be sent to
// connected peers.
type Query struct {
	// Req is the message request to send.
	Req wire.Message

	// HandleResp is a response handler that will be called for every
	// message received from the peer that the request was made to. It
	// should validate the response against the request made, and return
	// whether the request was answered by this particular response.
	//
	// The first bool returned indicates whether the request was fully
	// answered by this response. If the response did not fully answer the
	// request, the second boolean indicates whether the response did
	// make progress towards answering the request. This is used for the
	// requests types where more than one response is expected.
	HandleResp func(req, resp wire.Message, peer string) (bool, bool)
}

// QueryAccess is an interface defining the API for making queries to bitcoin
// peers.
type QueryAccess interface {
	// Query distributes the slice of queries to the set of connected
	// peers. It returns an error channel where the final result of the
	// batch of queries will be sent. Responses for the individual queries
	// should be handled by the response handler of each Query.
	Query(queries []*Query, options ...QueryOption) chan error
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
