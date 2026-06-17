package workmanager

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/query"
	"github.com/stretchr/testify/require"
)

type livePeer struct {
	requests      chan wire.Message
	responses     chan<- wire.Message
	subscriptions chan chan wire.Message
	quit          chan struct{}
}

var _ query.Peer = (*livePeer)(nil)

func (l *livePeer) QueueMessageWithEncoding(msg wire.Message,
	_ chan<- struct{}, _ wire.MessageEncoding) {

	l.requests <- msg
}

func (l *livePeer) SubscribeRecvMsg() (<-chan wire.Message, func()) {
	msgChan := make(chan wire.Message)
	l.subscriptions <- msgChan

	return msgChan, func() {}
}

func (l *livePeer) Addr() string {
	return "live-peer"
}

func (l *livePeer) LastPingMicros() int64 {
	return 0
}

func (l *livePeer) OnDisconnect() <-chan struct{} {
	return l.quit
}

func TestWorkerNotFoundFailsFast(t *testing.T) {
	t.Parallel()

	peer := &livePeer{
		requests:      make(chan wire.Message),
		subscriptions: make(chan chan wire.Message),
		quit:          make(chan struct{}),
	}
	results := make(chan *jobResult)
	quit := make(chan struct{})
	defer close(quit)

	wk := NewWorker(peer)
	go wk.Run(results, quit)

	select {
	case peer.responses = <-peer.subscriptions:
	case <-time.After(time.Second):
		t.Fatalf("worker did not subscribe")
	}

	var blockHash chainhash.Hash
	blockHash[0] = 1
	inv := wire.NewInvVect(wire.InvTypeWitnessBlock, &blockHash)
	getData := wire.NewMsgGetData()
	require.NoError(t, getData.AddInvVect(inv))
	notFound := wire.NewMsgNotFound()
	require.NoError(t, notFound.AddInvVect(inv))

	job := &queryJob{
		index:    1,
		timeout:  time.Hour,
		encoding: wire.WitnessEncoding,
		Request: &query.Request{
			Req: getData,
			HandleResp: func(_, _ wire.Message,
				_ string) query.Progress {

				return query.Progress{}
			},
		},
	}

	select {
	case wk.NewJob() <- job:
	case <-time.After(time.Second):
		t.Fatalf("worker did not accept job")
	}

	select {
	case <-peer.requests:
	case <-time.After(time.Second):
		t.Fatalf("request was not sent")
	}

	select {
	case peer.responses <- notFound:
	case <-time.After(time.Second):
		t.Fatalf("worker did not receive notfound")
	}

	select {
	case result := <-results:
		require.Same(t, job, result.job)
		require.Same(t, peer, result.peer)
		require.ErrorIs(t, result.err, query.ErrPeerNotFound)

	case <-time.After(time.Second):
		t.Fatalf("worker did not report notfound")
	}
}
