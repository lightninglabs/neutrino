package headersync

import (
	"context"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/lightninglabs/neutrino/headerlist"
	"github.com/stretchr/testify/require"
)

// anchorDriverFixture is a driver at height zero of a chain whose durable tip
// never moves, with one anchor request for the span up to stopHeight in
// flight from each of the given peers.
type anchorDriverFixture struct {
	driver  *Driver
	request AnchorRequest
	start   Hash
}

// newAnchorDriverFixture returns a driver on the production anchor policy,
// which stages the first confirmed reply. With a checkpoint, the chain is
// below its last checkpoint, and the anchor span runs up to it; without one,
// the span runs up to a lookahead height on the frontier.
func newAnchorDriverFixture(t *testing.T, checkpoint bool,
	peers ...string) *anchorDriverFixture {

	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	t.Cleanup(cancel)

	const stopHeight = 10

	cfg := ProductionConfig()
	start := testHash(100)
	stop := testHash(110)
	request := AnchorRequest{
		StartHeight: 0,
		StartHash:   start,
		StopHeight:  stopHeight,
	}

	params := chaincfg.SimNetParams
	params.Checkpoints = nil
	if checkpoint {
		request.StopHash = stop
		params.Checkpoints = []chaincfg.Checkpoint{
			{Height: stopHeight, Hash: &stop},
		}
	}

	manager := NewManager(NewHeaderSyncFSM(0, start, cfg))
	manager.Start(ctx)
	t.Cleanup(manager.Stop)
	_, ok, err := manager.AddAnchor(ctx, 0, start, true)
	require.NoError(t, err)
	require.True(t, ok)

	session := NewSession()
	for _, peer := range peers {
		session.TrackAnchor(peer, request)
	}

	// The durable tip stays at the genesis header: the commit of the
	// staged range is in flight for the whole test.
	genesis := &wire.BlockHeader{}
	store := &headerfs.MockBlockHeaderStore{}
	store.On("ChainTip").Return(genesis, uint32(0), nil)

	tipCfg := TipConfig{
		Params: &params,
		BlockHeaders: func() headerfs.BlockHeaderStore {
			return store
		},
	}
	tip := NewTipState(tipCfg)
	tip.Reset(headerlist.Node{Header: *genesis, Height: 0})

	runtime := &Runtime{
		cfg:        cfg,
		manager:    manager,
		session:    session,
		controller: NewController(manager, session, cfg),
	}
	driver := newDriver(runtime, session, tip, DriverConfig{
		Tip: tipCfg,
		RequestTimeout: func() time.Duration {
			return time.Minute
		},
	})

	return &anchorDriverFixture{
		driver:  driver,
		request: request,
		start:   start,
	}
}

// anchorRequestsSent returns the anchor requests among the effects, by peer.
func anchorRequestsSent(effects []DriverEffect) map[string]SendGetHeaders {
	sent := make(map[string]SendGetHeaders)
	for _, effect := range effects {
		send, ok := effect.(SendGetHeaders)
		if !ok || send.Kind != RequestAnchor {
			continue
		}
		sent[send.PeerID] = send
	}

	return sent
}

// requireReplyTaken checks that the driver took a headers message as an
// answer to one of its requests, rather than handing it back to the executor.
func requireReplyTaken(t *testing.T, effects []DriverEffect) {
	t.Helper()

	for _, effect := range effects {
		_, ok := effect.(NoProgressReply)
		require.False(t, ok, "the driver handed the reply back")
	}
}

// requireAnchorSpanContinued checks that the effects ask the idle peers, and
// not the peer whose hedged anchor request is still in flight, for the rest of
// the span above the reply, and that nothing plans or dispatches ranges while
// the commit is in flight.
func requireAnchorSpanContinued(t *testing.T, request AnchorRequest,
	headers []*wire.BlockHeader, effects []DriverEffect) {

	t.Helper()

	rest := SendGetHeaders{
		Kind:        RequestAnchor,
		StartHeight: uint32(len(headers)),
		StartHash:   headers[len(headers)-1].BlockHash(),
		StopHeight:  request.StopHeight,
		StopHash:    request.StopHash,
	}
	sent := anchorRequestsSent(effects)
	for _, peerID := range []string{"replier", "idle"} {
		rest.PeerID = peerID
		require.Equal(t, rest, sent[peerID],
			"the rest of the span was not asked of peer %s",
			peerID)
	}
	require.NotContains(t, sent, "hedged",
		"a peer with an anchor request in flight was asked again")

	// Planning waits for the commit, so the batch ends with the follow-up
	// of the discovery round and nothing dispatches.
	require.NotEmpty(t, effects)
	followUp, ok := effects[len(effects)-1].(Then)
	require.True(t, ok, "the batch does not end with a follow-up")
	for _, event := range followUp.Events {
		require.IsType(t, anchorWaiting{}, event)
	}
	for _, effect := range effects {
		_, submitted := effect.(SubmitRangeCommit)
		require.False(t, submitted, "a second commit was submitted")
	}
}

// TestAnchorReplyContinuesSpanWhileCommitPending checks that an anchor reply
// that stops short of its checkpoint, and whose staged range is committing,
// still asks an idle peer for the rest of the span before the commit lands.
//
// The reply's range commits asynchronously. Waiting for the commit left the
// rest of the span to the range Continue planned afterwards, and that range
// was often assigned to a peer whose hedged anchor request for the same span
// was still in flight, where it sat until that request answered or timed out:
// several seconds per checkpoint, for every checkpoint on mainnet.
func TestAnchorReplyContinuesSpanWhileCommitPending(t *testing.T) {
	t.Parallel()

	f := newAnchorDriverFixture(t, true, "replier", "hedged")
	headers := makeTestHeaders(f.start, 5)
	view := View{
		Peers: []PeerCandidate{
			{ID: "replier", Height: 20},
			{ID: "hedged", Height: 20},
			{ID: "idle", Height: 20},
		},
		CommitPending: true,
	}

	result, err := f.driver.Handle(t.Context(), HeadersReceived{
		PeerID: "replier", PeerHeight: 20, Headers: headers,
	}, view)
	require.NoError(t, err)
	requireReplyTaken(t, result.Effects)

	// The reply staged the range below the boundary it named.
	_, staged := f.driver.session.StagedRange(1)
	require.True(t, staged, "the reply did not stage its range")

	requireAnchorSpanContinued(t, f.request, headers, result.Effects)
}

// TestAnchorReplyContinuesSpanWhileCommitPendingOnFrontier checks the same
// continuation past the last checkpoint, where the reply first overlaps the
// discovery of the span above its boundary with the commit. The follow-up of
// that round sees the commit in flight, discovers the rest of the span, and
// leaves planning to the commit's result.
func TestAnchorReplyContinuesSpanWhileCommitPendingOnFrontier(
	t *testing.T) {

	t.Parallel()

	f := newAnchorDriverFixture(t, false, "replier", "hedged")
	headers := makeTestHeaders(f.start, 5)
	view := View{
		Peers: []PeerCandidate{
			{ID: "replier", Height: 20},
			{ID: "hedged", Height: 20},
			{ID: "idle", Height: 20},
		},
		CommitPending: true,
	}

	result, err := f.driver.Handle(t.Context(), HeadersReceived{
		PeerID: "replier", PeerHeight: 20, Headers: headers,
	}, view)
	require.NoError(t, err)
	requireReplyTaken(t, result.Effects)

	// The reply pipelines the frontier above its boundary, and leaves the
	// rest of its own span to the follow-up of that round.
	require.NotEmpty(t, result.Effects)
	followUp, ok := result.Effects[len(result.Effects)-1].(Then)
	require.True(t, ok, "the batch does not end with a follow-up")
	require.Len(t, followUp.Events, 1)
	require.IsType(t, anchorPipelined{}, followUp.Events[0])

	result, err = f.driver.Handle(t.Context(), followUp.Events[0], view)
	require.NoError(t, err)

	requireAnchorSpanContinued(t, f.request, headers, result.Effects)
}
