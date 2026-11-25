package neutrino

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/filtermatch"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// hashBackend uses a block hash as its match evidence.
type hashBackend struct {
	match  func(chainhash.Hash, [][]byte, bool) (filtermatch.Decision[chainhash.Hash], error)
	verify func(chainhash.Hash, *btcutil.Block) error
}

func (b hashBackend) Match(_ context.Context, hash chainhash.Hash,
	scripts [][]byte, batch bool) (filtermatch.Decision[chainhash.Hash], error) {

	return b.match(hash, scripts, batch)
}

func (b hashBackend) Verify(hash chainhash.Hash, block *btcutil.Block) error {
	return b.verify(hash, block)
}

// TestRescanWithMatcher drives a source exposing only headers and blocks.
// A miss still emits both wallet callbacks. A hit verifies the same block
// before either callback can observe it.
func TestRescanWithMatcher(t *testing.T) {
	chain := newMockChainSource(3)
	// The mock reports a GetBlock on this channel before returning it.
	chain.blocksQueried = make(chan chainhash.Hash, 2)
	var events []string
	var queries int
	backend := hashBackend{
		match: func(hash chainhash.Hash, scripts [][]byte, batch bool) (
			filtermatch.Decision[chainhash.Hash], error) {

			if !batch || len(scripts) != 1 {
				return filtermatch.Decision[chainhash.Hash]{},
					errors.New("wrong historical watch")
			}
			queries++
			if queries == 1 {
				return filtermatch.Skip[chainhash.Hash](), nil
			}
			return filtermatch.Candidate(hash), nil
		},
		verify: func(hash chainhash.Hash, block *btcutil.Block) error {
			if *block.Hash() != hash {
				return errors.New("wrong block")
			}
			events = append(events, "verify")
			return nil
		},
	}
	// Embedding ScanSource hides the concrete mock's GetCFilter method.
	src := struct{ ScanSource }{chain}
	r := NewRescanWithMatcher(src, filtermatch.NewBlockMatcher(backend),
		StartBlock(&headerfs.BlockStamp{}),
		EndBlock(&headerfs.BlockStamp{Height: 2}),
		WatchInputs(*makeTestInputWithScript()),
		NotificationHandlers(rpcclient.NotificationHandlers{
			OnFilteredBlockConnected: func(_ int32,
				_ *wire.BlockHeader, txs []*btcutil.Tx) {

				if txs == nil {
					events = append(events, "skipped")
				} else {
					events = append(events, "checked")
				}
			},
			OnBlockConnected: func(_ *chainhash.Hash,
				_ int32, _ time.Time) {

				events = append(events, "connected")
			},
		}),
	)
	require.NoError(t, <-r.Start())
	r.WaitForShutdown()
	require.Equal(t, []string{
		"skipped", "connected", "verify", "checked", "connected",
	}, events)
	require.Len(t, chain.blocksQueried, 1)
}

// TestUtxoMatcherRejectsCandidate ensures failed verification never turns
// a spend lookup into a successful report. Birth blocks bypass the matcher.
func TestUtxoMatcherRejectsCandidate(t *testing.T) {
	created := wire.NewMsgTx(2)
	created.AddTxOut(wire.NewTxOut(5000, []byte{0x51}))
	chain := newSwapChain(2)
	birth := chain.add(1, &wire.MsgBlock{
		Header:       wire.BlockHeader{Nonce: 1},
		Transactions: []*wire.MsgTx{created},
	})
	next := chain.add(2, &wire.MsgBlock{
		Header: wire.BlockHeader{PrevBlock: birth, Nonce: 2},
	})
	rejected := errors.New("candidate rejected")
	cfg := chain.config()
	cfg.BlockFilterMatches = nil
	cfg.BlockMatcher = filtermatch.NewBlockMatcher(hashBackend{
		match: func(hash chainhash.Hash, scripts [][]byte, batch bool) (
			filtermatch.Decision[chainhash.Hash], error) {

			if hash != next || !batch || len(scripts) != 1 {
				return filtermatch.Decision[chainhash.Hash]{},
					errors.New("unexpected filter request")
			}
			return filtermatch.Candidate(hash), nil
		},
		verify: func(chainhash.Hash, *btcutil.Block) error {
			return rejected
		},
	})
	report, err := runScan(t, cfg, wire.OutPoint{Hash: created.TxHash()})
	require.ErrorIs(t, err, rejected)
	require.Nil(t, report)
}

// TestUnresolvedDecision verifies a backend cannot accidentally skip a
// block by returning a zero decision and a nil error.
func TestUnresolvedDecision(t *testing.T) {
	backend := hashBackend{
		match: func(chainhash.Hash, [][]byte, bool) (
			filtermatch.Decision[chainhash.Hash], error) {

			return filtermatch.Decision[chainhash.Hash]{}, nil
		},
	}
	matcher := filtermatch.NewBlockMatcher(backend)
	block, err := filtermatch.FetchMatchingBlock(
		t.Context(), matcher, chainhash.Hash{}, nil, false, nil,
	)
	require.Error(t, err)
	require.Nil(t, block)
}
