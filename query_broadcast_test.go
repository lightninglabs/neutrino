package neutrino

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/pushtx"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
	"github.com/stretchr/testify/require"
)

const (
	// sendTxTestTimeout is the per-attempt broadcast timeout used for the
	// transaction broadcast tests.
	sendTxTestTimeout = 200 * time.Millisecond

	// sendTxTestRejectTimeout is how long a peer that took the
	// transaction is given to follow up with a reject.
	sendTxTestRejectTimeout = 20 * time.Millisecond
)

// peerBehavior describes how a scripted peer reacts to a transaction inv.
type peerBehavior uint8

const (
	// peerSilent never answers the inv.
	peerSilent peerBehavior = iota

	// peerAccepts requests the transaction and stays quiet afterwards.
	peerAccepts

	// peerRejectsInvalid requests the transaction and then rejects it as
	// invalid.
	peerRejectsInvalid
)

// TestSendTransactionRejectSemantics exercises how the per-peer outcomes of a
// transaction broadcast are folded into the final result.
func TestSendTransactionRejectSemantics(t *testing.T) {
	t.Parallel()

	tx := wire.NewMsgTx(2)
	tx.AddTxOut(&wire.TxOut{Value: 1, PkScript: []byte{0x51}})
	txHash := tx.TxHash()

	getData := wire.NewMsgGetData()
	require.NoError(t, getData.AddInvVect(
		wire.NewInvVect(wire.InvTypeTx, &txHash),
	))
	reject := wire.NewMsgReject(
		wire.CmdTx, wire.RejectInvalid, "bad-txns",
	)
	reject.Hash = txHash

	script := func(b peerBehavior) []wire.Message {
		switch b {
		case peerAccepts:
			return []wire.Message{getData}
		case peerRejectsInvalid:
			return []wire.Message{getData, reject}
		default:
			return nil
		}
	}

	testCases := []struct {
		name      string
		peers     []peerBehavior
		wantErr   bool
		wantTxFor int
	}{{
		name:  "no peers",
		peers: nil,
	}, {
		name:  "silent peers only",
		peers: []peerBehavior{peerSilent, peerSilent},
	}, {
		name:      "all accept",
		peers:     []peerBehavior{peerAccepts, peerAccepts},
		wantTxFor: 2,
	}, {
		name: "all repliers reject",
		peers: []peerBehavior{
			peerRejectsInvalid, peerSilent, peerRejectsInvalid,
		},
		wantErr:   true,
		wantTxFor: 2,
	}, {
		name: "below invalid threshold",
		peers: []peerBehavior{
			peerRejectsInvalid, peerAccepts, peerAccepts,
		},
		wantTxFor: 3,
	}, {
		name: "at invalid threshold",
		peers: []peerBehavior{
			peerRejectsInvalid, peerRejectsInvalid,
			peerAccepts,
		},
		wantErr:   true,
		wantTxFor: 3,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scripts := make(map[string][]wire.Message)
			for i, b := range tc.peers {
				addr := string(rune('a'+i)) + ":8333"
				scripts[addr] = script(b)
			}

			peers := newScriptedPeers(scripts)
			wm := querysyncwm.NewWorkManager(
				&querysyncwm.Config{
					Peers: func() []querysyncwm.Peer {
						out := make(
							[]querysyncwm.Peer,
							len(peers),
						)
						for i, p := range peers {
							out[i] = p
						}

						return out
					},
				},
			)

			s := &ChainService{
				workManager:      wm,
				broadcastTimeout: sendTxTestTimeout,
			}

			done := make(chan struct{})
			err := s.sendTransaction(
				tx, RejectTimeout(sendTxTestRejectTimeout),
				DoneChan(done),
			)
			if tc.wantErr {
				var bErr *pushtx.BroadcastError
				require.ErrorAs(t, err, &bErr)
				require.Equal(t, pushtx.Invalid, bErr.Code)
			} else {
				require.NoError(t, err)
			}

			// The done channel is closed once the broadcast ends.
			select {
			case <-done:
			default:
				t.Fatal("done channel not closed")
			}

			// Every peer that asked for the transaction got it in
			// addition to the initial inv.
			gotTx := 0
			for _, p := range peers {
				if p.numQueued() > 1 {
					gotTx++
				}
			}
			require.Equal(t, tc.wantTxFor, gotTx)
		})
	}
}

// TestOnPeerUnresponsiveQuarantines checks that an unresponsive query peer is
// quarantined for the configured duration, by exact address, and not banned.
func TestOnPeerUnresponsiveQuarantines(t *testing.T) {
	t.Parallel()

	s := &ChainService{
		query: make(chan interface{}, 1),
		quit:  make(chan struct{}),
	}

	before := time.Now()
	s.onPeerUnresponsive(&scriptedPeer{addr: "1.2.3.4:8333"})

	select {
	case msg := <-s.query:
		q, ok := msg.(quarantinePeerMsg)
		require.True(t, ok, "unexpected message %T", msg)
		require.Equal(t, "1.2.3.4:8333", q.addr)
		require.WithinDuration(
			t, before.Add(unresponsivePeerQuarantine), q.until,
			time.Minute,
		)
		require.GreaterOrEqual(
			t, q.until.Sub(before), unresponsivePeerQuarantine,
		)

	default:
		t.Fatal("peer was not quarantined")
	}
}
