package neutrino

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/pushtx"
	querysyncwm "github.com/lightninglabs/neutrino/querysync/workmanager"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
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

	// peerRejectsUnasked rejects the transaction as invalid without ever
	// asking for it.
	peerRejectsUnasked

	// peerRejectsConfirmed requests the transaction and then claims it is
	// already in the chain.
	peerRejectsConfirmed

	// peerRequestsRepeatedly asks for the transaction in one getdata
	// with the same vector many times over, then in a second getdata.
	peerRequestsRepeatedly
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

	confirmed := wire.NewMsgReject(
		wire.CmdTx, wire.RejectDuplicate, "transaction already exists",
	)
	confirmed.Hash = txHash

	repeated := wire.NewMsgGetData()
	for i := 0; i < 50; i++ {
		require.NoError(t, repeated.AddInvVect(
			wire.NewInvVect(wire.InvTypeTx, &txHash),
		))
	}

	script := func(b peerBehavior) []wire.Message {
		switch b {
		case peerAccepts:
			return []wire.Message{getData}
		case peerRejectsInvalid:
			return []wire.Message{getData, reject}
		case peerRejectsUnasked:
			return []wire.Message{reject}
		case peerRejectsConfirmed:
			return []wire.Message{getData, confirmed}
		case peerRequestsRepeatedly:
			return []wire.Message{repeated, getData}
		default:
			return nil
		}
	}

	testCases := []struct {
		name      string
		peers     []peerBehavior
		wantErr   bool
		wantCode  pushtx.BroadcastErrorCode
		wantTxFor int

		// wantQueued, if set, is how many messages each peer that
		// got the transaction was sent: the inv and one tx.
		wantQueued int
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
	}, {
		// A reject only counts from a peer we sent the transaction to,
		// so one peer can't pair its reject with an honest peer's
		// silent acceptance.
		name:      "unasked reject is ignored",
		peers:     []peerBehavior{peerAccepts, peerRejectsUnasked},
		wantTxFor: 1,
	}, {
		// A confirmed claim from the only peer that asked for the
		// transaction is reported as known, not as confirmed.
		name:      "confirmed claim is not a verdict",
		peers:     []peerBehavior{peerRejectsConfirmed, peerSilent},
		wantErr:   true,
		wantCode:  pushtx.Mempool,
		wantTxFor: 1,
	}, {
		name:       "repeated requests get one tx",
		peers:      []peerBehavior{peerRequestsRepeatedly},
		wantTxFor:  1,
		wantQueued: 2,
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
				wantCode := tc.wantCode
				if wantCode == 0 {
					wantCode = pushtx.Invalid
				}
				require.Equal(t, wantCode, bErr.Code)
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
					if tc.wantQueued != 0 {
						require.Equal(
							t, tc.wantQueued,
							p.numQueued(),
						)
					}
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

	// QuarantinePeer hands off asynchronously so the block handler can't
	// deadlock against peerHandler, so wait briefly for the message.
	case <-time.After(5 * time.Second):
		t.Fatal("peer was not quarantined")
	}
}

// TestSendTransactionOutcomeProperty checks, for any mix of peer behaviors,
// that the broadcast verdict depends only on the peers that asked for the
// transaction: it is Invalid exactly when every one of them rejected it as
// invalid, or at least the invalid threshold did, and each of them was sent
// the transaction once however often it asked.
func TestSendTransactionOutcomeProperty(t *testing.T) {
	t.Parallel()

	tx := wire.NewMsgTx(2)
	tx.AddTxOut(&wire.TxOut{Value: 1, PkScript: []byte{0x51}})
	txHash := tx.TxHash()

	getData := wire.NewMsgGetData()
	require.NoError(t, getData.AddInvVect(
		wire.NewInvVect(wire.InvTypeTx, &txHash),
	))
	repeated := wire.NewMsgGetData()
	for i := 0; i < 5; i++ {
		require.NoError(t, repeated.AddInvVect(
			wire.NewInvVect(wire.InvTypeTx, &txHash),
		))
	}
	reject := wire.NewMsgReject(
		wire.CmdTx, wire.RejectInvalid, "bad-txns",
	)
	reject.Hash = txHash

	rapid.Check(t, func(rt *rapid.T) {
		behaviors := rapid.SliceOfN(rapid.SampledFrom([]peerBehavior{
			peerAccepts, peerRejectsInvalid, peerRejectsUnasked,
			peerRequestsRepeatedly,
		}), 1, 5).Draw(rt, "peers")

		scripts := make(map[string][]wire.Message)
		var requesters, invalid int
		for i, b := range behaviors {
			var msgs []wire.Message
			switch b {
			case peerAccepts:
				msgs = []wire.Message{getData}
				requesters++
			case peerRejectsInvalid:
				msgs = []wire.Message{getData, reject}
				requesters++
				invalid++
			case peerRejectsUnasked:
				msgs = []wire.Message{reject}
			case peerRequestsRepeatedly:
				msgs = []wire.Message{repeated, getData}
				requesters++
			}
			scripts[string(rune('a'+i))+":8333"] = msgs
		}

		peers := newScriptedPeers(scripts)
		wm := querysyncwm.NewWorkManager(&querysyncwm.Config{
			Peers: func() []querysyncwm.Peer {
				out := make([]querysyncwm.Peer, len(peers))
				for i, p := range peers {
					out[i] = p
				}

				return out
			},
		})
		s := &ChainService{
			workManager:      wm,
			broadcastTimeout: sendTxTestTimeout,
		}
		// A reject has to cross two goroutine hops within the reject
		// timeout, so this test gives it more room than the table
		// above, to stay steady on a loaded machine.
		err := s.sendTransaction(tx, RejectTimeout(sendTxTestTimeout))

		wantInvalid := requesters > 0 && (invalid == requesters ||
			float32(invalid)/float32(requesters) >=
				defaultQueryOptions().invalidTxThreshold)
		if wantInvalid {
			var bErr *pushtx.BroadcastError
			require.ErrorAs(rt, err, &bErr)
			require.Equal(rt, pushtx.Invalid, bErr.Code)
		} else {
			require.NoError(rt, err)
		}

		// Every peer gets the inv, and a requester gets one tx.
		for _, p := range peers {
			require.LessOrEqual(rt, p.numQueued(), 2)
		}
	})
}
