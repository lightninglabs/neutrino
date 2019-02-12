package pushtx_test

import (
	"testing"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/pushtx"
)

// TestParseBroadcastErrorCode ensures that we properly construct a
// BroadcastError with the appropriate error code from a wire.MsgReject.
func TestParseBroadcastErrorCode(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		msg  *wire.MsgReject
		code pushtx.BroadcastErrorCode
	}{
		{
			name: "dust transaction",
			msg: &wire.MsgReject{
				Code: wire.RejectDust,
			},
		},
		{
			name: "invalid transaction",
			msg: &wire.MsgReject{
				Code:   wire.RejectInvalid,
				Reason: "spends inexistent output",
			},
			code: pushtx.Invalid,
		},
		{
			name: "nonstandard transaction",
			msg: &wire.MsgReject{
				Code:   wire.RejectNonstandard,
				Reason: "",
			},
			code: pushtx.Invalid,
		},
		{
			name: "insufficient fee transaction",
			msg: &wire.MsgReject{
				Code:   wire.RejectInsufficientFee,
				Reason: "",
			},
			code: pushtx.InsufficientFee,
		},
		{
			name: "bitcoind mempool double spend",
			msg: &wire.MsgReject{
				Code:   wire.RejectDuplicate,
				Reason: "txn-mempool-conflict",
			},
			code: pushtx.Invalid,
		},
		{
			name: "bitcoind transaction in mempool",
			msg: &wire.MsgReject{
				Code:   wire.RejectDuplicate,
				Reason: "txn-already-in-mempool",
			},
			code: pushtx.Mempool,
		},
		{
			name: "bitcoind transaction in chain",
			msg: &wire.MsgReject{
				Code:   wire.RejectDuplicate,
				Reason: "txn-already-known",
			},
			code: pushtx.Confirmed,
		},
		{
			name: "btcd mempool double spend",
			msg: &wire.MsgReject{
				Code:   wire.RejectDuplicate,
				Reason: "already spent",
			},
			code: pushtx.Invalid,
		},
		{
			name: "btcd transaction in mempool",
			msg: &wire.MsgReject{
				Code:   wire.RejectDuplicate,
				Reason: "already have transaction",
			},
			code: pushtx.Mempool,
		},
		{
			name: "btcd transaction in chain",
			msg: &wire.MsgReject{
				Code:   wire.RejectDuplicate,
				Reason: "transaction already exists",
			},
			code: pushtx.Confirmed,
		},
	}

	for _, testCase := range testCases {
		test := testCase
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			broadcastErr := pushtx.ParseBroadcastError(
				test.msg, "127.0.0.1:8333",
			)
			if broadcastErr.Code != test.code {
				t.Fatalf("expected BroadcastErrorCode %v, got "+
					"%v", test.code, broadcastErr.Code)
			}
		})
	}
}
