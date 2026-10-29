package rescan

import (
	"github.com/btcsuite/btcd/address/v2"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/filtermatch"
	lndactor "github.com/lightningnetwork/lnd/actor"
)

const (
	// rescanActorID is the ID of the actor that hosts a rescan.
	rescanActorID = "neutrino-rescan"

	// mailboxSize is the capacity of the rescan actor's mailbox.
	mailboxSize = 100
)

// RescanActorMsg is the sealed interface for all mailbox messages handled by
// the rescan actor runtime.
type RescanActorMsg interface {
	lndactor.Message
	rescanActorMsgSealed()
}

// RescanActorResp is the sealed interface for Ask responses from the rescan
// actor runtime.
type RescanActorResp interface {
	rescanActorRespSealed()
}

// processNextBlockMsg is a continuation: it runs the next Syncing or Rewinding
// batch. The actor sends it to itself, and a retry sends it after a failed
// batch.
type processNextBlockMsg struct {
	lndactor.BaseMessage
}

func (*processNextBlockMsg) MessageType() string {
	return "process_next_block"
}
func (*processNextBlockMsg) rescanActorMsgSealed() {}

// stopRescanMsg is the barrier Stop asks for. It moves the FSM to Terminal.
type stopRescanMsg struct {
	lndactor.BaseMessage
}

func (*stopRescanMsg) MessageType() string   { return "stop_rescan" }
func (*stopRescanMsg) rescanActorMsgSealed() {}

// addWatchAddrsMsg carries an AddWatchAddrs request. Its slices are copies
// that the caller no longer shares.
type addWatchAddrsMsg struct {
	lndactor.BaseMessage

	Addrs    []address.Address
	Inputs   []filtermatch.InputWithScript
	RewindTo *uint32

	DisableDisconnectedNtfns bool
}

func (*addWatchAddrsMsg) MessageType() string   { return "add_watch_addrs" }
func (*addWatchAddrsMsg) rescanActorMsgSealed() {}

// blockConnectedMsg carries a connected-block notification from the bridge.
type blockConnectedMsg struct {
	lndactor.BaseMessage

	Header wire.BlockHeader
	Height uint32
}

func (*blockConnectedMsg) MessageType() string   { return "block_connected" }
func (*blockConnectedMsg) rescanActorMsgSealed() {}

// blockDisconnectedMsg carries a disconnected-block notification from the
// bridge. ChainTip is the header of the block's parent, the new tip.
type blockDisconnectedMsg struct {
	lndactor.BaseMessage

	Header   wire.BlockHeader
	Height   uint32
	ChainTip wire.BlockHeader
}

func (*blockDisconnectedMsg) MessageType() string {
	return "block_disconnected"
}

func (*blockDisconnectedMsg) rescanActorMsgSealed() {}

// currentStateReq asks for a copy of the current FSM state.
type currentStateReq struct {
	lndactor.BaseMessage
}

func (*currentStateReq) MessageType() string   { return "current_state" }
func (*currentStateReq) rescanActorMsgSealed() {}

// subscriptionFailedMsg reports that the block subscription could not be
// created, or closed while the rescan ran. Either ends the rescan.
type subscriptionFailedMsg struct {
	lndactor.BaseMessage

	Err error
}

func (*subscriptionFailedMsg) MessageType() string {
	return "subscription_failed"
}

func (*subscriptionFailedMsg) rescanActorMsgSealed() {}

// rescanActorAck is the answer to every request except currentStateReq.
type rescanActorAck struct{}

func (*rescanActorAck) rescanActorRespSealed() {}

// currentStateResp answers currentStateReq.
type currentStateResp struct {
	State RescanState
}

func (*currentStateResp) rescanActorRespSealed() {}

// newAddWatchAddrsMsg builds an addWatchAddrsMsg from copies of its
// arguments, so the caller may reuse them once the call returns.
func newAddWatchAddrsMsg(update WatchUpdate) *addWatchAddrsMsg {
	msg := &addWatchAddrsMsg{
		Addrs:  append([]address.Address(nil), update.Addrs...),
		Inputs: copyInputsWithScripts(update.Inputs),

		DisableDisconnectedNtfns: update.DisableDisconnectedNtfns,
	}

	if update.RewindTo != nil {
		rewind := *update.RewindTo
		msg.RewindTo = &rewind
	}

	return msg
}

// copyInputsWithScripts returns a deep copy of inputs, including each
// PkScript.
func copyInputsWithScripts(
	inputs []filtermatch.InputWithScript,
) []filtermatch.InputWithScript {

	if len(inputs) == 0 {
		return nil
	}

	cloned := make([]filtermatch.InputWithScript, len(inputs))
	for i := range inputs {
		cloned[i] = inputs[i]
		cloned[i].PkScript = append(
			[]byte(nil), inputs[i].PkScript...,
		)
	}

	return cloned
}
