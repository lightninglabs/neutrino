package rescan

import (
	"fmt"

	"github.com/btcsuite/btcd/address/v2"

	"github.com/btcsuite/btcd/wire/v2"
	neutrino "github.com/lightninglabs/neutrino"
	"github.com/lightninglabs/neutrino/blockntfns"
	lndactor "github.com/lightningnetwork/lnd/actor"
)

const (
	// RescanActorServiceKeyName is the receptionist key used to discover a
	// rescan actor within its actor system.
	RescanActorServiceKeyName = "neutrino-rescan"
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

// RescanActorServiceKey returns the service key used to spawn and discover the
// rescan actor within an actor system.
func RescanActorServiceKey() lndactor.ServiceKey[
	RescanActorMsg, RescanActorResp] {

	return lndactor.NewServiceKey[RescanActorMsg, RescanActorResp](
		RescanActorServiceKeyName,
	)
}

type processNextBlockMsg struct {
	lndactor.BaseMessage
}

func (*processNextBlockMsg) MessageType() string {
	return "process_next_block"
}
func (*processNextBlockMsg) rescanActorMsgSealed() {}

type stopRescanMsg struct {
	lndactor.BaseMessage
}

func (*stopRescanMsg) MessageType() string   { return "stop_rescan" }
func (*stopRescanMsg) rescanActorMsgSealed() {}

type addWatchAddrsMsg struct {
	lndactor.BaseMessage

	Addrs    []address.Address
	Inputs   []neutrino.InputWithScript
	RewindTo *uint32
}

func (*addWatchAddrsMsg) MessageType() string   { return "add_watch_addrs" }
func (*addWatchAddrsMsg) rescanActorMsgSealed() {}

type blockConnectedMsg struct {
	lndactor.BaseMessage

	Header wire.BlockHeader
	Height uint32
}

func (*blockConnectedMsg) MessageType() string   { return "block_connected" }
func (*blockConnectedMsg) rescanActorMsgSealed() {}

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

type currentStateReq struct {
	lndactor.BaseMessage
}

func (*currentStateReq) MessageType() string   { return "current_state" }
func (*currentStateReq) rescanActorMsgSealed() {}

type subscriptionClosedMsg struct {
	lndactor.BaseMessage
	Sub *blockntfns.Subscription
}

func (*subscriptionClosedMsg) MessageType() string {
	return "subscription_closed"
}

func (*subscriptionClosedMsg) rescanActorMsgSealed() {}

type rescanActorAck struct{}

func (*rescanActorAck) rescanActorRespSealed() {}

type currentStateResp struct {
	State RescanState
}

func (*currentStateResp) rescanActorRespSealed() {}

func newAddWatchAddrsMsg(
	addrs []address.Address,
	inputs []neutrino.InputWithScript,
	rewindTo *uint32,
) *addWatchAddrsMsg {

	msg := &addWatchAddrsMsg{
		Addrs:  append([]address.Address(nil), addrs...),
		Inputs: copyInputsWithScripts(inputs),
	}

	if rewindTo != nil {
		rewind := *rewindTo
		msg.RewindTo = &rewind
	}

	return msg
}

func copyInputsWithScripts(
	inputs []neutrino.InputWithScript,
) []neutrino.InputWithScript {

	if len(inputs) == 0 {
		return nil
	}

	cloned := make([]neutrino.InputWithScript, len(inputs))
	for i := range inputs {
		cloned[i] = inputs[i]
		cloned[i].PkScript = append(
			[]byte(nil), inputs[i].PkScript...,
		)
	}

	return cloned
}

func cloneRetryMsg(msg RescanActorMsg) (RescanActorMsg, error) {
	switch m := msg.(type) {
	case *processNextBlockMsg:
		return &processNextBlockMsg{}, nil

	case *subscriptionClosedMsg:
		return &subscriptionClosedMsg{Sub: m.Sub}, nil

	default:
		return nil, fmt.Errorf(
			"message type %T is not retryable", msg,
		)
	}
}
