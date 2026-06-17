package protofsm

import "github.com/lightningnetwork/lnd/fn/v2"

// MsgMapper maps an outside message into an FSM event. The upstream lnd
// package maps msgmux.PeerMsg instances; this isolated copy accepts any
// message so neutrino can bind its own p2p/query/test-harness message types
// without importing lnd's msgmux package.
type MsgMapper[Event any] interface {
	// MapMsg maps a message into an FSM event. If the message is not
	// meaningful for the target FSM, None should be returned.
	MapMsg(msg any) fn.Option[Event]
}

// MsgMapperFunc adapts a function into a MsgMapper.
type MsgMapperFunc[Event any] func(any) fn.Option[Event]

// MapMsg implements MsgMapper.
func (m MsgMapperFunc[Event]) MapMsg(msg any) fn.Option[Event] {
	return m(msg)
}
