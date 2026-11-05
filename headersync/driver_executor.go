package headersync

// This file holds what passes between the driver and its executor beyond the
// events of one handler: the configuration the executor hands the driver at
// construction, the view of executor-owned state that comes with every event,
// and the effect that asks the executor to run follow-up events.

import "time"

// DriverConfig is what the driver needs from the executor that runs it. The
// functions read executor state that changes after construction, so the
// driver reads them on each use instead of copying a value.
type DriverConfig struct {
	// Tip configures the validation cache the driver owns.
	Tip TipConfig

	// SerialFallback keeps the node on the serial header path. The
	// multi-peer scheduler needs several independent peers to confirm
	// discovered anchors, and the local simnet and regtest harnesses often
	// run with one, so those networks stay serial.
	SerialFallback bool

	// PublishedTip returns the height of the block header tip the chain
	// store has made visible. A commit trims the prefix of a range that is
	// already durable against it.
	PublishedTip func() uint32

	// RequestTimeout returns the base header request timeout. A rejected
	// anchor stays rejected, and a range that failed validation keeps its
	// peer out of assignment, for this long.
	RequestTimeout func() time.Duration

	// LogProgress reports a header that became durable to the executor's
	// progress logger.
	LogProgress func(timestamp time.Time, height int32)
}

// View is the executor-owned state the driver reads while handling an event.
// The executor builds a fresh one for every ProcessEvent call, including the
// follow-up events of a Then effect, so a decision made after an effect sees
// the state that effect produced. The driver never keeps a view across
// events.
type View struct {
	// Peers are the sync candidates, in the executor's order.
	Peers []PeerCandidate
}

// bestHeight returns the highest height a candidate advertises, or zero when
// no candidate advertises one.
func (v View) bestHeight() uint32 {
	var best uint32
	for _, peer := range v.Peers {
		if peer.Height < 0 {
			continue
		}
		if uint32(peer.Height) > best {
			best = uint32(peer.Height)
		}
	}

	return best
}

// Then asks the executor to handle the events in order, each with a fresh
// view and with its effects run before the next event. The driver emits it
// when the rest of a decision depends on the outcome of the effects before
// it: whether a request was written, or what the candidates look like once a
// peer is dropped. It re-enters the driver, so it is always the last effect
// of its batch.
type Then struct {
	// Events are the follow-up events, in order.
	Events []DriverEvent
}

func (Then) driverEffect() {}
