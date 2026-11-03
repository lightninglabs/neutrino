package headersync

import (
	"context"
	"time"
)

// TimeoutOutcome is the result of a RequestTimedOut event.
type TimeoutOutcome struct {
	// Matched is set when the timeout is for the request the peer still has
	// in flight. A timeout for a request that was answered or replaced
	// since is dropped.
	Matched bool

	// Anchor is the abandoned request, when an anchor request timed out.
	Anchor AnchorRequest
}

// RequestTimedOut reports that the deadline of a range or anchor request
// fired. A matching timeout quarantines and disconnects the peer.
type RequestTimedOut struct {
	PeerID      string
	Kind        RequestKind
	RequestID   uint64
	StartHeight uint32
	RangeID     RangeID
	LeaseEpoch  uint64

	// Timeout is the deadline that elapsed.
	Timeout time.Duration

	// StaleFor is how long a late reply to a timed-out range request is
	// still recognized as stale instead of as unsolicited.
	StaleFor time.Duration
}

func (RequestTimedOut) driverEvent() {}

// QuarantinePeer asks the executor to quarantine a peer's address for a short
// time, so that the rest of the node stops using it.
type QuarantinePeer struct {
	PeerID string
}

func (QuarantinePeer) driverEffect() {}

// DisconnectPeer asks the executor to drop a peer from the sync candidates
// and disconnect it.
type DisconnectPeer struct {
	PeerID string
}

func (DisconnectPeer) driverEffect() {}

// handleRequestTimedOut classifies a timeout and, if it matches, punishes the
// peer.
func (d *Driver) handleRequestTimedOut(ctx context.Context,
	event RequestTimedOut, result *DriverResult) {

	peerID := event.PeerID
	switch event.Kind {
	case RequestAnchor:
		request, ok := d.session.TimeoutAnchor(
			peerID, event.RequestID, event.StartHeight,
		)
		if !ok {
			return
		}

		log.Warnf("headersync anchor_timeout peer=%s start_height=%d "+
			"stop_height=%d timeout=%v", peerID,
			request.StartHeight, request.StopHeight, event.Timeout)
		result.Timeout = TimeoutOutcome{Matched: true, Anchor: request}

	case RequestRange:
		request, ok := d.session.TimeoutRange(
			peerID, event.RequestID, event.RangeID,
			event.LeaseEpoch,
		)
		if !ok {
			return
		}

		d.session.MarkStale(peerID, StaleRequest{
			Assignment: request.Assignment,
			ExpiresAt:  time.Now().Add(event.StaleFor),
		})
		log.Warnf("headersync range_timeout peer=%s range_id=%d "+
			"epoch=%d start_height=%d timeout=%v", peerID,
			event.RangeID, event.LeaseEpoch, event.StartHeight,
			event.Timeout)
		result.Timeout = TimeoutOutcome{Matched: true}
	}

	d.quarantinePeer(ctx, peerID, result)
	result.Effects = append(result.Effects, DisconnectPeer{PeerID: peerID})
}

// quarantinePeer blocks a peer from scheduling until its cooldown expires, and
// asks for the address quarantine.
func (d *Driver) quarantinePeer(ctx context.Context, peerID string,
	result *DriverResult) {

	d.setPeerState(ctx, peerID, PeerQuarantined, result)
	result.Effects = append(result.Effects, QuarantinePeer{PeerID: peerID})
}

// SerialTimedOut reports that the sync peer did not answer its serial
// getheaders request in time. The executor sends it only for the request
// still in flight, from the current sync peer, while headers are not synced.
type SerialTimedOut struct {
	// PeerID is the sync peer's address.
	PeerID string

	// StartHeight is the tip the request started from.
	StartHeight uint32

	// Timeout is the deadline that elapsed.
	Timeout time.Duration
}

func (SerialTimedOut) driverEvent() {}

// handleSerialTimedOut drops a stalled sync peer and starts over, unless the
// chain moved past the request in the meantime.
func (d *Driver) handleSerialTimedOut(ctx context.Context,
	event SerialTimedOut, view View, result *DriverResult) {

	var height uint32
	if view.CommitPending {
		height = d.cfg.PublishedTip()
	} else {
		_, currentHeight, err := d.tip.StoreTip()
		if err != nil {
			log.Errorf("Failed to get block header chain tip "+
				"while handling header sync timeout: %v", err)
			return
		}
		height = currentHeight
	}
	if height > event.StartHeight {
		return
	}

	peerID := event.PeerID
	log.Warnf("Header sync from peer %s stalled for %v at height %d; "+
		"quarantining peer and selecting a new sync peer",
		peerID, event.Timeout, height)

	// A slow or stalled peer is quarantined, never IP-banned: on regtest
	// and behind NAT or Tor one address can be the only usable peer.
	d.quarantinePeer(ctx, peerID, result)
	result.Effects = append(result.Effects, DisconnectPeer{PeerID: peerID})

	if d.runtime.manager.TrustsTipPeer(peerID) {
		result.Effects = append(result.Effects, SetSyncPeer{})
	}

	if view.CommitPending {
		return
	}

	if err := d.resetTipToStore(ctx); err != nil {
		log.Errorf("Failed to reset header state after header sync "+
			"timeout: %v", err)
		return
	}

	result.Effects = append(result.Effects, StartSync{})
}
