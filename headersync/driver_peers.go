package headersync

import (
	"context"
	"time"
)

// PeerStateChanged tells the driver that a peer moved to a new scheduling
// state. The driver updates the peer's cooldown, then the peer.
type PeerStateChanged struct {
	PeerID string
	State  PeerState
}

func (PeerStateChanged) driverEvent() {}

// PeerConnected registers a peer with the driver. If it's already registered,
// its RTT is refreshed instead.
type PeerConnected struct {
	// Peer is the peer's scheduling snapshot.
	Peer PeerSnapshot
}

func (PeerConnected) driverEvent() {}

// PeerDisconnected removes a peer from the driver.
type PeerDisconnected struct {
	// PeerID is the peer's address.
	PeerID string
}

func (PeerDisconnected) driverEvent() {}

// PeerRTTUpdated reports a new round trip time for a peer.
type PeerRTTUpdated struct {
	// PeerID is the peer's address.
	PeerID string

	// RTT is the new round trip time.
	RTT time.Duration
}

func (PeerRTTUpdated) driverEvent() {}

// RecoverPeers asks the driver to return every candidate peer whose cooldown
// has expired and that has no request in flight to the ready state. A lease
// that such a peer still holds without a request is released first.
type RecoverPeers struct {
	Peers []PeerCandidate
}

func (RecoverPeers) driverEvent() {}

// PeerDone tells the driver that a peer disconnected. The driver abandons the
// peer's in-flight requests and quarantines it if there were any.
type PeerDone struct {
	PeerID string
}

func (PeerDone) driverEvent() {}

// CooldownWakeDue asks the driver to schedule the next cooldown wakeup. The
// executor sends it when the previous wakeup fired.
type CooldownWakeDue struct{}

func (CooldownWakeDue) driverEvent() {}

// ArmCooldownWake asks the executor to arm one wakeup at the earliest peer
// cooldown expiry, unless a wakeup is already armed. Cooled-down peers are
// only recovered while dispatching. Without a wakeup, a sync waiting on a
// blocked peer (for example the second anchor observation under a two-peer
// policy) has no request or timer outstanding and never dispatches again.
type ArmCooldownWake struct {
	Until time.Time
}

func (ArmCooldownWake) driverEffect() {}

// handlePeerStateChanged applies a peer state change.
func (d *Driver) handlePeerStateChanged(ctx context.Context,
	event PeerStateChanged) []DriverEffect {

	var effects []DriverEffect

	effects = append(effects, d.setPeerState(
		ctx, event.PeerID, event.State,
	)...)

	return effects
}

// handlePeerConnected registers a peer, and blocks it if it's still in its
// cooldown.
func (d *Driver) handlePeerConnected(ctx context.Context,
	event PeerConnected) ([]DriverEffect, error) {

	var effects []DriverEffect

	peer := event.Peer
	_, created, err := d.runtime.RegisterPeer(ctx, peer)
	if err != nil {
		return effects, err
	}

	if !created {
		return effects, d.handlePeerRTTUpdated(ctx, PeerRTTUpdated{
			PeerID: peer.ID,
			RTT:    peer.RTT,
		})
	}

	until, ok := d.session.CooldownUntil(peer.ID, time.Now())
	if !ok {
		log.Debugf("Registered headersync peer peer=%s rank=%d rtt=%v",
			peer.ID, peer.Rank, peer.RTT)

		return effects, nil
	}

	effects = append(effects, d.setPeerState(ctx, peer.ID, PeerBlocked)...)
	log.Debugf("Registered headersync peer peer=%s in "+
		"cooldown_until=%s", peer.ID, until.Format(time.RFC3339))

	return effects, nil
}

// handlePeerDisconnected removes a peer.
func (d *Driver) handlePeerDisconnected(ctx context.Context,
	event PeerDisconnected) error {

	return d.runtime.UnregisterPeer(ctx, event.PeerID)
}

// handlePeerRTTUpdated updates a peer's round trip time.
func (d *Driver) handlePeerRTTUpdated(ctx context.Context,
	event PeerRTTUpdated) error {

	return d.runtime.UpdatePeerRTT(ctx, event.PeerID, event.RTT)
}

// handleCooldownWakeDue schedules the next cooldown wakeup.
func (d *Driver) handleCooldownWakeDue() []DriverEffect {
	var effects []DriverEffect

	effects = append(effects, d.scheduleCooldownWake()...)

	return effects
}

// setPeerState tracks the peer's cooldown, schedules a wakeup for it, and
// updates the peer.
func (d *Driver) setPeerState(ctx context.Context, peerID string,
	state PeerState) []DriverEffect {

	var effects []DriverEffect

	d.session.TrackPeerCooldown(
		peerID, state, time.Now(), DefaultPeerCooldown,
	)
	effects = append(effects, d.scheduleCooldownWake()...)

	err := d.runtime.UpdatePeerState(ctx, peerID, state)
	if err != nil {
		log.Warnf("Unable to update headersync peer state for %s: %v",
			peerID, err)
	}

	return effects
}

// scheduleCooldownWake asks for a wakeup at the earliest cooldown expiry, if
// any peer is still cooling down.
func (d *Driver) scheduleCooldownWake() []DriverEffect {
	var effects []DriverEffect

	until, ok := d.session.NextCooldownExpiry(time.Now())
	if !ok {
		return effects
	}

	effects = append(effects, ArmCooldownWake{Until: until})

	return effects
}

// handleRecoverPeers moves peers whose cooldown has expired back to ready.
func (d *Driver) handleRecoverPeers(ctx context.Context,
	event RecoverPeers) []DriverEffect {

	var effects []DriverEffect

	if d.session.CooldownCount() == 0 {
		return effects
	}

	now := time.Now()
	for _, peer := range event.Peers {
		peerID := peer.ID
		until, ok := d.session.CooldownExpired(peerID, now)
		if !ok {
			continue
		}
		if d.session.RangeActive(peerID) {
			continue
		}
		if d.session.AnchorActive(peerID) {
			continue
		}

		log.Infof("headersync peer_recovered peer=%s "+
			"cooldown_until=%s",
			peerID, until.Format(time.RFC3339))
		effects = append(effects, d.setPeerState(
			ctx, peerID, PeerReady,
		)...)
	}

	return effects
}

// replanRange plans a failed range again.
func (d *Driver) replanRange(ctx context.Context, rng HeaderRange) {
	result, err := d.runtime.controller.ReplanRange(ctx, rng)
	if err != nil {
		log.Warnf("headersync unable to replan failed range_id=%d: %v",
			rng.ID, err)
		return
	}
	if !result.OK {
		log.Warnf("headersync failed range_id=%d was not replanned",
			rng.ID)
		return
	}

	log.Infof("headersync range_replanned old_range_id=%d "+
		"new_range_id=%d start_height=%d stop_height=%d", rng.ID,
		result.ID, result.Range.StartHeight, result.Range.StopHeight)
}

// handlePeerDone abandons the requests of a disconnected peer. It reports
// whether the peer had a request in flight.
func (d *Driver) handlePeerDone(ctx context.Context,
	event PeerDone) ([]DriverEffect, bool) {

	var effects []DriverEffect

	var hadWork bool
	peerID := event.PeerID
	if _, ok := d.session.FinishRange(peerID); ok {
		d.session.ClearStale(peerID)
		effects = append(effects, d.setPeerState(
			ctx, peerID, PeerQuarantined,
		)...)
		hadWork = true
	}
	if _, ok := d.session.FinishAnchor(peerID); ok {
		effects = append(effects, d.setPeerState(
			ctx, peerID, PeerQuarantined,
		)...)
		hadWork = true
	}

	return effects, hadWork
}

// PeerGone tells the driver that a peer disconnected and the executor dropped
// it from the candidates. The driver abandons its requests, removes it, and
// when it was the sync peer, resets the cache to the durable tip and asks for
// a fresh start.
type PeerGone struct {
	// PeerID is the peer's address.
	PeerID string
}

func (PeerGone) driverEvent() {}

// handlePeerGone abandons a disconnected peer's requests and removes it, then
// decides what the loss means for the sync.
func (d *Driver) handlePeerGone(ctx context.Context,
	event PeerGone) []DriverEffect {

	var effects []DriverEffect

	peerID := event.PeerID
	doneEffects, hadWork := d.handlePeerDone(ctx, PeerDone{PeerID: peerID})
	effects = append(effects, doneEffects...)

	err := d.runtime.UnregisterPeer(ctx, peerID)
	if err != nil {
		log.Warnf("Unable to remove headersync peer %s: %v", peerID,
			err)
	} else {
		log.Debugf("Unregistered headersync peer peer=%s", peerID)
	}

	// Attempt to find a new peer to sync from if the quitting peer is the
	// sync peer. Also, reset the header state.
	if d.runtime.manager.TrustsTipPeer(peerID) {
		effects = append(effects, SetSyncPeer{})
		if d.commitPending {
			return effects
		}

		// Reset the cache and the visible tip together, so they keep
		// describing the same header.
		if err := d.resetTipToStore(ctx); err != nil {
			return effects
		}
		effects = append(effects, StartSync{})

		return effects
	}

	if hadWork && !d.commitPending {
		effects = append(effects, then(Continue{}))
	}

	return effects
}
