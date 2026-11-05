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
	event PeerStateChanged, result *DriverResult) {

	d.setPeerState(ctx, event.PeerID, event.State, result)
}

// handlePeerConnected registers a peer, and blocks it if it's still in its
// cooldown. It reports in result whether the peer is new.
func (d *Driver) handlePeerConnected(ctx context.Context,
	event PeerConnected, result *DriverResult) error {

	peer := event.Peer
	_, created, err := d.runtime.RegisterPeer(ctx, peer)
	if err != nil {
		return err
	}
	result.PeerCreated = created

	if !created {
		return d.handlePeerRTTUpdated(ctx, PeerRTTUpdated{
			PeerID: peer.ID,
			RTT:    peer.RTT,
		})
	}

	until, ok := d.session.CooldownUntil(peer.ID, time.Now())
	if !ok {
		return nil
	}

	d.setPeerState(ctx, peer.ID, PeerBlocked, result)
	log.Debugf("Registered headersync peer peer=%s in "+
		"cooldown_until=%s", peer.ID, until.Format(time.RFC3339))

	return nil
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
func (d *Driver) handleCooldownWakeDue(result *DriverResult) {
	d.scheduleCooldownWake(result)
}

// setPeerState tracks the peer's cooldown, schedules a wakeup for it, and
// updates the peer.
func (d *Driver) setPeerState(ctx context.Context, peerID string,
	state PeerState, result *DriverResult) {

	d.session.TrackPeerCooldown(
		peerID, state, time.Now(), DefaultPeerCooldown,
	)
	d.scheduleCooldownWake(result)

	err := d.runtime.UpdatePeerState(ctx, peerID, state)
	if err != nil {
		log.Warnf("Unable to update headersync peer state for %s: %v",
			peerID, err)
	}
}

// scheduleCooldownWake asks for a wakeup at the earliest cooldown expiry, if
// any peer is still cooling down.
func (d *Driver) scheduleCooldownWake(result *DriverResult) {
	until, ok := d.session.NextCooldownExpiry(time.Now())
	if !ok {
		return
	}

	result.Effects = append(result.Effects, ArmCooldownWake{Until: until})
}

// handleRecoverPeers moves peers whose cooldown has expired back to ready.
func (d *Driver) handleRecoverPeers(ctx context.Context, event RecoverPeers,
	result *DriverResult) {

	if d.session.CooldownCount() == 0 {
		return
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
		d.setPeerState(ctx, peerID, PeerReady, result)
	}
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

// handlePeerDone abandons the requests of a disconnected peer.
func (d *Driver) handlePeerDone(ctx context.Context, event PeerDone,
	result *DriverResult) {

	peerID := event.PeerID
	if _, ok := d.session.FinishRange(peerID); ok {
		d.session.ClearStale(peerID)
		d.setPeerState(ctx, peerID, PeerQuarantined, result)
		result.HadWork = true
	}
	if _, ok := d.session.FinishAnchor(peerID); ok {
		d.setPeerState(ctx, peerID, PeerQuarantined, result)
		result.HadWork = true
	}
}
