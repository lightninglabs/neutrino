package headersync

import (
	"context"
	"time"
)

// PeerStateChanged tells the driver that a peer moved to a new scheduling
// state. The driver updates the peer's cooldown, then the peer actor.
type PeerStateChanged struct {
	PeerID string
	State  PeerState
}

func (PeerStateChanged) driverEvent() {}

// PeerRegistered tells the driver that a peer actor was just created for a
// peer. A peer that reconnects inside its cooldown is blocked again.
type PeerRegistered struct {
	PeerID string
}

func (PeerRegistered) driverEvent() {}

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

// handlePeerRegistered blocks a freshly registered peer that is still in its
// cooldown.
func (d *Driver) handlePeerRegistered(ctx context.Context,
	event PeerRegistered, result *DriverResult) {

	until, ok := d.session.CooldownUntil(event.PeerID, time.Now())
	if !ok {
		return
	}

	d.setPeerState(ctx, event.PeerID, PeerBlocked, result)
	log.Debugf("Registered headersync peer actor peer=%s in "+
		"cooldown_until=%s", event.PeerID, until.Format(time.RFC3339))
}

// handleCooldownWakeDue schedules the next cooldown wakeup.
func (d *Driver) handleCooldownWakeDue(result *DriverResult) {
	d.scheduleCooldownWake(result)
}

// setPeerState tracks the peer's cooldown, schedules a wakeup for it, and
// updates the peer actor.
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
		d.releaseOrphanedRange(ctx, peerID)
		d.setPeerState(ctx, peerID, PeerReady, result)
	}
}

// releaseOrphanedRange returns the active range of a peer that has no request
// outstanding to the queue. An ask that applied but reported an error can leave
// such a lease behind. While the peer was blocked, another peer could steal it;
// with no other peer, recovering the peer to ready would otherwise leave the
// range active forever with nothing running it.
func (d *Driver) releaseOrphanedRange(ctx context.Context, peerID string) {
	manager := d.runtime.Manager()
	peer, ok, err := manager.SnapshotPeer(ctx, peerID)
	if err != nil || !ok || peer.ActiveRange == NoRange {
		return
	}
	rng, ok, err := manager.SnapshotRange(ctx, peer.ActiveRange)
	if err != nil || !ok || rng.State != RangeActive ||
		rng.OwnerPeer != peerID {

		return
	}

	// Fail the lease under its current epoch, then plan the span again.
	// Go through the peer actor when there is one: it still holds the
	// range as active, and failing it only in the manager would leave the
	// actor busy forever, answering every later request with nothing.
	var result CompleteResult
	if peerActor, ok := d.runtime.Peer(peerID); ok {
		result, err = peerActor.CompleteRange(
			ctx, rng.ID, rng.LeaseEpoch, false, time.Now(),
		)
	} else {
		result, err = manager.CompleteRange(
			ctx, peerID, rng.ID, rng.LeaseEpoch, false, time.Now(),
		)
	}
	if err != nil || !result.Accepted {
		return
	}
	log.Infof("headersync orphaned_range_released peer=%s range_id=%d",
		peerID, rng.ID)
	d.replanRange(ctx, rng)
}

// replanRange plans a failed range again.
func (d *Driver) replanRange(ctx context.Context, rng HeaderRange) {
	result, err := d.runtime.Controller().ReplanRange(ctx, rng)
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
