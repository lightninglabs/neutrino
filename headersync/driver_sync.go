package headersync

// This file holds the decisions that start and continue parallel header sync:
// which path the node takes, when it plans, dispatches and discovers, and when
// it gives the sync peer up and asks the executor to choose one again. The
// executor writes the requests in between, and the driver picks the decision
// up again through the follow-up events of a Then effect.

import "context"

// BeginSync asks the driver to start syncing from the candidate the executor
// chose. The driver takes the parallel path when the network allows it and
// there is something to plan or discover, and otherwise asks for the serial
// path through StartSerialSync.
type BeginSync struct {
	// BestPeerID is the chosen peer.
	BestPeerID string

	// BestHeight is the height the peer advertises, or zero when it
	// advertises none, which keeps the node on the serial path.
	BestHeight uint32

	// TipHeight is the durable block header tip the executor read when it
	// chose the peer. A serial request starts there.
	TipHeight uint32
}

func (BeginSync) driverEvent() {}

// Continue asks the driver to carry on after something changed: a peer came
// or went, a commit landed, a cooldown ended. The driver plans, dispatches and
// restarts discovery, and when nothing is left in flight it releases the sync
// peer and asks the executor to start again.
type Continue struct{}

func (Continue) driverEvent() {}

// ResumeWork is Continue without the fresh start. The executor sends it while
// a serial request is in flight, since starting again could reselect the sync
// peer under that request. Dispatch alone would leave a span that waited on
// cooling peers without any discovery to restart it.
type ResumeWork struct{}

func (ResumeWork) driverEvent() {}

// DispatchWork asks the driver to hand queued ranges to the candidates and to
// start one request per idle peer. Each peer's request goes out before the
// next peer is considered, since a failed send blocks the peer and changes
// what the next one may steal from it.
type DispatchWork struct{}

func (DispatchWork) driverEvent() {}

// CooldownElapsed reports that the executor's cooldown wakeup fired.
type CooldownElapsed struct {
	// SerialRequestPending is set while a serial getheaders request is in
	// flight, which rules out a fresh start.
	SerialRequestPending bool
}

func (CooldownElapsed) driverEvent() {}

// SetSyncPeer asks the executor to record PeerID as the sync peer, or none
// when it is empty. The executor moves the tip-following lease with it.
type SetSyncPeer struct {
	// PeerID is the new sync peer, or empty for none.
	PeerID string

	// UnlessGone leaves the sync peer alone when PeerID has disconnected
	// in the meantime, which only the executor can see.
	UnlessGone bool
}

func (SetSyncPeer) driverEffect() {}

// StartSync asks the executor to choose a sync peer among the candidates and
// start syncing from it. It re-enters the driver, so it is always the last
// effect of its batch.
type StartSync struct{}

func (StartSync) driverEffect() {}

// StartSerialSync asks the executor to start the serial path from the peer:
// make it the sync peer and ask it for the headers after the tip, up to the
// stop hash.
type StartSerialSync struct {
	// PeerID is the peer to sync from.
	PeerID string

	// StartHeight is the durable tip the request starts from.
	StartHeight uint32

	// StopHash is the hash of the next checkpoint, or zero for a request
	// up to the peer's tip.
	StopHash Hash

	// StopHeight is the height of the next checkpoint, or zero when the
	// request runs up to the peer's tip.
	StopHeight uint32
}

func (StartSerialSync) driverEffect() {}

// ClearBootstrapDelay tells the executor that parallel sync has started, so
// it can drop its cold start admission delay.
type ClearBootstrapDelay struct{}

func (ClearBootstrapDelay) driverEffect() {}

// syncDiscovered follows the discovery round of BeginSync, once its requests
// are written.
type syncDiscovered struct {
	Sync  BeginSync
	Round DiscoveryOutcome
}

func (syncDiscovered) driverEvent() {}

// syncDispatched follows the dispatch of BeginSync, once its requests are
// written. Planned is the number of ranges planned, with a running discovery
// counting as one.
type syncDispatched struct {
	Sync    BeginSync
	Planned int
}

func (syncDispatched) driverEvent() {}

// continueDiscover follows the dispatch of Continue or ResumeWork, and starts
// discovery.
type continueDiscover struct {
	BestHeight    uint32
	StartWhenIdle bool
}

func (continueDiscover) driverEvent() {}

// continueDiscovered follows the discovery round of Continue or ResumeWork,
// once its requests are written.
type continueDiscovered struct {
	Round         DiscoveryOutcome
	StartWhenIdle bool
}

func (continueDiscovered) driverEvent() {}

// then returns a Then effect that re-enters the driver with the events.
func then(events ...DriverEvent) DriverEffect {
	return Then{Events: events}
}

// handleBeginSync decides between the parallel and the serial path.
func (d *Driver) handleBeginSync(ctx context.Context, event BeginSync,
	view View) []DriverEffect {

	var effects []DriverEffect

	// The multi-peer scheduler needs multiple independent peers to
	// confirm discovered anchors. The local sim/reg test harnesses often
	// run with a single peer, so keep those networks on the serial path.
	if event.BestHeight == 0 || d.cfg.SerialFallback {
		effects = append(effects, d.startSerialSync(event)...)

		return effects
	}

	planned, err := d.planRanges(ctx, event.BestHeight)
	if err != nil {
		log.Warnf("headersync failed to plan checkpoint ranges: %v",
			err)
		effects = append(effects, d.startSerialSync(event)...)

		return effects
	}
	if planned > 0 {
		effects = append(effects, d.dispatchSync(event, planned)...)

		return effects
	}

	discoverEffects, round := d.startDiscovery(
		ctx, view, event.BestHeight,
	)
	effects = append(effects, discoverEffects...)
	effects = append(effects, then(
		syncDiscovered{Sync: event, Round: round},
	))

	return effects
}

// handleSyncDiscovered counts a running discovery as planned work, then
// dispatches.
func (d *Driver) handleSyncDiscovered(event syncDiscovered) []DriverEffect {
	var effects []DriverEffect

	var planned int
	if d.discoveryActive(event.Round) {
		planned = 1
	}

	effects = append(effects, d.dispatchSync(event.Sync, planned)...)

	return effects
}

// dispatchSync makes the chosen peer the sync peer and dispatches the planned
// work, then judges whether the parallel path took the sync.
func (d *Driver) dispatchSync(sync BeginSync, planned int) []DriverEffect {
	var effects []DriverEffect

	effects = append(effects, SetSyncPeer{
		PeerID: sync.BestPeerID,
	})
	effects = append(effects, then(DispatchWork{}, syncDispatched{
		Sync: sync, Planned: planned,
	}))

	return effects
}

// handleSyncDispatched keeps the parallel path when it planned, discovered or
// started anything, and otherwise gives the sync peer back and falls back to
// the serial path.
func (d *Driver) handleSyncDispatched(event syncDispatched) []DriverEffect {
	var effects []DriverEffect

	if d.session.HasActiveWork() || event.Planned > 0 {
		effects = append(effects, ClearBootstrapDelay{})

		return effects
	}

	if d.runtime.manager.TrustsTipPeer(event.Sync.BestPeerID) {
		effects = append(effects, SetSyncPeer{})
	}

	effects = append(effects, d.startSerialSync(event.Sync)...)

	return effects
}

// startSerialSync asks the executor for the serial path, aimed at the next
// checkpoint while the chain is still within the checkpoints.
func (d *Driver) startSerialSync(sync BeginSync) []DriverEffect {
	var effects []DriverEffect

	effect := StartSerialSync{
		PeerID:      sync.BestPeerID,
		StartHeight: sync.TipHeight,
	}
	next := d.tip.NextCheckpoint()
	if next != nil && int32(sync.TipHeight) < next.Height {
		effect.StopHash = *next.Hash
		effect.StopHeight = uint32(next.Height)
	}

	effects = append(effects, effect)

	return effects
}

// resumeWork plans ranges, dispatches queued work and restarts discovery.
// With startWhenIdle, it releases the sync peer and asks for a fresh start
// once nothing is in flight.
func (d *Driver) resumeWork(ctx context.Context, view View,
	startWhenIdle bool) []DriverEffect {

	var effects []DriverEffect

	// The asynchronous commit in flight owns the header tip, and planning
	// before it settles would build on a stale tip.
	if d.commitPending {
		return effects
	}

	best := view.bestHeight()
	if _, err := d.planRanges(ctx, best); err != nil {
		log.Warnf("headersync failed to plan follow-up ranges: %v", err)
	}

	effects = append(effects, then(DispatchWork{}, continueDiscover{
		BestHeight: best, StartWhenIdle: startWhenIdle,
	}))

	return effects
}

// handleContinueDiscover restarts discovery once the queued work is
// dispatched.
func (d *Driver) handleContinueDiscover(ctx context.Context,
	event continueDiscover, view View) []DriverEffect {

	var effects []DriverEffect

	discoverEffects, round := d.startDiscovery(
		ctx, view, event.BestHeight,
	)
	effects = append(effects, discoverEffects...)
	effects = append(effects, then(continueDiscovered{
		Round: round, StartWhenIdle: event.StartWhenIdle,
	}))

	return effects
}

// handleContinueDiscovered asks for a fresh start when nothing is in flight.
func (d *Driver) handleContinueDiscovered(
	event continueDiscovered) []DriverEffect {

	var effects []DriverEffect

	active := d.discoveryActive(event.Round)
	if !event.StartWhenIdle || active || d.session.HasActiveWork() {
		return effects
	}

	effects = append(effects, d.restartSync()...)

	return effects
}

// restartSync releases the sync peer and asks the executor to choose one
// again.
func (d *Driver) restartSync() []DriverEffect {
	var effects []DriverEffect

	effects = append(effects, SetSyncPeer{}, StartSync{})

	return effects
}

// handleCooldownElapsed carries on with the recovered peers, then arms the
// next wakeup for any still cooling down.
func (d *Driver) handleCooldownElapsed(event CooldownElapsed) []DriverEffect {
	var effects []DriverEffect

	var resume DriverEvent = Continue{}
	if event.SerialRequestPending {
		resume = ResumeWork{}
	}

	effects = append(effects, then(resume, CooldownWakeDue{}))

	return effects
}

// handleDispatchWork recovers and assigns, then starts the peers one at a
// time.
func (d *Driver) handleDispatchWork(ctx context.Context,
	view View) []DriverEffect {

	var effects []DriverEffect

	effects = append(effects, d.handleRecoverPeers(
		ctx, RecoverPeers{Peers: view.Peers}, //nolint:gosimple
	)...)
	d.handleAssignRanges(ctx, AssignRanges{PeerCount: len(view.Peers)})

	if len(view.Peers) == 0 {
		return effects
	}
	events := make([]DriverEvent, 0, len(view.Peers))
	for _, peer := range view.Peers {
		events = append(events, DispatchPeer{PeerID: peer.ID})
	}

	effects = append(effects, then(events...))

	return effects
}

// planRanges plans the ranges between the durable tip and the next boundary,
// and reports how many it planned.
func (d *Driver) planRanges(ctx context.Context,
	bestHeight uint32) (int, error) {

	// Planning moves the FSM to the durable tip, so bring the cache and
	// the next checkpoint along first. Otherwise a write that landed but
	// reported an error leaves planning aimed at a stale checkpoint.
	if err := d.tip.ReconcileWithStore(); err != nil {
		return 0, err
	}

	header, height, err := d.tip.StoreTip()
	if err != nil {
		return 0, err
	}

	return d.planRangesFrom(ctx, ChainPoint{
		Height: height,
		Hash:   header.BlockHash(),
	}, d.nextCheckpointPoint(), bestHeight)
}

// nextCheckpointPoint returns the next checkpoint as a chain point, or nil
// when the chain is past its last checkpoint.
func (d *Driver) nextCheckpointPoint() *ChainPoint {
	next := d.tip.NextCheckpoint()
	if next == nil {
		return nil
	}

	return &ChainPoint{
		Height: uint32(next.Height),
		Hash:   *next.Hash,
	}
}

// startDiscovery plans the discovery of the next checkpoint's anchors, or of
// the next boundary above the tip once the chain is past its checkpoints, and
// asks for its requests. The caller judges the round once they are written.
func (d *Driver) startDiscovery(ctx context.Context, view View,
	bestHeight uint32) ([]DriverEffect, DiscoveryOutcome) {

	var effects []DriverEffect

	effects = append(effects, d.handleRecoverPeers(
		ctx, RecoverPeers{Peers: view.Peers}, //nolint:gosimple
	)...)

	checkpoint := d.nextCheckpointPoint()
	kind := "frontier"
	if checkpoint != nil {
		kind = "anchor"
	}

	header, height, err := d.tip.StoreTip()
	if err != nil {
		log.Warnf("headersync unable to get chain tip for %s "+
			"discovery: %v", kind, err)

		return effects, DiscoveryOutcome{Settled: true}
	}
	tip := ChainPoint{Height: height, Hash: header.BlockHash()}

	if checkpoint != nil {
		discoverEffects, round := d.discoverAnchors(
			ctx, tip, *checkpoint, bestHeight, view.Peers,
		)

		return append(effects, discoverEffects...), round
	}

	discoverEffects, round := d.discoverFrontier(
		ctx, tip, bestHeight, view.Peers,
	)

	return append(effects, discoverEffects...), round
}

// discoveryActive reports whether anchor discovery is running after the
// requests of a round were written. A settled round decided that on its own.
// Otherwise the session holds one request per write that succeeded, on top of
// the ones the round found already in flight.
func (d *Driver) discoveryActive(round DiscoveryOutcome) bool {
	if round.Settled {
		return round.Active
	}

	span := round.Span
	active := d.session.MatchingActiveAnchors(
		span.StartHeight, span.StartHash, span.StopHeight,
	)
	if started := active - round.ActivePeers; started > 0 {
		log.Infof("headersync anchor_discovery_started peers=%d "+
			"active_peers=%d start_height=%d stop_height=%d",
			started, round.ActivePeers, span.StartHeight,
			span.StopHeight)
	}

	return active > 0
}
