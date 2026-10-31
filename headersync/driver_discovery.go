package headersync

import "context"

// DiscoveryOutcome is the result of an event that plans anchor discovery.
type DiscoveryOutcome struct {
	// Settled is set when the event decided the outcome without asking for
	// any request. Active then tells whether discovery is running or
	// waiting on peers that are. Otherwise the effects hold the requests
	// to send, and ActivePeers counts the requests already in flight, so
	// that the executor can tell once it knows how many sends succeeded.
	Settled     bool
	Active      bool
	ActivePeers int

	// Span is the span the requests ask about.
	Span AnchorRequest
}

// DiscoverAnchors asks the driver to discover the anchors of the span between
// the durable tip and the next checkpoint.
type DiscoverAnchors struct {
	Tip        ChainPoint
	Stop       ChainPoint
	BestHeight uint32
	Peers      []PeerCandidate
}

func (DiscoverAnchors) driverEvent() {}

// DiscoverFrontier asks the driver to discover the next boundary above the
// durable tip when there is no checkpoint left to aim at.
type DiscoverFrontier struct {
	Tip        ChainPoint
	BestHeight uint32
	Peers      []PeerCandidate
}

func (DiscoverFrontier) driverEvent() {}

// PipelineFrontier asks the driver to discover the span above a boundary that
// was just learned, overlapping that discovery with the commit of the span
// below it.
type PipelineFrontier struct {
	After      ChainPoint
	BestHeight uint32
	Peers      []PeerCandidate
}

func (PipelineFrontier) driverEvent() {}

// DiscoverFrom asks the driver to discover the anchor at the end of a span
// again, leaving the excluded peers out of the round.
type DiscoverFrom struct {
	Span     AnchorRequest
	Peers    []PeerCandidate
	Excluded []string
}

func (DiscoverFrom) driverEvent() {}

// handleDiscoverAnchors plans the discovery of the next checkpoint's anchors.
func (d *Driver) handleDiscoverAnchors(ctx context.Context,
	event DiscoverAnchors, result *DriverResult) {

	span, ok, err := d.runtime.Controller().AnchorDiscoverySpan(
		ctx, event.Tip, event.Stop, event.BestHeight,
	)
	if err != nil {
		log.Warnf("headersync unable to plan anchor discovery: %v", err)
		result.Discovery = DiscoveryOutcome{Settled: true}
		return
	}
	if !ok {
		result.Discovery = DiscoveryOutcome{Settled: true}
		return
	}

	d.discoverFrom(span, event.Peers, nil, result)
}

// handleDiscoverFrontier plans the discovery of the next boundary above the
// tip.
func (d *Driver) handleDiscoverFrontier(ctx context.Context,
	event DiscoverFrontier, result *DriverResult) {

	span, ok, err := d.runtime.Controller().FrontierDiscoverySpan(
		ctx, event.Tip, event.BestHeight,
	)
	if err != nil {
		log.Warnf(
			"headersync unable to plan frontier discovery: %v",
			err,
		)
		result.Discovery = DiscoveryOutcome{Settled: true}
		return
	}
	if !ok {
		result.Discovery = DiscoveryOutcome{Settled: true}
		return
	}

	log.Debugf("headersync frontier_discovery_needed height=%d "+
		"target_height=%d start_height=%d stop_height=%d",
		event.Tip.Height, event.BestHeight, span.StartHeight,
		span.StopHeight)

	d.discoverFrom(span, event.Peers, nil, result)
}

// handlePipelineFrontier plans the discovery of the span above a boundary
// that was just learned.
func (d *Driver) handlePipelineFrontier(event PipelineFrontier,
	result *DriverResult) {

	span, ok := d.runtime.Controller().FrontierLookaheadSpan(
		event.After, event.BestHeight,
	)
	if !ok {
		result.Discovery = DiscoveryOutcome{Settled: true}
		return
	}

	log.Debugf("headersync frontier_discovery_pipelined "+
		"start_height=%d stop_height=%d target_height=%d",
		span.StartHeight, span.StopHeight, event.BestHeight)

	d.discoverFrom(span, event.Peers, nil, result)
}

// discoverFrom asks every peer the plan selects for the anchor at the end of
// the span.
func (d *Driver) discoverFrom(span AnchorRequest, peers []PeerCandidate,
	excluded []string, result *DriverResult) {

	plan := d.runtime.Controller().PlanAnchorRequests(
		span, peers, excluded...,
	)
	if plan.Deferred && len(plan.Peers) == 0 {
		log.Debugf(
			"headersync anchor_discovery_deferred "+
				"eligible_peers=%d "+
				"active_peers=%d required_peers=%d "+
				"start_height=%d "+
				"stop_height=%d",
			plan.EligiblePeers,
			plan.ActivePeers,
			plan.RequiredPeers,
			span.StartHeight,
			span.StopHeight,
		)

		result.Discovery = DiscoveryOutcome{
			Settled: true,
			Active:  plan.Active(),
		}
		return
	}

	for _, peer := range plan.Peers {
		result.Effects = append(result.Effects, SendGetHeaders{
			PeerID:      peer.ID,
			Kind:        RequestAnchor,
			StartHeight: span.StartHeight,
			StartHash:   span.StartHash,
			StopHeight:  span.StopHeight,
			StopHash:    span.StopHash,
		})
	}
	result.Discovery = DiscoveryOutcome{
		ActivePeers: plan.ActivePeers,
		Span:        span,
	}
}
