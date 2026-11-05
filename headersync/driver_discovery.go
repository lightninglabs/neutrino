package headersync

import "context"

// DiscoveryOutcome is what a discovery round leaves for the follow-up that
// judges it once its requests are written.
type DiscoveryOutcome struct {
	// Settled is set when the round decided the outcome without asking for
	// any request. Active then tells whether discovery is running or
	// waiting on peers that are. Otherwise the effects hold the requests
	// to send, and ActivePeers counts the requests already in flight, so
	// that the follow-up can tell once it knows how many sends succeeded.
	Settled     bool
	Active      bool
	ActivePeers int

	// Span is the span the requests ask about.
	Span AnchorRequest
}

// discoverAnchors plans the discovery of the next checkpoint's anchors in the
// span between the durable tip and the checkpoint.
func (d *Driver) discoverAnchors(ctx context.Context, tip, stop ChainPoint,
	bestHeight uint32, peers []PeerCandidate,
	result *DriverResult) DiscoveryOutcome {

	span, ok, err := d.runtime.controller.AnchorDiscoverySpan(
		ctx, tip, stop, bestHeight,
	)
	if err != nil {
		log.Warnf("headersync unable to plan anchor discovery: %v", err)

		return DiscoveryOutcome{Settled: true}
	}
	if !ok {
		return DiscoveryOutcome{Settled: true}
	}

	return d.discoverFrom(span, peers, nil, result)
}

// discoverFrontier plans the discovery of the next boundary above the durable
// tip, once there is no checkpoint left to aim at.
func (d *Driver) discoverFrontier(ctx context.Context, tip ChainPoint,
	bestHeight uint32, peers []PeerCandidate,
	result *DriverResult) DiscoveryOutcome {

	span, ok, err := d.runtime.controller.FrontierDiscoverySpan(
		ctx, tip, bestHeight,
	)
	if err != nil {
		log.Warnf(
			"headersync unable to plan frontier discovery: %v",
			err,
		)

		return DiscoveryOutcome{Settled: true}
	}
	if !ok {
		return DiscoveryOutcome{Settled: true}
	}

	log.Debugf("headersync frontier_discovery_needed height=%d "+
		"target_height=%d start_height=%d stop_height=%d",
		tip.Height, bestHeight, span.StartHeight, span.StopHeight)

	return d.discoverFrom(span, peers, nil, result)
}

// pipelineFrontier plans the discovery of the span above a boundary that was
// just learned, overlapping that discovery with the commit of the span below
// it.
func (d *Driver) pipelineFrontier(view View, after AnchorResponse,
	result *DriverResult) DiscoveryOutcome {

	bestHeight := view.bestHeight()
	span, ok := d.runtime.controller.FrontierLookaheadSpan(
		ChainPoint{Height: after.Height, Hash: after.Hash}, bestHeight,
	)
	if !ok {
		return DiscoveryOutcome{Settled: true}
	}

	log.Debugf("headersync frontier_discovery_pipelined "+
		"start_height=%d stop_height=%d target_height=%d",
		span.StartHeight, span.StopHeight, bestHeight)

	return d.discoverFrom(span, view.Peers, nil, result)
}

// discoverFrom asks every peer the plan selects for the anchor at the end of
// the span, leaving the excluded peers out of the round.
func (d *Driver) discoverFrom(span AnchorRequest, peers []PeerCandidate,
	excluded []string, result *DriverResult) DiscoveryOutcome {

	plan := d.runtime.controller.PlanAnchorRequests(
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

		return DiscoveryOutcome{
			Settled: true,
			Active:  plan.Active(),
		}
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

	return DiscoveryOutcome{
		ActivePeers: plan.ActivePeers,
		Span:        span,
	}
}
