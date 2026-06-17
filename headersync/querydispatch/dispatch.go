package querydispatch

import (
	"context"
	"fmt"
	"time"

	"github.com/lightninglabs/neutrino/headersync"
	"github.com/lightninglabs/neutrino/querysync"
)

// Controller adapts querysync's actor-backed scheduler to headersync's generic
// dispatch interface. Header range validation, lease epochs, active timeout
// stealing, staging, and ordered commit stay in headersync.
type Controller struct {
	manager *querysync.ManagerActor
}

var _ headersync.DispatchController = (*Controller)(nil)

// New creates a querysync-backed header range dispatch controller.
func New(manager *querysync.ManagerActor) *Controller {
	return &Controller{manager: manager}
}

// AddPeer mirrors a header peer into the querysync scheduler.
func (c *Controller) AddPeer(ctx context.Context,
	peer headersync.PeerSnapshot) error {

	return c.manager.AddPeer(ctx, toQueryPeer(peer))
}

// RemovePeer removes a mirrored header peer.
func (c *Controller) RemovePeer(ctx context.Context, id string) error {
	return c.manager.RemovePeer(ctx, id)
}

// UpdatePeerState mirrors peer scheduling state.
func (c *Controller) UpdatePeerState(ctx context.Context, id string,
	state headersync.PeerState) error {

	return c.manager.UpdatePeerState(ctx, id, toQueryPeerState(state))
}

// UpdatePeerRTT mirrors peer RTT.
func (c *Controller) UpdatePeerRTT(ctx context.Context, id string,
	rtt time.Duration) error {

	return c.manager.UpdatePeerRTT(ctx, id, rtt)
}

// AssignRange asks querysync to assign unstarted header range ownership.
func (c *Controller) AssignRange(ctx context.Context,
	rangeID headersync.RangeID) (headersync.DispatchAssignment, bool,
	error) {

	assignment, ok, err := c.manager.AskAssignLocalTask(
		ctx, querysync.Task{ID: toTaskID(rangeID)},
	)
	if err != nil || !ok {
		return headersync.DispatchAssignment{}, ok, err
	}

	return headersync.DispatchAssignment{
		PeerID:  assignment.PeerID,
		RangeID: fromTaskID(assignment.TaskID),
	}, true, nil
}

// StartRange mirrors the peer's local queue after headersync has moved the
// range into the active slot, then marks the peer busy in querysync.
func (c *Controller) StartRange(ctx context.Context,
	peer headersync.PeerSnapshot) error {

	if err := c.SyncPeer(ctx, peer); err != nil {
		return err
	}

	return c.manager.UpdatePeerState(
		ctx, peer.ID, querysync.PeerBusy,
	)
}

// CompleteRange marks a mirrored active range finished.
func (c *Controller) CompleteRange(ctx context.Context,
	peerID string) error {

	return c.manager.CompletePeerWork(ctx, peerID)
}

// SyncPeer replaces the mirrored peer snapshot.
func (c *Controller) SyncPeer(ctx context.Context,
	peer headersync.PeerSnapshot) error {

	return c.manager.AddPeer(ctx, toQueryPeer(peer))
}

// StealRange asks querysync to move unstarted range ownership to the thief.
func (c *Controller) StealRange(ctx context.Context,
	thiefID string) (headersync.DispatchStealResult, bool, error) {

	result, ok, err := c.manager.AskSteal(ctx, thiefID)
	if err != nil || !ok {
		return headersync.DispatchStealResult{}, ok, err
	}

	rangeIDs := make([]headersync.RangeID, 0, len(result.TaskIDs))
	for _, taskID := range result.TaskIDs {
		rangeIDs = append(rangeIDs, fromTaskID(taskID))
	}
	if len(rangeIDs) == 0 && result.TaskID != 0 {
		rangeIDs = append(rangeIDs, fromTaskID(result.TaskID))
	}
	if len(rangeIDs) == 0 {
		return headersync.DispatchStealResult{}, false,
			fmt.Errorf("querysync steal returned no task ids")
	}

	return headersync.DispatchStealResult{
		ThiefID:  result.ThiefID,
		DonorID:  result.DonorID,
		RangeID:  rangeIDs[0],
		RangeIDs: rangeIDs,
	}, true, nil
}

func toQueryPeer(peer headersync.PeerSnapshot) querysync.PeerSnapshot {
	return querysync.PeerSnapshot{
		ID:        peer.ID,
		Rank:      peer.Rank,
		RTT:       peer.RTT,
		State:     toQueryPeerState(peer.State),
		LocalWork: toTaskIDs(peer.LocalRanges),
	}
}

func toQueryPeerState(state headersync.PeerState) querysync.PeerState {
	switch state {
	case headersync.PeerReady:
		return querysync.PeerReady
	case headersync.PeerBusy:
		return querysync.PeerBusy
	case headersync.PeerBlocked:
		return querysync.PeerBlocked
	case headersync.PeerQuarantined:
		return querysync.PeerQuarantined
	case headersync.PeerDown:
		return querysync.PeerDown
	default:
		return querysync.PeerDown
	}
}

func toTaskIDs(rangeIDs []headersync.RangeID) []querysync.TaskID {
	taskIDs := make([]querysync.TaskID, 0, len(rangeIDs))
	for _, rangeID := range rangeIDs {
		taskIDs = append(taskIDs, toTaskID(rangeID))
	}

	return taskIDs
}

func toTaskID(rangeID headersync.RangeID) querysync.TaskID {
	return querysync.TaskID(rangeID)
}

func fromTaskID(taskID querysync.TaskID) headersync.RangeID {
	return headersync.RangeID(taskID)
}
