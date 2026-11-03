package querydispatch

import (
	"fmt"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/lightninglabs/neutrino/headersync"
	"github.com/lightninglabs/neutrino/querysync"
)

// Controller adapts querysync's scheduler to headersync's generic
// dispatch interface. Header range validation, lease epochs, active timeout
// stealing, staging, and ordered commit stay in headersync.
type Controller struct {
	scheduler *querysync.Scheduler
}

var _ headersync.DispatchController = (*Controller)(nil)

// New creates a querysync-backed header range dispatch controller.
func New(scheduler *querysync.Scheduler) *Controller {
	return &Controller{scheduler: scheduler}
}

// NewRuntime creates a production headersync runtime that mirrors generic range
// ownership through a querysync scheduler. The block handler is the only
// caller of the manager, which is what makes the scheduler safe to use.
// Header-specific protocol details remain in headersync and blockmanager. The
// tipCfg builds the validation cache that the runtime's driver owns.
func NewRuntime(tip headersync.ChainPoint, checkpoints []chaincfg.Checkpoint,
	tipCfg headersync.TipConfig) (*headersync.Runtime, int) {

	scheduler := querysync.NewScheduler(querysync.DefaultConfig(), nil, nil)

	return headersync.NewRuntime(tip, checkpoints, headersync.RuntimeConfig{
		Config:   headersync.ProductionConfig(),
		Dispatch: New(scheduler),
		Tip:      tipCfg,
	})
}

// AddPeer mirrors a header peer into the querysync scheduler.
func (c *Controller) AddPeer(peer headersync.PeerSnapshot) error {
	return c.scheduler.AddPeer(toQueryPeer(peer))
}

// RemovePeer removes a mirrored header peer.
func (c *Controller) RemovePeer(id string) error {
	c.scheduler.RemovePeer(id)

	return nil
}

// UpdatePeerState mirrors peer scheduling state.
func (c *Controller) UpdatePeerState(id string,
	state headersync.PeerState) error {

	return c.scheduler.UpdatePeerState(id, toQueryPeerState(state))
}

// UpdatePeerRTT mirrors peer RTT.
func (c *Controller) UpdatePeerRTT(id string,
	rtt time.Duration) error {

	return c.scheduler.UpdatePeerRTT(id, rtt)
}

// AssignRange asks querysync to assign unstarted header range ownership.
func (c *Controller) AssignRange(
	rangeID headersync.RangeID) (headersync.DispatchAssignment, bool,
	error) {

	assignment, ok := c.scheduler.AssignLocalTask(
		querysync.Task{ID: toTaskID(rangeID)},
	)
	if !ok {
		return headersync.DispatchAssignment{}, false, nil
	}

	return headersync.DispatchAssignment{
		PeerID:  assignment.PeerID,
		RangeID: fromTaskID(assignment.TaskID),
	}, true, nil
}

// StartRange mirrors the peer's local queue after headersync has moved the
// range into the active slot, then marks the peer busy in querysync.
func (c *Controller) StartRange(peer headersync.PeerSnapshot) error {
	if err := c.SyncPeer(peer); err != nil {
		return err
	}

	return c.scheduler.UpdatePeerState(peer.ID, querysync.PeerBusy)
}

// CompleteRange marks a mirrored active range finished.
func (c *Controller) CompleteRange(peerID string) error {
	return c.scheduler.CompletePeerWork(peerID)
}

// SyncPeer replaces the mirrored peer snapshot.
func (c *Controller) SyncPeer(peer headersync.PeerSnapshot) error {
	return c.scheduler.AddPeer(toQueryPeer(peer))
}

// StealRange asks querysync to move unstarted range ownership to the thief.
func (c *Controller) StealRange(
	thiefID string) (headersync.DispatchStealResult, bool, error) {

	result, ok := c.scheduler.StealOne(thiefID)
	if !ok {
		return headersync.DispatchStealResult{}, false, nil
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
