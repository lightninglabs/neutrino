package headersync

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"
)

// BridgeFixtures contains concrete model stories shared with the P spec. The
// bridge does not replace P checking; it keeps the same scenarios executable
// against the Go FSM as the implementation evolves.
type BridgeFixtures struct {
	Scenarios []BridgeScenario `json:"scenarios"`
}

// BridgeScenario describes one deterministic FSM story.
type BridgeScenario struct {
	Name          string         `json:"name"`
	CommittedTip  uint32         `json:"committed_tip"`
	CommittedHash uint32         `json:"committed_hash"`
	Anchors       []BridgeAnchor `json:"anchors"`
	Peers         []BridgePeer   `json:"peers"`
	Steps         []BridgeStep   `json:"steps"`
	Expect        BridgeExpect   `json:"expect"`
}

// BridgeAnchor is the fixture form of Anchor.
type BridgeAnchor struct {
	Height  uint32 `json:"height"`
	Hash    uint32 `json:"hash"`
	Trusted bool   `json:"trusted"`
}

// BridgePeer is the fixture form of PeerSnapshot.
type BridgePeer struct {
	ID          string    `json:"id"`
	Rank        int       `json:"rank"`
	RTTMillis   int       `json:"rtt_ms"`
	State       string    `json:"state"`
	LocalRanges []RangeID `json:"local_ranges,omitempty"`
	ActiveRange RangeID   `json:"active_range,omitempty"`
}

// BridgeStep is one applied transition.
type BridgeStep struct {
	Op          string  `json:"op"`
	RangeID     RangeID `json:"range_id,omitempty"`
	StartHeight uint32  `json:"start_height,omitempty"`
	StopHeight  uint32  `json:"stop_height,omitempty"`
	PeerID      string  `json:"peer_id,omitempty"`
	ThiefID     string  `json:"thief_id,omitempty"`
	LeaseEpoch  uint64  `json:"lease_epoch,omitempty"`
	Valid       bool    `json:"valid,omitempty"`
	AtUnix      int64   `json:"at_unix,omitempty"`
	Height      uint32  `json:"height,omitempty"`
	Hash        uint32  `json:"hash,omitempty"`
	Trusted     bool    `json:"trusted,omitempty"`
}

// BridgeExpect is the comparable output of running a scenario.
type BridgeExpect struct {
	CommittedTip      uint32              `json:"committed_tip"`
	CommittedHash     uint32              `json:"committed_hash"`
	CommitLog         []RangeID           `json:"commit_log,omitempty"`
	ReadyRanges       []RangeID           `json:"ready_ranges,omitempty"`
	ActiveAnchorCount int                 `json:"active_anchor_count,omitempty"`
	PrunedAnchors     int                 `json:"pruned_anchors,omitempty"`
	Ranges            []BridgeRangeExpect `json:"ranges,omitempty"`
	Peers             []BridgePeerExpect  `json:"peers,omitempty"`
	Metrics           BridgeMetrics       `json:"metrics"`
}

// BridgeRangeExpect is the fixture form of HeaderRange state.
type BridgeRangeExpect struct {
	ID         RangeID `json:"id"`
	State      string  `json:"state"`
	OwnerPeer  string  `json:"owner_peer,omitempty"`
	LeaseEpoch uint64  `json:"lease_epoch,omitempty"`
	Valid      bool    `json:"valid,omitempty"`
}

// BridgePeerExpect is the fixture form of peer state after a scenario.
type BridgePeerExpect struct {
	ID          string    `json:"id"`
	State       string    `json:"state"`
	LocalRanges []RangeID `json:"local_ranges,omitempty"`
	ActiveRange RangeID   `json:"active_range,omitempty"`
}

// BridgeMetrics is the fixture subset of Metrics.
type BridgeMetrics struct {
	RangesStolen     uint64 `json:"ranges_stolen,omitempty"`
	RangesCommitted  uint64 `json:"ranges_committed,omitempty"`
	StaleCompletions uint64 `json:"stale_completions,omitempty"`
}

// DecodeBridgeFixtures parses model bridge fixtures.
func DecodeBridgeFixtures(data []byte) (BridgeFixtures, error) {
	var fixtures BridgeFixtures
	if err := json.Unmarshal(data, &fixtures); err != nil {
		return BridgeFixtures{}, err
	}

	return fixtures, nil
}

// RunBridgeScenario applies a bridge scenario to the Go FSM.
func RunBridgeScenario(scenario BridgeScenario) (BridgeExpect, error) {
	fsm := NewHeaderSyncFSM(
		scenario.CommittedTip, hashFromBridge(scenario.CommittedHash),
		DefaultConfig(),
	)
	session := NewSession()
	var prunedAnchors int

	for _, anchor := range scenario.Anchors {
		fsm.AddOrConfirmAnchor(
			anchor.Height, hashFromBridge(anchor.Hash),
			anchor.Trusted,
		)
	}

	for _, peer := range scenario.Peers {
		state, err := ParsePeerState(peer.State)
		if err != nil {
			return BridgeExpect{}, err
		}

		err = fsm.AddPeer(PeerSnapshot{
			ID:          peer.ID,
			Rank:        peer.Rank,
			RTT:         time.Duration(peer.RTTMillis) * time.Millisecond,
			State:       state,
			LocalRanges: peer.LocalRanges,
			ActiveRange: peer.ActiveRange,
		})
		if err != nil {
			return BridgeExpect{}, err
		}
	}

	for _, step := range scenario.Steps {
		if err := runBridgeStep(fsm, session, &prunedAnchors,
			step); err != nil {

			return BridgeExpect{}, fmt.Errorf(
				"%s: %w", scenario.Name, err,
			)
		}
	}

	return bridgeObserve(fsm, session, prunedAnchors), nil
}

func runBridgeStep(fsm *HeaderSyncFSM, session *Session,
	prunedAnchors *int, step BridgeStep) error {

	switch step.Op {
	case "anchor":
		fsm.AddOrConfirmAnchor(
			step.Height, hashFromBridge(step.Hash), step.Trusted,
		)

	case "advance_tip":
		fsm.AdvanceCommittedTip(step.Height, hashFromBridge(step.Hash))

	case "track_anchor":
		session.TrackAnchor(step.PeerID, AnchorRequest{
			StartHeight: step.StartHeight,
			StopHeight:  step.StopHeight,
		})

	case "plan":
		_, _, err := fsm.PlanRange(
			step.RangeID, step.StartHeight, step.StopHeight,
		)
		return err

	case "assign":
		if _, ok := fsm.AssignNextQueuedRange(); !ok {
			return fmt.Errorf("assign found no work or owner")
		}

	case "start":
		if _, ok, err := fsm.StartPeerRange(step.PeerID); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("peer %s could not start range",
				step.PeerID)
		}

	case "steal":
		if _, ok, err := fsm.StealRange(
			step.ThiefID, step.RangeID,
		); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("peer %s could not steal range %d",
				step.ThiefID, step.RangeID)
		}

	case "complete":
		_, err := fsm.CompleteRangeAt(
			step.PeerID, step.RangeID, step.LeaseEpoch,
			step.Valid, time.Unix(step.AtUnix, 0),
		)
		return err

	case "commit":
		fsm.CommitReadyRanges()

	case "commit_and_prune":
		commit := fsm.CommitReadyRanges()
		if len(commit.Committed) > 0 {
			*prunedAnchors += session.PruneAnchorsAtOrBefore(
				commit.TipHeight,
			)
		}

	default:
		return fmt.Errorf("unknown bridge op %q", step.Op)
	}

	return nil
}

func bridgeObserve(fsm *HeaderSyncFSM, session *Session,
	prunedAnchors int) BridgeExpect {

	ranges := fsm.Ranges()
	var rangeExpect []BridgeRangeExpect
	for _, rng := range ranges {
		rangeExpect = append(rangeExpect, BridgeRangeExpect{
			ID:         rng.ID,
			State:      rng.State.String(),
			OwnerPeer:  rng.OwnerPeer,
			LeaseEpoch: rng.LeaseEpoch,
			Valid:      rng.Valid,
		})
	}

	peers := fsm.Peers()
	var peerExpect []BridgePeerExpect
	for _, peer := range peers {
		peerExpect = append(peerExpect, BridgePeerExpect{
			ID:          peer.ID,
			State:       peer.State.String(),
			LocalRanges: peer.LocalRanges,
			ActiveRange: peer.ActiveRange,
		})
	}

	metrics := fsm.Metrics()

	return BridgeExpect{
		CommittedTip:      fsm.CommittedTip(),
		CommittedHash:     hashToBridge(fsm.CommittedHash()),
		CommitLog:         fsm.CommitLog(),
		ReadyRanges:       fsm.ReadyRanges(),
		ActiveAnchorCount: session.ActiveAnchorCount(),
		PrunedAnchors:     prunedAnchors,
		Ranges:            rangeExpect,
		Peers:             peerExpect,
		Metrics: BridgeMetrics{
			RangesStolen:     metrics.RangesStolen,
			RangesCommitted:  metrics.RangesCommitted,
			StaleCompletions: metrics.StaleCompletions,
		},
	}
}

func hashFromBridge(value uint32) Hash {
	var hash Hash
	binary.LittleEndian.PutUint32(hash[:4], value)

	return hash
}

func hashToBridge(hash Hash) uint32 {
	return binary.LittleEndian.Uint32(hash[:4])
}
