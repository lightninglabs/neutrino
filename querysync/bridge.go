package querysync

import (
	"encoding/json"
	"fmt"
	"time"
)

// BridgeFixtures are JSON scenarios shared with the P model documentation. The
// bridge does not replace P checking; it keeps a concrete set of model stories
// executable against the Go FSM as implementation changes.
type BridgeFixtures struct {
	Dispatch []BridgeDispatchScenario `json:"dispatch"`
	Steal    []BridgeStealScenario    `json:"steal"`
}

// BridgeDispatchScenario describes one DispatchNext model story.
type BridgeDispatchScenario struct {
	Name              string             `json:"name"`
	BaseTimeoutMillis int                `json:"base_timeout_ms"`
	Peers             []BridgePeer       `json:"peers"`
	Tasks             []BridgeTask       `json:"tasks"`
	Expect            BridgeDispatchWant `json:"expect"`
}

// BridgeStealScenario describes one StealOne model story.
type BridgeStealScenario struct {
	Name   string          `json:"name"`
	Peers  []BridgePeer    `json:"peers"`
	Thief  string          `json:"thief"`
	Expect BridgeStealWant `json:"expect"`
}

// BridgePeer is the fixture form of PeerSnapshot.
type BridgePeer struct {
	ID        string   `json:"id"`
	Rank      int      `json:"rank"`
	RTTMillis int      `json:"rtt_ms"`
	State     string   `json:"state"`
	LocalWork []TaskID `json:"local_work"`
}

// BridgeTask is the fixture form of Task.
type BridgeTask struct {
	ID            TaskID   `json:"id"`
	BatchID       uint64   `json:"batch_id"`
	Canceled      bool     `json:"canceled"`
	TimeoutMillis int      `json:"timeout_ms,omitempty"`
	AvoidPeers    []string `json:"avoid_peers,omitempty"`
}

// BridgeDispatchWant is the fixture form of Assignment.
type BridgeDispatchWant struct {
	OK            bool   `json:"ok"`
	PeerID        string `json:"peer_id"`
	TaskID        TaskID `json:"task_id"`
	TimeoutMillis int    `json:"timeout_ms"`
}

// BridgeStealWant is the fixture form of StealResult.
type BridgeStealWant struct {
	OK             bool     `json:"ok"`
	ThiefID        string   `json:"thief_id"`
	DonorID        string   `json:"donor_id"`
	TaskID         TaskID   `json:"task_id"`
	TaskIDs        []TaskID `json:"task_ids,omitempty"`
	DonorRemaining int      `json:"donor_remaining"`
	ThiefWorkLen   int      `json:"thief_work_len"`
}

// DecodeBridgeFixtures parses model bridge fixtures.
func DecodeBridgeFixtures(data []byte) (BridgeFixtures, error) {
	var fixtures BridgeFixtures
	if err := json.Unmarshal(data, &fixtures); err != nil {
		return BridgeFixtures{}, err
	}

	return fixtures, nil
}

// RunBridgeDispatch applies a dispatch scenario to the Go FSM.
func RunBridgeDispatch(scenario BridgeDispatchScenario) (
	BridgeDispatchWant, error) {

	fsm, err := fsmFromBridgePeers(scenario.Peers, Config{
		BaseTimeout: time.Duration(scenario.BaseTimeoutMillis) *
			time.Millisecond,
	})
	if err != nil {
		return BridgeDispatchWant{}, err
	}

	for _, task := range scenario.Tasks {
		fsm.Enqueue(Task{
			ID:         task.ID,
			BatchID:    task.BatchID,
			Canceled:   task.Canceled,
			Timeout:    time.Duration(task.TimeoutMillis) * time.Millisecond,
			AvoidPeers: task.AvoidPeers,
		})
	}

	assignment, ok := fsm.DispatchNext()
	if !ok {
		return BridgeDispatchWant{OK: false}, nil
	}

	return BridgeDispatchWant{
		OK:            true,
		PeerID:        assignment.PeerID,
		TaskID:        assignment.TaskID,
		TimeoutMillis: int(assignment.Timeout / time.Millisecond),
	}, nil
}

// RunBridgeSteal applies a work-stealing scenario to the Go FSM.
func RunBridgeSteal(scenario BridgeStealScenario) (BridgeStealWant, error) {
	fsm, err := fsmFromBridgePeers(scenario.Peers, DefaultConfig())
	if err != nil {
		return BridgeStealWant{}, err
	}

	result, ok := fsm.StealOne(scenario.Thief)
	if !ok {
		return BridgeStealWant{OK: false}, nil
	}

	thief, ok := fsm.SnapshotPeer(scenario.Thief)
	if !ok {
		return BridgeStealWant{}, fmt.Errorf("%w: %s",
			ErrUnknownPeer, scenario.Thief)
	}

	return BridgeStealWant{
		OK:             true,
		ThiefID:        result.ThiefID,
		DonorID:        result.DonorID,
		TaskID:         result.TaskID,
		TaskIDs:        append([]TaskID(nil), result.TaskIDs...),
		DonorRemaining: result.DonorRemaining,
		ThiefWorkLen:   len(thief.LocalWork),
	}, nil
}

func fsmFromBridgePeers(peers []BridgePeer, cfg Config) (*SchedulerFSM,
	error) {

	fsm := NewSchedulerFSM(cfg)
	for _, peer := range peers {
		state, err := ParsePeerState(peer.State)
		if err != nil {
			return nil, err
		}

		err = fsm.AddPeer(PeerSnapshot{
			ID:        peer.ID,
			Rank:      peer.Rank,
			RTT:       time.Duration(peer.RTTMillis) * time.Millisecond,
			State:     state,
			LocalWork: peer.LocalWork,
		})
		if err != nil {
			return nil, err
		}
	}

	return fsm, nil
}
