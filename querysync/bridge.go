//nolint:tagliatelle // Preserve the model's snake_case fields.
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

	//nolint:tagliatelle // Trace fields use snake_case JSON.
	RTTNanos int64 `json:"rtt_ns,omitempty"`
}

// BridgeTask is the fixture form of Task.
type BridgeTask struct {
	ID            TaskID   `json:"id"`
	BatchID       uint64   `json:"batch_id"`
	Canceled      bool     `json:"canceled"`
	TimeoutMillis int      `json:"timeout_ms,omitempty"`
	AvoidPeers    []string `json:"avoid_peers,omitempty"`

	//nolint:tagliatelle // Trace fields use snake_case JSON.
	TimeoutNanos int64 `json:"timeout_ns,omitempty"`
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
		fsm.Enqueue(taskFromBridge(task))
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
		snapshot, err := peerFromBridge(peer)
		if err != nil {
			return nil, err
		}

		err = fsm.AddPeer(snapshot)
		if err != nil {
			return nil, err
		}
	}

	return fsm, nil
}

// bridgeDuration accepts legacy millisecond fixtures and exact new traces.
func bridgeDuration(nanos int64, millis int) time.Duration {
	if nanos != 0 {
		return time.Duration(nanos)
	}
	return time.Duration(millis) * time.Millisecond
}
