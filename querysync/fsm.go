package querysync

import (
	"fmt"
	"sort"
	"time"
)

// Metrics exposes counters that are useful both for production telemetry and
// deterministic tests.
type Metrics struct {
	Dispatched       uint64
	DroppedCanceled  uint64
	Owned            uint64
	Stolen           uint64
	FailedDispatches uint64
}

// SchedulerFSM is the pure manager state machine. It does not perform I/O and
// does not own goroutines; actor wrappers serialize access and bridge this pure
// state to the rest of neutrino.
type SchedulerFSM struct {
	cfg     Config
	peers   map[string]PeerSnapshot
	queue   []Task
	metrics Metrics
}

// NewSchedulerFSM creates a new scheduler FSM.
func NewSchedulerFSM(cfg Config) *SchedulerFSM {
	return &SchedulerFSM{
		cfg:   cfg.normalize(),
		peers: make(map[string]PeerSnapshot),
	}
}

// AddPeer adds or replaces a peer snapshot.
func (s *SchedulerFSM) AddPeer(peer PeerSnapshot) error {
	if peer.ID == "" {
		return fmt.Errorf("%w: empty peer id", ErrInvalidPeer)
	}

	if existing, ok := s.peers[peer.ID]; ok && peer.LocalWork == nil {
		peer.LocalWork = existing.LocalWork
	}

	s.peers[peer.ID] = peer.clone()
	return nil
}

// RemovePeer removes a peer from the scheduler.
func (s *SchedulerFSM) RemovePeer(id string) {
	delete(s.peers, id)
}

// SnapshotPeer returns a copy of the peer state.
func (s *SchedulerFSM) SnapshotPeer(id string) (PeerSnapshot, bool) {
	peer, ok := s.peers[id]
	if !ok {
		return PeerSnapshot{}, false
	}

	return peer.clone(), true
}

// Peers returns a stable snapshot of all peers.
func (s *SchedulerFSM) Peers() []PeerSnapshot {
	peers := make([]PeerSnapshot, 0, len(s.peers))
	for _, peer := range s.peers {
		peers = append(peers, peer.clone())
	}

	sort.Slice(peers, func(i, j int) bool {
		return peers[i].ID < peers[j].ID
	})

	return peers
}

// Enqueue appends a task to the global work queue.
func (s *SchedulerFSM) Enqueue(task Task) {
	task.AvoidPeers = append([]string(nil), task.AvoidPeers...)
	s.queue = append(s.queue, task)
}

// QueueLen returns the current global work queue length.
func (s *SchedulerFSM) QueueLen() int {
	return len(s.queue)
}

// Metrics returns the current scheduler counters.
func (s *SchedulerFSM) Metrics() Metrics {
	return s.metrics
}

// CancelBatch marks queued work in the batch as canceled. The next scheduling
// pass will drop the canceled work rather than dispatching it to a peer.
func (s *SchedulerFSM) CancelBatch(batchID uint64) {
	for i := range s.queue {
		if s.queue[i].BatchID == batchID {
			s.queue[i].Canceled = true
		}
	}
}

// UpdatePeerState updates a peer's scheduling state.
func (s *SchedulerFSM) UpdatePeerState(id string, state PeerState) error {
	peer, ok := s.peers[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownPeer, id)
	}

	peer.State = state
	s.peers[id] = peer
	return nil
}

// UpdatePeerRTT updates the latest observed peer RTT.
func (s *SchedulerFSM) UpdatePeerRTT(id string, rtt time.Duration) error {
	peer, ok := s.peers[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownPeer, id)
	}

	peer.RTT = rtt
	s.peers[id] = peer
	return nil
}

// SetPeerLocalWork replaces a peer's local, unclaimed work queue.
func (s *SchedulerFSM) SetPeerLocalWork(id string, work []TaskID) error {
	peer, ok := s.peers[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownPeer, id)
	}

	peer.LocalWork = append([]TaskID(nil), work...)
	s.peers[id] = peer
	return nil
}

// CompletePeerWork marks a peer's active slot as ready again.
func (s *SchedulerFSM) CompletePeerWork(id string) error {
	return s.UpdatePeerState(id, PeerReady)
}

// DispatchNext applies one global scheduling transition. It first removes
// canceled work at the head of the queue, then selects the best peer that can
// accept work without blocking.
func (s *SchedulerFSM) DispatchNext() (Assignment, bool) {
	for len(s.queue) > 0 && s.queue[0].Canceled {
		s.queue = s.queue[1:]
		s.metrics.DroppedCanceled++
	}

	if len(s.queue) == 0 {
		s.metrics.FailedDispatches++
		return Assignment{}, false
	}

	task := s.queue[0]
	assignment, ok := s.dispatchTask(task)
	if !ok {
		return Assignment{}, false
	}

	s.queue = s.queue[1:]
	return assignment, true
}

// DispatchTask applies one scheduling transition for a task owned outside the
// scheduler queue. This is the production migration bridge: the long-lived
// manager actor owns peer state and dispatch rules while the existing query
// work queue still owns concrete job ordering and cancellation.
func (s *SchedulerFSM) DispatchTask(task Task) (Assignment, bool) {
	if task.Canceled {
		s.metrics.DroppedCanceled++
		return Assignment{}, false
	}

	task.AvoidPeers = append([]string(nil), task.AvoidPeers...)
	return s.dispatchTask(task)
}

// AssignLocalTask moves an externally queued task into a peer-local owned
// queue without marking the peer busy. This is the production work-stealing
// bridge: the concrete query job remains in the query package, while the model
// decides which peer owns the next unstarted item and makes that ownership
// visible to StealOne.
func (s *SchedulerFSM) AssignLocalTask(task Task) (Assignment, bool) {
	if task.Canceled {
		s.metrics.DroppedCanceled++
		return Assignment{}, false
	}

	task.AvoidPeers = append([]string(nil), task.AvoidPeers...)

	peer, ok := s.selectLocalOwner(task.AvoidPeers)
	if !ok {
		s.metrics.FailedDispatches++
		return Assignment{}, false
	}

	peer.LocalWork = append(peer.LocalWork, task.ID)
	s.peers[peer.ID] = peer
	s.metrics.Owned++

	baseTimeout := s.cfg.BaseTimeout
	if task.Timeout > 0 {
		baseTimeout = task.Timeout
	}

	return Assignment{
		PeerID: peer.ID,
		TaskID: task.ID,
		Timeout: QueryTimeoutForPeer(
			baseTimeout, peer, s.cfg,
		),
	}, true
}

func (s *SchedulerFSM) dispatchTask(task Task) (Assignment, bool) {
	peer, ok := s.selectPeer(task.AvoidPeers)
	if !ok {
		s.metrics.FailedDispatches++
		return Assignment{}, false
	}

	peer.State = PeerBusy
	s.peers[peer.ID] = peer
	s.metrics.Dispatched++

	baseTimeout := s.cfg.BaseTimeout
	if task.Timeout > 0 {
		baseTimeout = task.Timeout
	}

	return Assignment{
		PeerID: peer.ID,
		TaskID: task.ID,
		Timeout: QueryTimeoutForPeer(
			baseTimeout, peer, s.cfg,
		),
	}, true
}

func (s *SchedulerFSM) selectPeer(avoidPeers []string) (PeerSnapshot, bool) {
	var (
		best  PeerSnapshot
		found bool
	)

	avoid := make(map[string]struct{}, len(avoidPeers))
	for _, peer := range avoidPeers {
		avoid[peer] = struct{}{}
	}

	for _, peer := range s.peers {
		if peer.State != PeerReady {
			continue
		}
		if _, ok := avoid[peer.ID]; ok {
			continue
		}

		if !found || peerOrdersBefore(peer, best) {
			best = peer
			found = true
		}
	}

	return best.clone(), found
}

func (s *SchedulerFSM) selectLocalOwner(avoidPeers []string) (
	PeerSnapshot, bool) {

	var (
		best  PeerSnapshot
		found bool
	)

	avoid := make(map[string]struct{}, len(avoidPeers))
	for _, peer := range avoidPeers {
		avoid[peer] = struct{}{}
	}

	for _, peer := range s.peers {
		if s.localWorkSpare(peer) == 0 {
			continue
		}
		if _, ok := avoid[peer.ID]; ok {
			continue
		}

		if !found || s.localOwnerOrdersBefore(peer, best) {
			best = peer
			found = true
		}
	}

	return best.clone(), found
}

func peerOrdersBefore(peer, candidate PeerSnapshot) bool {
	if peer.Rank != candidate.Rank {
		return peer.Rank < candidate.Rank
	}

	if peer.RTT != candidate.RTT {
		return peer.RTT < candidate.RTT
	}

	return peer.ID < candidate.ID
}

func (s *SchedulerFSM) localOwnerOrdersBefore(peer,
	candidate PeerSnapshot) bool {

	peerSpare := s.localWorkSpare(peer)
	candidateSpare := s.localWorkSpare(candidate)
	if peerSpare != candidateSpare {
		return peerSpare > candidateSpare
	}

	peerLoad := localOwnerLoad(peer)
	candidateLoad := localOwnerLoad(candidate)
	if peerLoad != candidateLoad {
		return peerLoad < candidateLoad
	}

	return peerOrdersBefore(peer, candidate)
}

func localOwnerLoad(peer PeerSnapshot) int {
	load := len(peer.LocalWork)
	if peer.State == PeerBusy {
		load++
	}

	return load
}

func (s *SchedulerFSM) localWorkSpare(peer PeerSnapshot) int {
	if peer.State != PeerReady && peer.State != PeerBusy {
		return 0
	}

	spare := s.cfg.MaxPeerOutstanding - localOwnerLoad(peer)
	if spare < 0 {
		return 0
	}

	return spare
}

// StealOne moves unclaimed work from the most loaded donor to an idle thief,
// filling the thief's available local queue capacity. Ready and busy donors
// retain one local task after the steal so useful peer-local queues do not
// immediately collapse into reshuffling. A busy peer with only one queued local
// task is not a useful donor yet: its active request should either finish or
// fail before the queued task is considered stranded. Blocked and quarantined
// donors can be drained because their local work is otherwise stranded.
func (s *SchedulerFSM) StealOne(thiefID string) (StealResult, bool) {
	thief, ok := s.peers[thiefID]
	if !ok || thief.State != PeerReady || len(thief.LocalWork) != 0 {
		return StealResult{}, false
	}

	thiefSpare := s.localWorkSpare(thief)
	if thiefSpare == 0 {
		return StealResult{}, false
	}

	donor, ok := s.selectStealDonor(thiefID)
	if !ok {
		return StealResult{}, false
	}

	stealCount := s.stealableWork(donor)
	if stealCount > thiefSpare {
		stealCount = thiefSpare
	}
	if stealCount == 0 {
		return StealResult{}, false
	}

	stolenTasks := append([]TaskID(nil), donor.LocalWork[:stealCount]...)
	donor.LocalWork = append([]TaskID(nil), donor.LocalWork[stealCount:]...)
	thief.LocalWork = append(thief.LocalWork, stolenTasks...)

	s.peers[donor.ID] = donor
	s.peers[thief.ID] = thief
	s.metrics.Stolen++

	return StealResult{
		ThiefID:        thief.ID,
		DonorID:        donor.ID,
		TaskID:         stolenTasks[0],
		TaskIDs:        stolenTasks,
		DonorRemaining: len(donor.LocalWork),
	}, true
}

func (s *SchedulerFSM) selectStealDonor(thiefID string) (PeerSnapshot, bool) {
	var (
		best  PeerSnapshot
		found bool
	)

	for _, peer := range s.peers {
		if peer.ID == thiefID || s.stealableWork(peer) == 0 {
			continue
		}

		if !found || betterStealDonor(peer, best) {
			best = peer
			found = true
		}
	}

	return best.clone(), found
}

func (s *SchedulerFSM) stealableWork(peer PeerSnapshot) int {
	switch peer.State {
	case PeerReady:
		if len(peer.LocalWork) <= 1 {
			return 0
		}

		return len(peer.LocalWork) - 1

	case PeerBusy:
		if len(peer.LocalWork) <= 1 {
			return 0
		}

		return len(peer.LocalWork) - 1

	case PeerBlocked, PeerQuarantined:
		return len(peer.LocalWork)

	default:
		return 0
	}
}

func betterStealDonor(peer, candidate PeerSnapshot) bool {
	peerPriority := stealDonorPriority(peer)
	candidatePriority := stealDonorPriority(candidate)
	if peerPriority != candidatePriority {
		return peerPriority > candidatePriority
	}

	peerStealable := stealableWorkForOrder(peer)
	candidateStealable := stealableWorkForOrder(candidate)
	if peerStealable != candidateStealable {
		return peerStealable > candidateStealable
	}

	if peer.Rank != candidate.Rank {
		return peer.Rank > candidate.Rank
	}

	return peer.ID < candidate.ID
}

func stealDonorPriority(peer PeerSnapshot) int {
	switch {
	case peer.State == PeerBlocked || peer.State == PeerQuarantined:
		return 3

	case peer.State == PeerReady && len(peer.LocalWork) > 1:
		return 2

	case peer.State == PeerBusy && len(peer.LocalWork) > 1:
		return 2

	default:
		return 0
	}
}

func stealableWorkForOrder(peer PeerSnapshot) int {
	switch peer.State {
	case PeerReady:
		if len(peer.LocalWork) <= 1 {
			return 0
		}

		return len(peer.LocalWork) - 1

	case PeerBusy:
		if len(peer.LocalWork) <= 1 {
			return 0
		}

		return len(peer.LocalWork) - 1

	case PeerBlocked, PeerQuarantined:
		return len(peer.LocalWork)

	default:
		return 0
	}
}
