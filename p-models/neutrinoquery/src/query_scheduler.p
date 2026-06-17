// query_scheduler.p - Neutrino query scheduler executable specification.
//
// This file is written as an executable design note. P syntax is intentionally
// close to a small imperative language: records model state, pure functions
// model decisions, and tests assert the invariants that must keep holding as the
// Go scheduler evolves.
//
// The model is intentionally small. It captures the scheduling invariants that
// matter for robustness under poor network conditions:
//
//   * canceled queued work is dropped by the scheduler, not dispatched;
//   * a blocked peer cannot stall other capable peers;
//   * a quarantined peer cannot receive new work until it recovers;
//   * peer RTT raises the timeout floor used for a query attempt;
//   * queued work can be assigned into bounded peer-local owned queues;
//   * an idle peer may steal enough unclaimed work to fill its local capacity;
//   * stealing leaves ready and busy donors with at least one local task;
//   * busy singleton donors are left alone until their active request finishes
//     or fails, while blocked/quarantined donors can be drained because they
//     are not making progress.

enum PeerState {
    // PeerReady means the peer has no active query and its mailbox can accept a
    // new assignment immediately.
    PeerReady,

    // PeerBusy means the peer owns an active query. The global scheduler should
    // wait for a result or cancellation before giving it more work.
    PeerBusy,

    // PeerBlocked is the important production failure mode: the peer looks
    // connected, but its worker queue cannot accept a send right now, or the
    // manager is temporarily avoiding it for the current task because it just
    // failed that task. A blocked high-ranked peer must not stall the entire
    // dispatcher.
    PeerBlocked,

    // PeerQuarantined is stronger than blocked. The manager has seen enough
    // repeated non-responses that production should stop assigning new work and
    // hand the peer to the expiring ban/quarantine path. This state is still
    // recoverable: when the ban decays and the peer reconnects it can re-enter
    // PeerReady.
    PeerQuarantined,

    // PeerDown means the peer is disconnected or otherwise unavailable.
    PeerDown
}

// QueryTask is the global work item. The model keeps only the fields needed for
// scheduling safety: a stable identity and whether the enclosing batch has
// canceled it.
type QueryTask = (
    id: int,
    canceled: bool
);

// PeerModel abstracts one neutrino worker. rank is lower-is-better, rtt_ms is
// the latest ping RTT sample, and local_work is the peer-owned queue used by the
// work-stealing portion of the model.
type PeerModel = (
    id: int,
    rank: int,
    rtt_ms: int,
    peer_state: PeerState,
    local_work: seq[int]
);

// DispatchResult is what the scheduler decided for one attempt. NoPeer/NoTask
// means the scheduler intentionally did not dispatch.
type DispatchResult = (
    peer_id: int,
    task_id: int,
    timeout_ms: int
);

fun NoPeer(): int {
    return 0;
}

fun NoTask(): int {
    return 0;
}

fun MinQueryTimeoutMs(): int {
    return 2000;
}

fun MaxQueryTimeoutMs(): int {
    return 32000;
}

fun RTTMultiplier(): int {
    return 8;
}

// Constructors keep tests readable and make it explicit when future fields are
// added to the model. That matters because P records are positional enough that
// ad hoc literals become hard to audit.
fun NewTask(id: int, canceled: bool): QueryTask {
    return (id = id, canceled = canceled);
}

fun NewPeer(id: int, rank: int, rtt_ms: int, peer_state: PeerState,
    local_work: seq[int]): PeerModel {

    return (
        id = id,
        rank = rank,
        rtt_ms = rtt_ms,
        peer_state = peer_state,
        local_work = local_work
    );
}

// EmptyDispatch is an explicit no-op decision. The Go scheduler should reach
// this shape when all queued work is canceled, no peer can accept work, or no
// live work exists.
fun EmptyDispatch(): DispatchResult {
    return (
        peer_id = NoPeer(),
        task_id = NoTask(),
        timeout_ms = 0
    );
}

// PeerCanAccept is the model equivalent of a non-blocking worker send. This is
// the key distinction from the old behavior: connected is not enough; the peer
// must be ready to accept work without blocking the dispatcher.
fun PeerCanAccept(peer: PeerModel): bool {
    return peer.peer_state == PeerReady;
}

// MaxPeerOutstandingWork is a bounded queue policy, not a protocol constant.
// The important invariant is the cap: local ownership can be deeper than one
// item, but a peer cannot hide an unbounded backlog from the manager. The count
// includes the active query slot for busy peers.
fun MaxPeerOutstandingWork(): int {
    return 4;
}

// PeerOutstandingLoad counts unstarted local work plus the active query slot.
// This is the unit the scheduler uses to decide if a peer has room for more
// ownership.
fun PeerOutstandingLoad(peer: PeerModel): int {
    var load: int;

    load = sizeof(peer.local_work);
    if (peer.peer_state == PeerBusy) {
        load = load + 1;
    }

    return load;
}

// PeerLocalSpare returns the number of additional unstarted tasks this peer can
// own. Ready and busy peers can own local work; blocked, quarantined, and down
// peers must not receive new ownership from the global queue.
fun PeerLocalSpare(peer: PeerModel): int {
    var spare: int;

    if (!(peer.peer_state == PeerReady || peer.peer_state == PeerBusy)) {
        return 0;
    }

    spare = MaxPeerOutstandingWork() - PeerOutstandingLoad(peer);
    if (spare < 0) {
        return 0;
    }

    return spare;
}

// PeerCanOwnLocalWork is intentionally broader than PeerCanAccept. A busy peer
// can own queued follow-up tasks even though it cannot start them immediately.
// The bounded cap keeps global work visible to the manager instead of parking a
// deep backlog behind one lagging peer.
fun PeerCanOwnLocalWork(peer: PeerModel): bool {
    return PeerLocalSpare(peer) > 0;
}

// PeerOrdersBefore models ranking plus RTT tie-breaking. The current Go patch
// only uses RTT for timeout floors, but keeping RTT in the model selection order
// documents the intended next step: fast peers should become more likely to get
// work when peers are otherwise equivalent.
fun PeerOrdersBefore(peer: PeerModel, candidate: PeerModel): bool {
    if (peer.rank < candidate.rank) {
        return true;
    }

    if (peer.rank > candidate.rank) {
        return false;
    }

    if (peer.rtt_ms < candidate.rtt_ms) {
        return true;
    }

    if (peer.rtt_ms > candidate.rtt_ms) {
        return false;
    }

    return peer.id < candidate.id;
}

// QueryTimeoutForPeer is the formal version of the dynamic timeout rule:
//
//   timeout = clamp(max(base_timeout, latest_rtt * multiplier), min, max)
//
// A zero RTT sample means "unknown", so the base timeout remains in force.
fun QueryTimeoutForPeer(base_timeout_ms: int, peer: PeerModel): int {
    var timeout_ms: int;
    var rtt_timeout_ms: int;

    timeout_ms = base_timeout_ms;
    if (timeout_ms < MinQueryTimeoutMs()) {
        timeout_ms = MinQueryTimeoutMs();
    }

    if (peer.rtt_ms > 0) {
        rtt_timeout_ms = peer.rtt_ms * RTTMultiplier();
        if (rtt_timeout_ms > timeout_ms) {
            timeout_ms = rtt_timeout_ms;
        }
    }

    if (timeout_ms > MaxQueryTimeoutMs()) {
        timeout_ms = MaxQueryTimeoutMs();
    }

    return timeout_ms;
}

// FirstLiveTask captures the cancellation invariant. A canceled queued task is
// dead scheduler state; it should be discarded in the dispatcher instead of
// being handed to a worker just to produce a canceled result later.
fun FirstLiveTask(tasks: seq[QueryTask]): int {
    var i: int;

    i = 0;
    while (i < sizeof(tasks)) {
        if (!tasks[i].canceled) {
            return tasks[i].id;
        }

        i = i + 1;
    }

    return NoTask();
}

// SelectPeer chooses the best peer that can accept work now. A blocked
// high-ranked peer is skipped, which is the load-awareness property missing
// from a scheduler that blindly blocks on the first ranked worker.
fun SelectPeer(peers: seq[PeerModel]): int {
    var i: int;
    var found: bool;
    var candidate: PeerModel;

    i = 0;
    while (i < sizeof(peers)) {
        if (PeerCanAccept(peers[i])) {
            if (!found || PeerOrdersBefore(peers[i], candidate)) {
                found = true;
                candidate = peers[i];
            }
        }

        i = i + 1;
    }

    if (!found) {
        return NoPeer();
    }

    return candidate.id;
}

// PeerByID is total because P functions must return something for every input.
// Callers treat id=NoPeer as the sentinel miss.
fun PeerByID(peers: seq[PeerModel], id: int): PeerModel {
    var i: int;

    i = 0;
    while (i < sizeof(peers)) {
        if (peers[i].id == id) {
            return peers[i];
        }

        i = i + 1;
    }

    return NewPeer(NoPeer(), 0, 0, PeerDown, default(seq[int]));
}

// LocalOwnerOrdersBefore chooses the peer with the most spare local capacity,
// then falls back to lower outstanding load, then to the same rank/RTT ordering
// used for immediate dispatch. This lets fast peers build a useful queue while
// still spreading ownership before anyone reaches the cap.
fun LocalOwnerOrdersBefore(peer: PeerModel, candidate: PeerModel): bool {
    if (PeerLocalSpare(peer) > PeerLocalSpare(candidate)) {
        return true;
    }

    if (PeerLocalSpare(peer) < PeerLocalSpare(candidate)) {
        return false;
    }

    if (PeerOutstandingLoad(peer) < PeerOutstandingLoad(candidate)) {
        return true;
    }

    if (PeerOutstandingLoad(peer) > PeerOutstandingLoad(candidate)) {
        return false;
    }

    return PeerOrdersBefore(peer, candidate);
}

// SelectLocalOwner chooses the peer that should own the next queued task. This
// is separate from SelectPeer because ownership and active dispatch are distinct
// phases in production: a peer can own future work without blocking the manager
// on its mailbox right now.
fun SelectLocalOwner(peers: seq[PeerModel]): int {
    var i: int;
    var found: bool;
    var candidate: PeerModel;

    i = 0;
    while (i < sizeof(peers)) {
        if (PeerCanOwnLocalWork(peers[i])) {
            if (!found || LocalOwnerOrdersBefore(peers[i], candidate)) {
                found = true;
                candidate = peers[i];
            }
        }

        i = i + 1;
    }

    if (!found) {
        return NoPeer();
    }

    return candidate.id;
}

// DispatchNext is the core global scheduling rule. It first drops canceled
// queued work, then picks a peer that can accept now, then derives the timeout
// for the selected peer. The returned tuple is the contract the Go dispatcher
// should implement for one assignment attempt.
fun DispatchNext(peers: seq[PeerModel], tasks: seq[QueryTask],
    base_timeout_ms: int): DispatchResult {

    var peer_id: int;
    var task_id: int;
    var peer: PeerModel;

    task_id = FirstLiveTask(tasks);
    if (task_id == NoTask()) {
        return EmptyDispatch();
    }

    peer_id = SelectPeer(peers);
    if (peer_id == NoPeer()) {
        return EmptyDispatch();
    }

    peer = PeerByID(peers, peer_id);

    return (
        peer_id = peer_id,
        task_id = task_id,
        timeout_ms = QueryTimeoutForPeer(base_timeout_ms, peer)
    );
}

// The helpers below model peer-local queues. These queues are the foundation for
// work stealing: fast idle peers should be able to pull unclaimed work away from
// overloaded peers without violating local queue order.
fun AppendWork(work: seq[int], task_id: int): seq[int] {
    work += (sizeof(work), task_id);
    return work;
}

fun DropFirstWork(work: seq[int]): seq[int] {
    var result: seq[int];
    var i: int;

    i = 1;
    while (i < sizeof(work)) {
        result += (sizeof(result), work[i]);
        i = i + 1;
    }

    return result;
}

fun ReplacePeer(peers: seq[PeerModel], peer: PeerModel): seq[PeerModel] {
    var result: seq[PeerModel];
    var i: int;

    i = 0;
    while (i < sizeof(peers)) {
        if (peers[i].id == peer.id) {
            result += (sizeof(result), peer);
        } else {
            result += (sizeof(result), peers[i]);
        }

        i = i + 1;
    }

    return result;
}

// AssignLocalTask moves a queued task into a peer-owned local queue without
// marking the peer busy. This is the model form of the production manager's
// owned-work transition: ownership is visible to stealing, while active network
// dispatch remains a separate peer event.
fun AssignLocalTask(peers: seq[PeerModel], task: QueryTask): seq[PeerModel] {
    var owner_id: int;
    var owner: PeerModel;

    if (task.canceled) {
        return peers;
    }

    owner_id = SelectLocalOwner(peers);
    if (owner_id == NoPeer()) {
        return peers;
    }

    owner = PeerByID(peers, owner_id);
    owner = NewPeer(
        owner.id, owner.rank, owner.rtt_ms, owner.peer_state,
        AppendWork(owner.local_work, task.id)
    );

    return ReplacePeer(peers, owner);
}

// StealableWork returns how many local tasks can be moved away from a donor.
// Ready and busy donors keep one local task to avoid starvation and avoid
// steal ping-pong: if a peer just stole a small queue and started its first
// task, another idle peer should not immediately drain that last queued task.
// Blocked and quarantined donors may be drained because they are not accepting
// sends.
fun StealableWork(peer: PeerModel): int {
    if (peer.peer_state == PeerReady) {
        if (sizeof(peer.local_work) <= 1) {
            return 0;
        }

        return sizeof(peer.local_work) - 1;
    }

    if (peer.peer_state == PeerBusy) {
        if (sizeof(peer.local_work) <= 1) {
            return 0;
        }

        return sizeof(peer.local_work) - 1;
    }

    if (peer.peer_state == PeerBlocked ||
        peer.peer_state == PeerQuarantined) {

        return sizeof(peer.local_work);
    }

    return 0;
}

// StealDonorPriority separates work that is truly stranded from work that is
// merely queued behind an active peer. Blocked/quarantined donors are best
// because stealing from them directly heals a stall. Ready or busy donors with
// surplus local work are next. Busy singletons are not stealable; the active
// request should get a chance to finish or fail before the last queued task is
// considered stranded.
fun StealDonorPriority(peer: PeerModel): int {
    if (peer.peer_state == PeerBlocked ||
        peer.peer_state == PeerQuarantined) {

        return 3;
    }

    if (peer.peer_state == PeerReady && sizeof(peer.local_work) > 1) {
        return 2;
    }

    if (peer.peer_state == PeerBusy && sizeof(peer.local_work) > 1) {
        return 2;
    }

    return 0;
}

// StealDonorOrdersBefore chooses the donor whose work is most useful to move.
// Priority is considered before raw work count so stranded work on blocked
// peers is healed before merely balancing surplus ready or busy donors.
fun StealDonorOrdersBefore(peer: PeerModel, candidate: PeerModel): bool {
    if (StealDonorPriority(peer) > StealDonorPriority(candidate)) {
        return true;
    }

    if (StealDonorPriority(peer) < StealDonorPriority(candidate)) {
        return false;
    }

    if (StealableWork(peer) > StealableWork(candidate)) {
        return true;
    }

    if (StealableWork(peer) < StealableWork(candidate)) {
        return false;
    }

    if (peer.rank > candidate.rank) {
        return true;
    }

    if (peer.rank < candidate.rank) {
        return false;
    }

    return peer.id < candidate.id;
}

// SelectStealDonor chooses the best donor with stealable unclaimed work.
fun SelectStealDonor(peers: seq[PeerModel], thief_id: int): int {
    var i: int;
    var found: bool;
    var candidate: PeerModel;

    i = 0;
    while (i < sizeof(peers)) {
        if (peers[i].id != thief_id && StealableWork(peers[i]) > 0) {

            if (!found || StealDonorOrdersBefore(peers[i], candidate)) {

                found = true;
                candidate = peers[i];
            }
        }

        i = i + 1;
    }

    if (!found) {
        return NoPeer();
    }

    return candidate.id;
}

// StealOne is the work-stealing transition. An idle thief with an empty local
// queue can move the oldest unclaimed tasks from the most-loaded donor into its
// own local queue until the thief reaches capacity or the donor runs out of
// stealable work. The safety invariant remains: stealing never loses,
// duplicates, or reorders stolen tasks.
fun StealOne(peers: seq[PeerModel], thief_id: int): seq[PeerModel] {
    var thief: PeerModel;
    var donor: PeerModel;
    var donor_id: int;
    var stolen_task: int;
    var steal_count: int;
    var i: int;

    thief = PeerByID(peers, thief_id);
    if (!PeerCanAccept(thief) || sizeof(thief.local_work) != 0) {
        return peers;
    }

    donor_id = SelectStealDonor(peers, thief_id);
    if (donor_id == NoPeer()) {
        return peers;
    }

    donor = PeerByID(peers, donor_id);
    steal_count = StealableWork(donor);
    if (steal_count > PeerLocalSpare(thief)) {
        steal_count = PeerLocalSpare(thief);
    }

    if (steal_count == 0) {
        return peers;
    }

    i = 0;
    while (i < steal_count) {
        stolen_task = donor.local_work[0];
        donor = NewPeer(
            donor.id, donor.rank, donor.rtt_ms, donor.peer_state,
            DropFirstWork(donor.local_work)
        );
        thief = NewPeer(
            thief.id, thief.rank, thief.rtt_ms, thief.peer_state,
            AppendWork(thief.local_work, stolen_task)
        );

        i = i + 1;
    }

    peers = ReplacePeer(peers, donor);
    peers = ReplacePeer(peers, thief);

    return peers;
}
