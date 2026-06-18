// header_sync.p - Neutrino parallel header sync executable specification.
//
// This file is intentionally written in a literate style. The goal is to make
// the design readable to engineers who do not use P every day while still
// keeping the rules executable. The model focuses on the safety properties that
// matter when block header sync moves from one active peer to parallel ranges:
//
//   * ranges are only planned between confirmed anchors;
//   * a range lease has one current owner/epoch;
//   * stealing increments the epoch so old responses become stale;
//   * peers may complete ranges out of order;
//   * completed ranges are staged before commit;
//   * commits advance only through the next contiguous staged range;
//   * if a legacy/fallback writer already persisted headers, the model can
//     reconcile the visible tip before planning more frontier work.

enum HeaderPeerState {
    HeaderPeerReady,
    HeaderPeerBusy,
    HeaderPeerBlocked,
    HeaderPeerQuarantined,
    HeaderPeerDown
}

enum HeaderRangeState {
    RangeQueued,
    RangeOwned,
    RangeActive,
    RangeStaged,
    RangeCommitted,
    RangeFailed,
    RangeStale
}

type HeaderAnchor = (
    height: int,
    hash: int,
    trusted: bool,
    confirmations: int,
    rejected: bool
);

type ActiveAnchorRequest = (
    peer_id: int,
    start_height: int,
    stop_height: int
);

type HeaderRange = (
    id: int,
    start_height: int,
    start_hash: int,
    stop_height: int,
    stop_hash: int,
    lease_epoch: int,
    owner_peer: int,
    range_state: HeaderRangeState,
    valid: bool
);

type HeaderPeer = (
    id: int,
    rank: int,
    rtt_ms: int,
    peer_state: HeaderPeerState,
    local_ranges: seq[int],
    active_range: int
);

type HeaderSyncModel = (
    committed_tip: int,
    committed_hash: int,
    anchors: seq[HeaderAnchor],
    active_anchor_requests: seq[ActiveAnchorRequest],
    ranges: seq[HeaderRange],
    peers: seq[HeaderPeer],
    commit_log: seq[int]
);

fun NoPeer(): int { return 0; }
fun NoRange(): int { return 0; }
fun AnchorConfirmationsRequired(): int { return 2; }
fun MaxPeerOutstandingRanges(): int { return 4; }
fun MaxRangeHeaders(): int { return 2000; }

fun NewAnchor(height: int, hash: int, trusted: bool): HeaderAnchor {
    var confirmations: int;

    confirmations = 1;
    if (trusted) {
        confirmations = AnchorConfirmationsRequired();
    }

    return (
        height = height,
        hash = hash,
        trusted = trusted,
        confirmations = confirmations,
        rejected = false
    );
}

fun NewRange(id: int, start_height: int, start_hash: int,
    stop_height: int, stop_hash: int): HeaderRange {

    return (
        id = id,
        start_height = start_height,
        start_hash = start_hash,
        stop_height = stop_height,
        stop_hash = stop_hash,
        lease_epoch = 0,
        owner_peer = NoPeer(),
        range_state = RangeQueued,
        valid = false
    );
}

fun NewPeer(id: int, rank: int, rtt_ms: int,
    peer_state: HeaderPeerState): HeaderPeer {

    return (
        id = id,
        rank = rank,
        rtt_ms = rtt_ms,
        peer_state = peer_state,
        local_ranges = default(seq[int]),
        active_range = NoRange()
    );
}

fun NewModel(committed_tip: int, committed_hash: int): HeaderSyncModel {
    return (
        committed_tip = committed_tip,
        committed_hash = committed_hash,
        anchors = default(seq[HeaderAnchor]),
        active_anchor_requests = default(seq[ActiveAnchorRequest]),
        ranges = default(seq[HeaderRange]),
        peers = default(seq[HeaderPeer]),
        commit_log = default(seq[int])
    );
}

// AdvanceCommittedTip models the production reconciliation path between the
// pure scheduler and the durable block-header store. Most progress should come
// from CommitReadyRanges, but during migration the block manager can still
// persist a short serial fallback span. Once storage says the tip is already
// ahead, the scheduler must not keep planning from its stale in-memory tip or
// it can skip the newly persisted boundary and strand staged frontier ranges.
fun AdvanceCommittedTip(model: HeaderSyncModel, height: int,
    hash: int): HeaderSyncModel {

    var i: int;
    var range: HeaderRange;

    if (height < model.committed_tip) {
        return model;
    }

    model.anchors = AddOrConfirmAnchor(model.anchors, height, hash, true);
    if (height == model.committed_tip) {
        model.committed_hash = hash;
        return model;
    }

    model.committed_tip = height;
    model.committed_hash = hash;

    i = 0;
    while (i < sizeof(model.ranges)) {
        range = model.ranges[i];
        if (range.stop_height <= height) {
            range = (
                id = range.id,
                start_height = range.start_height,
                start_hash = range.start_hash,
                stop_height = range.stop_height,
                stop_hash = range.stop_hash,
                lease_epoch = range.lease_epoch,
                owner_peer = NoPeer(),
                range_state = RangeCommitted,
                valid = true
            );
            model.ranges = ReplaceRange(model.ranges, range);
        } else if (range.start_height < height) {
            range = (
                id = range.id,
                start_height = range.start_height,
                start_hash = range.start_hash,
                stop_height = range.stop_height,
                stop_hash = range.stop_hash,
                lease_epoch = range.lease_epoch,
                owner_peer = NoPeer(),
                range_state = RangeStale,
                valid = false
            );
            model.ranges = ReplaceRange(model.ranges, range);
        }

        i = i + 1;
    }

    return model;
}

// AddOrConfirmAnchor is the anchor-discovery rule. Trusted checkpoints are
// immediately confirmed. Discovered anchors require independent matching
// observations. Conflicting discovered anchors for the same height are marked
// rejected so the range planner cannot fan out work across an ambiguous
// boundary. Trusted checkpoints can replace a discovered conflict because they
// are a local chain parameter, not a peer claim.
fun AddOrConfirmAnchor(anchors: seq[HeaderAnchor], height: int, hash: int,
    trusted: bool): seq[HeaderAnchor] {

    var result: seq[HeaderAnchor];
    var i: int;
    var anchor: HeaderAnchor;
    var found: bool;

    i = 0;
    while (i < sizeof(anchors)) {
        anchor = anchors[i];
        if (anchor.height == height) {
            found = true;
            if (anchor.hash == hash) {
                if (trusted) {
                    anchor = NewAnchor(height, hash, true);
                } else if (!anchor.rejected && anchor.confirmations <
                    AnchorConfirmationsRequired()) {

                    anchor = (
                        height = anchor.height,
                        hash = anchor.hash,
                        trusted = anchor.trusted,
                        confirmations = anchor.confirmations + 1,
                        rejected = anchor.rejected
                    );
                }
            } else if (trusted) {
                anchor = NewAnchor(height, hash, true);
            } else if (!anchor.trusted) {
                anchor = (
                    height = anchor.height,
                    hash = anchor.hash,
                    trusted = anchor.trusted,
                    confirmations = anchor.confirmations,
                    rejected = true
                );
            }
        }

        result += (sizeof(result), anchor);
        i = i + 1;
    }

    if (!found) {
        result += (sizeof(result), NewAnchor(height, hash, trusted));
    }

    return result;
}

fun AnchorByHeight(anchors: seq[HeaderAnchor], height: int): HeaderAnchor {
    var i: int;

    i = 0;
    while (i < sizeof(anchors)) {
        if (anchors[i].height == height) {
            return anchors[i];
        }

        i = i + 1;
    }

    return NewAnchor(0, 0, false);
}

fun AnchorConfirmed(anchor: HeaderAnchor): bool {
    return !anchor.rejected && (
        anchor.trusted ||
        anchor.confirmations >= AnchorConfirmationsRequired()
    );
}

// Active anchor requests are in-flight getheaders messages used to discover or
// hedge a future boundary. They are not chain state. Once ordered commit reaches
// or passes their stop height, any late response for that request cannot unlock
// new work; keeping it active would only block the scheduler until timeout.
fun AddActiveAnchorRequest(model: HeaderSyncModel, peer_id: int,
    start_height: int, stop_height: int): HeaderSyncModel {

    model.active_anchor_requests += (
        sizeof(model.active_anchor_requests),
        (peer_id = peer_id, start_height = start_height,
            stop_height = stop_height)
    );

    return model;
}

fun PruneObsoleteAnchorRequests(model: HeaderSyncModel,
    committed_tip: int): HeaderSyncModel {

    var kept: seq[ActiveAnchorRequest];
    var i: int;
    var request: ActiveAnchorRequest;

    i = 0;
    while (i < sizeof(model.active_anchor_requests)) {
        request = model.active_anchor_requests[i];
        if (request.stop_height > committed_tip) {
            kept += (sizeof(kept), request);
        }

        i = i + 1;
    }

    model.active_anchor_requests = kept;

    return model;
}

// NextAnchorDiscoveryStart is the fanout-window rule. The visible chain tip may
// still be waiting for an earlier range to commit, but anchor discovery is
// allowed to run ahead from the furthest confirmed anchor we already know. This
// keeps future range boundaries warming while earlier peer actors are still
// downloading or committing their ranges.
fun NextAnchorDiscoveryStart(model: HeaderSyncModel, committed_tip: int,
    committed_hash: int, stop_height: int): HeaderAnchor {

    var i: int;
    var frontier: HeaderAnchor;
    var anchor: HeaderAnchor;

    frontier = NewAnchor(committed_tip, committed_hash, true);
    i = 0;
    while (i < sizeof(model.anchors)) {
        anchor = model.anchors[i];
        if (anchor.height >= committed_tip &&
            anchor.height < stop_height &&
            AnchorConfirmed(anchor) &&
            anchor.height > frontier.height) {

            frontier = anchor;
        }

        i = i + 1;
    }

    return frontier;
}

fun ShouldDiscoverAnchorAhead(model: HeaderSyncModel, committed_tip: int,
    committed_hash: int, stop_height: int): bool {

    var frontier: HeaderAnchor;

    frontier = NextAnchorDiscoveryStart(
        model, committed_tip, committed_hash, stop_height
    );

    return stop_height - frontier.height > MaxRangeHeaders();
}

// NextFrontierDiscoveryStop is the post-checkpoint lane. Once there is no
// trusted checkpoint hash ahead, getheaders uses a zero stop hash and peers
// return up to the protocol cap. The manager still bounds the expected height
// window so two matching peer responses can confirm the newly discovered
// boundary and stage the exact headers already downloaded.
fun NextFrontierDiscoveryStop(frontier_height: int,
    best_height: int): int {

    if (best_height - frontier_height > MaxRangeHeaders()) {
        return frontier_height + MaxRangeHeaders();
    }

    return best_height;
}

fun RangeByID(ranges: seq[HeaderRange], id: int): HeaderRange {
    var i: int;

    i = 0;
    while (i < sizeof(ranges)) {
        if (ranges[i].id == id) {
            return ranges[i];
        }

        i = i + 1;
    }

    return NewRange(NoRange(), 0, 0, 0, 0);
}

fun RangeBySpan(ranges: seq[HeaderRange], start_height: int,
    stop_height: int): HeaderRange {

    var i: int;

    i = 0;
    while (i < sizeof(ranges)) {
        if (ranges[i].start_height == start_height &&
            ranges[i].stop_height == stop_height &&
            !(ranges[i].range_state == RangeFailed ||
                ranges[i].range_state == RangeStale)) {

            return ranges[i];
        }

        i = i + 1;
    }

    return NewRange(NoRange(), 0, 0, 0, 0);
}

fun PeerByID(peers: seq[HeaderPeer], id: int): HeaderPeer {
    var i: int;

    i = 0;
    while (i < sizeof(peers)) {
        if (peers[i].id == id) {
            return peers[i];
        }

        i = i + 1;
    }

    return NewPeer(NoPeer(), 0, 0, HeaderPeerDown);
}

fun ReplaceRange(ranges: seq[HeaderRange],
    next: HeaderRange): seq[HeaderRange] {

    var result: seq[HeaderRange];
    var i: int;

    i = 0;
    while (i < sizeof(ranges)) {
        if (ranges[i].id == next.id) {
            result += (sizeof(result), next);
        } else {
            result += (sizeof(result), ranges[i]);
        }

        i = i + 1;
    }

    return result;
}

fun ReplacePeer(peers: seq[HeaderPeer], next: HeaderPeer): seq[HeaderPeer] {
    var result: seq[HeaderPeer];
    var i: int;

    i = 0;
    while (i < sizeof(peers)) {
        if (peers[i].id == next.id) {
            result += (sizeof(result), next);
        } else {
            result += (sizeof(result), peers[i]);
        }

        i = i + 1;
    }

    return result;
}

fun AppendID(ids: seq[int], id: int): seq[int] {
    ids += (sizeof(ids), id);
    return ids;
}

fun DropFirstID(ids: seq[int]): seq[int] {
    var result: seq[int];
    var i: int;

    i = 1;
    while (i < sizeof(ids)) {
        result += (sizeof(result), ids[i]);
        i = i + 1;
    }

    return result;
}

fun RemoveID(ids: seq[int], id: int): seq[int] {
    var result: seq[int];
    var i: int;

    i = 0;
    while (i < sizeof(ids)) {
        if (ids[i] != id) {
            result += (sizeof(result), ids[i]);
        }

        i = i + 1;
    }

    return result;
}

fun PeerOutstandingRanges(peer: HeaderPeer): int {
    var count: int;

    count = sizeof(peer.local_ranges);
    if (peer.active_range != NoRange()) {
        count = count + 1;
    }

    return count;
}

fun PeerCanOwnRange(peer: HeaderPeer): bool {
    if (!(peer.peer_state == HeaderPeerReady ||
        peer.peer_state == HeaderPeerBusy)) {

        return false;
    }

    return PeerOutstandingRanges(peer) < MaxPeerOutstandingRanges();
}

fun PeerCanStartRange(peer: HeaderPeer): bool {
    return peer.peer_state == HeaderPeerReady &&
        peer.active_range == NoRange() &&
        sizeof(peer.local_ranges) > 0;
}

fun PeerOrdersBefore(peer: HeaderPeer, candidate: HeaderPeer): bool {
    if (peer.rank < candidate.rank) { return true; }
    if (peer.rank > candidate.rank) { return false; }
    if (peer.rtt_ms < candidate.rtt_ms) { return true; }
    if (peer.rtt_ms > candidate.rtt_ms) { return false; }

    return peer.id < candidate.id;
}

fun SelectRangeOwner(peers: seq[HeaderPeer]): int {
    var i: int;
    var found: bool;
    var candidate: HeaderPeer;

    i = 0;
    while (i < sizeof(peers)) {
        if (PeerCanOwnRange(peers[i])) {
            if (!found ||
                PeerOutstandingRanges(peers[i]) <
                    PeerOutstandingRanges(candidate) ||
                (PeerOutstandingRanges(peers[i]) ==
                    PeerOutstandingRanges(candidate) &&
                    PeerOrdersBefore(peers[i], candidate))) {

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

fun CanPlanRange(model: HeaderSyncModel, start_height: int,
    stop_height: int): bool {

    var start_anchor: HeaderAnchor;
    var stop_anchor: HeaderAnchor;

    if (start_height >= stop_height) {
        return false;
    }
    // Bitcoin getheaders replies are capped at 2000 headers. A larger span may
    // still be safe eventually, but only after anchor discovery creates an
    // intermediate confirmed boundary. Until then, planning it as one range would
    // silently turn the parallel scheduler back into a partial serial download.
    if (stop_height - start_height > MaxRangeHeaders()) {
        return false;
    }

    start_anchor = AnchorByHeight(model.anchors, start_height);
    stop_anchor = AnchorByHeight(model.anchors, stop_height);

    return AnchorConfirmed(start_anchor) && AnchorConfirmed(stop_anchor);
}

fun PlanRange(model: HeaderSyncModel, id: int, start_height: int,
    stop_height: int): HeaderSyncModel {

    var start_anchor: HeaderAnchor;
    var stop_anchor: HeaderAnchor;

    if (!CanPlanRange(model, start_height, stop_height)) {
        return model;
    }

    start_anchor = AnchorByHeight(model.anchors, start_height);
    stop_anchor = AnchorByHeight(model.anchors, stop_height);
    model.ranges += (sizeof(model.ranges), NewRange(
        id, start_height, start_anchor.hash, stop_height, stop_anchor.hash
    ));

    return model;
}

// StageDiscoveredRange is the key pipelining rule for wide checkpoint gaps.
// Anchor discovery already downloads the headers from the current frontier to
// the next 2000-header boundary. Once that boundary is confirmed, those same
// headers are not just metadata: they are a valid completed range. Staging the
// discovery response keeps the frontier moving without a second getheaders
// round trip for the same span.
fun StageDiscoveredRange(model: HeaderSyncModel, id: int, peer_id: int,
    start_height: int, stop_height: int): HeaderSyncModel {

    var start_anchor: HeaderAnchor;
    var stop_anchor: HeaderAnchor;
    var existing: HeaderRange;
    var staged: HeaderRange;

    existing = RangeBySpan(model.ranges, start_height, stop_height);
    if (existing.id != NoRange()) {
        return model;
    }

    if (!CanPlanRange(model, start_height, stop_height)) {
        return model;
    }

    start_anchor = AnchorByHeight(model.anchors, start_height);
    stop_anchor = AnchorByHeight(model.anchors, stop_height);
    staged = NewRange(
        id, start_height, start_anchor.hash, stop_height, stop_anchor.hash
    );
    staged = (
        id = staged.id,
        start_height = staged.start_height,
        start_hash = staged.start_hash,
        stop_height = staged.stop_height,
        stop_hash = staged.stop_hash,
        lease_epoch = staged.lease_epoch + 1,
        owner_peer = peer_id,
        range_state = RangeStaged,
        valid = true
    );
    model.ranges += (sizeof(model.ranges), staged);

    return model;
}

fun AddPeer(model: HeaderSyncModel, peer: HeaderPeer): HeaderSyncModel {
    model.peers += (sizeof(model.peers), peer);
    return model;
}

// SetPeerState models the production cooldown/recovery boundary. The timer
// itself lives outside the pure model; once cooldown expires, the manager emits
// an explicit ready event and the peer can be considered for ownership again.
fun SetPeerState(model: HeaderSyncModel, peer_id: int,
    peer_state: HeaderPeerState): HeaderSyncModel {

    var peer: HeaderPeer;

    peer = PeerByID(model.peers, peer_id);
    peer = (
        id = peer.id,
        rank = peer.rank,
        rtt_ms = peer.rtt_ms,
        peer_state = peer_state,
        local_ranges = peer.local_ranges,
        active_range = peer.active_range
    );
    model.peers = ReplacePeer(model.peers, peer);

    return model;
}

fun AssignNextQueuedRange(model: HeaderSyncModel): HeaderSyncModel {
    var i: int;
    var owner_id: int;
    var owner: HeaderPeer;
    var range: HeaderRange;

    owner_id = SelectRangeOwner(model.peers);
    if (owner_id == NoPeer()) {
        return model;
    }

    i = 0;
    while (i < sizeof(model.ranges)) {
        if (model.ranges[i].range_state == RangeQueued) {
            owner = PeerByID(model.peers, owner_id);
            range = model.ranges[i];
            range = (
                id = range.id,
                start_height = range.start_height,
                start_hash = range.start_hash,
                stop_height = range.stop_height,
                stop_hash = range.stop_hash,
                lease_epoch = range.lease_epoch + 1,
                owner_peer = owner_id,
                range_state = RangeOwned,
                valid = false
            );
            owner = (
                id = owner.id,
                rank = owner.rank,
                rtt_ms = owner.rtt_ms,
                peer_state = owner.peer_state,
                local_ranges = AppendID(owner.local_ranges, range.id),
                active_range = owner.active_range
            );

            model.ranges = ReplaceRange(model.ranges, range);
            model.peers = ReplacePeer(model.peers, owner);

            return model;
        }

        i = i + 1;
    }

    return model;
}

fun StartPeerRange(model: HeaderSyncModel,
    peer_id: int): HeaderSyncModel {

    var peer: HeaderPeer;
    var range_id: int;
    var range: HeaderRange;

    peer = PeerByID(model.peers, peer_id);
    if (!PeerCanStartRange(peer)) {
        return model;
    }

    range_id = peer.local_ranges[0];
    range = RangeByID(model.ranges, range_id);
    if (range.range_state != RangeOwned || range.owner_peer != peer_id) {
        return model;
    }

    peer = (
        id = peer.id,
        rank = peer.rank,
        rtt_ms = peer.rtt_ms,
        peer_state = HeaderPeerBusy,
        local_ranges = DropFirstID(peer.local_ranges),
        active_range = range_id
    );
    range = (
        id = range.id,
        start_height = range.start_height,
        start_hash = range.start_hash,
        stop_height = range.stop_height,
        stop_hash = range.stop_hash,
        lease_epoch = range.lease_epoch,
        owner_peer = peer_id,
        range_state = RangeActive,
        valid = false
    );

    model.peers = ReplacePeer(model.peers, peer);
    model.ranges = ReplaceRange(model.ranges, range);

    return model;
}

// StealRange moves either unstarted owned work or an expired active range to a
// ready thief. The caller models the timeout/expiry decision. The key safety
// action is the lease epoch bump: any response from the old owner is now stale.
fun StealRange(model: HeaderSyncModel, thief_id: int,
    range_id: int): HeaderSyncModel {

    var thief: HeaderPeer;
    var donor: HeaderPeer;
    var range: HeaderRange;

    thief = PeerByID(model.peers, thief_id);
    range = RangeByID(model.ranges, range_id);
    if (!PeerCanOwnRange(thief) || range.owner_peer == NoPeer() ||
        range.owner_peer == thief_id ||
        !(range.range_state == RangeOwned ||
            range.range_state == RangeActive)) {

        return model;
    }

    donor = PeerByID(model.peers, range.owner_peer);
    donor = (
        id = donor.id,
        rank = donor.rank,
        rtt_ms = donor.rtt_ms,
        peer_state = HeaderPeerReady,
        local_ranges = RemoveID(donor.local_ranges, range_id),
        active_range = NoRange()
    );
    thief = (
        id = thief.id,
        rank = thief.rank,
        rtt_ms = thief.rtt_ms,
        peer_state = thief.peer_state,
        local_ranges = AppendID(thief.local_ranges, range_id),
        active_range = thief.active_range
    );
    range = (
        id = range.id,
        start_height = range.start_height,
        start_hash = range.start_hash,
        stop_height = range.stop_height,
        stop_hash = range.stop_hash,
        lease_epoch = range.lease_epoch + 1,
        owner_peer = thief_id,
        range_state = RangeOwned,
        valid = false
    );

    model.peers = ReplacePeer(model.peers, donor);
    model.peers = ReplacePeer(model.peers, thief);
    model.ranges = ReplaceRange(model.ranges, range);

    return model;
}

fun CompleteRange(model: HeaderSyncModel, peer_id: int, range_id: int,
    lease_epoch: int, valid: bool): HeaderSyncModel {

    var peer: HeaderPeer;
    var range: HeaderRange;
    var next_peer_state: HeaderPeerState;

    range = RangeByID(model.ranges, range_id);
    if (range.owner_peer != peer_id || range.lease_epoch != lease_epoch ||
        range.range_state != RangeActive) {

        return model;
    }

    peer = PeerByID(model.peers, peer_id);
    next_peer_state = peer.peer_state;
    if (next_peer_state == HeaderPeerBusy) {
        next_peer_state = HeaderPeerReady;
    }
    peer = (
        id = peer.id,
        rank = peer.rank,
        rtt_ms = peer.rtt_ms,
        peer_state = next_peer_state,
        local_ranges = peer.local_ranges,
        active_range = NoRange()
    );

    if (valid) {
        range = (
            id = range.id,
            start_height = range.start_height,
            start_hash = range.start_hash,
            stop_height = range.stop_height,
            stop_hash = range.stop_hash,
            lease_epoch = range.lease_epoch,
            owner_peer = peer_id,
            range_state = RangeStaged,
            valid = true
        );
    } else {
        range = (
            id = range.id,
            start_height = range.start_height,
            start_hash = range.start_hash,
            stop_height = range.stop_height,
            stop_hash = range.stop_hash,
            lease_epoch = range.lease_epoch,
            owner_peer = peer_id,
            range_state = RangeFailed,
            valid = false
        );
    }

    model.peers = ReplacePeer(model.peers, peer);
    model.ranges = ReplaceRange(model.ranges, range);

    return model;
}

fun NextCommittableRangeAt(model: HeaderSyncModel, tip_height: int,
    tip_hash: int): HeaderRange {

    var i: int;

    i = 0;
    while (i < sizeof(model.ranges)) {
        if (model.ranges[i].range_state == RangeStaged &&
            model.ranges[i].valid &&
            model.ranges[i].start_height == tip_height &&
            model.ranges[i].start_hash == tip_hash) {

            return model.ranges[i];
        }

        i = i + 1;
    }

    return NewRange(NoRange(), 0, 0, 0, 0);
}

fun NextCommittableRange(model: HeaderSyncModel): HeaderRange {
    return NextCommittableRangeAt(
        model, model.committed_tip, model.committed_hash
    );
}

// ReadyRanges is the model equivalent of the production preflight used by the
// blockmanager. It tells the implementation which staged ranges are safe to
// persist to disk, but it deliberately does not advance committed_tip. That
// separation is what lets side-effectful storage writes happen before the FSM
// exposes a newer visible chain tip.
fun ReadyRanges(model: HeaderSyncModel): seq[int] {
    var ready: seq[int];
    var tip_height: int;
    var tip_hash: int;
    var next: HeaderRange;

    tip_height = model.committed_tip;
    tip_hash = model.committed_hash;
    next = NextCommittableRangeAt(model, tip_height, tip_hash);

    while (next.id != NoRange()) {
        ready += (sizeof(ready), next.id);
        tip_height = next.stop_height;
        tip_hash = next.stop_hash;
        next = NextCommittableRangeAt(model, tip_height, tip_hash);
    }

    return ready;
}

fun CommitReadyRanges(model: HeaderSyncModel): HeaderSyncModel {
    var next: HeaderRange;

    next = NextCommittableRange(model);
    while (next.id != NoRange()) {
        next = (
            id = next.id,
            start_height = next.start_height,
            start_hash = next.start_hash,
            stop_height = next.stop_height,
            stop_hash = next.stop_hash,
            lease_epoch = next.lease_epoch,
            owner_peer = next.owner_peer,
            range_state = RangeCommitted,
            valid = true
        );

        model.ranges = ReplaceRange(model.ranges, next);
        model.committed_tip = next.stop_height;
        model.committed_hash = next.stop_hash;
        model.commit_log += (sizeof(model.commit_log), next.id);

        next = NextCommittableRange(model);
    }

    return model;
}

// CommitReadyRangesAndPruneObsoleteAnchors is the controller-level production
// rule. The manager first advances visible chain state through contiguous
// staged ranges. Only after that state transition is durable may the controller
// remove obsolete hedged anchor requests whose stop height is no longer ahead
// of the visible tip.
fun CommitReadyRangesAndPruneObsoleteAnchors(
    model: HeaderSyncModel): HeaderSyncModel {

    model = CommitReadyRanges(model);
    model = PruneObsoleteAnchorRequests(model, model.committed_tip);

    return model;
}
