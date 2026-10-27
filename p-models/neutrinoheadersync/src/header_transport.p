// This layer models request identity and retained payloads around the scheduler.
// A wire reply has no request ID. Its first parent can exclude a different
// locator, but two leases sharing a locator are indistinguishable on the wire.
// Timer callbacks do carry a generation, so an old timeout cannot cancel a
// replacement request. Elapsed time itself is abstracted to timer delivery.
type HeaderWireRequest = (
    peer_id: int, range_id: int, lease_epoch: int, token: int, live: bool
);

type HeaderWireState = (
    scheduler: HeaderSyncModel,
    requests: seq[HeaderWireRequest],
    payloads: seq[int],
    validation_tip: int,
    retry_pending: bool,
    retry_generation: int
);

fun ContainsID(ids: seq[int], id: int): bool {
    var i: int;
    i = 0;
    while (i < sizeof(ids)) {
        if (ids[i] == id) { return true; }
        i = i + 1;
    }
    return false;
}

fun NewHeaderWireState(model: HeaderSyncModel): HeaderWireState {
    return (scheduler = model, requests = default(seq[HeaderWireRequest]),
        payloads = default(seq[int]), validation_tip = model.committed_tip,
        retry_pending = false,
        retry_generation = 0);
}

fun ArmHeaderWireRequest(wire: HeaderWireState, peer_id: int,
    range_id: int, token: int): HeaderWireState {

    var i: int;
    var kept: seq[HeaderWireRequest];
    var range: HeaderRange;
    range = RangeByID(wire.scheduler.ranges, range_id);
    assert range.range_state == RangeActive && range.owner_peer == peer_id;
    i = 0;
    while (i < sizeof(wire.requests)) {
        if (wire.requests[i].peer_id != peer_id) {
            kept += (sizeof(kept), wire.requests[i]);
        }
        i = i + 1;
    }
    kept += (sizeof(kept), (peer_id = peer_id, range_id = range_id,
        lease_epoch = range.lease_epoch, token = token, live = true));
    wire.requests = kept;
    return wire;
}

fun ExpireHeaderWireRequest(wire: HeaderWireState, peer_id: int,
    token: int): HeaderWireState {

    var i: int;
    var request: HeaderWireRequest;
    i = 0;
    while (i < sizeof(wire.requests)) {
        request = wire.requests[i];
        if (request.peer_id == peer_id && request.token == token &&
            request.live) {

            request.live = false;
            wire.requests[i] = request;
            wire.scheduler = SetPeerState(wire.scheduler, peer_id,
                HeaderPeerQuarantined);
        }
        i = i + 1;
    }
    return wire;
}

// Expiry alone need not change the scheduler lease epoch. A late reply must
// therefore be rejected by request matching before CompleteRange is called.
// Otherwise a reply can create staged work with no retained header payload.
fun DeliverHeaderWireReply(wire: HeaderWireState, peer_id: int,
    reply_range: int, valid: bool): HeaderWireState {

    var i: int;
    var request: HeaderWireRequest;
    var range: HeaderRange;
    var reply: HeaderRange;
    reply = RangeByID(wire.scheduler.ranges, reply_range);
    i = 0;
    while (i < sizeof(wire.requests)) {
        request = wire.requests[i];
        if (request.peer_id == peer_id && request.live &&
            RangeByID(wire.scheduler.ranges,
                request.range_id).start_hash == reply.start_hash) {

            request.live = false;
            wire.requests[i] = request;
            wire.scheduler = CompleteRange(wire.scheduler, peer_id,
                request.range_id, request.lease_epoch, valid);
            range = RangeByID(wire.scheduler.ranges, request.range_id);
            if (range.range_state == RangeStaged &&
                !ContainsID(wire.payloads, range.id)) {

                wire.payloads += (sizeof(wire.payloads), range.id);
            }
            return wire;
        }
        i = i + 1;
    }
    return wire;
}

// A store outage must leave a retry obligation even when every network request
// has completed. One pending timer coalesces repeated failures. Recovery drains
// staging without requiring another peer reply or downloading the data again.
fun TryHeaderWireCommit(wire: HeaderWireState,
    storage_ok: bool): HeaderWireState {

    var i: int;
    var kept: seq[int];
    var ready: seq[int];
    ready = ReadyRanges(wire.scheduler);
    if (sizeof(ready) == 0) {
        return wire;
    }
    // The compatibility validator can prepare a memory tip before writing.
    // On failure it must restore the durable parent, otherwise a later actor
    // commit rejects its own retained range as disconnected from that parent.
    wire.validation_tip = RangeByID(wire.scheduler.ranges,
        ready[sizeof(ready) - 1]).stop_height;
    if (!storage_ok) {
        wire.validation_tip = wire.scheduler.committed_tip;
        if (!wire.retry_pending) {
            wire.retry_pending = true;
            wire.retry_generation = wire.retry_generation + 1;
        }
        return wire;
    }
    wire.scheduler = CommitReadyRanges(wire.scheduler);
    wire.validation_tip = wire.scheduler.committed_tip;
    if (wire.retry_pending) {
        wire.retry_pending = false;
        wire.retry_generation = wire.retry_generation + 1;
    }
    i = 0;
    while (i < sizeof(wire.payloads)) {
        if (RangeByID(wire.scheduler.ranges,
            wire.payloads[i]).range_state != RangeCommitted) {

            kept += (sizeof(kept), wire.payloads[i]);
        }
        i = i + 1;
    }
    wire.payloads = kept;
    return wire;
}

fun RetryHeaderWireCommit(wire: HeaderWireState, generation: int,
    storage_ok: bool): HeaderWireState {

    var previous_tip: int;
    if (!wire.retry_pending || generation != wire.retry_generation) {
        return wire;
    }
    previous_tip = wire.scheduler.committed_tip;
    wire.retry_pending = false;
    wire = TryHeaderWireCommit(wire, storage_ok);
    if (wire.scheduler.committed_tip > previous_tip) {
        wire = ResumeHeaderWirePlanning(wire);
    }
    return wire;
}

// A successful retry is a scheduling event, not just a storage event. At a
// checkpoint it must plan the next known span without waiting for more network
// traffic. This projects continuation onto the nearest schedulable anchor;
// discovery beyond sparse anchors remains in the main scheduler model.
fun ResumeHeaderWirePlanning(wire: HeaderWireState): HeaderWireState {
    var i: int;
    var stop: int;
    var next_id: int;
    var height: int;
    stop = 0;
    next_id = 1;
    i = 0;
    while (i < sizeof(wire.scheduler.ranges)) {
        if (wire.scheduler.ranges[i].id >= next_id) {
            next_id = wire.scheduler.ranges[i].id + 1;
        }
        i = i + 1;
    }
    i = 0;
    while (i < sizeof(wire.scheduler.anchors)) {
        height = wire.scheduler.anchors[i].height;
        if (CanPlanRange(wire.scheduler, wire.scheduler.committed_tip,
            height) && (stop == 0 || height < stop)) {

            stop = height;
        }
        i = i + 1;
    }
    if (stop > 0) {
        wire.scheduler = PlanRange(wire.scheduler, next_id,
            wire.scheduler.committed_tip, stop);
        wire.scheduler = AssignNextQueuedRange(wire.scheduler);
    }
    return wire;
}

fun AssertHeaderWirePayloads(wire: HeaderWireState) {
    var i: int;
    var range: HeaderRange;
    i = 0;
    while (i < sizeof(wire.scheduler.ranges)) {
        range = wire.scheduler.ranges[i];
        if (range.range_state == RangeStaged) {
            assert ContainsID(wire.payloads, range.id),
                "staged work must have a retained payload";
        }
        i = i + 1;
    }
}
