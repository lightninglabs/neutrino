// header_sync_explore.p - nondeterministic exploration of the header sync
// model.
//
// The scripted tests in header_sync_test.p each follow one timeline. This
// file lets the checker choose the timeline instead: an environment machine
// picks every scheduler and peer event with choose and $, within a step
// budget, and two spec monitors judge every schedule.
//
//   * HeaderSyncSafety checks, after every event, that each range lease has
//     one owner and the peer table agrees with it, that an active range is
//     owned by the peer that runs it, that the committed tip never moves
//     back, and that commits drain in height order.
// Each peer is its own machine that sends replies concurrently, so the
// checker also explores their interleavings, not just the explorer's choices.
//
//   * HeaderSyncLiveness stays hot until the explorer reaches the target tip.
//     After the fault budget is spent, the explorer heals the environment and
//     drives only honest events. A schedule that never syncs leaves the
//     monitor hot, which the checker reports as a liveness violation.

event eHeaderSyncModel: HeaderSyncModel;
event ePeerReply: (peer_id: int, valid: bool, stale: bool);
event ePeerQuiet: int;
event eHeaderSyncExploreStart;
event eHeaderSyncSynced;

// ExplorePeers is the number of peers in the explored network.
fun ExplorePeers(): int { return 3; }

// ExploreSpan is the distance between trusted anchors.
fun ExploreSpan(): int { return 10; }

// ExploreTarget is the tip every schedule must reach after healing.
fun ExploreTarget(): int { return 30; }

// ExploreFaultSteps bounds the explorer's own nondeterministic events per
// peer reply.
fun ExploreFaultSteps(): int { return 2; }

// ExplorePeerReplies bounds the replies each peer machine sends.
fun ExplorePeerReplies(): int { return 3; }

// ExploreHealRounds bounds the honest phase.
fun ExploreHealRounds(): int { return 12; }

// ExploreHash is the honest anchor hash at height.
fun ExploreHash(height: int): int { return 100 + height; }

// SpanHasLiveRange reports whether a non-terminal range covers the span.
fun SpanHasLiveRange(model: HeaderSyncModel, start_height: int): bool {
    var i: int;
    var range: HeaderRange;

    i = 0;
    while (i < sizeof(model.ranges)) {
        range = model.ranges[i];
        if (range.start_height == start_height &&
            range.range_state != RangeFailed &&
            range.range_state != RangeStale) {

            return true;
        }
        i = i + 1;
    }
    return false;
}

spec HeaderSyncSafety observes eHeaderSyncModel {
    var last_tip: int;

    start state Watch {
        on eHeaderSyncModel do (model: HeaderSyncModel) {
            var owners: map[int, int];
            var i: int;
            var j: int;
            var peer: HeaderPeer;
            var range: HeaderRange;
            var last_start: int;

            assert model.committed_tip >= last_tip,
                format("committed tip moved back {0} -> {1}", last_tip,
                    model.committed_tip);
            last_tip = model.committed_tip;

            i = 0;
            while (i < sizeof(model.peers)) {
                peer = model.peers[i];
                j = 0;
                while (j < sizeof(peer.local_ranges)) {
                    assert !(peer.local_ranges[j] in owners),
                        format("range {0} has two owners",
                            peer.local_ranges[j]);
                    owners[peer.local_ranges[j]] = peer.id;
                    j = j + 1;
                }
                if (peer.active_range != NoRange()) {
                    assert !(peer.active_range in owners),
                        format("range {0} has two owners",
                            peer.active_range);
                    owners[peer.active_range] = peer.id;
                    range = RangeByID(model.ranges, peer.active_range);
                    assert range.range_state == RangeActive &&
                        range.owner_peer == peer.id,
                        format("peer {0} runs range {1} it does not own",
                            peer.id, peer.active_range);
                }
                i = i + 1;
            }

            last_start = -1;
            i = 0;
            while (i < sizeof(model.commit_log)) {
                range = RangeByID(model.ranges, model.commit_log[i]);
                assert range.start_height > last_start,
                    "commits must drain in height order";
                last_start = range.start_height;
                i = i + 1;
            }
        }
    }
}

spec HeaderSyncLiveness observes eHeaderSyncExploreStart,
    eHeaderSyncSynced {

    start cold state Idle {
        on eHeaderSyncExploreStart goto Syncing;
    }

    hot state Syncing {
        on eHeaderSyncSynced goto Synced;
    }

    cold state Synced {
        ignore eHeaderSyncExploreStart, eHeaderSyncSynced;
    }
}

// HeaderSyncPeerEnv is one peer's network behaviour: a bounded series of
// replies, each valid or not and possibly late after its lease moved.
machine HeaderSyncPeerEnv {
    start state Run {
        entry (args: (explorer: machine, peer_id: int)) {
            var i: int;

            i = 0;
            while (i < ExplorePeerReplies()) {
                send args.explorer, ePeerReply, (peer_id = args.peer_id,
                    valid = $, stale = $);
                i = i + 1;
            }
            send args.explorer, ePeerQuiet, args.peer_id;
        }
    }
}

machine HeaderSyncExplorer {
    var model: HeaderSyncModel;
    var next_id: int;
    var quiet: int;

    start state Init {
        entry {
            var height: int;
            var peer_id: int;

            if ($) {
                model = NewProductionModel(0, ExploreHash(0));
            } else {
                model = NewModel(0, ExploreHash(0));
            }
            height = 0;
            while (height <= ExploreTarget()) {
                model = AddOrConfirmAnchor(model, height,
                    ExploreHash(height), true);
                height = height + ExploreSpan();
            }
            peer_id = 1;
            while (peer_id <= ExplorePeers()) {
                model = AddPeer(model, NewPeer(peer_id, 0, 10,
                    HeaderPeerReady));
                peer_id = peer_id + 1;
            }
            next_id = 1;
            PlanMissingSpans();

            peer_id = 1;
            while (peer_id <= ExplorePeers()) {
                new HeaderSyncPeerEnv((explorer = this,
                    peer_id = peer_id));
                peer_id = peer_id + 1;
            }

            announce eHeaderSyncExploreStart;
            announce eHeaderSyncModel, model;
            goto Faults;
        }
    }

    state Faults {
        on ePeerReply do (reply: (peer_id: int, valid: bool,
            stale: bool)) {

            var step: int;

            CompleteActive(reply.peer_id, reply.valid, reply.stale);
            announce eHeaderSyncModel, model;
            step = 0;
            while (step < ExploreFaultSteps()) {
                ChooseEvent();
                announce eHeaderSyncModel, model;
                step = step + 1;
            }
        }

        on ePeerQuiet do (peer_id: int) {
            quiet = quiet + 1;
            if (quiet == ExplorePeers()) {
                goto Heal;
            }
        }
    }

    state Heal {
        entry {
            var round: int;
            var peer_id: int;

            round = 0;
            while (round < ExploreHealRounds() &&
                model.committed_tip < ExploreTarget()) {

                peer_id = 1;
                while (peer_id <= ExplorePeers()) {
                    model = SetPeerState(model, peer_id,
                        HeaderPeerReady);
                    peer_id = peer_id + 1;
                }
                PlanMissingSpans();
                model = AssignNextQueuedRange(model);
                peer_id = 1;
                while (peer_id <= ExplorePeers()) {
                    model = StartPeerRange(model, peer_id);
                    CompleteActive(peer_id, true, false);
                    peer_id = peer_id + 1;
                }
                model = CommitReadyRanges(model);
                announce eHeaderSyncModel, model;
                round = round + 1;
            }

            if (model.committed_tip >= ExploreTarget()) {
                announce eHeaderSyncSynced;
            } else {
                print format("stuck: tip={0} ranges={1} peers={2}",
                    model.committed_tip, model.ranges, model.peers);
            }
            goto Done;
        }
    }

    state Done {
        ignore ePeerReply, ePeerQuiet;
    }

    // ChooseEvent applies one nondeterministically chosen event.
    fun ChooseEvent() {
        var op: int;
        var peer_id: int;
        var range_id: int;

        op = choose(7);
        peer_id = choose(ExplorePeers()) + 1;
        if (op == 0) {
            model = AssignNextQueuedRange(model);
        } else if (op == 1 || op == 2) {
            model = StartPeerRange(model, peer_id);
        } else if (op == 3) {
            if (next_id > 1) {
                range_id = choose(next_id - 1) + 1;
                model = StealRange(model, peer_id, range_id);
            }
        } else if (op == 4) {
            model = CommitReadyRanges(model);
        } else if (op == 5) {
            model = SetPeerState(model, peer_id, HeaderPeerBlocked);
        } else {
            PlanMissingSpans();
        }
    }

    // CompleteActive completes the peer's active range, with its current
    // lease epoch or a stale one.
    fun CompleteActive(peer_id: int, valid: bool, stale: bool) {
        var peer: HeaderPeer;
        var range: HeaderRange;
        var epoch: int;

        peer = PeerByID(model.peers, peer_id);
        if (peer.active_range == NoRange()) {
            return;
        }
        range = RangeByID(model.ranges, peer.active_range);
        epoch = range.lease_epoch;
        if (stale && epoch > 0) {
            epoch = epoch - 1;
        }
        model = CompleteRange(model, peer_id, range.id, epoch, valid);
    }

    // PlanMissingSpans replans every anchor span above the tip that has no
    // live range, as production does after a failure.
    fun PlanMissingSpans() {
        var span_start: int;

        span_start = model.committed_tip;
        while (span_start < ExploreTarget()) {
            if (!SpanHasLiveRange(model, span_start)) {
                model = PlanRange(model, next_id, span_start,
                    span_start + ExploreSpan());
                next_id = next_id + 1;
            }
            span_start = span_start + ExploreSpan();
        }
    }
}

test tcHeaderSyncExplore [main=HeaderSyncExplorer]:
    assert HeaderSyncSafety, HeaderSyncLiveness in
    { HeaderSyncExplorer, HeaderSyncPeerEnv };
