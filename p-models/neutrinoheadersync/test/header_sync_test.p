machine TestPlansOnlyConfirmedAnchors {
    start state Init {
        entry {
            var model: HeaderSyncModel;

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 100, 200, false);

            model = PlanRange(model, 1, 0, 100);
            assert sizeof(model.ranges) == 0,
                "unconfirmed discovered anchor must not create range work";

            model.anchors = AddOrConfirmAnchor(model.anchors, 100, 200, false);
            model = PlanRange(model, 1, 0, 100);
            assert sizeof(model.ranges) == 1,
                "confirmed discovered anchor should create range work";

            goto Done;
        }
    }

    state Done {}
}

machine TestOversizedSpanRequiresIntermediateAnchor {
    start state Init {
        entry {
            var model: HeaderSyncModel;

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(
                model.anchors, MaxRangeHeaders() + 1, 200, true
            );

            model = PlanRange(model, 1, 0, MaxRangeHeaders() + 1);
            assert sizeof(model.ranges) == 0,
                "oversized checkpoint span needs a confirmed intermediate anchor";

            model.anchors = AddOrConfirmAnchor(
                model.anchors, MaxRangeHeaders(), 199, false
            );
            model.anchors = AddOrConfirmAnchor(
                model.anchors, MaxRangeHeaders(), 199, false
            );

            model = PlanRange(model, 1, 0, MaxRangeHeaders());
            model = PlanRange(
                model, 2, MaxRangeHeaders(), MaxRangeHeaders() + 1
            );
            assert sizeof(model.ranges) == 2,
                "confirmed intermediate anchor should split the large span";

            goto Done;
        }
    }

    state Done {}
}

machine TestAnchorDiscoveryPipelinesAheadOfActiveRange {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var frontier: HeaderAnchor;
            var range: HeaderRange;

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(
                model.anchors, MaxRangeHeaders(), 200, false
            );
            model.anchors = AddOrConfirmAnchor(
                model.anchors, MaxRangeHeaders(), 200, false
            );
            model.anchors = AddOrConfirmAnchor(
                model.anchors, MaxRangeHeaders() * 3, 400, true
            );
            model = AddPeer(model, NewPeer(1, 0, 10, HeaderPeerReady));
            model = PlanRange(model, 1, 0, MaxRangeHeaders());
            model = AssignNextQueuedRange(model);
            model = StartPeerRange(model, 1);

            range = RangeByID(model.ranges, 1);
            assert range.range_state == RangeActive,
                "earlier range should still be active";

            frontier = NextAnchorDiscoveryStart(
                model, model.committed_tip, model.committed_hash,
                MaxRangeHeaders() * 3
            );
            assert frontier.height == MaxRangeHeaders(),
                "discovery should advance from furthest confirmed anchor";
            assert ShouldDiscoverAnchorAhead(
                model, model.committed_tip, model.committed_hash,
                MaxRangeHeaders() * 3
            ),
                "active range must not block ahead-of-tip discovery";

            goto Done;
        }
    }

    state Done {}
}

machine TestConfirmedDiscoveryResponseStagesRange {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var staged: HeaderRange;
            var ready: seq[int];

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(
                model.anchors, MaxRangeHeaders(), 200, false
            );
            model.anchors = AddOrConfirmAnchor(
                model.anchors, MaxRangeHeaders(), 200, false
            );

            // Discovery has already fetched the exact headers for this span.
            // The production implementation should stage them immediately
            // instead of scheduling the same range for another peer request.
            model = StageDiscoveredRange(
                model, 1, 7, 0, MaxRangeHeaders()
            );

            staged = RangeByID(model.ranges, 1);
            assert staged.range_state == RangeStaged,
                "confirmed discovery response should become staged work";
            assert staged.owner_peer == 7,
                "staged discovery range should retain source peer";
            assert staged.lease_epoch == 1,
                "staged discovery range should have a concrete lease";

            ready = ReadyRanges(model);
            assert sizeof(ready) == 1,
                "staged discovery range should be ready to commit";
            assert ready[0] == 1,
                "ready staged discovery range should preserve range id";

            model = CommitReadyRanges(model);
            assert model.committed_tip == MaxRangeHeaders(),
                "committing staged discovery range should advance tip";

            goto Done;
        }
    }

    state Done {}
}

machine TestPostCheckpointFrontierDiscoveryStagesRange {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var frontier: HeaderAnchor;
            var stop_height: int;
            var staged: HeaderRange;

            model = NewModel(100000, 500);
            model.anchors = AddOrConfirmAnchor(
                model.anchors, 100000, 500, true
            );

            frontier = NextAnchorDiscoveryStart(
                model, model.committed_tip, model.committed_hash,
                110000
            );
            stop_height = NextFrontierDiscoveryStop(
                frontier.height, 110000
            );
            assert stop_height == 102000,
                "post-checkpoint discovery should use one bounded window";

            // The first peer response discovers the post-checkpoint boundary.
            // The second matching response confirms it, after which the exact
            // headers can be staged even though the boundary was not a static
            // chaincfg checkpoint.
            model.anchors = AddOrConfirmAnchor(
                model.anchors, stop_height, 700, false
            );
            model.anchors = AddOrConfirmAnchor(
                model.anchors, stop_height, 700, false
            );
            model = StageDiscoveredRange(
                model, 1, 9, frontier.height, stop_height
            );

            staged = RangeByID(model.ranges, 1);
            assert staged.range_state == RangeStaged,
                "confirmed frontier boundary should stage downloaded headers";

            model = CommitReadyRanges(model);
            assert model.committed_tip == stop_height,
                "post-checkpoint staged range should advance committed tip";

            goto Done;
        }
    }

    state Done {}
}

machine TestFrontierLookaheadStagesBeforeCommit {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var frontier: HeaderAnchor;
            var first_stop: int;
            var second_stop: int;
            var ready: seq[int];

            model = NewModel(100000, 500);
            model.anchors = AddOrConfirmAnchor(
                model.anchors, 100000, 500, true
            );

            frontier = NextAnchorDiscoveryStart(
                model, model.committed_tip, model.committed_hash,
                110000
            );
            first_stop = NextFrontierDiscoveryStop(
                frontier.height, 110000
            );

            // The first frontier response gives us both data to stage and the
            // boundary hash needed to ask for the next frontier window. The
            // production manager should not wait for disk commit before sending
            // that next getheaders; doing so would serialize the entire
            // post-checkpoint lane behind local validation/write work.
            model.anchors = AddOrConfirmAnchor(
                model.anchors, first_stop, 700, false
            );
            model.anchors = AddOrConfirmAnchor(
                model.anchors, first_stop, 700, false
            );
            model = StageDiscoveredRange(
                model, 1, 9, frontier.height, first_stop
            );

            second_stop = NextFrontierDiscoveryStop(first_stop, 110000);
            assert second_stop == 104000,
                "lookahead should move to the next bounded frontier window";

            // This second staged range models the next peer response arriving
            // before the first staged range has been committed. Ordered commit
            // must still be the only thing that advances the visible tip.
            model.anchors = AddOrConfirmAnchor(
                model.anchors, second_stop, 900, false
            );
            model.anchors = AddOrConfirmAnchor(
                model.anchors, second_stop, 900, false
            );
            model = StageDiscoveredRange(
                model, 2, 10, first_stop, second_stop
            );
            assert model.committed_tip == 100000,
                "lookahead staging must not advance visible chain tip";

            ready = ReadyRanges(model);
            assert sizeof(ready) == 2,
                "contiguous lookahead ranges should be ready together";
            assert ready[0] == 1 && ready[1] == 2,
                "lookahead ranges should preserve ordered commit sequence";

            model = CommitReadyRanges(model);
            assert model.committed_tip == second_stop,
                "ordered commit should drain pipelined frontier ranges";

            goto Done;
        }
    }

    state Done {}
}

machine TestOutOfOrderCompletionStagesUntilGapFilled {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var first: HeaderRange;
            var second: HeaderRange;

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 10, 110, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 20, 120, true);
            model = AddPeer(model, NewPeer(1, 0, 10, HeaderPeerReady));
            model = AddPeer(model, NewPeer(2, 0, 10, HeaderPeerReady));
            model = PlanRange(model, 1, 0, 10);
            model = PlanRange(model, 2, 10, 20);
            model = AssignNextQueuedRange(model);
            model = AssignNextQueuedRange(model);
            model = StartPeerRange(model, 1);
            model = StartPeerRange(model, 2);

            first = RangeByID(model.ranges, 1);
            second = RangeByID(model.ranges, 2);
            model = CompleteRange(model, 2, 2, second.lease_epoch, true);
            model = CommitReadyRanges(model);
            assert model.committed_tip == 0,
                "later completed range must remain staged behind gap";
            assert sizeof(model.commit_log) == 0,
                "out-of-order completion must not commit";

            model = CompleteRange(model, 1, 1, first.lease_epoch, true);
            model = CommitReadyRanges(model);
            assert model.committed_tip == 20,
                "commit should drain all now-contiguous staged ranges";
            assert sizeof(model.commit_log) == 2,
                "both staged ranges should commit in order";
            assert model.commit_log[0] == 1,
                "first committed range should fill the gap";
            assert model.commit_log[1] == 2,
                "second committed range should follow";

            goto Done;
        }
    }

    state Done {}
}

machine TestExternalTipAdvancePlansGapBeforeStagedFrontier {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var gap: HeaderRange;
            var staged: HeaderRange;
            var ready: seq[int];

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 20, 120, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 30, 130, true);

            // This is the failure mode seen in the live sqlite testnet3 run:
            // later frontier discovery responses were already staged, but a
            // short serial/fallback write had advanced the durable tip to a
            // boundary the FSM had not yet made visible. Reconciliation must
            // add that boundary before the manager keeps discovering farther
            // ahead, otherwise the staged range after the gap can never commit.
            model = StageDiscoveredRange(model, 1, 9, 20, 30);
            model = AdvanceCommittedTip(model, 10, 110);
            assert model.committed_tip == 10,
                "external durable tip should become manager-visible";

            model = PlanRange(model, 2, 10, 20);
            gap = RangeByID(model.ranges, 2);
            assert gap.range_state == RangeQueued,
                "known missing frontier span should become fetchable work";

            ready = ReadyRanges(model);
            assert sizeof(ready) == 0,
                "later staged frontier range must wait for the planned gap";

            model = AddPeer(model, NewPeer(1, 0, 10, HeaderPeerReady));
            model = AssignNextQueuedRange(model);
            model = StartPeerRange(model, 1);
            gap = RangeByID(model.ranges, 2);
            model = CompleteRange(model, 1, 2, gap.lease_epoch, true);
            model = CommitReadyRanges(model);

            staged = RangeByID(model.ranges, 1);
            assert staged.range_state == RangeCommitted,
                "committing the gap should drain the already staged range";
            assert model.committed_tip == 30,
                "visible tip should advance through all contiguous work";

            goto Done;
        }
    }

    state Done {}
}

machine TestReadyRangesDoesNotAdvanceCommittedTip {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var range: HeaderRange;
            var ready: seq[int];

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 10, 110, true);
            model = AddPeer(model, NewPeer(1, 0, 10, HeaderPeerReady));
            model = PlanRange(model, 1, 0, 10);
            model = AssignNextQueuedRange(model);
            model = StartPeerRange(model, 1);
            range = RangeByID(model.ranges, 1);
            model = CompleteRange(model, 1, 1, range.lease_epoch, true);

            ready = ReadyRanges(model);
            assert sizeof(ready) == 1,
                "ready preflight should expose the contiguous staged range";
            assert ready[0] == 1,
                "ready preflight should report the staged range id";
            assert model.committed_tip == 0,
                "ready preflight must not advance visible committed tip";
            assert sizeof(model.commit_log) == 0,
                "ready preflight must not append commit log entries";

            model = CommitReadyRanges(model);
            assert model.committed_tip == 10,
                "explicit commit should advance visible committed tip";

            goto Done;
        }
    }

    state Done {}
}

machine TestCommitPrunesObsoleteAnchorRequests {
    start state Init {
        entry {
            var model: HeaderSyncModel;

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 10, 110, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 20, 120, true);

            // Two peers are hedging discovery. The first request becomes
            // obsolete when the staged range commits through height 10. The
            // second is still useful because it can produce the next boundary.
            model = AddActiveAnchorRequest(model, 1, 0, 10);
            model = AddActiveAnchorRequest(model, 2, 10, 20);
            model = StageDiscoveredRange(model, 1, 1, 0, 10);

            model = CommitReadyRangesAndPruneObsoleteAnchors(model);

            assert model.committed_tip == 10,
                "commit should still advance through staged range";
            assert sizeof(model.active_anchor_requests) == 1,
                "obsolete hedged anchor request should be pruned";
            assert model.active_anchor_requests[0].stop_height == 20,
                "future anchor request must remain active";

            goto Done;
        }
    }

    state Done {}
}

machine TestConflictingDiscoveredAnchorRejected {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var anchor: HeaderAnchor;

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 100, 200, false);
            model.anchors = AddOrConfirmAnchor(model.anchors, 100, 201, false);

            anchor = AnchorByHeight(model.anchors, 100);
            assert anchor.rejected,
                "conflicting discovered anchors should reject the boundary";

            model.anchors = AddOrConfirmAnchor(model.anchors, 100, 200, false);
            model = PlanRange(model, 1, 0, 100);
            assert sizeof(model.ranges) == 0,
                "rejected anchor must not create range work";

            model.anchors = AddOrConfirmAnchor(model.anchors, 100, 200, true);
            model = PlanRange(model, 1, 0, 100);
            assert sizeof(model.ranges) == 1,
                "trusted checkpoint should recover a rejected boundary";

            goto Done;
        }
    }

    state Done {}
}

machine TestStaleCompletionCannotOverwriteLease {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var old_epoch: int;
            var current: HeaderRange;

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 10, 110, true);
            model = AddPeer(model, NewPeer(1, 0, 10, HeaderPeerReady));
            model = AddPeer(model, NewPeer(2, 0, 10, HeaderPeerReady));
            model = PlanRange(model, 1, 0, 10);
            model = AssignNextQueuedRange(model);
            model = StartPeerRange(model, 1);

            old_epoch = RangeByID(model.ranges, 1).lease_epoch;
            model = StealRange(model, 2, 1);

            // The old owner reports a failure after the lease was stolen.
            // That response is stale and must not poison the current range.
            model = CompleteRange(model, 1, 1, old_epoch, false);
            current = RangeByID(model.ranges, 1);
            assert current.owner_peer == 2,
                "stale completion must not change current owner";
            assert current.range_state == RangeOwned,
                "stale completion must not fail or stage current range";

            model = StartPeerRange(model, 2);
            current = RangeByID(model.ranges, 1);
            model = CompleteRange(model, 2, 1, current.lease_epoch, true);
            model = CommitReadyRanges(model);
            assert model.committed_tip == 10,
                "current lease completion should still commit";

            goto Done;
        }
    }

    state Done {}
}

machine TestInvalidCompletionDoesNotCommit {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var range: HeaderRange;

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 10, 110, true);
            model = AddPeer(model, NewPeer(1, 0, 10, HeaderPeerReady));
            model = PlanRange(model, 1, 0, 10);
            model = AssignNextQueuedRange(model);
            model = StartPeerRange(model, 1);
            range = RangeByID(model.ranges, 1);

            model = CompleteRange(model, 1, 1, range.lease_epoch, false);
            model = CommitReadyRanges(model);
            assert model.committed_tip == 0,
                "invalid range must not advance committed tip";
            assert RangeByID(model.ranges, 1).range_state == RangeFailed,
                "invalid completion should fail the range";

            goto Done;
        }
    }

    state Done {}
}

machine TestCompletionPreservesPeerPenalty {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var range: HeaderRange;
            var peer: HeaderPeer;

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 10, 110, true);
            model = AddPeer(model, NewPeer(1, 0, 10, HeaderPeerReady));
            model = PlanRange(model, 1, 0, 10);
            model = AssignNextQueuedRange(model);
            model = StartPeerRange(model, 1);
            range = RangeByID(model.ranges, 1);

            model = SetPeerState(model, 1, HeaderPeerQuarantined);
            model = CompleteRange(model, 1, 1, range.lease_epoch, false);
            peer = PeerByID(model.peers, 1);

            assert peer.peer_state == HeaderPeerQuarantined,
                "completion must not clear an explicit peer penalty";
            assert peer.active_range == NoRange(),
                "completion should still clear the active range";

            goto Done;
        }
    }

    state Done {}
}

machine TestRecoveredPeerCanOwnRangeAgain {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var range: HeaderRange;

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 10, 110, true);
            model = AddPeer(model, NewPeer(1, 0, 10, HeaderPeerReady));
            model = PlanRange(model, 1, 0, 10);

            model = SetPeerState(model, 1, HeaderPeerBlocked);
            model = AssignNextQueuedRange(model);
            range = RangeByID(model.ranges, 1);
            assert range.range_state == RangeQueued,
                "blocked peer must not receive new range ownership";

            model = SetPeerState(model, 1, HeaderPeerReady);
            model = AssignNextQueuedRange(model);
            range = RangeByID(model.ranges, 1);
            assert range.owner_peer == 1,
                "recovered peer should become schedulable again";
            assert range.range_state == RangeOwned,
                "recovered peer should own queued work";

            goto Done;
        }
    }

    state Done {}
}

machine TestActiveRangeCanBeStolen {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var before: HeaderRange;
            var after: HeaderRange;
            var donor: HeaderPeer;
            var thief: HeaderPeer;

            model = NewModel(0, 100);
            model.anchors = AddOrConfirmAnchor(model.anchors, 0, 100, true);
            model.anchors = AddOrConfirmAnchor(model.anchors, 10, 110, true);
            model = AddPeer(model, NewPeer(1, 0, 10, HeaderPeerReady));
            model = AddPeer(model, NewPeer(2, 0, 10, HeaderPeerReady));
            model = PlanRange(model, 1, 0, 10);
            model = AssignNextQueuedRange(model);
            model = StartPeerRange(model, 1);
            before = RangeByID(model.ranges, 1);

            model = StealRange(model, 2, 1);
            after = RangeByID(model.ranges, 1);
            donor = PeerByID(model.peers, 1);
            thief = PeerByID(model.peers, 2);

            assert after.owner_peer == 2,
                "stolen range should move to thief";
            assert after.lease_epoch == before.lease_epoch + 1,
                "steal should bump lease epoch";
            assert donor.active_range == NoRange(),
                "donor should no longer own active range";
            assert sizeof(thief.local_ranges) == 1,
                "thief should queue stolen range";

            goto Done;
        }
    }

    state Done {}
}

machine HeaderSyncTestDriver {
    start state RunTests {
        entry {
            new TestPlansOnlyConfirmedAnchors();
            new TestOversizedSpanRequiresIntermediateAnchor();
            new TestAnchorDiscoveryPipelinesAheadOfActiveRange();
            new TestConfirmedDiscoveryResponseStagesRange();
            new TestPostCheckpointFrontierDiscoveryStagesRange();
            new TestFrontierLookaheadStagesBeforeCommit();
            new TestOutOfOrderCompletionStagesUntilGapFilled();
            new TestReadyRangesDoesNotAdvanceCommittedTip();
            new TestCommitPrunesObsoleteAnchorRequests();
            new TestConflictingDiscoveredAnchorRejected();
            new TestStaleCompletionCannotOverwriteLease();
            new TestInvalidCompletionDoesNotCommit();
            new TestCompletionPreservesPeerPenalty();
            new TestRecoveredPeerCanOwnRangeAgain();
            new TestActiveRangeCanBeStolen();

            goto Done;
        }
    }

    state Done {}
}

test tcHeaderSync [main=HeaderSyncTestDriver]:
  { TestPlansOnlyConfirmedAnchors,
    TestOversizedSpanRequiresIntermediateAnchor,
    TestAnchorDiscoveryPipelinesAheadOfActiveRange,
    TestConfirmedDiscoveryResponseStagesRange,
    TestPostCheckpointFrontierDiscoveryStagesRange,
    TestFrontierLookaheadStagesBeforeCommit,
    TestOutOfOrderCompletionStagesUntilGapFilled,
    TestReadyRangesDoesNotAdvanceCommittedTip,
    TestCommitPrunesObsoleteAnchorRequests,
    TestConflictingDiscoveredAnchorRejected,
    TestStaleCompletionCannotOverwriteLease,
    TestInvalidCompletionDoesNotCommit,
    TestCompletionPreservesPeerPenalty,
    TestRecoveredPeerCanOwnRangeAgain,
    TestActiveRangeCanBeStolen,
    HeaderSyncTestDriver };
