fun HeaderWireFixture(): HeaderSyncModel {
    var model: HeaderSyncModel;
    model = NewProductionModel(0, 100);
    model = AddOrConfirmAnchor(model, 0, 100, true);
    model = AddOrConfirmAnchor(model, 10, 110, true);
    model = AddOrConfirmAnchor(model, 20, 120, true);
    model = AddPeer(model, NewPeer(1, 0, 10, HeaderPeerReady));
    model = PlanRange(model, 1, 0, 10);
    model = PlanRange(model, 2, 10, 20);
    model = AssignNextQueuedRange(model);
    model = AssignNextQueuedRange(model);
    model = StartPeerRange(model, 1);
    return model;
}

machine TestExpiredWireReplyCannotStage {
    start state Init {
        entry (timeout_first: bool) {
            var wire: HeaderWireState;
            wire = NewHeaderWireState(HeaderWireFixture());
            wire = ArmHeaderWireRequest(wire, 1, 1, 1);
            // Explore reply-before-timeout and timeout-before-reply. An
            // expired reply cannot stage data even while its epoch is current.
            if (timeout_first) {
                wire = ExpireHeaderWireRequest(wire, 1, 1);
                wire = DeliverHeaderWireReply(wire, 1, 1, true);
                assert RangeByID(wire.scheduler.ranges, 1).range_state ==
                    RangeActive;
                assert sizeof(wire.payloads) == 0;
                wire.scheduler = AddPeer(wire.scheduler,
                    NewPeer(2, 0, 10, HeaderPeerReady));
                wire.scheduler = StealRange(wire.scheduler, 2, 1);
                wire.scheduler = StartPeerRange(wire.scheduler, 2);
                wire = ArmHeaderWireRequest(wire, 2, 1, 2);
                wire = DeliverHeaderWireReply(wire, 2, 1, true);
            } else {
                wire = DeliverHeaderWireReply(wire, 1, 1, true);
                wire = ExpireHeaderWireRequest(wire, 1, 1);
                assert PeerByID(wire.scheduler.peers, 1).peer_state ==
                    HeaderPeerReady;
            }
            AssertHeaderWirePayloads(wire);
            wire = TryHeaderWireCommit(wire, true);
            assert wire.scheduler.committed_tip == 10;
            goto Done;
        }
    }
    state Done {}
}

machine TestDuplicateWireReplyPreservesNextRequest {
    start state Init {
        entry {
            var wire: HeaderWireState;
            wire = NewHeaderWireState(HeaderWireFixture());
            wire = ArmHeaderWireRequest(wire, 1, 1, 1);
            wire = DeliverHeaderWireReply(wire, 1, 1, true);
            wire.scheduler = StartPeerRange(wire.scheduler, 1);
            wire = ArmHeaderWireRequest(wire, 1, 2, 2);
            wire = DeliverHeaderWireReply(wire, 1, 1, true);
            wire = ExpireHeaderWireRequest(wire, 1, 1);
            assert wire.requests[0].live && wire.requests[0].range_id == 2,
                "old callbacks cannot consume replacement work";
            wire = DeliverHeaderWireReply(wire, 1, 2, true);
            AssertHeaderWirePayloads(wire);
            wire = TryHeaderWireCommit(wire, true);
            assert wire.scheduler.committed_tip == 20;
            goto Done;
        }
    }
    state Done {}
}

machine TestWriteRecoveryNeedsNoNetworkReply {
    start state Init {
        entry {
            var wire: HeaderWireState;
            var generation: int;
            wire = NewHeaderWireState(HeaderWireFixture());
            wire = ArmHeaderWireRequest(wire, 1, 1, 1);
            wire = DeliverHeaderWireReply(wire, 1, 1, true);
            wire.scheduler = StartPeerRange(wire.scheduler, 1);
            wire = ArmHeaderWireRequest(wire, 1, 2, 2);
            wire = DeliverHeaderWireReply(wire, 1, 2, true);
            wire = TryHeaderWireCommit(wire, false);
            generation = wire.retry_generation;
            assert wire.validation_tip == wire.scheduler.committed_tip,
                "failed writes cannot publish a speculative validation tip";
            wire = TryHeaderWireCommit(wire, false);
            assert wire.retry_pending && wire.retry_generation == generation,
                "repeated failures coalesce into one retry wakeup";
            wire = DeliverHeaderWireReply(wire, 1, 2, true);
            AssertHeaderWirePayloads(wire);
            assert wire.scheduler.committed_tip == 0;
            wire = RetryHeaderWireCommit(wire, generation, true);
            assert wire.scheduler.committed_tip == 20 &&
                sizeof(wire.scheduler.commit_log) == 2;
            assert sizeof(wire.payloads) == 0 && !wire.retry_pending;
            wire = RetryHeaderWireCommit(wire, generation, true);
            assert sizeof(wire.scheduler.commit_log) == 2,
                "old retry delivery cannot commit work twice";
            goto Done;
        }
    }
    state Done {}
}

machine TestCheckpointRetryResumesPlanning {
    start state Init {
        entry {
            var model: HeaderSyncModel;
            var wire: HeaderWireState;
            var next: HeaderRange;
            model = NewProductionModel(0, 100);
            model = AddOrConfirmAnchor(model, 0, 100, true);
            model = AddOrConfirmAnchor(model, 10, 110, true);
            model = AddOrConfirmAnchor(model, 20, 120, true);
            model = AddPeer(model, NewPeer(1, 0, 10, HeaderPeerReady));
            // Only the first checkpoint span exists initially. Preplanning
            // both spans would hide a missing continuation after retry.
            model = PlanRange(model, 1, 0, 10);
            model = AssignNextQueuedRange(model);
            model = StartPeerRange(model, 1);
            wire = NewHeaderWireState(model);
            wire = ArmHeaderWireRequest(wire, 1, 1, 1);
            wire = DeliverHeaderWireReply(wire, 1, 1, true);
            wire = TryHeaderWireCommit(wire, false);
            wire = RetryHeaderWireCommit(wire, wire.retry_generation, true);
            assert wire.scheduler.committed_tip == 10;
            next = RangeByID(wire.scheduler.ranges, 2);
            assert next.range_state == RangeOwned && next.start_height == 10,
                "retry recovery must schedule beyond the committed checkpoint";
            wire.scheduler = StartPeerRange(wire.scheduler, 1);
            wire = ArmHeaderWireRequest(wire, 1, 2, 2);
            wire = DeliverHeaderWireReply(wire, 1, 2, true);
            wire = TryHeaderWireCommit(wire, true);
            assert wire.scheduler.committed_tip == 20;
            AssertHeaderWirePayloads(wire);
            goto Done;
        }
    }
    state Done {}
}
