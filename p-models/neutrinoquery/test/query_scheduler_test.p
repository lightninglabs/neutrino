// query_scheduler_test.p - Neutrino query scheduler model checks.
//
// Each test machine is a small executable story. It constructs a scheduler
// state, applies one model transition, and asserts the property we want the Go
// implementation to preserve. Keeping these as short stories makes the model
// useful to contributors who know neutrino but are new to P.

machine TestSchedulerSkipsBlockedRankedPeer {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var tasks: seq[QueryTask];
            var empty: seq[int];
            var result: DispatchResult;

            peers += (sizeof(peers), NewPeer(
                1, 0, 20, PeerBlocked, empty
            ));
            peers += (sizeof(peers), NewPeer(
                2, 10, 50, PeerReady, empty
            ));
            tasks += (sizeof(tasks), NewTask(100, false));

            // Peer 1 has the best rank but is blocked. The scheduler must not
            // wait on it while peer 2 can accept the work immediately.
            result = DispatchNext(peers, tasks, MinQueryTimeoutMs());

            assert result.peer_id == 2,
                "blocked best-ranked peer must not stall capable peer";
            assert result.task_id == 100,
                "live task must be dispatched to capable peer";

            goto Done;
        }
    }

    state Done {}
}

machine TestSchedulerSkipsBusyRankedPeer {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var tasks: seq[QueryTask];
            var empty: seq[int];
            var result: DispatchResult;

            peers += (sizeof(peers), NewPeer(
                1, 0, 20, PeerBusy, empty
            ));
            peers += (sizeof(peers), NewPeer(
                2, 10, 50, PeerReady, empty
            ));
            tasks += (sizeof(tasks), NewTask(100, false));

            // Busy means the peer already owns a query. Even if it has the
            // best rank, assigning another task would violate the one-active
            // query invariant used by the production worker today.
            result = DispatchNext(peers, tasks, MinQueryTimeoutMs());

            assert result.peer_id == 2,
                "busy best-ranked peer must not receive additional work";
            assert result.task_id == 100,
                "live task must be dispatched to capable peer";

            goto Done;
        }
    }

    state Done {}
}

machine TestSchedulerSkipsQuarantinedRankedPeer {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var tasks: seq[QueryTask];
            var empty: seq[int];
            var result: DispatchResult;

            peers += (sizeof(peers), NewPeer(
                1, 0, 20, PeerQuarantined, empty
            ));
            peers += (sizeof(peers), NewPeer(
                2, 10, 50, PeerReady, empty
            ));
            tasks += (sizeof(tasks), NewTask(100, false));

            // Quarantine is stronger than a transient blocked mailbox: the
            // peer has repeatedly failed to respond and must not receive more
            // work until the external ban/quarantine path lets it recover.
            result = DispatchNext(peers, tasks, MinQueryTimeoutMs());

            assert result.peer_id == 2,
                "quarantined best-ranked peer must not receive work";
            assert result.task_id == 100,
                "live task must be dispatched to recoverable peer";

            goto Done;
        }
    }

    state Done {}
}

machine TestSchedulerUsesRTTAsRankTieBreaker {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var tasks: seq[QueryTask];
            var empty: seq[int];
            var result: DispatchResult;

            peers += (sizeof(peers), NewPeer(
                1, 4, 800, PeerReady, empty
            ));
            peers += (sizeof(peers), NewPeer(
                2, 4, 50, PeerReady, empty
            ));
            tasks += (sizeof(tasks), NewTask(100, false));

            // Ranking history remains the primary signal, but when two peers
            // have the same score the lower RTT peer should get the next query.
            // This gives the production scheduler an immediate latency signal
            // without discarding punishment/reward history.
            result = DispatchNext(peers, tasks, MinQueryTimeoutMs());

            assert result.peer_id == 2,
                "equal-ranked lower RTT peer should be preferred";
            assert result.task_id == 100,
                "live task must be dispatched to selected peer";

            goto Done;
        }
    }

    state Done {}
}

machine TestSchedulerDropsCanceledQueuedWork {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var tasks: seq[QueryTask];
            var empty: seq[int];
            var result: DispatchResult;

            peers += (sizeof(peers), NewPeer(
                1, 0, 20, PeerReady, empty
            ));
            tasks += (sizeof(tasks), NewTask(100, true));

            // A canceled queued task should be consumed by scheduler cleanup,
            // not by a worker round trip.
            result = DispatchNext(peers, tasks, MinQueryTimeoutMs());

            assert result.task_id == NoTask(),
                "canceled queued task must not be dispatched";
            assert result.peer_id == NoPeer(),
                "canceled queued task must not consume peer capacity";

            goto Done;
        }
    }

    state Done {}
}

machine TestSchedulerRTTTimeoutFloor {
    start state Init {
        entry {
            var empty: seq[int];
            var peer: PeerModel;

            // Poor links need a timeout floor based on observed RTT. A 750ms
            // RTT is too close to the old 2s fixed timeout once jitter and
            // multi-message responses are included, so the model expects the
            // multiplier rule to raise it.
            peer = NewPeer(1, 0, 750, PeerReady, empty);
            assert QueryTimeoutForPeer(MinQueryTimeoutMs(), peer) == 6000,
                "750ms RTT with multiplier 8 should produce 6s timeout";

            peer = NewPeer(1, 0, 9000, PeerReady, empty);
            assert QueryTimeoutForPeer(MinQueryTimeoutMs(), peer) ==
                MaxQueryTimeoutMs(),
                "RTT-derived timeout must be capped";

            goto Done;
        }
    }

    state Done {}
}

machine TestWorkStealingMovesUnclaimedWorkToIdlePeer {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var donor_work: seq[int];
            var empty: seq[int];
            var donor: PeerModel;
            var thief: PeerModel;

            donor_work += (sizeof(donor_work), 10);
            donor_work += (sizeof(donor_work), 11);
            donor_work += (sizeof(donor_work), 12);

            peers += (sizeof(peers), NewPeer(
                1, 0, 20, PeerReady, donor_work
            ));
            peers += (sizeof(peers), NewPeer(
                2, 10, 10, PeerReady, empty
            ));

            // Peer 2 is idle and peer 1 has spare local work. Stealing fills
            // the thief up to capacity from the oldest donor tasks, while
            // leaving a ready donor with one local task so it is not starved.
            peers = StealOne(peers, 2);
            donor = PeerByID(peers, 1);
            thief = PeerByID(peers, 2);

            assert sizeof(donor.local_work) == 1,
                "work stealing should leave ready donor one local task";
            assert sizeof(thief.local_work) == 2,
                "idle thief should receive a bounded stolen batch";
            assert thief.local_work[0] == 10,
                "stealing should preserve first donor task order";
            assert thief.local_work[1] == 11,
                "stealing should preserve second donor task order";

            goto Done;
        }
    }

    state Done {}
}

machine TestWorkStealingLeavesBusyDonorQueuedWork {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var donor_work: seq[int];
            var empty: seq[int];
            var donor: PeerModel;
            var thief: PeerModel;

            donor_work += (sizeof(donor_work), 10);
            donor_work += (sizeof(donor_work), 11);
            donor_work += (sizeof(donor_work), 12);

            peers += (sizeof(peers), NewPeer(
                1, 0, 20, PeerBusy, donor_work
            ));
            peers += (sizeof(peers), NewPeer(
                2, 10, 10, PeerReady, empty
            ));

            // A busy donor may contribute surplus queued work, but it should
            // keep one local task. This prevents a peer that just stole a
            // small queue and started work from being drained again before it
            // has a chance to make progress.
            peers = StealOne(peers, 2);
            donor = PeerByID(peers, 1);
            thief = PeerByID(peers, 2);

            assert sizeof(donor.local_work) == 1,
                "busy donor should retain one queued local task";
            assert donor.local_work[0] == 12,
                "busy donor should retain newest local task";
            assert sizeof(thief.local_work) == 2,
                "idle thief should receive busy donor surplus";
            assert thief.local_work[0] == 10,
                "stealing should preserve busy donor first task order";
            assert thief.local_work[1] == 11,
                "stealing should preserve busy donor second task order";

            goto Done;
        }
    }

    state Done {}
}

machine TestWorkStealingIgnoresBusySingletonDonor {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var busy_work: seq[int];
            var ready_work: seq[int];
            var empty: seq[int];
            var busy: PeerModel;
            var ready: PeerModel;
            var thief: PeerModel;

            busy_work += (sizeof(busy_work), 10);
            ready_work += (sizeof(ready_work), 20);
            ready_work += (sizeof(ready_work), 21);

            peers += (sizeof(peers), NewPeer(
                1, 100, 20, PeerBusy, busy_work
            ));
            peers += (sizeof(peers), NewPeer(
                2, 0, 20, PeerReady, ready_work
            ));
            peers += (sizeof(peers), NewPeer(
                3, 10, 10, PeerReady, empty
            ));

            // A busy singleton is not stealable. The active request should
            // get a chance to finish or fail before the final queued task is
            // treated as stranded, so the thief takes ready surplus instead.
            peers = StealOne(peers, 3);
            busy = PeerByID(peers, 1);
            ready = PeerByID(peers, 2);
            thief = PeerByID(peers, 3);

            assert sizeof(busy.local_work) == 1,
                "busy singleton should be left alone when surplus exists";
            assert busy.local_work[0] == 10,
                "busy singleton task should be preserved";
            assert sizeof(ready.local_work) == 1,
                "ready surplus donor should retain one local task";
            assert ready.local_work[0] == 21,
                "ready donor should retain newest local task";
            assert sizeof(thief.local_work) == 1,
                "thief should receive one surplus task";
            assert thief.local_work[0] == 20,
                "thief should receive oldest surplus task";

            goto Done;
        }
    }

    state Done {}
}

machine TestAssignLocalTaskBalancesOwnedWork {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var loaded_work: seq[int];
            var empty: seq[int];
            var loaded: PeerModel;
            var selected: PeerModel;

            loaded_work += (sizeof(loaded_work), 10);
            loaded_work += (sizeof(loaded_work), 11);

            peers += (sizeof(peers), NewPeer(
                1, 0, 20, PeerReady, loaded_work
            ));
            peers += (sizeof(peers), NewPeer(
                2, 10, 50, PeerReady, empty
            ));

            // Ownership is not the same thing as active dispatch. The
            // lower-ranked peer already owns two unstarted tasks, so the next
            // queued task should be assigned to the empty peer instead of
            // deepening the backlog on peer 1.
            peers = AssignLocalTask(peers, NewTask(12, false));
            loaded = PeerByID(peers, 1);
            selected = PeerByID(peers, 2);

            assert sizeof(loaded.local_work) == 2,
                "owned-work assignment must not overload the donor";
            assert sizeof(selected.local_work) == 1,
                "empty peer should own the next queued task";
            assert selected.local_work[0] == 12,
                "owned-work assignment should preserve task identity";

            goto Done;
        }
    }

    state Done {}
}

machine TestAssignLocalTaskBuildsBoundedPeerQueue {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var local_work: seq[int];
            var peer: PeerModel;

            local_work += (sizeof(local_work), 10);
            peers += (sizeof(peers), NewPeer(
                1, 0, 20, PeerReady, local_work
            ));

            // Local ownership should be deeper than one item so a good peer
            // can stay fed without round-tripping through the global queue for
            // every task. The cap still keeps ownership bounded and visible.
            peers = AssignLocalTask(peers, NewTask(11, false));
            peer = PeerByID(peers, 1);

            assert sizeof(peer.local_work) == 2,
                "owned-work assignment should build a bounded local queue";
            assert peer.local_work[0] == 10,
                "existing local work should be preserved";
            assert peer.local_work[1] == 11,
                "new local work should be appended in FIFO order";

            goto Done;
        }
    }

    state Done {}
}

machine TestAssignLocalTaskDoesNotExceedPeerCapacity {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var local_work: seq[int];
            var peer: PeerModel;

            local_work += (sizeof(local_work), 10);
            local_work += (sizeof(local_work), 11);
            local_work += (sizeof(local_work), 12);
            local_work += (sizeof(local_work), 13);
            peers += (sizeof(peers), NewPeer(
                1, 0, 20, PeerReady, local_work
            ));

            // The bounded queue is what prevents a fast-looking peer from
            // hiding the whole batch behind its actor. Once capacity is full,
            // the manager must leave later tasks globally visible.
            peers = AssignLocalTask(peers, NewTask(14, false));
            peer = PeerByID(peers, 1);

            assert sizeof(peer.local_work) == MaxPeerOutstandingWork(),
                "owned-work assignment must respect peer capacity";
            assert peer.local_work[0] == 10,
                "first queued task should be preserved at capacity";
            assert peer.local_work[3] == 13,
                "last queued task should be preserved at capacity";

            goto Done;
        }
    }

    state Done {}
}

machine TestWorkStealingDoesNotStealSingleton {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var donor_work: seq[int];
            var empty: seq[int];
            var donor: PeerModel;
            var thief: PeerModel;

            donor_work += (sizeof(donor_work), 10);

            peers += (sizeof(peers), NewPeer(
                1, 0, 20, PeerReady, donor_work
            ));
            peers += (sizeof(peers), NewPeer(
                2, 10, 10, PeerReady, empty
            ));

            // A donor with one task is not overloaded. The thief remains idle
            // instead of starving the donor.
            peers = StealOne(peers, 2);
            donor = PeerByID(peers, 1);
            thief = PeerByID(peers, 2);

            assert sizeof(donor.local_work) == 1,
                "stealing must not drain donor below one local task";
            assert sizeof(thief.local_work) == 0,
                "idle thief must remain idle when no donor has spare work";

            goto Done;
        }
    }

    state Done {}
}

machine TestWorkStealingDoesNotStealBusySingleton {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var donor_work: seq[int];
            var empty: seq[int];
            var donor: PeerModel;
            var thief: PeerModel;

            donor_work += (sizeof(donor_work), 10);

            peers += (sizeof(peers), NewPeer(
                1, 0, 20, PeerBusy, donor_work
            ));
            peers += (sizeof(peers), NewPeer(
                2, 10, 10, PeerReady, empty
            ));

            // A busy donor with one queued task is not yet stranded. Its
            // active request should finish or fail before the final local task
            // is made stealable.
            peers = StealOne(peers, 2);
            donor = PeerByID(peers, 1);
            thief = PeerByID(peers, 2);

            assert sizeof(donor.local_work) == 1,
                "busy singleton should keep its final queued task";
            assert sizeof(thief.local_work) == 0,
                "idle thief must not steal from busy singleton";

            goto Done;
        }
    }

    state Done {}
}

machine TestWorkStealingCanDrainBlockedDonor {
    start state Init {
        entry {
            var peers: seq[PeerModel];
            var blocked_work: seq[int];
            var empty: seq[int];
            var blocked: PeerModel;
            var thief: PeerModel;

            blocked_work += (sizeof(blocked_work), 10);

            peers += (sizeof(peers), NewPeer(
                1, 0, 20, PeerBlocked, blocked_work
            ));
            peers += (sizeof(peers), NewPeer(
                2, 10, 10, PeerReady, empty
            ));

            // A blocked donor is different from a ready donor with one task:
            // it is not accepting sends, so keeping that singleton local would
            // strand progress. The idle peer should be allowed to drain it.
            peers = StealOne(peers, 2);
            blocked = PeerByID(peers, 1);
            thief = PeerByID(peers, 2);

            assert sizeof(blocked.local_work) == 0,
                "blocked donor can be drained by work stealing";
            assert sizeof(thief.local_work) == 1,
                "idle thief should receive blocked donor work";
            assert thief.local_work[0] == 10,
                "stealing should preserve the blocked donor task";

            goto Done;
        }
    }

    state Done {}
}

machine QuerySchedulerTestDriver {
    start state RunTests {
        entry {
            // The driver starts each story. The checker can then explore
            // schedules around machine creation and completion.
            new TestSchedulerSkipsBlockedRankedPeer();
            new TestSchedulerSkipsBusyRankedPeer();
            new TestSchedulerSkipsQuarantinedRankedPeer();
            new TestSchedulerUsesRTTAsRankTieBreaker();
            new TestSchedulerDropsCanceledQueuedWork();
            new TestSchedulerRTTTimeoutFloor();
            new TestAssignLocalTaskBalancesOwnedWork();
            new TestAssignLocalTaskBuildsBoundedPeerQueue();
            new TestAssignLocalTaskDoesNotExceedPeerCapacity();
            new TestWorkStealingMovesUnclaimedWorkToIdlePeer();
            new TestWorkStealingLeavesBusyDonorQueuedWork();
            new TestWorkStealingIgnoresBusySingletonDonor();
            new TestWorkStealingDoesNotStealSingleton();
            new TestWorkStealingDoesNotStealBusySingleton();
            new TestWorkStealingCanDrainBlockedDonor();

            goto Done;
        }
    }

    state Done {}
}

test tcQueryScheduler [main=QuerySchedulerTestDriver]:
  { TestSchedulerSkipsBlockedRankedPeer,
    TestSchedulerSkipsBusyRankedPeer,
    TestSchedulerSkipsQuarantinedRankedPeer,
    TestSchedulerUsesRTTAsRankTieBreaker,
    TestSchedulerDropsCanceledQueuedWork,
    TestSchedulerRTTTimeoutFloor,
    TestAssignLocalTaskBalancesOwnedWork,
    TestAssignLocalTaskBuildsBoundedPeerQueue,
    TestAssignLocalTaskDoesNotExceedPeerCapacity,
    TestWorkStealingMovesUnclaimedWorkToIdlePeer,
    TestWorkStealingLeavesBusyDonorQueuedWork,
    TestWorkStealingIgnoresBusySingletonDonor,
    TestWorkStealingDoesNotStealSingleton,
    TestWorkStealingDoesNotStealBusySingleton,
    TestWorkStealingCanDrainBlockedDonor,
    QuerySchedulerTestDriver };
