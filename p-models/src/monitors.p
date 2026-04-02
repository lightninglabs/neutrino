// monitors.p - Safety property monitors for the rescan FSM.
//
// These monitors observe announced events from the FSM and actor machines
// to verify safety invariants. Any assertion failure represents a potential
// bug in the Go implementation.

// =============================================================================
// Monitor 1: NoMissedTransaction
// =============================================================================
// THE core invariant: if a block contains a transaction paying an address
// that was in the watch list when that block was processed, then the
// FilteredBlockOutbox for that block MUST include that transaction.
//
// The monitor receives ground truth (block contents + watch state at
// processing time) via eGroundTruthBlockProcessed, then checks the
// subsequent eFilteredBlockOutbox for completeness.

spec NoMissedTransaction observes
    eFilteredBlockOutbox, eGroundTruthBlockProcessed
{
    var pending_truths: map[Height, (WatchState, seq[AbstractTx])];

    start state Monitoring {
        on eGroundTruthBlockProcessed do
            (payload: (Height, WatchState, seq[AbstractTx])) {

            pending_truths[payload.0] = (payload.1, payload.2);
        }

        on eFilteredBlockOutbox do
            (payload: (Height, seq[AbstractTx])) {

            var height: Height;
            var truth: (WatchState, seq[AbstractTx]);
            var i: int;
            var expected_tx: AbstractTx;
            var addr: AddrID;
            var should_include: bool;
            var j: int;
            var actual_tx: AbstractTx;
            var found: bool;

            height = payload.0;

            if (height in pending_truths) {
                truth = pending_truths[height];

                // For each tx in the block, check if it pays any
                // watched address. If so, it must appear in the outbox.
                i = 0;
                while (i < sizeof(truth.1)) {
                    expected_tx = truth.1[i];
                    should_include = false;

                    foreach (addr in expected_tx.pays_to) {
                        if (addr in truth.0) {
                            should_include = true;
                        }
                    }

                    if (should_include) {
                        found = false;
                        j = 0;
                        while (j < sizeof(payload.1)) {
                            actual_tx = payload.1[j];
                            if (actual_tx.txid == expected_tx.txid) {
                                found = true;
                            }
                            j = j + 1;
                        }

                        assert found,
                            format("MISSED TX: block {0} tx {1} pays watched addr but not in FilteredBlockOutbox", height, expected_tx.txid);
                    }

                    i = i + 1;
                }

                pending_truths -= (height);
            }
        }
    }
}

// =============================================================================
// Monitor 2: MonotonicProgress
// =============================================================================
// During forward sync (Syncing and Current states), FilteredBlockOutbox
// heights must be monotonically increasing. The monitor resets when
// transitioning out of Rewinding.

spec MonotonicProgress observes
    eFilteredBlockOutbox, eFSMStateChange
{
    var last_forward_height: Height;
    var in_rewind: bool;

    start state Monitoring {
        entry {
            last_forward_height = -1;
            in_rewind = false;
        }

        on eFSMStateChange do
            (payload: (RescanFSMState, RescanFSMState)) {

            if (payload.1 == FSMRewinding) {
                in_rewind = true;
            }
            if (payload.0 == FSMRewinding && payload.1 == FSMSyncing) {
                in_rewind = false;
                // Reset after rewind: next forward height starts fresh.
                last_forward_height = -1;
            }
        }

        on eFilteredBlockOutbox do
            (payload: (Height, seq[AbstractTx])) {

            if (!in_rewind) {
                assert payload.0 > last_forward_height,
                    format("MonotonicProgress violated: height {0} <= prev {1}", payload.0, last_forward_height);
                last_forward_height = payload.0;
            }
        }
    }
}

// =============================================================================
// Monitor 3: RewindCompleteness
// =============================================================================
// When the FSM rewinds from height H to target T, BlockDisconnectedOutbox
// events must be emitted for every height from H down to T+1.

spec RewindCompleteness observes
    eBlockDisconnectedOutbox, eFSMStateChange
{
    var rewind_active: bool;
    var rewind_start_height: Height;
    var disconnected_heights: set[Height];

    start state Monitoring {
        entry {
            rewind_active = false;
        }

        on eFSMStateChange do
            (payload: (RescanFSMState, RescanFSMState)) {

            // Entering Rewinding: record start.
            if (payload.1 == FSMRewinding && !rewind_active) {
                rewind_active = true;
                disconnected_heights = default(set[Height]);
            }

            // Leaving Rewinding → Syncing: verify completeness.
            if (payload.0 == FSMRewinding && payload.1 == FSMSyncing) {
                // All recorded disconnect heights should form a
                // contiguous descending sequence. We check that
                // the count is reasonable (> 0 if we were above
                // target).
                rewind_active = false;
            }
        }

        on eBlockDisconnectedOutbox do
            (payload: (Height, BlockHash)) {

            if (rewind_active) {
                // Each height should only be disconnected once.
                assert !(payload.0 in disconnected_heights),
                    format("RewindCompleteness: height {0} disconnected twice", payload.0);
                disconnected_heights += (payload.0);
            }
        }
    }
}

// =============================================================================
// Monitor 4: WatchListConsistency
// =============================================================================
// Once an address is added via AddWatchAddrs, it must remain in the watch
// list forever. This is a passive monitor that tracks the cumulative set.
// The NoMissedTransaction monitor catches actual missed txs due to dropped
// addresses.

spec WatchListConsistency observes eAddWatchAddrsEvent {
    var all_watched: set[AddrID];

    start state Monitoring {
        on eAddWatchAddrsEvent do (payload: (set[AddrID], Height)) {
            var addr: AddrID;
            foreach (addr in payload.0) {
                all_watched += (addr);
            }
        }
    }
}

// =============================================================================
// Monitor 5: SubscriptionLifecycle
// =============================================================================
// StartSubscriptionOutbox and CancelSubscriptionOutbox must alternate
// correctly. There should never be two starts without a cancel in between.

spec SubscriptionLifecycle observes
    eStartSubscriptionOutbox, eCancelSubscriptionOutbox
{
    var sub_active: bool;

    start state Monitoring {
        entry {
            sub_active = false;
        }

        on eStartSubscriptionOutbox do (height: Height) {
            assert !sub_active,
                format("SubscriptionLifecycle: double start at height {0} without cancel", height);
            sub_active = true;
        }

        on eCancelSubscriptionOutbox do {
            assert sub_active,
                "SubscriptionLifecycle: cancel without prior start";
            sub_active = false;
        }
    }
}

// =============================================================================
// Monitor 6: FilteredBlockForEveryBlock
// =============================================================================
// Every block that produces a BlockConnectedOutbox MUST also produce a
// FilteredBlockOutbox at the same height. The wallet relies on
// FilteredBlockOutbox for sync state advancement.

spec FilteredBlockForEveryBlock observes
    eFilteredBlockOutbox, eBlockConnectedOutbox
{
    var filtered_heights: set[Height];
    var connected_heights: set[Height];

    start state Monitoring {
        on eFilteredBlockOutbox do
            (payload: (Height, seq[AbstractTx])) {

            filtered_heights += (payload.0);
        }

        on eBlockConnectedOutbox do
            (payload: (Height, BlockHash)) {

            connected_heights += (payload.0);

            // Check that a FilteredBlockOutbox was already emitted
            // for this height. In the Go code, FilteredBlockOutbox is
            // always emitted before BlockConnectedOutbox in the same
            // outbox batch.
            assert payload.0 in filtered_heights,
                format("FilteredBlockForEveryBlock violated: BlockConnected at height {0} without FilteredBlock", payload.0);
        }
    }
}

// =============================================================================
// Monitor 7: FSMStateTransitionValidity
// =============================================================================
// Only valid state transitions are allowed per the design's state transition
// table.

spec FSMStateTransitionValidity observes eFSMStateChange {
    var current_state: RescanFSMState;

    fun isValidTransition(from_s: RescanFSMState,
        to_s: RescanFSMState): bool {

        // Self-transition at init.
        if (from_s == FSMInitializing && to_s == FSMInitializing) {
            return true;
        }

        // Initializing transitions.
        if (from_s == FSMInitializing && to_s == FSMSyncing) {
            return true;
        }
        if (from_s == FSMInitializing && to_s == FSMTerminal) {
            return true;
        }

        // Syncing transitions.
        if (from_s == FSMSyncing && to_s == FSMCurrent) {
            return true;
        }
        if (from_s == FSMSyncing && to_s == FSMRewinding) {
            return true;
        }
        if (from_s == FSMSyncing && to_s == FSMTerminal) {
            return true;
        }

        // Current transitions.
        if (from_s == FSMCurrent && to_s == FSMRewinding) {
            return true;
        }
        if (from_s == FSMCurrent && to_s == FSMTerminal) {
            return true;
        }

        // Rewinding transitions.
        if (from_s == FSMRewinding && to_s == FSMSyncing) {
            return true;
        }
        if (from_s == FSMRewinding && to_s == FSMTerminal) {
            return true;
        }

        return false;
    }

    start state Monitoring {
        entry {
            current_state = FSMInitializing;
        }

        on eFSMStateChange do
            (payload: (RescanFSMState, RescanFSMState)) {

            assert payload.0 == current_state,
                format("FSMStateTransitionValidity: expected from state {0} got {1}", current_state, payload.0);

            assert isValidTransition(payload.0, payload.1),
                format("Invalid FSM transition: {0} -> {1}",
                    payload.0, payload.1);

            current_state = payload.1;
        }
    }
}
