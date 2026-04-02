// rescan_test.p - Test corpus for the rescan FSM P model.
//
// Each test machine exercises a specific scenario. Tests receive observation
// events from the FSM via the eObsFSM* event family (separate from the
// e*Outbox events that monitors observe via announce).

// =============================================================================
// Chain Builder Helpers
// =============================================================================

// BuildLinearChain creates a sequence of n+1 blocks (heights 0..n) with no
// transactions. Hash scheme: hash = height + 1 (so genesis hash = 1).
fun BuildLinearChain(n: int): seq[BlockContent] {
    var chain_blocks: seq[BlockContent];
    var i: int;
    var prev_hash: BlockHash;
    var header: BlockHeader;
    var content: BlockContent;

    prev_hash = 0;
    i = 0;
    while (i <= n) {
        header = (hash = i + 1, prev_block = prev_hash, height = i);
        content = (
            header = header,
            txs = default(seq[AbstractTx])
        );
        chain_blocks += (sizeof(chain_blocks), content);
        prev_hash = i + 1;
        i = i + 1;
    }
    return chain_blocks;
}

// BuildChainWithTxAt creates a linear chain where the block at tx_height
// contains a transaction paying tx_addr.
fun BuildChainWithTxAt(n: int, tx_height: Height,
    tx_addr: AddrID): seq[BlockContent] {

    var chain_blocks: seq[BlockContent];
    var i: int;
    var prev_hash: BlockHash;
    var header: BlockHeader;
    var content: BlockContent;
    var tx: AbstractTx;
    var tx_list: seq[AbstractTx];
    var pays_to: set[AddrID];

    prev_hash = 0;
    i = 0;
    while (i <= n) {
        header = (hash = i + 1, prev_block = prev_hash, height = i);

        if (i == tx_height) {
            pays_to = default(set[AddrID]);
            pays_to += (tx_addr);
            tx = (txid = 1000 + i, pays_to = pays_to);
            tx_list = default(seq[AbstractTx]);
            tx_list += (0, tx);
            content = (header = header, txs = tx_list);
        } else {
            content = (
                header = header,
                txs = default(seq[AbstractTx])
            );
        }

        chain_blocks += (sizeof(chain_blocks), content);
        prev_hash = i + 1;
        i = i + 1;
    }
    return chain_blocks;
}

// =============================================================================
// Test 1: Basic Sync to Current
// =============================================================================

machine TestBasicSyncToCurrent {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var chain_data: seq[BlockContent];

    start state Init {
        entry {
            chain_data = BuildLinearChain(5);

            chain_m = new MockChain((
                observer = this,
                blocks = chain_data,
                tip_height = 5
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 100
            ));

            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMSelfTell do {
            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMRescanFinished do
            (payload: (Height, BlockHash)) {

            assert payload.0 == 5,
                format("expected finished at 5, got {0}", payload.0);
            goto Done;
        }

        on eObsFSMFilteredBlock do { }
        on eObsFSMBlockConnected do { }
        on eObsFSMStartSubscription do { }
    }

    state Done {
        ignore eObsFSMFilteredBlock, eObsFSMBlockConnected,
               eObsFSMStartSubscription, eObsFSMSelfTell,
               eObsFSMRescanFinished, eBlockConnectedEvent;
    }
}

// =============================================================================
// Test 2: AddWatchAddrs During Sync With Rewind Finds Tx
// =============================================================================

machine TestAddWatchMidSyncRewindFindsTx {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var chain_data: seq[BlockContent];
    var batch_count: int;
    var found_tx: bool;

    start state Init {
        entry {
            chain_data = BuildChainWithTxAt(15, 5, 42);

            chain_m = new MockChain((
                observer = this,
                blocks = chain_data,
                tip_height = 15
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 3
            ));

            batch_count = 0;
            found_tx = false;

            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMSelfTell do {
            var addr_set: set[AddrID];
            batch_count = batch_count + 1;

            // After 3 batches (past height 5), add addr with rewind.
            if (batch_count == 3) {
                addr_set = default(set[AddrID]);
                addr_set += (42);
                send fsm_m, eAddWatchAddrsEvent, (addr_set, 4);
            }

            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMFilteredBlock do
            (payload: (Height, seq[AbstractTx])) {

            if (sizeof(payload.1) > 0) {
                found_tx = true;
            }
        }

        on eObsFSMRescanFinished do
            (payload: (Height, BlockHash)) {

            assert found_tx,
                "INVARIANT VIOLATED: tx at height 5 missed after rewind to height 4";
            goto Done;
        }

        on eObsFSMBlockConnected do { }
        on eObsFSMBlockDisconnected do { }
        on eObsFSMStartSubscription do { }
        on eObsFSMCancelSubscription do { }
    }

    state Done {
        ignore eObsFSMFilteredBlock, eObsFSMBlockConnected,
               eObsFSMBlockDisconnected, eObsFSMStartSubscription,
               eObsFSMCancelSubscription, eObsFSMSelfTell,
               eObsFSMRescanFinished, eBlockConnectedEvent;
    }
}

// =============================================================================
// Test 3: Self-Tell Interleaving (THE core race condition test)
// =============================================================================

machine TestSelfTellInterleaving {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var actor_m: RescanActor;
    var chain_data: seq[BlockContent];
    var found_tx: bool;

    start state Init {
        entry {
            var addr_set: set[AddrID];

            chain_data = BuildChainWithTxAt(20, 8, 99);

            chain_m = new MockChain((
                observer = this,
                blocks = chain_data,
                tip_height = 20
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 2
            ));

            actor_m = new RescanActor((
                fsm = fsm_m,
                chain = chain_m,
                observer = this
            ));

            found_tx = false;

            // Start the actor.
            send actor_m, eActorStart;

            // Concurrently add watch with rewind to height 5.
            addr_set = default(set[AddrID]);
            addr_set += (99);
            send actor_m, eActorAddWatchAddrs, (addr_set, 5);
        }

        on eObservedFilteredBlock do
            (payload: (Height, seq[AbstractTx])) {

            if (sizeof(payload.1) > 0) {
                found_tx = true;
            }
        }

        on eObservedRescanFinished do
            (payload: (Height, BlockHash)) {

            assert found_tx,
                "RACE CONDITION: AddWatchAddrs interleaved with self-tell but tx at height 8 was missed";
            goto Done;
        }

        // Absorb all other events.
        on eObservedBlockConnected do { }
        on eObservedBlockDisconnected do { }
        on eObservedStartSubscription do { }
        on eObservedCancelSubscription do { }
        on eObservedSelfTell do { }
        on eObservedRescanProgress do { }

        // Also absorb raw FSM observer events (FSM sends to us).
        on eObsFSMFilteredBlock do { }
        on eObsFSMBlockConnected do { }
        on eObsFSMBlockDisconnected do { }
        on eObsFSMStartSubscription do { }
        on eObsFSMCancelSubscription do { }
        on eObsFSMRescanFinished do { }
        on eObsFSMSelfTell do { }
    }

    state Done {
        ignore eObservedFilteredBlock, eObservedBlockConnected,
               eObservedBlockDisconnected, eObservedRescanFinished,
               eObservedStartSubscription, eObservedCancelSubscription,
               eObservedSelfTell, eObservedRescanProgress,
               eObsFSMFilteredBlock, eObsFSMBlockConnected,
               eObsFSMBlockDisconnected, eObsFSMStartSubscription,
               eObsFSMCancelSubscription, eObsFSMRescanFinished,
               eObsFSMSelfTell, eBlockConnectedEvent;
    }
}

// =============================================================================
// Test 4: Rewind Emits All Disconnections
// =============================================================================

machine TestRewindCompleteness {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var chain_data: seq[BlockContent];
    var disconnect_count: int;
    var synced_once: bool;

    start state Init {
        entry {
            chain_data = BuildLinearChain(10);

            chain_m = new MockChain((
                observer = this,
                blocks = chain_data,
                tip_height = 10
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 100
            ));

            disconnect_count = 0;
            synced_once = false;

            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMSelfTell do {
            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMRescanFinished do
            (payload: (Height, BlockHash)) {

            var addr_set: set[AddrID];

            if (!synced_once) {
                synced_once = true;

                // Rewind from 10 to 3.
                addr_set = default(set[AddrID]);
                addr_set += (1);
                send fsm_m, eAddWatchAddrsEvent, (addr_set, 3);
            } else {
                // Second finish after rewind + re-sync.
                // Heights 10,9,8,7,6,5,4 = 7 disconnections.
                assert disconnect_count == 7,
                    format("expected 7 disconnections, got {0}",
                        disconnect_count);
                goto Done;
            }
        }

        on eObsFSMBlockDisconnected do
            (payload: (Height, BlockHash)) {

            disconnect_count = disconnect_count + 1;
        }

        on eObsFSMFilteredBlock do { }
        on eObsFSMBlockConnected do { }
        on eObsFSMStartSubscription do { }
        on eObsFSMCancelSubscription do { }
    }

    state Done {
        ignore eObsFSMFilteredBlock, eObsFSMBlockConnected,
               eObsFSMBlockDisconnected, eObsFSMStartSubscription,
               eObsFSMCancelSubscription, eObsFSMSelfTell,
               eObsFSMRescanFinished, eBlockConnectedEvent;
    }
}

// =============================================================================
// Test 5: Subscription Lifecycle (Start/Cancel Ordering)
// =============================================================================

machine TestSubscriptionLifecycle {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var chain_data: seq[BlockContent];
    var sub_started: bool;
    var sub_cancelled: bool;
    var finished_count: int;

    start state Init {
        entry {
            chain_data = BuildLinearChain(5);

            chain_m = new MockChain((
                observer = this,
                blocks = chain_data,
                tip_height = 5
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 100
            ));

            sub_started = false;
            sub_cancelled = false;
            finished_count = 0;

            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMSelfTell do {
            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMStartSubscription do (height: Height) {
            assert !sub_started || sub_cancelled,
                "double start subscription without cancel";
            sub_started = true;
            sub_cancelled = false;
        }

        on eObsFSMCancelSubscription do {
            assert sub_started,
                "cancel subscription without prior start";
            sub_cancelled = true;
        }

        on eObsFSMRescanFinished do
            (payload: (Height, BlockHash)) {

            var addr_set: set[AddrID];

            finished_count = finished_count + 1;

            if (finished_count == 1) {
                // Trigger rewind from Current.
                addr_set = default(set[AddrID]);
                addr_set += (1);
                send fsm_m, eAddWatchAddrsEvent, (addr_set, 2);
            } else {
                assert sub_started,
                    "subscription must be started after current";
                goto Done;
            }
        }

        on eObsFSMFilteredBlock do { }
        on eObsFSMBlockConnected do { }
        on eObsFSMBlockDisconnected do { }
    }

    state Done {
        ignore eObsFSMFilteredBlock, eObsFSMBlockConnected,
               eObsFSMBlockDisconnected, eObsFSMStartSubscription,
               eObsFSMCancelSubscription, eObsFSMSelfTell,
               eObsFSMRescanFinished, eBlockConnectedEvent;
    }
}

// =============================================================================
// Test 6: Batch Boundary - Reaching Tip Mid-Batch
// =============================================================================

machine TestBatchBoundaryTipMidBatch {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var chain_data: seq[BlockContent];
    var filtered_count: int;

    start state Init {
        entry {
            chain_data = BuildLinearChain(7);

            chain_m = new MockChain((
                observer = this,
                blocks = chain_data,
                tip_height = 7
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 5
            ));

            filtered_count = 0;

            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMSelfTell do {
            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMFilteredBlock do
            (payload: (Height, seq[AbstractTx])) {

            filtered_count = filtered_count + 1;
        }

        on eObsFSMRescanFinished do
            (payload: (Height, BlockHash)) {

            assert payload.0 == 7,
                format("expected finished at 7, got {0}", payload.0);
            assert filtered_count == 7,
                format("expected 7 filtered blocks, got {0}",
                    filtered_count);
            goto Done;
        }

        on eObsFSMBlockConnected do { }
        on eObsFSMStartSubscription do { }
    }

    state Done {
        ignore eObsFSMFilteredBlock, eObsFSMBlockConnected,
               eObsFSMStartSubscription, eObsFSMSelfTell,
               eObsFSMRescanFinished, eBlockConnectedEvent;
    }
}

// =============================================================================
// Test 7: Stop From Initializing
// =============================================================================

machine TestStopFromInitializing {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var chain_data: seq[BlockContent];

    start state Init {
        entry {
            chain_data = BuildLinearChain(3);

            chain_m = new MockChain((
                observer = this,
                blocks = chain_data,
                tip_height = 3
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 100
            ));

            send fsm_m, eStopEvent;
            goto Done;
        }
    }

    state Done {}
}

// =============================================================================
// Test 8: New Blocks Via Subscription After Sync
// =============================================================================

machine TestSubscriptionNewBlocks {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var actor_m: RescanActor;
    var chain_data: seq[BlockContent];
    var sub_blocks_seen: int;

    start state Init {
        entry {
            chain_data = BuildLinearChain(3);

            chain_m = new MockChain((
                observer = this,
                blocks = chain_data,
                tip_height = 3
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 100
            ));

            actor_m = new RescanActor((
                fsm = fsm_m,
                chain = chain_m,
                observer = this
            ));

            sub_blocks_seen = 0;

            send actor_m, eActorStart;
        }

        on eObservedRescanFinished do
            (payload: (Height, BlockHash)) {

            var new_block: BlockContent;
            var header: BlockHeader;

            // Subscription active. Add 2 new blocks.
            header = (hash = 5, prev_block = 4, height = 4);
            new_block = (
                header = header,
                txs = default(seq[AbstractTx])
            );
            send chain_m, eChainExtendBlock, new_block;

            header = (hash = 6, prev_block = 5, height = 5);
            new_block = (
                header = header,
                txs = default(seq[AbstractTx])
            );
            send chain_m, eChainExtendBlock, new_block;

            goto WaitingForBlocks;
        }

        // Absorb events during sync.
        on eObservedFilteredBlock do { }
        on eObservedBlockConnected do { }
        on eObservedStartSubscription do { }
        on eObservedSelfTell do { }
        on eObsFSMFilteredBlock do { }
        on eObsFSMBlockConnected do { }
        on eObsFSMStartSubscription do { }
        on eObsFSMRescanFinished do { }
        on eObsFSMSelfTell do { }
    }

    state WaitingForBlocks {
        on eObservedFilteredBlock do
            (payload: (Height, seq[AbstractTx])) {

            sub_blocks_seen = sub_blocks_seen + 1;

            if (sub_blocks_seen == 2) {
                assert payload.0 == 5,
                    format("expected last block at 5, got {0}",
                        payload.0);
                goto Done;
            }
        }

        on eObservedBlockConnected do { }
        on eObservedStartSubscription do { }
        on eObservedCancelSubscription do { }
        on eObservedRescanFinished do { }
        on eObservedSelfTell do { }
        on eObservedRescanProgress do { }
        on eObservedBlockDisconnected do { }
        on eObsFSMFilteredBlock do { }
        on eObsFSMBlockConnected do { }
        on eObsFSMBlockDisconnected do { }
        on eObsFSMStartSubscription do { }
        on eObsFSMCancelSubscription do { }
        on eObsFSMRescanFinished do { }
        on eObsFSMSelfTell do { }
        on eBlockConnectedEvent do { }
    }

    state Done {
        ignore eObservedFilteredBlock, eObservedBlockConnected,
               eObservedBlockDisconnected, eObservedRescanFinished,
               eObservedStartSubscription, eObservedCancelSubscription,
               eObservedSelfTell, eObservedRescanProgress,
               eObsFSMFilteredBlock, eObsFSMBlockConnected,
               eObsFSMBlockDisconnected, eObsFSMStartSubscription,
               eObsFSMCancelSubscription, eObsFSMRescanFinished,
               eObsFSMSelfTell, eBlockConnectedEvent;
    }
}

// =============================================================================
// Test 9: Concurrent AddWatchAddrs from Multiple Senders
// =============================================================================
// Two external callers send AddWatchAddrs to the actor simultaneously with
// different rewind targets. Verifies that both addresses end up in the watch
// list and no txs paying either address are missed.

machine TestConcurrentAddWatch {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var actor_m: RescanActor;
    var chain_data: seq[BlockContent];
    var found_tx_a: bool;
    var found_tx_b: bool;

    start state Init {
        entry {
            var chain_blocks: seq[BlockContent];
            var i: int;
            var prev_hash: BlockHash;
            var header: BlockHeader;
            var content: BlockContent;
            var tx: AbstractTx;
            var tx_list: seq[AbstractTx];
            var pays_to: set[AddrID];
            var addr_set: set[AddrID];

            // Build 20-block chain with:
            //   - tx at height 5 paying addr 10
            //   - tx at height 12 paying addr 20
            prev_hash = 0;
            i = 0;
            while (i <= 20) {
                header = (
                    hash = i + 1, prev_block = prev_hash, height = i
                );

                if (i == 5) {
                    pays_to = default(set[AddrID]);
                    pays_to += (10);
                    tx = (txid = 1005, pays_to = pays_to);
                    tx_list = default(seq[AbstractTx]);
                    tx_list += (0, tx);
                    content = (header = header, txs = tx_list);
                } else if (i == 12) {
                    pays_to = default(set[AddrID]);
                    pays_to += (20);
                    tx = (txid = 1012, pays_to = pays_to);
                    tx_list = default(seq[AbstractTx]);
                    tx_list += (0, tx);
                    content = (header = header, txs = tx_list);
                } else {
                    content = (
                        header = header,
                        txs = default(seq[AbstractTx])
                    );
                }

                chain_blocks += (sizeof(chain_blocks), content);
                prev_hash = i + 1;
                i = i + 1;
            }

            chain_m = new MockChain((
                observer = this,
                blocks = chain_blocks,
                tip_height = 20
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 2
            ));

            actor_m = new RescanActor((
                fsm = fsm_m,
                chain = chain_m,
                observer = this
            ));

            found_tx_a = false;
            found_tx_b = false;

            // Start the actor.
            send actor_m, eActorStart;

            // Concurrently: sender A adds addr 10 with rewind to 3.
            addr_set = default(set[AddrID]);
            addr_set += (10);
            send actor_m, eActorAddWatchAddrs, (addr_set, 3);

            // Concurrently: sender B adds addr 20 with rewind to 10.
            addr_set = default(set[AddrID]);
            addr_set += (20);
            send actor_m, eActorAddWatchAddrs, (addr_set, 10);
        }

        on eObservedFilteredBlock do
            (payload: (Height, seq[AbstractTx])) {

            var j: int;
            j = 0;
            while (j < sizeof(payload.1)) {
                if (payload.1[j].txid == 1005) {
                    found_tx_a = true;
                }
                if (payload.1[j].txid == 1012) {
                    found_tx_b = true;
                }
                j = j + 1;
            }
        }

        on eObservedRescanFinished do
            (payload: (Height, BlockHash)) {

            assert found_tx_a,
                "CONCURRENT: tx at height 5 paying addr 10 was missed";
            assert found_tx_b,
                "CONCURRENT: tx at height 12 paying addr 20 was missed";
            goto Done;
        }

        on eObservedBlockConnected do { }
        on eObservedBlockDisconnected do { }
        on eObservedStartSubscription do { }
        on eObservedCancelSubscription do { }
        on eObservedSelfTell do { }
        on eObservedRescanProgress do { }
        on eObsFSMFilteredBlock do { }
        on eObsFSMBlockConnected do { }
        on eObsFSMBlockDisconnected do { }
        on eObsFSMStartSubscription do { }
        on eObsFSMCancelSubscription do { }
        on eObsFSMRescanFinished do { }
        on eObsFSMSelfTell do { }
    }

    state Done {
        ignore eObservedFilteredBlock, eObservedBlockConnected,
               eObservedBlockDisconnected, eObservedRescanFinished,
               eObservedStartSubscription, eObservedCancelSubscription,
               eObservedSelfTell, eObservedRescanProgress,
               eObsFSMFilteredBlock, eObsFSMBlockConnected,
               eObsFSMBlockDisconnected, eObsFSMStartSubscription,
               eObsFSMCancelSubscription, eObsFSMRescanFinished,
               eObsFSMSelfTell, eBlockConnectedEvent;
    }
}

// =============================================================================
// Test 10: Chain Extension During Sync
// =============================================================================
// The chain tip moves forward while the FSM is syncing. Verifies that the FSM
// correctly detects the new tip and does not terminate early.

machine TestChainExtensionDuringSync {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var chain_data: seq[BlockContent];
    var extended: bool;

    start state Init {
        entry {
            // Start with a 5-block chain.
            chain_data = BuildLinearChain(5);

            chain_m = new MockChain((
                observer = this,
                blocks = chain_data,
                tip_height = 5
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 3
            ));

            extended = false;

            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMSelfTell do {
            var header: BlockHeader;
            var new_block: BlockContent;

            // After first batch (3 blocks), extend the chain to 8.
            if (!extended) {
                extended = true;

                header = (hash = 7, prev_block = 6, height = 6);
                new_block = (
                    header = header,
                    txs = default(seq[AbstractTx])
                );
                send chain_m, eChainExtendBlock, new_block;

                header = (hash = 8, prev_block = 7, height = 7);
                new_block = (
                    header = header,
                    txs = default(seq[AbstractTx])
                );
                send chain_m, eChainExtendBlock, new_block;

                header = (hash = 9, prev_block = 8, height = 8);
                new_block = (
                    header = header,
                    txs = default(seq[AbstractTx])
                );
                send chain_m, eChainExtendBlock, new_block;
            }

            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMRescanFinished do
            (payload: (Height, BlockHash)) {

            // Should finish at 8, not 5.
            assert payload.0 == 8,
                format("expected finished at 8, got {0}", payload.0);
            goto Done;
        }

        on eObsFSMFilteredBlock do { }
        on eObsFSMBlockConnected do { }
        on eObsFSMStartSubscription do { }
    }

    state Done {
        ignore eObsFSMFilteredBlock, eObsFSMBlockConnected,
               eObsFSMStartSubscription, eObsFSMSelfTell,
               eObsFSMRescanFinished, eBlockConnectedEvent;
    }
}

// =============================================================================
// Test 11: Rewind-Within-Rewind (Lower Target During Active Rewind)
// =============================================================================
// AddWatchAddrs with a lower target arrives while already rewinding. Verifies
// the target is updated and all blocks from original start down to the new
// target are disconnected.

machine TestRewindWithinRewind {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var chain_data: seq[BlockContent];
    var disconnect_count: int;
    var synced_once: bool;
    var rewind_started: bool;
    var lowered_target: bool;

    start state Init {
        entry {
            chain_data = BuildLinearChain(15);

            chain_m = new MockChain((
                observer = this,
                blocks = chain_data,
                tip_height = 15
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 100
            ));

            disconnect_count = 0;
            synced_once = false;
            rewind_started = false;
            lowered_target = false;

            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMSelfTell do {
            var addr_set: set[AddrID];

            // If we've started rewinding but haven't lowered the target
            // yet, send a new AddWatchAddrs with a lower target.
            if (rewind_started && !lowered_target) {
                lowered_target = true;
                addr_set = default(set[AddrID]);
                addr_set += (3);
                // Lower target from 10 to 5.
                send fsm_m, eAddWatchAddrsEvent, (addr_set, 5);
            }

            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMRescanFinished do
            (payload: (Height, BlockHash)) {

            var addr_set: set[AddrID];

            if (!synced_once) {
                synced_once = true;

                // First rewind from 15 to 10.
                addr_set = default(set[AddrID]);
                addr_set += (1);
                send fsm_m, eAddWatchAddrsEvent, (addr_set, 10);
                rewind_started = true;
            } else {
                // Second finish. Disconnects should cover 15 down to 6
                // (heights 15,14,13,12,11,10,9,8,7,6 = 10 disconnects).
                // (Original rewind to 10: heights 15..11 = 5, then
                // lowered to 5: heights 10..6 = 5, total 10.)
                assert disconnect_count == 10,
                    format("expected 10 disconnections, got {0}",
                        disconnect_count);
                goto Done;
            }
        }

        on eObsFSMBlockDisconnected do
            (payload: (Height, BlockHash)) {

            disconnect_count = disconnect_count + 1;
        }

        on eObsFSMFilteredBlock do { }
        on eObsFSMBlockConnected do { }
        on eObsFSMStartSubscription do { }
        on eObsFSMCancelSubscription do { }
    }

    state Done {
        ignore eObsFSMFilteredBlock, eObsFSMBlockConnected,
               eObsFSMBlockDisconnected, eObsFSMStartSubscription,
               eObsFSMCancelSubscription, eObsFSMSelfTell,
               eObsFSMRescanFinished, eBlockConnectedEvent;
    }
}

// =============================================================================
// Test 12: Rewind to Genesis (Height 0 Edge Case)
// =============================================================================
// Verifies that rewinding all the way to height 0 works correctly: all blocks
// from tip to 1 are disconnected, then re-sync finds the tx.

machine TestRewindToGenesis {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var actor_m: RescanActor;
    var chain_data: seq[BlockContent];
    var found_tx: bool;

    start state Init {
        entry {
            var addr_set: set[AddrID];

            // 8-block chain with tx at height 2 paying addr 77.
            chain_data = BuildChainWithTxAt(8, 2, 77);

            chain_m = new MockChain((
                observer = this,
                blocks = chain_data,
                tip_height = 8
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 3
            ));

            actor_m = new RescanActor((
                fsm = fsm_m,
                chain = chain_m,
                observer = this
            ));

            found_tx = false;

            // Start the actor.
            send actor_m, eActorStart;

            // Add watch with rewind to 0 (genesis). This is the most
            // extreme rewind possible.
            addr_set = default(set[AddrID]);
            addr_set += (77);
            send actor_m, eActorAddWatchAddrs, (addr_set, 0);
        }

        on eObservedFilteredBlock do
            (payload: (Height, seq[AbstractTx])) {

            if (sizeof(payload.1) > 0) {
                found_tx = true;
            }
        }

        on eObservedRescanFinished do
            (payload: (Height, BlockHash)) {

            assert found_tx,
                "EDGE CASE: rewind to genesis missed tx at height 2";
            goto Done;
        }

        on eObservedBlockConnected do { }
        on eObservedBlockDisconnected do { }
        on eObservedStartSubscription do { }
        on eObservedCancelSubscription do { }
        on eObservedSelfTell do { }
        on eObservedRescanProgress do { }
        on eObsFSMFilteredBlock do { }
        on eObsFSMBlockConnected do { }
        on eObsFSMBlockDisconnected do { }
        on eObsFSMStartSubscription do { }
        on eObsFSMCancelSubscription do { }
        on eObsFSMRescanFinished do { }
        on eObsFSMSelfTell do { }
    }

    state Done {
        ignore eObservedFilteredBlock, eObservedBlockConnected,
               eObservedBlockDisconnected, eObservedRescanFinished,
               eObservedStartSubscription, eObservedCancelSubscription,
               eObservedSelfTell, eObservedRescanProgress,
               eObsFSMFilteredBlock, eObsFSMBlockConnected,
               eObsFSMBlockDisconnected, eObsFSMStartSubscription,
               eObsFSMCancelSubscription, eObsFSMRescanFinished,
               eObsFSMSelfTell, eBlockConnectedEvent;
    }
}

// =============================================================================
// Test 13: Rapid Rewind-Sync-Rewind Cycling
// =============================================================================
// Three consecutive rewinds with different targets, exercising the
// Current→Rewind→Sync→Current→Rewind→Sync→Current cycle. Each rewind adds
// a new address. All txs must be found.

machine TestRapidRewindCycles {
    var chain_m: MockChain;
    var fsm_m: RescanFSM;
    var chain_data: seq[BlockContent];
    var finished_count: int;
    var found_tx_a: bool;
    var found_tx_b: bool;
    var found_tx_c: bool;

    start state Init {
        entry {
            var chain_blocks: seq[BlockContent];
            var i: int;
            var prev_hash: BlockHash;
            var header: BlockHeader;
            var content: BlockContent;
            var tx: AbstractTx;
            var tx_list: seq[AbstractTx];
            var pays_to: set[AddrID];

            // 12-block chain with txs at heights 2, 6, 10.
            prev_hash = 0;
            i = 0;
            while (i <= 12) {
                header = (
                    hash = i + 1, prev_block = prev_hash, height = i
                );

                if (i == 2) {
                    pays_to = default(set[AddrID]);
                    pays_to += (50);
                    tx = (txid = 1002, pays_to = pays_to);
                    tx_list = default(seq[AbstractTx]);
                    tx_list += (0, tx);
                    content = (header = header, txs = tx_list);
                } else if (i == 6) {
                    pays_to = default(set[AddrID]);
                    pays_to += (60);
                    tx = (txid = 1006, pays_to = pays_to);
                    tx_list = default(seq[AbstractTx]);
                    tx_list += (0, tx);
                    content = (header = header, txs = tx_list);
                } else if (i == 10) {
                    pays_to = default(set[AddrID]);
                    pays_to += (70);
                    tx = (txid = 1010, pays_to = pays_to);
                    tx_list = default(seq[AbstractTx]);
                    tx_list += (0, tx);
                    content = (header = header, txs = tx_list);
                } else {
                    content = (
                        header = header,
                        txs = default(seq[AbstractTx])
                    );
                }

                chain_blocks += (sizeof(chain_blocks), content);
                prev_hash = i + 1;
                i = i + 1;
            }

            chain_m = new MockChain((
                observer = this,
                blocks = chain_blocks,
                tip_height = 12
            ));

            fsm_m = new RescanFSM((
                chain = chain_m,
                observer = this,
                start_height = 0,
                start_hash = 1,
                initial_watch = default(WatchState),
                batch_size = 100
            ));

            finished_count = 0;
            found_tx_a = false;
            found_tx_b = false;
            found_tx_c = false;

            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMSelfTell do {
            send fsm_m, eProcessNextBlockEvent;
        }

        on eObsFSMFilteredBlock do
            (payload: (Height, seq[AbstractTx])) {

            var j: int;
            j = 0;
            while (j < sizeof(payload.1)) {
                if (payload.1[j].txid == 1002) {
                    found_tx_a = true;
                }
                if (payload.1[j].txid == 1006) {
                    found_tx_b = true;
                }
                if (payload.1[j].txid == 1010) {
                    found_tx_c = true;
                }
                j = j + 1;
            }
        }

        on eObsFSMRescanFinished do
            (payload: (Height, BlockHash)) {

            var addr_set: set[AddrID];

            finished_count = finished_count + 1;

            if (finished_count == 1) {
                // Rewind 1: add addr 70, rewind to 8.
                addr_set = default(set[AddrID]);
                addr_set += (70);
                send fsm_m, eAddWatchAddrsEvent, (addr_set, 8);
            } else if (finished_count == 2) {
                // Rewind 2: add addr 60, rewind to 4.
                addr_set = default(set[AddrID]);
                addr_set += (60);
                send fsm_m, eAddWatchAddrsEvent, (addr_set, 4);
            } else if (finished_count == 3) {
                // Rewind 3: add addr 50, rewind to 1.
                addr_set = default(set[AddrID]);
                addr_set += (50);
                send fsm_m, eAddWatchAddrsEvent, (addr_set, 1);
            } else {
                // Fourth finish: all three txs must be found.
                assert found_tx_a,
                    "RAPID CYCLE: tx 1002 at height 2 missed";
                assert found_tx_b,
                    "RAPID CYCLE: tx 1006 at height 6 missed";
                assert found_tx_c,
                    "RAPID CYCLE: tx 1010 at height 10 missed";
                goto Done;
            }
        }

        on eObsFSMBlockConnected do { }
        on eObsFSMBlockDisconnected do { }
        on eObsFSMStartSubscription do { }
        on eObsFSMCancelSubscription do { }
    }

    state Done {
        ignore eObsFSMFilteredBlock, eObsFSMBlockConnected,
               eObsFSMBlockDisconnected, eObsFSMStartSubscription,
               eObsFSMCancelSubscription, eObsFSMSelfTell,
               eObsFSMRescanFinished, eBlockConnectedEvent;
    }
}

// =============================================================================
// Test Declarations
// =============================================================================

test tcBasicSync [main=TestBasicSyncToCurrent]:
    assert NoMissedTransaction, MonotonicProgress,
           FilteredBlockForEveryBlock, FSMStateTransitionValidity
    in { MockChain, RescanFSM, TestBasicSyncToCurrent };

test tcAddWatchRewind [main=TestAddWatchMidSyncRewindFindsTx]:
    assert NoMissedTransaction, MonotonicProgress, RewindCompleteness,
           FSMStateTransitionValidity
    in { MockChain, RescanFSM, TestAddWatchMidSyncRewindFindsTx };

test tcRewindComplete [main=TestRewindCompleteness]:
    assert MonotonicProgress, RewindCompleteness,
           SubscriptionLifecycle, FSMStateTransitionValidity
    in { MockChain, RescanFSM, TestRewindCompleteness };

test tcSubLifecycle [main=TestSubscriptionLifecycle]:
    assert MonotonicProgress, SubscriptionLifecycle,
           FSMStateTransitionValidity
    in { MockChain, RescanFSM, TestSubscriptionLifecycle };

test tcBatchBoundary [main=TestBatchBoundaryTipMidBatch]:
    assert MonotonicProgress, FilteredBlockForEveryBlock,
           FSMStateTransitionValidity
    in { MockChain, RescanFSM, TestBatchBoundaryTipMidBatch };

test tcStopInit [main=TestStopFromInitializing]:
    assert FSMStateTransitionValidity
    in { MockChain, RescanFSM, TestStopFromInitializing };

test tcSelfTellInterleaving [main=TestSelfTellInterleaving]:
    assert NoMissedTransaction, MonotonicProgress, SubscriptionLifecycle,
           FSMStateTransitionValidity
    in { MockChain, RescanFSM, RescanActor, TestSelfTellInterleaving };

test tcSubscriptionNewBlocks [main=TestSubscriptionNewBlocks]:
    assert MonotonicProgress, SubscriptionLifecycle,
           FilteredBlockForEveryBlock, FSMStateTransitionValidity
    in { MockChain, RescanFSM, RescanActor, TestSubscriptionNewBlocks };

test tcConcurrentAddWatch [main=TestConcurrentAddWatch]:
    assert NoMissedTransaction, MonotonicProgress, RewindCompleteness,
           SubscriptionLifecycle, FSMStateTransitionValidity
    in { MockChain, RescanFSM, RescanActor, TestConcurrentAddWatch };

test tcChainExtension [main=TestChainExtensionDuringSync]:
    assert MonotonicProgress, FilteredBlockForEveryBlock,
           FSMStateTransitionValidity
    in { MockChain, RescanFSM, TestChainExtensionDuringSync };

test tcRewindWithinRewind [main=TestRewindWithinRewind]:
    assert MonotonicProgress, RewindCompleteness,
           SubscriptionLifecycle, FSMStateTransitionValidity
    in { MockChain, RescanFSM, TestRewindWithinRewind };

test tcRewindToGenesis [main=TestRewindToGenesis]:
    assert NoMissedTransaction, MonotonicProgress, RewindCompleteness,
           SubscriptionLifecycle, FSMStateTransitionValidity
    in { MockChain, RescanFSM, RescanActor, TestRewindToGenesis };

test tcRapidRewindCycles [main=TestRapidRewindCycles]:
    assert NoMissedTransaction, MonotonicProgress, RewindCompleteness,
           SubscriptionLifecycle, FSMStateTransitionValidity
    in { MockChain, RescanFSM, TestRapidRewindCycles };
