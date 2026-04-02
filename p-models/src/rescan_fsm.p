// rescan_fsm.p - Core rescan FSM state machine.
//
// This machine mirrors the Go implementation in rescan/state_*.go exactly.
// It has 5 P states matching the 5 Go states: Initializing, Syncing, Current,
// Rewinding, Terminal. The key design property being verified is that all
// state mutations flow through the FSM event channel, serializing access and
// preventing the race conditions that plagued the old select-based rescan loop.
//
// Event architecture:
//   - announce e*Outbox events → monitors observe these for safety properties.
//   - send observer, eObsFSM* → test observer receives these for assertions.
//   The two event families are intentionally separate so monitors don't see
//   duplicate events from both announce and send.

// =============================================================================
// FSM Input Events
// =============================================================================

// ProcessNextBlockEvent triggers batch processing in Syncing and Rewinding
// states. Emitted as SelfTellOutbox so the actor mailbox can interleave
// AddWatchAddrs between batches.
event eProcessNextBlockEvent;

// BlockConnectedEvent arrives from the subscription bridge when a new block
// is connected to the chain. Only processed in Current state.
event eBlockConnectedEvent: (Height, BlockHeader);

// BlockDisconnectedEvent arrives from the subscription bridge during a reorg.
// Only processed in Current state.
event eBlockDisconnectedEvent: (Height, BlockHeader, BlockHeader);

// AddWatchAddrsEvent adds addresses to the watch list with an optional
// rewind target. The second field is the rewind_to height (0 = no rewind).
// Mirrors Go's AddWatchAddrsEvent with RewindTo *uint32.
event eAddWatchAddrsEvent: (set[AddrID], Height);

// StopEvent signals graceful shutdown. Transitions to Terminal from any state.
event eStopEvent;

// =============================================================================
// Outbox Events (announced for monitor observation ONLY)
// =============================================================================
// These are announced via P's `announce` keyword, making them visible to all
// `spec` monitors that `observes` them. They are NOT sent to any machine.

// FilteredBlockOutbox is emitted for EVERY block processed, even when no
// transactions match. The wallet relies on this to advance its sync state.
event eFilteredBlockOutbox: (Height, seq[AbstractTx]);

// BlockConnectedOutbox is the simpler block connected notification.
event eBlockConnectedOutbox: (Height, BlockHash);

// BlockDisconnectedOutbox is emitted when a block is walked back.
event eBlockDisconnectedOutbox: (Height, BlockHash);

// RescanProgressOutbox is emitted periodically during sync.
event eRescanProgressOutbox: Height;

// RescanFinishedOutbox signals that the FSM has caught up to the chain tip.
event eRescanFinishedOutbox: (Height, BlockHash);

// StartSubscriptionOutbox tells the actor to start the subscription bridge.
event eStartSubscriptionOutbox: Height;

// CancelSubscriptionOutbox tells the actor to cancel the subscription bridge.
event eCancelSubscriptionOutbox;

// SelfTellOutbox asks the actor to re-send ProcessNextBlockEvent through
// its mailbox (the self-Tell pattern for mailbox interleaving).
event eSelfTellOutbox;

// FSM state change event observed by monitors.
event eFSMStateChange: (RescanFSMState, RescanFSMState);

// Ground truth event announced by the FSM for the NoMissedTransaction monitor.
event eGroundTruthBlockProcessed: (Height, WatchState, seq[AbstractTx]);

// =============================================================================
// Observer Events (sent to test observer, NOT announced to monitors)
// =============================================================================
// These use SEPARATE event types from the outbox events. Monitors use
// `observes` which captures ALL sends of an event type globally. If we sent
// e*Outbox events to the observer, the monitors would see each event twice
// (once from announce, once from send). The eObsFSM* family avoids this.

event eObsFSMFilteredBlock: (Height, seq[AbstractTx]);
event eObsFSMBlockConnected: (Height, BlockHash);
event eObsFSMBlockDisconnected: (Height, BlockHash);
event eObsFSMRescanFinished: (Height, BlockHash);
event eObsFSMStartSubscription: Height;
event eObsFSMCancelSubscription;
event eObsFSMSelfTell;

// =============================================================================
// RescanFSM Machine
// =============================================================================

machine RescanFSM {
    var chain: machine;
    var observer: machine;
    var cur_height: Height;
    var cur_hash: BlockHash;
    var watch: WatchState;
    var batch_size: int;
    var target_height: Height;
    var scanning: bool;

    // =========================================================================
    // Initializing State
    // =========================================================================

    start state Initializing {
        entry (init: RescanFSMInit) {
            chain = init.chain;
            observer = init.observer;
            cur_height = init.start_height;
            cur_hash = init.start_hash;
            watch = init.initial_watch;
            batch_size = init.batch_size;
            scanning = false;

            announce eFSMStateChange, (FSMInitializing, FSMInitializing);
        }

        on eProcessNextBlockEvent do {
            announce eFSMStateChange, (FSMInitializing, FSMSyncing);
            announce eSelfTellOutbox;

            if (observer != default(machine)) {
                send observer, eObsFSMSelfTell;
            }

            goto Syncing;
        }

        on eAddWatchAddrsEvent do (payload: (set[AddrID], Height)) {
            var addr: AddrID;
            foreach (addr in payload.0) {
                watch += (addr);
            }
        }

        on eStopEvent do {
            announce eFSMStateChange, (FSMInitializing, FSMTerminal);
            goto Terminal;
        }
    }

    // =========================================================================
    // Syncing State
    // =========================================================================

    state Syncing {
        on eProcessNextBlockEvent do {
            var i: int;
            var best_height: Height;
            var best_hash: BlockHash;
            var next_height: Height;
            var header: BlockHeader;
            var filter_matched: bool;
            var relevant_txs: seq[AbstractTx];

            // Get current chain tip.
            send chain, eChainGetBestBlock, this;
            receive {
                case eChainBestBlockResp: (resp: (Height, BlockHash)) {
                    best_height = resp.0;
                    best_hash = resp.1;
                }
            }

            i = 0;
            while (i < batch_size) {
                // Check if caught up to chain tip.
                if (cur_height >= best_height) {
                    announce eStartSubscriptionOutbox, cur_height;
                    announce eRescanFinishedOutbox,
                        (cur_height, cur_hash);
                    announce eFSMStateChange, (FSMSyncing, FSMCurrent);

                    if (observer != default(machine)) {
                        send observer, eObsFSMStartSubscription,
                            cur_height;
                        send observer, eObsFSMRescanFinished,
                            (cur_height, cur_hash);
                    }

                    goto Current;
                }

                next_height = cur_height + 1;

                // Fetch header for next block.
                send chain, eChainGetHeaderByHeight,
                    (this, next_height);
                receive {
                    case eChainHeaderByHeightResp: (hdr: BlockHeader) {
                        header = hdr;
                    }
                }

                // Model StartTime as always passed for simplicity.
                scanning = true;

                filter_matched = false;
                relevant_txs = default(seq[AbstractTx]);

                if (sizeof(watch) > 0) {
                    send chain, eChainFilterMatch,
                        (this, header.hash, watch);
                    receive {
                        case eChainFilterMatchResp:
                            (resp: (bool, seq[AbstractTx])) {

                            filter_matched = resp.0;
                            relevant_txs = resp.1;
                        }
                    }
                }

                // Announce ground truth for the NoMissedTransaction
                // monitor: captures watch state + block txs at
                // processing time.
                send chain, eChainGetBlock, (this, header.hash);
                receive {
                    case eChainBlockResp: (bc: BlockContent) {
                        announce eGroundTruthBlockProcessed,
                            (next_height, watch, bc.txs);
                    }
                }

                // Always emit FilteredBlockOutbox for monitor.
                announce eFilteredBlockOutbox,
                    (next_height, relevant_txs);
                // Always emit BlockConnectedOutbox for monitor.
                announce eBlockConnectedOutbox,
                    (next_height, header.hash);

                // Send observer events for test assertions.
                if (observer != default(machine)) {
                    send observer, eObsFSMFilteredBlock,
                        (next_height, relevant_txs);
                    send observer, eObsFSMBlockConnected,
                        (next_height, header.hash);
                }

                cur_height = next_height;
                cur_hash = header.hash;
                i = i + 1;
            }

            // Batch done, not at tip. Self-tell to continue.
            announce eSelfTellOutbox;
            if (observer != default(machine)) {
                send observer, eObsFSMSelfTell;
            }
        }

        on eAddWatchAddrsEvent do (payload: (set[AddrID], Height)) {
            var addr: AddrID;

            foreach (addr in payload.0) {
                watch += (addr);
            }

            // Check for rewind request.
            if (payload.1 > 0 && payload.1 < cur_height) {
                target_height = payload.1;
                announce eFSMStateChange, (FSMSyncing, FSMRewinding);
                announce eSelfTellOutbox;

                if (observer != default(machine)) {
                    send observer, eObsFSMSelfTell;
                }
                goto Rewinding;
            }
        }

        on eStopEvent do {
            announce eFSMStateChange, (FSMSyncing, FSMTerminal);
            goto Terminal;
        }

        ignore eBlockConnectedEvent, eBlockDisconnectedEvent;
    }

    // =========================================================================
    // Current State
    // =========================================================================

    state Current {
        on eBlockConnectedEvent do (payload: (Height, BlockHeader)) {
            var header: BlockHeader;
            var filter_matched: bool;
            var relevant_txs: seq[AbstractTx];

            header = payload.1;

            // A stale retry of the block already at our current tip can
            // arrive after that block was recovered through another path,
            // such as rewind + sync. Treat it as an idempotent no-op.
            if (payload.0 == cur_height && header.hash == cur_hash) {
                return;
            }

            // Validate chain link.
            assert header.prev_block == cur_hash,
                format("Current: block at {0} has prev_block {1} expected {2}", payload.0, header.prev_block, cur_hash);

            filter_matched = false;
            relevant_txs = default(seq[AbstractTx]);

            if (sizeof(watch) > 0) {
                send chain, eChainFilterMatch,
                    (this, header.hash, watch);
                receive {
                    case eChainFilterMatchResp:
                        (resp: (bool, seq[AbstractTx])) {

                        filter_matched = resp.0;
                        relevant_txs = resp.1;
                    }
                }
            }

            // Ground truth for monitor.
            send chain, eChainGetBlock, (this, header.hash);
            receive {
                case eChainBlockResp: (bc: BlockContent) {
                    announce eGroundTruthBlockProcessed,
                        (payload.0, watch, bc.txs);
                }
            }

            // Monitor announcements.
            announce eFilteredBlockOutbox,
                (payload.0, relevant_txs);
            announce eBlockConnectedOutbox,
                (payload.0, header.hash);

            // Observer sends.
            if (observer != default(machine)) {
                send observer, eObsFSMFilteredBlock,
                    (payload.0, relevant_txs);
                send observer, eObsFSMBlockConnected,
                    (payload.0, header.hash);
            }

            cur_height = payload.0;
            cur_hash = header.hash;
        }

        on eBlockDisconnectedEvent do
            (payload: (Height, BlockHeader, BlockHeader)) {

            if (payload.1.hash != cur_hash) {
                return;
            }

            announce eBlockDisconnectedOutbox, (cur_height, cur_hash);

            if (observer != default(machine)) {
                send observer, eObsFSMBlockDisconnected,
                    (cur_height, cur_hash);
            }

            cur_height = cur_height - 1;
            cur_hash = payload.2.hash;
        }

        on eAddWatchAddrsEvent do (payload: (set[AddrID], Height)) {
            var addr: AddrID;

            foreach (addr in payload.0) {
                watch += (addr);
            }

            if (payload.1 > 0 && payload.1 < cur_height) {
                target_height = payload.1;

                announce eCancelSubscriptionOutbox;
                announce eSelfTellOutbox;
                announce eFSMStateChange, (FSMCurrent, FSMRewinding);

                if (observer != default(machine)) {
                    send observer, eObsFSMCancelSubscription;
                    send observer, eObsFSMSelfTell;
                }

                goto Rewinding;
            }
        }

        on eStopEvent do {
            announce eCancelSubscriptionOutbox;
            announce eFSMStateChange, (FSMCurrent, FSMTerminal);

            if (observer != default(machine)) {
                send observer, eObsFSMCancelSubscription;
            }

            goto Terminal;
        }

        ignore eProcessNextBlockEvent;
    }

    // =========================================================================
    // Rewinding State
    // =========================================================================

    state Rewinding {
        on eProcessNextBlockEvent do {
            var i: int;
            var prev_header: BlockHeader;
            var prev_height: Height;

            i = 0;
            while (i < batch_size) {
                if (cur_height <= target_height) {
                    scanning = true;
                    announce eSelfTellOutbox;
                    announce eFSMStateChange,
                        (FSMRewinding, FSMSyncing);

                    if (observer != default(machine)) {
                        send observer, eObsFSMSelfTell;
                    }

                    goto Syncing;
                }

                announce eBlockDisconnectedOutbox,
                    (cur_height, cur_hash);

                if (observer != default(machine)) {
                    send observer, eObsFSMBlockDisconnected,
                        (cur_height, cur_hash);
                }

                // Walk back to previous block.
                send chain, eChainGetHeaderByHash,
                    (this, cur_hash);
                receive {
                    case eChainHeaderByHashResp:
                        (resp: (BlockHeader, Height)) {

                        send chain, eChainGetHeaderByHash,
                            (this, resp.0.prev_block);
                        receive {
                            case eChainHeaderByHashResp:
                                (prev_resp: (BlockHeader, Height)) {

                                prev_header = prev_resp.0;
                                prev_height = prev_resp.1;
                            }
                        }
                    }
                }

                cur_height = prev_height;
                cur_hash = prev_header.hash;
                i = i + 1;
            }

            announce eSelfTellOutbox;
            if (observer != default(machine)) {
                send observer, eObsFSMSelfTell;
            }
        }

        on eAddWatchAddrsEvent do (payload: (set[AddrID], Height)) {
            var addr: AddrID;

            foreach (addr in payload.0) {
                watch += (addr);
            }

            if (payload.1 > 0 && payload.1 < target_height) {
                target_height = payload.1;
            }
        }

        on eStopEvent do {
            announce eFSMStateChange, (FSMRewinding, FSMTerminal);
            goto Terminal;
        }

        ignore eBlockConnectedEvent, eBlockDisconnectedEvent;
    }

    // =========================================================================
    // Terminal State
    // =========================================================================

    state Terminal {
        ignore eProcessNextBlockEvent, eBlockConnectedEvent,
               eBlockDisconnectedEvent, eAddWatchAddrsEvent, eStopEvent;
    }
}
