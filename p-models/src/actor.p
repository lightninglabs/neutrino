// actor.p - Actor wrapper modeling self-tell interleaving and subscription
// bridge.
//
// This machine models the RescanActor from actor.go. Its primary purpose in
// the P model is to expose the self-tell pattern to P's nondeterministic
// scheduler: when the FSM emits eSelfTellOutbox, the actor converts it to
// eActorSelfTell sent to itself. P's mailbox semantics mean this event
// competes with any pending eActorAddWatchAddrs from external callers,
// exercising all possible interleavings.

// =============================================================================
// Actor External Events
// =============================================================================

// External callers use these to interact with the actor.
event eActorStart;
event eActorAddWatchAddrs: (set[AddrID], Height);
event eActorStop;

// Internal self-tell: dispatched back through the actor's own mailbox.
event eActorSelfTell;

// =============================================================================
// Actor Observation Events (sent to test observer)
// =============================================================================

event eObservedFilteredBlock: (Height, seq[AbstractTx]);
event eObservedBlockConnected: (Height, BlockHash);
event eObservedBlockDisconnected: (Height, BlockHash);
event eObservedRescanFinished: (Height, BlockHash);
event eObservedRescanProgress: Height;
event eObservedStartSubscription: Height;
event eObservedCancelSubscription;
event eObservedSelfTell;

// =============================================================================
// RescanActor Machine
// =============================================================================

machine RescanActor {
    var fsm: machine;
    var chain: machine;
    var observer: machine;
    var sub_active: bool;

    start state Active {
        entry (init: RescanActorInit) {
            fsm = init.fsm;
            chain = init.chain;
            observer = init.observer;
            sub_active = false;
        }

        // =====================================================================
        // Actor lifecycle events
        // =====================================================================

        on eActorStart do {
            // Kick off the FSM with the first ProcessNextBlockEvent.
            send fsm, eProcessNextBlockEvent;
        }

        on eActorStop do {
            send fsm, eStopEvent;
        }

        // =====================================================================
        // External API: AddWatchAddrs
        // =====================================================================
        // In the Go code, AddWatchAddrs spawns a goroutine that calls
        // sendAndDispatch. Here, the P scheduler naturally interleaves
        // this with any pending self-tells.

        on eActorAddWatchAddrs do (payload: (set[AddrID], Height)) {
            send fsm, eAddWatchAddrsEvent, payload;
        }

        // =====================================================================
        // Self-tell pattern
        // =====================================================================
        // When the FSM emits eSelfTellOutbox, the actor sends eActorSelfTell
        // to itself. This goes through P's mailbox, competing with any other
        // pending events (e.g., eActorAddWatchAddrs). On receipt, we forward
        // ProcessNextBlockEvent to the FSM.

        on eSelfTellOutbox do {
            send this, eActorSelfTell;
        }

        on eActorSelfTell do {
            send fsm, eProcessNextBlockEvent;

            if (observer != default(machine)) {
                send observer, eObservedSelfTell;
            }
        }

        // =====================================================================
        // Outbox event dispatch (from FSM to observer)
        // =====================================================================

        on eFilteredBlockOutbox do
            (payload: (Height, seq[AbstractTx])) {

            if (observer != default(machine)) {
                send observer, eObservedFilteredBlock, payload;
            }
        }

        on eBlockConnectedOutbox do (payload: (Height, BlockHash)) {
            if (observer != default(machine)) {
                send observer, eObservedBlockConnected, payload;
            }
        }

        on eBlockDisconnectedOutbox do
            (payload: (Height, BlockHash)) {

            if (observer != default(machine)) {
                send observer, eObservedBlockDisconnected, payload;
            }
        }

        on eRescanProgressOutbox do (height: Height) {
            if (observer != default(machine)) {
                send observer, eObservedRescanProgress, height;
            }
        }

        on eRescanFinishedOutbox do
            (payload: (Height, BlockHash)) {

            if (observer != default(machine)) {
                send observer, eObservedRescanFinished, payload;
            }
        }

        // =====================================================================
        // Subscription lifecycle
        // =====================================================================

        on eStartSubscriptionOutbox do (height: Height) {
            sub_active = true;
            send chain, eChainSubscribe, (this, height);

            if (observer != default(machine)) {
                send observer, eObservedStartSubscription, height;
            }
        }

        on eCancelSubscriptionOutbox do {
            if (sub_active) {
                sub_active = false;
                send chain, eChainUnsubscribe;
            }

            if (observer != default(machine)) {
                send observer, eObservedCancelSubscription;
            }
        }

        // =====================================================================
        // Subscription bridge: forward block notifications from chain to FSM
        // =====================================================================
        // In the Go code, bridgeSubscription reads from
        // sub.Notifications and converts to FSM events. Here, the chain
        // sends eBlockConnectedEvent directly to the actor (since we
        // registered as the subscriber), and we forward to the FSM.

        on eBlockConnectedEvent do (payload: (Height, BlockHeader)) {
            send fsm, eBlockConnectedEvent, payload;
        }

        on eBlockDisconnectedEvent do
            (payload: (Height, BlockHeader, BlockHeader)) {

            send fsm, eBlockDisconnectedEvent, payload;
        }

        // Absorb FSM state change announcements that arrive as events.
        ignore eFSMStateChange, eGroundTruthBlockProcessed;
    }
}
