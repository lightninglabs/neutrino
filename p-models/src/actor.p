// actor.p - Actor wrapper modeling mailbox serialization, self-tell
// interleaving, and the subscription bridge.
//
// This machine models the RescanActor from actor.go after the migration onto
// the real lnd actor runtime. The important abstraction is unchanged: start,
// stop, add-watch requests, self-tells, and subscription bridge notifications
// all flow through one mailbox and are serialized by the actor runtime.
//
// Its primary purpose in the P model is to expose the same self-tell pattern
// to P's nondeterministic scheduler: when the FSM emits eSelfTellOutbox, the
// actor converts it to eActorSelfTell sent to itself. P's mailbox semantics
// mean this event competes with any pending eActorAddWatchAddrs from external
// callers or bridge-delivered block notifications, exercising the same
// interleavings as the real runtime.

// =============================================================================
// Actor External Events
// =============================================================================

// External callers use these to interact with the actor.
event eActorStart;
event eActorAddWatchAddrs: (set[AddrID], Height);
event eActorStop;

// Internal self-tell: dispatched back through the actor's own mailbox.
event eActorSelfTell;

// Internal mailbox event emitted when the subscription bridge dies
// unexpectedly and the actor needs to recover by re-subscribing.
event eActorSubscriptionClosed;

// =============================================================================
// Actor Callback Dispatch Events (announced for monitors)
// =============================================================================

// The real actor dispatches both filtered and plain disconnect callbacks for a
// BlockDisconnectedOutbox. These callback edges are wrapper-level behavior, so
// we model them explicitly at the actor layer.
event eFilteredBlockDisconnectedCallback: (Height, BlockHash);
event eBlockDisconnectedCallback: (Height, BlockHash);

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
    var last_known_height: Height;

    start state Active {
        entry (init: RescanActorInit) {
            fsm = init.fsm;
            chain = init.chain;
            observer = init.observer;
            sub_active = false;
            last_known_height = 0;
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
        // In the real Go code, AddWatchAddrs is an Ask against the actor
        // mailbox. Here, the P scheduler naturally interleaves it with any
        // pending self-tells or bridge-delivered notifications.

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
            last_known_height = payload.0;

            if (observer != default(machine)) {
                send observer, eObservedBlockConnected, payload;
            }
        }

        on eBlockDisconnectedOutbox do
            (payload: (Height, BlockHash)) {

            if (payload.0 > 0) {
                last_known_height = payload.0 - 1;
            } else {
                last_known_height = 0;
            }

            announce eFilteredBlockDisconnectedCallback, payload;
            announce eBlockDisconnectedCallback, payload;

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
            last_known_height = height;
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

        on eActorSubscriptionClosed do {
            if (sub_active) {
                sub_active = false;
                send chain, eChainSubscribe, (this, last_known_height);
                sub_active = true;
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
