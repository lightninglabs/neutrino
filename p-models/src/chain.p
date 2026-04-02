// chain.p - Abstract chain source providing headers, filters, and blocks.
//
// MockChain models the neutrino.ChainSource interface that the rescan FSM
// calls synchronously during state transitions. It stores a linear chain of
// blocks, supports subscription for new block notifications, and computes
// filter matches as deterministic set intersection (abstracting GCS filters).

// =============================================================================
// Chain Request/Response Events
// =============================================================================

// BestBlock query: returns (height, hash) of chain tip.
event eChainGetBestBlock: machine;
event eChainBestBlockResp: (Height, BlockHash);

// Header by height: returns the BlockHeader at the given height.
event eChainGetHeaderByHeight: (machine, Height);
event eChainHeaderByHeightResp: BlockHeader;

// Full block by hash: returns the BlockContent for the given hash.
event eChainGetBlock: (machine, BlockHash);
event eChainBlockResp: BlockContent;

// Header by hash: returns (BlockHeader, Height) for the given hash.
// Used by Rewinding state to walk backward via PrevBlock.
event eChainGetHeaderByHash: (machine, BlockHash);
event eChainHeaderByHashResp: (BlockHeader, Height);

// Filter match: given a block hash and watch list, returns whether the block's
// GCS filter matches any watched address, plus the relevant transactions.
// In the real code this is two calls: GetCFilter + gcs.Match, then GetBlock +
// extractBlockMatches. We collapse them into one for the model.
event eChainFilterMatch: (machine, BlockHash, WatchState);
event eChainFilterMatchResp: (bool, seq[AbstractTx]);

// =============================================================================
// Chain Mutation Events (test driver)
// =============================================================================

// Extends the chain by appending a new block. If a subscriber is active, the
// block is delivered as a BlockConnectedEvent notification.
event eChainExtendBlock: BlockContent;

// Disconnects the current tip. If a subscriber is active, the chain delivers a
// BlockDisconnectedEvent for the old tip and rewinds the tip to the previous
// block.
event eChainDisconnectTip;

// Simulates the subscription bridge dying unexpectedly while the chain
// advances by one block before the actor can recover.
event eChainCloseSubscriptionAndExtendBlock: BlockContent;

// =============================================================================
// Subscription Events
// =============================================================================

// Subscribe at a given height. The chain will deliver BlockConnectedEvent for
// all blocks above subscribe_height through the subscriber machine.
event eChainSubscribe: (machine, Height);
event eChainUnsubscribe;

// =============================================================================
// MockChain Machine
// =============================================================================

machine MockChain {
    var blocks_by_height: map[Height, BlockContent];
    var blocks_by_hash: map[BlockHash, BlockContent];
    var tip_height: Height;
    var tip_hash: BlockHash;
    var subscriber: machine;
    var sub_active: bool;
    var observer: machine;

    start state Active {
        entry (init: MockChainInit) {
            var i: int;
            var bc: BlockContent;

            observer = init.observer;
            tip_height = init.tip_height;
            sub_active = false;

            // Index all provided blocks by height and hash.
            i = 0;
            while (i < sizeof(init.blocks)) {
                bc = init.blocks[i];
                blocks_by_height[bc.header.height] = bc;
                blocks_by_hash[bc.header.hash] = bc;

                if (bc.header.height == tip_height) {
                    tip_hash = bc.header.hash;
                }

                i = i + 1;
            }
        }

        on eChainGetBestBlock do (reply_to: machine) {
            send reply_to, eChainBestBlockResp, (tip_height, tip_hash);
        }

        on eChainGetHeaderByHeight do (req: (machine, Height)) {
            var bc: BlockContent;

            assert req.1 in blocks_by_height,
                format("MockChain: height {0} not found", req.1);

            bc = blocks_by_height[req.1];
            send req.0, eChainHeaderByHeightResp, bc.header;
        }

        on eChainGetBlock do (req: (machine, BlockHash)) {
            assert req.1 in blocks_by_hash,
                format("MockChain: hash {0} not found", req.1);

            send req.0, eChainBlockResp, blocks_by_hash[req.1];
        }

        on eChainGetHeaderByHash do (req: (machine, BlockHash)) {
            var bc: BlockContent;

            assert req.1 in blocks_by_hash,
                format("MockChain: hash {0} not found for header lookup",
                    req.1);

            bc = blocks_by_hash[req.1];
            send req.0, eChainHeaderByHashResp,
                (bc.header, bc.header.height);
        }

        on eChainFilterMatch do
            (req: (machine, BlockHash, WatchState)) {

            var bc: BlockContent;
            var matched: bool;
            var relevant: seq[AbstractTx];
            var i: int;
            var tx: AbstractTx;
            var addr: AddrID;
            var tx_relevant: bool;

            assert req.1 in blocks_by_hash,
                format("MockChain: hash {0} not found for filter match",
                    req.1);

            bc = blocks_by_hash[req.1];
            matched = false;

            // Deterministic filter match: iterate through txs, check if
            // any pays_to address is in the watch list.
            i = 0;
            while (i < sizeof(bc.txs)) {
                tx = bc.txs[i];
                tx_relevant = false;

                foreach (addr in tx.pays_to) {
                    if (addr in req.2) {
                        tx_relevant = true;
                    }
                }

                if (tx_relevant) {
                    matched = true;
                    relevant += (sizeof(relevant), tx);
                }

                i = i + 1;
            }

            send req.0, eChainFilterMatchResp, (matched, relevant);
        }

        on eChainExtendBlock do (bc: BlockContent) {
            blocks_by_height[bc.header.height] = bc;
            blocks_by_hash[bc.header.hash] = bc;
            tip_height = bc.header.height;
            tip_hash = bc.header.hash;

            // If a subscriber is active, deliver the new block as a
            // connected notification.
            if (sub_active) {
                send subscriber, eBlockConnectedEvent,
                    (bc.header.height, bc.header);
            }
        }

        on eChainDisconnectTip do {
            var disconnected: BlockContent;
            var chain_tip: BlockContent;

            assert tip_height > 0,
                "MockChain: cannot disconnect genesis tip";

            disconnected = blocks_by_height[tip_height];
            chain_tip = blocks_by_height[tip_height - 1];

            tip_height = chain_tip.header.height;
            tip_hash = chain_tip.header.hash;

            if (sub_active) {
                send subscriber, eBlockDisconnectedEvent,
                    (
                        disconnected.header.height,
                        disconnected.header,
                        chain_tip.header
                    );
            }
        }

        on eChainCloseSubscriptionAndExtendBlock do (bc: BlockContent) {
            var closed_subscriber: machine;

            closed_subscriber = subscriber;
            sub_active = false;

            blocks_by_height[bc.header.height] = bc;
            blocks_by_hash[bc.header.hash] = bc;
            tip_height = bc.header.height;
            tip_hash = bc.header.hash;

            if (closed_subscriber != default(machine)) {
                send closed_subscriber, eActorSubscriptionClosed;
            }
        }

        on eChainSubscribe do (req: (machine, Height)) {
            var h: Height;
            var bc: BlockContent;

            subscriber = req.0;
            sub_active = true;

            // Deliver backlog: any blocks between subscribe_height+1 and
            // current tip. This mirrors the real subscription behavior
            // where Subscribe(height) delivers all blocks from height+1.
            h = req.1 + 1;
            while (h <= tip_height) {
                if (h in blocks_by_height) {
                    bc = blocks_by_height[h];
                    send subscriber, eBlockConnectedEvent,
                        (bc.header.height, bc.header);
                }
                h = h + 1;
            }
        }

        on eChainUnsubscribe do {
            sub_active = false;
        }
    }
}
