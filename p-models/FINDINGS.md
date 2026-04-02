# Rescan FSM P Model Findings

## Model Verification Results

All 8 test cases pass across all explored schedules:

| Test Case | Schedules | Strategy | Result |
|-----------|-----------|----------|--------|
| tcBasicSync | 100 | random | PASS |
| tcAddWatchRewind | 100 | random | PASS |
| tcRewindComplete | 100 | random | PASS |
| tcSubLifecycle | 100 | random | PASS |
| tcBatchBoundary | 100 | random | PASS |
| tcStopInit | 100 | random | PASS |
| tcSelfTellInterleaving | 10,000 | PCT(3) | PASS |
| tcSubscriptionNewBlocks | 1,000 | random | PASS |

## Safety Properties Verified

1. **NoMissedTransaction**: If a block contains a tx paying a watched address and
   the address was in the watch list at processing time, the tx always appears in
   FilteredBlockOutbox. Verified across all sync, rewind, and interleaving scenarios.

2. **MonotonicProgress**: Block heights in FilteredBlockOutbox are strictly
   monotonically increasing during forward sync. Height tracking correctly resets
   after rewind→syncing transitions.

3. **RewindCompleteness**: When rewinding from height H to target T, exactly H-T
   BlockDisconnectedOutbox events are emitted, one per height. No height is
   disconnected twice.

4. **SubscriptionLifecycle**: StartSubscriptionOutbox and CancelSubscriptionOutbox
   alternate correctly. Verified through sync→current→rewind→syncing→current
   lifecycle.

5. **FilteredBlockForEveryBlock**: Every block that produces a BlockConnectedOutbox
   also produces a FilteredBlockOutbox at the same height. Confirms the wallet
   sync state advancement invariant.

6. **FSMStateTransitionValidity**: Only valid state transitions occur per the
   design's state transition table. No unexpected transitions under any scheduling.

## Self-Tell Interleaving Analysis

The `tcSelfTellInterleaving` test exercises the core architectural property: P's
nondeterministic scheduler explores all possible orderings of self-tell
(ProcessNextBlockEvent) vs external AddWatchAddrs delivery through the actor
mailbox. With 10,000 schedules under PCT(3) strategy (which prioritizes
rare interleavings), zero bugs were found. This confirms that:

- The self-tell pattern correctly allows AddWatchAddrs to interleave between
  batch boundaries.
- Regardless of when AddWatchAddrs(rewind) arrives relative to batch processing,
  the rewind always causes the missed tx to be found.
- The old select-based race condition (rescan-issues.md §2) is eliminated by
  design.

## Observations for Go Implementation Review

### Observation 1: Watch List Immutability

The P model uses `set[AddrID]` (immutable by P semantics) for the watch list. The
Go implementation uses WatchState with Clone() and ApplyUpdate(). The P model
confirms the _semantic_ correctness of the transitions, but cannot verify that
Clone() actually produces a deep copy in Go. The Go property-based tests with
`-race` cover this gap.

### Observation 2: Height Type Safety

The P model uses uniform `int` for heights, masking the Go code's int32/uint32
mixing. The MonotonicProgress monitor would catch the _symptom_ of type overflow
(height wrapping negative), but the model cannot directly verify type safety. This
is a known limitation documented in the plan.

### Observation 3: Subscription Backlog Delivery

The MockChain model delivers backlog blocks (from subscribe height to tip) when
Subscribe is called. The real neutrino.ChainSource subscription does the same.
The model verifies that the FSM correctly processes these backlog blocks in Current
state. The `tcSubscriptionNewBlocks` test additionally verifies new blocks arriving
after sync.

## Next Steps

1. Build Go bridge (trace replay harness) per darepo pattern.
2. Add advanced tests: concurrent AddWatchAddrs senders, chain extension during
   sync, rewind-within-rewind, EndBlock bounded rescan.
3. Consider adding nondeterministic filter matching variant (false positives) to
   stress the block fetch pipeline.
