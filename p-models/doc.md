# Neutrino Rescan FSM: P Formal Model

This directory contains an executable P 3.0.4 model of the neutrino rescan
FSM state machine. The model mirrors the Go implementation in `rescan/` and
uses P's exhaustive state-space exploration to verify safety properties that
Go unit tests and property-based tests cannot fully cover, particularly around
nondeterministic message interleaving.

## Why a Formal Model

The rescan pipeline replaces a racy `select`-based event loop with an
actor+FSM that serializes all state mutations through a single mailbox. The
core design bet is that the self-tell pattern (emitting `ProcessNextBlockEvent`
as an outbox event routed back through the actor mailbox) allows external
`AddWatchAddrs` messages to interleave between batch boundaries without
missing any transactions.

Go's `-race` detector catches data races, and property-based tests with
`pgregory.net/rapid` exercise random event sequences. But neither can
exhaustively explore all possible orderings of concurrent message delivery.
P's model checker does exactly this: it systematically explores every schedule
the nondeterministic scheduler can produce, checking safety monitors at each
step.

This approach follows an internal pattern where P model suites have been used
to find real implementation bugs by making protocol and infrastructure contracts
executable and exhaustively checking all possible message interleavings.

## Model Architecture

The model is ~2,400 lines of P across 6 files, organized into source machines
and test scenarios.

### Abstraction Strategy

Real-world details are abstracted to preserve the semantic relationships that
matter for correctness while discarding cryptographic and network complexity:

| Real Concept | P Abstraction |
|---|---|
| `btcutil.Address` + pkScript | `AddrID = int` |
| `chainhash.Hash` | `BlockHash = int` |
| `wire.BlockHeader` | `(hash, prev_block, height)` tuple |
| GCS compact block filter | Deterministic set intersection |
| `neutrino.ChainSource` | `MockChain` machine with req/resp events |
| WatchState (Addrs + Inputs + List) | `set[AddrID]` |
| Actor goroutine + channels | P machine with mailbox semantics |

### File Layout

```
p-models/
  rescan.pproj              Project file
  src/
    types.p       (100 L)   Abstract types, enums, init configs
    chain.p       (204 L)   MockChain: headers, filters, blocks, subscriptions
    rescan_fsm.p  (493 L)   Core FSM: 5 states matching Go exactly
    actor.p       (183 L)   Actor wrapper: self-tell, subscription bridge
    monitors.p    (338 L)   7 safety monitors
  test/
    rescan_test.p (1101 L)  11 test scenarios + declarations
  bridge/
    go.mod                  Go module for bridge
    trace_parser.go         P trace JSON parser
    event_mapping.go        Event type constants and payload types
    replay_harness.go       FSM simulator for trace conformance checking
    replay_test.go          Bridge tests
  scripts/
    check.sh                Full test run
    check-quick.sh          CI-sized quick run
```

### Event Architecture

The model uses two separate event families to avoid double-counting by monitors:

1. **`e*Outbox` events** (e.g., `eFilteredBlockOutbox`): emitted via P's
   `announce` keyword. Monitors (`spec`) that `observes` these events see them
   exactly once.

2. **`eObsFSM*` events** (e.g., `eObsFSMFilteredBlock`): sent via `send` to
   the test observer machine. These are NOT observed by monitors, preventing
   the duplication that occurs when P specs see both `announce` and `send` of
   the same event type.

This separation matches the darepo pattern where `eObserved*` events are used
for test assertions while `announce`d events drive monitors.

### MockChain Machine

`MockChain` models `neutrino.ChainSource` with:

- Block storage indexed by height and hash.
- Synchronous request/response for `BestBlock`, `GetHeaderByHeight`,
  `GetBlock`, `GetHeaderByHash`, and `FilterMatch`.
- Filter matching via deterministic set intersection: a block matches if any
  tx's `pays_to` set intersects the watch list.
- Subscription support: `Subscribe(height)` delivers backlog blocks from
  `height+1` to tip, then delivers new blocks added via `ExtendBlock`.

### RescanFSM Machine (5 States)

The FSM has 5 P states that mirror the Go states exactly:

**Initializing**: Buffers `AddWatchAddrs` events. On `ProcessNextBlockEvent`,
transitions to Syncing with a self-tell.

**Syncing**: Processes up to `batch_size` blocks per `ProcessNextBlockEvent`.
For each block: fetches header, checks filter match against watch list, always
emits `FilteredBlockOutbox` (even with no match), emits `BlockConnectedOutbox`.
At chain tip, transitions to Current with `StartSubscriptionOutbox` and
`RescanFinishedOutbox`. On `AddWatchAddrs` with rewind target below current
height, transitions to Rewinding.

**Current**: Processes `BlockConnectedEvent` from subscription bridge. Validates
`PrevBlock` chain link. Handles `BlockDisconnectedEvent` for reorgs (ignores if
not current tip). On `AddWatchAddrs` with rewind, emits
`CancelSubscriptionOutbox` and transitions to Rewinding.

**Rewinding**: Walks backward in batches, emitting `BlockDisconnectedOutbox` for
each height. `AddWatchAddrs` can lower the target. At target, transitions to
Syncing with `scanning = true`.

**Terminal**: All events ignored. Reached via `StopEvent` from any state.

### Actor Machine

`RescanActor` models the Go actor wrapper. Its key role is exposing the
self-tell pattern to P's nondeterministic scheduler:

1. FSM emits `eSelfTellOutbox` via announce.
2. Actor sends `eActorSelfTell` to itself (goes through P mailbox).
3. P scheduler can deliver any pending event next: the self-tell OR an
   external `eActorAddWatchAddrs`.
4. This exercises all possible interleavings that the Go goroutine scheduler
   could produce.

The actor also models the subscription bridge: it registers as the chain's
subscriber, receives `eBlockConnectedEvent` notifications, and forwards them
to the FSM.

## Safety Monitors

Seven `spec` monitors verify safety properties by observing announced events:

### 1. NoMissedTransaction

The core invariant. The FSM announces `eGroundTruthBlockProcessed` with the
watch list state and block transactions at processing time. The monitor then
checks the subsequent `eFilteredBlockOutbox` to verify that every tx paying a
watched address appears in the outbox. Any violation is a critical bug: the
wallet would have permanent UTXO invisibility.

### 2. MonotonicProgress

Heights in `eFilteredBlockOutbox` must be strictly monotonically increasing
during forward sync. Resets when transitioning from Rewinding to Syncing.

### 3. RewindCompleteness

During a rewind, each height should be disconnected exactly once. Tracked via a
`set[Height]` of observed disconnections with a duplicate assertion.

### 4. WatchListConsistency

Passive monitor tracking the cumulative set of all added addresses. Feeds into
NoMissedTransaction's ground truth checking.

### 5. SubscriptionLifecycle

`StartSubscriptionOutbox` and `CancelSubscriptionOutbox` must alternate.
Asserts on double-start (without cancel) and cancel-without-start.

### 6. FilteredBlockForEveryBlock

Every `eBlockConnectedOutbox` must be preceded by an `eFilteredBlockOutbox` at
the same height. This verifies the wallet sync state advancement invariant: the
old `notifyBlockWithFilter` called `OnFilteredBlockConnected` for every block.

### 7. FSMStateTransitionValidity

Only transitions from the design's state transition table are allowed. Asserts
that the `from` state matches the monitor's tracked current state.

## Test Corpus

11 test scenarios exercise the FSM across different dimensions:

### FSM-Only Tests (deterministic dispatch)

| Test | Scenario | Key Property |
|---|---|---|
| tcBasicSync | 5-block chain, sync to tip | Correct finished height |
| tcAddWatchRewind | Add addr after passing tx, rewind finds it | NoMissedTransaction |
| tcRewindComplete | Rewind from 10 to 3, verify 7 disconnections | RewindCompleteness |
| tcSubLifecycle | Sync→Current→Rewind→Sync→Current | Start/Cancel alternation |
| tcBatchBoundary | 7 blocks, batch_size=5, tip mid-second-batch | Accumulated outbox preserved |
| tcStopInit | Stop from Initializing | Clean terminal transition |
| tcChainExtension | Chain grows from 5→8 during sync | FSM detects new tip |
| tcRewindWithinRewind | Lower rewind target during active rewind | Target update correctness |

### Actor Tests (nondeterministic interleaving)

| Test | Scenario | Key Property |
|---|---|---|
| tcSelfTellInterleaving | AddWatchAddrs concurrent with batch processing | Tx found under all orderings |
| tcConcurrentAddWatch | Two senders, different addrs and rewind targets | Both txs found |
| tcSubscriptionNewBlocks | New blocks via subscription after sync | Blocks processed in Current |

## Results

All 11 tests pass with 0 bugs across 13,000+ explored schedules:

| Test | Schedules | Strategy |
|---|---|---|
| tcBasicSync | 100 | random |
| tcAddWatchRewind | 100 | random |
| tcRewindComplete | 100 | random |
| tcSubLifecycle | 100 | random |
| tcBatchBoundary | 100 | random |
| tcStopInit | 100 | random |
| tcChainExtension | 100 | random |
| tcRewindWithinRewind | 100 | random |
| tcSelfTellInterleaving | 10,000 | PCT(3) |
| tcConcurrentAddWatch | 1,000 | PCT(3) |
| tcSubscriptionNewBlocks | 1,000 | random |

The PCT(3) strategy (probabilistic concurrency testing with 3 priority switch
points) is specifically designed to find bugs caused by rare thread
interleavings. 10,000 schedules with this strategy and zero bugs provides high
confidence in the self-tell design.

## What the Model Verifies vs. What It Cannot

### Verified

- All state transitions are valid per the design table.
- No transaction paying a watched address is ever missed, regardless of when
  `AddWatchAddrs` arrives relative to batch processing.
- `FilteredBlockOutbox` is emitted for every block (wallet sync state).
- Rewind disconnects all blocks from current height to target.
- Subscription start/cancel lifecycle alternates correctly.
- Forward progress heights are monotonically increasing.
- Concurrent `AddWatchAddrs` from multiple senders preserves all addresses and
  finds all relevant transactions.
- Chain tip movement during sync is handled correctly.
- Rewind target lowering during active rewind works.

### Not Verified (abstracted away)

- **Go memory safety**: `WatchState.Clone()` depth. The P model uses immutable
  `set[AddrID]` which P guarantees is copy-by-value. The Go code uses slice
  `copy()` which could share underlying arrays.
- **Height type safety**: P uses uniform `int`. The Go code mixes `int32` and
  `uint32`, which could wrap at 2^31.
- **GCS filter false positives**: The model uses deterministic set intersection.
  Real GCS filters can have false positives (never false negatives), causing
  unnecessary block fetches but not missed transactions.
- **Network I/O failures**: The model's `MockChain` always responds
  successfully. The Go code handles filter fetch errors with retry logic in the
  actor wrapper.
- **Actual cryptographic validation**: Block header validation, filter
  verification, and transaction parsing are abstracted.

## Go Bridge

The `bridge/` directory contains Go code for connecting the P model to the Go
implementation:

- `trace_parser.go`: Parses P model checker trace JSON output.
- `event_mapping.go`: Maps P event names and state enum values to Go types.
- `replay_harness.go`: FSM simulator that replays traces and checks
  conformance (valid transitions, state consistency).
- `replay_test.go`: Tests for the bridge. Automatically processes any
  counterexample traces in `PCheckerOutput/`.

When the P model checker finds a bug, it writes a `*.trace.json` file. The
bridge parses this trace and replays it against the Go FSM simulator to confirm
the bug reproduces in Go. This closes the loop between the formal specification
and the implementation.

## Running the Model

### Prerequisites

- .NET 8 SDK
- P 3.0.4: `dotnet tool install --global P --version 3.0.4`

### Quick Check

```bash
./p-models/scripts/check-quick.sh
```

### Full Check

```bash
./p-models/scripts/check.sh
```

### Manual

```bash
cd p-models
p compile --pproj rescan.pproj
p check PGenerated/PChecker/net8.0/RescanFSMModels.dll -tc tcSelfTellInterleaving -s 10000 --sch-pct 3
```

### Bridge Tests

```bash
cd p-models/bridge
go test ./...
```

## Extending the Model

To add a new test scenario:

1. Add a test machine in `test/rescan_test.p` with a `start state Init`.
2. Use `eObsFSM*` events for test assertions (not raw `e*Outbox` events).
3. Add `ignore` clauses for all events in terminal states.
4. Add a `test tcName [main=TestMachineName]: assert ... in { ... };`
   declaration listing all machines and monitors.
5. Add the test case name to `scripts/check.sh`.

To add a new monitor:

1. Add a `spec MonitorName observes event1, event2 { ... }` in `monitors.p`.
2. Use `assert` for safety property violations.
3. Add the monitor to relevant `test` declarations.
