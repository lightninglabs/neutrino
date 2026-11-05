# Header Sync Driver Architecture

The `headersync` package decides what block headers to ask for, from whom,
and when a batch is ready to be written. This document describes the
package's internals: the `Driver` state machine and its executor in the
block manager, the `Manager` and the pure `HeaderSyncFSM` behind it, the
per-peer machines, the `Session` bookkeeping, the planner in `Controller`,
and the validation cache in `TipState`. It reflects the code at master
`2e8b203` (PR #446) plus the five commits of the PR that moves the driver's
handlers to returned effects, makes a pending commit a driver state, and runs
the block handler as an actor.

For how header sync fits beside filter sync, the stores and the query work
manager, see [sync-architecture.md](sync-architecture.md). That document
defines the terms used here without restating them: a _range_ is the
half-open interval `(StartHeight, StopHeight]` between two confirmed anchors,
an _anchor_ is a height and hash the planner can use as a range boundary, a
_lease_ is the `LeaseEpoch` on a range that changes when its owner changes,
and a _checkpoint_ is a trusted anchor from the chain parameters. For the
deterministic simulation harness that exercises this code, see
[deterministic-sync-testing.md](deterministic-sync-testing.md).

## Components

`Driver` is the decision maker, and `blockManager` is its executor. The
driver takes one event, reads the world through a `View`, updates the
bookkeeping it owns, and returns a list of effects. The block handler
actor then performs those effects in order: it sends `getheaders`, arms
timers, submits writes to the chain store actor, and feeds the outcome of
each back to the driver as the next event. The block handler has one mailbox,
and every input to the block manager is a message in it. The driver's policy calls go
through `Controller`, which composes the `Manager` (range and anchor state)
and the `Session` (requests, timers, staged payloads).

```mermaid
flowchart TB
    subgraph BM["blockManager (blockmanager_driver.go, blockmanager_headersync.go, blockmanager_serial.go)"]
        direction TB
        bh["block handler actor (blockmanager_actor.go)<br/>one mailbox: peer messages, deadlines,<br/>timers, donePeerMsg, chainstore.Result"]
        ex["handleDriverEvent agenda loop<br/>executeDriverEffect"]
        pend["pendingHeaderCommit<br/>one chain store mutation in flight"]
    end

    subgraph DR["headersync.Driver (driver*.go)"]
        direction TB
        drv["driverState and commitPendingState<br/>two states, protofsm machine"]
        tip["TipState (tip_state.go)<br/>headerList, reorgList, nextCheckpoint"]
        sess["Session (session.go)<br/>rangeRequests, anchorRequests,<br/>staleRequests, stagedRanges, cooldowns"]
    end

    subgraph CT["Controller (controller.go, planner.go)"]
        direction TB
        ctl["PlanCheckpointRanges, PlanFrontierRanges,<br/>AnchorDiscoverySpan, PlanAnchorRequests,<br/>FinishAnchorHeaders, InvalidateRange,<br/>CommitReadyRanges"]
    end

    subgraph MG["Manager (manager.go, fsm.go)"]
        direction TB
        mgr["managerState<br/>protofsm machine over ManagerMsg"]
        fsm["HeaderSyncFSM<br/>ranges, anchors, peers,<br/>committedTip, tipLease"]
        peer["Peer, one per peer<br/>peerReadyState ... peerDownState"]
        qd["querydispatch.Controller<br/>querysync.Scheduler"]
    end

    subgraph CS["chainstore"]
        act["chain store actor<br/>BlockCommit, Reorg, Rollback,<br/>SnapshotRequest"]
    end

    bh -- "Inbound, or a direct event" --> ex
    ex -- "ProcessEvent(event, View)" --> drv
    drv -- "[]DriverEffect<br/>(Deliver hands an Inbound back)" --> ex
    drv --> tip
    drv --> sess
    drv --> ctl
    ctl --> mgr
    ctl --> sess
    drv -- "RequestRange,<br/>CompleteRange" --> peer
    peer -- "outbox" --> mgr
    mgr --> fsm
    mgr --> qd
    ex -- "SubmitRangeCommit,<br/>CommitExtension, CommitReorg,<br/>CheckpointRollback" --> pend
    pend -- "TryAsk" --> act
    act -- "chainstore.Result,<br/>posted to the mailbox" --> bh
```

The driver never calls the chain store, a peer, or a timer. Every arrow out
of `DR` toward the network or the disk is an effect that `blockManager`
performs. The arrow back from the chain store actor is a `chainstore.Result`
that `postChainResult` puts in the block handler's mailbox (the executor
passes it as the callback of `chainstore.Client.TryAskFunc`). The block
handler turns it into a `RangesCommitted`, `ExtensionCommitted`,
`ReorgCommitted`, `CheckpointRolledBack`, `SnapshotAdopted` or
`CommitAbandoned` event.

### Responsibilities

| Type | Owns | Does not own |
| --- | --- | --- |
| `Driver` | The `TipState` validation cache, the request ID counter, the current `View`, the per-event `commitPending` flag and error; the sequencing of controller, manager, peer and session calls within one event | A goroutine, timers, peer I/O, the chain store and the candidate peer list (all in `blockManager`); sync phases (none exist in the driver) |
| `driverState` | Nothing. It is an empty struct, the driver's idle state: it dispatches on the event type through `Driver.dispatch`, writes through the `*Driver` environment, and moves to `commitPendingState` when the effects submit a chain store mutation | Any data |
| `commitPendingState` | The `Inbound` events deferred while a mutation is in flight, oldest first. It applies every other event as `driverState` does, defers `Inbound`, and replays the deferred events as a trailing `Then` when it returns to idle | Any bookkeeping; the pending commit itself, which is `pendingHeaderCommit` in `blockManager` |
| `View` | A snapshot of `blockManager` state for one `ProcessEvent` call: `Peers`, the candidate list as `PeerCandidate` values. It has no other field | Persistence across events; the executor rebuilds it per call; the pending commit, which the driver's state records |
| `blockManager` | The block handler actor and its mailbox, the candidate peer list (a `list.List` held by the `blockHandler` behavior), `pendingHeaderCommit`, every `time.AfterFunc` timer, the serial sync peer, the `getheaders` wire send, and the chain store `TryAskFunc` | Range, anchor and lease state; validation |
| `Session` | In-flight `RangeRequest` and `AnchorRequest` per peer with their `*time.Timer`, `StaleRequest` entries, `StagedRange` payloads, peer cooldowns and `nextRangeID` | Range state (in `HeaderSyncFSM`); timer creation (the executor creates timers and the session only stops them) |
| `TipState` | `headerList` (the validation chain that trails the durable store), `reorgList` (scratch chain for a competing branch) and `nextCheckpoint`; contextual checks through `HeaderValidator` | Durable storage, which it reads through `TipConfig.BlockHeaders` and never writes |
| `Controller` | Planning and recovery policy: checkpoint and frontier ranges, discovery spans, hedged peer choice, anchor confirmation, invalidation fan-out, replanning | Any state of its own; peer punishment, which the driver emits as effects |
| `Manager` | A one-state protofsm machine over `ManagerMsg` that serializes every `HeaderSyncFSM` call and mirrors peer and range changes into `DispatchController` | Policy; goroutines; the tip lease methods, which bypass the machine |
| `HeaderSyncFSM` | `ranges`, `anchors`, `checkpoints`, `peers` snapshots, `committedTip`, `commitLog`, `metrics` and `tipLease` | Header payloads, timers, I/O |
| `Peer` | One protofsm machine per peer with the peer's RTT and active `RangeAssignment` | The authoritative lease, which the FSM's range table holds |
| `querydispatch.Controller` | The private `querysync.Scheduler` that picks range owners and steal donors; range IDs map one to one onto task IDs | Validation, leases, commit order |
| `Runtime` | Construction and shutdown of the manager, session, controller, driver and the `peers` map under `Runtime.mu` | Message routing; the driver calls each part directly |

## The driver loop

`Driver.ProcessEvent(ctx, event, view)` returns `[]DriverEffect`, the
machine's outbox. The executor runs the effects in order after the call
returns. Each effect that can fail or that produces a result reports back as
a further event, so the driver never waits inside a handler.

The block handler is an lnd actor (`blockHandler` in `blockmanager_actor.go`)
with one mailbox of capacity `MaxPeers*10 + 1`. `blockManager.post` queues a
`blockInput` message for it, and `Receive` handles one at a time. A
`newPeerMsg`, `invMsg`, `headersMsg` or `headerSyncTimeoutMsg` goes through
`dispatchInbound`, which hands the driver an `Inbound{Message}` event. A
`donePeerMsg` and the bootstrap, cooldown and commit retry timer messages go
straight to `handlePeerMessage`. A `chainstore.Result` goes to
`finishAsyncHeaderCommit`. `Receive` runs an event to completion, including
the follow-ups of its effects, before it reads the next message.

```mermaid
sequenceDiagram
    autonumber
    participant P as ServerPeer
    participant BH as block handler actor (Receive)
    participant D as Driver.ProcessEvent
    participant S as driver state ProcessEvent
    participant EX as executeDriverEffect
    participant CS as chain store actor

    P->>BH: headersMsg in the mailbox
    BH->>D: ProcessEvent(ctx, Inbound{headersMsg}, driverView(peers))
    D->>S: machine.ProcessEvent under StateMachine.mu
    S-->>D: idle state: Outbox = [Deliver{headersMsg}]
    D-->>BH: effects, d.err
    BH->>EX: Deliver runs handlePeerMessage
    EX->>BH: handleHeadersMsgWithPeers builds HeadersReceived
    BH->>D: ProcessEvent(ctx, HeadersReceived, fresh driverView(peers))
    D->>S: handleHeadersReceived: Session, Controller, Manager, TipState
    S-->>D: NextState, Outbox = effects, d.err set
    D-->>BH: effects, d.err
    loop each agenda item, next item first
        alt Then{Events}
            BH->>BH: push the events on the agenda
        else SendGetHeaders
            BH->>EX: sendHeaderRequest(sp, locator, stopHash)
            EX->>P: getheaders
            EX-->>BH: RequestSent or RequestFailed, pushed on the agenda
        else ArmRequestTimer
            BH->>EX: time.AfterFunc(headerTimeoutForPeer)
            EX-->>BH: TimerArmed{Timer}, pushed on the agenda
        else SubmitRangeCommit
            BH->>EX: submitRangeCommit sets pendingHeaderCommit
            EX->>CS: TryAskFunc(BlockCommit{ExpectedParent, Headers})
            Note over D: the driver is now in commitPendingState
        end
    end
    CS-->>BH: chainstore.Result, posted to the mailbox
    BH->>D: ProcessEvent(RangesCommitted{Prepared})
    Note over D: back to idle, deferred Inbound events replay as a Then
```

`handleDriverEvent` keeps an agenda, a stack of items, where each item is
either an event for the driver or an effect to execute. It pops the next
item. An event is run through `processDriverEvent`, which gives it a fresh
`View` and its own `headerSyncOpCtx` deadline of five seconds, and pushes its
effects on the agenda in order. A `Then` effect pushes its events. Any other
effect is run by `executeDriverEffect`, which returns the events that report
its outcome (`RequestSent`, `RequestFailed`, `TimerArmed` or
`CommitAbandoned`), and those are pushed in turn. Everything an event pushed
runs before whatever followed that event on the agenda. A `Then` therefore
does not call `handleDriverEvent` again, and the driver is never entered from
inside one of its handlers or effects. A
`Deliver` effect is the one case that nests: it calls `handlePeerMessage`,
which can call `handleDriverEvent` with its own agenda, after the outer
`ProcessEvent` has returned.

Five rules make this loop safe. The code documents each, and the executor
enforces none of them at runtime.

**A re-entering effect is the last effect of its batch.** `Then{Events}`,
`StartSync`, `Deliver`, and any effect whose executor re-enters the driver
(such as `SendGetHeaders`, which reports `RequestSent`, or `SetSyncPeer`,
which goes through `setSyncPeer` and posts `FollowTip`) runs after the
effects before it. A later `ProcessEvent` therefore sees the state those
effects produced. `handleSnapshotAdopted` emits `ScheduleCommitRetry` before
the restart for this reason. The driver never uses protofsm's
`InternalEvent`; all re-entry goes through the executor.

**Every `ProcessEvent` call gets a fresh `View`.** `processDriverEvent` calls
`driverView(peers)` on every event, including each event of a `Then` and
each outcome event. The view holds only `Peers` (nil when the caller passed
no list, as `FollowTip` does). Because `DisconnectPeer` removes the peer from
the candidate list before the next event runs, a follow-up event sees it. The
view lives in the `Driver` environment rather than in the event, so a
replayed event cannot carry a stale one.

**The driver's error travels beside the effects.** Each state's
`ProcessEvent` stores the handler's error in `d.err` and always returns a nil
transition error. `Driver.ProcessEvent` returns `effects, d.err`. protofsm
discards the outbox when a transition fails, and the driver has already
dealt with the failure by logging it or scheduling a retry, so the effects it
emitted must still run. Only `PeerConnected`, `PeerDisconnected`,
`PeerRTTUpdated`, `PlanRanges` and `CommitReady` can set `d.err`, and only
`registerHeaderSyncPeer` reads the outcome's error; every other call site
discards it.

**A handler never calls `ProcessEvent`.** The machine's `StateMachine.mu` is
held for the whole call and is not reentrant, so a nested call would
deadlock. A handler that needs a follow-up emits `Then`, and the executor's
agenda loop runs it after the outer call returned.

**One commit at a time, and the driver does not read peers while it is
pending.** The driver enters `commitPendingState` when an event's effects
include `SubmitRangeCommit`, `CommitExtension`, `CommitReorg` or
`CheckpointRollback` (`submitsCommit`). It leaves the state on an event that
says how the mutation ended: `RangesCommitted`, `ExtensionCommitted`,
`ReorgCommitted`, `CheckpointRolledBack`, `SnapshotAdopted` or
`CommitAbandoned` (`endsCommit`). While in the state, `Inbound` is the one
deferred event (`defersWhileCommitPending`), so no peer message or request
deadline reaches a handler, and nothing reads the header stores that the
mutation holds, until the chain store answers. Every other event runs, with
`Driver.commitPending` set for the handlers, so `resumeWork`,
`commitReadyRanges`, `handlePeerGone`, `handleAnchorPipelined` and
`handleAnchorHeaders` defer anything that would read or move the tip.
`donePeerMsg` and the timer messages are not `Inbound` and are never held
back by the mailbox.

### The driver's two states

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> CommitPending: event emits SubmitRangeCommit,<br/>CommitExtension, CommitReorg<br/>or CheckpointRollback
    Idle --> Idle: any other event<br/>(Inbound becomes Deliver)
    CommitPending --> CommitPending: Inbound, appended to deferred
    CommitPending --> CommitPending: other event<br/>(handlers see commitPending)
    CommitPending --> CommitPending: ending event that submits<br/>the next commit, deferred kept
    CommitPending --> Idle: RangesCommitted, ExtensionCommitted,<br/>ReorgCommitted, CheckpointRolledBack,<br/>SnapshotAdopted or CommitAbandoned,<br/>trailing Then{deferred}
```

Idle is `driverState` and CommitPending is `commitPendingState`. In Idle an
`Inbound` event becomes a `Deliver{Message}` effect. In CommitPending the same
event is appended to the state's `deferred` list and produces no effects. An
ending event runs its handler as an idle driver would (`dispatch` is called
with the pending flag cleared), because the mutation has ended by then. If
that handler's effects submit another mutation, which no handler of an ending
event does today, the driver stays in CommitPending and keeps `deferred`.
Otherwise it returns to Idle and appends `Then{Events: deferred}` to the
effects, so each replayed event sees the state the effects before it
produced. `CommitAbandoned` has no case in `dispatch`; it only ends the state.
The executor sends it when the chain store refuses a command
(`Result.Err` in `finishAsyncHeaderCommit`) and when it cannot find the peer
that a `CommitExtension`, `CommitReorg` or `CheckpointRollback` effect names.

The deferred queue has no bound. The old `peerChan` filled and blocked the
peer goroutines while a commit was pending; the mailbox keeps draining, so
the queue holds every reply and message that arrives for as long as the
mutation does. In practice that is the replies to requests already in flight,
which grows only while the chain store is down and the snapshot retries.

`continueAnchorSpan` still runs while a commit is pending, because the
driver's own follow-ups (`anchorPipelined` and the like) are not `Inbound` and
are not deferred. The handler of the reply that submitted the commit runs in
Idle, so it learns of the commit from its own `submitted` result rather than
from `commitPending`.

## Events and effects

Events are the inputs to the `ProcessEvent` of `driverState` and `commitPendingState`. The table groups them
by who produces them. Types in lowercase are unexported and exist only as
`Then` follow-ups. The executor sends `Inbound` for every peer message and
request deadline; the driver turns it into a `Deliver` effect in the idle
state and defers it while a commit is pending.

| Event | Source | Declared in | Handler |
| --- | --- | --- | --- |
| `Inbound{Message}` | `dispatchInbound`, for a `newPeerMsg`, `invMsg`, `headersMsg` or `headerSyncTimeoutMsg` from the mailbox | `driver_inbound.go:9` | Idle: becomes `Deliver{Message}`. Commit pending: appended to `deferred` |
| `HeadersReceived{PeerID, PeerHeight, Headers, HeadersSynced}` | Peer reply, built by `handleHeadersMsgWithPeers` when `Deliver` runs | `driver_reply.go:16` | `handleHeadersReceived` |
| `RequestTimedOut{PeerID, Kind, RequestID, StartHeight, RangeID, LeaseEpoch, Timeout, StaleFor}` | Request timer via `handleHeaderSyncTimeoutMsg` | `driver_timeout.go:11` | `handleRequestTimedOut` |
| `SerialTimedOut{PeerID, StartHeight, Timeout}` | Serial deadline, only when `requestID` matches `serialRequestID` and the peer is the sync peer | `driver_timeout.go:154` | `handleSerialTimedOut` |
| `CooldownElapsed{SerialRequestPending}` | Cooldown wake timer (`headerSyncCooldownMsg`) | `driver_sync.go:55` | `handleCooldownElapsed` |
| `RetryCommit{}` | Commit retry timer (`headerSyncCommitRetryMsg`) | `driver_commit.go:28` | `handleRetryCommit` |
| `RequestSent{Request}`, `RequestFailed{Request, Err}` | Outcome of `SendGetHeaders`, returned by `executeDriverEffect` | `driver_dispatch.go:49`, `:57` | `handleRequestSent`, `handleRequestFailed` |
| `TimerArmed{PeerID, Kind, RequestID, Timer}` | Outcome of `ArmRequestTimer`, returned by `executeDriverEffect` | `driver_dispatch.go:66` | `handleTimerArmed` |
| `CommitAbandoned{}` | `finishAsyncHeaderCommit` when the store refused the command (`Result.Err`); `executeDriverEffect` when a commit effect names a peer it cannot find. Ends the commit pending state | `driver_commit.go:45` | No `case` in `dispatch`; the state change is its whole effect |
| `RangesCommitted{Prepared}` | Chain store result for `SubmitRangeCommit` | `driver_commit.go:34` | `handleRangesCommitted` |
| `ExtensionCommitted{PeerID, Extension}` | Chain store result for `CommitExtension` | `driver_serial.go:613` | `handleExtensionCommitted` |
| `ReorgCommitted{PeerID, Reorg, Nodes, Mismatch, Verdict}` | Chain store result for `CommitReorg` | `driver_serial.go:625` | `handleReorgCommitted` |
| `CheckpointRolledBack{PeerID, Node}` | Chain store result for `CheckpointRollback` | `driver_serial.go:648` | `handleCheckpointRolledBack` |
| `SnapshotAdopted{Node, CheckpointPeerID}` | `finishChainStoreSnapshot` after a failed mutation | `driver_serial.go:661` | `handleSnapshotAdopted` |
| `PeerConnected{Peer}` | `registerHeaderSyncPeer` from `handleNewPeerMsg` | `driver_peers.go:19` | `handlePeerConnected` |
| `PeerGone{PeerID}` | `handleDonePeerMsg` | `driver_peers.go:270` | `handlePeerGone` |
| `BeginSync{BestPeerID, BestHeight, TipHeight}` | `blockManager.startSync` after choosing the best peer | `driver_sync.go:15` | `handleBeginSync` |
| `Continue{}` | `handleNewPeerMsg` while a sync peer is set; also a `Then` follow-up | `driver_sync.go:34` | `resumeWork(startWhenIdle=true)` |
| `FollowTip{PeerID}` | `blockManager.setSyncPeer` | `driver_serial.go:47` | `handleFollowTip` |
| `ResumeWork{}`, `DispatchWork{}`, `DispatchPeer{PeerID}`, `CooldownWakeDue{}` | `Then` follow-ups | `driver_sync.go:42`, `:50`, `driver_dispatch.go:41`, `driver_peers.go:64` | `resumeWork(false)`, `handleDispatchWork`, `handleDispatchPeer`, `handleCooldownWakeDue` |
| `syncDiscovered`, `syncDispatched`, `continueDiscover`, `continueDiscovered` | `Then` follow-ups of sync start and resume | `driver_sync.go:112` to `:140` | `handleSyncDiscovered`, `handleSyncDispatched`, `handleContinueDiscover`, `handleContinueDiscovered` |
| `anchorPipelined`, `anchorPlanned`, `anchorWaiting`, `anchorReplanned`, `anchorTimedOut` | `Then` follow-ups of anchor discovery | `driver_anchor.go:25` to `:54`, `driver_timeout.go:47` | `handleAnchorPipelined`, `handleAnchorPlanned`, `handleAnchorWaiting`, `handleAnchorReplanned`, `handleAnchorTimedOut` |
| `PeerStateChanged`, `PeerDisconnected`, `PeerRTTUpdated`, `RecoverPeers`, `PeerDone`, `PlanRanges`, `CommitReady`, `TipHeaders` | Declared events with a `case` arm, but produced only by tests or called as plain functions from other handlers | `driver_peers.go`, `driver_dispatch.go:24`, `driver_commit.go:22`, `driver_serial.go:27` | As named |
| `AssignRanges{PeerCount}` | Declared at `driver_dispatch.go:32`; no `case` arm, so it is a no-op if sent as an event | | `handleAssignRanges` is called directly from `handleDispatchWork` |

Effects are the outbox. `handleDriverEvent` expands `Then` itself, and
`executeDriverEffect` in `blockmanager_driver.go` switches over every other
one; an unknown type is logged and dropped. The four commit effects
(`SubmitRangeCommit`, `CommitExtension`, `CommitReorg`, `CheckpointRollback`)
also move the driver to the commit pending state.

| Effect | Executor action | Declared in |
| --- | --- | --- |
| `Then{Events}` | `handleDriverEvent` pushes the events on its agenda; each runs in order with a fresh view, before whatever followed the `Then` | `driver_executor.go:70` |
| `Deliver{Message}` | `handlePeerMessage(peers, Message)`, which handles the message and may start a nested `handleDriverEvent` | `driver_inbound.go:18` |
| `StartSync{}` | `blockManager.startSync(peers)`, which may emit `BeginSync` | `driver_sync.go:79` |
| `SendGetHeaders{PeerID, Kind, Range, StartHeight, StartHash, StopHeight, StopHash}` | One-hash locator `[StartHash]`, `sendHeaderRequest`, then `RequestSent` or `RequestFailed` | `driver_dispatch.go:78` |
| `ArmRequestTimer{PeerID, Kind, RequestID, StartHeight, RangeID, LeaseEpoch}` | `armHeaderSyncRequestTimer` with `headerTimeoutForPeer`, then `TimerArmed` | `driver_dispatch.go:98` |
| `ArmCooldownWake{Until}` | `armHeaderCooldownWake`, a single timer, no-op if armed | `driver_peers.go:73` |
| `ScheduleCommitRetry{}`, `CancelCommitRetry{}` | One `time.Second` timer with a generation counter | `driver_reply.go:49`, `driver_commit.go:62` |
| `DisarmSerialTimeout{}` | `disarmHeaderSyncTimeout`, which bumps `serialRequestID` | `driver_serial.go:83` |
| `QuarantinePeer{PeerID}` | `cfg.QuarantinePeer` for `defaultHeaderPeerQueryQuarantine`, ten seconds | `driver_timeout.go:31` |
| `DisconnectPeer{PeerID}` | `removeSyncCandidate` and `sp.Disconnect()` | `driver_timeout.go:39` |
| `DropCandidate{PeerID}` | `removeSyncCandidate` only | `driver_anchor.go:16` |
| `DisconnectTipPeer{PeerID}` | `sp.Disconnect()` only | `driver_serial.go:62` |
| `UpdatePeerHeight{PeerID, Height}` | `sp.UpdateLastBlockHeight` | `driver_serial.go:71` |
| `SubmitRangeCommit{Prepared}` | `submitRangeCommit`: `pendingHeaderCommit.ranges`, `TryAskFunc(chainstore.BlockCommit, postChainResult)` | `driver_commit.go:53` |
| `CommitExtension{PeerID, Extension}` | `submitSerialExtension`: `pendingHeaderCommit.serial`, `TryAskFunc(chainstore.BlockCommit, postChainResult)`; returns `CommitAbandoned` if the peer is unknown | `driver_serial.go:90` |
| `CommitReorg{PeerID, Reorg, Headers, Nodes, Work, Mismatch}` | `submitSerialHeaderReorg`: `TryAskFunc(chainstore.Reorg)`, `CompareOnly` when `Mismatch` is set; returns `CommitAbandoned` if the peer is unknown | `driver_serial.go:104` |
| `CheckpointRollback{PeerID, Node, ExpectedTip}` | `submitCheckpointRollback`: `TryAskFunc(chainstore.Rollback)`; returns `CommitAbandoned` if the peer is unknown | `driver_serial.go:132` |
| `SetSyncPeer{PeerID, UnlessGone}` | `setSyncPeer(nil)` for an empty ID, else `setSyncPeer(sp)`, skipped if `UnlessGone` and the peer disconnected; `setSyncPeer` posts `FollowTip` | `driver_sync.go:65` |
| `StartSerialSync{PeerID, StartHeight, StopHash, StopHeight}` | `blockManager.startSerialSync` | `driver_sync.go:86` |
| `ContinueSerial{PeerID, FinalHash, FinalHeight, StopHash}` | `finishSerialHeaderFollowup` | `driver_serial.go:676` |
| `NoProgressReply{PeerID}` | `handleNoProgressReply`; the only effect of its event because it can call `startSync` | `driver_reply.go:58` |
| `ClearBootstrapDelay{}` | `clearHeaderSyncBootstrapDelay` | `driver_sync.go:106` |

## Manager, FSM and peers

`Manager` is a `protofsm.StateMachine[ManagerMsg, managerOutbox, *managerEnv]`
with one state, `managerState` (`"headersync-manager"`). Every message runs
synchronously on the caller's goroutine through `Manager.ask`, transitions
back to `managerState{}`, and emits one response. There is no mailbox and no
deadline check. After `Stop`, or before `Start`, `ask` returns
`ErrActorStopped`. `SetTipLease`, `TrustsTipPeer` and `TipLease` call the FSM
directly and bypass the machine. The sync's phases live in the range states
and the peer machines, not in a top-level enum.

The range lifecycle is drawn in
[sync-architecture.md](sync-architecture.md#anchors-and-ranges) with the
manager-level names. The diagram below gives the same lifecycle with the
`HeaderSyncFSM` method that performs each transition, which is what a reader
of `fsm.go` sees.

```mermaid
stateDiagram-v2
    [*] --> Queued: PlanRange<br/>(epoch 0, no owner)
    [*] --> Staged: StageDiscoveredRange<br/>(epoch + 1, owner = discoverer)
    Queued --> Staged: stageRangeFromDiscovery<br/>(epoch + 1)
    Queued --> Owned: AssignQueuedRangeToPeer<br/>(epoch + 1, LocalRanges append)
    Owned --> Active: StartPeerRange<br/>(peer Busy, epoch unchanged)
    Owned --> Owned: stealRange<br/>(epoch + 1, owner = thief)
    Active --> Owned: stealRange from Blocked<br/>or Quarantined donor (epoch + 1)
    Owned --> Queued: requeuePeerRange on RemovePeer<br/>(epoch + 1)
    Active --> Queued: requeuePeerRange on RemovePeer<br/>(epoch + 1)
    Active --> Staged: CompleteRangeAt valid,<br/>owner and epoch match
    Active --> Failed: CompleteRangeAt invalid
    Staged --> Committed: CommitReadyRanges<br/>(tip advances, trusted anchor at stop)
    Staged --> Failed: InvalidateRange (the bad range)
    Queued --> Stale: InvalidateRange or RejectAnchor<br/>on a boundary anchor (epoch + 1)
    Owned --> Stale: InvalidateRange or RejectAnchor<br/>(epoch + 1)
    Active --> Stale: InvalidateRange or RejectAnchor<br/>(epoch + 1, owner revoked)
    Committed --> [*]
    Failed --> [*]: ReplanRange under a new ID
    Stale --> [*]
```

`AdvanceCommittedTip` moves ranges without going through these arrows. A
forward move marks every range with `StopHeight <= tip` Committed and every
range that straddles the tip Stale, with no epoch change and no `commitLog`
entry. A rewind (`rewindCommittedTip`) deletes non-checkpoint anchors at or
above the new height and marks every range that stops above it Stale with an
epoch bump. Ranges are never deleted from `HeaderSyncFSM.ranges`; terminal
ranges stay in the map and `ReplanRange` allocates a new ID for the same span.

A stale completion is a `CompleteRangeAt` whose owner, epoch or Active state
does not match. It counts in `StaleCompletions` and changes nothing. The
driver checks the same three fields in `handleRangeReply` through
`SnapshotRange` before calling `CompleteRange`, so an honest peer whose lease
ended while its reply was in flight is not punished.

### Peer machines

Each `Peer` is a `protofsm.StateMachine[PeerMsg, peerOutbox, *peerEnv]`. Its
states carry the RTT and, when busy, the active `RangeAssignment`. Each
transition emits an outbox that `applyPeerOutbox` turns into a `Manager`
call, so the FSM's `PeerSnapshot` follows the machine.

```mermaid
stateDiagram-v2
    [*] --> Ready: Peer.Start sends PeerReadyEvent
    Ready --> Busy: PeerRangeAssignedEvent<br/>(after StartPeerRange)
    Ready --> Blocked: PeerBlockedEvent
    Ready --> Quarantined: PeerQuarantinedEvent
    Ready --> Down: PeerDownEvent
    Busy --> Ready: PeerRangeCompletedEvent<br/>(range and epoch match)
    Busy --> Ready: PeerRangeStolenEvent<br/>(range and old epoch match)
    Busy --> Ready: PeerReadyEvent (drops active range)
    Busy --> Blocked: PeerBlockedEvent (keeps active range)
    Busy --> Quarantined: PeerQuarantinedEvent (keeps active range)
    Busy --> Down: PeerDownEvent (keeps active range)
    Blocked --> Ready: PeerReadyEvent, no active range
    Blocked --> Busy: PeerReadyEvent, active range held
    Quarantined --> Ready: PeerReadyEvent, no active range
    Quarantined --> Busy: PeerReadyEvent, active range held
    Down --> Ready: PeerReadyEvent
    Blocked --> Quarantined: PeerQuarantinedEvent
    Quarantined --> Blocked: PeerBlockedEvent
```

The state names in the code are `peerReadyState`, `peerBusyState`,
`peerBlockedState`, `peerQuarantinedState` and `peerDownState`. Only Ready
starts work: `PeerNeedRangeEvent` in Ready emits `peerStartRangeOutbox`,
which calls `Manager.StartPeerRange` and then re-enters the machine with
`PeerRangeAssignedEvent`. In every other state the need events emit only a
snapshot. `PeerRangeCompletedEvent` and `PeerRangeStolenEvent` in Blocked,
Quarantined or Down clear the active range and keep the state. In
production, `PeerReadyEvent` after the initial one comes only from cooldown
recovery (`Runtime.UpdatePeerState(PeerReady)`), and `PeerDownEvent` has no
production sender.

The driver maps every state change through `setPeerState`, which records a
cooldown of `DefaultPeerCooldown` (two minutes) in the session for a Blocked
or Quarantined peer, emits `ArmCooldownWake` for the earliest expiry, and
calls `Runtime.UpdatePeerState`. `handleRecoverPeers` returns a peer to
Ready once its cooldown has expired and it holds no active request.

### Tip lease

`TipLease{PeerID, Epoch}` in `HeaderSyncFSM` names the one peer whose tip
headers the driver trusts before the node is synced. `SetTipLease` is a
no-op for the same holder and otherwise sets the holder and increments
`Epoch`; an empty ID releases it. The holder is always the executor's sync
peer: `blockManager.setSyncPeer` stores the `syncPeer` atomic pointer and
then posts `FollowTip{PeerID}`, whose handler calls `manager.SetTipLease`.
The lease has no expiry. It is granted by `SetSyncPeer{PeerID}` from
`dispatchSync` or a reorg winner, and by the executor's `startSerialSync`.
It is released by `SetSyncPeer{}` from `restartSync`, the anchor handlers
when discovery stalls, `handleSerialTimedOut`, `handlePeerGone` and
`handleSyncDispatched`, and by the executor on a send failure or a
no-progress reply. `Epoch` is written but has no reader outside tests.

## Sync start and dispatch

`blockManager.startSync` picks the best candidate (height, then RTT, then
address), applies the bootstrap delay (`headerSyncBootstrapMinPeers`, three
peers, or `headerSyncBootstrapMaxWait`, two seconds, before the first
lease), and emits `BeginSync`. It returns at once if `pendingHeaderCommit` is set or
a sync peer is already set. The driver then chooses between the parallel
and serial paths.

```mermaid
flowchart TB
    begin["BeginSync{BestPeerID, BestHeight, TipHeight}"]
    serial["startSerialSync<br/>StartSerialSync effect,<br/>stop at next checkpoint"]
    plan["planRanges<br/>ReconcileWithStore,<br/>PlanCheckpointRanges or PlanFrontierRanges"]
    disc["startDiscovery<br/>AnchorDiscoverySpan or FrontierDiscoverySpan,<br/>PlanAnchorRequests"]
    dsync["dispatchSync<br/>SetSyncPeer{Best},<br/>Then{DispatchWork, syncDispatched}"]
    dw["handleDispatchWork<br/>handleRecoverPeers, AssignQueuedRanges,<br/>Then{DispatchPeer per candidate}"]
    dp["handleDispatchPeer<br/>Peer.RequestRange, else RequestStolenRange<br/>SendGetHeaders{RequestRange}"]
    judge["handleSyncDispatched"]
    keep["ClearBootstrapDelay"]

    begin -- "BestHeight == 0 or SerialFallback" --> serial
    begin --> plan
    plan -- "error" --> serial
    plan -- "planned > 0" --> dsync
    plan -- "planned == 0" --> disc
    disc -- "Then{syncDiscovered}" --> dsync
    dsync --> dw --> dp --> judge
    judge -- "active work or planned > 0" --> keep
    judge -- "nothing in flight" --> serial
```

1. **Plan.** `planRanges` reconciles `TipState` with the store, reads the
   store tip and calls `Controller.PlanCheckpointRanges` while a checkpoint
   lies ahead, or `PlanFrontierRanges` past the last one. Both call
   `AdvanceCommittedTip` first, then `PlanConfirmedAnchorRanges`, which
   creates one Queued range per adjacent pair of confirmed anchors. A range
   is at most `MaxRangeHeaders` (`DefaultMaxRangeHeaders`, 2,000) headers.
2. **Discover.** If nothing can be planned, `startDiscovery` picks a span
   and asks peers for it (see the next section). A running discovery counts
   as one planned unit in `handleSyncDiscovered`.
3. **Dispatch.** `handleDispatchWork` recovers cooled peers, then
   `Controller.AssignQueuedRanges` loops `Manager.AssignNextQueuedRange` up
   to `peerCount * MaxPeerOutstandingRanges` (`DefaultMaxPeerOutstandingRanges`,
   4) times. With dispatch set, which production always does,
   `assignRangeWithDispatch` takes the lowest queued range, asks
   `querydispatch.Controller.AssignRange` (a `querysync.AssignLocalTask`)
   for the owner, and calls `AssignQueuedRangeToPeer`. `DispatchPeer` then
   runs once per candidate, in candidate order, because a failed send
   blocks a peer and changes what the next one may steal.
4. **Request.** `handleDispatchPeer` skips a peer with a range or anchor
   request in flight. Otherwise `Peer.RequestRange` starts the peer's first
   owned range. If it has none, `Peer.RequestStolenRange` asks
   `Manager.StealNextRange`, which tries `querydispatch.Controller.StealRange`
   first and falls back to the FSM's `StealNextRange`. An active range can
   be stolen only from a Blocked or Quarantined donor, and only then does
   `markRangeStolen` send `PeerRangeStolenEvent` to the donor. On success
   the driver emits `SendGetHeaders{Kind: RequestRange}`.
5. **Track.** `RequestSent` increments `requestID`, calls
   `Session.TrackRange` and emits `ArmRequestTimer`. `TimerArmed` attaches
   the `*time.Timer` through `AttachRangeTimer`, or stops it if the request
   is already gone. `RequestFailed` blocks the peer.

`Continue` and `ResumeWork` run `resumeWork`, which returns at once while
`Driver.commitPending` is set, otherwise plans from `view.bestHeight()` and emits
`Then{DispatchWork, continueDiscover}`. With `Continue`, `continueDiscovered`
also calls `restartSync` (`SetSyncPeer{}` then `StartSync`) when nothing is
in flight, so a stalled sync picks a new peer. `CooldownElapsed` chooses
`ResumeWork` instead when a serial request is pending, so the serial peer is
not replaced.

## Anchor discovery

Past the last confirmed anchor the planner has nothing to plan, so the
driver asks peers for headers from the furthest confirmed anchor and uses
the last header of each reply as a new anchor. `Controller.AnchorDiscoverySpan`
picks the span inside a checkpoint interval when `stop - start` exceeds
`MaxRangeHeaders`, with `StopHash` set to the checkpoint hash.
`FrontierDiscoverySpan` and `FrontierLookaheadSpan` pick a span of at most
`MaxRangeHeaders` past the last checkpoint with a zero `StopHash`.

`Controller.PlanAnchorRequests` hedges the span across up to
`AnchorRequestFanout` peers, 4 in `ProductionConfig`, counting the requests
already in flight for the same span (`Session.MatchingActiveAnchors`).
Eligible peers have `Height >= StopHeight`, are not excluded, and hold no
range or anchor request. A reply confirms its anchor once
`AnchorConfirmationsRequired` peers report the same hash, 1 in production
(`DefaultConfig` requires 2), so a single structurally valid reply confirms.

```mermaid
flowchart TB
    recv["handleAnchorHeaders<br/>Controller.FinishAnchorHeaders"]
    err["ScheduleCommitRetry,<br/>Then{Continue}"]
    mism["ErrRangeStartMismatch:<br/>waitForAnchor from start"]
    conflict["conflictAnchor<br/>RejectAnchor, no punishment,<br/>replanAnchor"]
    reject["rejectAnchor<br/>QuarantinePeer, DropCandidate,<br/>replanAnchor excluding peer"]
    stale["stale completion<br/>PruneAnchorsAtOrBefore(tip)"]
    staged["Session.StageRange,<br/>commitReadyRanges"]
    pipe["pipelineFrontier<br/>Then{anchorPipelined}"]
    cont["continueAnchorSpan<br/>waitForAnchor from reply boundary,<br/>Then{anchorWaiting}"]
    plan["planAfterAnchor<br/>planRanges(StopHeight),<br/>Then{DispatchWork, anchorPlanned}"]
    wait["waitForAnchor excluding reporter<br/>(not yet confirmed)"]
    fin["finishAnchorPlan<br/>continueAnchorSpan, else restartSync when idle"]

    recv -- "controller error" --> err
    recv -- "rejected, start mismatch" --> mism
    recv -- "rejected, conflict" --> conflict
    recv -- "rejected, other" --> reject
    recv -- "Height <= store tip" --> stale
    recv -- "Staged" --> staged
    staged -- "confirmed, no checkpoint ahead" --> pipe
    staged -- "commitPending or submitted" --> cont
    staged --> plan
    recv -- "not staged" --> plan
    plan -- "not confirmed" --> wait
    plan --> fin
    pipe -- "commitPending" --> cont
    pipe --> plan
```

`Controller.FinishAnchorHeaders` matches the reply to the peer's
`AnchorRequest` by `headers[0].PrevBlock == StartHash`, runs
`ValidateAnchorResponse` (non-empty, at most `DefaultMaxRangeHeaders`
headers, connected, and trusted only when the last header lands on
`StopHeight` with the expected hash), then `Manager.AddAnchor`. When the
anchor is confirmed, `StageDiscoveredRange` stages the reply's span directly
unless that span already exists as Staged or Committed or overlaps live
work; an existing Queued, Owned or Active range for the same span is
converted to Staged with an epoch bump.

`continueAnchorSpan` is what keeps discovery moving while a commit is
pending or has just been submitted. When the reply stopped short of `StopHeight`, it calls
`waitForAnchor` from the reply's last header to the same stop and returns
true. It reads no tip and plans nothing, so it is safe to run while
`Driver.commitPending` is set, and `handleAnchorHeaders` and
`handleAnchorPipelined` run only it in that case. The reply that staged the
range and submitted the commit was handled in the idle state, where
`commitPending` is false, so `handleAnchorHeaders` also takes this branch when
`commitReadyRanges` reports `submitted`. The `anchorPipelined` follow-up runs
in the commit pending state and sees the flag. The driver's own follow-ups
are not `Inbound`, so the state does not defer them. Planning waits for the
commit result: `Continue` runs once `RangesCommitted` lands and plans then. Without this, the rest of the span would be left to
the range planned after the commit, which is often assigned to a peer whose
hedged request for the same span is still in flight.

`handleAnchorWaiting`, `handleAnchorReplanned` and `handleAnchorTimedOut` run
after the follow-up round has been sent. Each returns if `discoveryActive`
reports that the round's requests are still in flight. Otherwise they
release the lease with `SetSyncPeer{}` when the sync peer has no anchor
work left, and `handleAnchorReplanned` and `handleAnchorTimedOut` emit
`StartSync`. An anchor timeout quarantines the peer, emits `DisconnectPeer`,
and re-discovers from the same request excluding that peer.

## From a range reply to a durable commit

This is the main pipeline. A `headers` message from a peer becomes a
`HeadersReceived` event, and `handleHeadersReceived` tries it as an anchor
reply, then a range reply, then an empty reply (`NoProgressReply`), and
otherwise hands it to `handleTipHeaders` for the serial path. The pipeline
has two halves: a reply becomes a Staged range, and Staged ranges become a
durable commit. The first diagram covers the reply.

```mermaid
flowchart TB
    recv["HeadersReceived<br/>handleRangeReply"]
    match["Session.FinishRangeHeaders<br/>first PrevBlock is StartHash"]
    stalerep["handleStaleRangeReply<br/>Session.PopStale, Then Continue"]
    snap["Manager.SnapshotRange<br/>state Active, owner, epoch match"]
    stalec["stale completion<br/>Then Continue"]
    val["ValidateHeaderRange<br/>exact count, connected,<br/>ends on StopHash"]
    lag["lagging: PeerBlocked"]
    contra["StopAnchorContradicted:<br/>PeerBlocked, rejectUntrustedAnchor"]
    bad["rejectRangePeer<br/>QuarantinePeer, DisconnectPeer,<br/>replanRange"]
    complete["Peer.CompleteRange<br/>CompleteRangeAt: Active to Staged"]
    stage["Session.StageRange<br/>then the commit step"]

    recv --> match
    match -- "no request" --> stalerep
    match --> snap
    snap -- "mismatch" --> stalec
    snap --> val
    val -- "empty and peer behind" --> lag
    val -- "contradicts stop anchor" --> contra
    val -- "invalid" --> bad
    val --> complete
    complete -- "error or stale" --> stalec
    complete --> stage
```

The second diagram starts from the Staged ranges and ends at the durable
tip.

```mermaid
flowchart TB
    ready["commitReadyRanges<br/>ReconcileWithStore, Manager.ReadyRanges,<br/>trimDurableRange"]
    wait["wait: commit pending<br/>or nothing ready"]
    prep["TipState.PrepareRangeHeaders<br/>PoW, difficulty, timestamps,<br/>checkpoint hash"]
    submit["SubmitRangeCommit<br/>pendingHeaderCommit set"]
    inval["Controller.InvalidateRange<br/>rejectInvalidCommit"]
    retry["ScheduleCommitRetry<br/>1 s, RetryCommit"]
    store["chain store actor<br/>BlockCommit: ExpectedParent, Headers"]
    snapshot["SnapshotRequest, retry each 1 s<br/>SnapshotAdopted: TipState.Reset,<br/>AdvanceCommittedTip"]
    landed["RangesCommitted<br/>TipState.FinishRange,<br/>Controller.CommitReadyRanges,<br/>DeleteStagedRange, Then Continue"]

    ready -- "commit pending or none" --> wait
    ready --> prep
    prep -- "ValidationError" --> inval
    prep -- "other error" --> retry
    prep --> submit --> store
    store -- "Reply.Err: ambiguous" --> snapshot
    store -- "Result.Err: never ran" --> retry
    store --> landed
    retry --> ready
    snapshot --> retry
```

1. **Match.** `Session.FinishRangeHeaders` consumes the peer's
   `RangeRequest` when the first header's `PrevBlock` equals the
   assignment's `StartHash` (an empty reply always matches) and stops its
   timer. A reply with no request goes to `handleStaleRangeReply`, which pops
   the `StaleRequest` left by a timeout, logs it, and emits `Then{Continue}`
   only when the session has no active work. Stale headers are never staged.
2. **Check the lease.** `Manager.SnapshotRange` returns the range. If it is
   not `RangeActive`, the owner is not this peer, or the `LeaseEpoch`
   differs from the request's, the completion is stale and the driver emits
   `Then{Continue}` without touching the FSM.
3. **Validate structure.** `ValidateHeaderRange` checks that the first
   header connects to `StartHash`, the headers connect to each other, and
   the last hash is `StopHash`; `handleRangeReply` also requires the exact
   count. A longer reply whose header at the expected index hashes to
   `StopHash` is trimmed first. An empty reply from a peer whose height is
   below `StopHeight` marks the peer `PeerBlocked`. A reply that
   `Controller.StopAnchorContradicted` finds to hold a different hash at
   `StopHeight` blocks the peer and, after completion, rejects the untrusted
   stop anchor. Any other invalid reply goes through `rejectRangePeer`
   (quarantine for `defaultHeaderPeerQueryQuarantine`, ten seconds, and
   `DisconnectPeer`) and `replanRange`.
4. **Complete.** `Peer.CompleteRange` runs the peer machine's
   `PeerRangeCompletedEvent`, whose outbox calls `Manager.CompleteRange` and
   `HeaderSyncFSM.CompleteRangeAt`. A valid completion moves the range to
   `RangeStaged` and the peer back to Ready. A completion error schedules a
   commit retry and blocks the peer.
5. **Stage.** `Session.StageRange` keeps the header pointers under the range
   ID. Ranges stage out of order, because peers answer at different speeds.
6. **Commit.** `commitReadyRanges` returns at once if `Driver.commitPending`.
   It calls `TipState.ReconcileWithStore` so a write that reported an error
   but landed is picked up, then `Manager.ReadyRanges`, which walks the
   contiguous Staged ranges from `committedTip`. With nothing ready it prunes
   retired staging and emits `CancelCommitRetry`. For each ready range,
   `trimDurableRange` drops the prefix that `cfg.PublishedTip` already
   covers, by comparing the stored header at the boundary with the staged
   one. `TipState.PrepareRangeHeaders` then runs `HeaderValidator.Check` on
   every header over a `ValidationChain` overlay (difficulty and retarget,
   median time, proof of work against `PowLimit`, timestamp against
   `TimeSource`) and compares the hash at `nextCheckpoint`. A
   `ValidationError` names the range and peer at fault; any other error is
   a store or lookup failure.
7. **Submit.** On success the driver emits `SubmitRangeCommit{Prepared}` and
   moves to the commit pending state. The executor sets
   `pendingHeaderCommit.ranges` and calls
   `chainStore.TryAskFunc(chainstore.BlockCommit{ExpectedParent, Headers},
   postChainResult)`, which returns at once.
   `PreparedCommit` holds value copies of the headers, so the chain store
   actor can read them on its own goroutine.
8. **Land.** `Receive` passes the `chainstore.Result` from the mailbox to
   `finishAsyncHeaderCommit`. A `Result.Err` means the command never ran; the
   pending commit is cleared, a retry is scheduled, and the executor sends
   `CommitAbandoned` so the driver leaves the commit pending state. A `Reply.Err` means the write may have applied
   in part; the executor marks the pending commit `snapshot` and sends
   `chainstore.SnapshotRequest`, retrying every second until the store
   reports a valid block tip, then posts `SnapshotAdopted`, whose handler
   resets `TipState`, advances the FSM tip and schedules a commit retry. The
   snapshot retry is a `time.AfterFunc` that posts `chainstore.Result{Retry:
   true}` to the mailbox. On
   success the executor clears the pending commit and runs
   `RangesCommitted{Prepared}` (or the event for the serial, reorg or
   rollback command that was in flight) through `handleDriverEvent`.
9. **Acknowledge.** `handleRangesCommitted` calls `TipState.FinishRange`
   (pushes the nodes into `headerList` and moves the checkpoint cursor) and
   `Controller.CommitReadyRanges`. The FSM drains every contiguous Staged
   range, marks each `RangeCommitted`, moves `committedTip`, adds a trusted
   anchor at each stop, and prunes anchor requests at or below the tip. The
   driver deletes the staged payloads, emits `CancelCommitRetry` and
   `Then{Continue}`.

When `PrepareRangeHeaders` returns a `ValidationError`,
`Controller.InvalidateRange` marks the range Failed, deletes every
untrusted anchor up to the next trusted one, marks dependent ranges Stale,
discards their staged payloads and requests (marking range requests stale
until `now + RequestTimeout()`), and sends `PeerRangeStolenEvent` to each
revoked owner. `rejectInvalidCommit` then quarantines and disconnects the
peer named in the error, if it is still a candidate, and emits
`Then{Continue}`.

The commit retry is one `time.Second` timer kept by
`scheduleHeaderCommitRetry` with a generation counter, so a cancelled or
superseded timer that already fired is ignored. Staged payloads stay in the
session across the retry, so a recovered store does not need a new network
reply to drain staging.

## Tip following and the serial path

The serial path asks one peer for headers after a block locator, one round
trip at a time. The driver enters it from `handleBeginSync` when
`BestHeight == 0` or `cfg.SerialFallback` is set (simnet and regtest), when
planning fails, or when `handleSyncDispatched` finds neither planned nor
active work. There is no explicit switch back: every `StartSync` re-runs
`BeginSync`, which takes the parallel path whenever it can plan or discover.
After the node has caught up, new blocks announced by `inv` go through
`handleInvMsg`, which sends `getheaders` with a locator built from the
driver's `Tip()` and the store's `LatestBlockLocator` and arms the serial
deadline.

```mermaid
flowchart TB
    th["TipHeaders{PeerID, Headers}<br/>holdsLease = TrustsTipPeer"]
    conn["HeadersConnected"]
    disc["DisconnectTipPeer"]
    ext["extendSerialTip<br/>headersAfterTip,<br/>TipState.PrepareSerialExtension"]
    cpm["SerialCheckpointMismatch:<br/>planCheckpointRollback<br/>CheckpointRollback effect"]
    commit["UpdatePeerHeight, DisarmSerialTimeout (lease holder),<br/>CommitExtension"]
    fork["followSerialFork<br/>locateFork: known, unknown parent,<br/>below previous checkpoint"]
    ignore["ignore (no lease, not synced)"]
    vf["validateFork<br/>ResetReorg, CheckReorgHeader,<br/>CalcWork"]
    pr["planReorg<br/>CommitReorg{Headers, Work}<br/>or CommitReorg{Mismatch, CompareOnly}"]
    verdict["ReorgCommitted{Verdict}"]
    won["SetSyncPeer{peer, UnlessGone},<br/>TipState.Reset + Extend,<br/>advanceTip, ContinueSerial"]
    lost["ReorgLessWork or EqualWork:<br/>DisconnectTipPeer or restartSyncIfIdle"]
    landed["ExtensionCommitted<br/>TipState.Extend, advanceTip,<br/>ContinueSerial"]

    th --> conn
    conn -- "false" --> disc
    conn --> ext
    ext -- "checkpoint mismatch" --> cpm
    ext -- "other error" --> disc
    ext -- "extends tip" --> commit --> landed
    ext -- "does not extend" --> fork
    fork -- "no lease and not synced" --> ignore
    fork -- "unknown parent or too deep" --> disc
    fork --> vf
    vf -- "rule failure" --> disc
    vf --> pr --> verdict
    verdict -- "NotRejected" --> won
    verdict -- "NotRejected with Mismatch" --> cpm
    verdict --> lost
```

`handleTipHeaders` is a chain of `fn.Result` steps: `requireConnected` checks
`HeadersConnected`, then `applyTipHeaders` tries `extendSerialTip` and falls
through to `followSerialFork` when the batch does not extend the tip.
`tipFailureEffects` is the one place an error becomes effects: a
`SerialCheckpointMismatch` plans a checkpoint rollback, `errIgnoreTipHeaders`
produces nothing, and any other error is `DisconnectTipPeer`. `headersAfterTip` drops the leading headers the cache
already holds, and `TipState.PrepareSerialExtension` validates the rest over
a `ValidationChain` overlay, stopping at `nextCheckpoint` so headers beyond
the checkpoint in the same message are re-requested by `ContinueSerial`. The
extension does not check the lease: any peer's connecting batch is accepted.
`DisarmSerialTimeout` is emitted only for the lease holder, since the
deadline belongs to that peer's request. `SerialExtension` holds its own
header copies because the write completes on the chain store goroutine.

A batch that does not extend the tip goes to `followSerialFork`.
`locateFork` fails with `errIgnoreTipHeaders` when the sender holds no lease
and the node is not synced, and fails with an error that disconnects the peer
when the parent is unknown or below `FindPreviousCheckpoint(tip)`.
Otherwise `validateFork` checks each
header against `reorgList` while summing `blockchain.CalcWork`. `planReorg`
emits `CommitReorg` with the branch up to and including the next checkpoint,
or with `Mismatch` set and no headers when the branch has the wrong hash at
the checkpoint height; the executor then sends `chainstore.Reorg` with
`CompareOnly`. The `ReorgVerdict` decides the outcome: `ReorgLessWork`, or
`ReorgEqualWork` with a mismatch, disconnects the peer; `ReorgEqualWork`
alone restarts sync when idle; `ReorgNotRejected` with a mismatch triggers
a checkpoint rollback; and a won reorg makes the peer the sync peer,
resets the cache to the fork and extends it with the new nodes.

A checkpoint rollback (`planCheckpointRollback`) fetches the header at
`FindPreviousCheckpoint` and emits `CheckpointRollback{Node, ExpectedTip}`,
which becomes `chainstore.Rollback`. `CheckpointRolledBack` resets the
cache, advances the FSM tip, disconnects the peer and restarts sync when
idle. If the checkpoint header cannot be fetched, the driver emits only
`ScheduleCommitRetry`.

The serial deadline is armed by `requestHeaders` with
`headerTimeoutForPeer`: the configured `HeaderSyncTimeout` or
`defaultHeaderSyncTimeout` (30 seconds), raised to `headerSyncRTTMultiplier`
(8) times the peer's RTT and capped at `maxHeaderSyncTimeout` (two minutes).
The same function sizes range and anchor request timers. A
`headerSyncTimeoutMsg` becomes `SerialTimedOut` only when its `requestID`
equals `serialRequestID`, the peer is the sync peer and the node is not
synced. `handleSerialTimedOut` ignores it if the store tip has moved past
`StartHeight`; otherwise it quarantines and disconnects the peer, releases
the lease if held, resets the cache to the store and emits `StartSync`. It
has no commit pending branch, because a `SerialTimedOut` reaches the driver
as an `Inbound` message and waits while a commit is in flight.

## Concurrency

Every piece of mutable state in this package has one owner. Timers and the
chain store actor run on other goroutines and reach the driver only as
messages in the block handler's mailbox.

| Goroutine | Owns |
| --- | --- |
| Block handler actor (`blockHandler.Receive`) | The candidate peer list, `pendingHeaderCommit`, `headerSyncTimer`, `serialRequestID`, the commit retry, cooldown and bootstrap timers with their generation counters; the only caller of `Driver.ProcessEvent` |
| Block handler actor, through the driver | `TipState`, `Session` (all maps and `nextRangeID`), `Driver.requestID`, `Driver.view`, `Driver.commitPending`, `Driver.err`, the state's `deferred` list; `HeaderSyncFSM` through `Manager.ask`; each `Peer` machine; the `querysync.Scheduler` inside `querydispatch.Controller` |
| `time.AfterFunc` callbacks | Nothing. A request timer posts a `headerSyncTimeoutMsg`; the cooldown, commit retry and bootstrap timers post their timer messages; the snapshot retry posts a `chainstore.Result{Retry: true}`. All go through `blockManager.post` into the mailbox |
| Chain store actor | The durable headers and the published tip. It reads value copies in `PreparedCommit` and `SerialExtension` and answers through the `TryAskFunc` callback, which calls `postChainResult` |
| Other goroutines | `blockManager.syncPeer` as an atomic snapshot through `SyncPeer()`; `TipState` through test-only accessors (`SessionForTest`, `TipForTest`), which race with `ProcessEvent` and are not for production |

`Session` has no mutex, `Manager` takes no lock of its own, and `TipState`
has none either; the comment on each names the block handler as the one
goroutine that touches it. `Runtime.mu` guards only the `peers` map and is
held across every `Peer.Stop` in `Runtime.Stop`, which runs after the block
handler has stopped. The `StateMachine.mu` inside the driver, the manager and
each peer serializes one `ProcessEvent` at a time, and the executor's
re-entry always happens after the outer call returned.

The mailbox is first in, first out, so a reply that was queued before a
request deadline is handled before it. The old bookkeeping that held a
deadline behind queued peer messages (`queuedBeforeTimeout`) no longer
exists. While a commit is pending, the driver defers the `Inbound` messages
that reach it, so a reply that arrives just after a deadline is still handled
before the peer is punished, in arrival order. A fired timer that was
cancelled or superseded is voided by the generation counter it carries, or by
`serialRequestID` for the serial deadline, so the driver never acts on a
timeout for a request it does not track. `Session.TimeoutRange` and
`TimeoutAnchor` match on request ID (and range ID and epoch for ranges)
before they consume anything.

The commit pending state is the driver's only knowledge of the pending
commit. Any handler that would plan from the tip, read the store tip, or
submit a second write checks `Driver.commitPending` and defers. A `Then`
follow-up cannot be used to get around this, because each follow-up is a
separate `ProcessEvent` call that runs in whichever state the effects before
it left the driver.

## Known gaps

- The querysync owner-selection and steal policies behind
  `querydispatch.Controller.AssignRange` and `StealRange` were not read. The
  mirror between `HeaderSyncFSM.peers` and the scheduler is only partially
  kept: `AdvanceCommittedTip`, `StageDiscoveredRange`, `RemovePeer` and the
  dispatch steal path change FSM peer state without a dispatch update, and
  `toQueryPeer` passes an empty, non-nil `LocalRanges`, which resets the
  scheduler's local work for that peer. The effect on assignment was not
  traced.
- A `Peer` machine whose active range was detached by `AdvanceCommittedTip`,
  discovery staging or `RemovePeer` is not notified, so it stays in
  `peerBusyState` until a cooldown recovery sends `PeerReadyEvent`. Whether
  another path clears this was not confirmed.
- `Controller.CommitReadyRanges` drains every contiguous Staged range, not
  only the ranges in the `PreparedCommit` that was written. This is correct
  only because no range can be staged while the driver is in the commit
  pending state, which rests on the state deferring `Inbound` (a range reply
  is staged only from a `HeadersReceived` that `Deliver` produces); nothing
  in `Controller` or `Manager` checks it.
- The `ErrRangeStartMismatch` branch of `handleAnchorHeaders` appears
  unreachable, because `Session.FinishAnchorHeaders` already requires the
  same first-parent match before the controller validates the reply.
- `handleAnchorWaiting` emits `StartSync` only when `cfg.SerialFallback` is
  set, and `handleBeginSync` never takes the parallel path in that case, so
  the branch may be unreachable in production.
- `planCheckpointRollback` on a fetch failure, and a serial admission
  failure (`Result.Err`, which also sends `CommitAbandoned`), schedule only a
  commit retry. The recovery path
  from there (`RetryCommit`, `Continue`, `restartSync` when idle) was not
  traced end to end.
- Several comments name an older division of labour: `ValidateHeaderRange`
  says contextual checks belong to the block manager, `lightChainCtx` says
  checkpoints are checked in `handleHeadersMsg`, and `PrepareSerialExtension`
  says checkpoint rejection stays on another path. The code places all three
  in `TipState`.
- `TipLease.Epoch`, `driverOutcome.sent` and `HeaderSyncFSM.rangeKeys` are
  written and never read in non-test code. `HeaderSyncFSM.ranges` and
  `commitLog` grow without bound, and `Session.staleRequests` has no sweep.
- The chain store's handling of a `BlockCommit` whose `ExpectedParent` does
  not match the durable tip was not read; the document assumes it surfaces
  as `Reply.Err` and therefore takes the snapshot path.
