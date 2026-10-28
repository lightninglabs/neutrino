# Deterministic Sync Testing

The sync tests combine P scenarios, Go bridge fixtures, virtual-time actor
tests, and a seeded production-path simulator (the header-sync DST). They
cover different boundaries; a passing model check does not prove header
validation or SQL crash recovery.

## Run And Replay

```sh
# Fixed corpus and every regression seed, each run twice for determinism.
go test . -run '^TestHeaderTransportDST$' -count=1

# A reproducible swarm campaign. -dst.seed picks the generator, -dst.swarm
# the number of seeds. A failure prints the seed and the run count.
go test . -run '^TestHeaderTransportDSTSwarm$' -dst.swarm=50000 -dst.seed=7

# Replay one seed, with block manager logs.
go test . -run '^TestHeaderTransportDSTReplay$' -dst.replay=<hex> -dst.log

# Coverage-guided fuzzing over the same runner.
go test . -run '^$' -fuzz '^FuzzHeaderTransportDST$' -fuzztime=10m

# Fixed regressions and the small FSM-level story.
go test . -run '^TestHeaderTransport|^TestHeaderSyncDSTExhaustive$'
go test ./headersync/... ./querysync/... -count=1
```

`FuzzHeaderTransportDST` writes a failing input under
`testdata/fuzz/FuzzHeaderTransportDST`; replay it with
`go test . -run 'FuzzHeaderTransportDST/<file>'`. The fuzz body re-runs one
input in sixteen to check determinism; the fixed corpus is always run twice.

The `dst-nightly` workflow fuzzes for a bounded time with a cached corpus and
runs four swarm campaigns whose generator seed derives from the run number.

## What The Runner Drives

`blockmanager_transport_dst_test.go` builds the real block manager, its
headersync runtime and querysync dispatch actor over in-memory header stores
(`dst_store_test.go`), inside a `testing/synctest` bubble. Peers connect and
leave through the production new-peer and done-peer handlers. Production
getheaders requests go through the internal `headerTransport`; replies are
encoded, decoded, and queued through `QueueHeaders`, then handled by the same
serialized `handlePeerMessage` entry point as the block handler. Production
request timers, commit retries and the bootstrap timer expire in virtual time.
The harness never calls commit, steal or ownership changes itself.

Chains are mined once per process (`dst_fixture_test.go`): the honest chain,
a heavier reorg branch, and for every start height a PoW-valid fork, a chain
whose first header's timestamp is not after median time past, and a chain
with an unexpected difficulty. The fixture panics if any two mined headers
share a hash.

One run costs well under a millisecond of CPU. Before the in-memory stores
and the shared fixture, bbolt setup was 95% of each run and fuzzing managed
about 0.2 executions per second.

## Swarm Configuration

The first three seed bytes select a mode and the set of enabled fault classes;
each later byte is one op. An op whose class is disabled degrades to an honest
reply, so every seed still makes progress. Modes:

- `checkpoint_all`: a checkpoint every two heights, all ranges pre-planned.
- `checkpoint`: the same checkpoints, started through production `startSync`.
- `anchor`: one checkpoint at the target, so production discovers anchors.
- `frontier1`: no checkpoints, the production one-observation anchor policy.
- `frontier2`: no checkpoints, two matching observations per anchor.

Discovery modes scale the range cap down to four headers, and simulated peers
honour the same cap, so a twelve-header chain needs several spans.

Fault classes:

- Duplicate replies, stalls past the request timeout, peer churn, and blocked
  outbound sends.
- Store faults: down, a write that lands but reports an error, and a write
  that lands its first half and then fails.
- Actor asks that report a caller deadline before or after the actor applies
  them, at CompleteRange, FinishAnchorHeaders, RequestRange and
  CommitReadyRanges (the unexported `headerSyncAskFault` seam).
- Byzantine replies: corrupt, partial, empty, overlong, reordered, gapped,
  a PoW-valid fork (checkpoint modes only), a stale timestamp, an unexpected
  difficulty, and replies to another peer's request.
- Restarts over the same stores, shutdown at an arbitrary point, and RTT
  updates that change peer ranking.

Callbacks that are ready at the same virtual instant are sorted and then
shuffled by the seed: `synctest` makes the set deterministic, not its order.

## Invariants

`dst_invariants_test.go` runs after every handled message:

- The durable tip never decreases, except the serial path's rollback to the
  previous checkpoint, and never below that checkpoint.
- Every durable header is on the honest chain, and heights are written in
  order, a height again only after a rollback removed it.
- The visible tip and validation cache agree with each other, and trail the
  store only while a commit retry is armed to reconcile them.
- Ranges have one owner, lease epochs never decrease, committed ranges are at
  or below the durable tip, and no range commits twice.
- A peer holding a lease has a wire request outstanding, and an active range
  owned by a ready or busy peer has a tracked request.
- A peer that never misbehaved or stalled is never quarantined, banned or
  disconnected, and no peer is ever IP-banned for being unresponsive.

After the scripted phase, the heal phase restores storage and keeps every
peer slot filled with an honest peer, then lets replies and real deadlines
finish. The run must reach the honest tip with nothing active, staged,
retrying, or non-terminal. Two runs of a seed must produce the same events,
steps, writes, bans and final range and peer tables.

Two known gaps are counted rather than failed, so the swarm keeps finding
other bugs:

- F12: when a lying peer answers anchor discovery first, the honest reporter
  of the conflicting anchor is quarantined. Which reporter to punish is a
  policy decision.
- F13: under the one-observation policy, a liar's discovery reply confirms
  its own boundary. If its payload is never staged, commit-time validation
  never runs, the anchor is never rejected, and honest replies for the span
  fail its stop hash.

## Model And Trace Boundaries

The P header model exposes its confirmation policy explicitly. Header contents
and contextual validity are abstract inputs to P; the DST mines real SimNet
chains and runs the production contextual validator. The model treats storage
as atomic. The DST does not: it injects writes that land and then fail.

Querysync's JSON recorder preserves nanosecond RTTs and task durations, so
replay retains peer ordering when samples differ by less than a millisecond.
These are Go scheduler traces, not P checker schedules.

## Remaining DST Work

- The filter-header (cfheader) path and querysync's work manager are not
  driven by the DST; they need a cfheader runner with a query transport.
- Tip following and reorgs after sync completes, including the
  forward-only `AdvanceCommittedTip` (F3) and filter headers left above a
  fork point, need a dedicated phase.
- SQL stores are not run inside the bubble; their rollback tests run
  separately.
- The P models still script single timelines; they do not explore
  nondeterminism, and nothing derives P scenarios from DST traces.
