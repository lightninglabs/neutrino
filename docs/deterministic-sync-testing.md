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
  them. Blockmanager call sites (CompleteRange, RequestRange and
  CommitReadyRanges) go through the unexported `headerSyncAskFault` seam;
  the controller's own asks (planning, discovered-range staging and anchor
  recording) go through the test-only `RuntimeConfig.AskFault`.
- Byzantine replies: corrupt, partial, empty, overlong, reordered, gapped,
  a PoW-valid fork (checkpoint modes only), a stale timestamp, an unexpected
  difficulty, and replies to another peer's request.
- Restarts over the same stores, shutdown at an arbitrary point, and RTT
  updates that change peer ranking.
- With the reorg class enabled, a phase after healing: peers announce three
  more blocks, and in the frontier modes a heavier branch forking below the
  tip, which drives the serial extension and reorg paths and the block
  notifications.

Callbacks that are ready at the same virtual instant are sorted and then
shuffled by the seed: `synctest` makes the set deterministic, not its order.

## Invariants

`dst_invariants_test.go` runs after every handled message:

- The durable tip never decreases, except the serial path's rollback to the
  previous checkpoint, and never below that checkpoint.
- Every durable header is on the honest chain, and heights are written in
  order, a height again only after a rollback removed it.
- The visible tip and validation cache agree with each other, and trail the
  store only while a commit retry is armed to reconcile them. The next
  checkpoint is always above the visible tip.
- Ranges have one owner, lease epochs never decrease, committed ranges are at
  or below the durable tip, and no range commits twice.
- A peer holding a lease has a wire request outstanding, and an active range
  owned by a ready or busy peer has a tracked request.
- A peer that never misbehaved or stalled is never quarantined, banned or
  disconnected, and no peer is ever IP-banned for being unresponsive.

After the scripted phase, the heal phase restores storage and gives every
empty peer slot a fresh honest peer. A faulted peer that is still connected
stays: replacing it would run the done-peer reset and hide a stall it left
behind. Replies and real deadlines then finish the work; virtual time moves
every round, and after a few idle rounds the phase waits out a peer
cooldown. The run must reach the honest tip with nothing active, staged,
retrying, or non-terminal. Two runs of a seed must produce the same events,
steps, writes, bans and final range and peer tables.

Two known gaps are counted rather than failed, so the swarm keeps finding
other bugs. Each excuse is narrow, and anything outside it still fails:

- F12: when a lying peer answers anchor discovery first, the honest reporter
  of the conflicting anchor is quarantined. Only a peer whose honest reply
  covered a liar's anchor or range stop is excused. Which reporter to punish
  is a policy decision.
- F13: under the one-observation policy, a liar's discovery reply confirms
  its own boundary. If its payload is never staged, commit-time validation
  never runs, the anchor is never rejected, and honest replies for the span
  fail its stop hash. Only a run stuck exactly at the start of a range that
  ends at such an anchor is excused.

## Model And Trace Boundaries

The P header model exposes its confirmation policy explicitly. Header contents
and contextual validity are abstract inputs to P; the DST mines real SimNet
chains and runs the production contextual validator. The model treats storage
as atomic. The DST does not: it injects writes that land and then fail.

Querysync's JSON recorder preserves nanosecond RTTs and task durations, so
replay retains peer ordering when samples differ by less than a millisecond.
These are Go scheduler traces, not P checker schedules.

## Remaining DST Work

- There is no cfheader DST. The filter-header path, cfHandler and querysync's
  work manager are not driven, so neither is the producer-side invariant that
  after a reorg every stored filter header's block hash equals the block
  header store's hash at that height: `rollBackToHeight` reads the filter tip
  once while cfHandler can still write old-fork filter headers. A cfheader
  runner needs a query transport over the work manager.
- Block notifications are recorded and compared across runs, but their order
  is not asserted. A connected notification for an old-fork block can arrive
  after its disconnected notification; subscribers must tolerate that.
- SQL stores are not run inside the bubble; their rollback tests run
  separately.
- `tcHeaderSyncExplore` explores interleavings of three peer machines and
  the explorer's choices, but P scenarios are not yet derived from DST
  traces, and the query model has no exploration test.
