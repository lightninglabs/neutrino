# Deterministic Sync Testing

The sync tests combine P scenarios, Go bridge fixtures, virtual-time actor
tests, and a seeded production-path harness. They cover different boundaries;
a passing model check does not prove header validation or SQL crash recovery.

## Run And Replay

```sh
go test . -run '^TestHeaderSyncDSTReplay$' -count=1
go test -race . -run '^TestHeaderSyncDSTReplay$' -count=2
go test . -run '^$' -fuzz '^FuzzHeaderSyncDST$' -fuzztime=60s -parallel=1
go test ./headersync/... ./querysync/... -count=1
go test . -run '^TestHeaderTransport|^TestHeaderCommitRetryGeneration'
go test . -run '^$' -fuzz '^FuzzHeaderTransportDST$' -fuzztime=2m -parallel=1
go test . -run '^$' -fuzz '^FuzzHeaderTransportCheckpointDST$' -fuzztime=2m
```

`blockmanager_dst_test.go` uses `testing/synctest` to control elapsed time and
wait for actor quiescence. Seed bytes determine disconnection versus timeout,
response latency, delivery of an old response before or after replacement,
invalid discovery, and a failed store write. Each fixed seed runs twice and
must produce the same observable outcome. Go fuzzing saves a failing input
under `testdata/fuzz/FuzzHeaderSyncDST`. Generated inputs also run twice and
must agree, rather than checking replay only for the fixed corpus. Replay a
saved failure with:

```sh
go test . -run '^FuzzHeaderSyncDST/<saved-input-hash>$'
```

The seed controls simulated fault order. It does not select every Go scheduler
interleaving. The race detector and repeated runs remain separate checks.

## Production Code Exercised

The harness constructs the real blockmanager and its headersync runtime,
including peer actors and the querysync-backed dispatch actor. Simulated
responses pass through structural range validation, actor completion,
staging, contextual header validation, and ordered persistence. The harness
does not implement a second scheduler or an alternative ownership policy.

The fixed scenarios assert that later ranges cannot advance the visible tip
across a gap, a healthy peer can finish stranded work, old leases cannot
replace newer results, and valid staging survives a failed write. Invalid
discovery must be replaceable without restarting the manager. Separate
invalidation tests check that independently checkpoint-backed staging and
requests survive failure in another interval.

The fixture uses the existing legacy block-header store. Write failures are
injected at `BlockHeaderStore.WriteHeaders`. SQLite and PostgreSQL rollback
tests run separately; SQL driver timers are not placed in a synctest bubble.
No process crash or disk corruption is simulated here.

## Model And Trace Boundaries

The P header model exposes its confirmation policy explicitly. Its recovery
scenario executes both the one-observation production policy and the
two-observation default policy. It separates permanent contextual-validation
failure from transient storage failure. The Go bridge uses the same concrete
range and anchor recovery story, including dependent-request cleanup.

Header contents and contextual validity are abstract inputs to P. Production
Go tests mine small SimNet chains and execute the actual contextual validator.
The model treats storage as atomic; it does not establish filesystem durability.

Querysync's JSON recorder preserves nanosecond RTTs and task durations, so
replay retains peer ordering when samples differ by less than a millisecond.
Existing millisecond-only traces remain readable. These are Go scheduler
traces, not P checker schedules.

## Transport Runner

`blockmanager_transport_dst_test.go` adds a seeded transport around the real
headersync/querysync runtime. Production getheaders requests go through an
internal `headerTransport`; the default remains `ServerPeer.PushGetHeadersMsg`.
The simulator has one outbound slot per connection, encodes/decodes header
messages, and delivers them through `QueueHeaders` and the same serialized
`handlePeerMessage` entry point used by the production block handler.

Unlike the first harness, this runner expires the real request timers. It
generates reply reordering, duplicates, partial/corrupt replies, blocked sends,
disconnect/reconnect, stalls, and write outages. Simultaneous timeout delivery
order is explicitly chosen by the seed. `synctest` alone does not fix callback
ordering. Each input runs twice and compares actor events, event steps, writes,
and counters. Resource use is bounded to 24 input actions and 32 recovery rounds.

After each event, an independent oracle checks unique ownership, retained
staged payloads, increasing durable heights, agreement between the durable and
visible tips, and agreement with the validation cache. The recovery phase
restores storage and peer availability, then uses replies and production timers
only. It does not force FSM ownership changes or call commit to make progress.

The saved seed `5b15bb10e03b320e` reproduces the write-outage stall found by the
first campaign. See [the campaign report](dst-transport-results-2026-10-25.md)
for its decoded sequence, failure mechanism, and replay command.

`FuzzHeaderTransportCheckpointDST` initially plans only the first checkpoint
span, so healing must also schedule follow-up spans. Its fixed regression
requires a successful commit retry to dispatch beyond that checkpoint without
another peer message. The matching regression delivers an old reply after the
same peer starts a new range and requires unchanged ownership and peer quality.
Session tests check both range and discovery matching, including timer retention.

The P transport layer tracks live wire requests, retained payloads, retry
generations, and validation-cache rollback. Its scenarios cover expired replies
before lease transfer, both response/timeout orders, duplicate replies, obsolete
timer callbacks, storage recovery without another network reply, and planning
after checkpoint retry. Matching uses the first parent hash, not a wire request
ID: two leases using the same locator remain indistinguishable. These are
abstract contract checks, not mechanically derived implementation refinement.

## Remaining DST Work

The original harness still injects timeout events. The transport runner covers
production timers and header-message encode/decode, but not TCP framing,
handshake, the concrete ServerPeer socket writer, or SQL process crashes.

The simulated outbound slot is not the concrete btcd peer send queue. Expand
that boundary separately if socket-writer scheduling becomes relevant. SQL
commit/restart fault injection remains separate from virtual-time actor tests.
Wire matching coverage excludes replies with a different locator without
consuming the current request. It does not establish identity for replies
sharing a locator or mechanically compare every Go transition with P.

## Coverage Gates

The runner keeps production actor/FSM logic in the test path. Checked items
below are covered by the current bounded corpus; unchecked items remain open.

- [x] Put a simulated transport behind the real peer request/send boundary.
  A test must expire a production request timer without directly marking its
  owner blocked, then observe reassignment and eventual commit.
- [x] Generate seeded event sequences with multiple peers and ranges, rather
  than only varying the current fixed recovery story. Cover blocked queues,
  partial replies, churn, cancellation, and shutdown. Every failure must save
  a replayable seed, with no wall-clock sleeps or external network dependency.
- [x] Check ownership, epochs, staging, cache, and commit invariants per event.
  Require bounded progress after explicitly restoring a viable peer and store.
- [x] Preserve newer requests and peer quality under different-locator duplicates.
- [ ] Add exact wire-transition comparison with the P/bridge contract.
- [x] Run the bounded seed corpus through the existing `go test ./...` CI lanes.
- [ ] Run longer continuous fuzz campaigns separately from unit CI.
  Keep SQL restart/crash checks outside the virtual-time actor bubble until
  driver behavior can be controlled without changing production semantics.
