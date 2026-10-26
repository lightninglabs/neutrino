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

## Remaining DST Work

The harness injects timeout events; it does not exercise the full ServerPeer
socket, decode, outbound queue, and production getheaders timer path. A full
network simulation still needs a simulated transport at that boundary.

The next scenarios must drive the real request timers and bounded peer queues,
including blocked sends, partial responses, reconnects, cancellation, and
shutdown. Add SQL commit/restart fault injection separately from virtual-time
actor tests. Expand seed generation and compare traces against model invariants
before treating the harness as production-wide deterministic simulation.

## Next Acceptance Gates

Transport-level DST is the next testing milestone, not a claim established by
the current harness. Keep the production actor/FSM logic in that test path.

- [ ] Put a simulated transport behind the real peer request/send boundary.
  A test must expire a production request timer without directly marking its
  owner blocked, then observe reassignment and eventual commit.
- [ ] Generate seeded event sequences with multiple peers and ranges, rather
  than only varying the current fixed recovery story. Cover blocked queues,
  partial replies, churn, cancellation, and shutdown. Every failure must save
  a replayable seed, with no wall-clock sleeps or external network dependency.
- [ ] Check ownership, epoch, staging, and commit invariants after each event.
  Compare implementation transitions with bridge/model expectations, and
  require bounded progress under an explicitly responsive-peer assumption.
- [ ] Run a bounded seed corpus in CI, with longer fuzz campaigns separately.
  Keep SQL restart/crash checks outside the virtual-time actor bubble until
  driver behavior can be controlled without changing production semantics.
