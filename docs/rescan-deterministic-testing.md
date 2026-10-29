# Deterministic rescan testing

`rescan/dst_test.go` runs the production rescan actor inside
`testing/synctest` against a simulated chain. Each run is driven by a seed,
and the same seed always produces the same run, so a failing seed is a
reproducible bug report. The harness follows the header-sync DST in
`blockmanager_transport_dst_test.go` and `docs/deterministic-sync-testing.md`:
the same seed layout, swarm, replay, regression list and double-run
determinism check.

## What runs for real

| Component | Real or simulated |
| --- | --- |
| `RescanActor`, its lnd actor mailbox and protofsm machine, the retry timers and the subscription bridge | Real, inside the synctest bubble |
| `blockntfns.SubscriptionManager`, including backlog delivery from the requested height | Real, over the simulated notification source |
| Chain headers, filters and blocks | Simulated by `dstWorld` and the fault layer `dstSource` |
| Time | Virtual, from synctest |

The actor's own goroutines dequeue its mailbox, so the runner does not play
the event loop. It orders stimuli instead: after every stimulus it waits for
the bubble to settle and sleeps one virtual nanosecond, so retry deadlines
from successive stimuli never coincide. A reorg announced from inside a
fault hook (on the actor goroutine) also sleeps one nanosecond before the
hook returns, so the bridge queues the whole burst of notifications before
the actor's own continuations. Without that sleep, about one swarm seed in
450 double-ran with a different error count.

## The world

`rescan/dst_world_test.go` holds three pieces.

- **Fixture.** Every block is a pure function of its parent and a branch
  salt, built on first use and shared by every run in the process. The main
  chain reaches past `ProgressInterval`, so a scan that starts at
  `ProgressInterval - 10` crosses it. The content of a block depends on its
  height and salt: plain blocks, payments to the watched address A,
  payments to addresses B and C (watched only by the property test),
  payments to an address nobody watches, and spends of the newest recent
  payment. The first three blocks above each start height are always plain,
  so a wallet birthday there precedes every relevant transaction.
- **Active chain and notification source.** `dstWorld` keeps the honest
  active chain and implements `blockntfns.NotificationSource`.
  `NotificationsSinceHeight` follows the block manager: no backlog for
  height 0 or for the tip, an error above the tip, and otherwise a connect
  for every active block above the height. The harness changes the chain
  first and then announces the change: disconnects from the top down, then
  connects from the bottom up. The notification channel is unbuffered, so an
  announcement returns only after the manager has fanned it out.
- **Fault layer.** `dstSource` is the `ChainSource` the actor sees.
  `Subscribe` registers with the real manager. Header lookups by hash know
  only the active chain, as the header store does, while filter and block
  fetches also serve orphans, as the caches do. With no fault armed, every
  method is honest.

## Seed format

```
byte 0       mode        (byte % 16)
bytes 1..2   fault mask  (little-endian uint16, one bit per fault class)
bytes 3..    ops         (at most 64)
op = value % 8, arg = value / 8
```

A seed shorter than three bytes runs every fault class in mode 0, with its
bytes as ops.

The mode configures production:

| Mode bits | Effect |
| --- | --- |
| 0 and 1 | Batch size 1, 2, 3 or `DefaultBatchSize` |
| 2 | Wallet birthday at the third block above the start |
| 3 | Start at `ProgressInterval - 10` instead of genesis |

Each fault class reproduces a fault the scripted harness injected before:

| Bit | Class | Fault |
| --- | --- | --- |
| 0 | filter | The first filter fetch fails. Each faulty new-block op fails the next `arg % 3` fetches. |
| 1 | block | The first block fetch fails. |
| 2 | subscribe | The first `Subscribe` fails. |
| 3 | handoff | The first successful `Subscribe` replaces the tip before it registers, so the subscription never sees the reorg. The actor subscribes before it scans, so today this only reorgs the chain ahead of the scan. |
| 4 | historical reorg | While the historical scan fetches the filter four blocks above the start, the chain reorgs from three blocks above the start. |
| 5 | deep reorg | The same reorg from one block above the start, below the scanned tip. |
| 6 | rewind reorg | The chain reorgs from three blocks above the start while the scripted rewind walks back through it. |
| 7 | live retry | The filter fetch of a live block fails just before the block is replaced. |
| 8 | gate | A new block's filter fetch is held while a rewind waits in the mailbox. |

Bits 9 to 15 are free for the fault classes of later work.

| Op | Action |
| --- | --- |
| 0 | New block. With the filter class, the next `arg % 3` filter fetches fail. With the gate class and bit 4 of `arg`, the fetch is held while a rewind to two blocks above the start waits in the mailbox. |
| 1, 5 | Sleep one retry interval. |
| 2 | Rewind to `start + arg % (tip - start + 1)`. |
| 3 | `AddWatchAddrs` with no change. |
| 4, 6 | New block, with no faults. |
| 7 | `CurrentState`. |

An op whose class is off performs the honest action, so every seed keeps
making progress. Ops 4 to 7 are honest placeholders for later fault classes.

## Run phases

1. **Initial sync.** Start the actor and sleep six retry intervals. The
   scan must be current at the tip.
2. **Rewind.** Rewind to two blocks above the start, with the rewind reorg
   if its class is on. The scan must be current at the tip again.
3. **Live reorg.** Announce a new block, replace it with a sibling, and
   check that no obsolete retry reports an error afterwards.
4. **Ops.** Apply each op, settle, and run the op-boundary checks.
5. **Heal.** Turn every fault off and sleep 20 retry intervals. The heal
   phase resets nothing production would not.
6. **Quiescence.** The scan must be current at the active tip, and the
   oracle's quiescence checks must pass.

## The oracle

`rescan/dst_oracle_test.go` is an independent statement of what the
callbacks must show. It keeps the chain the callbacks delivered, reads the
honest active chain from the world, and keeps a dated history of the watch
set: the watched address scripts and every outpoint a delivered payment
created. Each check is a named invariant, and each violation is recorded
under its name, so a failure lists every property that broke. The checks run
after every callback and at every op boundary.

| Invariant | Asserts |
| --- | --- |
| `noMissedRelevantTx` | Each filtered connect carries exactly the block's transactions that pay a watched script or spend a watched outpoint, taken in block order. |
| `noDuplicateFiltered` | No block gets a second filtered connect without a filtered disconnect in between. |
| `filteredPairing` | Each filtered connect or disconnect is followed at once by its plain twin, and no other callback interleaves. |
| `progressMonotone` | Progress reports the delivered tip, and rises unless the delivered chain dropped below the last report. |
| `rewindRedelivers` | After a rewind below the delivered tip, no connect beyond those already in flight arrives before the delivered chain walks back to the target. Every rewind walks back by quiescence. |
| `finishedOnce` | Finished reports the delivered tip, never repeats without a disconnect in between, and has fired since the start and since the last rewind that walked back whenever the scan is current at a phase boundary. |
| `callbackContinuity` | Each connect extends the delivered tip, and each disconnect removes it. |
| `quiescentAtTip` | At each phase boundary and after heal, the actor is current at the active tip and the delivered chain is the active chain. |

No invariant has an excuse. Every one fails on at least one hand-written
mutant of production code. The mutants live in `rescan/testdata/mutants` as
patches; the last section shows how to run them.

## Running it

```sh
# Corpus and regression seeds, each run twice and compared.
go test ./rescan -run '^TestRescanDST$'

# A reproducible swarm campaign. Seeds run in parallel batches of 256, and
# a failure reports the lowest failing index of its batch.
go test ./rescan -run '^TestRescanDSTSwarm$' -rescan.swarm=50000 -rescan.seed=7

# Replay one seed, with rescan and notification logs.
go test ./rescan -run '^TestRescanDSTReplay$' -rescan.replay=<hex> -rescan.log

# Coverage-guided fuzzing over the same runner.
go test ./rescan -run '^$' -fuzz '^FuzzRescanDST$' -fuzztime=10m

# The state-machine property test over the same oracle.
go test ./rescan -run '^TestPropertyRescanStateMachine$' -rapid.checks=10000
```

The double run compares the callback list, the reported payments, the error
count, the final watch list and the final state. `FuzzRescanDST` runs every
input once and double-runs a sample keyed by a hash of the input.

When a swarm seed finds a bug, add its hex to `dstRegressionSeeds` with a
comment naming the bug. The list starts with the two inputs the scripted
harness saved under `testdata/fuzz/FuzzRescanDST`, re-encoded with their old
flags as mode and fault mask. The saved files still run under the new
decoder as two more seeds.

## The property test

`TestPropertyRescanStateMachine` in `rescan/property_fsm_test.go` drives the
rescan states directly, without the actor, and sends every outbox event to
the same oracle. Rapid chooses among extending the chain, reorging its top
one to three blocks, delivering the next notification, retrying a failed
event, running an owed continuation, rewinding, and watching address B or C
with or without a rewind. The order rapid picks is the mailbox order the
actor would see. A run ends by draining everything and requiring the scan to
be current at the tip. The test reaches watch updates and reorgs at any
point, which the DST ops do not yet generate, but it cannot see the actor's
retry admission or its callback dispatch.

## Judging the harness

Each mutant patch applies to the production code. Run the swarm against them
with the blind-recall script from the dst skill, which checks out a scratch
worktree, applies one patch at a time and reports runs-to-find:

```sh
recall.sh --repo . --cmd "go test ./rescan/ -run '^TestRescanDSTSwarm\$' \
    -rescan.seed=1 -rescan.swarm=20000 -count=1" \
    --runs-regex 'failed after ([0-9]+) runs' \
    rescan/testdata/mutants/*.patch
```

The `recall-` patches revert recorded fixes, the `inv-` patches each break
the property one invariant names, and `probe-drop-relevant-txs.patch` empties
the syncing `RelevantTxs`.

## Known gaps

- No op adds a watched address or outpoint, reorgs the chain, delays,
  duplicates or reorders a notification, closes the subscription, fails a
  header-store read, or stops the actor mid-run. The reverted e05249c guards
  need a stale notification after a disconnect, so the swarm does not find
  them. Their scripted tests in `dst_review_test.go` do.
- A reorg replaces the whole branch before its first disconnect is
  announced. The block manager rolls the header store back one block per
  disconnect, so the DST never shows a header store shorter than the
  delivered tip mid-reorg.
- A rapid-model retry re-applies the failed event without the actor's
  retry admission checks, so mutants in `actor.go` are out of its reach.
- The heal phase only sleeps. It does not check that no retry is armed, that
  `continuationPending` is clear, or that nothing fires after `Stop`.
- The fixture's `IsCurrent` always reports true, so the "rescan started
  before the chain is current" behaviour is not reached.
- Determinism comes from settling between stimuli. The harness does not
  choose the order of messages that become ready at the same instant.
- The race detector is not part of the swarm. Run `go test -race ./rescan`
  separately.
