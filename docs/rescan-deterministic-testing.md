# Deterministic rescan testing

`rescan/dst_test.go` runs the production rescan actor, mailbox, state machine,
subscription bridge, and retry timers inside `testing/synctest`. The chain
fixture supplies headers, filters, blocks, and block notifications; it can fail
individual fetches or subscriptions and gate a filter fetch while a rewind is
queued. The callback oracle tracks the connected hash stack, verifies every
connect and disconnect, and checks that a watched payment is delivered after
both initial sync and rewind. A same-height reorg at subscription handoff and
an obsolete retry after a live reorg exercise the historical/live boundary.
Deep catch-up and rewind reorgs deliver ordered disconnects with parent headers
and replacement connects through the subscription bridge, while orphan header
lookups fail permanently. The replacement chain contains a watched payment.

The bounded byte input chooses a sequence of block announcements, filter
failures, virtual-time advances, rewinds, and watch updates. Each replay checks
callback continuity at every stimulus and final recovery after faults are
removed. Each seed runs twice and compares the full callback sequence and tip.
Virtual time separates successive stimulus deadlines, but synctest does not
control every scheduler decision or explore every possible interleaving. The
filter gate explicitly arranges one mailbox update while a chain fetch is in
flight. The production actor subscribes before historical scanning and retains
that subscription through rewinds, so these tests can deliver disconnects for
blocks that became orphaned during either phase. Run the race detector
separately to check for data races.

The fixture starts from an active genesis anchor. If a configured start stamp
is already orphaned before the first subscription succeeds, earlier disconnect
notifications are unavailable. The actor reports an error and retries without
reporting the scan current; the caller must reconcile or restart from a known
active anchor. A reorg crossing the start stamp after subscription relies on
the ordered disconnect notifications and their parent headers.

Run the fixed corpus and a fuzz campaign with:

```sh
go test ./rescan -run '^TestRescanDST$'
go test ./rescan -run '^$' -fuzz '^FuzzRescanDST$' -fuzztime=60s -parallel=1
go test ./rescan -run '^$' -fuzz '^FuzzDSTCatchupMailboxBurst$' -fuzztime=60s -parallel=1
go test -race ./rescan
```

`TestDSTCatchupMailboxBurst` gates a historical filter fetch, queues 101 to
164 connected notifications, and checks the complete callback sequence and a
watched payment. Its fuzz target varies the burst size and whether the first
subscription fails, then runs the same input twice. The fixed regression tests
also cover a canonical successor arriving while its predecessor retries and a
rewind accepted before the first subscription succeeds. All three regressions
fail on commit `3482e82` and pass with the corresponding fixes.

Go saves minimized inputs under `rescan/testdata/fuzz/FuzzRescanDST`. Replay a
saved case with `go test ./rescan -run 'FuzzRescanDST/<seed-name>'`. The
`9cc2bc806a385370` input exposed independently scheduled retry deadlines that
could interleave with a rewind; the fixture now separates deadlines for
successive stimuli rather than assuming synctest orders equal timers.

The seed `b15b16f1ef330f91` combines a failed initial subscription, block
fetch failure, historical reorg, and rewind reorg. It exposed a fixture bug:
the second replacement had the same header hash as the first, but the fixture
marked that hash unavailable as an orphan. Replacement headers now use a
distinct nonce and the seed remains in the corpus.
