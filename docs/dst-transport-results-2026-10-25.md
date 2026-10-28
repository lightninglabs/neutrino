# Header Transport DST Results

The runner uses the real headersync/querysync actor graph, production request
timers and response handlers, a bounded simulated transport, and injected
header-store failures. It syncs twelve mined SimNet headers across six known
anchor ranges. The network ID enables the production actor path; this is not
a mainnet consensus, live-network throughput, or SQL crash benchmark.

## Reproduced Failures

The fixed timeout corpus exposed orphan staging. After a timeout, the stale
reply handler called `CompleteRange` with the old epoch and `valid=true`.
Before anyone stole the lease, that epoch was still current. The FSM accepted
completion, but the stale handler never retained the headers. Ordered commit
then waited on staged work whose payload did not exist. Expired replies are
now discarded without completing scheduler work.

The first fuzz campaign failed after 43.02s and saved this minimized input:

```text
testdata/fuzz/FuzzHeaderTransportDST/5b15bb10e03b320e
[]byte("Z9!!\x8d")
hex: 5a3921218d
```

It delivers an initial response, makes the store unavailable, lets the viable
peer respond while other requests time out, and repeats a prior response.
After the environment recovered, durable height remained at2 of12 even though
all remaining ranges were valid and staged.

Two mechanisms blocked recovery. There was no autonomous commit retry once
network work had completed. A duplicate could also enter the compatibility
validator, append to its memory list, fail the write, and leave the validation
tip ahead of disk. Actor commit retries then saw their retained range as
disconnected from that speculative parent.

The production fixes coalesce transient commit failures behind one
generation-checked retry timer and restore the durable validation parent after
a failed compatibility write. The minimized input passes repeatedly without
the harness explicitly invoking commit during recovery.

```sh
go test . -run '^FuzzHeaderTransportDST/5b15bb10e03b320e$' -count=3
go test . -run '^TestHeaderTransportDST$' -count=3
```

## Campaigns

The initial post-fix campaigns passed: 255 executions in 122.311s and 484 in
302.350s, with three and thirteen coverage-interesting inputs respectively.
Neither produced a new failure. Coverage-interesting does not mean a bug.

Independent review then found a checkpoint-boundary continuation bug. A
successful storage retry advanced the checkpoint but did not call scheduling
again. The original fixture preplanned every range, hiding the missing work.
`TestHeaderTransportCheckpointRetry` starts with only one planned span. It
failed before the fix because no follow-up request was sent, then passed once
successful retries resumed scheduling. A separate fuzz target now exercises
incremental checkpoint planning.

The user-approved response-matching API also fixes duplicates consuming newer
requests. `TestHeaderTransportDuplicatePreservesRequest` reproduced a responsive
peer losing active range 4 and becoming quarantined after replay of its prior
reply. The API compares the first parent hash before stopping the current
request timer. Both range and discovery callers discard unmatched responses
without handing them to the compatibility writer. Empty responses still reach
validation, and matching responses still undergo normal structural/contextual
validation. Two leases with the same locator remain indistinguishable.

After these fixes, fresh two-minute campaigns passed: 29 executions for the
preplanned target (122.625s) and 76 for the incremental checkpoint target
(122.384s). Neither found another failing input. They ran alongside the full
suite and race tests; execution counts are coverage observations, not a
throughput comparison.

A subsequent three-minute incremental-checkpoint campaign passed 193
executions in 182.326s, with six new coverage-interesting inputs and no new
failure. All three post-matching campaigns used two fuzz workers.

The full Go suite passed, with the root package taking 158.282s. Focused race
tests passed twice, with the root package taking 92.058s. The same independent
reviewer rechecked the fixes and found no actionable issues by source tracing.
That review did not independently rerun the test jobs.

The saved failure, matching regression, and checkpoint regression also passed
three repeated runs. A direct compatibility-writer regression retains cache
rollback coverage now that duplicate matching prevents the original seed from
entering that writer. It passed three race-enabled runs in 3.654s. Pinned Linux
lint reported zero new issues against base `560c950`. This is delta validation,
not signoff for the full PR stack.

The P model adds request/payload state and a coalesced retry obligation, with
generation guards and validation-cache rollback. Its bounded check passed
1,000 schedules with zero reported bugs; the final checker reported one
timeline and 48 scheduling points. The model now also covers first-parent
matching and successful retry planning beyond a checkpoint.
This is not exhaustive proof or automatic refinement between P and Go.

## Boundaries

The simulator does not model TCP framing, handshake, concrete socket writes,
SQL durability, or all Go scheduling interleavings. Peer admission bypasses
handshake but uses the production registration and disconnect handlers.
Duplicate replies with a different locator preserve newer requests and peer
quality in a dedicated regression. Same-locator ambiguity and exact Go/P
transition refinement remain outside the claim. No live lnd throughput
measurement was performed in this DST round.
