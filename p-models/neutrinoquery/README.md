# Neutrino Query Scheduler Model

This model captures the intended query scheduler contract before the production
code grows a full actor/FSM or deterministic simulation harness.

The model currently covers:

- canceled queued work is not dispatched to peers;
- a blocked or full high-ranked peer must not prevent another capable peer from
  receiving work;
- a quarantined high-ranked peer must not receive work until it recovers;
- ping RTT raises the per-peer timeout floor and remains clamped;
- queued work is assigned into bounded, balanced peer-local owned queues before
  active dispatch;
- an idle peer can steal a bounded batch of unclaimed work from an overloaded
  peer;
- work stealing does not drain ready or busy donors below their last local task;
- a busy donor with only one queued local task is not stealable until its active
  request finishes or fails;
- work stealing can drain a blocked donor whose work would otherwise be
  stranded.

These are deliberately small, executable invariants. The bridge fixtures in
`bridge/scenarios.json` describe the same stories the later Go `querysync`
bridge will replay against the implementation. Once the actor/FSM package lands,
manager actor traces can be checked against the same scheduler decisions as the
implementation evolves.

## Run

```shell
./p-models/scripts/check.sh
```

The Go-side bridge and actor/FSM tests land with the `querysync` package in a
later PR.
