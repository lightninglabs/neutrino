# Rescan actor chain and shutdown assumptions

The actor subscribes before historical scanning and keeps that subscription
while catching up and rewinding. Disconnection notifications supply the
headers needed to undo scanned blocks, including headers removed from the
active header store. The subscription fails only once the ChainService has
stopped: if `Subscribe` fails at start, or the subscription closes while the
rescan runs, the actor reports the error once through `OnError` and stops in
`StateTerminal`, as the legacy rescan loop does.

The caller must provide a start stamp and header on the active chain. If the
anchor was already disconnected before subscription, or if the subscription
is unavailable while a deep reorg removes the scanned ancestry, the actor may
be unable to recover the missing headers. It reports errors and retries rather
than reporting a completed scan. The caller must reconcile its wallet state
and restart from a known active-chain anchor when the missing ancestry cannot
be recovered.

The actor cancels its block and filter queries before waiting for the mailbox
on shutdown. The underlying ChainSource must honor the cancellation query
option for an in-flight fetch to return promptly. Retry delays and bridge sends
are also cancelled on shutdown.

The P model captures early subscription and idempotent start requests. Its
historical scan still abstracts disconnect delivery and does not establish the
runtime ordering covered by the deterministic reorg tests.

An `AddWatchAddrs` update with no rewind applies to future scanning only. To
cover blocks already processed or concurrent with registration, callers must
provide a prior active-chain rewind height. Scanning resumes after that anchor.

`AddWatchAddrs` waits up to five seconds for an acknowledgement. A timeout does
not remove an update already queued in the mailbox: it can still apply after
the caller returns. Callers must treat a timeout as an unknown outcome and
reconcile the current watch state before assuming registration failed. The
actor does not roll back wallet state maintained by its caller.
