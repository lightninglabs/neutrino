# headersync

Package `headersync` decides which block headers to request, from which
peers, and when a batch is ready to be written. The `Driver` takes one event
at a time and returns the effects for the block manager to execute: requests to
send, timers to arm, and commits to submit to the chain store actor. Behind
the driver sit the `Manager` and its `HeaderSyncFSM` (ranges, anchors and
leases), one machine per peer, the `Session` request bookkeeping, and the
`TipState` validation cache.

The architecture, including the driver loop, the event and effect catalog, and
the path from a range reply to a durable commit, is described in
[docs/headersync-architecture.md](https://github.com/lightninglabs/neutrino/blob/master/docs/headersync-architecture.md). For how header
sync fits with filter sync, the stores, and the query work manager, see
[docs/sync-architecture.md](https://github.com/lightninglabs/neutrino/blob/master/docs/sync-architecture.md).
