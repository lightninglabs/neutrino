# chainstore

Package `chainstore` runs every write to the block header and filter header
stores on one actor, so the two chains can't drift apart. It holds a rollback
until it finishes, compares reorg work on the actor's own turn, checks writes
that may have applied before reporting an error, and publishes the block and
filter tips through `TipFeed`.

The architecture, including the command catalog, the commit and reorg flows,
and the tip feed, is described in
[docs/chainstore-architecture.md](https://github.com/lightninglabs/neutrino/blob/master/docs/chainstore-architecture.md). The storage
backends and SQL schema are in the Storage section of
[docs/sync-architecture.md](https://github.com/lightninglabs/neutrino/blob/master/docs/sync-architecture.md#storage).
