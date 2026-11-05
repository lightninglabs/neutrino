# filtersync

Package `filtersync` decides what regular filter header sync does next. Its
`Syncer` takes one event and returns the effects that the `cfHeaderSync`
executor in the root package performs: wait on the tip feed, fetch
checkpoints, resolve conflicts between peers, fetch headers, or retry. The
package also holds the checks run on the checkpoints and headers that peers
serve.

The architecture, including the Syncer's phases, checkpoint resolution, and
which peers are banned and why, is described in
[docs/filtersync-architecture.md](https://github.com/lightninglabs/neutrino/blob/master/docs/filtersync-architecture.md). For the
overview, see [docs/sync-architecture.md](https://github.com/lightninglabs/neutrino/blob/master/docs/sync-architecture.md).
