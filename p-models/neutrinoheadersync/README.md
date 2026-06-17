# Neutrino Header Sync Model

This model captures the intended parallel block header sync contract before the
production `blockmanager` path is migrated away from a single active sync peer.

The model currently covers:

- checkpoint/trusted anchors and discovered anchor confirmation;
- range planning only between confirmed anchors;
- peer-owned local range queues and active range leases;
- work stealing that bumps a range lease epoch;
- stale completions that cannot overwrite the current owner or staged result;
- invalid completions that do not advance the committed tip;
- out-of-order range completion with strictly ordered commit.

The central design is a reorder buffer: peers may complete validated ranges in
any order, but the visible committed header tip only advances through the next
contiguous staged range.

## Run

```shell
./p-models/scripts/check.sh
```
