# Sync Neutrino over HTTP

This service syncs Bitcoin block headers and compact filter headers into
SQLite, then follows the server's tip. It downloads compact filters and full
blocks on demand through Neutrino's normal query and rescan APIs. It uses no
Bitcoin P2P connections or DNS seeds.

Run from the repository root:

```sh
go run ./examples/blockdn -network=mainnet
go run ./examples/blockdn -network=signet
go run ./examples/blockdn -network=testnet3
go run ./examples/blockdn -network=testnet4
```

The mainnet default URL is `https://block-dn.org`. The test network defaults
are `https://signet.block-dn.org`,
`https://testnet3.block-dn.org`, and `https://testnet4.block-dn.org`.
Availability depends on the public service. The example checks the server's
genesis hash before creating local state and rejects a different network.
Use `-url` to select your own block-dn instance:

```sh
go run ./examples/blockdn \
  -network=testnet4 \
  -url=https://your-testnet4-server.example \
  -datadir=/tmp/neutrino-http \
  -poll=15s \
  -once \
  -timeout=15m
```

`-once` exits after catching up to a recent server observation. Without it,
the service follows new blocks until interrupted. `-timeout` bounds the total
run time. Progress reports the paired header height, target height, tip hash,
and whether the service considers itself current. Polling and download errors
are reported and retried. Restarting uses the existing SQLite state at
`<datadir>/<network>/neutrino_chain.sqlite`.

`-verify-tip` also downloads the tip compact filter and full block, checks
their commitments, and prints the verified height, hash, and transaction
count. With `-once`, the example exits after this verification succeeds.
Without `-once`, it verifies each new tip. Verification errors stop the
example; `-timeout` bounds the total sync and verification time.

## Public endpoint validation

The public hostname routing incident reported in
[block-dn#32](https://github.com/guggero/block-dn/issues/32) was fixed on
2026-11-03. The maintainer confirmed a reverse proxy problem. After the fix,
all four default status endpoints reported the expected network genesis.
The temporary cross-hostname workaround is no longer needed.

Default-URL runs after the fix synced and validated the tip compact filter
and full block on these networks:

| Network | Verified height | Transactions in tip block |
| --- | ---: | ---: |
| mainnet | 974438 | 3273 |
| signet | 329305 | 108 |
| testnet4 | 158802 | 1 |

The testnet4 run reopened its previous SQLite state and followed nine new
blocks. The mainnet and signet runs started from genesis. These checks verify
sync to the configured server's advertised tip, not independent network
freshness.

For an end-to-end check through the default mainnet hostname:

```sh
go run ./examples/blockdn \
  -network=mainnet \
  -datadir=/tmp/neutrino-blockdn-real \
  -once -verify-tip -timeout=10m
```

Some CDN entries still held data from before the proxy fix. For example,
`/headers/900000` returned a cached 100,000-record file whose header at the
advertised mainnet tip did not match `/status`. A fresh query key returned the
correct mainnet file and tip. The client now includes the expected genesis
hash in data cache keys, scopes mutable height-based data to the observed tip
hash, and uses a fresh key for each status poll. Sealed header files keep a
stable network-specific key. The existing consensus and commitment checks
continue to reject mismatched responses.

## Library configuration

Set `neutrino.Config.BlockDN` to a `*neutrino.BlockDNConfig` alongside a normal
storage configuration. `BaseURL` selects the server; `HTTPClient` can supply
an HTTP proxy or custom transport. Peer lists and `HeadersImport` cannot be
combined with this mode. P2P query workers and connection managers are not
constructed or started. Peer connection operations return `ErrP2PDisabled`.

`BlockDNStatus()` reports progress and the most recent sync error.
`IsCurrent()` becomes false after an unsuccessful poll, when filters lag the
server's block tip, or when the last successful observation becomes stale.
Contexts cancel downloads during queries and shutdown.

Initial sync reads bounded header files and commits batches of 2,000 paired
headers. At the live tip, short extensions fetch individual headers by hash
and derive filter commitments from individual compact filters. Unchanged tips
require only a status request. Historical scans fetch filter batches; ordinary
individual queries fetch one filter. Every filter must match its local filter
header commitment. Blocks must match the requested header and pass Neutrino's
block sanity and witness commitment checks.

Reorganizations stage and validate a replacement branch before removing local
history. The branch must have more accumulated work than the replaced suffix.
`MaxReorgDepth` defaults to 10,000 and bounds both rollback depth and replacement
length. Larger forks return an error without adopting them. Subscribers receive
disconnects before replacement connects. An interrupted commit that leaves
block headers ahead of filter headers is repaired on the next sync.

## Trust and broadcasting

The configured server supplies chain freshness and filter commitments between
Neutrino's hardcoded checkpoints. Proof of work and filter hash checks detect
invalid or inconsistent data, but a single server can withhold newer blocks or
supply false filter commitments beyond a known checkpoint. `current=true`
means caught up to this server; it does not prove that the server advertises
the current network tip. For independent comparison, set
`BlockDNConfig.WitnessURLs` or the example's
`-witness-url`. Each witness must agree on the block hash and filter header at
the selected height before the service adopts it or reports itself current.
A lagging, unavailable, or disagreeing witness pauses sync. Use separate
operators: several URLs for the same operator do not provide independence.
Tip agreement alone does not authenticate every historical filter-header
entry. Historical imports retain the source trust described above.

The public block-dn API does not document transaction submission. The example
therefore runs without a broadcaster. `SendTransaction` returns
`ErrBlockDNReadOnly` immediately. Library callers can supply
`BlockDNConfig.Broadcast`, a context-aware callback to their own HTTPS
transaction submission service. Neutrino uses this callback for its existing
rebroadcast loop, including rebroadcasts after tip changes. The callback must
map already-known or confirmed responses to `pushtx.BroadcastError` values.

Browser deployment also requires a working WASM SQLite driver and persistent
browser storage. This example is a native Go service; it does not add those
storage components.
