# Neutrino: Privacy-Preserving Bitcoin Light Client

[![Build Status](https://github.com/lightninglabs/neutrino/actions/workflows/main.yml/badge.svg)](https://github.com/lightninglabs/neutrino/actions/workflows/main.yml)
[![Godoc](https://godoc.org/github.com/lightninglabs/neutrino?status.svg)](https://godoc.org/github.com/lightninglabs/neutrino)
[![Coverage Status](https://coveralls.io/repos/github/lightninglabs/neutrino/badge.svg?branch=master)](https://coveralls.io/github/lightninglabs/neutrino?branch=master)

Neutrino is a Bitcoin light client written in Go and designed with mobile
Lightning Network clients in mind. It uses a
[new proposal](https://lists.linuxfoundation.org/pipermail/bitcoin-dev/2017-June/014474.html)
for compact block filters to minimize bandwidth and storage use on the client
side, while attempting to preserve privacy and minimize processor load on full
nodes serving light clients.

## Mechanism of operation
The light client synchronizes only block headers and a chain of compact block
filter headers specifying the correct filters for each block. Filters are loaded
lazily and stored in the database upon request; blocks are loaded lazily and not
saved.

[docs/sync-architecture.md](docs/sync-architecture.md) describes the sync
pipeline in detail: which component owns which state, how header ranges move
from planning to durable commit, how filter headers are verified, and how the
storage backends are laid out.

## Usage
The client is instantiated as an object using `NewChainService` and then
started. Upon start, the client sets up its database and other relevant files
and connects to the p2p network. At this point, it becomes possible to query the
client.

### Queries
There are various types of queries supported by the client. There are many ways
to access the database, for example, to get block headers by height and hash; in
addition, it's possible to get a full block from the network using
`GetBlockFromNetwork` by hash. However, the most useful methods are specifically
tailored to scan the blockchain for data relevant to a wallet or a smart
contract platform such as a [Lightning Network node like 
`lnd`](https://github.com/lightningnetwork/lnd). These are described below.

#### Rescan
`Rescan` allows a wallet to scan a chain for specific TXIDs, outputs, and
addresses. A start and end block may be specified along with other options. If
no end block is specified, the rescan continues until stopped. If no start block
is specified, the rescan begins with the latest known block. While a rescan
runs, it notifies the client of each connected and disconnected block; the
notifications follow the
[btcjson](https://github.com/btcsuite/btcd/blob/master/btcjson/chainsvrwsntfns.go)
format with the option to use any of the relevant notifications. It's important
to note that "recvtx" and "redeemingtx" notifications are only sent when a
transaction is confirmed, not when it enters the mempool; the client does not
currently support accepting 0-confirmation transactions.

#### GetUtxo
`GetUtxo` allows a wallet or smart contract platform to check that a UTXO exists
on the blockchain and has not been spent. It is **highly recommended** to
specify a start block; otherwise, in the event that the UTXO doesn't exist on
the blockchain, the client will download all the filters back to block 1
searching for it. The client scans from the tip of the chain backwards, stopping
when it finds the UTXO having been either spent or created; if it finds neither,
it keeps scanning backwards until it hits the specified start block or, if a
start block isn't specified, the first block in the blockchain. It returns a
`SpendReport` containing either a `TxOut` including the `PkScript` required to
spend the output, or containing information about the spending transaction,
spending input, and block height in which the spending transaction was seen.

### Stopping the client
Calling `Stop` on the `ChainService` client allows the user to stop the client;
the method doesn't return until the `ChainService` is cleanly shut down.

## Storage backends
Neutrino supports two persistent storage backends. They are mutually exclusive
and the caller selects exactly one when calling `NewChainService`.

### `walletdb` (default, legacy)
Pre-existing deployments pass an open `walletdb.DB` (bbolt) handle via
`Config.Database`. Block headers and filter headers are stored in append-only
flat files alongside the bbolt database; ban records and compact filters live
inside the bbolt database. This is the historical backend and remains fully
supported.

```go
db, _ := walletdb.Create("bdb", filepath.Join(dataDir, "neutrino.db"), true, 10*time.Second)
cs, err := neutrino.NewChainService(neutrino.Config{
    DataDir:     dataDir,
    Database:    db,
    ChainParams: chaincfg.MainNetParams,
})
```

### SQL (`SQLite` or `PostgreSQL`)
Setting `Config.SQLConfig` switches all persistent stores
(`headerfs.BlockHeaderStore`, `headerfs.FilterHeaderStore`,
`filterdb.FilterDatabase`, `banman.Store`) to a single SQL database. The
schema is managed via `golang-migrate`; downgrade protection refuses to start
when the on-disk version exceeds the binary's `LatestMigrationVersion`.

```go
import (
    "github.com/lightninglabs/neutrino/sqldb"
    sqldbv2 "github.com/lightningnetwork/lnd/sqldb/v2"
)

cs, err := neutrino.NewChainService(neutrino.Config{
    DataDir:     dataDir,
    ChainParams: chaincfg.MainNetParams,
    SQLConfig: &sqldb.Config{
        Backend: sqldb.BackendSqlite,
        Sqlite:  &sqldbv2.SqliteConfig{},
    },
})
```

The PostgreSQL backend takes a DSN instead:

```go
SQLConfig: &sqldb.Config{
    Backend: sqldb.BackendPostgres,
    Postgres: &sqldbv2.PostgresConfig{
        Dsn: "postgres://user:pass@host:5432/neutrino?sslmode=disable",
    },
},
```

### Migrating from `walletdb` to SQL
Upgrading is automatic. On the first start with `SQLConfig` set, neutrino
looks in `DataDir` for the legacy state and, if it finds any, imports it into
the SQL tables before the chain service comes up:

* `block_headers.bin` and `reg_filter_headers.bin`, the flat header files,
* `neutrino.db`, the `walletdb` file (the name lnd uses) holding the filter
  and ban buckets. It is opened read-only.

```go
cs, err := neutrino.NewChainService(neutrino.Config{
    DataDir:     dataDir, // Where block_headers.bin and neutrino.db live.
    ChainParams: chaincfg.MainNetParams,
    SQLConfig: &sqldb.Config{
        Backend: sqldb.BackendSqlite,
        Sqlite:  &sqldbv2.SqliteConfig{},
    },
})
```

Do not also pass `Config.Database`: it is mutually exclusive with `SQLConfig`,
and the `walletdb` must be closed by its previous owner before neutrino can
open it for the import.

The import is a fixed pipeline: the flat files are read in large chunks, block
hashes are computed on parallel workers, and a single writer inserts each chunk
with one bulk statement (a `generate_series`/recursive CTE over the raw bytes,
so no per-row parameters are bound). Filters stream out of the `walletdb`
without being held in memory. On a synthetic mainnet-sized chain (900k block
and filter headers, 20k filters) this takes a few seconds on either backend.
While importing, neutrino also validates the data: the first header must be
the genesis block of `ChainParams`, every header must commit to the hash of
the one before it, and the filter header file may be shorter than the block
header file but never longer. Violations, as well as truncated or corrupt
files, fail the start with an error naming the problem instead of silently
dropping the data.

The import runs inside migration version 2 of the `neutrino_migrations`
tracking table, so it happens at most once:

* A fresh install has no legacy files; version 2 is recorded as a no-op.
* If the import fails or the process dies part way, version 2 is not
  recorded. The next start wipes the partial SQL rows and imports again from
  the (untouched) legacy state.
* Only after the import was recorded are `block_headers.bin` and
  `reg_filter_headers.bin` renamed to `*.bak`. The `walletdb` file is left in
  place; delete it, and the `*.bak` files, once you are happy with the SQL
  state.

Two knobs override the automatic behavior:

* `Config.LegacyImport` points the importer at non-default locations (custom
  file names, a `walletdb` at a different path via `BoltPath`) or hands it an
  already-open `walletdb.DB`.
* `SQLConfig.SkipLegacyMigration` disables the import. Note that version 2 is
  still recorded, so the legacy files will **not** be imported by a later
  start without the flag; neutrino logs a warning when it skips files that
  exist.
