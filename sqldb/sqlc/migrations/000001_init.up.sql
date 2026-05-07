-- chain_tips stores the current tip of each persisted chain. There is at most
-- one row per chain_type. The chain_type values match neutrino's
-- headerfs.HeaderType: 0 = block headers, 1 = regular filter headers.
CREATE TABLE IF NOT EXISTS chain_tips (
    chain_type   INTEGER NOT NULL PRIMARY KEY,
    tip_hash     BLOB    NOT NULL,
    tip_height   BIGINT  NOT NULL CHECK (tip_height >= 0),
    updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- block_headers stores raw 80-byte serialized bitcoin block headers keyed by
-- height. A UNIQUE index on block_hash supports hash->height lookups.
CREATE TABLE IF NOT EXISTS block_headers (
    height       BIGINT NOT NULL PRIMARY KEY CHECK (height >= 0),
    block_hash   BLOB   NOT NULL,
    raw_header   BLOB   NOT NULL,
    CONSTRAINT block_headers_hash_uniq UNIQUE (block_hash)
);

CREATE INDEX IF NOT EXISTS block_headers_hash_idx
    ON block_headers (block_hash);

-- filter_headers stores 32-byte filter header hashes keyed by height. The
-- block_hash column is the bitcoin block hash this filter header corresponds
-- to. There is no foreign key to block_headers because the two chains drift
-- independently during sync.
CREATE TABLE IF NOT EXISTS filter_headers (
    height       BIGINT NOT NULL PRIMARY KEY CHECK (height >= 0),
    block_hash   BLOB   NOT NULL,
    filter_hash  BLOB   NOT NULL,
    CONSTRAINT filter_headers_block_hash_uniq UNIQUE (block_hash)
);

CREATE INDEX IF NOT EXISTS filter_headers_block_hash_idx
    ON filter_headers (block_hash);

-- regular_filters stores GCS regular filters keyed by block hash. A
-- zero-length filter_bytes value is the legacy "nil filter" sentinel and is
-- preserved as such by the SQL store implementation.
CREATE TABLE IF NOT EXISTS regular_filters (
    block_hash    BLOB NOT NULL PRIMARY KEY,
    filter_bytes  BLOB NOT NULL
);

-- banned_peers stores active and historical peer bans. ip_net is the binary
-- wire-encoded IPNet form produced by neutrino/banman/codec.go::encodeIPNet,
-- preserved verbatim from the legacy bdb encoding so the migrator can copy
-- rows in without re-encoding.
CREATE TABLE IF NOT EXISTS banned_peers (
    id            INTEGER PRIMARY KEY,
    ip_net        BLOB    NOT NULL,
    expiration    TIMESTAMP NOT NULL,
    reason        INTEGER NOT NULL,
    CONSTRAINT banned_peers_ip_net_uniq UNIQUE (ip_net)
);

CREATE INDEX IF NOT EXISTS banned_peers_expiration_idx
    ON banned_peers (expiration);
