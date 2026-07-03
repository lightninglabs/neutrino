-- fee_samples stores one per-block fee observation per height, produced by
-- the feeest.Sampler from fully fetched blocks. Height is the primary key:
-- under normal operation there is at most one observed block per height, and
-- on a reorg the orphaned suffix is deleted via DeleteFeeSamplesFromHeight
-- before samples from the new chain arrive. Upserts replace the row in place
-- so a competing block at the same height simply overwrites the orphan.
CREATE TABLE IF NOT EXISTS fee_samples (
    height             BIGINT  NOT NULL PRIMARY KEY CHECK (height >= 0),
    block_hash         BLOB    NOT NULL,
    timestamp          BIGINT  NOT NULL,
    total_fees         BIGINT  NOT NULL CHECK (total_fees >= 0),
    total_weight       BIGINT  NOT NULL CHECK (total_weight >= 0),
    coinbase_weight    BIGINT  NOT NULL CHECK (coinbase_weight >= 0),
    min_known_tx_rate  BIGINT  NOT NULL CHECK (min_known_tx_rate >= 0),
    known_tx_count     INTEGER NOT NULL CHECK (known_tx_count >= 0),
    flags              INTEGER NOT NULL
);
