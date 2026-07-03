-- name: UpsertFeeSample :exec
INSERT INTO fee_samples (
    height, block_hash, timestamp, total_fees, total_weight,
    coinbase_weight, min_known_tx_rate, known_tx_count, flags
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (height) DO UPDATE
SET block_hash        = excluded.block_hash,
    timestamp         = excluded.timestamp,
    total_fees        = excluded.total_fees,
    total_weight      = excluded.total_weight,
    coinbase_weight   = excluded.coinbase_weight,
    min_known_tx_rate = excluded.min_known_tx_rate,
    known_tx_count    = excluded.known_tx_count,
    flags             = excluded.flags;

-- name: GetFeeSampleByHeight :one
SELECT * FROM fee_samples
WHERE height = $1;

-- name: GetFeeSamplesTipN :many
SELECT * FROM fee_samples
ORDER BY height DESC
LIMIT $1;

-- name: GetFeeSampleRange :many
SELECT * FROM fee_samples
WHERE height BETWEEN sqlc.arg('start_height') AND sqlc.arg('end_height')
ORDER BY height ASC;

-- name: GetFeeSampleTip :one
SELECT CAST(COALESCE(MAX(height), 0) AS BIGINT) AS tip FROM fee_samples;

-- name: DeleteFeeSamplesBeforeHeight :exec
DELETE FROM fee_samples
WHERE height < $1;

-- name: DeleteFeeSamplesFromHeight :exec
DELETE FROM fee_samples
WHERE height >= $1;
