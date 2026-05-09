-- name: InsertBlockHeader :exec
INSERT INTO block_headers (height, block_hash, raw_header)
VALUES ($1, $2, $3);

-- name: GetBlockHeaderByHeight :one
SELECT raw_header FROM block_headers
WHERE height = $1;

-- name: GetBlockHeaderByHash :one
SELECT height, raw_header FROM block_headers
WHERE block_hash = $1;

-- name: GetBlockHeaderHeightByHash :one
SELECT height FROM block_headers
WHERE block_hash = $1;

-- name: GetBlockHeaderRange :many
SELECT height, raw_header FROM block_headers
WHERE height BETWEEN sqlc.arg('start_height') AND sqlc.arg('end_height')
ORDER BY height ASC;

-- name: DeleteBlockHeadersFromHeight :exec
DELETE FROM block_headers
WHERE height > $1;

-- name: CountBlockHeaders :one
SELECT COUNT(*) FROM block_headers;
