-- name: InsertFilterHeader :exec
INSERT INTO filter_headers (height, block_hash, filter_hash)
VALUES ($1, $2, $3);

-- name: GetFilterHeaderByHeight :one
SELECT block_hash, filter_hash FROM filter_headers
WHERE height = $1;

-- name: GetFilterHeaderByBlockHash :one
SELECT height, filter_hash FROM filter_headers
WHERE block_hash = $1;

-- name: GetFilterHeaderRange :many
SELECT height, block_hash, filter_hash FROM filter_headers
WHERE height BETWEEN sqlc.arg('start_height') AND sqlc.arg('end_height')
ORDER BY height ASC;

-- name: DeleteFilterHeadersFromHeight :exec
DELETE FROM filter_headers
WHERE height > $1;

-- name: PurgeFilterHeaders :exec
DELETE FROM filter_headers;

-- name: CountFilterHeaders :one
SELECT COUNT(*) FROM filter_headers;
