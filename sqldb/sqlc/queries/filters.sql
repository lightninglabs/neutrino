-- name: PutFilter :exec
INSERT INTO regular_filters (block_hash, filter_bytes)
VALUES ($1, $2)
ON CONFLICT (block_hash) DO UPDATE
SET filter_bytes = excluded.filter_bytes;

-- name: GetFilter :one
SELECT filter_bytes FROM regular_filters
WHERE block_hash = $1;

-- name: PurgeRegularFilters :exec
DELETE FROM regular_filters;
