-- name: GetChainTip :one
SELECT tip_hash, tip_height FROM chain_tips
WHERE chain_type = $1;

-- name: UpsertChainTip :exec
INSERT INTO chain_tips (chain_type, tip_hash, tip_height, updated_at)
VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
ON CONFLICT (chain_type) DO UPDATE
SET tip_hash   = excluded.tip_hash,
    tip_height = excluded.tip_height,
    updated_at = CURRENT_TIMESTAMP;

-- name: DeleteChainTip :exec
DELETE FROM chain_tips
WHERE chain_type = $1;
