-- name: UpsertBan :exec
INSERT INTO banned_peers (ip_net, expiration, reason)
VALUES ($1, $2, $3)
ON CONFLICT (ip_net) DO UPDATE
SET expiration = excluded.expiration,
    reason     = excluded.reason;

-- name: GetBan :one
SELECT expiration, reason FROM banned_peers
WHERE ip_net = $1;

-- name: DeleteBan :exec
DELETE FROM banned_peers
WHERE ip_net = $1;

-- name: DeleteExpiredBans :execrows
DELETE FROM banned_peers
WHERE expiration <= $1;
