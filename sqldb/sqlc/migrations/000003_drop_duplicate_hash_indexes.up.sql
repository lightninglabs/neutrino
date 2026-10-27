-- The UNIQUE constraints on block_headers.block_hash and
-- filter_headers.block_hash already create sqlite autoindexes. The explicit
-- non-unique indexes duplicated every header insert and inflated WAL/checkpoint
-- work on the hottest initial-sync tables.
DROP INDEX IF EXISTS block_headers_hash_idx;
DROP INDEX IF EXISTS filter_headers_block_hash_idx;
