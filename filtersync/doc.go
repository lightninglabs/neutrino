// Package filtersync holds the checks that regular filter header sync runs on
// what peers serve: the checkpoint sanity and agreement checks, the per-height
// mismatch detection, the block-based reconciliation of disputed filters and
// the batch timeout policy. None of them touch the network or own a goroutine;
// the block manager calls them from its filter sync loop.
package filtersync
