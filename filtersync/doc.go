// Package filtersync decides what regular filter header sync does next, and
// holds the checks it runs on what peers serve.
//
// The Syncer is a state machine. It owns the progress of the sync: the
// checkpoints each peer served and the height they were fetched for, the
// checkpoints the peers and the store agree on, the block header tip the
// current pass syncs up to, and the phase. It does no I/O, reads no clock and
// starts no goroutine. Handle takes one Event and returns the Effects the
// caller performs in return: wait on the tip feed, read the block header tip,
// fetch checkpoints, resolve a conflict, fetch checkpointed or uncheckpointed
// headers, sleep before a retry, or poll the quit channel.
//
// A pass waits until the filter header tip lags the block header tip by a
// checkpoint interval or the block header sync is current, reads the block
// header tip, fetches checkpoints from the peers when the ones it holds do not
// reach that tip, caps them at the tip, resolves which list is right, and
// fetches the headers up to the agreed checkpoints. A pass that ends with the
// block headers still syncing, or the filter headers still a checkpoint
// interval behind, starts over. A pass that ends caught up waits at the tip,
// and fetches the uncheckpointed headers each time the filter tip hash differs
// from the block tip hash. A round that fails with a store or chain store
// error sleeps for the retry delay and then retries the same step. A
// checkpointed batch that ends in a query error is logged and ends the pass,
// so the next pass starts while the filter tip still lags.
//
// Three invariants hold:
//
//   - Every phase but PhaseIdle and PhaseStopped waits for exactly one kind of
//     event, and the run loop feeds exactly that one. An event that does not
//     fit the phase is dropped and logged, and the empty result ends the run
//     loop.
//
//   - Every effect but PollQuit yields an event, and PollQuit yields one only
//     when the quit channel is closed. The Syncer places the effect that
//     always yields an event last in its batch, so the run loop can drop the
//     rest of a batch once an effect yields an event.
//
//   - Quit stops the Syncer in any phase with no effects, and nothing leaves
//     PhaseStopped.
//
// The block manager runs the Syncer from one goroutine, and performs the
// effects on that goroutine, network rounds included. A round blocks the
// goroutine for as long as it takes, which is fine because nothing else sends
// to it and the tip feed is level-triggered: a tip change that lands during a
// round is seen by the next wait.
//
// The rest of the package is the checks the rounds run on what peers serve:
// the checkpoint sanity and agreement checks, the per-height mismatch
// detection, the block-based reconciliation of disputed filters and the batch
// timeout policy. None of them touch the network either.
package filtersync
