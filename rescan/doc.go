// Package rescan scans the chain for transactions that pay a watched address
// or spend a watched outpoint, using compact block filters, and reports every
// scanned block to the wallet.
//
// A RescanActor hosts the scan. Its mailbox serializes every request,
// continuation and block notification, and each mailbox turn drives a
// protofsm state machine synchronously. The states are Initializing,
// Syncing, Current, Rewinding and Terminal; docs/rescan-actor.md has the
// state diagram. A state never performs a side effect itself. It returns
// outbox events, and the actor dispatches them as wallet callbacks or as a
// continuation once the transition has committed.
//
// # Heights
//
// Every non-terminal state carries a Cursor, the last scanned block. Three
// conventions follow from it:
//
//   - The start block counts as already scanned. The cursor starts at
//     ActorConfig.StartStamp, so the first block scanned is at
//     StartStamp.Height+1.
//   - A rewind keeps its target block. AddWatchAddrs with rewindTo set to h
//     disconnects every block above h, from the cursor down to h+1, and
//     then scans them again from h+1.
//   - The block subscription delivers its backlog from the height after the
//     one it is given. The actor subscribes at height 0, which asks for no
//     backlog, before it scans anything, and catches up from the header
//     store instead.
//
// # Logging
//
// The neutrino package does not register this package's logger, since that
// would be an import cycle. Its logs are silent until the embedding program
// calls UseLogger.
package rescan
