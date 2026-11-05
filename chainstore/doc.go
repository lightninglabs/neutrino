// Package chainstore serializes every mutation of the block header store and
// the filter header store behind one actor.
//
// The two stores must agree: a filter header is only valid for the block it
// was derived from, so a filter batch can't land on a block that a concurrent
// rollback just removed. Before this package, the block handler and the filter
// handler each wrote on their own goroutine, and that binding held only by
// convention. Now both submit commands to a Client, and a Store executes them
// one at a time.
//
// Three properties hold across every command:
//
//   - A store can apply a write and then report an error. We never trust the
//     returned error alone: we read the durable tip, verify that it's the
//     expected parent or a prefix of the submitted batch, and publish exactly
//     that much.
//
//   - A rollback that has been admitted always finishes, even across failed
//     steps. The Store retains it as a rollbackIntent, and every later command
//     resumes it before doing anything else.
//
//   - Notifications follow durability. A disconnect is sent only after the
//     block is gone from the store, and a connect only after the block's
//     filter header is durable.
//
// The Store owns no published state. It reports tip changes and notifications
// through a Host, which today is the block manager.
package chainstore
