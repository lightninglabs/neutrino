package headersync

import (
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/lightninglabs/neutrino/headerlist"
)

// ResetTip replaces the validation cache with a single node and moves the
// checkpoint cursor past it. The executor sends it when the durable tip moves
// in a way the cache cannot follow by extension: after a rollback, a reorg or
// a failed store write.
type ResetTip struct {
	// Node is the new cache tip.
	Node headerlist.Node
}

func (ResetTip) driverEvent() {}

// ExtendTip appends nodes that are now durable to the validation cache and
// moves the checkpoint cursor past the last one. The nodes must connect to the
// cache tip and must not be empty.
type ExtendTip struct {
	// Nodes are the new cache entries, oldest first.
	Nodes []headerlist.Node
}

func (ExtendTip) driverEvent() {}

// ReconcileTip fast-forwards the validation cache to the durable block header
// tip. A failure is reported as the error of the event.
type ReconcileTip struct{}

func (ReconcileTip) driverEvent() {}

// PrepareRanges validates staged ranges as one batch that extends the
// validation cache, without changing the cache. The batch is the Prepared
// field of the result. A failure is reported as the error of the event, and is
// a ValidationError when the peer's data is at fault.
type PrepareRanges struct {
	// Ranges are the ranges to validate, in commit order.
	Ranges []RangeCommit
}

func (PrepareRanges) driverEvent() {}

// FinishRange adopts a prepared batch into the validation cache after the
// store has committed it, and logs the checkpoints it verified.
type FinishRange struct {
	// Prepared is the batch the store committed.
	Prepared PreparedCommit
}

func (FinishRange) driverEvent() {}

// ResetTip is the plain-method form of the ResetTip event. The block manager
// uses it when it builds a runtime, which happens before it can run the
// driver's events.
func (d *Driver) ResetTip(node headerlist.Node) {
	d.tip.Reset(node)
}

// Tip returns the newest node of the validation cache, or nil when the cache
// is empty. The node links back to its parents.
func (d *Driver) Tip() *headerlist.Node {
	return d.tip.Tip()
}

// NextCheckpoint returns the first checkpoint above the cache tip, or nil when
// the tip is past the last checkpoint.
func (d *Driver) NextCheckpoint() *chaincfg.Checkpoint {
	return d.tip.NextCheckpoint()
}
