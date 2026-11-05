package headersync

import (
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/lightninglabs/neutrino/headerlist"
)

// ResetTip replaces the validation cache with a single node and moves the
// checkpoint cursor past it. The block manager uses it when it builds a
// runtime, which happens before it can run the driver's events.
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
