package headersync

import (
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/lightninglabs/neutrino/headerlist"
)

// SessionForTest returns the driver's request bookkeeping. Tests use it to
// seed and inspect request state that the driver otherwise only builds up
// through events. Production code must not call it: a read races with Handle,
// and a write bypasses the invariants the driver keeps.
func (d *Driver) SessionForTest() *Session {
	return d.session
}

// ManagerForTest returns the manager. Tests use it to seed and inspect
// FSM state. Production code goes through the driver.
func (r *Runtime) ManagerForTest() *Manager {
	return r.manager
}

// ControllerForTest returns the controller, for the same reason.
func (r *Runtime) ControllerForTest() *Controller {
	return r.controller
}

// SetNextCheckpointForTest overrides the checkpoint cursor. Tests use it to
// aim validation at a checkpoint that the chain parameters do not hold.
// Production code moves the cursor through Reset and Extend.
func (t *TipState) SetNextCheckpointForTest(next *chaincfg.Checkpoint) {
	t.nextCheckpoint = next
}

// PushBackForTest appends a node to the validation cache without moving the
// checkpoint cursor. Tests use it to seed the cache. Production code goes
// through Extend, which also moves the cursor.
func (t *TipState) PushBackForTest(node headerlist.Node) {
	t.headerList.PushBack(node)
}
