package headersync

// SessionForTest returns the driver's request bookkeeping. Tests use it to
// seed and inspect request state that the driver otherwise only builds up
// through events. Production code must not call it: a read races with Handle,
// and a write bypasses the invariants the driver keeps.
func (d *Driver) SessionForTest() *Session {
	return d.session
}
