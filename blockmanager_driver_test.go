package neutrino

import "github.com/lightninglabs/neutrino/headersync"

// sessionForTest returns the driver's request bookkeeping, so that a test can
// seed and inspect request state that the driver otherwise only builds
// through events.
func (b *blockManager) sessionForTest() *headersync.Session {
	return b.headerSync.Driver().SessionForTest()
}
