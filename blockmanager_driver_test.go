package neutrino

import (
	"container/list"

	"github.com/lightninglabs/neutrino/headersync"
)

// tipForTest returns the validation cache owned by the driver.
func (b *blockManager) tipForTest() *headersync.TipState {
	return b.headerSync.Driver().TipForTest()
}

// sessionForTest returns the driver's request bookkeeping, so that a test can
// seed and inspect request state that the driver otherwise only builds
// through events.
func (b *blockManager) sessionForTest() *headersync.Session {
	return b.headerSync.Driver().SessionForTest()
}

// planHeaderSyncRanges plans the ranges up to the best height and reports how
// many the driver planned. Planning takes one range ID per range, and leaves
// the cursor alone when it fails, so the cursor's advance is the count.
func (b *blockManager) planHeaderSyncRanges(bestHeight uint32) (int, error) {
	session := b.sessionForTest()
	before := session.NextRangeID()
	outcome := b.handleDriverEvent(nil, headersync.PlanRanges{
		BestHeight: bestHeight,
	})

	return int(session.NextRangeID() - before), outcome.err
}

// dispatchHeaderSyncWork hands queued ranges to the candidates and reports
// whether any request was written.
func (b *blockManager) dispatchHeaderSyncWork(peers *list.List) bool {
	return b.handleDriverEvent(peers, headersync.DispatchWork{}).sent > 0
}

// startHeaderAnchorDiscovery continues sync, which restarts anchor discovery
// above the durable tip, and reports whether a discovery request is in
// flight afterwards.
func (b *blockManager) startHeaderAnchorDiscovery(peers *list.List) bool {
	b.handleDriverEvent(peers, headersync.Continue{})

	return b.sessionForTest().ActiveAnchorCount() > 0
}

// updateHeaderSyncPeerState moves a peer to a scheduling state.
func (b *blockManager) updateHeaderSyncPeerState(sp *ServerPeer,
	state headersync.PeerState) {

	b.handleDriverEvent(nil, headersync.PeerStateChanged{
		PeerID: sp.Addr(),
		State:  state,
	})
}

// commitReadyHeaderSyncRanges asks the driver to commit the staged ranges
// that are ready. The error is a ValidationError when a peer's data is at
// fault.
func (b *blockManager) commitReadyHeaderSyncRanges() error {
	return b.handleDriverEvent(nil, headersync.CommitReady{}).err
}
