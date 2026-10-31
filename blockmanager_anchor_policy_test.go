package neutrino

import (
	"testing"
	"testing/synctest"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/lightninglabs/neutrino/headersync"
	"github.com/stretchr/testify/require"
)

// queuedDiscovery returns the slots whose peers have a discovery request
// queued for the span that starts at the durable tip.
func (d *dstRun) queuedDiscovery() []int {
	var slots []int
	for slot, peer := range d.current {
		if peer == nil {
			continue
		}
		for _, request := range d.transport.queued {
			if request.peer == peer && !request.stopKnown {
				slots = append(slots, slot)
				break
			}
		}
	}
	return slots
}

// anchorAt returns the anchor the FSM holds at height.
func (d *dstRun) anchorAt(height uint32) (headersync.Anchor, bool) {
	anchors, err := d.bm.headerSync.Manager().Anchors(d.t.Context())
	require.NoError(d.t, err)
	for _, anchor := range anchors {
		if anchor.Height == height {
			return anchor, true
		}
	}
	return headersync.Anchor{}, false
}

// TestHeaderAnchorConflictPunishesNobody checks the policy for two peers that
// report different boundaries for one height: neither is punished, because
// nothing yet shows which one lied. The recorded anchor is rejected, the span
// is discovered again, and the sync still reaches the honest tip.
func TestHeaderAnchorConflictPunishesNobody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeFrontier2})
		slots := d.queuedDiscovery()
		require.GreaterOrEqual(t, len(slots), 2, "need two reporters")

		// The first reporter lies. Two observations are required, so
		// its anchor is recorded but not yet confirmed or staged.
		var request dstHeaderRequest
		for _, queued := range d.transport.queued {
			if queued.peer == d.current[slots[0]] {
				request = queued
			}
		}
		require.True(t, d.reply(slots[0], dstReplyBadTime))
		recorded, ok := d.anchorAt(request.stop)
		require.True(t, ok)
		require.False(t, recorded.Rejected)

		// The honest peer disagrees with the recorded anchor.
		require.True(t, d.reply(slots[1], dstReplyHonest))

		anchor, ok := d.anchorAt(request.stop)
		require.True(t, ok)
		require.True(t, anchor.Rejected || anchor.Hash !=
			recorded.Hash, "conflict left the anchor in place")
		require.Empty(t, d.punished, "a reporter was punished for a "+
			"conflict that proves nothing")
		honest := d.current[slots[1]]
		snapshot, ok, err := d.bm.headerSync.Manager().SnapshotPeer(
			t.Context(), honest.Addr(),
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, headersync.PeerReady, snapshot.State,
			"the conflicting reporter must not be held back")

		d.heal()
		require.NoError(t, d.quiescent())
		d.check("conflict_healed")
	})
}

// TestHeaderConflictWithTrustedAnchorReplansNothing checks that a discovery
// reply contradicting a boundary that already committed rejects nothing, and
// so asks no peer for the span again. It is not punished either: a short
// reorg looks the same.
func TestHeaderConflictWithTrustedAnchorReplansNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeFrontier1})

		// The first honest reply commits [0, 4], which trusts height 4.
		require.True(t, d.reply(0, dstReplyHonest))
		require.EqualValues(t, 4, d.inv.lastTip)
		trusted, ok := d.anchorAt(4)
		require.True(t, ok)
		require.True(t, trusted.Trusted)

		// A hedged peer's request for the same span is still tracked,
		// and the peer answers with another chain. The commit pruned
		// the real one, so track it again.
		peer := d.current[1]
		session := d.bm.headerSync.Session()
		session.FinishAnchor(peer.Addr())
		for d.transport.queuedFor(peer) {
			d.transport.take(peer)
		}
		genesis := chaincfg.SimNetParams.GenesisBlock.Header.BlockHash()
		session.TrackAnchor(peer.Addr(), headersync.AnchorRequest{
			StartHeight: 0, StartHash: genesis, StopHeight: 4,
		})
		sent := d.transport.sent
		d.deliver(&headersMsg{
			peer: peer,
			headers: d.answer(dstHeaderRequest{
				peer: peer, start: 0, stop: 4,
			}, dstReplyBadTime),
		})
		require.Equal(t, sent, d.transport.sent,
			"a conflict that rejected nothing asked for the span "+
				"again")
		require.False(t, session.AnchorActive(peer.Addr()),
			"the reply did not consume the request, so the test "+
				"proves nothing")
		require.Empty(t, d.punished)

		anchor, ok := d.anchorAt(4)
		require.True(t, ok)
		require.True(t, anchor.Trusted)
		require.False(t, anchor.Rejected)
	})
}
