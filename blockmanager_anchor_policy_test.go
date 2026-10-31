package neutrino

import (
	"testing"
	"testing/synctest"
	"time"

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

// poisonAnchor leaves the run with a confirmed, unvalidated liar anchor at
// height 8 and a range [4, 8] planned on it and leased to a peer. The first
// honest reply commits [0, 4], then the peers' discovery requests are dropped
// so that the injected anchor, not discovery, drives the next span. It
// returns the slot holding the poisoned range.
func (d *dstRun) poisonAnchor() (int, headersync.Anchor) {
	d.t.Helper()
	ctx := d.t.Context()
	require.True(d.t, d.reply(0, dstReplyHonest))
	require.EqualValues(d.t, 4, d.inv.lastTip)

	session := d.bm.headerSync.Session()
	for _, peer := range d.current {
		session.FinishAnchor(peer.Addr())
		for d.transport.queuedFor(peer) {
			d.transport.take(peer)
		}
	}

	// The liar's boundary at height 8 never matches the honest chain.
	hash := dstChains().badTime[4][3].BlockHash()
	anchor, ok, err := d.bm.headerSync.Manager().AddAnchor(
		ctx, 8, hash, false,
	)
	require.NoError(d.t, err)
	require.True(d.t, ok)
	require.True(d.t, d.bm.headerSync.Controller().AnchorConfirmed(anchor),
		"one observation confirms the liar's anchor")

	planned, err := d.bm.planHeaderSyncRanges(8)
	require.NoError(d.t, err)
	require.Equal(d.t, 1, planned)
	require.True(d.t, d.bm.dispatchHeaderSyncWork(d.peers))
	d.pump()

	for slot, peer := range d.current {
		if session.RangeActive(peer.Addr()) {
			return slot, anchor
		}
	}
	d.t.Fatal("no peer leased the range planned on the liar's anchor")
	return -1, anchor
}

// TestHeaderStopContradictionRejectsUnvalidatedAnchor checks the policy for
// honest replies that fail the stop hash of an anchor no staged range has
// validated: the replier is not punished. The anchor is rejected, the work
// planned on it is retired, the replier is held back for the cooldown, and
// the sync still reaches the honest tip.
func TestHeaderStopContradictionRejectsUnvalidatedAnchor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeFrontier1})
		slot, poisoned := d.poisonAnchor()
		peer := d.current[slot]

		require.True(t, d.reply(slot, dstReplyHonest))
		require.Empty(t, d.punished, "an honest replier was punished "+
			"for contradicting an unvalidated anchor")

		anchor, ok := d.anchorAt(8)
		require.True(t, ok)
		require.Equal(t, poisoned.Hash, anchor.Hash)
		require.True(t, anchor.Rejected)
		require.False(t, d.bm.headerSync.Controller().AnchorConfirmed(
			anchor,
		))

		// The hold is the existing cooldown, which bounds the loop: a
		// blocked peer is leased no range, and a range reply is the
		// only way to contradict an anchor this way.
		manager := d.bm.headerSync.Manager()
		snapshot, ok, err := manager.SnapshotPeer(
			t.Context(), peer.Addr(),
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, headersync.PeerBlocked, snapshot.State)
		until, cooling := d.bm.headerSync.Session().CooldownUntil(
			peer.Addr(), time.Now(),
		)
		require.True(t, cooling)
		require.Equal(t, defaultHeaderPeerCooldown,
			time.Until(until).Round(time.Second))
		require.False(t, d.bm.headerSync.Session().RangeActive(
			peer.Addr(),
		))

		d.heal()
		require.NoError(t, d.quiescent())
		d.check("contradiction_healed")
		require.EqualValues(t, dstChainHeight, d.inv.lastTip)
	})
}

// TestHeaderStopContradictionOfTrustedAnchorPunishes checks that the lenient
// policy stops at validated boundaries. A checkpoint is trusted, so a reply
// that holds another header at a checkpoint-backed stop height is the peer's
// fault and is still punished.
func TestHeaderStopContradictionOfTrustedAnchorPunishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeCheckpointAll})
		var liar *ServerPeer
		for slot, peer := range d.current {
			if d.transport.queuedFor(peer) {
				liar = peer
				require.True(t, d.reply(slot, dstReplyFork))
				break
			}
		}
		require.NotNil(t, liar, "no range request was queued")
		require.Equal(t, "quarantine", d.punished[liar.Addr()],
			"a reply that contradicts a checkpoint went unpunished")
	})
}

// TestHeaderLiarPunishedWhenCommitProvesIt checks that dropping the immediate
// punishment does not let a liar go free once its payload is staged. A
// confirmed liar anchor whose range commits is validated contextually, the
// payload fails, and the liar alone is quarantined.
func TestHeaderLiarPunishedWhenCommitProvesIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newDSTRun(t, dstConfig{mode: dstModeFrontier1})
		liar := d.current[0].Addr()
		require.True(t, d.reply(0, dstReplyBadTime))
		require.Equal(t, "quarantine", d.punished[liar])

		d.heal()
		require.NoError(t, d.quiescent())
		for addr, reason := range d.punished {
			require.Equal(t, liar, addr,
				"honest peer punished: %s", reason)
		}
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
