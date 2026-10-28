package neutrino

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/btcsuite/btclog"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/banman"
	"github.com/lightninglabs/neutrino/headersync"
	"github.com/stretchr/testify/require"
)

var (
	// dstSwarmRuns is the number of seeds TestHeaderTransportDSTSwarm
	// runs.
	dstSwarmRuns = flag.Int("dst.swarm", 64,
		"number of swarm seeds in TestHeaderTransportDSTSwarm")

	// dstSwarmSeedFlag seeds the swarm campaign's seed generator.
	dstSwarmSeedFlag = flag.Uint64("dst.seed", 1,
		"seed of the swarm campaign generator")
)

// dstInvariants is the reusable oracle run after every simulated event. It
// is an independent statement of what must hold, not another implementation
// of dispatch, stealing, or commit.
type dstInvariants struct {
	d *dstRun

	// lastTip is the highest durable height seen so far.
	lastTip uint32

	// epochs is the highest lease epoch seen per range.
	epochs map[headersync.RangeID]uint64

	// committed is every range seen in a commit event.
	committed map[headersync.RangeID]bool

	// rollbacks counts serial-path rollbacks to a checkpoint.
	rollbacks int

	// reorg is set once the run switched to the reorg branch, which may
	// replace durable headers above the fork point.
	reorg bool

	// skipLeases disables the lease check for fixed tests that tamper
	// with session state directly.
	skipLeases bool
}

// newDSTInvariants returns an oracle for d.
func newDSTInvariants(d *dstRun) *dstInvariants {
	v := &dstInvariants{d: d}
	v.reset()
	return v
}

// reset forgets per-runtime state after a restart rebuilds the actors. The
// durable tip carries over: a restart must never lose committed headers.
func (v *dstInvariants) reset() {
	v.epochs = make(map[headersync.RangeID]uint64)
	v.committed = make(map[headersync.RangeID]bool)
}

// check asserts every per-event invariant.
func (v *dstInvariants) check(event string) {
	v.d.t.Helper()
	if v.d.stopped {
		return
	}
	v.durableChain(event)
	v.memoryMatchesDisk(event)
	v.rangeTable(event)
	v.commitOnce(event)
	if !v.skipLeases {
		v.leases(event)
	}
	v.honestImmunity(event)
}

// fail reports a violated invariant with the run's state.
func (v *dstInvariants) fail(event, format string, args ...any) {
	v.d.t.Helper()
	require.FailNowf(v.d.t, "dst invariant violated",
		"event=%s: %s\n%s", event, fmt.Sprintf(format, args...),
		v.d.describe())
}

// durableChain checks that the durable tip never decreases and that every
// durable header is on the honest chain, written exactly once and in order.
func (v *dstInvariants) durableChain(event string) {
	d := v.d
	_, height, err := d.store.ChainTip()
	require.NoError(d.t, err)
	// The serial path rolls back to the previous checkpoint when a peer
	// serves a header that fails the next one. That discards honest
	// headers, which is legacy policy, but the remaining store must still
	// be honest and never drop below the last checkpoint it verified.
	if height < v.lastTip && !v.reorg {
		v.rollbacks++
		checkpoint := v.d.bm.findPreviousHeaderCheckpoint(
			int32(v.lastTip),
		)
		if height < uint32(checkpoint.Height) {
			v.fail(event, "durable tip went backwards %d -> %d",
				v.lastTip, height)
		}
		v.lastTip = height
	}
	// An overlong reply may carry real headers past the advertised
	// target. Committing them is fine as long as they are honest. After a
	// reorg every height is checked again against the new chain.
	honest := d.transport.chain
	if int(height) >= len(honest) {
		v.fail(event, "durable tip %d past the honest chain", height)
	}
	from := v.lastTip + 1
	if v.reorg {
		from = 1
	}
	if height < v.lastTip && v.reorg {
		v.lastTip = height
	}
	for h := from; h <= height; h++ {
		header, err := d.store.FetchHeaderByHeight(h)
		require.NoError(d.t, err)
		if header.BlockHash() != honest[h].BlockHash() {
			v.fail(event, "durable header %d is off the honest "+
				"chain", h)
		}
	}
	// Writes extend the tip one height at a time. The store itself
	// rejects a write that does not extend its tip, so a height is only
	// written again after a rollback removed it.
	for i, written := range d.store.writes {
		prev := uint32(0)
		if i > 0 {
			prev = d.store.writes[i-1]
		}
		if written != prev+1 &&
			(v.rollbacks == 0 && !v.reorg || written > prev) {

			v.fail(event, "duplicate or skipped durable header "+
				"writes=%v", d.store.writes)
		}
	}
	v.lastTip = height
}

// memoryMatchesDisk checks the visible tip and validation cache against the
// store. Memory may trail disk only after an ambiguous write, and then only
// while a commit retry is armed to reconcile it.
func (v *dstInvariants) memoryMatchesDisk(event string) {
	d := v.d
	bm := d.bm
	if back := bm.headerList.Back(); back == nil ||
		uint32(back.Height) != bm.headerTip {

		v.fail(event, "validation cache %v disagrees with tip %d",
			back, bm.headerTip)
	}
	honest := d.transport.chain
	if bm.headerTip > v.lastTip {
		v.fail(event, "visible tip %d ahead of durable tip %d",
			bm.headerTip, v.lastTip)
	}
	if bm.headerTipHash != honest[bm.headerTip].BlockHash() {
		v.fail(event, "visible tip hash off the honest chain")
	}
	if bm.headerTip < v.lastTip && bm.headerCommitRetryTimer == nil {
		v.fail(event, "visible tip %d trails durable tip %d with no "+
			"commit retry armed", bm.headerTip, v.lastTip)
	}
}

// rangeTable checks unique ownership, lease epochs, and staged payloads.
func (v *dstInvariants) rangeTable(event string) {
	d := v.d
	ctx := d.t.Context()
	manager := d.bm.headerSync.Manager()
	session := d.bm.headerSync.Session()

	owners := make(map[headersync.RangeID]string)
	for _, peer := range d.current {
		if peer == nil {
			continue
		}
		snapshot, ok, err := manager.SnapshotPeer(ctx, peer.Addr())
		require.NoError(d.t, err)
		if !ok {
			continue
		}
		ids := append([]headersync.RangeID(nil),
			snapshot.LocalRanges...)
		if snapshot.ActiveRange != headersync.NoRange {
			ids = append(ids, snapshot.ActiveRange)
		}
		for _, id := range ids {
			if owner, dup := owners[id]; dup {
				v.fail(event, "range %d owned by %s and %s",
					id, owner, peer.Addr())
			}
			owners[id] = peer.Addr()
		}
	}

	honest := dstChains().honest
	for id := headersync.RangeID(1); id < session.NextRangeID(); id++ {
		rng, ok, err := manager.SnapshotRange(ctx, id)
		require.NoError(d.t, err)
		if !ok {
			continue
		}
		if rng.LeaseEpoch < v.epochs[id] {
			v.fail(event, "range %d lease epoch went back %d -> %d",
				id, v.epochs[id], rng.LeaseEpoch)
		}
		v.epochs[id] = rng.LeaseEpoch

		switch rng.State {
		case headersync.RangeOwned, headersync.RangeActive:
			if rng.OwnerPeer != owners[id] &&
				d.slotOfAddr(rng.OwnerPeer) >= 0 {

				v.fail(event, "range %d owner %s not in peer "+
					"table (%s)", id, rng.OwnerPeer,
					owners[id])
			}

		case headersync.RangeCommitted:
			if rng.StopHeight > v.lastTip {
				v.fail(event, "range %d committed past "+
					"durable tip %d", id, v.lastTip)
			}

		case headersync.RangeStaged:
			// An ask that applied but reported an error can
			// leave a staged range without headers. The commit
			// path requeues it once it is next, and the heal
			// phase's quiescence check catches a range that is
			// never recovered.
			staged, ok := session.StagedRange(id)
			if !ok {
				continue
			}
			if !dstMatchesChain(staged.Headers, honest,
				rng.StartHeight) {

				// A lying peer's payload may be staged, but
				// only the durable-chain check decides
				// whether it is ever committed.
				d.faultAddr(staged.PeerID,
					"staged_dishonest")
			}
		}
	}
}

// commitOnce checks that no range is committed twice.
func (v *dstInvariants) commitOnce(event string) {
	committed := make(map[headersync.RangeID]bool)
	for _, e := range v.d.recorder.Events() {
		for _, id := range e.Committed {
			if committed[id] {
				v.fail(event, "range %d committed twice", id)
			}
			committed[id] = true
		}
	}
}

// leases checks that every lease the session tracks has a wire request
// behind it, and that every active range owned by a usable peer has a
// tracked request. A lease without a request can only finish by timeout or
// steal; one owned by a healthy peer would never finish.
func (v *dstInvariants) leases(event string) {
	d := v.d
	ctx := d.t.Context()
	session := d.bm.headerSync.Session()
	manager := d.bm.headerSync.Manager()
	for _, peer := range d.current {
		if peer == nil {
			continue
		}
		id := peer.Addr()
		if session.RangeActive(id) && !d.transport.queuedFor(peer) &&
			d.faulted[id] == "" {

			v.fail(event, "honest peer %s holds a lease with no "+
				"wire request", id)
		}
		snapshot, ok, err := manager.SnapshotPeer(ctx, id)
		require.NoError(d.t, err)
		if !ok || snapshot.ActiveRange == headersync.NoRange {
			continue
		}
		usable := snapshot.State == headersync.PeerReady ||
			snapshot.State == headersync.PeerBusy
		if usable && !session.RangeActive(id) {
			v.fail(event, "peer %s state=%s owns active range %d "+
				"with no tracked request", id, snapshot.State,
				snapshot.ActiveRange)
		}
	}
}

// honestImmunity checks that a peer that never misbehaved or stalled is
// never quarantined, banned, or disconnected, and that no peer is ever
// IP-banned for being unresponsive.
func (v *dstInvariants) honestImmunity(event string) {
	d := v.d
	for addr, reason := range d.punished {
		if d.faulted[addr] != "" {
			continue
		}

		// Known policy gap (F12): when a lying peer answers anchor
		// discovery first, the honest reporter of the conflicting
		// anchor is the one quarantined. Count it instead of failing
		// so the run can still find other bugs.
		if d.anchorLiar && d.cfg.mode.discovery() {
			d.faulted[addr] = "anchor_conflict"
			d.conflictPunishments++
			continue
		}
		v.fail(event, "honest peer %s punished: %s", addr, reason)
	}
	nonResponsive := fmt.Sprintf(":%v", banman.NonResponsivePeer)
	for _, ban := range d.bans {
		if len(ban) >= len(nonResponsive) &&
			ban[len(ban)-len(nonResponsive):] == nonResponsive {

			v.fail(event, "unresponsive peer IP-banned: %s", ban)
		}
	}
	ctx := d.t.Context()
	for _, peer := range d.current {
		if peer == nil || d.faulted[peer.Addr()] != "" {
			continue
		}
		snapshot, ok, err := d.bm.headerSync.Manager().SnapshotPeer(
			ctx, peer.Addr(),
		)
		require.NoError(d.t, err)
		if ok && snapshot.State == headersync.PeerQuarantined {
			v.fail(event, "honest peer %s quarantined",
				peer.Addr())
		}
	}
}

// quiescent returns nil once the run reached the honest tip with no work
// left: nothing active, staged, retrying, or non-terminal.
func (d *dstRun) quiescent() error {
	ctx := d.t.Context()
	session := d.bm.headerSync.Session()
	switch {
	case d.inv.lastTip < d.target:
		return fmt.Errorf("durable tip %d, want %d", d.inv.lastTip,
			d.target)
	case d.bm.headerTip != d.inv.lastTip:
		return fmt.Errorf("visible tip %d, durable tip %d",
			d.bm.headerTip, d.inv.lastTip)
	case session.ActiveRangeCount() != 0:
		return fmt.Errorf("%d active ranges",
			session.ActiveRangeCount())
	case session.ActiveAnchorCount() != 0:
		return fmt.Errorf("%d active anchors",
			session.ActiveAnchorCount())
	case d.bm.headerCommitRetryTimer != nil:
		return fmt.Errorf("commit retry still armed")
	}
	manager := d.bm.headerSync.Manager()
	for id := headersync.RangeID(1); id < session.NextRangeID(); id++ {
		if _, ok := session.StagedRange(id); ok {
			return fmt.Errorf("range %d still staged", id)
		}
		rng, ok, err := manager.SnapshotRange(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		// An overlong reply can teach an honest anchor past what any
		// peer advertises. Work above the target waits for a peer that
		// has it, which is not a stall.
		switch {
		case rng.State == headersync.RangeCommitted,
			rng.State == headersync.RangeFailed,
			rng.State == headersync.RangeStale,
			rng.StartHeight >= d.target:

		default:
			return fmt.Errorf("range %+v not terminal", rng)
		}
	}
	return nil
}

// poisonedAnchor reports whether the FSM holds a confirmed untrusted anchor
// off the honest chain. Under the one-observation policy, a liar's discovery
// reply confirms its own boundary. When its payload is never staged (an
// overlap kept it out), commit-time validation never runs, nothing rejects the
// anchor, and honest replies for the span fail its stop hash (known gap F13).
func (d *dstRun) poisonedAnchor() bool {
	if !d.anchorLiar || !d.cfg.mode.discovery() {
		return false
	}
	anchors, err := d.bm.headerSync.Manager().Anchors(d.t.Context())
	require.NoError(d.t, err)
	honest := dstChains().honest
	for _, anchor := range anchors {
		if anchor.Trusted || int(anchor.Height) >= len(honest) {
			continue
		}
		if anchor.Hash != honest[anchor.Height].BlockHash() &&
			d.bm.headerSync.Controller().AnchorConfirmed(anchor) {

			return true
		}
	}
	return false
}

// dstMatchesChain reports whether headers continue chain after start.
func dstMatchesChain(headers []*wire.BlockHeader, chain []*wire.BlockHeader,
	start uint32) bool {

	for i, header := range headers {
		height := int(start) + 1 + i
		if height >= len(chain) ||
			header.BlockHash() != chain[height].BlockHash() {

			return false
		}
	}
	return true
}

// slotOfAddr returns the slot of the connected peer with addr, or -1.
func (d *dstRun) slotOfAddr(addr string) int {
	for slot, peer := range d.current {
		if peer != nil && peer.Addr() == addr {
			return slot
		}
	}
	return -1
}

// dstReplay is a hex seed for TestHeaderTransportDSTReplay.
var dstReplay = flag.String("dst.replay", "",
	"hex seed replayed by TestHeaderTransportDSTReplay")

// TestHeaderTransportDSTReplay replays one seed given with -dst.replay, for
// example one printed by a failing swarm campaign. Add -dst.log for logs.
func TestHeaderTransportDSTReplay(t *testing.T) {
	if *dstReplay == "" {
		t.Skip("no -dst.replay seed")
	}
	seed, err := hex.DecodeString(*dstReplay)
	require.NoError(t, err)
	runHeaderTransportDST(t, seed)
}

// dstLog enables block manager logging for replaying one seed.
var dstLog = flag.Bool("dst.log", false, "log block manager output")

// enableDSTLog routes block manager logs to stderr when -dst.log is set.
func enableDSTLog() {
	if !*dstLog {
		return
	}
	logger := btclog.NewBackend(os.Stderr).Logger("DST")
	logger.SetLevel(btclog.LevelTrace)
	UseLogger(logger)
}
