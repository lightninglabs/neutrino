package filtersync

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// reorgWorld is the chain and the peers a Syncer runs against in
// TestSyncerSurvivesReorgs. Its tip can be replaced between any two events.
// Honest peers serve checkpoints only for a stop block they still know, and
// every checkpoint they serve is derived from its stop hash, so a list can be
// traced back to the block it was fetched for.
type reorgWorld struct {
	// height and gen identify the block header tip. Each reorg bumps gen,
	// which replaces the tip hash.
	height uint32
	gen    uint32

	// cp is the block checkpoint above every tip, if the chain has one.
	cp *chaincfg.Checkpoint

	// peers is the number of honest peers.
	peers int
}

// tipHash returns the hash of the current block header tip.
func (w *reorgWorld) tipHash() chainhash.Hash {
	var b [8]byte
	binary.BigEndian.PutUint32(b[:4], w.gen)
	binary.BigEndian.PutUint32(b[4:], w.height)

	return chainhash.DoubleHashH(b[:])
}

// tips returns the tip feed snapshot.
func (w *reorgWorld) tips() chainstore.Tips {
	return chainstore.Tips{BlockHeight: w.height, BlockHash: w.tipHash()}
}

// stopHeight returns the height of a stop block peers still serve, and false
// if no peer knows it any more.
func (w *reorgWorld) stopHeight(stop chainhash.Hash) (uint32, bool) {
	switch {
	case stop == w.tipHash():
		return w.height, true

	case w.cp != nil && stop == *w.cp.Hash:
		return uint32(w.cp.Height), true
	}

	return 0, false
}

// checkpointOf returns the checkpoint an honest peer serves at index i for
// the chain that ends at stop.
func checkpointOf(stop chainhash.Hash, i int) *chainhash.Hash {
	h := chainhash.DoubleHashH(append(stop[:], byte(i), byte(i>>8)))

	return &h
}

// TestSyncerSurvivesReorgs drives a Syncer through a run of reorgs and failed
// rounds, then lets the world settle, and checks two things. Every agreed
// checkpoint list the Syncer hands to the checkpointed fetch was served for
// the tip the fetch is bound to (or the block checkpoint above it), so the
// fetch can't ban honest peers for a list of another chain. And once the
// world settles, the Syncer reaches the tip within a bounded number of
// events, whatever tip it was on when the faults stopped.
func TestSyncerSurvivesReorgs(t *testing.T) {
	t.Parallel()

	rapid.Check(t, testSyncerSurvivesReorgs)
}

func testSyncerSurvivesReorgs(t *rapid.T) {
	w := &reorgWorld{
		height: rapid.Uint32Range(
			interval, 6*interval,
		).Draw(t, "height"),
		peers: rapid.IntRange(1, 3).Draw(t, "peers"),
	}
	if rapid.Bool().Draw(t, "block_checkpoint") {
		w.cp = &chaincfg.Checkpoint{
			Height: int32(10 * interval), Hash: cpHash,
		}
	}

	var cps []chaincfg.Checkpoint
	if w.cp != nil {
		cps = append(cps, *w.cp)
	}
	s := newTestSyncer(cps...)

	// The faulty part of the run, then the settled part, which must reach
	// the tip within settleBudget events.
	const settleBudget = 40
	faulty := rapid.IntRange(0, 80).Draw(t, "faulty_events")
	total := faulty + settleBudget

	effects := s.Handle(Start{Tips: w.tips()})
	for step := 0; step < total; step++ {
		chaos := step < faulty
		if chaos && rapid.IntRange(0, 3).Draw(t, "reorg") == 0 {
			w.gen++
			w.height = rapid.Uint32Range(
				interval, 6*interval,
			).Draw(t, "reorg_height")
		}
		fail := func(label string) bool {
			return chaos && rapid.IntRange(0, 2).Draw(t, label) == 0
		}

		var event Event
		for _, effect := range effects {
			event = answer(t, w, effect, fail)
			if event != nil {
				break
			}
		}
		require.NotNil(t, event, "batch %v yields no event", effects)

		effects = s.Handle(event)
		if !chaos && s.Phase() == PhaseWaitingAtTip {
			return
		}
	}

	t.Fatalf("syncer didn't reach the tip within %d events of the "+
		"world settling, phase %v", settleBudget, s.Phase())
}

// answer performs one effect against the world and returns the event it
// yields, or nil for a PollQuit. fail decides whether a round fails.
func answer(t *rapid.T, w *reorgWorld, effect Effect,
	fail func(string) bool) Event {

	switch e := effect.(type) {
	case PollQuit:
		return nil

	case WaitForTips:
		return TipsChanged{Tips: w.tips()}

	case ReadBlockTip:
		if fail("read_fails") {
			return BlockTipRead{Err: errTest}
		}

		return BlockTipRead{
			Hash: w.tipHash(), Height: w.height, Tips: w.tips(),
		}

	case FetchCheckpoints:
		byPeer := make(map[string][]*chainhash.Hash)
		height, ok := w.stopHeight(e.StopHash)
		if !ok {
			return CheckpointsFetched{ByPeer: byPeer}
		}
		for p := 0; p < w.peers; p++ {
			if fail("peer_silent") {
				continue
			}
			n := int(height / interval)
			list := make([]*chainhash.Hash, n)
			for i := range list {
				list[i] = checkpointOf(e.StopHash, i)
			}
			byPeer[fmt.Sprintf("peer%d", p)] = list
		}

		return CheckpointsFetched{ByPeer: byPeer}

	case ResolveConflict:
		if fail("resolve_fails") {
			return ConflictResolved{Err: errTest}
		}

		// The lists agree with each other whenever they were served
		// for the same stop block, which every reply of one fetch is.
		for _, list := range e.Checkpoints {
			return ConflictResolved{Good: list}
		}

		return ConflictResolved{}

	case FetchCheckpointed:
		// The binding the executor relies on: every checkpoint was
		// served for the chain that ends at the fetch's tip, or at the
		// block checkpoint above it.
		for i, cp := range e.Checkpoints {
			forTip := *cp == *checkpointOf(e.TipHash, i)
			forCp := w.cp != nil &&
				*cp == *checkpointOf(*w.cp.Hash, i)
			require.Truef(t, forTip || forCp, "checkpoint %d of "+
				"the fetch bound to %v was served for "+
				"another chain", i, e.TipHash)
		}

		// The executor refuses a fetch whose tip was replaced.
		if e.TipHash != w.tipHash() || e.TipHeight != w.height {
			return CheckpointedFetched{Err: errTest}
		}
		if fail("fetch_fails") {
			return CheckpointedFetched{Err: errTest}
		}

		return CheckpointedFetched{
			HeadersSynced: true,
			Tips: chainstore.Tips{
				BlockHeight:  w.height,
				BlockHash:    w.tipHash(),
				FilterHeight: w.height,
				FilterHash:   w.tipHash(),
			},
		}

	case FetchUncheckpointed:
		return UncheckpointedFetched{}

	case RetryAfter:
		return RetryElapsed{Tips: w.tips()}
	}

	t.Fatalf("unknown effect %T", effect)

	return nil
}
