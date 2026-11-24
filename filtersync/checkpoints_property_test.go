package filtersync

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/chainstore"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// TestResolvedCheckpointsCoverTheTip checks, for any set of cfcheckpt replies,
// that every list the Syncer hands to the resolver has exactly one entry per
// checkpoint interval up to the tip. The checkpointed fetch indexes the
// agreed list by the filter tip's interval, so a list shorter than that is
// what made the batch count wrap. With the replies bound to their stop
// height, a short list can't reach it whatever the peers send.
func TestResolvedCheckpointsCoverTheTip(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		tipHeight := rapid.Uint32Range(
			interval, 20*interval,
		).Draw(t, "tip_height")

		// Half the time the chain has a block checkpoint above the
		// tip, so the lists are fetched up to it and then capped.
		var cps []chaincfg.Checkpoint
		if rapid.Bool().Draw(t, "block_checkpoint_above") {
			cpHeight := rapid.Uint32Range(
				tipHeight+1, 25*interval,
			).Draw(t, "block_checkpoint_height")
			cps = append(cps, chaincfg.Checkpoint{
				Height: int32(cpHeight), Hash: cpHash,
			})
		}

		byPeer := make(map[string][]*chainhash.Hash)
		numPeers := rapid.IntRange(0, 5).Draw(t, "num_peers")
		want := int(tipHeight / wire.CFCheckptInterval)
		if len(cps) > 0 {
			want = int(cps[0].Height) / wire.CFCheckptInterval
		}
		honest := make(map[string]bool)
		for i := 0; i < numPeers; i++ {
			// Half the peers serve the honest length for the stop
			// height, the rest any length.
			n := want
			if !rapid.Bool().Draw(t, "honest_len") {
				n = rapid.IntRange(0, 30).Draw(t, "list_len")
			}
			if n == want {
				honest[string(rune('a'+i))] = true
			}
			list := make([]*chainhash.Hash, n)
			for j := range list {
				list[j] = hashOf(byte(j))
			}
			byPeer[string(rune('a'+i))] = list
		}

		s := newTestSyncer(cps...)
		s.Handle(Start{})
		s.Handle(TipsChanged{})
		s.Handle(BlockTipRead{
			Hash:   tipHash,
			Height: tipHeight,
			Tips:   chainstore.Tips{BlockHeight: tipHeight},
		})
		effects := s.Handle(CheckpointsFetched{ByPeer: byPeer})

		// Every list the resolver sees covers the tip exactly, and
		// every honest reply is among them.
		atTip := int(tipHeight / wire.CFCheckptInterval)
		for _, e := range effects {
			rc, ok := e.(ResolveConflict)
			if !ok {
				continue
			}
			for peer, list := range rc.Checkpoints {
				require.Len(t, list, atTip, "peer %v", peer)
			}
			for peer := range honest {
				require.Contains(t, rc.Checkpoints, peer)
			}
		}
		if len(honest) > 0 {
			require.IsType(t, ResolveConflict{}, effects[0])
		}
	})
}
