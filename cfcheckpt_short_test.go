package neutrino

import (
	"testing"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// tipOnlyFilterStore is a filter header store that only answers ChainTip.
type tipOnlyFilterStore struct {
	headerfs.FilterHeaderStore

	height uint32
}

// ChainTip returns a tip at the store's height.
func (s *tipOnlyFilterStore) ChainTip() (*chainhash.Hash, uint32, error) {
	return &chainhash.Hash{}, s.height, nil
}

// TestCheckpointedFetchRejectsShortCheckpoints checks that a checkpoint list
// that doesn't reach the filter header tip is refused, rather than wrapping
// the batch count and sizing billions of queries from it.
func TestCheckpointedFetchRejectsShortCheckpoints(t *testing.T) {
	t.Parallel()

	c := &cfHeaderSync{cfg: &blockManagerCfg{}, quit: make(chan struct{})}
	store := &tipOnlyFilterStore{height: 870_000}

	// One checkpoint, while the store's tip is 870 intervals in.
	checkpoints := []*chainhash.Hash{{}}
	err := c.getCheckpointedCFHeaders(
		checkpoints, store, wire.GCSFilterRegular,
	)
	require.ErrorContains(t, err, "don't reach the filter header tip")
}
