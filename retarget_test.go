package neutrino

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/lightninglabs/neutrino/chainsync"
	"github.com/stretchr/testify/require"
)

// TestInvalidRetargetParameters rejects unsafe parameters before either entry
// point opens stores or attempts retarget arithmetic.
func TestInvalidRetargetParameters(t *testing.T) {
	t.Parallel()

	for _, interval := range []time.Duration{
		0, -time.Second, 500 * time.Millisecond,
		1500 * time.Millisecond, 14*24*time.Hour + time.Second,
	} {
		t.Run(interval.String(), func(t *testing.T) {
			t.Parallel()

			params := chaincfg.SigNetParams
			params.TargetTimePerBlock = interval
			service, err := NewChainService(Config{
				ChainParams: params,
			})
			require.ErrorIs(
				t, err, chainsync.ErrInvalidRetargetParams,
			)
			require.Nil(t, service)

			manager, err := newBlockManager(&blockManagerCfg{
				ChainParams: params,
			})
			require.ErrorIs(
				t, err, chainsync.ErrInvalidRetargetParams,
			)
			require.Nil(t, manager)
		})
	}
}

// TestBlockManagerCustomRetarget checks the retarget parameters consumed by
// live header validation without starting the block manager's goroutines.
func TestBlockManagerCustomRetarget(t *testing.T) {
	t.Parallel()

	base, _, _, err := setupBlockManager(t)
	require.NoError(t, err)

	for _, tc := range []struct {
		interval time.Duration
		blocks   int32
	}{
		{interval: 30 * time.Second, blocks: 40320},
		{interval: 11 * time.Second, blocks: 109963},
	} {
		t.Run(tc.interval.String(), func(t *testing.T) {
			cfg := *base.cfg
			cfg.ChainParams.TargetTimePerBlock = tc.interval
			bm, err := newBlockManager(&cfg)
			require.NoError(t, err)
			require.Equal(t, chainsync.Retarget{
				BlocksPerRetarget:   tc.blocks,
				MinRetargetTimespan: 302400,
				MaxRetargetTimespan: 4838400,
			}, chainsync.Retarget{
				BlocksPerRetarget:   bm.blocksPerRetarget,
				MinRetargetTimespan: bm.minRetargetTimespan,
				MaxRetargetTimespan: bm.maxRetargetTimespan,
			})
		})
	}
}
