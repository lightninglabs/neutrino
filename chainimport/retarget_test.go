package chainimport

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/lightninglabs/neutrino/chainsync"
	"github.com/stretchr/testify/require"
)

// TestImportInvalidRetargetParameters rejects fractional and unsafe intervals
// before any import source or destination store is used.
func TestImportInvalidRetargetParameters(t *testing.T) {
	t.Parallel()

	for _, interval := range []time.Duration{
		0, -time.Second, 500 * time.Millisecond,
		1500 * time.Millisecond, 14*24*time.Hour + time.Second,
	} {
		t.Run(interval.String(), func(t *testing.T) {
			t.Parallel()

			params := chaincfg.SigNetParams
			params.TargetTimePerBlock = interval
			importer, err := NewHeadersImport(&ImportOptions{
				TargetChainParams:   params,
				BlockHeadersSource:  "unused-block-headers",
				FilterHeadersSource: "unused-filter-headers",
			})
			require.ErrorIs(
				t, err, chainsync.ErrInvalidRetargetParams,
			)
			require.Nil(t, importer)
		})
	}
}

// TestImportCustomRetarget checks the parameters consumed by header import.
func TestImportCustomRetarget(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		interval time.Duration
		blocks   int32
	}{
		{interval: 30 * time.Second, blocks: 40320},
		{interval: 11 * time.Second, blocks: 109963},
	} {
		t.Run(tc.interval.String(), func(t *testing.T) {
			t.Parallel()

			params := chaincfg.SigNetParams
			params.TargetTimePerBlock = tc.interval
			validator, err := newBlockHeadersImportSourceValidator(
				params, nil, blockchain.BFNone, nil,
			)
			require.NoError(t, err)

			v := validator.(*blockHeadersImportSourceValidator)
			require.Equal(t, chainsync.Retarget{
				BlocksPerRetarget:   tc.blocks,
				MinRetargetTimespan: 302400,
				MaxRetargetTimespan: 4838400,
			}, v.retarget)
		})
	}
}
