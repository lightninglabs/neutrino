package chainsync

import (
	"math"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/stretchr/testify/require"
)

// TestNewRetargetDefaultNetworks checks the retarget values of the default
// networks. All btcd default networks share mainnet's two week timespan, ten
// minute interval and adjustment factor of four, so they derive identical
// values.
func TestNewRetargetDefaultNetworks(t *testing.T) {
	t.Parallel()

	for _, params := range []chaincfg.Params{
		chaincfg.MainNetParams, chaincfg.TestNet3Params,
		chaincfg.TestNet4Params, chaincfg.RegressionNetParams,
		chaincfg.SimNetParams, chaincfg.SigNetParams,
	} {
		t.Run(params.Name, func(t *testing.T) {
			t.Parallel()

			retarget, err := NewRetarget(params)
			require.NoError(t, err)
			require.Equal(t, Retarget{
				BlocksPerRetarget:   2016,
				MinRetargetTimespan: 302400,
				MaxRetargetTimespan: 4838400,
			}, retarget)
		})
	}
}

// TestNewRetargetCustomIntervals checks whole-second custom block intervals.
func TestNewRetargetCustomIntervals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		interval time.Duration
		blocks   int32
	}{
		{interval: time.Second, blocks: 1209600},
		{interval: 30 * time.Second, blocks: 40320},
		{interval: 11 * time.Second, blocks: 109963},
		{interval: 14 * 24 * time.Hour, blocks: 1},
	}
	for _, tc := range tests {
		t.Run(tc.interval.String(), func(t *testing.T) {
			t.Parallel()

			params := chaincfg.SigNetParams
			params.TargetTimePerBlock = tc.interval
			retarget, err := NewRetarget(params)
			require.NoError(t, err)
			require.Equal(t, Retarget{
				BlocksPerRetarget:   tc.blocks,
				MinRetargetTimespan: 302400,
				MaxRetargetTimespan: 4838400,
			}, retarget)
		})
	}
}

// TestNewRetargetInvalidParameters rejects fractional durations and parameters
// that would cause invalid or overflowing retarget calculations.
func TestNewRetargetInvalidParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		change func(*chaincfg.Params)
	}{
		{
			name: "zero interval",
			change: func(p *chaincfg.Params) {
				p.TargetTimePerBlock = 0
			},
		},
		{
			name: "negative interval",
			change: func(p *chaincfg.Params) {
				p.TargetTimePerBlock = -time.Second
			},
		},
		{
			name: "subsecond interval",
			change: func(p *chaincfg.Params) {
				p.TargetTimePerBlock = 500 * time.Millisecond
			},
		},
		{
			name: "fractional seconds",
			change: func(p *chaincfg.Params) {
				p.TargetTimePerBlock = 1500 * time.Millisecond
			},
		},
		{
			name: "interval above timespan",
			change: func(p *chaincfg.Params) {
				p.TargetTimePerBlock = p.TargetTimespan +
					time.Second
			},
		},
		{
			name: "zero timespan",
			change: func(p *chaincfg.Params) {
				p.TargetTimespan = 0
			},
		},
		{
			name: "negative timespan",
			change: func(p *chaincfg.Params) {
				p.TargetTimespan = -time.Second
			},
		},
		{
			name: "fractional timespan",
			change: func(p *chaincfg.Params) {
				p.TargetTimespan += 500 * time.Millisecond
			},
		},
		{
			name: "zero adjustment factor",
			change: func(p *chaincfg.Params) {
				p.RetargetAdjustmentFactor = 0
			},
		},
		{
			name: "negative adjustment factor",
			change: func(p *chaincfg.Params) {
				p.RetargetAdjustmentFactor = -1
			},
		},
		{
			name: "retarget block count overflow",
			change: func(p *chaincfg.Params) {
				p.TargetTimespan = (math.MaxInt32 + 1) *
					time.Second
				p.TargetTimePerBlock = time.Second
			},
		},
		{
			name: "zero minimum retarget timespan",
			change: func(p *chaincfg.Params) {
				p.RetargetAdjustmentFactor = 1209601
			},
		},
		{
			name: "maximum retarget timespan overflow",
			change: func(p *chaincfg.Params) {
				p.TargetTimespan = time.Duration(math.MaxInt64)
				p.TargetTimespan -= p.TargetTimespan %
					time.Second
				p.RetargetAdjustmentFactor = 9000000000
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			params := chaincfg.SigNetParams
			tc.change(&params)
			retarget, err := NewRetarget(params)
			require.ErrorIs(t, err, ErrInvalidRetargetParams)
			require.Equal(t, Retarget{}, retarget)
		})
	}
}
