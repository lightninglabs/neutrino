package chainsync

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/stretchr/testify/require"
)

// TestValidateCFHeader tests filter header validation against checkpoints,
// including successful validation, mismatch detection, and unknown height
// handling.
func TestValidateCFHeader(t *testing.T) {
	t.Parallel()

	// We'll modify our backing list of checkpoints for this test.
	height := uint32(999)
	header := hashFromStr(
		"4a242283a406a7c089f671bb8df7671e5d5e9ba577cea1047d30a7f4919d" +
			"f193",
	)
	filterHeaderCheckpoints = map[wire.BitcoinNet]map[uint32]*chainhash.Hash{
		chaincfg.MainNetParams.Net: {
			height: header,
		},
	}

	// Expect the control at height to succeed.
	err := ValidateCFHeader(
		chaincfg.MainNetParams, wire.GCSFilterRegular, height, header,
	)
	require.NoError(t, err)

	// Pass an invalid header, this should return an error.
	header = hashFromStr(
		"000000000006a7c089f671bb8df7671e5d5e9ba577cea1047d30a7f4919d" +
			"f193",
	)
	err = ValidateCFHeader(
		chaincfg.MainNetParams, wire.GCSFilterRegular, height, header,
	)
	require.ErrorIs(t, err, ErrCheckpointMismatch)

	// Finally, control an unknown height. This should also pass since we
	// don't have the checkpoint stored.
	err = ValidateCFHeader(
		chaincfg.MainNetParams, wire.GCSFilterRegular, 99, header,
	)
	require.NoError(t, err)
}
