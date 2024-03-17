package sideload

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/integration/rpctest"
	"github.com/btcsuite/btcd/wire"
	"github.com/stretchr/testify/require"
)

func testNewBinaryBlkHdrLoader(t *testing.T,
	testBin *TestCfg) LoaderSource[*wire.BlockHeader] {

	harness, err := rpctest.New(
		&chaincfg.SimNetParams, nil, []string{"--txindex"}, "",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, harness.TearDown())
	})

	err = harness.SetUp(false, 0)
	require.NoError(t, err)

	headers := GenerateValidBlockHeaders(
		uint32(testBin.EndHeight-testBin.StartHeight), harness, t,
	)
	bLoader, err := newBinaryBlkHdrLoader(GenerateEncodedBinaryReader(
		t, testBin, headers),
	)
	require.NoError(t, err)

	return bLoader
}
