package sideload

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/integration/rpctest"
	"github.com/btcsuite/btcd/wire"
	"github.com/stretchr/testify/require"
)

// testNewBinaryBlkHdrLoader sets up a test environment and creates a new binary
// block header loader for testing purposes. It utilizes the provided TestCfg
// to configure the test parameters, including the start and end heights for
// the block headers to be generated. The function performs the following steps:
//  1. Initializes a new rpctest harness with the SimNetParams to simulate
//     a Bitcoin network environment for testing.
//  2. Sets up the test harness, ensuring it's ready for block generation.
//  3. Generates a sequence of valid block headers based on the TestCfg parameters.
//  4. Encodes these headers into a binary format, simulating the kind of data
//     a real binary block header loader might process.
//  5. Instantiates a new binaryBlkHdrLoader with the encoded binary data.
//  6. Returns the loader as a LoaderSource instance, ready for use in tests.
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
