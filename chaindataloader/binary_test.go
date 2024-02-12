package chaindataloader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

const (
	testDataDir = "testdata/"
)

type testSideLoadConfig struct {
	name         string
	filePath     string
	endHeight    uint32
	startHeight  uint32
	network      wire.BitcoinNet
	tipHeight    uint32
	err          bool
	tipErr       bool
	dataType     dataType
	dataTypeSize uint32
}

// testBinaryBlockHeader tests fetching block headers from a binary file.
func testBinaryBlockHeader() []testSideLoadConfig {
	testCases := []testSideLoadConfig{

		{
			name:         "invalid network in header file",
			filePath:     "start_2_end_10_encoded_8_invalidNetwork_headers.bin",
			err:          true,
			dataType:     blockHeaders,
			dataTypeSize: uint32(headerfs.BlockHeaderSize),
		},
		{
			name: "valid regtest headers with start height, 2 and end " +
				"height, 10",
			filePath:     "start_2_end_10_encoded_8_valid_regtest_headers.bin",
			endHeight:    10,
			startHeight:  2,
			network:      wire.TestNet,
			tipHeight:    1,
			tipErr:       true,
			err:          false,
			dataType:     blockHeaders,
			dataTypeSize: uint32(headerfs.BlockHeaderSize),
		},
		{
			name: "valid testnet headers with start height, 0 and end " +
				"height, 8",
			filePath:     "start_0_end_8_encoded_8_valid_testnet_headers.bin",
			endHeight:    8,
			startHeight:  0,
			network:      wire.TestNet3,
			tipHeight:    3,
			tipErr:       false,
			err:          false,
			dataType:     blockHeaders,
			dataTypeSize: uint32(headerfs.BlockHeaderSize),
		},
		{
			name: "invalid data type in source",
			filePath: "start_0_end_10_encoded_10_invalidDataType_testnet_" +
				"headers.bin",
			endHeight:    10,
			startHeight:  0,
			tipHeight:    0,
			tipErr:       false,
			network:      wire.TestNet3,
			err:          true,
			dataType:     blockHeaders,
			dataTypeSize: uint32(headerfs.BlockHeaderSize),
		},
		{
			name: "valid testnet headers with start height, 0 and end " +
				"height, 10",
			filePath:     "start_0_end_10_encoded_10_valid_testnet_headers.bin",
			endHeight:    10,
			startHeight:  0,
			tipHeight:    0,
			tipErr:       false,
			network:      wire.TestNet3,
			err:          false,
			dataType:     blockHeaders,
			dataTypeSize: uint32(headerfs.BlockHeaderSize),
		},
	}
	return testCases
}

// TestNewBinarySideLoad tests that the Reader returns the appropriate data
// required for fetching headers when initialized.
func TestNewBinarySideLoad(t *testing.T) {
	testCases := append([]testSideLoadConfig{},
		testBinaryBlockHeader()...,
	)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testFile := filepath.Join(testDataDir, tc.filePath)

			r, err := os.Open(testFile)

			require.NoError(t, err)

			bReader, err := newBinaryChainDataLoader(r, tc.dataType)

			if tc.err {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
			}

			if err != nil {
				return
			}

			require.Equal(t, bReader.startHeight, tc.startHeight)

			require.Equal(t, bReader.endHeight, tc.endHeight)

			require.Equal(t, bReader.chain, tc.network)

			require.Equal(t, bReader.dataTypeSize, tc.dataTypeSize)

			tipErr := bReader.SetHeight(tc.tipHeight)

			if tc.tipErr {
				require.NotNil(t, tipErr)
			} else {
				require.Nil(t, tipErr)
			}

			if tipErr != nil {
				return
			}
		})
	}
}

// TestGenerateBlockHeaders tests fetching headers from the Binary file.
func TestGenerateBlockHeaders(t *testing.T) {
	testFile := filepath.Join(testDataDir,
		"start_0_end_10_encoded_10_valid_testnet_headers.bin")

	r, err := os.Open(testFile)

	require.NoError(t, err)

	reader, err := newBinaryChainDataLoader(r, blockHeaders)

	require.NoError(t, err)

	require.NotNil(t, reader)

	// set height to fetch header at height 1
	err = reader.SetHeight(0)

	if err != nil {
		t.Fatalf("Expected no error but got %v", err)
	}

	firstHeader, err := reader.fetchBlockHeaders(1)

	require.NoError(t, err)

	require.Len(t, firstHeader, 1)

	// After fetching the first header the tracker should advance so that we can
	// fetch the next one.
	secondHeader, err := reader.fetchBlockHeaders(1)

	require.NoError(t, err)

	require.Len(t, secondHeader, 1)

	// Assuming the source file has valid headers,
	// ensure the loader fetches the expected headers.
	require.NotEqual(t, firstHeader[0], secondHeader[0])

	require.NotEqual(t, firstHeader[0], secondHeader[0].PrevBlock)

	// Though the tracker should fetch header at height 3 at this point, setting
	// the height to zero should reset the tracker and also compute the right
	// offset to obtain the header at height one.
	err = reader.SetHeight(0)

	if err != nil {
		t.Fatalf("Expected no error but got %v", err)
	}

	header, err := reader.fetchBlockHeaders(1)

	require.NoError(t, err)

	require.Len(t, header, 1)

	require.Equal(t, firstHeader[0], header[0])
}
