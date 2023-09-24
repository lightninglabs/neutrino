package chaindataloader

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	filePath = "testdata/start_0_end_10_encoded_10_valid_testnet_headers.bin"
)

// TestNewBlockHeaderReader tests that the NewBlockHeaderReader function returns
// the appropriate Reader implementation based on the source type indicated in
// the config.
func TestNewBlockHeaderReader(t *testing.T) {
	type testConfig struct {
		StartHeight int
		SourceType  SourceType
		Path        string
		name        string
		error       error
		reader      bool
	}
	testCases := []testConfig{
		{
			name:       "Unsupported source type",
			SourceType: SourceType(uint8(100)),
			Path:       filePath,
			error:      ErrUnsupportedSourceType,
			reader:     false,
		},
		{
			name:       "supported source type",
			SourceType: Binary,
			Path:       filePath,
			error:      nil,
			reader:     true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			reader, err := os.Open(testCase.Path)

			require.NoError(t, err)

			r, err := NewBlockHeaderReader(&ReaderConfig{
				SourceType: testCase.SourceType,
				Reader:     reader,
			})

			assert.ErrorIs(t, err, testCase.error)

			if r != nil && !testCase.reader {
				t.Errorf("Expected no Reader")
			}
		})
	}
}
