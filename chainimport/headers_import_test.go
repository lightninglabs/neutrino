package chainimport

import (
	"testing"

	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	"github.com/stretchr/testify/require"
)

// TestHeadersConjunctionProperty tests the headers conjunction property for
// the import sources. Both block and filter headers are required to be present
// for the import to start.
func TestHeadersConjunctionProperty(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name         string
		options      *ImportOptions
		expectErr    bool
		expectErrMsg string
	}{
		{
			name: "MissingFilterHeaderImportSource",
			options: &ImportOptions{
				BlockHeadersSource:  "/path/to/blocks",
				FilterHeadersSource: "",
			},
			expectErr: true,
			expectErrMsg: "missing filter headers source " +
				"path",
		},
		{
			name: "MissingBlockHeaderImportSource",
			options: &ImportOptions{
				BlockHeadersSource:  "",
				FilterHeadersSource: "/path/to/filters",
			},
			expectErr: true,
			expectErrMsg: "missing block headers source " +
				"path",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewHeadersImport(tc.options)
			if tc.expectErr {
				require.ErrorContains(t, err, tc.expectErrMsg)
				return
			}
			require.NoError(t, err)
		})
	}
}
