package rescan

import (
	"errors"
	"os"
	"testing"

	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// TestStoreErrClassification checks which header store errors end the
// rescan: a missing header is a reorg race and is retried, and any other
// read failure ends it. The headerfs tests cover the flat file store's
// wrapped read errors.
func TestStoreErrClassification(t *testing.T) {
	notFound := []error{
		headerfs.ErrHashNotFound,
		headerfs.ErrHeightNotFound,
	}
	for _, err := range notFound {
		require.Equal(t, err, storeErr(err), "%v", err)
	}

	fatal := []error{
		os.ErrClosed,
		errors.New("database not open"),
	}
	for _, err := range fatal {
		var storeError *HeaderStoreError
		require.ErrorAs(t, storeErr(err), &storeError, "%v", err)
		require.ErrorIs(t, storeErr(err), err)
	}
	require.NoError(t, storeErr(nil))
}
