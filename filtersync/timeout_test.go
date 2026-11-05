package filtersync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCheckpointedCFHeadersBatchTimeoutScales checks that the batch timeout
// scales with the number of queries between its minimum and its maximum.
func TestCheckpointedCFHeadersBatchTimeoutScales(t *testing.T) {
	t.Parallel()

	require.Equal(
		t, checkpointedCFHeadersMinBatchTimeout,
		CheckpointedCFHeadersBatchTimeout(1),
	)

	scaledQueries := int(
		(checkpointedCFHeadersMinBatchTimeout /
			checkpointedCFHeadersPerQueryTimeout) + 1,
	)
	require.Equal(
		t, time.Duration(scaledQueries)*
			checkpointedCFHeadersPerQueryTimeout,
		CheckpointedCFHeadersBatchTimeout(scaledQueries),
	)

	hugeQueries := int(
		(checkpointedCFHeadersMaxBatchTimeout /
			checkpointedCFHeadersPerQueryTimeout) + 1,
	)
	require.Equal(
		t, checkpointedCFHeadersMaxBatchTimeout,
		CheckpointedCFHeadersBatchTimeout(hugeQueries),
	)
}
