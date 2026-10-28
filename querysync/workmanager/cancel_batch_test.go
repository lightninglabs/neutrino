package workmanager

import (
	"testing"
	"time"

	"github.com/lightninglabs/neutrino/querysync"
	"github.com/stretchr/testify/require"
)

// TestCancelBatchPreservesOtherWork checks that an accepted batch is retired
// independently and that its late result cannot affect scheduler accounting.
func TestCancelBatchPreservesOtherWork(t *testing.T) {
	events := &querysync.SchedulerEventRecorder{}
	wm, workers := startWorkManager(t, 1, nil, events)
	defer func() {
		require.NoError(t, wm.Stop())
	}()

	cancel := make(chan struct{})
	first := wm.Query([]*Request{{}}, Cancel(cancel))
	oldJob := recvJob(t, workers[0], first)
	second := wm.Query([]*Request{{}})
	close(cancel)

	select {
	case err := <-first:
		require.ErrorIs(t, err, ErrJobCanceled)
	case <-time.After(time.Second):
		t.Fatal("canceled batch was not retired")
	}

	// The worker can return the old result after the batch was retired.
	// Its slot must open for the other batch without recording that result.
	sendResult(t, workers[0], oldJob, nil)
	newJob := recvJob(t, workers[0], second)
	sendResult(t, workers[0], newJob, nil)
	requireQueryDone(t, second)

	var results []querysync.SchedulerEvent
	for _, event := range events.Events() {
		if event.Type == querysync.SchedulerEventTaskResult {
			results = append(results, event)
		}
	}
	require.Len(t, results, 1)
	require.EqualValues(t, newJob.index, results[0].TaskID)
	require.True(t, results[0].OK)
}
