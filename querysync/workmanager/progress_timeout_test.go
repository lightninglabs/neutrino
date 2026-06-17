package workmanager

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/lightninglabs/neutrino/query"
	"github.com/stretchr/testify/require"
)

// TestWorkManagerProgressTimeout preserves the upstream batch timeout
// contract.
func TestWorkManagerProgressTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		hard    time.Duration
		idle    time.Duration
		advance time.Duration
	}{
		{
			name: "idle without peers", hard: time.Hour,
			idle: 10 * time.Second, advance: 11 * time.Second,
		},
		{
			name: "hard cap wins", hard: 5 * time.Second,
			idle: 10 * time.Second, advance: 6 * time.Second,
		},
		{
			name: "zero hard cap fires immediately",
			idle: 10 * time.Second, advance: time.Nanosecond,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				wm, _ := startWorkManager(t, 0, nil, nil)
				defer func() { require.NoError(t, wm.Stop()) }()
				result := wm.Query(
					[]*query.Request{{}},
					query.Timeout(
						tc.hard,
					),
					query.ProgressTimeout(tc.idle),
				)
				synctest.Wait()
				time.Sleep(tc.advance)
				synctest.Wait()
				select {
				case err := <-result:
					require.ErrorIs(
						t,
						err,
						query.ErrQueryTimeout,
					)
				default:
					t.Fatal("batch did not time out")
				}
			})
		})
	}
}

// TestWorkManagerProgressTimeoutReset checks successful completion re-arms
// idle time.
func TestWorkManagerProgressTimeoutReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wm, workers := startWorkManager(t, 1, nil, nil)
		defer func() { require.NoError(t, wm.Stop()) }()
		synctest.Wait()
		result := wm.Query(
			[]*query.Request{{}, {}},
			query.Timeout(
				time.Hour,
			),
			query.ProgressTimeout(10*time.Second),
		)
		job := recvJob(t, workers[0], result)
		synctest.Wait()
		time.Sleep(6 * time.Second)
		sendResult(t, workers[0], job, nil)
		synctest.Wait()
		last := recvJob(t, workers[0], result)
		time.Sleep(6 * time.Second)
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf(
				"successful progress did not reset idle "+
					"timeout: %v",
				err,
			)
		default:
		}
		sendResult(t, workers[0], last, nil)
		requireQueryDone(t, result)
		// A completed batch's timer cannot cancel a later batch.
		time.Sleep(11 * time.Second)
		next := wm.Query([]*query.Request{{}}, query.Timeout(time.Hour))
		job = recvJob(t, workers[0], next)
		time.Sleep(11 * time.Second)
		sendResult(t, workers[0], job, nil)
		requireQueryDone(t, next)
	})
}

// TestWorkManagerFailureDoesNotResetIdle checks retries cannot keep silent
// work alive.
func TestWorkManagerFailureDoesNotResetIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wm, workers := startWorkManager(t, 1, nil, nil)
		defer func() { require.NoError(t, wm.Stop()) }()
		result := wm.Query(
			[]*query.Request{{}},
			query.Timeout(time.Hour),
			query.NoRetryMax(),
			query.ProgressTimeout(10*time.Second),
		)
		job := recvJob(t, workers[0], result)
		time.Sleep(6 * time.Second)
		sendResult(t, workers[0], job, query.ErrQueryTimeout)
		synctest.Wait()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		select {
		case err := <-result:
			require.ErrorIs(t, err, query.ErrQueryTimeout)
		default:
			t.Fatal("failed request reset the idle timeout")
		}
	})
}
