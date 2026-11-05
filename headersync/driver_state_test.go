package headersync

import (
	"testing"

	"github.com/lightninglabs/neutrino/protofsm"
	"github.com/stretchr/testify/require"
)

// enterCommitPending moves the driver into the commit pending state, as the
// effect that submits a mutation does.
func enterCommitPending(t *testing.T, d *Driver) {
	t.Helper()

	machine, err := protofsm.NewStateMachine[DriverEvent, DriverEffect](
		protofsm.StateMachineCfg[DriverEvent, DriverEffect, *Driver]{
			InitialState: commitPendingState{},
			Env:          d,
		},
	)
	require.NoError(t, err)
	machine.Start(t.Context())

	d.machine = machine
}

// requireDriverState checks which of its two states the driver is in.
func requireDriverState(t *testing.T, d *Driver, pending bool) {
	t.Helper()

	state := d.machine.CurrentState()
	if pending {
		require.IsType(t, commitPendingState{}, state)
		return
	}
	require.IsType(t, driverState{}, state)
}

// TestCommitPendingDefersOutsideEvents checks that a pending commit holds back
// the messages from peers and the deadlines of requests, and replays them in
// order as the follow-up of the event that ends the commit.
func TestCommitPendingDefersOutsideEvents(t *testing.T) {
	t.Parallel()

	f := newAnchorDriverFixture(t, false)
	enterCommitPending(t, f.driver)

	headers := Inbound{Message: "headers"}
	timeout := Inbound{Message: "timeout"}
	later := Inbound{Message: "later"}
	for _, event := range []DriverEvent{headers, timeout, later} {
		effects, err := f.driver.ProcessEvent(
			t.Context(), event, View{},
		)
		require.NoError(t, err)
		require.Empty(t, effects, "%T ran during a pending commit",
			event)
		requireDriverState(t, f.driver, true)
	}

	// The commit ends without a result. The driver goes idle and replays
	// what it held, oldest first, as the last effect of the batch.
	effects, err := f.driver.ProcessEvent(
		t.Context(), CommitAbandoned{}, View{},
	)
	require.NoError(t, err)
	requireDriverState(t, f.driver, false)

	require.NotEmpty(t, effects)
	replay, ok := effects[len(effects)-1].(Then)
	require.True(t, ok, "the batch does not end with the replay")
	require.Equal(t, []DriverEvent{headers, timeout, later}, replay.Events)

	// Idle again, the driver hands a message back at once.
	effects, err = f.driver.ProcessEvent(
		t.Context(), Inbound{Message: "now"}, View{},
	)
	require.NoError(t, err)
	require.Equal(t, []DriverEffect{Deliver{Message: "now"}}, effects)
}

// TestCommitPendingHandlesInternalEvents checks that the events the driver
// raises for itself, and a peer's departure, are not held back by a pending
// commit.
func TestCommitPendingHandlesInternalEvents(t *testing.T) {
	t.Parallel()

	f := newAnchorDriverFixture(t, false)
	enterCommitPending(t, f.driver)

	// A departure is bookkeeping that cannot wait, so it runs and the
	// driver stays pending.
	effects, err := f.driver.ProcessEvent(
		t.Context(), PeerGone{PeerID: "gone"}, View{},
	)
	require.NoError(t, err)
	require.Empty(t, effects)
	requireDriverState(t, f.driver, true)

	// Nothing was deferred, so ending the commit replays nothing.
	effects, err = f.driver.ProcessEvent(
		t.Context(), CommitAbandoned{}, View{},
	)
	require.NoError(t, err)
	require.Empty(t, effects)
	requireDriverState(t, f.driver, false)
}

// TestCommitStatePredicates checks which effects start a commit and which
// events end or defer one.
func TestCommitStatePredicates(t *testing.T) {
	t.Parallel()

	for _, effect := range []DriverEffect{
		SubmitRangeCommit{}, CommitExtension{}, CommitReorg{},
		CheckpointRollback{},
	} {
		require.True(t, submitsCommit([]DriverEffect{
			SetSyncPeer{}, effect,
		}), "%T", effect)
	}
	require.False(t, submitsCommit([]DriverEffect{
		StartSync{}, ScheduleCommitRetry{}, Then{},
	}))

	for _, event := range []DriverEvent{
		RangesCommitted{}, ExtensionCommitted{}, ReorgCommitted{},
		CheckpointRolledBack{}, SnapshotAdopted{}, CommitAbandoned{},
	} {
		require.True(t, endsCommit(event), "%T", event)
		require.False(t, defersWhileCommitPending(event), "%T", event)
	}

	require.True(t, defersWhileCommitPending(Inbound{}))
	require.False(t, endsCommit(Inbound{}))
	require.False(t, defersWhileCommitPending(HeadersReceived{}))

	// The driver's own follow-ups must keep running while a commit is
	// pending, or an anchor span would stop being discovered.
	require.False(t, defersWhileCommitPending(anchorPipelined{}))
}
