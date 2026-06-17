package protofsm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/stretchr/testify/require"
)

type testEvent interface {
	testEvent()
}

type stepEvent struct{}

func (stepEvent) testEvent() {}

type emitEvent struct{}

func (emitEvent) testEvent() {}

type finishEvent struct{}

func (finishEvent) testEvent() {}

type errEvent struct{}

func (errEvent) testEvent() {}

type outboxEvent struct {
	value string
}

type testEnv struct {
	name string
}

type testState struct {
	name     string
	terminal bool
}

func (t testState) String() string {
	return t.name
}

func (t testState) IsTerminal() bool {
	return t.terminal
}

func (t testState) ProcessEvent(_ context.Context, event testEvent,
	env testEnv) (*StateTransition[testEvent, outboxEvent, testEnv], error) {

	switch event.(type) {
	case stepEvent:
		return &StateTransition[testEvent, outboxEvent, testEnv]{
			NextState: testState{name: env.name + "-stepped"},
			NewEvents: fn.Some(EmittedEvent[testEvent, outboxEvent]{
				Outbox: []outboxEvent{{value: "stepped"}},
			}),
		}, nil

	case emitEvent:
		return &StateTransition[testEvent, outboxEvent, testEnv]{
			NextState: testState{name: "emitting"},
			NewEvents: fn.Some(EmittedEvent[testEvent, outboxEvent]{
				InternalEvent: []testEvent{finishEvent{}},
				Outbox:        []outboxEvent{{value: "emitting"}},
			}),
		}, nil

	case finishEvent:
		return &StateTransition[testEvent, outboxEvent, testEnv]{
			NextState: testState{
				name:     "done",
				terminal: true,
			},
			NewEvents: fn.Some(EmittedEvent[testEvent, outboxEvent]{
				Outbox: []outboxEvent{{value: "done"}},
			}),
		}, nil

	case errEvent:
		return nil, errors.New("boom")

	default:
		return &StateTransition[testEvent, outboxEvent, testEnv]{
			NextState: t,
		}, nil
	}
}

func newTestStateMachine(t *testing.T) *StateMachine[
	testEvent, outboxEvent, testEnv] {

	t.Helper()

	machine, err := NewStateMachine[testEvent, outboxEvent](
		StateMachineCfg[testEvent, outboxEvent, testEnv]{
			InitialState: testState{name: "start"},
			Env:          testEnv{name: "env"},
		},
	)
	require.NoError(t, err)

	machine.Start(context.Background())

	return machine
}

func TestStateMachineProcessesEvents(t *testing.T) {
	machine := newTestStateMachine(t)
	defer machine.Stop()

	outbox, err := machine.ProcessEvent(context.Background(), stepEvent{})
	require.NoError(t, err)
	require.Equal(t, []outboxEvent{{value: "stepped"}}, outbox)

	currentState := machine.CurrentState()
	require.Equal(t, "env-stepped", currentState.String())
}

func TestStateMachineProcessesInternalEvents(t *testing.T) {
	machine := newTestStateMachine(t)
	defer machine.Stop()

	sub := machine.RegisterStateEvents()
	defer machine.RemoveStateSub(sub)

	outbox, err := machine.ProcessEvent(context.Background(), emitEvent{})
	require.NoError(t, err)
	require.Equal(t, []outboxEvent{
		{value: "emitting"},
		{value: "done"},
	}, outbox)

	currentState := machine.CurrentState()
	require.Equal(t, "done", currentState.String())
	require.True(t, currentState.IsTerminal())
}

func TestStateMachineReportsErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)
	machine, err := NewStateMachine[testEvent, outboxEvent](
		StateMachineCfg[testEvent, outboxEvent, testEnv]{
			ErrorReporter: ErrorReporterFunc(func(err error) {
				errChan <- err
			}),
			InitialState: testState{name: "start"},
			Env:          testEnv{name: "env"},
		},
	)
	require.NoError(t, err)

	machine.Start(ctx)
	defer machine.Stop()

	_, err = machine.ProcessEvent(ctx, errEvent{})
	require.EqualError(t, err, "boom")

	select {
	case err := <-errChan:
		require.EqualError(t, err, "boom")

	case <-time.After(time.Second):
		t.Fatalf("expected state machine error")
	}
}

func TestStateMachineRejectsEventsBeforeStart(t *testing.T) {
	machine, err := NewStateMachine[testEvent, outboxEvent](
		StateMachineCfg[testEvent, outboxEvent, testEnv]{
			InitialState: testState{name: "start"},
			Env:          testEnv{name: "env"},
		},
	)
	require.NoError(t, err)

	_, err = machine.ProcessEvent(context.Background(), stepEvent{})
	require.ErrorIs(t, err, ErrStateMachineShutdown)
}
