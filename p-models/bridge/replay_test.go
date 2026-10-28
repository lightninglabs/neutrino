package bridge

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReplayTraceFromFile replays announcements captured from an actual P run.
func TestReplayTraceFromFile(t *testing.T) {
	path := filepath.Join(
		"testdata",
		"basic_sync_p_trace.json",
	)
	trace, err := ParseTraceFile(path)
	require.NoError(t, err)
	require.NotEmpty(t, trace.Entries)

	result, err := ReplayTrace(trace)
	require.NoError(t, err)
	require.Empty(t, result.Errors)
	require.Equal(t, []StateTransition{
		{From: StateInitializing, To: StateInitializing},
		{From: StateInitializing, To: StateSyncing},
		{From: StateSyncing, To: StateCurrent},
	}, result.StateTransitions)
	require.Len(t, result.FilteredBlocks, 5)
	for i, block := range result.FilteredBlocks {
		require.Equal(t, i+1, block.Height)
	}

	// Also replay any locally generated checker traces, as the original
	// test did, while keeping the checked-in fixture mandatory.
	traceDir := filepath.Join(
		"..",
		"PCheckerOutput",
		"BugFinding",
	)
	entries, err := os.ReadDir(traceDir)
	if os.IsNotExist(err) {
		return
	}
	require.NoError(t, err)
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			trace, err := ParseTraceFile(
				filepath.Join(
					traceDir,
					entry.Name(),
				),
			)
			require.NoError(t, err)
			require.NotEmpty(t, trace.Entries)
			result, err := ReplayTrace(trace)
			require.NoError(t, err)
			require.Empty(t, result.Errors)
		})
	}
}

// TestValidTransitionTable verifies the transition table matches the P model.
func TestValidTransitionTable(t *testing.T) {
	// Valid transitions.
	require.True(t, IsValidTransition(
		StateInitializing, StateInitializing,
	))
	require.True(t, IsValidTransition(
		StateInitializing, StateSyncing,
	))
	require.True(t, IsValidTransition(
		StateInitializing, StateRewinding,
	))
	require.True(t, IsValidTransition(
		StateInitializing, StateTerminal,
	))
	require.True(t, IsValidTransition(
		StateSyncing, StateCurrent,
	))
	require.True(t, IsValidTransition(
		StateSyncing, StateRewinding,
	))
	require.True(t, IsValidTransition(
		StateSyncing, StateTerminal,
	))
	require.True(t, IsValidTransition(
		StateCurrent, StateRewinding,
	))
	require.True(t, IsValidTransition(
		StateCurrent, StateTerminal,
	))
	require.True(t, IsValidTransition(
		StateRewinding, StateSyncing,
	))
	require.True(t, IsValidTransition(
		StateRewinding, StateTerminal,
	))

	// Invalid transitions.
	require.False(t, IsValidTransition(
		StateInitializing, StateCurrent,
	))
	require.False(t, IsValidTransition(
		StateSyncing, StateInitializing,
	))
	require.False(t, IsValidTransition(
		StateCurrent, StateSyncing,
	))
	require.False(t, IsValidTransition(
		StateCurrent, StateInitializing,
	))
	require.False(t, IsValidTransition(
		StateRewinding, StateCurrent,
	))
	require.False(t, IsValidTransition(
		StateRewinding, StateInitializing,
	))
	require.False(t, IsValidTransition(
		StateTerminal, StateInitializing,
	))
}

// TestFSMStateString verifies state name formatting.
func TestFSMStateString(t *testing.T) {
	require.Equal(
		t,
		"Initializing",
		StateInitializing.String(),
	)
	require.Equal(t, "Syncing", StateSyncing.String())
	require.Equal(t, "Current", StateCurrent.String())
	require.Equal(t, "Rewinding", StateRewinding.String())
	require.Equal(t, "Terminal", StateTerminal.String())
	require.Equal(t, "Unknown", FSMState(99).String())
}

// TestPTraceInvalidPayload rejects numeric overflow and transaction payloads
// the native trace decoder cannot yet interpret faithfully.
func TestPTraceInvalidPayload(t *testing.T) {
	for _, log := range []string{
		"RescanFSM(3) announced " +
			"event eFSMStateChange " +
			"with payload " +
			"<99999999999999999999999" +
			"9,1,>.",
		"RescanFSM(3) announced " +
			"event " +
			"eFilteredBlockOutbox " +
			"with payload " +
			"<1,(<txid:1,>),>.",
	} {
		event := "eFSMStateChange"
		if strings.Contains(
			log,
			"eFilteredBlockOutbox",
		) {

			event = "eFilteredBlockOutbox"
		}
		data, err := json.Marshal(
			[][]map[string]interface{}{{{
				"type": "Announce",
				"details": map[string]interface{}{
					"event": event,
					"log":   log,
				},
			}}},
		)
		require.NoError(t, err)
		_, err = ParseTrace(data)
		require.Error(t, err)
	}
}
