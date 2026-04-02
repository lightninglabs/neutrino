package bridge

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReplayTraceFromFile tests the bridge by replaying any P model checker
// trace files found in the PCheckerOutput directory. This validates that the
// trace parser, event mapping, and replay harness work together.
func TestReplayTraceFromFile(t *testing.T) {
	// Look for trace files in the standard P checker output directory.
	traceDir := filepath.Join("..", "PCheckerOutput", "BugFinding")

	entries, err := os.ReadDir(traceDir)
	if err != nil {
		t.Skipf("no PCheckerOutput directory found (run P checker "+
			"first): %v", err)
		return
	}

	var traceFiles []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".json" {
			traceFiles = append(traceFiles,
				filepath.Join(traceDir, e.Name()))
		}
	}

	if len(traceFiles) == 0 {
		t.Skip("no trace JSON files found in PCheckerOutput")
		return
	}

	for _, tf := range traceFiles {
		t.Run(filepath.Base(tf), func(t *testing.T) {
			trace, err := ParseTraceFile(tf)
			require.NoError(t, err)
			require.NotEmpty(t, trace.Entries)

			result, err := ReplayTrace(trace)
			require.NoError(t, err)

			t.Logf("Replayed %d events, %d state transitions, "+
				"%d filtered blocks",
				result.TotalEvents,
				len(result.StateTransitions),
				len(result.FilteredBlocks))

			// No conformance errors should be found.
			require.Empty(t, result.Errors,
				"conformance errors: %v", result.Errors)
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
		StateInitializing, StateRewinding,
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
	require.Equal(t, "Initializing", StateInitializing.String())
	require.Equal(t, "Syncing", StateSyncing.String())
	require.Equal(t, "Current", StateCurrent.String())
	require.Equal(t, "Rewinding", StateRewinding.String())
	require.Equal(t, "Terminal", StateTerminal.String())
	require.Equal(t, "Unknown", FSMState(99).String())
}
