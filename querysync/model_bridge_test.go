package querysync

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPModelBridgeFixtures(t *testing.T) {
	data, err := os.ReadFile(
		"../p-models/neutrinoquery/bridge/scenarios.json",
	)
	require.NoError(t, err)

	fixtures, err := DecodeBridgeFixtures(data)
	require.NoError(t, err)

	for _, scenario := range fixtures.Dispatch {
		t.Run("dispatch/"+scenario.Name, func(t *testing.T) {
			got, err := RunBridgeDispatch(scenario)
			require.NoError(t, err)
			require.Equal(t, scenario.Expect, got)
		})
	}

	for _, scenario := range fixtures.Steal {
		t.Run("steal/"+scenario.Name, func(t *testing.T) {
			got, err := RunBridgeSteal(scenario)
			require.NoError(t, err)
			require.Equal(t, scenario.Expect, got)
		})
	}
}
