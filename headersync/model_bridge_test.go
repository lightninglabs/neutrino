package headersync

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPModelBridgeFixtures(t *testing.T) {
	data, err := os.ReadFile(
		"../p-models/neutrinoheadersync/bridge/scenarios.json",
	)
	require.NoError(t, err)

	fixtures, err := DecodeBridgeFixtures(data)
	require.NoError(t, err)

	for _, scenario := range fixtures.Scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			got, err := RunBridgeScenario(scenario)
			require.NoError(t, err)
			require.Equal(t, scenario.Expect, got)
		})
	}
}
