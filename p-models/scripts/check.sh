#!/bin/bash
# check.sh - Compile and run all rescan FSM P model tests.
#
# Prerequisites:
#   - .NET 8 SDK
#   - P 3.0.4: dotnet tool install --global P --version 3.0.4
#
# Usage:
#   ./p-models/scripts/check.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Ensure dotnet and P are on PATH.
export PATH="$HOME/.dotnet:$HOME/.dotnet/tools:$PATH"
export DOTNET_ROOT="${DOTNET_ROOT:-$HOME/.dotnet}"

cd "${PROJECT_DIR}"

echo "=== Compiling RescanFSMModels ==="
p compile --pproj rescan.pproj

DLL="PGenerated/PChecker/net8.0/RescanFSMModels.dll"
SCHEDULES=100

echo ""
echo "=== Running FSM-only tests (${SCHEDULES} schedules each) ==="

for tc in tcBasicSync tcAddWatchRewind tcRewindComplete tcSubLifecycle \
          tcBatchBoundary tcStopInit tcChainExtension tcRewindWithinRewind \
          tcRapidRewindCycles; do
    echo "  > ${tc}"
    p check "${DLL}" -tc "${tc}" -s "${SCHEDULES}" 2>&1 | grep -E "Found|Explored"
done

echo ""
echo "=== Running Actor interleaving tests (1000 schedules each) ==="

for tc in tcSelfTellInterleaving tcSubscriptionNewBlocks \
          tcSubscriptionClosureRecovery tcConcurrentAddWatch \
          tcSubscriptionDisconnectReattach tcRewindToGenesis \
          tcStopCurrent tcNoOpRewindCurrent \
          tcReplayCurrentTipAfterRecovery; do
    echo "  > ${tc}"
    p check "${DLL}" -tc "${tc}" -s 1000 2>&1 | grep -E "Found|Explored"
done

echo ""
echo "=== All test cases passed ==="
