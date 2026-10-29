#!/bin/bash
# check-quick.sh - CI-sized quick run of rescan FSM P model tests.
#
# Runs only the deterministic FSM tests with 10 schedules each.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

export PATH="$HOME/.dotnet:$HOME/.dotnet/tools:$PATH"
export DOTNET_ROOT="${DOTNET_ROOT:-$HOME/.dotnet}"

cd "${PROJECT_DIR}"

echo "=== Quick check: compile + basic tests ==="
p compile --pproj rescan.pproj

DLL="PGenerated/PChecker/net8.0/RescanFSMModels.dll"

for tc in tcBasicSync tcBatchBoundary tcStopInit; do
    echo "  > ${tc}"
    p check "${DLL}" -tc "${tc}" -s 10 2>&1 | grep -E "Found|Explored"
done

echo "PASSED (quick check)"
