#!/usr/bin/env bash
# Run the neutrino P models.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$PROJECT_DIR")"
BUILD_DIR="${REPO_ROOT}/PGenerated/PChecker/net8.0"

SCHEDULES="${SCHEDULES:-50}"
MAX_STEPS="${MAX_STEPS:-300}"
TIMEOUT="${TIMEOUT:-120}"

cd "$REPO_ROOT"

echo "=== P Neutrino Model Checking ==="
echo "Bounds:"
echo "  SCHEDULES: $SCHEDULES"
echo "  MAX_STEPS: $MAX_STEPS"
echo "  TIMEOUT: ${TIMEOUT}s"
echo ""

if ! command -v p >/dev/null 2>&1; then
    echo "Error: P compiler not found"
    echo "Install with: dotnet tool install --global P --version 3.0.4"
    exit 1
fi

EXPECTED_P_VERSION="3.0.4"
P_VERSION="$(p --version 2>/dev/null | awk '{print $NF}')"
case "$P_VERSION" in
    "${EXPECTED_P_VERSION}"|"${EXPECTED_P_VERSION}".*)
        ;;
    *)
        echo "Warning: expected P ${EXPECTED_P_VERSION}, got ${P_VERSION:-unknown}"
        ;;
esac

run_model() {
    local name="$1"
    local project="$2"
    local dll="$3"
    local testcase="$4"

    echo ""
    echo "=== Checking ${name} ==="

    rm -rf "${REPO_ROOT}/PGenerated"
    p compile -pp "$project"

    timeout "$TIMEOUT" p check "${BUILD_DIR}/${dll}" \
        --testcase "$testcase" \
        --schedules "$SCHEDULES" \
        --max-steps "$MAX_STEPS"
}

run_model \
    "query scheduler" \
    "${PROJECT_DIR}/neutrinoquery/infra.pproj" \
    "NeutrinoQueryModels.dll" \
    "tcQueryScheduler"

run_model \
    "header sync" \
    "${PROJECT_DIR}/neutrinoheadersync/infra.pproj" \
    "NeutrinoHeaderSyncModels.dll" \
    "tcHeaderSync"
