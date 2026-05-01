#!/usr/bin/env bash
# Run the test suite inside a Docker container using the docker-compose stack.
#
# Usage:
#   ./scripts/run_tests_with_compose.sh [--down] [pytest args...]
#
# Examples:
#   ./scripts/run_tests_with_compose.sh                    # run tests, leave db up
#   ./scripts/run_tests_with_compose.sh --down              # run tests, then tear down
#   ./scripts/run_tests_with_compose.sh -v test/test_api.py  # run specific tests with verbose

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

TEAR_DOWN=false
EXTRA_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --down)
      TEAR_DOWN=true
      ;;
    *)
      EXTRA_ARGS+=("$arg")
      ;;
  esac
done

# Prefer docker compose (v2) over docker-compose (v1)
COMPOSE_CMD="docker compose"
if ! docker compose version &>/dev/null; then
  COMPOSE_CMD="docker-compose"
fi

echo "Starting Postgres and running tests in Docker..."
# Pass extra args to pytest; -- separates compose args from the command
$COMPOSE_CMD run --build --rm test python -m pytest "${EXTRA_ARGS[@]}"
RESULT=$?

if [[ "$TEAR_DOWN" == "true" ]]; then
  echo "Tearing down docker-compose services..."
  $COMPOSE_CMD down
fi

exit $RESULT
