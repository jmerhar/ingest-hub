#!/usr/bin/env bash
# Every check that needs nothing but a clone.
#
# All of them run before the target fails: stopping at the first hides the rest.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
status=0

run() {
  printf '\n\033[1m--- %s ---\033[0m\n' "$1"
  shift
  "$@" || status=1
}

run "shellcheck" shellcheck bin/*.sh
run "ruff" env RUFF=backend/.venv/bin/ruff bin/ci-ruff.sh
run "versions agree" python3 bin/version.py check
run "compose files parse" bin/check-compose.sh

exit "$status"
