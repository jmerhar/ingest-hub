#!/usr/bin/env bash
# Lint and format-check all the Python: every project, its tests, and the scripts in bin/.
#
# One ruff.toml at the root covers all of it. Called by CI with the runner's ruff on PATH;
# `make lint` runs the same checks from the virtualenv.
set -euo pipefail
cd "$(dirname "$0")/.."
ruff="${RUFF:-ruff}"
"$ruff" check .
"$ruff" format --check .
