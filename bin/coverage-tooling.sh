#!/usr/bin/env bash
# Fetch the shared coverage tooling from jmerhar/coverage.
#
# Fetched rather than vendored so a local summary is exactly what CI enforces. Refreshed on
# every run because v1 moves within its major version: -z makes an unchanged file cost a 304,
# and a failed request falls back to the copy already on disk, so this still works offline.
set -euo pipefail

cd "$(dirname "$0")/.."
target=".coverage-report.py"

curl -fsSL -z "$target" -o "$target" \
  https://raw.githubusercontent.com/jmerhar/coverage/v1/bin/coverage-report.py \
  || test -f "$target"
