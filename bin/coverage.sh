#!/usr/bin/env bash
# Run the suite with coverage and print the summary.
#
# The summary and the gate are shared tooling from jmerhar/coverage, fetched with the rest of the
# shared tooling and configured by coverage.toml, so the numbers here are the ones CI enforces.
# Arguments go to the summary (`make coverage` asks for Markdown with --format md).
set -euo pipefail

cd "$(dirname "$0")/.."

bin/devtools-fetch.sh
bin/test-backend.sh -q \
  --cov=ingest_hub --cov-report=term-missing \
  --cov-report=xml --cov-report=json --cov-report=html

echo
python3 .devtools/coverage/coverage-report.py "$@"
