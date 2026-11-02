#!/usr/bin/env bash
# Every compose file, and each combination of them that is run, must resolve and validate.
#
# Catches a typo in a compose file before a deployment does. Run by `make lint` and CI.
# Environment: DOCKER names the command run.
set -euo pipefail

cd "$(dirname "$0")/.."
docker="${DOCKER:-docker}"
"$docker" compose -f docker-compose.yml config -q
"$docker" compose -f docker-compose.yml -f docker-compose.build.yml config -q
