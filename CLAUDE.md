# ingest-hub

A config-driven ingest service: source adapters write points through jigkit sinks. See README.md
for behaviour and the data model.

## Data in the repository

Tests use synthetic data and stand-in servers; examples use placeholders. A deployment's
configuration, credentials and collected data never belong in the repository: no real device
identifiers, hostnames, topics, push URLs or tokens, in code, tests, fixtures, examples or commit
messages.

## Writing for readers of this repository

Commit messages, comments and docs describe ingest-hub in its own terms. They make sense to
someone who has only this repository and the public ones it links to, so they never refer to a
particular installation, the system a deployment migrated from, or anything else a reader cannot
see.

## Layout

- `backend/src/ingest_hub/collect/` — the adapter runtime. It imports only jigkit and the standard
  library, never the rest of ingest_hub, so it can move into jigkit; keep it that way.
- `backend/src/ingest_hub/adapters/` — one package per source kind, registered in
  `adapters/__init__.py`. Nothing adapter- or consumer-specific goes into `collect/`.
- `bin/` — every Makefile target and CI step calls a script here.

## jigkit

jigkit is maintained in its own repository (github.com/jmerhar/jigkit). If something is missing
or wrong there, fix it in jigkit and release it, rather than working around it here.

## Checks

`make check` (lint + coverage, gate in `coverage.toml`) must pass before a commit.
