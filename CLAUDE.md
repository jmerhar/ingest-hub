# ingest-hub

A config-driven ingest service: source adapters write points through jigkit sinks. See README.md
for behaviour and the data model.

## Privacy

The repository is public. Never commit device IDs or names, house names, real database names,
hostnames, MQTT topics with device names, push URLs or tokens, in code, tests, fixtures, examples,
commit messages or history. Tests use synthetic data; examples use placeholders. The real
configuration lives on the deployment host.

## Layout

- `backend/src/ingest_hub/collect/` — the adapter runtime. It imports only jigkit and the standard
  library, never the rest of ingest_hub, so it can move into jigkit; keep it that way.
- `backend/src/ingest_hub/adapters/` — one package per source kind, registered in
  `adapters/__init__.py`. Nothing adapter- or consumer-specific goes into `collect/`.
- `bin/` — every Makefile target and CI step calls a script here.

## jigkit

jigkit is maintained in its own repository by a separate session. If something is missing or
wrong there, report it to that session instead of patching jigkit or working around it here.

## Checks

`make check` (lint + coverage, gate in `coverage.toml`) must pass before a commit.
