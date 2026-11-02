# Changelog

Notable changes to ingest-hub. The format follows [Keep a Changelog](https://keepachangelog.com/),
and versions follow [Semantic Versioning](https://semver.org/).

## Unreleased

### Added

- The adapter runtime: one task per source with its own back-off, one spool and delivery per
  source, a heartbeat per source, and the `ingest_progress` table recording how far each stream
  is complete.
- `ingest-hub run [--only SOURCE]`, configured by a TOML file of destinations and sources.
- InfluxDB 3 and 2.x destinations.
- The Tuya cloud adapter: every device and status code from the report log, typed from each
  device's specification, one table per device category.
- `ingest-hub import-tuya-dump`, which loads a saved report-log dump through the same path.
