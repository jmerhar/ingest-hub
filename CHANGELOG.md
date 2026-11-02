# Changelog

Notable changes to ingest-hub. The format follows [Keep a Changelog](https://keepachangelog.com/),
and versions follow [Semantic Versioning](https://semver.org/).

## Unreleased

### Added

- Alerts through Apprise when a source keeps failing, its destination refuses batches or backs
  up, or one of its streams falls behind; repeated while the problem lasts, with a message when it
  clears.
- Each source's status records its streams' progress, restored from saved state at startup.

## 0.1.0 — 2026-11-02

### Added

- An MQTT source: JSON messages on subscribed topics, routed to tables by topic filter, with
  tags from topic segments, numbers written as floats by default, and a persistent session.
- A Frank Energie source: electricity market prices for today and tomorrow, with the
  excluded-VAT parts derived at a fixed rate.
- `ingest-hub compare`, which checks a shadow table against the table it is meant to reproduce.
- The adapter runtime: one task per source with its own back-off, one spool and delivery per
  source, a heartbeat per source, and the `ingest_progress` table recording how far each stream
  is complete.
- `ingest-hub run [--only SOURCE]`, configured by a TOML file of destinations and sources.
- InfluxDB 3 and 2.x destinations.
- The Tuya cloud adapter: every device and status code from the report log, typed from each
  device's specification, one table per device category.
- `ingest-hub import-tuya-dump`, which loads a saved report-log dump through the same path.
