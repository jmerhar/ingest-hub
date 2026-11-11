# Changelog

Notable changes to ingest-hub. The format follows [Keep a Changelog](https://keepachangelog.com/),
and versions follow [Semantic Versioning](https://semver.org/).

## Unreleased

### Added

- A Vaillant source, through the myVAILLANT cloud: hourly energy per device, operation mode and
  energy type, with progress per device, and the system's live state and current power, kept
  within an hourly request budget that survives restarts.
- A Vaillant data export source: imports the EU Data Act exports dropped into a folder or
  uploaded on the status page (controller state, power, eBUS readings, operating statistics and
  energy totals), and with `fetch` signs in to the portal itself and fetches the previous day's
  export while it is missing.
- Uploading files to a source from the status page.
- A Modbus TCP source, read-only, driven by register maps; maps for AlphaESS hybrid inverters and
  batteries and for Hoymiles DTU-Pro micro-inverter gateways. `skip_zero` leaves out zero
  readings of running totals, and `as_float` keeps an unscaled number a float.
- An AlphaESS cloud source, through AlphaESS's Open API: five-minute readings and daily totals,
  with history.
- A Zaptec charger source, read through a bridge that holds the Zaptec login: the charger's
  state, completed sessions and their energy readings.
- With a customer's login, the Frank Energie source also reads their account: the contract's
  prices (electricity per market period, and gas), billed usage per hour, smart battery and smart
  heating results, and the month's costs.
- `progress_hours_by_source` in `[alerts]`: a stall threshold for particular sources.

### Changed

- The Frank Energie source reports a failed round and carries on, rather than restarting.
- An error without a message, such as a timeout, is reported by its type.
- The status page shows an error that a later success has cleared in grey.

## 0.2.0 — 2026-11-04

### Added

- Alerts through Apprise when a source keeps failing, its destination refuses batches or backs
  up, or one of its streams falls behind; repeated while the problem lasts, with a message when it
  clears.
- Each source's status records its streams' progress, restored from saved state at startup.
- A read-only status page, `/api/status` and `/health`, served by `ingest-hub run` when
  `WEB_PORT` is set (as it is in the image); the health probe then asks `/health`.
- Streams can carry a label, shown on the status page beside the stream's identifier; Tuya
  devices are labelled with their names. The page has an icon.

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
