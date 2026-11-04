# ingest-hub

A self-hosted service that collects data from outside sources into time-series databases. Sources
and destinations are declared in one configuration file; each source runs in its own task with its
own back-off, spool and heartbeat, so one failing API never holds up another.

Built on [jigkit](https://github.com/jmerhar/jigkit) (sinks, spool, delivery, heartbeat).

## Running it

```sh
mkdir -p data/config secrets
cp examples/sources.toml data/config/sources.toml   # then edit it
printf '%s' "<token>" > secrets/influxdb_token      # one file per credential, no newline
docker compose up -d
```

`docker-compose.yml` pulls the published image (`ghcr.io/jmerhar/ingest-hub`); everything mutable
is in `./data`, and `./secrets` is mounted read-only at `/run/secrets`.

| Command | What it does |
|---|---|
| `ingest-hub run` | Collect from every enabled source until SIGTERM. |
| `ingest-hub run --only NAME` | Collect from one source, even a disabled one. The same image can run a source as its own compose service this way. |
| `ingest-hub check [--only NAME]` | Validate the configuration, read every secret, and ask each destination whether it would accept a write. |
| `ingest-hub import-tuya-dump DIR --source NAME` | Write a saved Tuya report-log dump using that source's settings. |
| `ingest-hub compare --destination NAME OLD NEW` | Compare a shadow table with the table it is meant to reproduce: the schemas, and per minute the row counts and the mean of every numeric column (`--hours`, `--tolerance`). Exits 0 when they match. |

### Environment

| Variable | Default | |
|---|---|---|
| `DATA_DIR` | `/data` in the image | Configuration, spools, cursors, status file, log. |
| `CONFIG_FILE` | `$DATA_DIR/config/sources.toml` | The configuration file. |
| `SPOOL_MAX_BYTES` | 256 MiB | Each source's spool limit; beyond it the oldest batches are dropped. |
| `WEB_PORT` | `8000` in the image | The status page and `/health`; unset, `run` serves nothing and the health probe reads the status file. |
| `LOG_LEVEL` | `INFO` | |
| `TZ` | `UTC` | |

## Configuration

See [`examples/sources.toml`](examples/sources.toml). A `[destinations.NAME]` table is one
database (`influx3` or `influx2`); a `[sources.NAME]` table is one source, its adapter (`kind`),
its destination and its adapter's `params`. Credentials are always files, named by `*_file`
paths; a literal credential in the file is refused.

### Status page

With `WEB_PORT` set, `ingest-hub run` also serves:

- `/`: a read-only page showing each source's state, last success and error, points and batches
  written, spool backlog, heartbeat, and how far each of its streams is complete, refreshed every
  15 s;
- `/api/status`: the same as JSON;
- `/health`: 200 while the runtime is running and refreshing its status, 503 otherwise. It does
  not depend on any destination, since restarting the process would not bring a database back.

Nothing on it can change anything, and it shows no credentials, but it does show what is being
collected; publish it only behind a reverse proxy you control.

### Alerts

An optional `[alerts]` section sends notifications through
[Apprise](https://github.com/caronc/apprise) to the URLs in `urls_file` (one per line, so any of
Apprise's services: Telegram, email, Signal, ntfy, an Apprise API server, …). Each source is
checked once a minute:

| Alert | When |
|---|---|
| failing | its latest news is an error, and it has not completed a round of work for `failing_minutes` (30) |
| backlog | its spool's oldest batch has waited for `backlog_minutes` (15): the destination is unreachable or refusing writes |
| stalled | one of its streams' `complete_until` is more than `progress_hours` (3) behind |
| rejected | its destination refused batches for good since the last check |

`progress_hours_by_source` sets the stall threshold for particular sources, such as one whose
progress trails by design (`{ heating = 6 }`). An ongoing problem is repeated every
`repeat_hours` (6), and a message follows when it clears.

## What gets written

Every point carries the time its source measured it, never the time it was written, so a backlog
replayed after an outage lands where it belongs. Writing the same point again overwrites it.

### `ingest_progress`

Every source that keeps a position reports, in its destination, how far each of its streams is
complete:

| Column | |
|---|---|
| `source` (tag) | The source's name in the configuration. |
| `stream` (tag) | What the source keeps a position for; for Tuya, the device ID. |
| `complete_until` (int64) | Unix milliseconds. Every point of the stream measured before this has been fetched and written (or spooled to be written) together with this row or before it. |
| `time` | When the row was written. |

A stream with nothing to report still advances, so a consumer can tell "nothing happened" from
"not fetched yet": wait until `max(complete_until)` for a stream passes the end of the period you
need. `complete_until` never moves backwards. A source may still store a late point with an
earlier time (Tuya surfaces some reports late, and each poll re-fetches a window), so a consumer
that needs exact totals re-reads its most recent periods.

### Tuya

One table per device category, `table_prefix` + category (`tuya_cz` for plugs, `tuya_cs` for
dehumidifiers), at millisecond precision from Tuya's own event times. Tags `device_id` (exactly
as Tuya gives it) and `category`; a field `name` holds the device's name at the time. One field per
status code, typed from the device's specification:

| Specification | Stored as |
|---|---|
| `Integer` with `scale` s | float, the reported integer ÷ 10^s (`cur_power` 1561 → 156.1 W, `cur_voltage` 2312 → 231.2 V, `cur_current` in mA) |
| `Integer` listed in `unscaled_codes` | the reported integer. By default only `add_ele`: energy in **Wh since the previous report**, to be summed by the consumer |
| `Boolean` | bool |
| `Bitmap` | integer |
| `Enum`, `String`, others | text |
| a code the specification does not describe, or a value that does not parse | text, in `<code>_raw` |

The source polls every `poll_minutes`, fetching each device from its cursor minus
`overlap_minutes` (once a day, minus `rescan_hours`) up to now, and advances the device's
`complete_until` to `lag_minutes` before the poll's start. Tuya keeps about a week of logs, so a
source stopped for longer than that loses the gap.

### MQTT

Each route maps an MQTT topic filter to a table. A message whose topic matches becomes one point:

- its fields are the payload's numeric top-level values (a JSON object is expected; text,
  booleans, nulls and nested values are left out), written as floats unless the route sets
  `numbers = "keep"`, or only the keys a route lists in `fields`;
- its tags are the route's, where `{N}` stands for the topic's N-th segment (from 0);
- its time is when the hub received it, at the source's `precision`, unless the route names a
  `timestamp_field` holding Unix seconds.

The session is persistent, under the source's name unless `client_id` is set, so the broker keeps
QoS 1 and 2 messages for the hub while it reconnects. A dropped connection restarts the source
after its back-off. MQTT sources write no `ingest_progress`: a pushed stream has no position to
report.

### Frank Energie prices

`kind = "frank"` polls Frank Energie's public GraphQL `marketPrices` every `poll_hours` (6) for
today and tomorrow (by UTC date): 15-minute periods from `quarter_hours_from` (1 Oct 2025),
hourly before. One point per period in `table` (`electricity_price`), at the period's start, tags
`market` (`power`) and `resolution`, all fields floats:

| Field | |
|---|---|
| `market_price_excl_vat` | Frank's spot price |
| `market_price_incl_vat` | spot + Frank's VAT on it |
| `surcharge_incl_vat`, `energy_tax_incl_vat` | as Frank reports them |
| `surcharge_excl_vat`, `energy_tax_excl_vat` | those ÷ (1 + `vat_rate`) |
| `total_excl_vat` | spot + surcharge excl. + energy tax excl. |
| `total_incl_vat` | Frank's all-in price |
| `vat_rate` | the configured rate (0.21) |

Tomorrow's prices appear in the afternoon; until then that day is skipped and logged, not an
error.

### Vaillant

`kind = "vaillant"` reads a myVAILLANT account through
[myPyllant](https://github.com/signalkraft/myPyllant), with the app's login in `username_file` and
`password_file`, and the `brand` and `country` it was registered under.

- **Energy**, every `energy_minutes` (60): each device's hourly buckets for every operation mode
  and energy type it reports, in `vaillant_energy`: tags `device` (Vaillant's device type,
  lower-cased, such as `heatpump`), `operation_mode` (`heating`, `domestic_hot_water`) and
  `energy_type` (`consumed_electrical_energy`, `heat_generated`, `earned_environment_energy`), a
  float field `wh` as Vaillant reports it, and `product`, at the start of the hour in UTC. Each
  poll re-reads `rescan_hours` (48) before the device's cursor, since buckets arrive late and are
  revised, and the device's `complete_until` trails the poll by `lag_hours` (3). The consumption
  figures are Vaillant's own estimate, not a meter's.
- **Live state**, every `state_minutes` (20): `vaillant_power` (current power per device) and
  `vaillant_system`, `vaillant_circuit`, `vaillant_zone` and `vaillant_dhw` with their
  temperatures, pressures, setpoints and states, stamped when read.

Vaillant counts requests per clock hour (about twenty) and refuses the rest of the hour once its
quota is spent. A state poll takes about four requests and an energy poll one per series (the
energy poll reuses the devices the last state poll found); every poll logs how many it took. The
source counts its own requests against `requests_per_hour` (18), keeping the count in its state
directory across restarts: a poll that would not fit in what is left of the hour waits for the
next one, and a due energy poll holds state polls back until it has run. A refusal (429, or 403
naming the quota) or a server error, at login or later, is waited out for as long as Vaillant
says, at most `pause_minutes` (60), and a refusal counts the hour as spent; bucket series are
requested one at a time, a couple of seconds apart, and the first energy poll starts a minute
after the first state poll.

### Vaillant data exports

`kind = "vaillant_export"` imports the data exports a myVAILLANT user can download under the EU
Data Act (from Vaillant's portal at eu-data-act.vaillant-group.com). An export is a JSON array of
messages and holds far more than the app's API: the controller's state every few minutes, power
every minute or two, the heat pump's eBUS readings every few minutes, and hourly running totals. Drop export files into `inbox`; every
`scan_minutes` (10) the source imports each `*.json` file there and moves it to `imported/`, or to
`failed/` when it cannot be read. Importing overlapping exports writes the same points again.

| Table | From | Tags | Fields |
|---|---|---|---|
| `vaillant_system`, `_zone`, `_circuit`, `_dhw` | `system-control.state` | `zone`, `circuit`, `dhw` (index) | the controller's state, the `vaillant` source's tables and columns |
| `vaillant_power` | `status_message_mpc` | `device` | `current_power` (W), the `vaillant` source's table |
| `vaillant_ebus` | `ebus.responses.interpreted` | `device` | one per data point, in snake case |
| `vaillant_stats` | `end-user-system-realtime-stats.inbound` | `device`, `operation_mode` | `operating_minutes`, `on_off_cycles` (running totals) |
| `vaillant_energy_total` | `end-user-system-emf.inbound` | `device`, `operation_mode`, `energy_type` | `wh` (running total) |

eBUS values are floats when numeric, booleans for `on`/`off` and `true`/`false`, and strings
otherwise; `NO_VALUE` is left out. Controller state names are the export's in snake case, as the app's API
gives them, except `currentCircuitTemperature`, written as `current_dhw_temperature`. Every point is stamped with its message's timestamp. A device is
named by its type in the export's topology (`heat_pump`, `hp_accessory`), or `ebus_<address>`;
`devices` maps those names to others, such as the `vaillant` source's:

```toml
[sources.heating_export.params]
inbox = "/data/inbox/vaillant"
devices = { heat_pump = "heatpump", hp_accessory = "electric_auxiliary_heater" }
```

## Replacing another writer

Point a new source at a shadow table next to the one the old writer fills, run both, then
`ingest-hub compare` the two. Once the schemas are identical and the minutes agree, rename the
route's table to the real one and stop the old writer.

## Development

```sh
make install     # backend/.venv with the test tools
make test        # the suite
make check       # lint and coverage, as CI runs them
make help        # everything else
```

Tests use synthetic devices and stand-in servers only, so the suite needs no network access and
no credentials.

## Licence

GPL-3.0-or-later.
