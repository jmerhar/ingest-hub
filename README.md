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

### Environment

| Variable | Default | |
|---|---|---|
| `DATA_DIR` | `/data` in the image | Configuration, spools, cursors, status file, log. |
| `CONFIG_FILE` | `$DATA_DIR/config/sources.toml` | The configuration file. |
| `SPOOL_MAX_BYTES` | 256 MiB | Each source's spool limit; beyond it the oldest batches are dropped. |
| `LOG_LEVEL` | `INFO` | |
| `TZ` | `UTC` | |

## Configuration

See [`examples/sources.toml`](examples/sources.toml). A `[destinations.NAME]` table is one
database (`influx3` or `influx2`); a `[sources.NAME]` table is one source, its adapter (`kind`),
its destination and its adapter's `params`. Credentials are always files, named by `*_file`
paths; a literal credential in the file is refused.

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

## Development

```sh
make install     # backend/.venv with the test tools
make test        # the suite
make check       # lint and coverage, as CI runs them
make help        # everything else
```

Tests use synthetic devices and stand-in servers only. Nothing that names a real device, house,
database, host or topic belongs in this repository.

## Licence

GPL-3.0-or-later.
