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
| `ingest-hub google-auth --source NAME [--address URL]` | Consent to a Google Health API source reading a person's data: without `--address`, show the address to open; with the address the browser ended up on, finish and keep the sign-in. |
| `ingest-hub home-connect-auth --source NAME` | Sign a Home Connect source in: show a code to confirm in a browser, wait until it is confirmed, and keep the sign-in. |

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

- `/`: a page showing each source's state, last success and error, points and batches written,
  spool backlog, heartbeat, and how far each of its streams is complete, refreshed every 15 s. A
  source that imports files from a folder (`vaillant_export`, `resmed_sd`, `google_takeout`) has
  an upload button, with what to upload and the largest file it takes. A source that needs a person
  to sign in through a browser (`home_connect`, `google_health`) has a sign-in button, which shows
  the page to open and what to do there, and the source starts again as soon as the sign-in is
  kept;
- `/api/status`: the same as JSON;
- `POST /api/sources/NAME/files`: save the request body in that source's inbox, named by the upload
  time and the `X-Filename` header (required), and import it at once; at most 64 MiB unless the
  source allows more, written to disk as it arrives;
- `POST /api/sources/NAME/sign-in` (JSON): start signing that source in, replacing a sign-in
  started before; the answer and the source's `sign_in.current` in the status say what to open.
  When the step asks for no answer it finishes by itself; otherwise `POST
  /api/sources/NAME/sign-in/answer` with `{"answer": "…"}` finishes it;
- `/health`: 200 while the runtime is running and refreshing its status, 503 otherwise. It does
  not depend on any destination, since restarting the process would not bring a database back.

It shows no credentials, and the only things it can change are adding files to an inbox and
signing a source in, but it does show what is being collected and has no login, so anyone who
can reach it could sign a source in to another account; publish it only behind a reverse proxy
you control.

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

## Sources

| Kind | Collects | How | Needs | Details |
|---|---|---|---|---|
| `tuya` | Tuya devices: smart plugs, dehumidifiers, … | polls each device's report log | a Tuya cloud project's keys | [Tuya](docs/sources/tuya.md) |
| `mqtt` | JSON messages on MQTT topics | subscribes to a broker | the broker, and its login if any | [MQTT](docs/sources/mqtt.md) |
| `frank` | Frank Energie's market prices; with a customer's login, the contract's prices, billed usage, smart battery and heating results | polls | a Frank Energie login, for the account | [Frank Energie](docs/sources/frank.md) |
| `vaillant` | a Vaillant heat pump's energy and live state | polls the myVAILLANT cloud within its request limit | a myVAILLANT login | [Vaillant](docs/sources/vaillant.md) |
| `vaillant_export` | Vaillant's EU Data Act exports | imports uploaded or dropped files; with `fetch`, fetches them daily | a myVAILLANT login, to fetch | [Vaillant](docs/sources/vaillant.md) |
| `resmed_sd` | a ResMed CPAP's SD card | imports uploaded or dropped zips | | [ResMed SD cards](docs/sources/resmed.md) |
| `google_takeout` | Google Health (Fitbit, Pixel Watch) from Google Takeout | imports uploaded or dropped zips | | [Google Health](docs/sources/google.md) |
| `google_health` | Google Health through its API | polls | a Google Cloud OAuth client, and each person's sign-in | [Google Health](docs/sources/google.md) |
| `modbus` | a Modbus TCP device, through a register map (AlphaESS, Hoymiles DTU bundled) | reads registers, read-only | the device on the network | [Modbus](docs/sources/modbus.md) |
| `alphaess_cloud` | AlphaESS systems' history and daily totals | polls AlphaESS's Open API | a developer account's app ID and secret | [AlphaESS cloud](docs/sources/alphaess.md) |
| `zaptec` | a Zaptec charger's state and sessions | polls a bridge that holds the Zaptec login | the bridge | [Zaptec](docs/sources/zaptec.md) |
| `home_connect` | Home Connect appliances: Bosch, Siemens, Neff, … | follows Home Connect's event stream | a registered application, and a sign-in | [Home Connect](docs/sources/home-connect.md) |

### Every source

- **Configuration:** a `[sources.NAME]` table names its `kind`, its `destination` and its
  `params`, which the source's page describes. `enabled = false` keeps it out of
  `ingest-hub run`, though `--only NAME` still runs it.
- **Credentials** are files named by `*_file` parameters, never values in the configuration.
- **State:** a source keeps its cursors, and the sign-in of one that needs a person to sign in,
  in `$DATA_DIR/state/NAME/`, across restarts; a file holding a token is readable by its owner
  only. Points waiting for an unreachable destination are spooled in `$DATA_DIR/spool/NAME/`.
- **Failures:** a source that fails starts again after its own back-off, from seconds up to 15
  minutes, without holding up any other; the status page shows its errors, and [alerts](#alerts)
  report one that keeps failing.
- **Heartbeat:** `heartbeat_url_file` names a file holding a URL, such as an Uptime Kuma push
  monitor's, that is called after each write of the source's points.
- **Signing in:** a source that needs a person to sign in through a browser (`google_health`,
  `home_connect`) has a sign-in button on the [status page](#status-page), and a command for
  setups without it.
- **Files:** a source that imports files (`vaillant_export`, `resmed_sd`, `google_takeout`) takes
  them from an upload button on the status page, or from its `inbox` folder.

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
