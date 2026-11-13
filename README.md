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

- `/`: a page showing each source's state, last success and error, points and batches written,
  spool backlog, heartbeat, and how far each of its streams is complete, refreshed every 15 s. A
  source that imports files from a folder (`vaillant_export`, `resmed_sd`, `google_takeout`) has
  an upload button;
- `/api/status`: the same as JSON;
- `POST /api/sources/NAME/files`: save the request body in that source's inbox, named by the upload
  time and the `X-Filename` header (required), and import it at once; at most 64 MiB unless the
  source allows more, written to disk as it arrives;
- `/health`: 200 while the runtime is running and refreshing its status, 503 otherwise. It does
  not depend on any destination, since restarting the process would not bring a database back.

It shows no credentials, and the only thing it can change is adding files to an inbox, but it
does show what is being collected and has no login; publish it only behind a reverse proxy you
control.

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

### Frank Energie

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

With a customer's login (`username_file`, `password_file`) the source also reads their account
every `account_minutes` (60), into tables named with `table_prefix` (`frank_`):

| Table | Tags | What |
|---|---|---|
| `frank_price` | `carrier`, `resolution`, `unit` | the contract's prices: electricity per public market period with the contract's surcharge and energy tax (Frank gives a contract's prices only as hourly averages), gas as the contract gives it |
| `frank_usage` | `carrier` (`electricity`, `gas`, `feed_in`) | billed `usage` and `costs` per hour |
| `frank_battery`, `frank_battery_day` | `battery` | a smart-trading battery's state, and each day's trading results in EUR |
| `frank_heating`, `frank_heating_day` | `device` | smart heating's state and totals, and each day's results |
| `frank_month` | `site` | the month's actual and expected costs so far |

Days are counted in `timezone` (`Europe/Amsterdam`). The first time, prices and usage are read
from `history_days` (60) back (not before the site's delivery start), battery and heating days
from the device's creation; each round reads at most a month of each, and while history remains
the next round follows a minute later. Frank has a
day's usage a day or two late and can report it before every hour is in, so a day is read again
until every carrier with hours has all of them, or for 7 days, after which it is taken as it is
and reported; `ingest_progress` stream `usage` says how far usage is complete. `site` picks one of the account's sites; the first by default. A failed round
of prices or account data is reported, and the source carries on with the next.

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
says, at most `pause_minutes` (60). A quota refusal counts the hour as spent and is logged, with the
requests counted that hour and Vaillant's message, but not reported as a problem, since the wait
ends with the hour; a refusal that persists shows as writes and progress that stop. Bucket series are
requested one at a time, a couple of seconds apart, and the first energy poll starts a minute
after the first state poll.

### Vaillant data exports

`kind = "vaillant_export"` imports the data exports a myVAILLANT user can download under the EU Data
Act (from Vaillant's portal at eu-data-act.vaillant-group.com). An export is a JSON array of
messages and holds far more than the app's API: the controller's state every few minutes, power
every minute or two while it changes (hourly when idle), the heat pump's eBUS readings every few
minutes, and hourly running totals. Upload an export on the status page, or drop it into `inbox`;
the source imports each `*.json` file there at once when uploaded, otherwise every `scan_minutes`
(10), and moves it to `imported/`, or to `failed/` when it cannot be read. Importing overlapping
exports writes the same points again.

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

With `fetch`, the source also fetches exports from the portal itself, signing in as the portal's
web page does, its login form's proof-of-work check (Altcha) included. The portal gives the
previous day's records (by UTC), adding them in stages through the following morning, so a day
counts as imported once an export, fetched or dropped, holds both eBUS readings and controller
state from its last hour. While yesterday isn't, the source fetches from `not_before` (12:00, in
`timezone`, UTC by default) each day, at most every `retry_minutes` (60), saves the export in
`inbox` as `euda-data-<UTC time>.json`, readable by its owner only, and imports it. A day not
complete by the end of the next is missed. A failed fetch, including an account that sees no
systems, is reported and keeps the round from counting as a success, so the source's alerts
fire. Give it a myVAILLANT account of its own rather than the `vaillant` source's, so that a
locked account cannot stop both. `realm` is the account's myVAILLANT realm, by country:

```toml
[sources.heating_export.params.fetch]
username_file = "/run/secrets/vaillant_export_username"
password_file = "/run/secrets/vaillant_export_password"
# realm = "vaillant-netherlands-b2c"
# retry_minutes = 60
# not_before = 12:00
# timezone = "Europe/Amsterdam"
```

### ResMed SD cards

`kind = "resmed_sd"` imports what a ResMed AirSense 10 or 11 records on its SD card. Upload a zip
of the card on the status page or drop it into `inbox`; it is imported at once when uploaded,
otherwise every `scan_minutes` (10), and moved to `imported/`, or to `failed/` when it is not a
card that can be read. A whole card is larger than an upload may be, nearly all of it the 25 Hz
breathing waveforms (`*_BRP.edf`), which are not imported, so leave them out:

```sh
cd /path/to/card && zip -r ~/cpap.zip . -x '*_BRP.edf'
```

`card` names a folder holding a copy of the card instead, read in place every scan and never
changed. A card keeps every night it has, so each copy repeats the ones before it: the source
keeps the size and CRC-32 of every file it has imported, in its state directory
(`resmed-imported.json`), and imports a file again only when it has changed. Delete that file to
import everything again.

| Table | From | Tags | Fields |
|---|---|---|---|
| `cpap_night` | `STR.edf`: one point per day, at its start (noon) | `night` | every value of the day |
| `cpap_mask` | `STR.edf`: one point per mask-on period, at its start; periods starting in the same minute are one | `night` | `minutes` |
| `cpap_event` | `*_EVE.edf`, `*_CSL.edf`: one point per event, at its time | `night`, `type` | `duration` (s) |
| `cpap_detail` | `*_PLD.edf`: every 2 s | `night` | `mask_press`, `press`, `epr_press` (cmH2O), `leak` (L/s), `resp_rate`, `tid_vol` (L), `min_vent` (L/min), `snore`, `flow_lim` |
| `cpap_oximetry` | `*_SA2.edf`: every second, when an oximeter recorded | `night` | `pulse`, `spo2` |

Every point is also tagged `device`, the serial number the files carry; `table_prefix` (`cpap_`)
renames the tables. A field is its signal's label in snake case, without its rate: `AHI` → `ahi`,
`Leak.95` → `leak_95`, `S.EPR.Level` → `s_epr_level`, `MaskPress.2s` → `mask_press`. Values are in
the files' units: leak is in L/s, where ResMed's app shows L/min. A night holds the therapy minutes
(`duration`), the indices (`ahi`, `oai`, `cai`, `hi`, …), percentiles of pressure, leak and
breathing (`_50`, `_95`, `_max`), and the day's settings (`s_*`, `mode`), so a change of settings
shows from the night it took effect. Event types are the annotations in snake case:
`obstructive_apnea`, `central_apnea`, `hypopnea`, `apnea` (unclassified), `arousal`.

The files mark a missing value as -1; it is left out, never written as zero. A day without therapy
keeps only its settings, and a day with neither has no point.

`night` is the device's own day, `YYYY-MM-DD`: a day runs from noon to noon, so every session
belongs to the date of the evening it started on, as in the card's `DATALOG` folders. It is a tag
on every table, for joining with other sleep data.

The files give the device's clock without a zone. `timezone` sets the zone it is read in, an IANA
name or a fixed offset such as `+01:00`; by default it is the card's own `TimeZoneOffset` setting,
or for a copy without it the last card's, or UTC when no card has had one. The device keeps that
offset all year and wherever it is, without summer time, so the default reads its times correctly
where an IANA zone with summer time would put summer nights an hour off. Set `timezone` before the
first import: a point's time depends on it, so a night imported again in another zone is written a
second time, an hour or more from the first.

### Google Takeout

`kind = "google_takeout"` imports a Google Takeout export of Google Health: the Fitbit and Pixel
Watch data a Google account keeps. Choose Google Health at takeout.google.com, as a `.zip`, and
upload the zip, or every zip when Takeout splits the export, on the status page, which takes up to
2 GiB each; or drop them into `inbox`. Every zip waiting in the inbox is imported as one export,
then moved to `imported/`, or to `failed/` when it is not an export that can be read.

Each export repeats the whole history. The source keeps the size and CRC-32 of every file it has
imported, in its state directory (`google-takeout.json`), and imports a file again only when it
has changed, as most of them do from one export to the next as they grow; the same points are
written again over the old ones.

| Table | From | Tags |
|---|---|---|
| `google_daily_heart_rate_variability`, `_daily_oxygen_saturation`, `_daily_respiratory_rate`, `_daily_sleep_temperature_derivations`, `_daily_readiness`, `_stress_score` | one row per day, computed on waking | `date`, `night` |
| `google_daily_resting_heart_rate`, `_demographic_vo2max`, `_cardio_acute_chronic_workload_ratio` | one row per day | `date` |
| `google_oxygen_saturation` (per minute), `_heart_rate_variability` (5 min), `_respiratory_rate_sleep_summary` (per sleep) | samples during sleep | `source`, `night` |
| `google_steps` (per minute), `_weight`, `_body_fat` | samples | `source` |
| `google_sleep` | one point per sleep session, at its start | `night` |
| `google_sleep_stage` | one point per stage, at its start, field `seconds` | `night`, `stage` |
| `google_sleep_score` | one point per scored sleep, at its end | `night` |
| `google_heart_rate_minute` | with `heart_rate_minutes = true`: heart rate per minute, `mean`, `min`, `max`, `count` | `source` |

`table_prefix` (`google_`) renames the tables; the rest of a name is the export's folder in snake
case, which is also the Google Health API's name for the data. A field is its column's header in
snake case: `average heart rate variability milliseconds` →
`average_heart_rate_variability_milliseconds`. The respiratory summary's headers say "milli breaths
per minute" over values in breaths per minute, so its fields drop the "milli":
`deep_sleep_stats_breaths_per_minute`. Numbers are floats; the readiness levels and the workload
label are text. An empty value, `NaN`, text in a column of numbers, and in the measurements a
value of zero or less, which the export writes where it has none (-1 in respiratory rates and
sleep scores, 0.0 in heart rate variability and oxygen saturation), are left out.

`source` is the export's `data source`. Steps are recorded by several at once, the watch and the
phone among them, so the same minute can appear once per source: never add steps across sources.

Daily values are civil dates (`date`). Those computed from a night's sleep carry the date of the
morning after; they are also tagged `night`, the date before, and stamped at noon of that night,
so they line up with the night's sleep and with `cpap_night`. The others are stamped at midnight
of their date. The stress score has its overall score in every row that has one and the
breakdown (sleep, responsiveness and exertion points) only where Premium gave one; rows without a
score are left out.

A sleep session's night is that of its local start, from the offset the export gives it, so a
night away keeps its own times. `main` is true for each night's longest session and false for the
others, such as a nap. The export can hold the same sleep twice, recorded by two versions of the
staging algorithm side by side; a session that overlaps a longer one is left out, with its stages
and score. Stages and scores take the night of their session, and the respiratory summaries that
of the session that ended when they were taken. Samples without an offset are put in a night in
`timezone` (UTC by default), which also places civil dates on the time axis.

A night key is shared with `resmed_sd`: both tag a night with the date of the evening it started
on. The CPAP's clock keeps a fixed offset, so only a session starting within an hour of noon
while that offset differs from local time (a nap at 12:30 in summer, say) can get a different
night in each.

```toml
[sources.health.params]
inbox = "/data/inbox/takeout"
timezone = "Europe/Amsterdam"
# heart_rate_minutes = true
```

### Modbus

`kind = "modbus"` reads a Modbus TCP device (`host`, `port` 502, `unit` 1) through a register
map: every `interval_seconds` (10) it reads the registers the map lists, in as few requests as
the map allows, and writes one point per table, `<prefix><table>`, stamped with the start of the
round. It only ever reads (function codes 3 and 4), so it cannot change a device's settings.
`tags` adds tags to every point; `table_prefix` overrides the map's.

| Map (`map = ...`) | Device | Tables |
|---|---|---|
| `alphaess` | AlphaESS hybrid inverter and battery (SMILE, Hi; unit 85) | `alphaess_grid`, `_battery`, `_inverter` (PV strings, backup output), `_pv_meter`, `_system`, `_timing`, `_dispatch`, and hourly `_inverter_info`, `_system_info`, `_system_config` |
| `hoymiles-dtu` | Hoymiles microinverters through a DTU-Pro(-S) (unit 1) | `hoymiles_port` per inverter port, tagged `serial` and `port`; hourly `hoymiles_dtu` |

A map can also be a TOML file named by its path; the format is described in
`backend/src/ingest_hub/adapters/modbus/registers.py`. Numbers are integers when the map does not
scale them and floats when it does. A request spanning several registers that the device refuses
as "illegal address" is retried register by register, and a register refused on its own is left
out after one warning, so a map listing a register a model lacks costs a warning, not the round.
A device that accepts only one client (AlphaESS does) can be read through a Modbus proxy that
shares its connection, such as evcc's.

### AlphaESS cloud

`kind = "alphaess_cloud"` reads AlphaESS systems through AlphaESS's Open API
([open.alphaess.com](https://open.alphaess.com)): a developer account's app ID and secret
(`app_id_file`, `app_secret_file`) sign each request, and the account sees the systems bound to it
(`systems` limits it to some). Every `poll_minutes` (15) it writes, tagged `sn`:

- `alphaess_cloud_power`: each five-minute reading, `pv_power` (the system's own PV inputs),
  `load`, `soc`, `grid_import`, `feed_in`, `battery_power` (negative while charging),
  `charging_pile`;
- `alphaess_cloud_energy`: each day's totals in kWh at the start of the day, `pv`, `charge`,
  `discharge`, `grid_import`, `grid_export`, `grid_charge`, `charging_pile`.

The API gives times and days in the system's time zone, `timezone` (UTC). The first time, the
source reads back `history_days` (60), skipping days with no data; after that, from the last
complete day. Today and yesterday are read on every poll, since a day's last readings can arrive
after midnight. The Modbus source gives far more detail every few seconds; this one adds the
history from before it ran and AlphaESS's own daily totals.

### Zaptec

`kind = "zaptec"` reads a Zaptec charger without logging in to Zaptec: Zaptec limits token
requests per address, so a second login from the same host can lock out whatever already
controls the charger. Instead it reads an HTTP bridge (`bridge_url`) that keeps one Zaptec session
and serves:

- `GET /state.json`: `{"charger_id", "age_seconds", "observations": {"<StateId>": "<value>"}}`,
  the charger's observations as Zaptec's `/api/chargers/{id}/state` gives them, or 503 while
  Zaptec cannot be reached or the charger is offline;
- `GET /sessions?from=<ISO>&to=<ISO>&pageSize=<n>[&cursor=…]`: `{"sessions": […], "cursor",
  "hasMore"}`, the charger's completed sessions as Zaptec's `/api/sessions/archived` gives them,
  those that ended between `from` and `to`.

| Table | Every | Contents |
|---|---|---|
| `zaptec_state` | `state_seconds` (60) | one field per observation, named after Zaptec's name in snake case (from its public `/api/constants`, or `observation_<id>` when it has none); numbers as floats, other values up to 256 characters as strings in `<name>_text` |
| `zaptec_session` | `sessions_minutes` (20) | per completed session, at its end: `id`, `energy` (kWh), `start_time`, `duration_seconds`, and the flags `aborted`, `voided`, `offline`, `stopped_by_rfid`, `signed`, `reliable_clock` |
| `zaptec_session_energy` | with the sessions | each session's energy readings at their times, tagged `session` |

All are tagged `charger`. Sessions are read from a day before the latest session end seen, the
first time from `history_days` (365) back. The state holds what the charger reports, which
includes the identifier of the RFID card in use; the session tables leave out who authorised a
session.

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
