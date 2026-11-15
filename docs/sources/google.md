# Google Health

## Google Takeout exports

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
| `google_oxygen_saturation` (per minute), `_heart_rate_variability` (5 min), `_respiratory_rate_sleep_summary` (per sleep) | samples during sleep | `night` |
| `google_steps` (per minute) | samples | `source` |
| `google_weight`, `_body_fat` | samples | |
| `google_sleep` | one point per sleep session, at its start | `night` |
| `google_sleep_stage` | one point per stage, at its start, field `seconds` | `night`, `stage` |
| `google_sleep_score` | one point per scored sleep, at its end | `night` |
| `google_heart_rate_minute` | with `heart_rate_minutes = true`: heart rate per minute, `mean`, `min`, `max`, `count` | `source` |

Every point is also tagged `person`, whose data it is (`person` in `params`, required).
`table_prefix` (`google_`) renames the tables; the rest of a name is the export's folder in snake
case, which is also the Google Health API's name for the data. A field is its column's header in
snake case: `average heart rate variability milliseconds` →
`average_heart_rate_variability_milliseconds`. The respiratory summary's headers say "milli breaths
per minute" over values in breaths per minute, so its fields drop the "milli":
`deep_sleep_stats_breaths_per_minute`. Numbers are floats; the readiness levels and the workload
label are text. An empty value, `NaN`, text in a column of numbers, and in the measurements a
value of zero or less, which the export writes where it has none (-1 in respiratory rates and
sleep scores, 0.0 in heart rate variability and oxygen saturation), are left out.

`source` is the export's `data source`, kept for steps and heart rate, which the watch and the
phone both record: the same minute can appear once per source, so never add them across sources.

Daily values are civil dates (`date`). Those computed from a night's sleep carry the date of the
morning after; they are also tagged `night`, the date before, and stamped at noon of that night,
so they line up with the night's sleep and with `cpap_night`. The others are stamped at midnight
of their date. The stress score has its overall score in every row that has one and the
breakdown (sleep, responsiveness and exertion points) only where Premium gave one; rows without a
score are left out.

A sleep session's night is that of its local start, from the offset the export gives it, so a
night away keeps its own times. Some exports give a session's and its score's times in local
time labelled `+00:00`, while its stages carry the true offset; the source then takes the stages'
offset, which gives the times the Google Health API gives. The export can hold the same sleep
twice, recorded by two versions of the staging algorithm side by side; of sessions that overlap,
the newest algorithm's is kept, then the longest, and the others are left out with their stages
and scores. `main` is true for each night's main sleep, as the API defines it: its longest session
with stages, or its longest of all when none has stages. Stages and scores take the night of
their session, and the respiratory summaries that of the session that ended when they were taken.
Samples without an offset are put in a night in `timezone` (UTC by default), which also places
civil dates on the time axis.

With a `google_health` source reading the same account, set `api_data = false`: the export then
adds only what the API lacks (sleep scores, readiness, stress, the cardio workload ratio and the
demographic VO2 max), and reads the sleep sessions only to give the scores their nights. The API
gives sleep stages to the half minute where the export rounds them to the minute, so the two
cannot share a night's stages.

A night key is shared with `resmed_sd`: both tag a night with the date of the evening it started
on. The CPAP's clock keeps a fixed offset, so only a session starting within an hour of noon
while that offset differs from local time (a nap at 12:30 in summer, say) can get a different
night in each.

```toml
[sources.health.params]
person = "alex"
inbox = "/data/inbox/takeout"
timezone = "Europe/Amsterdam"
# api_data = false                  # when a google_health source reads the same account
# heart_rate_minutes = true
```

## The Google Health API

`kind = "google_health"` polls one person's Google Health data through the Google Health API
(`health.googleapis.com/v4`), into the same tables, fields and tags as `google_takeout`, so the
two land on the same series where they share data. Every `poll_minutes` (60) it reads each data
type from its cursor less `rescan_hours` (72) up to now, since watches sync late and Google revises
what it has; the first poll reads `history_days` (30) back, and the API holds the whole history,
so a large `history_days` reads it all once.

| Data type | Table | Tags |
|---|---|---|
| `daily-heart-rate-variability`, `daily-oxygen-saturation`, `daily-respiratory-rate`, `daily-sleep-temperature-derivations` | the same name in snake case | `date`, `night` |
| `daily-resting-heart-rate`, `daily-vo2-max` | likewise | `date` |
| `oxygen-saturation`, `heart-rate-variability`, `respiratory-rate-sleep-summary` | likewise | `night` |
| `steps` | `google_steps` | `source` (the recording device) |
| `weight`, `body-fat` | likewise | |
| `sleep` | `google_sleep`, `google_sleep_stage` | `night` (and `stage`) |
| `heart-rate`, with `heart_rate_minutes = true` | `google_heart_rate_minute` | `source` |

Every point is tagged `person`. Values are named as the export names them (Google's names in snake
case, `oxygen_saturation_percentage` for the SpO2 sample's `percentage`), and values of zero or
less in measurements are left out. Daily values are stamped at noon of their night, or at
midnight of their date, in `timezone` (UTC); samples carry their own offset. A sleep session still
being processed is left for a later poll, so its stages are written once final. The API leaves out
SpO2 readings it judges invalid, so it has fewer than an export, and gives some whole numbers
rounded (resting and non-REM heart rate). Sleep score, readiness, stress and the cardio workload
ratio are not in the API: import them from an export with `google_takeout`'s `api_data = false`.

The API needs a Google Cloud project with the Google Health API enabled, an OAuth consent screen
(External, its three `googlehealth.*.readonly` scopes, published "In production": while it is in
"Testing", Google ends a sign-in after seven days), and a "Desktop app" OAuth client, whose ID and
secret go in `client_id_file` and `client_secret_file`. An unverified app serves up to 100 people.
Each person then consents once, with the source's sign-in button on the status page: it opens
Google's consent, and the address the browser fails to open afterwards, on `127.0.0.1`, is pasted
back on the page. Without the web interface, the same takes two commands that wait for nothing,
so they also work through `docker exec`:

```sh
ingest-hub google-auth --source google_alex
# open the address it shows, sign in, allow all three, copy the address the browser ends up on
ingest-hub google-auth --source google_alex --address 'http://127.0.0.1:8765/?state=…&code=…'
```

The refresh token is kept in the source's state directory (`google-token.json`), readable by its
owner only. Google ends it when the person withdraws access or leaves it unused for six months;
the source then reports, on every poll, that the sign-in was refused and which command fixes it.

```toml
[sources.google_alex]
kind = "google_health"
destination = "health"

[sources.google_alex.params]
person = "alex"
client_id_file = "/run/secrets/google_health_client_id"
client_secret_file = "/run/secrets/google_health_client_secret"
timezone = "Europe/Amsterdam"
# history_days = 30
# heart_rate_minutes = true
```


See the [README](../../README.md#every-source) for what every source shares: credentials, state,
heartbeats and the status page.
