# Tuya

`kind = "tuya"` reads every device of a Tuya cloud project from its report logs, with the project's
keys in `access_id_file` and `access_secret_file`.

One table per device category, `table_prefix` + category (`tuya_cz` for plugs, `tuya_cs` for
dehumidifiers), at millisecond precision from Tuya's own event times. Tags `device_id` (exactly
as Tuya gives it), `category`, and `home`, the name of the home the device is in within the app
(its id when the home has no name or the project may not read it; no tag for a device in no
home). A field `name` holds the device's name at the time. One field per
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


See the [README](../../README.md#every-source) for what every source shares: credentials, state,
heartbeats and the status page.
