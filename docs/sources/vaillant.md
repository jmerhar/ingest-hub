# Vaillant

## The myVAILLANT cloud

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
ends with the hour; a refusal that persists shows as writes and progress that stop. Bucket series
are
requested one at a time, a couple of seconds apart, and the first energy poll starts a minute
after the first state poll.

## EU Data Act exports

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
otherwise; `NO_VALUE` is left out. Controller state names are the export's in snake case, as the
app's API
gives them, except `currentCircuitTemperature`, written as `current_dhw_temperature`. Every point is
stamped with its message's timestamp. A device is
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


See the [README](../../README.md#every-source) for what every source shares: credentials, state,
heartbeats and the status page.
