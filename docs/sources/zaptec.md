# Zaptec

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


See the [README](../../README.md#every-source) for what every source shares: credentials, state,
heartbeats and the status page.
