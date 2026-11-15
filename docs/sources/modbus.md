# Modbus

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


See the [README](../../README.md#every-source) for what every source shares: credentials, state,
heartbeats and the status page.
