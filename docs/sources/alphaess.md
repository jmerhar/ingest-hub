# AlphaESS cloud

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


See the [README](../../README.md#every-source) for what every source shares: credentials, state,
heartbeats and the status page.
