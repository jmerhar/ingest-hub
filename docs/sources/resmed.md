# ResMed SD cards

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

Every point is also tagged `person`, whose CPAP it is (`person` in `params`, required, so the
data of several people can share a database), and `device`, the serial number the files carry;
`table_prefix` (`cpap_`)
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

The device's clock drifts, by minutes a year, and has no setting to correct it. To check it, note
the true time when you start a session; the session's file names in `DATALOG` give the device's
time for the same moment, to the second. `clock` lists such moments, and the source corrects the
times between them in a line, and beyond them as the nearest two continue:

```toml
[sources.cpap.params]
person = "alex"
inbox = "/data/inbox/cpap"
clock = [
  { device = 2025-11-19T22:00:00, actual = 2025-11-19T22:00:00 },   # right when it was new
  { device = 2026-11-14T02:17:14, actual = 2026-11-14T02:09:00 },
]
```

Mask periods, events and the 2-second series are corrected. A night's summary keeps its noon, and
`night` stays the device's own day. Each night keeps the correction it was first imported with
(the state directory remembers it), so importing it again writes the same points; a moment added
to `clock` later corrects the nights imported after it.


See the [README](../../README.md#every-source) for what every source shares: credentials, state,
heartbeats and the status page.
