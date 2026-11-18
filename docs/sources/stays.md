# Stays

`kind = "stays"` reads a JSON feed of stays, such as a holiday rental's bookings, so other data
can be split by when someone was staying: what a guest's stay used, or what ran while the place
stood empty.

The source asks `GET url`, adding `token_param=TOKEN` (`token`) when `token_file` is set and
`since_param=YYYY-MM-DD` (`since`) to ask for stays from a date; set `since_param = ""` for a
feed that has no such parameter. It expects:

```json
{"from": "2026-10-19",
 "stays": [{"id": "26b21016bfab7bf2", "check_in": "2026-11-16", "check_out": "2026-11-22",
            "nights": 6, "source": "airbnb", "status": "booked"}]}
```

- `stays` lists every stay with a night on or after `from`. A stay missing from the list has been
  cancelled.
- `check_in` and `check_out` are dates in `timezone` (UTC). `check_out` is the day the stay ends,
  so it is not a night.
- `id` stays the same when a stay's dates change.
- `nights`, `source` (the channel it came through) and `status` are optional. `nights` defaults
  to the days between the dates.

Every `poll_minutes` (60) the source reads the feed. The first read asks `history_days` (365)
back, and later reads ask `window_days` (30) back. It writes one point per stay to `table`
(`stays`), tagged `stay` with the stay's id, at the moment of check-in. Its fields are:

| Field | Holds |
|---|---|
| `start` | the check-in moment, in Unix milliseconds: the `check_in` date at `check_in_time` (16:00) |
| `end` | the check-out moment, in Unix milliseconds: the `check_out` date at `check_out_time` (11:00) |
| `check_in`, `check_out` | the dates, as the feed gives them |
| `nights` | the number of nights |
| `source` | the channel the stay came through |
| `status` | the feed's status, or one the source sets (below) |

A `check_out_time` of `"24:00"` ends a stay at the end of its check-out day. With a
`check_in_time` of `"00:00"`, a stay then covers the whole of both days.

A stay is written again only when something about it changes. A stay that a read covering its
dates no longer lists is written again with status `cancelled`. When a stay's check-in moves, it
is written at its new moment, and its old point is written again with status `moved`. Each stay
so has one point with its current status, so leave out `cancelled` and `moved` to count the stays
that took place. The source keeps the stays it has seen in its state directory. The feed's
progress, stream `stays`, is the time of the last good read.


See the [README](../../README.md#every-source) for what every source shares: credentials, state,
heartbeats and the status page.
