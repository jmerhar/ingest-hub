# Home Connect

`kind = "home_connect"` follows every appliance of a Home Connect account (Bosch, Siemens, Neff,
Gaggenau and the other brands on BSH's platform) through the official
[Home Connect API](https://api-docs.home-connect.com/), read-only. When it connects it reads the
paired appliances and each connected one's status, settings and programmes, then follows Home
Connect's event stream, writing each change as it arrives. Every `sample_minutes` (15) it writes
each appliance's whole state again from what it already knows, so a quiet appliance still has
recent points. A stream that ends is opened again, reading the full state again only when the last
read is `resync_minutes` (15) old, or for an appliance that reconnects: Home Connect allows 1000
requests a day, and the stream itself costs one.

A read replaces what the source holds for an appliance, since Home Connect stops sending what no
longer applies rather than clearing it. So when an appliance's operation state changes (a program
starts, finishes, pauses), its state is read again, at most every five minutes, and the
whole-state points then repeat only what is true: a finished program's progress and phase, or an
event such as the program having finished, are written when they happen and not repeated after.

Each appliance type has its table, `home_connect_<type>` (`home_connect_dishwasher`), tagged
`appliance` with its Home Connect ID. A key becomes the field `<kind>_<name>` in snake case:

| Key | Field | Value |
|---|---|---|
| `BSH.Common.Status.DoorState` | `status_door_state` | `Open` (enumerations without their type's prefix) |
| `BSH.Common.Setting.PowerState` | `setting_power_state` | `Off` |
| `BSH.Common.Option.RemainingProgramTime` | `option_remaining_program_time` | seconds, as a float |
| `Dishcare.Dishwasher.Event.RinseAidLack` | `event_rinse_aid_lack` | `Present`, `Off`, `Confirmed` |
| `BSH.Common.Root.ActiveProgram`, `…SelectedProgram` | `active_program`, `selected_program` | `Eco50` (empty when none) |

plus `connected`. Numbers are floats, booleans booleans, text strings; a field keeps the type it
was first written with, and a value of another type is left out and reported. The options of the
selected programme share their fields with the active one's, which win while a programme runs.
Each appliance's progress is the time of the last message received, labelled with its brand,
model and type. Energy and water use are not measured by the appliances; some report
`option_energy_forecast` and `option_water_forecast`, the selected programme's expected use in
percent.

It needs an application registered in the
[Home Connect Developer Program](https://developer.home-connect.com/) with the OAuth flow "Device
Flow"; its client ID goes in `client_id_file`. While the application is unpublished, only the Home
Connect account it names for testing can sign in. Sign in once, with the source's sign-in button
on the status page: it shows a link and a code to confirm there, and notices by itself when that
is done. Without the web interface, the command waits until the code it shows is confirmed:

```sh
ingest-hub home-connect-auth --source appliances
```

It asks for read-only access (`IdentifyAppliance Monitor Settings`). The tokens are kept in the
source's state directory (`home-connect-token.json`), readable by their owner only, and the access
token is renewed daily. A sign-in Home Connect no longer accepts, or none at all, ends each run
with the command that fixes it; the sign-in button fixes it too.

```toml
[sources.appliances]
kind = "home_connect"
destination = "home"

[sources.appliances.params]
client_id_file = "/run/secrets/home_connect_client_id"
# sample_minutes = 15
# resync_minutes = 15
```


See the [README](../../README.md#every-source) for what every source shares: credentials, state,
heartbeats and the status page.
