# Frank Energie

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

Gas prices go to `gas_table` (`gas_price`; set it to `""` to leave them out), hourly, tag
`resolution`, with the same fields. Both tables hold the prices Frank offers new customers; a
customer's own contract can differ (see `frank_price` below).

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
until electricity has all its hours and some usage and every other carrier with hours lacks at most
one (Frank leaves out a gas meter reading now and then, around midnight, and never fills it in),
or for 7 days, after which it is taken as it is
and reported; `ingest_progress` stream `usage` says how far usage is complete. `site` picks one of
the account's sites; the first by default. A failed round
of prices or account data is reported, and the source carries on with the next.


See the [README](../../README.md#every-source) for what every source shares: credentials, state,
heartbeats and the status page.
