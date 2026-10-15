# Parked: national TFR vs UN WPP

Compares each of the 100 most populous countries' own official fertility rate against UN WPP.
`build.py` writes the report to `tfr_charts.html`.

## State

- All 100 countries reviewed; findings logs in `redteam/`, campaign state in `redteam/AGENTS.md`.
- The UK is built from all three registration offices (`uk.py`).
- Series caches rebuild automatically when a loader's code changes.

## Not built

- Jordan: the civil registry's own rate, 2015-2022, Jordanians only.
- Honduras: registration-based rates for 2013-2016 from INE's vital-statistics bulletins.
- `refyears/` holds research on which year each survey estimate describes. It was dropped by
  decision; nothing reads it.

## Source data

The downloaded source files and the series caches are not in git. Most loaders re-download
what they need through `fetch()`.
