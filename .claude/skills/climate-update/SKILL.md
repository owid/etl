---
name: climate-update
description: Run OWID's monthly climate data update. Bumps all updateable climate-namespace datasets to one common version in a single PR with one announcement, skipping the frozen sources. Also supports a wildfires-only subset for fire season. Use when the user wants to update climate data, run the monthly climate update, or refresh wildfires mid-season.
metadata:
  internal: true
---

# Climate update (monthly umbrella update)

All OWID climate data is updated once a month as a single batch: every updateable
climate-namespace dataset is bumped to one common version date, in one PR, with one
Slack announcement. This skill owns *which* datasets, *in what order*, and the
single-PR / single-announcement discipline. It delegates the per-dataset mechanics
(snapshot → meadow → garden → grapher, diffs, metadata checks, chart upgrade) to
[`/update-dataset`](../update-dataset/SKILL.md) — run each chain through that flow on
the same branch.

The matching reminder lives in `owid-issues/.github/workflows/update-climate.yml`
(one monthly issue on the 10th). There are no per-dataset climate reminders.

## Modes

- **Full monthly update** (default): bump every updateable dataset below to a common
  new version date.
- **Wildfires-only subset**: bump just `weekly_wildfires` (and optionally
  `yearly_burned_area`) to a new version. Use during peak fire season when wildfire
  charts need fresher data than the monthly cadence. Same mechanics, narrower scope.

## Updateable datasets (climate.yml "UPDATEABLE" section)

Grouped by source. Bump all of these to the **same** new version date.

| Source | Garden dataset(s) | Snapshot(s) | Cadence |
|---|---|---|---|
| Copernicus ERA5 | `surface_temperature` | `surface_temperature.zip` | monthly |
| Copernicus ERA5 | `total_precipitation` | `total_precipitation.zip` | **yearly** |
| GWIS | `weekly_wildfires` | `weekly_wildfires.csv` | continuous (fire season) |
| GWIS | `yearly_burned_area` | `yearly_burned_area.csv` | **yearly** |
| NOAA (Equatorial Pacific) | `sst` | `sst.csv` | monthly |
| NOAA GML | `ghg_concentration` | `co2/ch4/n2o_concentration_monthly.csv` | monthly |
| NSIDC | `sea_ice_index` | `sea_ice_index.xlsx` | monthly |
| NASA Ozone Watch | `nasa_ozone_hole` | `nasa_ozone_hole_p1/p2.txt` | **yearly** |
| Rutgers Global Snow Lab | `snow_cover_extent` | `snow_cover_extent_*.csv` | monthly |
| Hawaii Ocean Time-series | `ocean_ph_levels` | `hawaii_ocean_time_series.csv` | monthly |
| Met Office Hadley Centre | `sea_surface_temperature` | `sea_surface_temperature_*.csv` | monthly |
| Met Office Hadley Centre | `near_surface_temperature` | `near_surface_temperature_*.csv` | **yearly** |
| NOAA NCEI | `ocean_heat_content` | `ocean_heat_content_*.csv` | monthly |

Plus two derived/aggregate datasets with no snapshot of their own:
- `long_run_ghg_concentration` (combines NOAA `ghg_concentration` with the frozen EPA series)
- `climate_change_impacts` (the aggregate; pulls most of the rows above)

The **yearly** sources usually show no change in a given month — that is expected, not a bug.

## Frozen — NEVER bump these (climate.yml "NOT UPDATEABLE" section)

Skip entirely. They receive no new data and several feed `climate_change_impacts`:

- EPA 2024-04-17: `ghg_concentration`, `ocean_heat_content`, `ice_sheet_mass_balance`, `mass_balance_us_glaciers`
- `global_sea_level` (2024-01-28, NOAA Climate.gov)
- `ipcc_scenarios`
- The 12 migrated legacy chains (one-off papers / historical sources, dates 2017-2022)

If a frozen source's producer ever republishes, that is a separate, deliberate version
bump — not part of the monthly run.

## Dependency order

1. Bump and run the leaf source chains (snapshot → meadow → garden) for every updateable
   dataset above. `climate_change_impacts`'s frozen deps (EPA, global_sea_level) stay on
   their old versions — reference them unchanged.
2. Bump and run `long_run_ghg_concentration`, then the `climate_change_impacts` aggregate.
3. Run all grapher steps and the `climate_change` explorer (the explorer stays `latest`).

The grapher families to rebuild: the 7 `surface_*` graphers, `total_precipitation_annual`,
the 4 wildfire graphers, `sst`/`sst_by_month`, `nasa_ozone_hole`, the 3 `sea_ice_*`
graphers, `climate_change_impacts_annual`/`_monthly`, and `yearly_burned_area`.

## Procedure

1. Create the branch + draft PR with `etl pr "📊 Update climate data" data`. **One** branch
   and **one** PR for the whole batch.
2. Run each updateable chain through the `/update-dataset` flow on that branch, all targeting
   today's date as `<new_version>` so they land on a common version. Bump the aggregate
   (`climate_change_impacts`) only after its sources are done, so it picks up the new versions
   once rather than repeatedly.
3. Chart remap on staging via the Indicator Upgrader. Variables remap by catalogPath, so most
   charts follow automatically. **Watch the once-off cases**: any dataset moving from `latest`
   or changing namespace needs its remap reviewed explicitly (see below).
4. Verify on staging: Anomalist + Chart Diff (enable "Show all charts").
5. **One** announcement: run [`/data-updates-comms`](../data-updates-comms/SKILL.md) for the
   combined batch, post to #data-updates-comms, and draft the single `/latest` post. Do not
   produce per-dataset announcements.

## One-off migrations (only on the first run after the refactor)

These are structural moves that happen once, then the dataset behaves like any other updateable one:

- **`weekly_wildfires`: `latest` → versioned.** Its grapher variables get new IDs, so the wildfire
  charts (and any wildfires explorer) need a ghost-variable remap — see
  [`/remapping-ghost-variables`](../remapping-ghost-variables/SKILL.md).
- **`ipcc_scenarios`: namespace `emissions` → `climate`**, and **EPA 2024-04-17: namespace `epa` → `climate`**
  (retires the `epa` namespace). ipcc also moves its standalone explorer. EPA has no charts of its own
  (it only feeds `climate_change_impacts`), so its move is chart-free; ipcc's needs an explorer/chart remap.

After these land, update this skill's inventory if any short_names or namespaces changed.
