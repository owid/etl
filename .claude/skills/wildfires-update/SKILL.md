---
name: wildfires-update
description: Update OWID's GWIS weekly wildfires data (climate/*/weekly_wildfires) on its own, outside the monthly climate batch. Covers the source's weekly grid, the provisional last data point, the snapshot's silent per-country failure mode, and the in-season weekly cadence. Use when the user wants to refresh wildfires data, run the weekly fire-season update, or asks why our wildfire numbers lag or keep being revised.
metadata:
  internal: true
---

# Wildfires update (weekly, in season)

`climate/<version>/weekly_wildfires` is the one climate dataset that is updated on its own
schedule: weekly from May to the end of September, when most of the world is in fire season,
and with the monthly climate batch the rest of the year. The mechanics are the same as any
other dataset (snapshot → meadow → garden → grapher, via
[`/update-dataset`](../update-dataset/SKILL.md)); what this skill adds is the source's timing,
the traps in its numbers, and the checks that catch a bad fetch.

For the full monthly batch, see [`/climate-update`](../climate-update/SKILL.md). The in-season
reminder is `owid-issues/.github/workflows/update-climate-wildfires.yml`.

## The chain

| Step | Path |
|---|---|
| snapshot | `climate/<version>/weekly_wildfires.csv` (EFFIS API, ~12k country-year requests per endpoint) |
| meadow | `climate/<version>/weekly_wildfires` |
| garden | `climate/<version>/weekly_wildfires` (+ regions, + `faostat_rl` for land area) |
| grapher | `weekly_wildfires`, `wildfires_by_week`, `wildfires_by_year`, `wildfires_by_week_average` |

`wildfires_by_week_average` reads `wildfires_by_week`, not the garden table, so it has to run
after it.

## The source's weekly grid

GWIS reports 7-day bins anchored at 1 January, **labelled by their last day**: bin N ends on
day 7N of the year. Three consequences, all of which have bitten us:

- **The closing weekday shifts every year.** In 2026 bins close on Wednesdays; in 2027 they
  close on Thursdays. Never hardcode a weekday in code, and re-derive it when reviewing the
  cron on the reminder workflow.
- **`mddate` is authoritative, the `week` field is not.** The API's `week` counter is offset
  by one from the ISO week (its week 30 carries `mddate` 20260729, which falls in ISO week 31).
  The snapshot script parses `mddate`; keep it that way.
- **All 52 bins are always returned**, with nulls for bins that have not closed yet. So the
  presence of a row means nothing; only a non-null value does.

## The last data point is provisional

A closed bin keeps being revised **upwards** for one to two weeks as burnt-area detections are
reprocessed. Measured on the 2026 season:

| Bin read | Age at read | Shortfall vs settled value |
|---|---|---|
| closing 07-08, read 07-10 | 2 days | −5.2% |
| closing 07-22, read 07-27 | 5 days | −8.6% |
| closing 07-15 and earlier | ≥12 days | 0.0% (byte-identical across reads) |

So the newest point on every wildfire chart is an undercount, by something like 5-10%, until
the next update or two fills it in. Two things follow:

- **Never quote the latest week as a finished number**, in an announcement, a chart footnote or
  a reply to a journalist. This is exactly the mechanism behind "this is the quietest fire
  season on record" readings taken at the leading edge.
- **Expect chart diff to show the previous update's last point moving up.** That is the source
  settling, not a bug in our pipeline. What *would* be a bug is an older point moving, or any
  point moving down.

Waiting longer does not fix this, it only trades staleness for accuracy: there is no day of the
week on which the freshest bin is final.

## When to run

A closed bin is in the API within 2 days of closing (observed on the 2026-06-19 and 2026-07-10
runs, both of which picked up a bin that had closed 2 days earlier). Whether it lands sooner is
unknown: the API sends no `Last-Modified` header and there is no timestamp in the payload, so
the publication moment cannot be pinned down without polling.

Practical rule: **run at least 2 days after the bin closes.** In 2026 bins close Wednesday, so
the reminder fires Friday. A Monday run buys nothing in freshness (it carries the same
Wednesday bin, 5 days old instead of 2) and costs 3 days of lag on the site.

## Checks that matter

### 1. Per-country coverage (the silent failure)

`fetch_with_retry` returns `{}` on a 404, `fetch_fires`/`fetch_emissions` turn that into `None`,
and the country-year is dropped from the snapshot without a word. The script's only guard is a
total row count against the previous snapshot, which keeps growing every week regardless, so it
will not catch a handful of countries going missing. Check coverage explicitly:

```python
import pandas as pd

df = pd.read_csv("data/snapshots/climate/<version>/weekly_wildfires.csv",
                 usecols=["country", "year", "month_day", "indicator", "value"])
cur = df[(df["year"] == df["year"].max()) & df["value"].notna()]
last = cur.groupby("country")["month_day"].max()
print(last.value_counts())        # expect ONE row: every country ends on the same bin
print(df["country"].nunique())    # expect 252
```

Every country must end on the same bin. A country ending earlier than the others means its
fetch failed, and every regional aggregate containing it is understated.

### 2. The bin actually advanced

Compare the last non-null bin against the previous version. If it has not moved, the fetch
returned stale data; do not ship it as an update.

### 3. Garden asserts

The garden step already asserts ≥251 countries, a complete country × date grid, no
partially-reported date, and that `Europe − Europe (excl. Russia) = Russia` with `World` still
the sum of the six continents. If one fires, check the source before relaxing it: each of them
exists because the failure it catches is otherwise invisible in the charts.

## Regions

Russia is around 90% of "Europe"'s burnt area in every year, so the continental total mostly
tracks the Russian fire season and can move opposite to the rest of the continent.
`Europe (excl. Russia)` is aggregated alongside it so readers can tell them apart. FAOSTAT
publishes no land area for that aggregate, so its denominator is derived explicitly as
Europe minus Russia; without that, every `share_*` indicator for it comes out null.

## Procedure

1. Branch and draft PR: `etl pr "📊 Update weekly wildfires data" data`.
2. Bump the chain: `etl update snapshot://climate/<old>/weekly_wildfires.csv --include-usages`,
   then move the new dag entries into the place of the old ones and delete the superseded step
   files. **Keep the old snapshot version folder on disk until the new snapshot has run** — the
   script reads the most recent earlier version to compare row counts against, and fails with a
   missing-`outs` assertion if you have already deleted it.
3. `etls climate/<new>/weekly_wildfires` (a few minutes; ~24k API requests over 8 threads).
4. Run the checks above, then `etlr data://grapher/climate/<new>/wildfires_by_week_average`
   to build the whole chain.
5. Archive: commit the dag removal, run `etl archive-dag`, commit the regenerated archive.
6. Verify on staging with Chart Diff, expecting the previous last point to tick up.
7. **No separate announcement.** Wildfire refreshes ride on the monthly climate announcement;
   posting one per weekly refresh would flood #data-updates-comms.
