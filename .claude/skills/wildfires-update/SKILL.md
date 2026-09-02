---
name: wildfires-update
description: Update OWID's GWIS weekly wildfires data (climate/*/weekly_wildfires) on its own, outside the monthly climate batch. A start-to-finish runbook covering the version bump, the snapshot, the checks, the chart remap on staging, and archiving. Also explains the source's weekly grid and why its most recent point is always an undercount. Use when the user wants to refresh wildfires data, run the weekly fire-season update, or asks why our wildfire numbers lag or keep being revised.
metadata:
  internal: true
---

# Wildfires update (weekly, in season)

`climate/<version>/weekly_wildfires` is the one climate dataset updated on its own schedule:
**weekly from May to the end of September**, when most of the world is in fire season, and with
the monthly climate batch the rest of the year. The reminder that triggers it is
`owid-issues/.github/workflows/update-climate-wildfires.yml`.

This is a full runbook, safe to follow without prior experience of this dataset. Run the steps
in order. If a check fails, **stop and ask** rather than working around it: every check here
exists because the failure it catches is invisible in the charts.

Related: [`/climate-update`](../climate-update/SKILL.md) for the monthly batch of all climate
datasets, [`/update-dataset`](../update-dataset/SKILL.md) for the generic update flow.

## What you are updating

| Step | Path |
|---|---|
| snapshot | `climate/<version>/weekly_wildfires.csv` (EFFIS API, ~24k requests, ~6 min) |
| meadow | `climate/<version>/weekly_wildfires` |
| garden | `climate/<version>/weekly_wildfires` (+ regions, + `faostat_rl` for land area) |
| grapher | `weekly_wildfires`, `wildfires_by_year`, `wildfires_by_week`, `wildfires_by_week_average` |

Four grapher datasets, feeding **14 charts and 2 narrative charts**. `wildfires_by_week_average`
reads `wildfires_by_week` rather than the garden table, so it has to run after it.

## Run it

Throughout, `<old>` is the current version folder (e.g. `2026-07-27`) and `<new>` is today's date.

### 1. Branch and draft PR

```bash
.venv/bin/etl pr "📊 Update weekly wildfires data" data
```

### 2. Bump the version

```bash
.venv/bin/etl update snapshot://climate/<old>/weekly_wildfires.csv --include-usages
```

This copies the step files into `<new>` folders and **appends** new entries to the end of
`dag/climate.yml`, leaving the old ones in place. Three manual fixes, all needed:

- Move the new dag entries into the place of the old wildfires block, preserving its nesting and
  its `# Global Wildfire Information System - Weekly wildfires.` comment header, and delete the
  appended block at the end of the file.
- Delete the old step files, but **not** the old snapshot folder yet:
  `git rm -r etl/steps/data/{meadow,garden,grapher}/climate/<old>`.
- `snapshots/climate/<old>/` has to stay on disk until step 3 has run, because the snapshot script
  compares row counts against the most recent *earlier* version. Delete it too early and step 3
  dies with `.dvc file is missing 'outs' field. Have you run the snapshot?`. If that happens,
  restore it with
  `git show HEAD:snapshots/climate/<old>/weekly_wildfires.csv.dvc > snapshots/climate/<old>/weekly_wildfires.csv.dvc`,
  run step 3, then delete it.

### 3. Fetch the snapshot

```bash
.venv/bin/etls climate/<new>/weekly_wildfires
```

Around 6 minutes, and it prints no progress until it finishes. Once it has, delete the old
snapshot folder: `git rm -r snapshots/climate/<old>`.

### 4. Build the rest of the chain

```bash
.venv/bin/etlr climate/<new> --private
```

Runs meadow, garden and all four grapher steps (plus any stale upstream deps). The garden step's
asserts run here; if one fires, see [Checks](#checks-that-must-pass) below.

### 5. Check the data

Run the three checks in [Checks](#checks-that-must-pass). Do not skip the per-country coverage
one: it is the only thing standing between a partial fetch and understated regional aggregates.

### 6. Commit, archive, push

```bash
make check                       # format, lint, typecheck
git add -A && git commit -m "📊 Update weekly wildfires data to <new>"
.venv/bin/etl archive-dag        # reads COMMITTED history, so commit first
git add dag/archive/climate.yml && git commit -m "🔨 Archive the superseded <old> wildfires steps"
git push -u origin <branch>
```

`archive-dag` should add exactly the six `<old>` wildfires steps. If it sweeps in unrelated steps
somebody else left un-archived, `git checkout` those lines to keep the PR scoped, and never
hand-edit the archive file.

### 7. Remap the charts on staging (do not skip this)

The version bump mints **new variable IDs**. Until they are mapped, every wildfire chart still
points at the old ones, so the site keeps showing the old data and **Chart Diff shows nothing at
all**. An empty Chart Diff after a wildfires update almost always means this step was skipped,
not that nothing changed.

First wait for the staging server to rebuild and upsert the new datasets (roughly five minutes
after the push). Then find the dataset IDs:

```python
from etl.config import OWIDEnv
env = OWIDEnv.from_staging("<branch>")
print(env.read_sql(
    "SELECT id, catalogPath, updatedAt FROM datasets WHERE catalogPath LIKE %(p)s ORDER BY catalogPath",
    params={"p": "climate/%wildfires%"},
).to_string())
```

Two things that will trip you up here: `datasets.catalogPath` has **no `grapher/` prefix**
(`climate/2026-08-01/weekly_wildfires`), and `read_sql` uses pymysql's `%(name)s` placeholders,
not `:name`. If the four `<new>` rows are missing, staging has not finished; wait rather than
proceeding.

Then match each old dataset to its new counterpart and apply. Four pairs, matching short names
in the same order (`weekly_wildfires`, `wildfires_by_year`, `wildfires_by_week`,
`wildfires_by_week_average`):

```bash
STAGING=1 .venv/bin/etl indicator-upgrade match -old <old_id> -new <new_id> --perfect-match-only
# ... once per pair, then:
STAGING=1 .venv/bin/etl indicator-upgrade upgrade --dry-run
STAGING=1 .venv/bin/etl indicator-upgrade upgrade
```

Expected output: names are identical across versions, so every old indicator gets a perfect
match (68 mappings in total for the 2026-08-01 run, across 14 charts and 2 narrative charts).
Two normal-looking warnings that are **not** problems:

- *"N unmatched variables in new dataset"* — `match` only considers old variables that at least
  one chart uses, so new indicators nothing charts yet are correctly left alone.
- *"All variables in the old dataset have been matched"* is the line you actually want to see. If
  instead some **old** variables are unmatched, stop: a short name changed and the mapping needs
  a human.

`etl indicator-upgrade auto` reports *"No dataset migrations detected"* for this dataset and does
nothing. Use the explicit per-pair `match` above. To reverse a bad remap:
`STAGING=1 .venv/bin/etl indicator-upgrade undo`.

Confirm it took, by checking that no chart is left on the old datasets:

```python
print(env.read_sql("""
  SELECT d.catalogPath AS dataset, COUNT(DISTINCT cd.chartId) AS n_charts
  FROM datasets d JOIN variables v ON v.datasetId = d.id
  LEFT JOIN chart_dimensions cd ON cd.variableId = v.id
  WHERE d.catalogPath LIKE %(p)s GROUP BY d.catalogPath ORDER BY d.catalogPath
""", params={"p": "climate/%wildfires%"}).to_string())
```

The `<old>` rows must all read 0 and the `<new>` rows must carry the charts (2 / 7 / 5 / 2 for
`weekly_wildfires` / `by_year` / `by_week` / `by_week_average`).

### 8. Verify on staging, then merge

```bash
curl -s "http://staging-site-<branch>/grapher/weekly-area-burnt-by-wildfires.csv" | awk -F, '{print $3}' | sort -u | tail -2
```

The last date should be the new bin, and production's should still be the old one. Then open
Chart Diff, enable "Show all charts", and read it with the next section in mind.

**No separate announcement.** Wildfire refreshes ride on the monthly climate announcement;
one post per weekly refresh would flood #data-updates-comms.

## How to read what changed

Two things always change, and only one of them is new data:

1. **A new weekly bin appears.** That is the update.
2. **The previous last point moves up.** That is the source settling, and it is expected. What
   would *not* be expected, and is worth stopping for: an older point moving, or any point
   moving down.

### The source's weekly grid

GWIS reports 7-day bins anchored at 1 January, **labelled by their last day**: bin N ends on day
7N of the year. Consequences:

- **The closing weekday shifts every year.** 2026 bins close on Wednesdays, 2027 bins on
  Thursdays. Never hardcode a weekday, and re-derive it when revisiting the reminder's cron.
- **`mddate` is authoritative; the API's `week` field is not** — it is offset by one from the ISO
  week (its week 30 carries `mddate` 20260729, which falls in ISO week 31).
- **All 52 bins are always returned**, with nulls for those that have not closed. A row existing
  means nothing; only a non-null value does.

### The last point is provisional

A closed bin keeps being revised **upwards** for one to two weeks as burnt-area detections are
reprocessed. Measured on the 2026 season:

| Bin read | Age at read | Shortfall vs value at a later read |
|---|---|---|
| closing 07-08, read 07-10 | 2 days | −5.2% |
| closing 07-22, read 07-27 | 5 days | −8.6% |
| closing 08-05, read 08-14 | 9 days | −9.2% |
| closing 07-29, read 08-07 | 16 days | −2.8% |
| closing 07-22, read 08-07 | 23 days | −2.5% |
| closing 07-15 and earlier, read 08-14 | ≥30 days | 0.0% (identical across reads) |

Don't read the older rows as a settled floor: they are lower bounds against the *next* read, not
against a final value, and each was still moving. Three-plus weeks is the first age at which a bin
has been observed not to move at all.

So the newest point on every wildfire chart is an undercount of roughly 5-10% until a later
update fills it in. **Never quote the latest week as a finished number** in an announcement, a
footnote, or a reply to a journalist: this is precisely the mechanism behind leading-edge
readings of a record-quiet fire season.

Waiting longer trades staleness for accuracy but never removes the effect. A closed bin is in the
API within 2 days (observed on the 2026-06-19 and 2026-07-10 runs); there is no
`Last-Modified` header and no timestamp in the payload, so the exact publication moment is
unknown. Hence the reminder runs 2 days after the bin closes, and no day of the week yields a
settled latest point.

## Checks that must pass

### 1. Per-country coverage — the silent failure

`fetch_with_retry` returns `{}` on a 404, `fetch_fires`/`fetch_emissions` turn that into `None`,
and that country-year vanishes from the snapshot without an error. The script's only guard is a
total row count against the previous snapshot, which grows every week regardless, so it does not
catch a handful of countries going missing.

```python
import pandas as pd

df = pd.read_csv("data/snapshots/climate/<new>/weekly_wildfires.csv",
                 usecols=["country", "year", "month_day", "indicator", "value"])
cur = df[(df["year"] == df["year"].max()) & df["value"].notna()]
print(cur.groupby("country")["month_day"].max().value_counts())  # expect ONE row
print(df["country"].nunique())                                    # expect 252
```

Every country must end on the same bin. One country ending earlier means its fetch failed, and
every regional aggregate containing it is understated.

### 2. The bin actually advanced

The last non-null bin must be newer than in the previous version. If it has not moved, there is
nothing to ship: the fetch returned the same data, which during fire season means a problem at
the source rather than a quiet week.

### 3. Garden asserts

The garden step already asserts ≥251 countries, a complete country × date grid, no partially
reported date, `Europe − Europe (excl. Russia) = Russia`, and `World` still equal to the sum of
the six continents. If one fires, check the source before touching the assert.

## Regions

Russia is around 90% of "Europe"'s burnt area every year, so the continental total mostly tracks
the Russian fire season and can move opposite to the rest of the continent.
`Europe (excl. Russia)` exists so readers can tell them apart. FAOSTAT publishes no land area for
that aggregate, so its denominator is derived as Europe minus Russia; without that, every
`share_*` indicator for it comes out null.
