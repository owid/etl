---
name: add-gdp-scatter
description: Add a scatter view (with GDP per capita on x) to existing OWID charts via the admin API, mirroring the admin UI's "Add scatter type" defaults, then retire the old standalone "X vs. GDP per capita" charts by redirecting their slugs to that scatter view. Trigger when the user pastes a table with columns `chart_admin_url`, `target_chart_admin_url`, `gdp_source` (part 1), or a list of `{grapher_url, target_chart_url}` pairs to redirect (part 2).
metadata:
  internal: true
---

# add-gdp-scatter

Bulk-add a scatter view to a set of OWID charts, with the same defaults the admin UI's `applyDefaultsForScatter` applies — plus extra parity checks against a reference scatter chart.

## When to invoke

The user pastes (TSV or CSV) a table like:

```
chart_admin_url	target_chart_admin_url	gdp_source
https://admin.owid.io/admin/charts/1035/edit	https://admin.owid.io/admin/charts/7842/edit	Maddison Project Database
https://admin.owid.io/admin/charts/6305/edit	https://admin.owid.io/admin/charts/6918/edit	World Bank
```

- `chart_admin_url` — the existing reference scatter chart used as the source of parity hints (yAxis log, y `display.name`, color/size override, tolerance, exclusions).
- `target_chart_admin_url` — the chart the user wants to gain a scatter view.
- `gdp_source` — one of (case-insensitive, substring matches accepted):
  - `World Bank` / `WDI` → variableId `1294305` (WDI 2026-07-27)
  - `Maddison` / `Maddison Project Database` → `900793`
  - `PWT` / `Penn World Table` → `1108541`

  `GDP_SOURCES` in the script is the authority on these ids; the WDI one goes stale on every WDI update — see the version check below.

The admin host that gets written to is `OWID_ENV.admin_api`, which auto-resolves to `staging-site-<branch>` on a feature branch. Confirm the branch before running.

## Pre-flight: GDP version check

Before processing any rows, the script queries `variables` for the latest id matching each canonical GDP-per-capita `catalogPath` pattern (`worldbank_wdi/.../ny_gdp_pcap_pp_kd`, `ggdc/.../maddison_project_database#gdp_per_capita`, `ggdc/.../penn_world_table#rgdpo_pc`). If the latest id differs from the one hardcoded in `GDP_SOURCES`, a `WARN` is printed at the top of stdout with the newer id and catalogPath. The script does NOT auto-switch — update `GDP_SOURCES` (and the `feedback_scatter_gdp_picker` memory) deliberately if the new version is the one we want.

**Read that WARN before the run, not after.** These ids go stale on every WDI update, and the "target already has `x` → leave it" guard means a re-run will **not** repair a target that already received the stale variable — the `x` dimension has to be rewritten directly, chart by chart. Cross-check against the id the source scatters actually plot: if the sources are on a newer version than `GDP_SOURCES`, the newer one is what you want, and bumping first saves a repair pass. (2026-08-04: every source plotted WDI 2026-07-27 `1294305` while the script was still pinned to 2026-02-27 `1204826`.)

## What the script does (per row)

Mirrors the admin's `applyDefaultsForScatter` and the extra moves we agreed on:

1. Adds `ScatterPlot` to `chartTypes`, preserving existing tabs. Seeds the schema default `[LineChart, DiscreteBar]` when `chartTypes` is unset.
2. Appends x (the chosen GDP variable), color, size dimensions if absent.
   - **color**: if the source uses a non-default color variable (e.g. World Bank income groups), mirror it; otherwise use `CONTINENTS_ID=900801`.
   - **size**: the rule is *always use the default `Population` indicator (`POPULATION_ID=953899`) for any population-type size*. If the source sizes by any population variant (regular, historical, WPP, …), the target gets the default Population. A genuinely **non-population** size (e.g. GDP, area) is mirrored as-is **but raises a `WARN`** so the bubble sizing gets a manual review. **If the source has no `size` dim at all, the target also gets none** — the script won't add sizing the curator deliberately omitted. Population variants are detected by the variable's name starting with "Population" or its catalogPath living under a `/population/` dataset; the action note records any normalization.
3. Sets `matchingEntitiesOnly: true`.
4. Sets `xAxis` to `scaleType: log` + `canChangeScaleType: true`.
5. **Y-axis log toggle (not forced):** when the source scatter is `scaleType: log`, only enable the toggle (`canChangeScaleType: true`) and leave the default **linear**. `yAxis` is shared across all views, so forcing log would flip the line/bar views too. **Mirrors explicit `yAxis` min/max bounds** the source sets (each bound copied independently) — **except a non-zero `min` is NOT mirrored when the target has a `Marimekko` or `Stacked*` view**, because those draw from a baseline and a scatter-tuned non-zero min would make them start above zero (misleading). **`DiscreteBar` is not in that set**: `DiscreteBarChart.yAxisConfig` hardcodes `min: undefined` and anchors at zero, so it ignores `yAxis.min` outright — withholding the min from a DiscreteBar target protects nothing and costs the scatter a well-fitted axis. A degenerate `min: 0` + `max: 0` (collapsed axis) has its `max` stripped. Note: y-axis bounds affect all views, not just scatter.

   **On y-axis bounds, prefer removing to pinning.** `Axis.updateDomainPreservingUserSettings` takes `min(config.min, data.min)` and `max(config.max, data.max)`, so an authored `min` is a hard floor across every view — which is why `{min: 0, max: 0}` renders identically to `{min: 0}` (the data max always wins) and is *not* the inert junk it looks like. When a reviewer says the scatter's axis is wasting space at zero, **dropping `min` usually beats mirroring the source's**: each view then fits its own data, and mirroring a non-zero min can clip a LineChart whose series run below it. (2026-08-04, chart 2201: source min was 5, but the line data reaches 0.92; dropping `min` gave the scatter 6.94–13.97 while the line chart moved only 0 → 0.92, and the DiscreteBar was unaffected either way.)

### The scatter's single-year default needs no config (usually)

A recurring question: *the scatter should show only the latest year, without affecting the other views — is that possible?* **Yes, and Grapher already does it — do not set `minTime`/`maxTime` for this.**

`minTime`/`maxTime` are **global**. Only the map has its own time (`map.time` / `map.startTime` in `MapConfigInterface`); there is no per-chart-type time override, so pinning them to the latest year would collapse a LineChart to a single point.

Grapher handles it at runtime instead. `checkSingleTimeSelectionPreferred` returns true for the `ScatterPlot` tab whenever the scatter is **not the primary chart type** and the chart is not in relative mode, and `adjustStateForTab` → `ensureTimeHandlesAreSensibleForTab` then collapses both time handles onto the end (latest) time. That is runtime state, so the line/bar/map views keep their full range. Since the applier *appends* `ScatterPlot`, it is never `chartTypes[0]` and the condition holds by construction.

**But it only fires when the reader CLICKS the tab** — see the shared caveat below.

Also: **`hideTimeline: true` breaks it even on a tab click.** With a hidden timeline, `timelineHandleTimeBounds` reads the **authored** `minTime`/`maxTime` on every chart tab and ignores the runtime handles, so the collapse never takes effect — and the reader has no slider to fix it. Authored `minTime == maxTime` is then the only fix, and it is only safe when every other tab is single-time anyway (`DiscreteBar`/`StackedDiscreteBar`/`Marimekko`). With a `LineChart`, `SlopeChart` or single-indicator `Dumbbell` in the mix, one global time cannot serve both — un-hide the timeline or accept the range. The script emits a `WARN` for each case. (2026-08-04: chart 1253, `DiscreteBar` + `hideTimeline`, needed `minTime`/`maxTime` = `latest`; the other 16 targets in that batch needed nothing.)

### `adjustStateForTab` fires on a tab CLICK only — not on a direct URL load

Both scatter adjustments — collapsing the time handles **and** clearing the entity selection — live in the same function behind the same guard:

```ts
if (!this.isEditor) {
    this.ensureEntitySelectionIsSensibleForTab(tab)
    this.ensureTimeHandlesAreSensibleForTab(tab)
}
```

So they always happen together, or not at all. `adjustStateForTab` has exactly one production caller, `onTabChange`, which in turn has exactly one: the `ContentSwitchers` tab control. A tab supplied in the URL takes a different path — `populateFromQueryParams` → `setTab`, which only assigns `this.tab`. Three consequences:

- **Clicking the scatter tab**: time collapses to the latest year *and* the selection is cleared. The scatter matches the old standalone chart.
- **Landing directly on `?tab=scatter`**: neither happens. The scatter opens on the authored time range with the authored entities highlighted — **unless the URL says otherwise**. This is the path **Part 2's redirect uses**, which is why every part of its stored `tab=scatter&time=latest&country=` is load-bearing: each param hand-supplies one adjustment the click would have made. `time=latest` stands in for `ensureTimeHandlesAreSensibleForTab`, and `country=` (present, empty) for `ensureEntitySelectionIsSensibleForTab` — `parseCountryParam` returns `valid([])` for an empty value and `setSelectedEntities([])` clears, so the scatter shows every entity unhighlighted. **Whenever a new tab-click adjustment is added upstream, a matching param has to be added here** or the two paths drift apart again. `country=` has not yet been exercised through a live redirect — confirm in a browser on the first `--apply` run (see "Verifying Part 2").
- **The admin editor shows neither**, because of the `isEditor` guard — deliberate, so switching tabs cannot mutate the authored config on save (grapher #6794). A scatter that looks wrong in `/admin/charts/<id>/edit` may be fine for readers. Verify on the chart page.

### The target's entity selection highlights the scatter, it does not filter it

Targets normally carry a `selectedEntityNames` list for their line/bar view (4–20 entities is typical) while the source scatters carry **none** — they show every country. That asymmetry does not hide data on the new scatter view:

- `ScatterPlotChartState.seriesNamesToHighlight` uses the selection to **highlight** only; every entity is still plotted.
- Axis domains narrow to the selection only via `pointsForAxisDomains`, and only when **`zoomToSelection`** is set. Check that field — with it, a scatter's axes really would zoom to the highlighted subset.
- On a **tab click**, `ensureEntitySelectionIsSensibleForTab` clears the selection entirely (`CHART_TYPES_THAT_SHOW_ALL_ENTITIES` is `[ScatterPlot, Marimekko]`) so long as it is still the authored one — the scatter then looks exactly like the old standalone chart. On a **direct URL load it does not**, for the reason in the section above; the authored entities render highlighted.

That second case is what **Part 2's redirect produces**, so a reader arriving by a retired scatter's URL sees the same data and axes but with a few countries emphasized — visually unlike the chart they used to get, and unlike what a reader who clicks the tab gets. Decide per batch whether that is acceptable, and see the `country=` note above for the fix.

### Cross-view safety (which fields are global)

`yAxis` (scaleType, min, max) is the only config the skill writes that meaningfully bleeds into the non-scatter views — hence the log-toggle and zero-baseline handling above. The others were checked and are safe: `xAxis.scaleType: log` is ignored by Line/DiscreteBar (they hardcode a linear time axis) and has no visible effect on Slope; the `color` dimension does **not** recolor line/bar (they color by entity); `size` is scatter-only (not even in the table tab); `matchingEntitiesOnly` is honored only by Scatter and Marimekko.
6. **Mirrors source's manually-set y `display.name`** when present.
6b. **Mirrors source `comparisonLines`** when the target has none. A scatter's reference line (e.g. `yEquals: 1` on a ratio-to-a-benchmark indicator) is often the whole point of its framing, so dropping it makes the migrated view say less than the chart it replaces. Never overwrites an existing set.
7. Emits warnings (no action) for:
   - Target has no `selectedEntityNames` — line/bar/slope views will fall back to Grapher defaults.
   - Target `stackMode: relative` — on scatter this is the "Display average annual change" mode; we want the toggle available but **off by default**, so a relative default is flagged for review.
   - Source `excludedEntityNames` — would apply across all views, not just scatter.
   - GDP coverage mismatch — if y-indicator's earliest year predates the chosen GDP's coverage (WDI≈1990, PWT≈1950, Maddison≈year 1), suggest a deeper-history alternative.
   - Few entities on default scatter view — counts entities with both a y- and an x-value within tolerance at the default time; if fewer than ~15 AND source uses higher tolerance, recommends bumping target's y `display.tolerance`.

Push uses `apps.chart_sync.admin_api.AdminAPI.update_chart(id, cfg)`.

## Run this as a checklist in the chat

**Create a `TodoWrite` list covering the WHOLE migration on the first step, before touching anything** — not just the part being worked on now. This migration's failure mode is not getting a step wrong, it is losing a step: the work spans two scripts, a human review round, a merge, and a production run, with days between them. Anything not on the list from the start gets discovered later by a reader hitting a 404 or an article rendering the wrong tab.

So **the reference sweep and Part 2 go on the list as pending from the very beginning**, even when the request is only "add the scatter views". They are the two that get forgotten, and they are the two that break things for readers.

The canonical items, in order:

1. Confirm the branch / which admin host `OWID_ENV` resolves to (on `master` that is **production**).
2. Pre-flight every row (`preflight_targets.py`); report and drop the blocked ones.
3. Act on the GDP version `WARN` — bump `GDP_SOURCES` *before* applying if the sources plot a newer id.
4. Apply (`apply_scatter_defaults.py`).
5. Verify every target: `ScatterPlot` present, log x-axis, and the **current** GDP id on `x`.
6. Display-name follow-up — **after the final applier run**, or the next run re-mirrors it.
7. Build the review HTML (`build_review.py`) and hand it to the topic owner.
8. Apply the reviewer's flagged notes; regenerate the HTML and re-import their JSON.
9. Chart-diff sign-off on staging, then merge.
10. **Confirm the scatter views actually reached production.** A merged PR is not evidence that they did: chart-sync only carries chart edits whose diffs were **approved** in Chart Diff, so a PR can merge green with every row ✅ on staging and leave production untouched. An abandoned first attempt (PR #6173, merged 2026-06-24) left production untouched on all seven of its pairs — deliberately: the `target_query_param` needed for Part 2 did not exist yet, so it was dropped and the migration restarted from scratch rather than left half-done. Whatever the reason, check production directly rather than inferring it from the merge.
11. **Reference sweep on the old charts** — `find-chart-references` over each source slug *and its aliases*, then `scripts/build_reference_handoff.py` to turn it into the handoff (it keeps the sweep's 📄 doc / 👁 preview / 🔗 page links and its "Find in the doc" search string — see below). Re-point embeds and links at the target's scatter view **before** retiring anything: an embed is never fixed by a redirect, and a link that works only via a 301 outlives everyone's memory of why. **Do not skip this because the Part 2 audit reports few references** — it counts a narrower set; see the key-chart trap below.
12. Narrative charts on the sources: replace where the parent is being retired (create → re-point articles → delete; never delete first).
13. **Part 2 audit** — `redirect_to_scatter.py` with no `--apply`. Read every verdict.
14. Part 2 `--apply` on staging, then the browser checks in "Verifying Part 2".
15. Part 2 `--apply --allow-production` once the scatter views are live on production, then the same checks against the live site.

Keep the list alive across turns: carry the untouched items forward rather than reporting only the delta, and say when one clears. Items 11–15 stay visible as pending the entire time Part 1 is being worked on.

## Workflow

1. **Parse the pasted table** into a JSON list, one object per row with keys `chart_admin_url`, `target_chart_admin_url`, `gdp_source`. Strip the header. Accept tab- or comma- separated.

2. **Pre-flight every row before writing anything** with `scripts/preflight_targets.py`. The applier does not validate that a target *can* take a scatter view — it returns `OK` on rows that change nothing useful — so this check has to happen first:

   ```bash
   echo '<JSON>' | .venv/bin/python .claude/skills/add-gdp-scatter/scripts/preflight_targets.py
   ```

   It reads each target's **production** config (the state a staging DB was cloned from, so the baseline holds even if an earlier run already touched staging) and checks the four conditions from "Picking targets": published, not already a `ScatterPlot`, not stacked-family, exactly one `y` dimension, and that `y` is the source's non-GDP indicator. Report the blocked rows to the user and drop them. Add `--emit` to pipe the runnable subset straight into the applier:

   ```bash
   echo '<JSON>' | .venv/bin/python .claude/skills/add-gdp-scatter/scripts/preflight_targets.py --emit \
     | .venv/bin/python .claude/skills/add-gdp-scatter/scripts/apply_scatter_defaults.py
   ```

3. **Run the script**, piping the JSON via stdin:

   ```bash
   echo '<JSON>' | .venv/bin/python .claude/skills/add-gdp-scatter/scripts/apply_scatter_defaults.py
   ```

   Output: two stdout tables.

   - **PER-ROW ACTIONS** — `chart`, `src`, `gdp_source`, `status`, `notes`. Statuses: `OK`, `SKIPPED` (e.g. stacked-family chart), `FAIL`, `ERR_PUT`, `ERROR`.
   - **Y-DIM DISPLAY NAMES** — `chart`, `varId`, manual `display.name` (on chart), ETL `display.name` (from `variables.display`), catalog `variable.name`. Only populated for `OK` rows.

4. **Show both tables to the user** verbatim (or formatted as markdown).

5. **Follow up on display names — and do it LAST.** Where a target ended up with a manual `display.name` but the ETL variable already defines a reasonable one (or `variable.name` is clean), use `AskUserQuestion` to let the user pick which manual overrides to drop. Then run a small inline Python block to delete the `name` key from `display` on each chosen chart (preserving `unit`/`shortUnit`/etc.), via the same `AdminAPI.update_chart` flow.

   **A later applier run silently undoes this.** Step 3 mirrors the source's manual y `display.name` onto the target whenever the two differ, so any name that came *from the source* is re-applied by the next run — the "idempotent re-run" verification in "Verifying after a run" will quietly revert the decision. Do this step after the final applier run, and if you must re-run afterwards, re-apply the drops. Check the run notes to tell the two cases apart: a `y.display.name: None → '…'` note means the applier added it from the source (it will come back), while a name that appears in the Y-DIM table with no such note was already on the target (it will not).

   Watch for the reverse case too: a target with **no** manual name falls back to the ETL `display.name`, which can be a bare dimension label like `"Mean"` — fine on a line chart next to its title, but an unlabeled-looking axis on the scatter. Offer to set one.

## Edge cases

- **Stacked-family chartTypes** (`StackedArea` / `StackedBar` / `StackedDiscreteBar`) without any line-family entry → `SKIPPED`. The user must redesign the chart manually. (Example from session: chart 3547.)
- **Target already has `x`/`color`/`size`** → leave it, like the admin's `if (!hasX)` does. The script will not overwrite an existing dimension.
- **Target is itself already a `ScatterPlot`** → the row is worthless and can be actively harmful, so **screen these out of the input before running**. A chart config holds exactly one `x` dimension, so a chart already scattering against some other variable can never also plot GDP: the guard above leaves `x` alone, the row still reports `OK`, and no GDP dimension is added. Worse, if the target's y differs from the source's, step 7 stamps the source's y `display.name` onto a different indicator. This is not a status the script reports — verify it yourself.

## Picking targets

A valid target is a **published, non-scatter, single-y-indicator chart that plots the source scatter's non-GDP indicator on its `y` axis**. When a target list is generated by query, all four conditions have to be filters, or the list quietly fills with rows that cannot work:

- **on `y`, not just present** — matching the indicator anywhere in the target's dimensions pulls in scatter twins where it is the `x` (e.g. `cereal-yield-vs-extreme-poverty-scatter` instead of `cereal-yield-vs-extreme-poverty`).
- **not a `ScatterPlot`** — see the edge case above.
- **not stacked-family** — the script `SKIPPED`s those.
- **exactly one `y` indicator** — a scatter plots one y series, so a multi-series line/bar chart is ambiguous (an 11- or 22-series cause-of-death chart is a nonsense target). In `prod_semantic`, `is_single_indicator` is exactly `COUNT(DISTINCT y indicator) = 1`, verified with zero disagreements.

Two things to expect from a correctly filtered list: **fewer rows**, and some sources with **no target at all** — that is the honest answer when the indicator is only ever plotted by scatters, and it is better than a fallback. Also beware `coalesce(type, 'LineChart')`: `type` is NULL for the charts that never set `chartTypes`, so a bare `type != 'ScatterPlot'` evaluates to NULL and silently drops exactly the plain line charts that make the best targets.

`scripts/find_targets.sql` is a worked query over the analytics semantic layer that applies all of the above and returns one row per published GDP scatter, with its target (or NULL). Run it in Metabase, or from here:

```bash
.venv/bin/python -c "
from etl.analytics.data import read_analytics
print(read_analytics(open('.claude/skills/add-gdp-scatter/scripts/find_targets.sql').read()).to_csv(index=False))"
```

It uses `/* */` comments deliberately: `read_analytics` flattens the SQL onto one line, where a `--` comment would swallow the rest of the query and fail with a misleading `Unexpected end of statement`. As of 2026-08-04 it returns 167 GDP scatters — 124 with a target, 43 without.
- **Source has `excludedEntityNames`** → warning only. Exclusions on the target would also hide those entities from line/bar/map views, which is rarely intended.
- **GDP coverage mismatch** → warning only; the user picks per chart whether to switch sources.
- **Sparse scatter view** → warning only; tolerance affects all views, not just scatter.

## What this skill explicitly does NOT do

- Does not add the canonical GDP footnote (`note: "GDP per capita is expressed in [international-$]…"`). Request separately if you want it.
- Does not apply source `excludedEntityNames`.
- Does not adjust `selectedEntityNames`, `originUrl`, `subtitle`, `title`, or `note`.
- Does not push to production — only to whatever environment `OWID_ENV` resolves to.

## Hard rule: never migrate GDP-per-capita text

The source charts are scatter-vs-GDP charts, so their title/subtitle/footnote describe the GDP relationship ("… vs. GDP per capita", "GDP per capita is adjusted for inflation and differences in living costs between countries", etc.). The target's primary view is **not** the scatter, so that framing does not belong on it. **Whenever porting any text from a source chart to a target (title, subtitle, footnote, display name), strip every GDP-per-capita clause first** — the "vs. GDP per capita" phrasing and the inflation/living-costs boilerplate tail. Port only the part describing the target's own indicator.

## Reviewing the migration side-by-side

`scripts/build_review.py` renders a self-contained HTML for stepping through each pair — the **old standalone scatter** on the left, the **scatter view the target gained** on the right — with approve / flag / note per row. Decisions persist in `localStorage`, mirror to a JSON on disk (Chrome/Edge), and import back. Same shape as `map-charts-to-mdim/scripts/build_review.py`; it takes the applier's own JSON on stdin, so the reviewed set is exactly the applied set:

```bash
echo '<JSON>' | STAGING=1 .venv/bin/python \
  .claude/skills/add-gdp-scatter/scripts/build_review.py --name scatter_batch1
```

What it adds over the mdim reviewer, because the asymmetry here is different: on the left the scatter **was** the whole chart, on the right it is one tab among several. So every row makes the target's secondary status explicit — a `SECONDARY · tab N of M · opens on <tab>` badge, and the full tab list as chips with **★ on the tab readers actually land on** and the scatter highlighted. Watch for a default of **Map** or **Table**: grapher adds those outside `chartTypes`, so readers may not land on a chart tab at all.

The right pane toggles (or press `v`) between the two states a URL can produce:

- **Redirect view** — `?tab=scatter&time=latest&country=`, exactly what a reader following the retired slug gets.
- **Default view** — what a reader opening the target sees first.

The third state — after a reader *clicks* the scatter tab — no URL can reproduce (see "`adjustStateForTab` fires on a tab CLICK only"). Open the Default view and click the scatter tab **inside the frame**: it should match the Redirect view. That comparison is the practical check that the redirect's `time=`/`country=` params really stand in for the click, which is the one thing about Part 2 that has never been verified live.

Per-row flags are split so the "With warnings" filter stays worth using. **Warnings** are possible defects — no `ScatterPlot` tab, scatter as the primary type, `hideTimeline` with a time range, `stackMode: relative`, source `excludedEntityNames` that the target will not carry. **Context** is expected-but-needed-to-read-the-panes, e.g. that the target selects N entities which both routes should clear — so if you *do* see highlighting, one of the two mechanisms failed. Keep new checks on the right side of that line; a warning on every row is the same as no warnings.

Decisions are fingerprinted on both configs' `fullMd5` plus the GDP variable id, so a re-run of the applier (which rewrites the target) invalidates stale approvals instead of silently keeping them.

**After fixing a flagged chart, the reviewer has to reload.** The panes are iframes and are only re-pointed when their URL changes, so a config edit made on staging is invisible behind the browser cache — a reviewer checking their own fix sees the old chart and reasonably concludes nothing happened. Press **`r`** / hit **↻ Reload frames** to force a refetch. Regenerating the HTML is also worth doing after a round of fixes, because it refreshes the `fullMd5` fingerprints: import the reviewer's exported JSON into the new file and exactly the rows whose config changed come back as *to review*, while every untouched decision carries over.

## Verifying after a run

- Open `OWID_ENV.chart_site(slug)` for one of the targets and switch to the Scatter tab.
- Re-run the same input. The script is idempotent — all changes are guarded by "if absent" / "if not equal" checks; a second run should print `OK` with empty / minimal notes.
- Confirm every target got the **current** GDP id on `x` (`GDP_SOURCES`), not a stale one carried over from an earlier run.

### An admin write drops an empty `colorScale`, which chart-diff then reports

`configs_are_equal` compares the whole config minus `id`, `isPublished`, `bakedGrapherURL`, `adminBaseUrl`, `dataApiUrl` and `version` — so **`colorScale` is compared**. The admin API normalizes an empty `colorScale: {}` to null and drops the key on *any* write, and pushing `{}` back does not stick (verified — it re-normalizes). A target whose production config has `colorScale: {}` therefore shows an extra "colorScale removed" line in its chart-diff forever after being touched.

Two consequences worth knowing before someone reports it as a bug:

- It is cosmetic. Nothing renders differently, and **`colorScale` with real content is preserved** — e.g. `customHiddenCategories` survived intact on a Marimekko target.
- **Reverting a chart on staging does not remove it from chart-diff.** The revert is itself an admin write, so a chart restored to its exact production config still appears, differing only by the dropped `colorScale`. If a reverted chart shows up with no visible change, this is why — check `colorScale` before hunting for a real difference.

## Part 2: retire the old standalone scatter charts

Once the targets have their scatter view, each old standalone "X vs. GDP per capita" chart is retired by registering **its slug as a chart redirect on the target chart** carrying `?tab=scatter&time=latest&country=`, then unpublishing it. Use `scripts/redirect_to_scatter.py`.

That is the same thing as opening the target chart's admin editor, going to **Refs → "Alternative URLs for this chart"**, and filling in both fields: the old slug under **URL**, and `tab=scatter&time=latest&country=` under **Target query params (optional)**. That second field is new (grapher #6674, Jul 2026) and is what makes this possible — before it, `chart_slug_redirects` could only map slug → chart id, so this skill had to use the site `redirects` table instead.

Input: JSON list of `{grapher_url, target_chart_url}` (public `ourworldindata.org/grapher/<slug>` URLs).

```bash
# Audit only (default) — a full dry run: references, article follow-ups, and the verdict for
# every row, mutating nothing:
echo '<JSON>' | .venv/bin/python .claude/skills/add-gdp-scatter/scripts/redirect_to_scatter.py
# Apply — create the redirects, re-point the sources' own old slugs, unpublish the sources:
echo '<JSON>' | .venv/bin/python .claude/skills/add-gdp-scatter/scripts/redirect_to_scatter.py --apply
```

Other flags: `--skip-alias-repoint` (leave the sources' own old slugs alone — they are still audited, and any source that still has one is `BLOCKED`, because the unpublish would delete it), `--allow-manual-refs` (apply a row whose source an explorer / data insight / static viz references — only once those are re-pointed), `--allow-production` (required to `--apply` when `OWID_ENV` resolves to production, which it does on `master`).

### Pre-checks

All read-only, so the audit reports the verdict `--apply` will act on:

| verdict | meaning |
|---|---|
| `CREATE` / `UPDATE` | ready. `UPDATE` = a redirect for this slug exists with the wrong query params |
| `EXISTS` | redirect already correct — the alias re-point and the unpublish still run |
| `SKIPPED` | source == target, or the target has no `ScatterPlot` tab / isn't published. **This is the wrong-staging-server detector**, and what protects charts we couldn't generate a scatter for (e.g. StackedArea) |
| `CHAINED` | the *target's* slug is itself redirected away (chart, site or mdim redirect), **or another row in the same batch retires it**. The in-batch case is the worse one: retiring the target unpublishes it, which deletes every redirect pointing at it — including the one that row just created — leaving that source unpublished with no redirect at all |
| `CONFLICT` | the source slug is already claimed — by a chart redirect to a different chart, or by a `multi_dim_redirects` row, which **wins** over chart redirects (the mdim map is merged second in `_grapherRedirects.json`) |
| `SITE_EXISTS` | a site redirect already serves this source. It bakes as a static 301 matched before the grapher route runs, so ours would be dead weight — delete it first if you want the chart redirect's param merging |
| `BLOCKED` | Two causes. (a) The source is referenced by an **explorer / data insight / static viz** — those embed its config, so no redirect covers them and the unpublish would break them; re-point them, then pass `--allow-manual-refs`. (b) `--skip-alias-repoint` on a source that still has old slugs of its own: the unpublish deletes every redirect pointing at the source, so sparing them means not unpublishing. Move them by hand, or drop the flag |

### References audit of the OLD chart

`get_chart_references` counts (`wp/gdoc/expl/narr/ins/sviz`), flagging `MANUAL` when explorers / dataInsights / staticViz is non-zero — **a redirect alone does not fix those** (they embed the old chart's config directly). Those rows are turned into `BLOCKED` **before** the apply loop runs, so `--apply` cannot unpublish them: the loop gates purely on `status`, and leaving a MANUAL row at `CREATE` meant the audit flagged the breakage and then caused it anyway. Re-point the dependents, then re-run with `--allow-manual-refs`.

Plus a table of **article references that need a hand edit**, from `posts_gdocs_links`:

- an **embed** (any `componentType` that isn't `span-*`) resolves to the target chart but renders the target's **default tab** — `makeGrapherLinkedChart` builds its URL without a query string, so `tab=scatter` never reaches it;
- a **link** carrying its own `tab=` or `time=` keeps those values, because the visitor's params override the stored ones.

### Re-point every reference at the new scatter view

**Recommend this every time, and do it before applying.** The redirect is a safety net for readers who arrive by an old URL — it is not the fix for our own content. Every OWID surface that points at the retired chart should be edited to point at the target chart's scatter view instead:

```
/grapher/<target-slug>?tab=scatter&time=latest&country=
```

merged with whatever query string the reference already carries (its own params win, same rule as the redirect — so a reference with `tab=` or `time=` of its own needs a decision, not a blind merge). Two reasons it can't wait: an **embed** never gets fixed by a redirect at all (it resolves the chart itself and renders the target's default tab), and a **link** works but sends readers through an extra hop that will outlive everyone's memory of why it exists.

The script's own table covers only gdoc links and embeds — enough to spot the param collisions, not a full sweep. For the complete surface list use the shared **`find-chart-references`** skill, which is what `/map-charts-to-mdim` does for the same problem (see `scripts/audit_references.py` there: it calls `run_sweep` from `find-chart-references/scripts/reference_report.py` and adds only the replacement URL, which is the workflow-specific part):

```bash
.venv/bin/python .claude/skills/find-chart-references/scripts/find_references.py \
  --chart-slugs '<old-slug-1>,<old-slug-2>' --markdown ai/scatter-references.md
```

Include the sources' **aliases** in `--chart-slugs`: an article may well link an even older slug. The sweep catches what `get_chart_references` counts but doesn't locate — explorers, data insights, static viz, narrative charts, key-chart slots, WordPress posts — and it reports its own **gaps**, so a surface it couldn't check is visible rather than silently absent. Triage it the way that skill does: an **embed** is 🔴 and blocks the row (it breaks the moment the source is unpublished), a **link** is 🟡 (the 301 covers it; update the href anyway), and an unpublished or draft page is ℹ️.

### The handoff must keep find-chart-references' presentation

Pass the sweep's `--json` through `scripts/build_reference_handoff.py`, which adds the one workflow-specific column — the replacement URL — while **keeping every locating aid the sweep's own markdown provides**:

```bash
.venv/bin/python .claude/skills/find-chart-references/scripts/find_references.py \
  --chart-slugs '<slugs+aliases>' --json ai/<name>_references.json
.venv/bin/python .claude/skills/add-gdp-scatter/scripts/build_reference_handoff.py \
  --references ai/<name>_references.json --pairs ai/<name>_part2_pairs.json
```

Those aids are the difference between a row someone can fix and a row that names an article and leaves them to hunt through it:

- **📄 doc** — the Google Doc to edit. `posts_gdocs.id` *is* the Doc id, so it is a direct link, and editing the doc is the only way to fix an embed.
- **👁 preview** — the article in the admin previewer, which renders unpublished drafts the public page won't show.
- **🔗 page** — the published page, deep-linked with a scroll-to-text fragment when the reference has anchor text, so it opens *at* the reference.
- **Find in the doc** — a copy-paste search string: the **link text** for a prose hyperlink, or the **chart slug** for a block embed (the doc holds a bare grapher URL there, and `posts_gdocs_links.target` keeps the slug as the author typed it, so it still matches when the doc uses an older one). `—` means there is nothing to search for.

**Import those formatters from `find-chart-references/scripts/find_references.py`; never reimplement them** — `doc_url`, `gdoc_preview_url`, `deep_link`, `search_hint`, `cell`. A second copy drifts, and the drift shows up as a handoff whose links quietly stop resolving. Strip the tailscale suffix from the admin root you pass them, so the links read like the sweep's own (which are already short).

### Key-chart slots — the Part 2 audit cannot see them

`get_chart_references` counts `postsWordpress`, `postsGdocs`, `explorers`, `narrativeCharts`, `dataInsights` and `staticViz`. **Key charts are none of those** — a key chart is a chart↔tag association (`chart_tags.keyChartLevel`), not a row in any reference table — so `redirect_to_scatter.py` reports nothing for them and its verdicts look clean while topic pages quietly depend on the chart you are about to unpublish.

Unpublishing the source does not break a link here; it removes the chart from the topic page's key-chart list. So the loss is silent, on pages nobody is looking at during the migration. **Move each association to the target chart** (same tag, same `keyChartLevel`) as part of step 11.

This is the concrete reason step 11 says not to trust a quiet Part 2 audit: on 2026-08-04 the audit's own tables held 6 embeds + 3 links, while the full sweep found **15 key-chart slots across 13 topic pages** — the largest single category, and entirely invisible to Part 2.

### Narrative charts

**They do not block the retirement.** A narrative chart parented to a chart owns a materialized full config and renders from it, so unpublishing the parent leaves it intact (`isPublished` is in `NARRATIVE_CHART_PROPS_TO_OMIT`). Its only use of the parent slug is the "Explore the data" href, which `GrapherState.canonicalUrlIfIsNarrativeChart` builds as `/grapher/<parent-slug>` + `queryParamsForParentChart` — so the redirect covers it. `narrativeCharts` is therefore counted but deliberately **not** part of the `MANUAL` gate.

The one thing to check is those params: they arrive as *incoming* params on the redirect, so a narrative chart with its own `tab` or `time` overrides `tab=scatter&time=latest&country=`. The script lists every narrative chart on the sources with its params and says which way each will land.

**To actually fix one, replace it — the parent columns are INSERT-only, so there is no re-pointing API and never will be** (owid/owid-grapher#6872, closed as not-planned). Order matters, because `create` rejects a duplicate name, `delete` is refused while a **published** post references the name, and `update` writes only query params — there is no rename:

1. **Create** the replacement from `/grapher/<target-slug>?tab=scatter&time=latest&country=` using that chart's own **"Create narrative chart"** control, under a new kebab-case name. Use the control, not a bare create link — it parents to the view on screen. Entity selection and other controls open at the target's defaults and authored FAUST never transfers, so redo those by hand.
2. **Update the article(s)** to reference the new name.
3. **Delete** the old one — now unreferenced, so it succeeds. **Never delete first.**

If you must script it, `POST {admin_api}/narrative-charts` takes `{"type": "chart", "name": "<kebab-case>", "parentChartId": <target chart id>, "config": <the OLD narrative chart's rendered full config>}` and `DELETE {admin_api}/narrative-charts/<id>` removes the old. Get `config` from `AdminAPI.get_narrative_chart(<old id>)["configFull"]` — the endpoint derives the patch by diffing against the new parent, so pass the rendered full config, not the old patch. `AdminAPI` has no create/delete for narrative charts, so scripting means hand-rolled HTTP; prefer the UI.

### Apply, in this order

The order is forced by which calls trigger a bake:

1. **Create** (or delete-then-recreate) the redirect on the target. There is no update endpoint, so wrong query params mean delete + create; if the create fails the original row is put back, and if that restore also fails the row reports `CRITICAL` with the repair.
2. **Re-point the source's own old slugs** at the target. Unpublishing a chart deletes every `chart_slug_redirects` row pointing at it, so without this step those URLs become hard 404s. Each alias is deleted and re-created on the target — the UNIQUE constraint on `slug` leaves no other way. An alias's own query params are *not* carried over (they were written for the old chart) but are reported.
3. **Unpublish the source.** This is both what makes the redirect fire (it only resolves on a 404) and what triggers the static build.

Both failure directions are handled so no URL is ever left unserved. If any alias fails to move, it is restored on the source and **the unpublish is skipped** — otherwise the unpublish would delete the restored row and create exactly the 404 step 2 exists to prevent. If the unpublish itself fails, the source is likewise left published. Either way the row reports `CRITICAL` with what to do.

**Every bail-out that leaves the source published also rolls the redirect back**, including the skipped-unpublish one: a row touched in the last week bakes as an unconditional static 302 that does *not* wait for a 404 (see the mechanism notes), so leaving it behind would send readers away from the chart the bail-out just decided to keep serving. For an `UPDATE` the rollback re-creates the row that was replaced, rather than only deleting the replacement — deleting alone would end the run having destroyed a redirect it meant to re-point. Anything the rollback cannot undo is named in the report with the manual repair.

### Mechanism / environment notes

- `?tab=scatter` is the valid scatter tab query param (`GRAPHER_TAB_CONFIG_OPTIONS.scatter`); it is stored without the leading `?`.
- **Resolution is 404-only** at the edge, then a **301** with `max-age=86400`. A fresh row additionally gets a static **302** in `_redirects` for one week, listed ahead of the site redirects, to defeat the CDN cache.
- **The stored params are only a base**: the visitor's own query params override them key by key. Good for `?country=`/`?region=` links, which keep their selection through the hop.
- `POST /charts/<id>/redirects/new` triggers **no** static build (the delete and the unpublish do), and validates nothing — no duplicate, chain or self-redirect check. Hence the pre-checks above.
- **A source that is already unpublished bakes nothing**, so the redirect would serve nothing until an unrelated mutation happened to bake the site. When a row hits that combination — `CREATE` *or* `EXISTS`, with no aliases to re-point — the script asks for a deploy itself (`PUT /deploy`, the admin's "Manually triggered deploy"), once per run however many rows needed it, and reports `DEPLOY FAILED` with the manual repair if the call fails. `EXISTS` is included deliberately: a row can be there and still have never been baked, because a previous run's deploy failed or because someone added the alternative URL in the chart editor, which bakes nothing either. Every other path already has a delete or an unpublish doing it.
- `chart_slug_redirects` is **per-environment** and is **not** synced staging→production by chart-diff. Run on staging to test, then re-run `--apply --allow-production` against production `admin.owid.io` once the scatter views are live there.
- `OWID_ENV` (hence the admin host) is derived from the current git branch — be on the branch whose staging holds the scatter views. On `master` it resolves to **production**, which is what the guard is for.
- Once the redirect exists, `isSlugUsedInRedirect` blocks re-publishing the source (or any chart) on that slug. **To undo, delete the redirect rows first, then re-publish** — the reverse order is rejected.
- The reference queries union a chart's own slug with its `chart_slug_redirects` slugs, so afterwards the old chart's referrers show up under the **target's** Refs tab.

### Verifying Part 2

- `curl -sI <site>/grapher/<old-slug>` → 301 to `/grapher/<target-slug>?tab=scatter&time=latest&country=`.
- `curl -sI '<site>/grapher/<old-slug>?tab=chart'` → `Location` keeps `tab=chart`, proving incoming params win.
- `curl -s <site>/grapher/_grapherRedirects.json | jq '."<old-slug>"'` → `"<target-slug>?tab=scatter&time=latest&country="` (bare slugs on both sides — the baker passes an empty URL prefix).
- Each re-pointed alias resolves to the same target.
- Re-run the script: every row comes back `EXISTS` and nothing is mutated.
- **Open the redirected URL in a browser** and check the two things no `curl` can: the scatter opens on a **single latest year** (not a range with connecting trails), and **no entities are highlighted** — i.e. `time=latest` and the empty `country=` both survived the param merge and did the job the tab click would have done. Compare against clicking the scatter tab on the target directly; the two should look the same. If `country=` was dropped somewhere in the merge, the target's line/bar selection will show up emphasized — that is the symptom to look for.
