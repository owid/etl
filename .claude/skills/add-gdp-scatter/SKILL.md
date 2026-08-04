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
  - `World Bank` / `WDI` → variableId `1204826`
  - `Maddison` / `Maddison Project Database` → `900793`
  - `PWT` / `Penn World Table` → `1108541`

The admin host that gets written to is `OWID_ENV.admin_api`, which auto-resolves to `staging-site-<branch>` on a feature branch. Confirm the branch before running.

## Pre-flight: GDP version check

Before processing any rows, the script queries `variables` for the latest id matching each canonical GDP-per-capita `catalogPath` pattern (`worldbank_wdi/.../ny_gdp_pcap_pp_kd`, `ggdc/.../maddison_project_database#gdp_per_capita`, `ggdc/.../penn_world_table#rgdpo_pc`). If the latest id differs from the one hardcoded in `GDP_SOURCES`, a `WARN` is printed at the top of stdout with the newer id and catalogPath. The script does NOT auto-switch — update `GDP_SOURCES` (and the `feedback_scatter_gdp_picker` memory) deliberately if the new version is the one we want.

## What the script does (per row)

Mirrors the admin's `applyDefaultsForScatter` and the extra moves we agreed on:

1. Adds `ScatterPlot` to `chartTypes`, preserving existing tabs. Seeds the schema default `[LineChart, DiscreteBar]` when `chartTypes` is unset.
2. Appends x (the chosen GDP variable), color, size dimensions if absent.
   - **color**: if the source uses a non-default color variable (e.g. World Bank income groups), mirror it; otherwise use `CONTINENTS_ID=900801`.
   - **size**: the rule is *always use the default `Population` indicator (`POPULATION_ID=953899`) for any population-type size*. If the source sizes by any population variant (regular, historical, WPP, …), the target gets the default Population. A genuinely **non-population** size (e.g. GDP, area) is mirrored as-is **but raises a `WARN`** so the bubble sizing gets a manual review. **If the source has no `size` dim at all, the target also gets none** — the script won't add sizing the curator deliberately omitted. Population variants are detected by the variable's name starting with "Population" or its catalogPath living under a `/population/` dataset; the action note records any normalization.
3. Sets `matchingEntitiesOnly: true`.
4. Sets `xAxis` to `scaleType: log` + `canChangeScaleType: true`.
5. **Y-axis log toggle (not forced):** when the source scatter is `scaleType: log`, only enable the toggle (`canChangeScaleType: true`) and leave the default **linear**. `yAxis` is shared across all views, so forcing log would flip the line/bar views too. **Mirrors explicit `yAxis` min/max bounds** the source sets (each bound copied independently) — **except a non-zero `min` is NOT mirrored when the target has a bar/area view** (`DiscreteBar`/`Marimekko`/`Stacked*`), because those need a zero baseline and a scatter-tuned non-zero min would make bars start above zero (misleading). A degenerate `min: 0` + `max: 0` (collapsed axis) is treated as junk — never replicated, and stripped if already present. Note: y-axis bounds affect all views, not just scatter.

### Cross-view safety (which fields are global)

`yAxis` (scaleType, min, max) is the only config the skill writes that meaningfully bleeds into the non-scatter views — hence the log-toggle and zero-baseline handling above. The others were checked and are safe: `xAxis.scaleType: log` is ignored by Line/DiscreteBar (they hardcode a linear time axis) and has no visible effect on Slope; the `color` dimension does **not** recolor line/bar (they color by entity); `size` is scatter-only (not even in the table tab); `matchingEntitiesOnly` is honored only by Scatter and Marimekko.
6. **Mirrors source's manually-set y `display.name`** when present.
7. Emits warnings (no action) for:
   - Target has no `selectedEntityNames` — line/bar/slope views will fall back to Grapher defaults.
   - Target `stackMode: relative` — on scatter this is the "Display average annual change" mode; we want the toggle available but **off by default**, so a relative default is flagged for review.
   - Source `excludedEntityNames` — would apply across all views, not just scatter.
   - GDP coverage mismatch — if y-indicator's earliest year predates the chosen GDP's coverage (WDI≈1990, PWT≈1950, Maddison≈year 1), suggest a deeper-history alternative.
   - Few entities on default scatter view — counts entities with both a y- and an x-value within tolerance at the default time; if fewer than ~15 AND source uses higher tolerance, recommends bumping target's y `display.tolerance`.

Push uses `apps.chart_sync.admin_api.AdminAPI.update_chart(id, cfg)`.

## Workflow

1. **Parse the pasted table** into a JSON list, one object per row with keys `chart_admin_url`, `target_chart_admin_url`, `gdp_source`. Strip the header. Accept tab- or comma- separated.

2. **Run the script**, piping the JSON via stdin:

   ```bash
   echo '<JSON>' | .venv/bin/python .claude/skills/add-gdp-scatter/scripts/apply_scatter_defaults.py
   ```

   Output: two stdout tables.

   - **PER-ROW ACTIONS** — `chart`, `src`, `gdp_source`, `status`, `notes`. Statuses: `OK`, `SKIPPED` (e.g. stacked-family chart), `FAIL`, `ERR_PUT`, `ERROR`.
   - **Y-DIM DISPLAY NAMES** — `chart`, `varId`, manual `display.name` (on chart), ETL `display.name` (from `variables.display`), catalog `variable.name`. Only populated for `OK` rows.

3. **Show both tables to the user** verbatim (or formatted as markdown).

4. **Follow up on display names.** Where a target ended up with a manual `display.name` but the ETL variable already defines a reasonable one (or `variable.name` is clean), use `AskUserQuestion` to let the user pick which manual overrides to drop. Then run a small inline Python block to delete the `name` key from `display` on each chosen chart (preserving `unit`/`shortUnit`/etc.), via the same `AdminAPI.update_chart` flow.

## Edge cases

- **Stacked-family chartTypes** (`StackedArea` / `StackedBar` / `StackedDiscreteBar`) without any line-family entry → `SKIPPED`. The user must redesign the chart manually. (Example from session: chart 3547.)
- **Target already has `x`/`color`/`size`** → leave it, like the admin's `if (!hasX)` does. The script will not overwrite an existing dimension.
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

## Verifying after a run

- Open `OWID_ENV.chart_site(slug)` for one of the targets and switch to the Scatter tab.
- Re-run the same input. The script is idempotent — all changes are guarded by "if absent" / "if not equal" checks; a second run should print `OK` with empty / minimal notes.

## Part 2: retire the old standalone scatter charts

Once the targets have their scatter view, each old standalone "X vs. GDP per capita" chart is retired by registering **its slug as a chart redirect on the target chart** carrying `?tab=scatter&time=latest`, then unpublishing it. Use `scripts/redirect_to_scatter.py`.

That is the same thing as opening the target chart's admin editor, going to **Refs → "Alternative URLs for this chart"**, and filling in both fields: the old slug under **URL**, and `tab=scatter&time=latest` under **Target query params (optional)**. That second field is new (grapher #6674, Jul 2026) and is what makes this possible — before it, `chart_slug_redirects` could only map slug → chart id, so this skill had to use the site `redirects` table instead.

Input: JSON list of `{grapher_url, target_chart_url}` (public `ourworldindata.org/grapher/<slug>` URLs).

```bash
# Audit only (default) — a full dry run: references, article follow-ups, and the verdict for
# every row, mutating nothing:
echo '<JSON>' | .venv/bin/python .claude/skills/add-gdp-scatter/scripts/redirect_to_scatter.py
# Apply — create the redirects, re-point the sources' own old slugs, unpublish the sources:
echo '<JSON>' | .venv/bin/python .claude/skills/add-gdp-scatter/scripts/redirect_to_scatter.py --apply
```

Other flags: `--skip-alias-repoint` (leave the sources' own old slugs alone — they are still audited, and any source that still has one is `BLOCKED`, because the unpublish would delete it), `--allow-production` (required to `--apply` when `OWID_ENV` resolves to production, which it does on `master`).

### Pre-checks

All read-only, so the audit reports the verdict `--apply` will act on:

| verdict | meaning |
|---|---|
| `CREATE` / `UPDATE` | ready. `UPDATE` = a redirect for this slug exists with the wrong query params |
| `EXISTS` | redirect already correct — the alias re-point and the unpublish still run |
| `SKIPPED` | source == target, or the target has no `ScatterPlot` tab / isn't published. **This is the wrong-staging-server detector**, and what protects charts we couldn't generate a scatter for (e.g. StackedArea) |
| `CHAINED` | the *target's* slug is itself redirected away (chart, site or mdim redirect) |
| `CONFLICT` | the source slug is already claimed — by a chart redirect to a different chart, or by a `multi_dim_redirects` row, which **wins** over chart redirects (the mdim map is merged second in `_grapherRedirects.json`) |
| `SITE_EXISTS` | a site redirect already serves this source. It bakes as a static 301 matched before the grapher route runs, so ours would be dead weight — delete it first if you want the chart redirect's param merging |
| `BLOCKED` | `--skip-alias-repoint` on a source that still has old slugs of its own. The two cannot both hold: the unpublish deletes every redirect pointing at the source, so sparing them means not unpublishing. Move them by hand, or drop the flag |

### References audit of the OLD chart

`get_chart_references` counts (`wp/gdoc/expl/narr/ins/sviz`), flagging `MANUAL` when explorers / dataInsights / staticViz is non-zero — **a redirect alone does not fix those** (they embed the old chart's config directly). **Pull `MANUAL` rows out of the input** unless their dependents have been re-pointed first.

Plus a table of **article references that need a hand edit**, from `posts_gdocs_links`:

- an **embed** (any `componentType` that isn't `span-*`) resolves to the target chart but renders the target's **default tab** — `makeGrapherLinkedChart` builds its URL without a query string, so `tab=scatter` never reaches it;
- a **link** carrying its own `tab=` or `time=` keeps those values, because the visitor's params override the stored ones.

### Narrative charts

**They do not block the retirement.** A narrative chart parented to a chart owns a materialized full config and renders from it, so unpublishing the parent leaves it intact (`isPublished` is in `NARRATIVE_CHART_PROPS_TO_OMIT`). Its only use of the parent slug is the "Explore the data" href, which `GrapherState.canonicalUrlIfIsNarrativeChart` builds as `/grapher/<parent-slug>` + `queryParamsForParentChart` — so the redirect covers it. `narrativeCharts` is therefore counted but deliberately **not** part of the `MANUAL` gate.

The one thing to check is those params: they arrive as *incoming* params on the redirect, so a narrative chart with its own `tab` or `time` overrides `tab=scatter&time=latest`. The script lists every narrative chart on the sources with its params and says which way each will land.

**To actually fix one, replace it — the parent columns are INSERT-only, so there is no re-pointing API and never will be** (owid/owid-grapher#6872, closed as not-planned). Order matters, because `create` rejects a duplicate name, `delete` is refused while a **published** post references the name, and `update` writes only query params — there is no rename:

1. **Create** the replacement from `/grapher/<target-slug>?tab=scatter&time=latest` using that chart's own **"Create narrative chart"** control, under a new kebab-case name. Use the control, not a bare create link — it parents to the view on screen. Entity selection and other controls open at the target's defaults and authored FAUST never transfers, so redo those by hand.
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
- `chart_slug_redirects` is **per-environment** and is **not** synced staging→production by chart-diff. Run on staging to test, then re-run `--apply --allow-production` against production `admin.owid.io` once the scatter views are live there.
- `OWID_ENV` (hence the admin host) is derived from the current git branch — be on the branch whose staging holds the scatter views. On `master` it resolves to **production**, which is what the guard is for.
- Once the redirect exists, `isSlugUsedInRedirect` blocks re-publishing the source (or any chart) on that slug. **To undo, delete the redirect rows first, then re-publish** — the reverse order is rejected.
- The reference queries union a chart's own slug with its `chart_slug_redirects` slugs, so afterwards the old chart's referrers show up under the **target's** Refs tab.

### Verifying Part 2

- `curl -sI <site>/grapher/<old-slug>` → 301 to `/grapher/<target-slug>?tab=scatter&time=latest`.
- `curl -sI '<site>/grapher/<old-slug>?tab=chart'` → `Location` keeps `tab=chart`, proving incoming params win.
- `curl -s <site>/grapher/_grapherRedirects.json | jq '."<old-slug>"'` → `"<target-slug>?tab=scatter&time=latest"` (bare slugs on both sides — the baker passes an empty URL prefix).
- Each re-pointed alias resolves to the same target.
- Re-run the script: every row comes back `EXISTS` and nothing is mutated.
