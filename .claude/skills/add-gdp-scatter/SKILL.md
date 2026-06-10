---
name: add-gdp-scatter
description: Add a scatter view (with GDP per capita on x) to existing OWID charts via the admin API, mirroring the admin UI's "Add scatter type" defaults. Trigger when the user pastes a table with columns `chart_admin_url`, `target_chart_admin_url`, `gdp_source`.
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
5. **Y-axis log toggle (not forced):** when the source scatter is `scaleType: log`, only enable the toggle (`canChangeScaleType: true`) and leave the default **linear**. `yAxis` is shared across all views, so forcing log would flip the line/bar views too. **Mirrors explicit `yAxis` min/max bounds** the source sets (each bound copied independently). A degenerate `min: 0` + `max: 0` (collapsed axis) is treated as junk — never replicated, and stripped if already present. Note: y-axis bounds also affect all views, not just scatter (see the cross-view caveat below).
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

## Part 2: redirect old standalone scatter charts to the new scatter tab

Once the targets have their scatter view, the old standalone "X vs. GDP per capita" charts can be retired and redirected to `/grapher/<target-slug>?tab=scatter&time=latest` (the scatter tab on the latest year). Use `scripts/redirect_to_scatter.py`.

Input: JSON list of `{grapher_url, target_chart_url}` (public `ourworldindata.org/grapher/<slug>` URLs).

```bash
# Audit only (default) — reports what references each OLD chart, never mutates:
echo '<JSON>' | .venv/bin/python .claude/skills/add-gdp-scatter/scripts/redirect_to_scatter.py
# Apply — create site redirect + unpublish each source:
echo '<JSON>' | .venv/bin/python .claude/skills/add-gdp-scatter/scripts/redirect_to_scatter.py --apply
```

What it does per pair:
- Resolves both slugs to chart ids (`chart_configs.slug`).
- **References audit** of the OLD chart via `get_chart_references`. Counts `wp/gdoc/expl/narr/ins/sviz`; flags `MANUAL` when any of explorers / narrativeCharts / dataInsights / staticViz is non-zero — **a redirect alone does not fix those** (they embed the old chart's config directly). WordPress/Gdoc links are handled by the 302. **Review the audit and skip any MANUAL row** (e.g. pull it out of the input) unless its dependents have been re-pointed first.
- **Pre-flight guards** (under `--apply`): the target must have `ScatterPlot` in `chartTypes` and be `isPublished` — otherwise the row is `SKIPPED`. This is what protects charts we couldn't generate a scatter for (e.g. StackedArea), and what catches running against the wrong staging server (see branch note below).
- Creates the site redirect via `AdminAPI.create_site_redirect` (the site `redirects` table — supports the `?tab=scatter` query string; `chart_slug_redirects` does not). Duplicate/chained sources are reported, not fatal.
- Unpublishes the source chart (`isPublished: false`).

**Mechanism / environment notes:**
- `?tab=scatter` is the valid scatter tab query param (`GRAPHER_TAB_CONFIG_OPTIONS.scatter`).
- The site `redirects` table is **per-environment** and is **not** synced staging→production by chart-diff. So: run on staging to test, then re-run `--apply` against production `admin.owid.io` once the scatter views are live on prod.
- `OWID_ENV` (hence the admin host) is derived from the current git branch — be on the branch whose staging holds the scatter views before running.
- The live 302 only appears after the static `_redirects` file rebakes; the unpublish triggers that build, but allow a few minutes on staging.
