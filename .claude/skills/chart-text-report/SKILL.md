---
name: chart-text-report
description: Generate a compact Markdown report of user-facing chart text (Title, Subtitle, Footnote, description_short, description_key) for an MDim, a grapher/garden dataset, or a hand-picked list of indicators. Each field is tagged by source (override / inherited / missing) so the reader can tell what Grapher renders vs. what comes from the ETL. Trigger when the user wants to review, audit, or spot-check the user-facing text of one or many charts/indicators at once — e.g. "dump the FAUST for dataset X", "I want to review the text of all views in this MDim", "show me the chart text for these indicators".
metadata:
  internal: true
---

# Chart text report

Produce a Markdown audit of the user-facing chart text for a set of indicators or MDim views. The goal is editorial review: a reader should be able to scan the file and see exactly what Grapher renders, without opening every chart.

This skill generalizes the pattern first built for four inequality MDims (`incomes_pip`, `gini_pip`, `gini_lis`, `incomes_wid`). Ready-to-run scripts live alongside this file under `scripts/`:

- `scripts/_common.py` — shared helpers: grapher-channel metadata loader, inheritance resolvers, `BulletLibrary`, auto-slugs, preview URL, stopwords.
- `scripts/generate_mdim_text_report.py` — MDim view mode (supports `collapse_dims` and placeholder parametrization).
- `scripts/grapher_dataset_mode.py` — grapher-dataset mode (iterates every indicator column) and indicator-list mode (`--indicators <cp> <cp> ...` or `--indicators-file <path>`).

Rebuilding the MDim `.config.json` is done via `etlr <mdim> --export --private` — there is no DB-bypass helper. The user works on a staging server where MySQL is up, so `Collection.save()` runs cleanly (validates indicators + upserts the MDim config to the admin API). Change detection handles the common case:

- **Nothing changed** → ~2 s; nothing runs.
- **Garden `.meta.yml`, garden data, or MDim yaml/py changed** → etlr rebuilds only the affected upstream steps and the MDim export.

Do **not** add `--grapher` unless you specifically need to re-upload indicator data/metadata to MySQL — it triggers a `grapher://grapher/<dataset>` upload step that can take ~50 s per dataset and isn't needed for the FAUST report (the script reads metadata directly from the local grapher-channel feather files).

Do **not** add `--only` when you want garden/MDim edits to take effect — `--only` skips upstream dependency rebuilds by design. Use `--only --force` only when you explicitly want to re-run just the MDim step without touching anything upstream.

The original working copy that produced the reference output also lives at `ai/generate_mdim_text_report.py` and `ai/build_gini_pip_config.py`. Prefer the `scripts/` versions for new work — they import shared helpers from `_common.py` to avoid drift.

## When to use

- The user asks for a plain-text dump of the user-facing text of a chart / MDim / dataset, typically for a copy-editing pass.
- The user wants to confirm which text is overridden in an MDim vs. inherited from the indicator's `presentation.grapher_config`.
- The user wants a single Markdown file per chart group, not chart-by-chart exploration.

Do **not** use this skill if they just want a single `title`/`subtitle` for one chart — that's simpler to read inline.

## Fields reported

Only user-facing text is reported. Six fields total, sorted into two groups:

| Group | Fields | Where they come from |
|---|---|---|
| Chart-level FAUST (a subset of `Footnote, Axis titles, Units, Subtitle, Title`) | `Title`, `Subtitle`, `Footnote` | `presentation.grapher_config.{title, subtitle, note}` |
| Indicator-level metadata | `description_short`, `description_key` | top-level `VariableMeta.description_short`, `VariableMeta.description_key` |

Never report Axis titles or Units in the default output (keep the report skimmable). Never include `description_processing`.

## Critical inheritance rules

**Chart title / subtitle / footnote** resolve ONLY from `presentation.grapher_config.{title, subtitle, note}`. Do NOT fall back to `variable.title`, `presentation.title_public`, `display.name`, or `description_short` — those are data-page fields and produce text that does not match what Grapher actually renders.

**description_short / description_key** resolve from the namesake top-level fields on `VariableMeta` — not from `grapher_config`.

Not every chart has `presentation.grapher_config` populated: some charts are edited only in the admin DB, so the ETL metadata looks empty. Flag those fields as `[missing]` rather than inventing a fallback. See `.claude/projects/-Users-parriagadap-etl/memory/feedback_chart_faust_inheritance.md` for the full rule.

## Inputs the skill supports

| Input kind | Example | Source of per-entity text |
|---|---|---|
| MDim export | `wb/latest/incomes_pip#incomes_pip` | `export/multidim/<ns>/<ver>/<name>/<name>.config.json`, plus grapher-channel inheritance for each view's primary `y` indicator |
| Grapher/garden dataset | `data/grapher/wb/2026-03-24/world_bank_pip` | iterate columns across all tables; treat each column as an entity; all text is `[inherited]` |
| Hand-picked indicators | `grapher/wb/2026-03-24/world_bank_pip/incomes#share__...` | same as above but filtered to the listed columns |

**Always load indicator metadata from the GRAPHER channel**, not garden. The grapher channel flattens dimensional indicators into one column per combination and renders the Jinja metadata templates with those specific dimension values — that's what Grapher actually shows.

## Required output format

```
# <mdim_name or dataset_name> — <top title>

**Preview:** [<catalog_path>](<admin_url>)

Total views: **N**   (for MDims)

## How to read this file
- [override], [inherited], [missing] explanation

## Description-key bullet legend
- **<slug>** — <full bullet text>   (one row per unique bullet)

## <view or indicator heading — uses chart Title when resolvable>

**<Dim name>:** <Choice name> · **<Dim name>:** ...   (human-readable dims)

**Preview:** [...](...)                                (view-level link)

- **Title** [source] ...
- **Subtitle** [source] ...
- **Footnote** [source] ...
- **description_short** [source] ...
- **description_key** [source]
  - slug-1
  - slug-2
```

## Key implementation features (all required)

1. **Grapher-channel metadata loading**: `Dataset(data/grapher/<ns>/<ver>/<ds>).read(<table>, safe_types=False)[<col>].metadata`.

2. **Rebuilding the MDim `.config.json`**: use `etlr export://multidim/<ns>/<ver>/<name> --export --private`. This runs `Collection.save()` (`validate_indicators_in_db` + `save_config_local` + `upsert_to_db` — admin-API upsert, not a big data push) and lets etlr's change detection rebuild any upstream garden/MDim steps whose code or YAML changed. Don't add `--grapher` unless you need to push indicator data to MySQL (it pulls in the slow `grapher://grapher/<dataset>` upload step). If the command errors with a MySQL connection-refused trace, surface that to the user and stop — don't monkey-patch around it.

3. **Description-key dedup with auto slugs**: collect unique bullets into a per-file legend, auto-generate a short slug from the first ~3 non-stopword content words of each bullet (kebab-case), disambiguate collisions with `-2`/`-3` suffixes. Each view references bullets by their slugs, rendered as sub-bullets (not a comma-separated list).

4. **Dimension collapse (MDim only)**: accept a `collapse_dims: list[str]` per MDim. Group views whose non-collapsed dims match, render one section per group, show variant previews on separate links labelled by the collapsed dim's value.

5. **Placeholder parametrization**: when the Title / Subtitle / description_short / description_key vary across collapsed variants only by a simple substitution, collapse the text to a single `{dim}` placeholder. Try the raw value first (`day` in `per day`), then snake → space (`before_tax` → `before tax`), then snake → hyphen (`before-tax`); case-insensitive regex. If all variants collapse to the same placeholder-bearing string, use it; else fall back to sub-bullets.

6. **Global placeholder legend**: when one or more dims are parametrized, include a header line listing `` `{dim}` ∈ {val1, val2, ...} `` once at the top of the file instead of per-line.

7. **Human-readable dim selections subheader**: directly under each view heading, render the dim selections using the dimension `name` and choice `name` from the MDim config (`**Indicator:** Mean income · **Period:** Per day, Per month, Per year`). Filter out `nan` sentinel values.

7a. **Heading disambiguation when views share a title**: when two or more groups collapse to the same `## <Title>` heading (common when a dim doesn't appear in the rendered Title but does vary between groups — e.g. `survey_comparability` on `incomes_pip`), append `(Dim name: Choice name)` built from the non-collapsed dim(s) whose values differ across the colliding groups. Dim order follows the MDim config. Only the differentiating dim(s) are appended — shared dims are already visible in the selection subheader directly below the heading.

8. **Preview URLs**: main MDim URL is `https://admin.owid.io/admin/grapher/<urlquote(catalog_path)>`. Per-view URL appends `?dim1=slug1&dim2=slug2` from the view's `dimensions` dict.

9. **Override / inherited / missing tagging**: `[override]` = text explicitly set on the view (MDim `config.*` or `metadata.*`); `[inherited]` = resolved from the primary y-indicator's ETL metadata; `[missing]` = absent in both. For grapher-dataset and indicator-list inputs, every tag is `[inherited]` or `[missing]` (no view-level overrides exist).

10. **`ai/` directory output** (per project convention). One Markdown file per entity the user asked about.

## Expected workflow

1. Confirm the input kind with the user: one MDim, several MDims, a dataset's indicators, or a hand-picked list.
2. For MDim input, confirm which dimensions (if any) to collapse — `period` is a classic candidate because it usually just changes a unit word in every field.
3. For MDims, rebuild the `.config.json` exports using `etlr` (the full ETL path). For grapher/garden input, rely on the already-built dataset folder.
4. Run the appropriate script:
   - **MDim config rebuild** — one command (etlr's change detection handles garden/MDim edits; drop `--grapher` to skip the slow MySQL data upload — not needed for the FAUST report):
     ```
     .venv/bin/etlr \
         export://multidim/wb/latest/incomes_pip \
         export://multidim/wb/latest/gini_pip \
         export://multidim/lis/latest/gini_lis \
         export://multidim/wid/latest/incomes_wid \
         --export --private
     ```
     Only add `--grapher` if you've changed indicator data/metadata that also needs to land in MySQL for live rendering. No DB-bypass fallback: if MySQL is unreachable, report the error and stop.
   - **MDim mode (render the report)** — edit the `MDIMS` list at the top of `scripts/generate_mdim_text_report.py` or pass `--config <json>` with the same shape; then:
     ```
     .venv/bin/python .claude/skills/chart-text-report/scripts/generate_mdim_text_report.py
     ```
   - **Dataset mode** — audit every indicator of a grapher dataset:
     ```
     .venv/bin/python .claude/skills/chart-text-report/scripts/grapher_dataset_mode.py \
         --dataset data/grapher/wb/2026-03-24/world_bank_pip
     ```
   - **Indicator-list mode** — hand-picked catalogPaths:
     ```
     .venv/bin/python .claude/skills/chart-text-report/scripts/grapher_dataset_mode.py \
         --indicators 'grapher/wb/2026-03-24/world_bank_pip/incomes#thr__...' \
                      'grapher/wb/2026-03-24/world_bank_pip/incomes#share__...'
     ```
5. Show the user the output file paths and wait for feedback — the user almost always wants iterative tweaks to format (slug style, which dims to collapse, etc.). Dataset mode has no collapse/parametrization; if the user wants dataset views grouped by a shared dim, fall back to the MDim-style code path.

## Comparing the live config to a target FAUST report

A common workflow: the user shares a FAUST report that represents the **desired** end state (their edited copy of an earlier auto-generated report) and asks "does the live MDim match this?". The report is *usually* the source of truth — but not always. Sometimes the user updated the meta.yml *after* generating the report, in which case the **live config** is the desired target and the report is stale. **Always ask before assuming the direction.**

Two failure modes are common in WID-style MDim audits:

- **Text-content drift in inherited bullets.** The report shows the older shorter wording for welfare_type / methodology bullets while the live config has the new longer wording. This usually means the user rewrote `description_key_welfare_type` (or similar) *after* the FAUST run. Surface the diff side-by-side and ask which is the target — don't quietly revert the meta.yml.
- **View-count mismatch.** The report has more or fewer sections than the live config (e.g. report has 16 views, live has 13 because scatter views were dropped, or the report still mentions `before_vs_after_scatter` sections that were intentionally removed). When counts differ, list the missing/extra sections explicitly and ask before adding/removing views.

Before doing the field-by-field comparison, refresh everything the live config depends on. Skipping a step leaves a stale catalog, which produces phantom drift that isn't real:

```
.venv/bin/etlr garden/<ns>/<ver>/<ds> grapher/<ns>/<ver>/<ds> --private --force --only
.venv/bin/etlr multidim/<ns>/<ver>/<mdim> --export --only --private --force
```

Run both upstream steps — `garden --only` alone does NOT refresh the grapher channel, and the FAUST scripts (and ad-hoc `Dataset(grapher_path).read(...)` queries) read from grapher, not garden. Without the grapher refresh you'll see pre-edit metadata even though the meta.yml was already updated.

Then audit:

1. **Spot-check several view types**, not just one — overrides, `before_vs_after`, single-decile, all-decile (multi-indicator), share-vs-non-share. Different code paths populate different fields.
2. **Override fields live on the view; inherited fields don't.** A view's `metadata.description_key` in the `.config.json` only contains bullets the MDim explicitly set (via `view.metadata["description_key"] = [...]` or `view_metadata` in `group_views`). Empty array / missing key means the bullets come from the underlying y-indicator — read those via `Dataset(<grapher_path>).read(<table>, load_data=False)[<col>].metadata.description_key`.
3. **Programmatic display.name overrides on indicators within multi-indicator views** (e.g. `5th decile (median)` annotation on the decile_5 indicator inside a `thr+all` view) live on `view['indicators']['y'][i]['display']['name']`, not on the view's text fields. Inspect them per-indicator.
4. **Slug collisions in the report (`Income-share-decile` vs `income-share-decile`, `Expressed-constant-international` vs `expressed-constant-international`) are tooling artefacts** — the chart-text-report script can split a single bullet into two slugs because of trailing whitespace or invisible diffs. The actual rendered text is identical. Per the user's feedback, ignore capital/lowercase slug differences during audits.
5. **Check punctuation around markdown links specifically.** `[Economic Inequality.](url)` (period inside) vs `[Economic Inequality](url).` (period outside) is a common copy-edit issue and easy to miss.
6. **Common drift you'll see:**
   - `_post-tax_` / `_pre-tax_` hyphenation removed from welfare_type bullets
   - "after tax" qualifier removed from subtitle / description_short overrides
   - `description_key[1:]` drops removed (so leading "inequality" / "gini-coefficient" / etc. bullets are kept on grouped views)
   - New indicator-specific bullets added (`description_key_avg`, `description_key_thr`, `description_key_top_incomes`, etc.)
7. **If the live and target diverge, the fix usually lands in one of three places:**
   - the garden meta.yml `definitions.description_key_*` blocks (text content)
   - the MDim `.py` (override via `_assert_and_replace`, `_replace_welfare_type_bullet`, or `view.metadata[...] = ...`)
   - rarely, the indicator's `presentation.grapher_config` block (when the issue is title/subtitle/note rather than description_key)
8. **After every fix push**, re-run garden + grapher + MDim export and re-verify against the report.

## Things to avoid

- Do NOT fall back to `title` / `title_public` / `display.name` / `description_short` when resolving chart Title / Subtitle / Footnote. Use `grapher_config` only (see inheritance rules above).
- Do NOT report `description_processing`; it's noisy and the user explicitly doesn't care about it for FAUST review.
- Do NOT load metadata from the garden channel; it exposes pre-template Jinja text and unflattened dimensions. Always use the grapher channel.
- Do NOT monkey-patch around a MySQL outage by calling `Collection.save_config_local()` directly or stubbing out `validate_indicators_in_db` / `upsert_to_db`. The local config would drift from what the server actually publishes. If MySQL is down, stop and tell the user.
- Do NOT produce HTML `<details>` blocks or tables — the user's preferred format is a flat Markdown outline with bullet fields.

## Related memories and references

- `.claude/projects/-Users-parriagadap-etl/memory/faust_definition.md` — FAUST = Footnote, Axis titles, Units, Subtitle, Title.
- `.claude/projects/-Users-parriagadap-etl/memory/feedback_chart_faust_inheritance.md` — the inheritance rule, with the caveat about `grapher_config` not being universally populated.
- `.claude/skills/chart-text-report/scripts/` — the scripts this skill drives.
