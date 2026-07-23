---
name: faust-metadata-audit
description: Generate a compact Markdown audit of user-facing chart text (Title, Subtitle, Footnote, description_short, description_key — i.e. FAUST) for an MDim, a grapher/garden dataset, or a hand-picked list of indicators. Each field is tagged by source (override / inherited / missing) so the reader can tell what Grapher renders vs. what comes from the ETL metadata. Trigger when the user wants to review, audit, or spot-check the user-facing text of one or many charts/indicators at once — e.g. "audit the FAUST for dataset X", "dump the FAUST for dataset X", "I want to review the text of all views in this MDim", "show me the chart text for these indicators".
metadata:
  internal: true
---

# FAUST metadata audit

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

1a. **`description_key` arrives as a markdown STRING, not a list**: the grapher channel serializes it via `owid.catalog.core.meta.description_key_to_string` — multiple bullets become one string joined as `"- b1\n- b2\n…"`, a single bullet becomes plain prose (datasets built before the change still carry lists). `scripts/_common.py:description_key_as_list()` normalizes both forms back into a bullet list; both report modes route through it. The same trap hits **MDim step code** that asserts/replaces bullets from `tb[col].metadata.description_key`: `OLD_TEXT in list(dk)` silently iterates characters on the string form and the assertion fails (or, worse, a `for b in dk` loop explodes bullets into characters). Normalize first (see `_description_key_bullets` in `incomes_pip.py` / `gini_lis.py` / `gini_wid.py`), then do list-membership asserts and per-bullet swaps; setting either a list or a markdown string back on `view.metadata["description_key"]` is accepted (`Collection` converts lists via `_convert_description_key_lists`).

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
     .venv/bin/python .claude/skills/faust-metadata-audit/scripts/generate_mdim_text_report.py
     ```
   - **Dataset mode** — audit every indicator of a grapher dataset:
     ```
     .venv/bin/python .claude/skills/faust-metadata-audit/scripts/grapher_dataset_mode.py \
         --dataset data/grapher/wb/2026-03-24/world_bank_pip
     ```
   - **Indicator-list mode** — hand-picked catalogPaths:
     ```
     .venv/bin/python .claude/skills/faust-metadata-audit/scripts/grapher_dataset_mode.py \
         --indicators 'grapher/wb/2026-03-24/world_bank_pip/incomes#thr__...' \
                      'grapher/wb/2026-03-24/world_bank_pip/incomes#share__...'
     ```
5. Show the user the output file paths and wait for feedback — the user almost always wants iterative tweaks to format (slug style, which dims to collapse, etc.). Dataset mode has no collapse/parametrization; if the user wants dataset views grouped by a shared dim, fall back to the MDim-style code path.

## Comparing the live config to a target FAUST report

A common workflow: the user shares a FAUST report that represents the **desired** end state (their edited copy of an earlier auto-generated report) and asks "does the live MDim match this?". **Treat the report as the source of truth by default** — when the live config differs, the fix lands in the metadata to make the live match the report.

Two cases warrant a confirmation before silently editing the metadata to match:

- **Text-content drift in inherited bullets.** If the report shows older / shorter wording for welfare_type / methodology bullets while the live config has newer longer wording, surface the diff side-by-side and confirm before reverting — sometimes the user rewrote `description_key_welfare_type` (or similar) *after* generating the report and the live config is the up-to-date target. The report is still usually right; just don't auto-revert recent rewrites.
- **View-count mismatch.** If the report has more or fewer sections than the live config (e.g. report includes `before_vs_after_scatter` sections that were intentionally removed, or the live has views the report doesn't list), list the missing/extra sections explicitly and confirm before adding/removing views.

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
4. **Slug collisions in the report (`Income-share-decile` vs `income-share-decile`, `Expressed-constant-international` vs `expressed-constant-international`) are tooling artefacts** — the audit script can split a single bullet into two slugs because of trailing whitespace or invisible diffs. The actual rendered text is identical. Per the user's feedback, ignore capital/lowercase slug differences during audits.
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

## Target-driven description_key restructuring across sibling MDims

A recurring large-scale workflow: the user pastes an edited FAUST report as the *desired* state for one dataset's MDims, then repeats it for sibling datasets ("now do the same for LIS / WID"). Lessons that generalize:

1. **The delta concentrates in `description_key`.** Chart-level FAUST (Title/Subtitle/Footnote) and description_short almost always already match the target — verify that first and scope the work to bullet texts, per-variable list ordering, and the MDim mirror constants. Apply the new bullet ordering to the *whole dataset* (all tables/variables sharing the definitions), not just the MDim's indicators, unless told otherwise.
2. **Audit the target's legend↔views cross-references before editing.** Slugs referenced by views but missing from the legend usually map to an existing garden bullet — keep it unchanged (e.g. an `income-after-tax-missing` slug that was the existing posttax-availability bullet). Legend bullets referenced by no view get skipped (confirm once with the user; it has held every round).
3. **Fact-check target texts against each dataset's actual data** — hand-edited targets propagate copy-paste from the first dataset: "income or consumption" onto income-only datasets, "country or region" where no regional aggregates exist, regional-extrapolation bullets on indicators with zero regional values. Verify empirically (count non-null values for region entities per indicator family) rather than trusting either the target or the old metadata; drop/adapt per finding, with the user's sign-off pattern.
4. **Bullets describing UI affordances must match the view's actual UI.** A bullet like "this chart gives the option to show breaks" is wrong on grouped views that exist only for one choice of that dimension (no toggle) — drop the bullet or strip the affordance sentence via a view-level override in the MDim `.py` (see the grouped-view loop in `incomes_pip.py`). Beware MDims that keep their own config-level `definitions.description_key_*` overrides (e.g. `poverty_pip.config.yml`) — garden edits don't reach those views; align the config-local copies separately.
5. **New `#dod:…` links in a target may not exist.** Check the `dods` table via public Datasette (`SELECT name FROM dods WHERE name LIKE …`) before shipping; if missing, the established pattern is keep-the-link + list it in the PR body as a "create in admin" follow-up.
6. **Shared definitions serve more variants than the target shows.** Poverty vs inequality indicators, wealth vs income (WID), welfare-specific tables, extrapolated series — add Jinja branches so the target's income-flavored wording doesn't leak onto the other variants, and check the untouched-variant MDims (e.g. `wealth_wid`, `poverty_pip`) in the regenerated reports.
7. **Jinja dimension comparisons: match the value type used elsewhere in the same file.** Dimension values can be int in one dataset and str in another (`decile == 5` in LIS vs `decile == "5"` in PIP) — a wrong-type comparison renders the else-branch silently; copy the comparison form from a working definition (e.g. `threshold_title`) and spot-check the affected view in the report.
8. **Bulk list edits with Edit/replace_all: order by containment.** Reorder the anchored/longer per-variable blocks first and the bare short-tail patterns last, since short blocks are substrings of longer ones; then verify all lists at once with a small parser script over the meta.yml rather than re-reading each variable.
9. **Mirror constants change in lockstep.** MDim `.py` files hard-copy garden bullet texts (`OLD_*`/`NEW_*`) under assertions — every garden text edit needs the matching constant edit, and the rebuild's assertion pass is the drift check. Grep the repo for fragments of any text you change.
10. **Tag placement is cosmetic in target comparisons**: a view the target marks `[inherited]` may only be implementable as `[override]` (and vice versa when an obsolete override is removed) — identical bullet content is what matters.

## Regression diff: prove a refactor didn't change user-facing text

When you change an MDim `.py` (reorder indicators, flip a `before_vs_after` choice order, change which y-indicator is primary, rename a helper) and need to prove the rendered FAUST is **unchanged except for the intended diff**, diff two auto-generated reports instead of eyeballing one. This is the right check whenever a change shifts the **primary y-indicator** (`y[0]`), because that's what drives inheritance — the script resolves every `[inherited]` field from `y[0]`, so identical reports prove the inherited text doesn't depend on which variant is primary (e.g. flipping LIS `welfare_type` from `dhi`-first to `mi`-first leaves all six fields byte-identical because `mi`/`dhi` share every inheritable field and the overrides resolve the same).

`config_path` accepts **any** JSON path, not just the live `export/multidim/.../<name>.config.json` — so point two runs at two config snapshots:

1. Build the baseline config (e.g. `git checkout origin/master -- <step>.py && etlr <mdim> --export --grapher`) and copy its `<name>.config.json` to `/tmp/cfg_before/`. Restore your branch (`git checkout HEAD -- <step>.py`), rebuild, copy to `/tmp/cfg_after/`. (Note: `git checkout … -- a.py b.py` won't word-split an unquoted `$files` var in zsh — pass the paths literally or use an array.)
2. Run the report against each snapshot:
   ```
   echo '[{"name":"gini_lis_BEFORE","config_path":"/tmp/cfg_before/gini_lis.json","collapse_dims":[]}]' > /tmp/fb.json
   echo '[{"name":"gini_lis_AFTER","config_path":"/tmp/cfg_after/gini_lis.json","collapse_dims":[]}]'  > /tmp/fa.json
   .venv/bin/python .claude/skills/faust-metadata-audit/scripts/generate_mdim_text_report.py --config /tmp/fb.json
   .venv/bin/python .claude/skills/faust-metadata-audit/scripts/generate_mdim_text_report.py --config /tmp/fa.json
   ```
3. Diff, stripping the BEFORE/AFTER name token: `diff <(sed 's/BEFORE//g' ai/gini_lis_BEFORE.md) <(sed 's/AFTER//g' ai/gini_lis_AFTER.md)`. Byte-identical = Title/Subtitle/Footnote/description_short/description_key all render the same.

The FAUST diff only covers user-facing **text**. It will NOT catch indicator-order-only changes (e.g. a Dumbbell arrow direction or a LineChart series-color swap that follows column order) — pair it with a structural diff of the two `.config.json` files when order matters.

## Things to avoid

- Do NOT fall back to `title` / `title_public` / `display.name` / `description_short` when resolving chart Title / Subtitle / Footnote. Use `grapher_config` only (see inheritance rules above).
- Do NOT report `description_processing`; it's noisy and the user explicitly doesn't care about it for FAUST review.
- Do NOT load metadata from the garden channel; it exposes pre-template Jinja text and unflattened dimensions. Always use the grapher channel.
- Do NOT monkey-patch around a MySQL outage by calling `Collection.save_config_local()` directly or stubbing out `validate_indicators_in_db` / `upsert_to_db`. The local config would drift from what the server actually publishes. If MySQL is down, stop and tell the user.
- Do NOT produce HTML `<details>` blocks or tables — the user's preferred format is a flat Markdown outline with bullet fields.

## Related memories and references

- `.claude/projects/-Users-parriagadap-etl/memory/faust_definition.md` — FAUST = Footnote, Axis titles, Units, Subtitle, Title.
- `.claude/projects/-Users-parriagadap-etl/memory/feedback_chart_faust_inheritance.md` — the inheritance rule, with the caveat about `grapher_config` not being universally populated.
- `.claude/skills/faust-metadata-audit/scripts/` — the scripts this skill drives.
