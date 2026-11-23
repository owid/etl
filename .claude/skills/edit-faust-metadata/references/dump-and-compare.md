# Dump + compare mode (explicit request only)

Produce a Markdown audit of the user-facing chart text for a set of indicators or MDim views, for editorial review or as the target file of a mass edit. **Never suggest this mode proactively** — enter it only when the user asks for a dump/report/audit.

Scripts (shared helpers in `scripts/_common.py`: grapher-channel metadata loader, inheritance resolvers, `BulletLibrary`, auto-slugs, preview URL):

- `scripts/generate_mdim_text_report.py` — MDim view mode (supports `collapse_dims` and placeholder parametrization).
- `scripts/grapher_dataset_mode.py` — grapher-dataset mode (iterates every indicator column) and indicator-list mode (`--indicators <cp> <cp> ...` or `--indicators-file <path>`).

Rebuilding the MDim `.config.json` is done via `etlr viz://chart/<ns>/<ver>/<name>` without `--grapher`: `Chart.save()` then stops after `save_config_local`, skipping `validate_indicators_in_db` and the DB upsert (and the `grapher://grapher/<dataset>` upserts of its inputs), which is all the report needs. Change detection handles the common case: nothing changed → ~2 s; garden `.meta.yml`, garden data, or MDim yaml/py changed → etlr rebuilds only the affected steps.

Do **not** add `--only` when you want garden/MDim edits to take effect — it skips upstream rebuilds by design; use `--only --force` only to re-run just the MDim step without touching anything upstream.

## Fields reported

Only user-facing text. Six fields, two groups:

| Group | Fields | Where they come from |
|---|---|---|
| Chart-level FAUST | `Title`, `Subtitle`, `Footnote` | `presentation.grapher_config.{title, subtitle, note}` |
| Indicator-level metadata | `description_short`, `description_key` | top-level `VariableMeta` fields |

Never report Axis titles or Units in the default output (keep the report skimmable). Never include `description_processing`.

## Inputs supported

| Input kind | Example | Source of per-entity text |
|---|---|---|
| MDim export | `wb/latest/incomes_pip#incomes_pip` | `viz/chart/<ns>/<ver>/<name>/<name>.config.json`, plus grapher-channel inheritance for each view's primary `y` indicator |
| Grapher/garden dataset | `data/grapher/wb/2026-03-24/world_bank_pip` | iterate columns across all tables; all text is `[inherited]` |
| Hand-picked indicators | `grapher/wb/2026-03-24/world_bank_pip/incomes#share__...` | same, filtered to the listed columns |

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

1a. **`description_key` arrives as a markdown STRING, not a list**: the grapher channel serializes it via `owid.catalog.core.meta.description_key_to_string` — multiple bullets become one string joined as `"- b1\n- b2\n…"`, a single bullet becomes plain prose (some datasets still carry lists). `scripts/_common.py:description_key_as_list()` normalizes both forms back into a bullet list; both report modes route through it. The same trap hits **MDim step code** that asserts/replaces bullets from `tb[col].metadata.description_key`: `OLD_TEXT in list(dk)` silently iterates characters on the string form and the assertion fails (or, worse, a `for b in dk` loop explodes bullets into characters). Normalize first (see `_description_key_bullets` in `incomes_pip.py` / `gini_lis.py` / `gini_wid.py`), then do list-membership asserts and per-bullet swaps; setting either a list or a markdown string back on `view.metadata["description_key"]` is accepted (`Chart` converts lists via `_convert_description_key_lists`).

2. **Rebuilding the MDim `.config.json`**: use `etlr viz://chart/<ns>/<ver>/<name>` (no `--grapher`; see above), so the report never needs the DB.

3. **Description-key dedup with auto slugs**: collect unique bullets into a per-file legend, auto-generate a short slug from the first ~3 non-stopword content words of each bullet (kebab-case), disambiguate collisions with `-2`/`-3` suffixes. Each view references bullets by their slugs, rendered as sub-bullets.

4. **Dimension collapse (MDim only)**: accept a `collapse_dims: list[str]` per MDim. Group views whose non-collapsed dims match, render one section per group, show variant previews on separate links labelled by the collapsed dim's value.

5. **Placeholder parametrization**: when the Title / Subtitle / description_short / description_key vary across collapsed variants only by a simple substitution, collapse the text to a single `{dim}` placeholder. Try the raw value first (`day` in `per day`), then snake → space (`before_tax` → `before tax`), then snake → hyphen (`before-tax`); case-insensitive regex. If all variants collapse to the same placeholder-bearing string, use it; else fall back to sub-bullets.

6. **Global placeholder legend**: when one or more dims are parametrized, include a header line listing `` `{dim}` ∈ {val1, val2, ...} `` once at the top of the file.

7. **Human-readable dim selections subheader**: directly under each view heading, render the dim selections using the dimension `name` and choice `name` from the MDim config. Filter out `nan` sentinel values.

7a. **Heading disambiguation when views share a title**: when two or more groups collapse to the same `## <Title>` heading, append `(Dim name: Choice name)` built from the non-collapsed dim(s) whose values differ across the colliding groups. Dim order follows the MDim config; only the differentiating dim(s) are appended.

8. **Preview URLs**: main MDim URL is `https://admin.owid.io/admin/grapher/<urlquote(catalog_path)>`. Per-view URL appends `?dim1=slug1&dim2=slug2` from the view's `dimensions` dict.

9. **Override / inherited / missing tagging**: `[override]` = text explicitly set on the view (MDim `config.*` or `metadata.*`); `[inherited]` = resolved from the primary y-indicator's ETL metadata; `[missing]` = absent in both. For grapher-dataset and indicator-list inputs, every tag is `[inherited]` or `[missing]`.

10. **`ai/` directory output** (per project convention). One Markdown file per entity the user asked about.

## Dump-mode workflow

1. Confirm the input kind: one MDim, several MDims, a dataset's indicators, or a hand-picked list.
2. For MDim input, confirm which dimensions (if any) to collapse — `period` is a classic candidate.
3. For MDims, rebuild the `.config.json` exports using `etlr` (full ETL path). For grapher/garden input, rely on the already-built dataset folder.
4. Run the appropriate script:
   - **MDim config rebuild**:
     ```
     .venv/bin/etlr viz://chart/wb/latest/incomes_pip --grapher
     ```
   - **MDim mode** — edit the `MDIMS` list at the top of `scripts/generate_mdim_text_report.py` or pass `--config <json>`:
     ```
     .venv/bin/python .claude/skills/edit-faust-metadata/scripts/generate_mdim_text_report.py
     ```
   - **Dataset mode**:
     ```
     .venv/bin/python .claude/skills/edit-faust-metadata/scripts/grapher_dataset_mode.py \
         --dataset data/grapher/wb/2026-03-24/world_bank_pip
     ```
   - **Indicator-list mode**:
     ```
     .venv/bin/python .claude/skills/edit-faust-metadata/scripts/grapher_dataset_mode.py \
         --indicators 'grapher/wb/2026-03-24/world_bank_pip/incomes#thr__...' \
                      'grapher/wb/2026-03-24/world_bank_pip/incomes#share__...'
     ```
5. Show the user the output file paths and wait for feedback — the user almost always wants iterative tweaks to format. Dataset mode has no collapse/parametrization; if the user wants dataset views grouped by a shared dim, fall back to the MDim-style code path.

## Comparing the live config to a target FAUST report

A common workflow: the user shares a FAUST report that represents the **desired** end state (their edited copy of an earlier auto-generated report) and asks "does the live MDim match this?". **Treat the report as the source of truth by default** — when the live config differs, the fix lands in the metadata to make the live match the report.

Two cases warrant a confirmation before silently editing the metadata to match:

- **Text-content drift in inherited bullets.** If the report shows older / shorter wording while the live config has newer longer wording, surface the diff side-by-side and confirm before reverting — sometimes the user rewrote the definition *after* generating the report and the live config is the up-to-date target.
- **View-count mismatch.** If the report has more or fewer sections than the live config, list the missing/extra sections explicitly and confirm before adding/removing views.

Before doing the field-by-field comparison, refresh everything the live config depends on. Skipping a step leaves a stale catalog, which produces phantom drift that isn't real:

```
.venv/bin/etlr garden/<ns>/<ver>/<ds> grapher/<ns>/<ver>/<ds> --force --only
.venv/bin/etlr chart/<ns>/<ver>/<mdim> --grapher --only --force
```

Run both upstream steps — `garden --only` alone does NOT refresh the grapher channel, and the FAUST scripts read from grapher, not garden.

Then audit:

1. **Spot-check several view types**, not just one — overrides, `before_vs_after`, single-decile, all-decile (multi-indicator), share-vs-non-share. Different code paths populate different fields.
2. **Override fields live on the view; inherited fields don't.** A view's `metadata.description_key` in the `.config.json` only contains bullets the MDim explicitly set. Empty array / missing key means the bullets come from the underlying y-indicator — read those via `Dataset(<grapher_path>).read(<table>, load_data=False)[<col>].metadata.description_key`.
3. **Programmatic display.name overrides on indicators within multi-indicator views** live on `view['indicators']['y'][i]['display']['name']`, not on the view's text fields. Inspect them per-indicator.
4. **Slug collisions in the report (`Income-share-decile` vs `income-share-decile`) are tooling artefacts** — ignore capital/lowercase slug differences during audits.
5. **Check punctuation around markdown links specifically.** `[Economic Inequality.](url)` (period inside) vs `[Economic Inequality](url).` (period outside) is a common copy-edit issue and easy to miss.
6. **Common drift you'll see:** hyphenation removed from welfare_type bullets; qualifiers removed from subtitle / description_short overrides; `description_key[1:]` drops removed; new indicator-specific bullets added.
7. **If the live and target diverge, the fix usually lands in one of three places:** the garden meta.yml `definitions.description_key_*` blocks (text content); the MDim `.py` (override via `_assert_and_replace`, `_replace_welfare_type_bullet`, or `view.metadata[...] = ...`); rarely, the indicator's `presentation.grapher_config` block.
8. **After every fix push**, re-run garden + grapher + MDim export and re-verify against the report.

## Target-driven description_key restructuring across sibling MDims

A recurring large-scale workflow: the user pastes an edited FAUST report as the *desired* state for one dataset's MDims, then repeats it for sibling datasets ("now do the same for LIS / WID"). Lessons that generalize:

1. **The delta concentrates in `description_key`.** Chart-level FAUST and description_short almost always already match the target — verify that first and scope the work to bullet texts, per-variable list ordering, and the MDim mirror constants. Apply the new bullet ordering to the *whole dataset*, not just the MDim's indicators, unless told otherwise.
2. **Audit the target's legend↔views cross-references before editing.** Slugs referenced by views but missing from the legend usually map to an existing garden bullet — keep it unchanged. Legend bullets referenced by no view get skipped (confirm once with the user).
3. **Fact-check target texts against each dataset's actual data** — hand-edited targets propagate copy-paste from the first dataset: "income or consumption" onto income-only datasets, "country or region" where no regional aggregates exist. Verify empirically (count non-null values for region entities per indicator family) rather than trusting either the target or the old metadata.
4. **Bullets describing UI affordances must match the view's actual UI.** A bullet like "this chart gives the option to show breaks" is wrong on grouped views that exist only for one choice of that dimension — drop the bullet or strip the affordance sentence via a view-level override. Beware MDims that keep their own config-level `definitions.description_key_*` overrides — garden edits don't reach those views; align the config-local copies separately.
5. **New `#dod:…` links in a target may not exist.** Check them as in SKILL.md, "Metadata quality checks".
6. **Shared definitions serve more variants than the target shows.** Add Jinja branches so the target's wording doesn't leak onto other variants (poverty vs inequality, wealth vs income), and check the untouched-variant MDims in the regenerated reports.
7. **Jinja dimension comparisons: match the value type used elsewhere in the same file** (see SKILL.md, "Verifying on staging").
8. **Bulk list edits with Edit/replace_all: order by containment.** Reorder the anchored/longer per-variable blocks first and the bare short-tail patterns last; then verify all lists at once with a small parser script over the meta.yml.
9. **Mirror constants change in lockstep** (see the MDim routing in SKILL.md); the rebuild's assertion pass is the drift check.
10. **Tag placement is cosmetic in target comparisons**: a view the target marks `[inherited]` may only be implementable as `[override]` (and vice versa) — identical bullet content is what matters.

## Regression diff: prove a refactor didn't change user-facing text

When you change an MDim `.py` (reorder indicators, flip a choice order, change which y-indicator is primary) and need to prove the rendered FAUST is **unchanged except for the intended diff**, diff two auto-generated reports instead of eyeballing one. This is the right check whenever a change shifts the **primary y-indicator** (`y[0]`), because that's what drives inheritance.

`config_path` accepts **any** JSON path, not just the live `viz/chart/.../<name>.config.json` — so point two runs at two config snapshots:

1. Build the baseline config (e.g. `git checkout origin/master -- <step>.py && etlr <mdim> --grapher`) and copy its `<name>.config.json` to `/tmp/cfg_before/`. Restore your branch, rebuild, copy to `/tmp/cfg_after/`. (Note: `git checkout … -- a.py b.py` won't word-split an unquoted `$files` var in zsh — pass the paths literally or use an array.)
2. Run the report against each snapshot:
   ```
   echo '[{"name":"gini_lis_BEFORE","config_path":"/tmp/cfg_before/gini_lis.json","collapse_dims":[]}]' > /tmp/fb.json
   echo '[{"name":"gini_lis_AFTER","config_path":"/tmp/cfg_after/gini_lis.json","collapse_dims":[]}]'  > /tmp/fa.json
   .venv/bin/python .claude/skills/edit-faust-metadata/scripts/generate_mdim_text_report.py --config /tmp/fb.json
   .venv/bin/python .claude/skills/edit-faust-metadata/scripts/generate_mdim_text_report.py --config /tmp/fa.json
   ```
3. Diff, stripping the BEFORE/AFTER name token: `diff <(sed 's/BEFORE//g' ai/gini_lis_BEFORE.md) <(sed 's/AFTER//g' ai/gini_lis_AFTER.md)`. Byte-identical = all six fields render the same.

Metadata Diff does not replace this. It answers "what changed, and where does it land" on a server that has the build; this answers "prove nothing changed" offline, byte for byte, before anything is pushed.

The FAUST diff only covers user-facing **text**. It will NOT catch indicator-order-only changes (e.g. a Dumbbell arrow direction or a series-color swap that follows column order) — pair it with a structural diff of the two `.config.json` files when order matters.

## Things to avoid in dump mode

- Do NOT report `description_processing` in dump mode; the user explicitly doesn't care about it for FAUST review.
- Do NOT produce HTML `<details>` blocks or tables in dump-mode reports — the preferred format is a flat Markdown outline with bullet fields.
