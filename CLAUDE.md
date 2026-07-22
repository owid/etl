# Agent Guide

Our World in Data's ETL system - a content-addressable data pipeline with DAG-based execution.

## Critical Rules

- **Always use `.venv/bin/`** for all Python commands (`etl`, `python`, `pytest`)
- **Never mask problems** - no empty tables, no commented-out code, no silent exceptions
- **Trace issues upstream**: snapshot → meadow → garden → grapher
- **`dag/archive/*.yml` is a generated record** — it is reconstructed from git history by `etl archive-dag`, so never hand-edit it. It lists steps that were once active (with the commit where they were last active) purely for recovery; to bring one back, `git checkout` that commit.
- **Never delete a step without archiving it.** Removing or superseding an active step (new version, retirement, replacement) obligates you to archive it — deleting the files alone is a bug. Procedure: remove its `dag/*.yml` entry and delete its files → **commit** → run `etl archive-dag` (it reads *committed* history, so the removal must be committed first) → commit the regenerated `dag/archive/*.yml`. If `archive-dag` sweeps in unrelated steps others left un-archived, `git checkout` those files to keep your PR scoped (never hand-edit the archive). For a migrated/backport dataset, also delete its now-orphaned `snapshots/backport/latest/dataset_<id>_*` mirror files.
- **Ask the user** if unsure - don't guess
- **Always run `make check` before committing**
- If not told otherwise, save outputs to `ai/` directory.
- **Notebooks**: Always create AND execute immediately using `uv run jupyter nbconvert --to notebook --execute --inplace <path>`
- **Skills**: When creating new skills in `.claude/skills/`, always include `metadata: { internal: true }` in the SKILL.md frontmatter unless the user explicitly asks for the skill to be public. This prevents external skill indexes from crawling and listing our internal skills.

## Team

Everything you post to GitHub or Slack goes out under a **human's identity**. Any text you author and post that a reader could take for the human's own words **must** carry the attribution line below. This is mandatory — not a judgment call about whether the comment is "worth it."

1. **Attribute the work.** Put this blockquote as the *first line* of the content:

   ```
   > _Written by Claude <model name> — @<handle> at the wheel._
   ```

   Replace `<model name>` with the human-readable name of the model actually generating the content (e.g. "Sonnet 5", "Opus 4.8", "Fable 5", "Haiku 4.5") — not the literal string "Code". Keeping the "Claude" prefix makes the attribution recognizable even to readers unfamiliar with individual model names.

   It applies to **every** surface, **every** time you post:
   - PR descriptions / bodies
   - PR issue-level comments
   - **Inline review comments _and_ replies to review comments** (e.g. answering Codex / Copilot / a reviewer)
   - Standalone Slack messages or drafts

   Use the handle of the human directing the work (usually the current git user; ask if ambiguous).

   **The only exception** is a comment that is a bare mechanical token with *no prose* — a lone `@codex review` ping or a 👍. The moment your comment contains a sentence of explanation, it needs the line. When in doubt, include it.

2. **Use exact handles** from the table below when tagging colleagues. Don't guess — a wrong tag pings a real person. If a name isn't in this table, write the plain name (e.g. "Bastian") instead of `@`-tagging, and ask the user for the handle.

   | Name | GitHub handle |
   |---|---|
   | Pablo A Rosado | `@pabloarosado` |
   | Pablo Arriagada | `@paarriagadap` |
   | Veronika Samborska | `@veronikasamborska1994` |
   | Mojmir Vinkler | `@Marigold` |
   | Lucas Rodés-Guirao | `@lucasrodes` |
   | Tuna Acisu | `@antea04` |
   | Fiona Spooner | `@spoonerf` |
   | Edouard Mathieu | `@edomt` |

   The disclosure rule does **not** apply to OWID-reader-facing artifacts (e.g. the `/latest` data-update post on ourworldindata.org) — those are authored by the named human, not by Claude.

3. **This repo is public — keep internal context out of it.** PR descriptions, commit messages, and issue/review comments must never identify people who contact us (no names, roles, or employers — say "a reader pointed out ..." instead), and must not reference internal discussions (Slack threads, Notion docs) or who suggested what internally. Motivate changes using public facts only; internal context stays internal.

## Pipeline Overview

**snapshot** → **meadow** → **garden** → **grapher** → **export**

| Stage | Location | Purpose |
|-------|----------|---------|
| snapshot | `snapshots/` | DVC-tracked raw data |
| meadow | `etl/steps/data/meadow/` | Basic cleaning |
| garden | `etl/steps/data/garden/` | Business logic, harmonization |
| grapher | `etl/steps/data/grapher/` | MySQL ingestion |

**Snapshot is raw passthrough only.** It downloads the source files and writes them out using the source's own row labels, column labels, and period labels. That's it. The following all belong in **garden**, not in the snapshot script:

- Summing or merging rows from different source categories into one bucket
- Picking the most recent value when several source files report the same period
- Converting a source's period labels (fiscal quarters, season codes, week numbers) into dates
- Renaming source categories for the chart

If you find yourself doing any of these in the snapshot, move them to garden.

## Glossary

Internal terms that recur across this guide, the skills, and the codebase:

- **ETL:** the data pipeline that gets external datasets into the Grapher
- **Grapher:** OWID's charting tool / database for interactive visualizations
- **OMM:** OWID-Maintained Metric — a curated indicator for a topic
- **MDim / Multi-dimensional indicator:** interactive chart with toggleable views (e.g. total vs per capita)
- **Explorer:** a more complex interactive tool than a single chart, with multiple tabs/views
- **FAUST:** chart text (Footnote, Axis title, Unit, Subtitle, Title)
- **WYSK:** "What You Should Know" — metadata/description attached to a dataset or chart
- **Topic page:** an evergreen page on a topic (modular or linear)
- **Key insight:** a short, standalone data point highlighted on a topic page
- **Static viz:** a one-off image (Figma/Illustrator), not an interactive Grapher chart

## Running ETL Steps

```bash
.venv/bin/etlr namespace/version/dataset --private      # Run step
.venv/bin/etlr namespace/version/dataset --grapher      # Upload to grapher
.venv/bin/etlr namespace/version/dataset --dry-run      # Preview
.venv/bin/etlr namespace/version/dataset --force --only # Force re-run
```

Key flags: `--grapher/-g` (upload), `--dry-run` (preview), `--force/-f` (re-run), `--only/-o` (no deps), `--private` (always use)

### Running Snapshot Steps

```bash
.venv/bin/etls namespace/version/dataset               # Download & upload snapshot
.venv/bin/etls namespace/version/dataset --skip-upload  # Download only
```

**Important:**
- **Snapshot scripts need no `__main__` guard** — the `etls` CLI imports the module and invokes its click command `run` directly, so don't add `if __name__ == "__main__":` boilerplate. Many old scripts still carry it; don't copy them.
- **Avoid `--force`** — `etlr` has built-in change detection and re-runs steps whose **code, dag entries, or data** changed. Editing a step's `.py`/`.yml` or its dag dependency line is enough to trigger a rebuild — don't add `--force`. Reserve `--force --only` for the narrow case where nothing in the repo changed but you still need to re-run (e.g., upstream data was patched out-of-band). Never use `--force` alone.
- **`--only` requires deps on disk.** It skips dep resolution and won't download missing deps — even with `PREFER_DOWNLOAD=1`. If you hit a `FileNotFoundError` on a dep's `index.json`, drop `--only` and let etlr resolve the chain.
- **`PREFER_DOWNLOAD=1`** — Download already-built datasets from the OWID catalog instead of recomputing locally. Useful when verifying a downstream step still works after a dag edit (the upstream deps get fetched, not rebuilt). Doesn't help if you've edited the dataset's own code. It also **fails with `AccessDenied` when the target version isn't in the catalog yet** (e.g. a version you just created) — use it only to fetch already-published upstream deps, never for the new step you're building locally.
- For `grapher://` steps, always add `--grapher` flag
- **Pushing to the grapher DB:** running a `data://grapher/...` step (even with `--grapher`) only builds the dataset feather. The MySQL upsert is the separate `grapher://...` step. If a metadata-only change (`display`, `description_key`, etc.) isn't showing up in the grapher DB, run `etlr grapher://grapher/<path> --grapher` explicitly to force the variable upsert.
- **`STAGING=1`** — makes `etlr` target the current branch's staging server: `STAGING=1 .venv/bin/etlr grapher://grapher/<path> --grapher` upserts the indicators straight to `staging-site-<branch>`'s DB. Optional: staging rebuilds automatically after you push, so you only need this when you want a change reflected there right away, or when the automatic rebuild is unusually slow (rare, e.g. edits to the regions or FAOSTAT datasets that invalidate a large part of the DAG). `STAGING=<name>` targets another branch's staging server.
- **Version-bumping a grapher step mints new variable IDs**, so existing charts referencing the old indicators become ghost variables and must be remapped on staging (see the `remapping-ghost-variables` skill / `indicator_upgrade` CLI). Budget for this whenever you rename or re-version a grapher dataset.
- **Versioning hygiene for derived/OMM steps:** an OMM's version reflects when its combining logic was written, not its inputs — but when you repoint a derived step to a newer-dated dependency, bump the step's own version folder too. Leaving a step dated before the data it ingests is confusing and should be fixed when noticed.
- Some steps support **`SUBSET`** env var for fast dev iterations: `SUBSET='France,Germany' .venv/bin/etlr namespace/version/dataset --private`
- **No `.py` for simple downloads** — when a snapshot is a plain `url_download` (no custom fetch/parse/auth logic), create only the `.dvc` file; do **not** write `snapshots/.../<short>.py`. `etls <ns>/<version>/<short>` runs it straight from the `.dvc`. Write a script only when the download genuinely needs custom code (API pagination, auth, multi-file assembly, local/manual file input, non-trivial parsing before storing).

## Git Workflow

**Always use `etl pr`** - never use `git checkout -b` + `gh pr create` manually.

```bash
# 1. Create PR (creates new branch, does NOT commit)
.venv/bin/etl pr "Update dataset" data

# 2. Stage and commit
git add .
git commit -m "🔨🤖 Description"

# 3. Push
git push

# 4. Add PR description
gh pr edit <number> --body "..."
```

**Cleaning up after merge**: `etl pr-clean` lists local branches whose PR was merged or closed (it checks the GitHub PR state, so squash-merges are detected), then deletes the selected branch(es). For branches created in a worktree (`etl pr "..." --worktree`), it also removes the worktree and copies that worktree's Claude sessions back into the main repo's `~/.claude/projects/` dir so they stay resumable.

**After `etl pr --worktree`, verify the branch is current AND actually pushed.** Worktree creation can branch off a stale local `master` (missing recent merges → `_check_dag_completeness` "not in the DAG" errors on steps that should already exist) — fix with `git fetch origin master && git rebase origin/master`. That rebase then needs its own push: check `gh pr view --json additions,deletions` isn't `0`/`0` before assuming the PR reflects your commits.

**Post `@codex review` as a separate PR comment** (not in the PR description) when the PR is ready for a review pass. Do not repost it after every push/update unless the user asks or the changes are substantial enough to warrant a fresh review.

To run the full **review → wait → fix → re-review** loop hands-off (and watch CI) in the background while you keep working, use the `pr-babysitter` skill — it spawns a background agent that triggers Codex, judges and fixes the valid findings, and loops to a cap (never merges). Fire it proactively after pushing a substantial chunk to a PR branch.

### Commit Message Emojis

| Emoji | Use for |
|-------|---------|
| 🎉 | New feature |
| 🐛 | Bug fix |
| ✨ | Improvement |
| 🔨 | Code change |
| 📊 | Data updates |
| 📜 | Docs |
| 💄 | Formatting |

Add 🤖 after emoji for AI-written code: `🔨🤖 Refactor country mapping`

## Code Patterns

### Descriptive short names

Step, table, and indicator short names must be readable by any OWID colleague without context. Spell terms out instead of coining acronyms or initialisms:

- Steps: `future_of_food_and_agriculture_arable_land`, not `fofa_2050_arable_land`
- Indicators: `cropland_business_as_usual` / `cropland_stratified_societies`, not `cropland_bau` / `cropland_sss`

Only universally understood abbreviations are fine (`gdp`, `co2`, `un_wpp`-style producer acronyms that OWID already uses). If the source uses an internal acronym for a scenario, product, or variable, expand it in our short names — the acronym can live in titles/descriptions where there's room to define it.

### Preserving metadata/origins in steps

- **No `np.where`** — strips origins. Use `tb["col"] = tb["b"]; tb.loc[mask, "col"] = tb.loc[mask, "a"]`
- **No `pd.concat`** — strips origins. Use `pr.concat` (`from owid.catalog import processing as pr`)
- **No `pd.to_numeric` / `pd.to_datetime`** — strip origins. Use `pr.to_numeric` / `pr.to_datetime` (same `from owid.catalog import processing as pr`).
- **No `pd.DataFrame(tb)`** to "convert" a Table back to a plain DataFrame for downstream helpers — strips column origins. Tables are DataFrame subclasses; pass them through helpers directly and use `pr.*` for any combining ops.
- **`.dt.*` and `.str.*` accessors return plain Series** — they drop the Variable's metadata on assignment. After `tb[col] = tb[col].str.strip()` (or `.dt.date.astype(str)`), restore with `tb[col] = tb[col].copy_metadata(tb[other_col])` or save `tb[col].metadata` before and reassign after.
- **`pr.merge` / `pr.concat` require Tables on every side** — if you're merging in a synthetic axis (`pd.date_range`, etc.), wrap it as `Table(df.to_frame())` first, otherwise you get `AttributeError: 'DataFrame'/'Series' object has no attribute 'all_columns'`.
- **No `index.map()`** to pull columns from another table — loses origins. Use `tb.join(other[["col"]], how="left")`
- **`snap.read_csv/json/excel/feather/...`** — prefer over manual file reading + `pd.DataFrame`
- **Don't re-wrap `snap.read_csv()` output in `Table(...)`** — the Table constructor with a plain DataFrame argument drops column-level origins. Mutate the returned Table directly: `tb = snap.read_csv(); tb = tb.dropna(...)`
- **`paths.regions.harmonize_names(tb, country_col=..., countries_file=...)`** — current harmonization API (replaces `geo.harmonize_countries`)
- **Attach population with `paths.regions.add_population(tb, population_col=...)`** — never read population columns directly (`historical.population_historical`, `population_original.population`). Only the `population` table's `population` column carries the single collapsed *"Various sources"* origin; the other tables carry disaggregated HYDE/Gapminder/UN WPP origins that then leak onto your indicators. Add `data://garden/demography/<version>/population` as a dep.
- **`Table.format(keys, short_name=paths.short_name)`** sets the index, sorts, verifies integrity, and sets `short_name` in one call — use it in data steps. It takes an explicit key list; if `keys` is None (default) it uses `country` + `year`, but it is not limited to those. For a year-only table use `tb.format(["year"], short_name=paths.short_name)`. Don't hand-roll `set_index` + `tb.metadata.short_name`.
- **`*.meta.yml`**: the `dataset:` block carries only `update_period_days` and `owners` — everything else is inherited from origin. Always make sure `owners` is set (new dataset: the user; update: append the user if missing) — first entry is the accountable owner; canonical names per the `schemas/dataset-schema.json` enum, resolved via `etl.owners.resolve_owner`.
- **`grapher_config`: omit `$schema:`** — pinning a specific schema version ages badly. The default in `etl/config.py:DEFAULT_GRAPHER_SCHEMA` is applied automatically by `_validate_grapher_config`.

### Performance

- **Meadow: use categoricals** — low-cardinality string columns (`country`, `variant`, `sex`, `age`) should be `.astype("category")` before `.format()`. Dramatically reduces feather size and read time.
- **Garden: `safe_types=False`** — for large tables (>1M rows), use `ds.read("table", safe_types=False)` to preserve categoricals and avoid expensive type conversions.
- **Inspect feather schema** — use `pyarrow.feather.read_table(path).schema` to check if columns are `large_string` (bad) vs `dictionary` (good).

### Standard Garden Step
```python
from etl.helpers import PathFinder

paths = PathFinder(__file__)

def run() -> None:
    ds_input = paths.load_dataset("input_dataset")
    tb = ds_input["table_name"].reset_index()
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)
    tb = tb.format(short_name=paths.short_name)
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
```

### Correcting known upstream data errors (`.corrections.yml`)

For a known *source* error we patch locally until the provider fixes it, don't inline `.loc[...]`/`.drop(...)` — declare it in a `<short_name>.corrections.yml` next to the step and apply with `tb = paths.apply_corrections(tb)`. See `etl/data_corrections.py` for the format; `etl corrections -o /tmp/c.html --charts` inventories and visualises them all. For enumerated provider point-errors only — systematic recoding *rules* and aggregation stay in step code.

### Ad-hoc Data Exploration
```python
from etl.snapshot import Snapshot
snap = Snapshot("namespace/version/file.csv")
tb = snap.read_csv()
```

### Catalog System

Built on **owid.catalog** library:
- **Dataset**: Container for multiple tables with shared metadata
- **Table**: pandas.DataFrame subclass with rich metadata per column
- **Variable**: pandas.Series subclass with variable-specific metadata
- Content-based checksums for change detection
- Multiple formats (feather, parquet, csv) with automatic schema validation


### HTTP calls to OWID infra

When internal code hits an OWID host (catalog, grapher, `files.ourworldindata.org`, `search.owid.io`, Datasette, admin API, etc.), use the shared session from `etl.http` instead of bare `requests` / `httpx` / `pd.read_*(url)`. It pre-sets a `User-Agent: owid-etl/...` header so our traffic is distinguishable in CDN logs.

```python
from etl.http import session as http_session  # for requests
from etl.http import HEADERS                   # for httpx.AsyncClient(headers=HEADERS)
from etl.http import STORAGE_OPTIONS           # for pd.read_csv(url, storage_options=STORAGE_OPTIONS)
```

Don't tag calls to third-party hosts (GitHub, Notion, Slack, source-data providers in `snapshots/`, etc.) — they should keep the default UA.

### YAML Editing (preserve comments)
```python
from etl.files import ruamel_load, ruamel_dump
data = ruamel_load(file_path)
data['key'] = new_value
with open(file_path, 'w') as f:
    f.write(ruamel_dump(data))
```

### Writing origin / metadata fields

- **Consult the reference** — before writing `.dvc` `origin` or `.meta.yml` fields, look the field up in `schemas/definitions.json` (rendered at the [metadata reference](https://docs.owid.io/projects/etl/architecture/metadata/reference/)) and follow its `guidelines`. They're detailed and per-field: requirement level, good/bad examples, and when to omit optional fields (`title_snapshot`, `description_snapshot`, `attribution` all default to null / auto-generated). Each field has one job — don't fold content that belongs in one field into another.
- **License goes under `origin`, not at the top level.** In a snapshot `.dvc`, the license is `meta.origin.license` (4-space, inside `origin`) — never the top-level `meta.license` (2-space). Both parse (they differ only by indentation), but the top-level form is a deprecated `SnapshotMeta` field that doesn't travel with the origin, so the license is dropped from Grapher's per-origin metadata (which matters for multi-origin datasets). The wizard cookiecutter already does this correctly; a schema `not`-constraint + `test_snapshot_license_lives_under_origin` enforce it. Each origin in a multi-origin `.dvc` needs its own `license`.
- **`license.url` points to the producer's own license statement** — the page or PDF download link where the producer states the terms (often the same landing page as `url_main`). Never a `creativecommons.org` deed or other generic license page. If the producer states no license anywhere, leave `url` empty (don't fall back to the dataset's main page).
- **American spelling always**.

### Description fields: `.dvc` vs garden `description_processing`

Two different descriptions, two different jobs. Don't mix them:

- **`.dvc` `meta.origin.description`** describes what **the producer** publishes — the source's schema, calendar, structure, and any context the producer themselves gives about the data.
- **Garden `description_processing`** describes what **OWID** does to that data — aggregation, relabeling, deduplication, derivations, date conversion.

If the same sentence could fit in both, it belongs in garden — not in `.dvc`. Don't repeat producer-side facts in `description_processing`, and don't put OWID-side transformations in the `.dvc`.

### Origin metadata fields: follow the metadata reference

When writing or editing `.dvc` origin fields, follow the ETL metadata reference — `schemas/definitions.json` in this repo (rendered at https://docs.owid.io/projects/etl/architecture/metadata/reference/). It defines, per field, the requirement level, formatting guidelines, and good/bad examples. Don't infer field usage by copying other `.dvc` files — they may use optional fields for reasons that don't apply to your snapshot.

Mistakes the reference already covers but that keep happening:

- **`title_snapshot` / `description_snapshot`**: default to omitting both. Only use them when several snapshots are created from the same data product and need disambiguation. If a data product maps to a single snapshot — even one that is a subset of the product — describe that subset in `description` instead.
- **`attribution`**: omit — grapher builds `producer (year)` automatically. Only set it when that automatic format is genuinely uninformative (e.g. a well-known data product title should be cited alongside the producer).
- **`citation_full`**: follow the producer's requested citation, but with appropriate minor edits: don't fold other metadata fields into it — e.g. license text like "Licence: CC BY-NC-SA 3.0 IGO" belongs in `license`, not in the citation.
- **American spelling, always** — even when the producer's own text uses British spelling (adapting it is one of the "minor edits" the reference allows).

## Sanity checks

Silent data corruption is one of the easier bugs to miss. A step can run cleanly, pass type checks, and ship to staging while producing wrong numbers — and by the time someone notices on a chart, the bad data may already be live. Sanity checks are how we catch that at build time instead.

**Write them. Make them strict.** Every garden step that does anything more than a straight load-and-format should assert its assumptions about both the input it received and the output it produced. The bar isn't "would I notice this on a chart" — it's "could this go wrong, and if it did, would the step still finish without complaining?". If yes, that's a check.

### Where they go

- **Garden** is the main home. The input tables, the transformations, and the output table are all in one place, and that's where business logic lives. Put one `sanity_check_inputs(...)` right after loading meadow tables (before any transform), and one `sanity_check_outputs(...)` right before `paths.create_dataset(...)`. Call both from `run()`. See [`rff/2026-06-10/emissions_weighted_carbon_price.py`](etl/steps/data/garden/rff/2026-06-10/emissions_weighted_carbon_price.py) for a clean, recent example.
- **Snapshot** usually doesn't need checks — most snapshots just download and write. But when the snapshot itself does non-trivial parsing (PDF tables, custom binary formats, scraping), add integrity checks right before `snap.create_snapshot(...)`. If the parser can produce silently-wrong rows, the snapshot is the only place to catch it before the bad rows leak into meadow.
- **Meadow** should stay light. If you find yourself wanting checks there, it usually means the logic belongs in garden instead.

### How they look

Use plain `assert` with a clear error message. Hard-fail is the default — let the step crash loudly. Save `log.warning(...)` for checks that flag suspicious patterns the maintainer should *review* but that aren't always wrong (e.g., a country dropping to zero in the latest year — could be a real signal, could be incomplete reporting).

```python
def sanity_check_inputs(tb_economy: Table, tb_coverage: Table) -> None:
    assert set(tb_economy["jurisdiction"]) == set(tb_coverage["jurisdiction"]), "Jurisdictions don't match between the two input tables."
    assert not tb_economy.duplicated(subset=["jurisdiction", "year"]).any(), "Duplicate (jurisdiction, year) rows in economy table."
    price_cols = [c for c in tb_economy.columns if c not in ["jurisdiction", "year"]]
    assert tb_economy[price_cols].min().min() >= 0, "Negative price found — source error or unit mistake."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    # Soft signal — surface for review, don't fail.
    dropped = sorted(set(tb[tb["year"] == tb["year"].max()].query("value == 0")["country"]))
    if dropped:
        log.warning(f"Countries that dropped to zero in the latest year: {dropped}")
```

### Categories worth checking

Pick whichever apply to your step. Don't write all of these by default — write the ones that would catch a real failure mode for this dataset.

- **Shape / schema invariants.** Set-equality on the columns, categories, subcategories, variables, or any other identifier list you expect to be stable across versions. (`set(tb["category"]) == set(EXPECTED_CATEGORIES)`.) Catches schema changes at the source.
- **Key uniqueness.** `assert not tb.duplicated(subset=[...]).any()` on whatever key columns the table is supposed to be unique on. Catches accidental row duplication from joins.
- **Value ranges.** Non-negative where it should be, within plausible bounds where you know them, no NaN where you don't expect any. Catches unit mistakes and parser drift.
- **Cross-table / cross-source agreement.** If two source tables both report the same key, their overlap should agree to the dollar (or within a documented tolerance). Catches misaligned extractions.
- **Sum reconciles with published total.** If the source publishes both the components and the total, assert that summing the components equals the total within tolerance. See [`emissions/2026-02-11/emissions_by_custom_sector.py`](etl/steps/data/garden/emissions/2026-02-11/emissions_by_custom_sector.py:172) for an example. Catches row-shift extraction bugs and unit-mismatches.
- **No silent drops.** If you filter, dedupe, or aggregate, assert that the row count or aggregate matches what you'd expect. (`assert len(tb) == n_expected`.) Catches transforms that quietly lose data.
- **Coverage didn't shrink.** When updating a dataset version, check that you still have at least as many countries / quarters / categories as the previous version — a sudden drop is usually a parsing regression, not a real change.
- **Magnitude matches the previous version.** When rewriting or updating a step, compare output values against the previous live version — a silent unit regression (e.g. a dropped ×1e6 conversion) passes every schema check and ships wrong-by-a-million values. Source columns whose headers carry unit markers (`(mils)`, `(000)`, `%`) demand an explicit conversion in garden **plus** a magnitude assert (`assert 1e12 < tb[col].max() < 1e14`), so the conversion can't be lost in a future rewrite.

### When checks fail

Don't suppress the assertion to get the step to pass. Treat the failure as the signal it is: investigate, then either fix the upstream logic or — if the source genuinely changed — update the assertion (and document why in a `# NOTE:` comment near the check).

## Querying MySQL

### Quick queries (staging)
```bash
make query SQL="SELECT COUNT(*) FROM variables WHERE catalogPath IS NULL"
```
Automatically connects to `staging-site-{branch}` based on current git branch.

### Python (for more control)
```python
from etl.config import OWID_ENV
df = OWID_ENV.read_sql("SELECT * FROM datasets LIMIT 10")
```

**Prefer Python when the SQL contains `%` (LIKE patterns, JSON_EXTRACT paths) or single-quoted strings — `make query` re-interprets those via shell + make and breaks unpredictably.** Use `params={...}` for `%`/quoted values to dodge pymysql's own `%`-format-string parsing.

**`OWID_ENV` targets your local dev DB even when you're on a branch.** To query the branch's staging DB from Python, use `OWIDEnv.from_staging('<branch>')` (`from etl.config import OWIDEnv`) — e.g. `OWIDEnv.from_staging('my-branch').read_sql(...)`. Also note `make query` shells out to the `mysql` CLI, which may not be installed; if it errors with `mysql: command not found`, use the Python `from_staging(...).read_sql(...)` path instead.

### Production queries via public Datasette

When you need production data (which charts use an indicator, chart configs, gdoc links) and local/prod MySQL isn't reachable, query the public Datasette over HTTP:

```bash
curl -s "https://datasette-public.owid.io/owid.json?sql=<url-encoded SQL>"
```

`chart_dimensions` + `charts` + `chart_configs` answer "which charts use variable X"; `narrative_charts` and `posts_gdocs_links` cover derived charts and article references — together they answer the full "what does this dataset affect?" question when assessing the blast radius of a data fix.

## Verifying charts on staging

- **Indicator data/metadata API**: `https://api-staging.owid.io/staging-site-<branch>/v1/indicators/<id>.data.json` (and `.metadata.json`). The path prefix is `staging-site-<branch>`, **not** the bare branch name — a wrong prefix silently serves data from a different environment instead of 404ing, which looks exactly like "my fix didn't take". When in doubt, grep the staging chart page (`http://staging-site-<branch>/grapher/<slug>`) for `data.json` to get the exact URLs it loads.
- **Rendered chart without a browser**: `http://staging-site-<branch>/grapher/<slug>.svg` returns a server-side render — grep it for axis labels / entity names to verify a fix end-to-end (e.g. `grep -oE '>[0-9]+ [a-z]+[^<]*<'` to read the y-axis ticks).

## Additional Tools

Get `--help` for details on any command.

### Fast File Searching

Use `rg` (ripgrep) instead of `find -exec grep` - it's ~100x faster:
```bash
rg -l "pattern" -g "*.py" -g "!.venv"
```

## Package Management

Use `uv` (not pip):
```bash
uv add package_name
uv remove package_name
```

**Never run bare `uv sync`** — it prunes optional deps the repo needs (streamlit, etc.) and breaks `etl`/`etl pr`. The full environment is `uv sync --all-extras --group dev` (what `make .venv` runs); use that to install or repair the venv.

## VSCode Extensions

Extensions live in `vscode_extensions/<name>/`. After **every** code change, you must compile, package, and install — just compiling is NOT enough:

```bash
cd vscode_extensions/<name>
npm run compile
npx @vscode/vsce package --out install/<name>-<version>.vsix
code --install-extension install/<name>-<version>.vsix --force
```

Then tell the user to reload: `Cmd+Shift+P` → "Developer: Reload Window".

## GitHub Actions

When editing `.github/workflows/**` or `.github/actions/**`, follow the SHA-pinning rule imported below:

@.github/instructions/github-actions.instructions.md

## Extended Documentation

See `.claude/docs/` for:
- `debugging.md` - Data quality debugging approach
- `pipeline-stages.md` - Pipeline architecture details

## Individual Preferences

- @~/.claude/instructions/etl.md
