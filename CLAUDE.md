# Agent Guide

Our World in Data's ETL system - a content-addressable data pipeline with DAG-based execution.

## Critical Rules

- **Always use `.venv/bin/`** for all Python commands (`etl`, `python`, `pytest`)
- **Never mask problems** - no empty tables, no commented-out code, no silent exceptions
- **Trace issues upstream**: snapshot → meadow → garden → grapher
- **`dag/archive/*.yml` is a generated record** — it is reconstructed from git history by `etl archive-dag`, so never hand-edit it. It lists steps that were once active (with the commit where they were last active) purely for recovery; to bring one back, `git checkout` that commit.
- **Never delete a step without archiving it.** Removing or superseding an active step (new version, retirement, replacement) obligates you to archive it — deleting the files alone is a bug. Procedure: remove its `dag/*.yml` entry and delete its files → **commit** → run `etl archive-dag` (it reads *committed* history, so the removal must be committed first) → commit the regenerated `dag/archive/*.yml`. If `archive-dag` sweeps in unrelated steps others left un-archived, `git checkout` those files to keep your PR scoped (never hand-edit the archive). For a migrated/backport dataset, also delete its now-orphaned `snapshots/backport/latest/dataset_<id>_*` mirror files.
- **Ask the user** if unsure - don't guess
- **Say what's left open.** Multi-step work rarely ends with everything closed, so close the report (and the PR body) by saying what's still pending, who owns it, and what nobody checked. No fixed format — `.claude/docs/open-items.md` lists what tends to get dropped.
- **Always run `make check` before committing** (format, lint, typecheck on changed files). Run the test suite with `make unittest` (or `make test` for checks + tests + version-tracker); `lib/*` packages have their own venv and Makefile — run it from inside that directory.
- If not told otherwise, save outputs to `ai/` directory.
- **Notebooks**: Always create AND execute immediately using `uv run jupyter nbconvert --to notebook --execute --inplace <path>`
- **Skills**: When creating new skills in `.claude/skills/`, always include `metadata: { internal: true }` in the SKILL.md frontmatter unless the user explicitly asks for the skill to be public. This prevents external skill indexes from crawling and listing our internal skills.

## Start from a skill

Most recurring work here has a skill that runs it end to end. Reach for it **before** hand-rolling from the sections below — those document the underlying mechanics (`etls`, `etlr`, `etl pr`) that the skills already orchestrate, not a procedure to follow in parallel with one. Full descriptions live in `.claude/skills/`; this is just the entry-point index.

| Task | Skill |
|------|-------|
| Refresh an existing dataset to a new version | `/update-dataset` |
| Brand-new dataset from a file or link the user provides | `/create-dataset` |
| Add a new snapshot (`.dvc`, plus a script only if needed) | `/create-snapshot` |
| Scaffold meadow/garden/grapher steps for a snapshot that already exists | `/create-etl-steps` — the primitive `/create-dataset` calls; don't run it standalone unless scaffolding really is all you need |
| Bring a legacy (no-catalogPath) dataset into ETL | `/migrate-dataset` |
| Change user-facing chart/indicator text — title, subtitle, footnote, units, `description_short`, WYSK/`description_key`, entity selection | `/edit-faust-metadata` |
| Check that text against the Writing and Style Guide | `/check-metadata-style` |
| Build a multi-dim indicator, or an explorer | `/create-multidim`, `/create-explorer` |
| Review a dataset-update PR | `/review-data-pr` |
| Announce a finished update | `/data-updates-comms` |

One that's easy to skip and shouldn't be: `/edit-faust-metadata` owns **every** user-facing-text edit — it routes each field to the right layer (garden `.meta.yml` vs MDim yaml vs chart config on staging) and reports the blast radius on other charts before touching shared metadata.

## Team

Everything you post to GitHub or Slack goes out under a **human's identity**. Any text you author and post that a reader could take for the human's own words **must** carry the attribution line below. This is mandatory — not a judgment call about whether the comment is "worth it." (An agent posting under its own bot account, e.g. `chatgpt-codex-connector[bot]`, skips the line: the platform already attributes the content.)

1. **Attribute the work.** Put this blockquote as the *first line* of the content:

```
> _Written by <assistant> <model name> — @<handle> at the wheel._
```

Replace `<assistant>` with the product actually generating the content ("Claude", "Codex", "Copilot", ...) and `<model name>` with the human-readable name of its underlying model (e.g. "Fable 5", "Opus 4.8", "GPT-5.6"). For Claude, always use a model name, never the literal string "Code". Keeping the product prefix makes the attribution recognizable even to readers unfamiliar with individual model names.

It applies to **every** surface, **every** time you post:
- PR descriptions / bodies
- PR issue-level comments
- **Inline review comments _and_ replies to review comments** (e.g. answering Codex / Copilot / a reviewer)
- Standalone Slack messages or drafts

Use the handle of the human directing the work (usually the current git user; ask if ambiguous).

Besides the bot-account case above, **the only exception** is a comment that is a bare mechanical token with *no prose* — a lone `@codex review` ping or a 👍. The moment your comment contains a sentence of explanation, it needs the line. When in doubt, include it.

2. **Use exact handles** from the list below when tagging colleagues. Don't guess — a wrong tag pings a real person. If a name isn't on this list, write the plain name (e.g. "Bastian") instead of `@`-tagging, and ask the user for the handle.

Max Roser                @maxroser
Esteban Ortiz-Ospina     @eoo-owid
Edouard Mathieu          @edomt
Joe Hasell               @JoeHasell
Hannah Ritchie           @HannahRitchie
Daniel Bachler           @danyx23
Fiona Spooner            @spoonerf
Tuna Acisu               @antea04
Pablo Arriagada          @paarriagadap
Bastian Herre            @bastianherre
Bertha Rohenkohl         @bertharc
Charlie Giattino         @CGiattino
Pablo Rosado             @pabloarosado
Lucas Rodés-Guirao       @lucasrodes
Matthieu Bergel          @mlbrgl
Marcel Gerber            @marcelgerber
Sophia Mersmann          @sophiamersmann
Martin Račák             @rakyi
Ike Saunders             @ikesau
Mojmír Vinkler           @Marigold
Bobbie Macdonald         @bnjmacdonald
Marwa Boukarim           @mrwbkrm
Natalie Reynolds-Garcia  @natreygar
Angela Wenham            @angelawenham
Valerie Rogers Muigai    @ValRMuigai

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
| export | `etl/steps/export/` | Explorers, collections, APIs |

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
.venv/bin/etlr export://.../dataset --export             # Run an export:// step (mdim, explorer, static_viz, ...)
.venv/bin/etlr namespace/version/dataset --dry-run      # Preview
.venv/bin/etlr namespace/version/dataset --force --only # Force re-run
```

Key flags: `--grapher/-g` (upload), `--export` (required for any `export://...` step — mdims, explorers, static viz; omitting it makes `etlr` report "No steps matched" even though the step is in the DAG), `--dry-run` (preview), `--force/-f` (re-run), `--only/-o` (no deps), `--private` (always use)

**"The step completed" is not "the data is right".** After running a step for
someone, report what came out of it: row count, year range, entities, and a few
values from the latest year, plus whether anything changed against the published
catalog. `✅ No differences found` is itself a result worth reporting.

```bash
.venv/bin/etl diff REMOTE data/ --include <dataset> --verbose
```

### Running Snapshot Steps

```bash
.venv/bin/etls namespace/version/dataset               # Download & upload snapshot
.venv/bin/etls namespace/version/dataset --skip-upload  # Download only
```

**Important:**
- **Snapshot scripts need no `__main__` guard and no `click` decorators** — the `etls` CLI imports the module and calls its `run()` function itself, so don't add `if __name__ == "__main__":` boilerplate or `@click.command()` / `@click.option(...)`. New scripts should match the shape the wizard's cookiecutter emits: a plain `def run(upload: bool = True) -> None:`. Most existing scripts still carry both — they keep working, because `etl/snapshot_command.py` also accepts a click command — but don't copy them.
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

**Consult the reference first.** Before writing `.dvc` `origin` or `.meta.yml` fields, look the field up in `schemas/definitions.json` (rendered at the [metadata reference](https://docs.owid.io/projects/etl/architecture/metadata/reference/)) and follow its `guidelines`. They're detailed and per-field: requirement level, good/bad examples, and when to omit optional fields. Each field has one job — don't fold content that belongs in one field into another. Don't infer field usage by copying other `.dvc` files; they may use optional fields for reasons that don't apply to your snapshot.

Mistakes the reference already covers but that keep happening:

- **License goes under `origin`, not at the top level.** In a snapshot `.dvc`, the license is `meta.origin.license` (4-space, inside `origin`) — never the top-level `meta.license` (2-space). Both parse (they differ only by indentation), but the top-level form is a deprecated `SnapshotMeta` field that doesn't travel with the origin, so the license is dropped from Grapher's per-origin metadata (which matters for multi-origin datasets). The wizard cookiecutter already does this correctly; a schema `not`-constraint + `test_snapshot_license_lives_under_origin` enforce it. Each origin in a multi-origin `.dvc` needs its own `license`.
- **`license.url` points to the producer's own license statement** — the page or PDF download link where the producer states the terms (often the same landing page as `url_main`). Never a `creativecommons.org` deed or other generic license page. If the producer states no license anywhere, leave `url` empty (don't fall back to the dataset's main page).
- **`title_snapshot` / `description_snapshot`**: default to omitting both. Only use them when several snapshots are created from the same data product and need disambiguation. If a data product maps to a single snapshot — even one that is a subset of the product — describe that subset in `description` instead.
- **`attribution`**: omit — grapher builds `producer (year)` automatically. Only set it when that automatic format is genuinely uninformative (e.g. a well-known data product title should be cited alongside the producer).
- **`citation_full`**: follow the producer's requested citation, but with appropriate minor edits: don't fold other metadata fields into it — e.g. license text like "Licence: CC BY-NC-SA 3.0 IGO" belongs in `license`, not in the citation.
- **American spelling, always** — even when the producer's own text uses British spelling (adapting it is one of the "minor edits" the reference allows).

### Description fields: `.dvc` vs garden `description_processing`

Two different descriptions, two different jobs. Don't mix them:

- **`.dvc` `meta.origin.description`** describes what **the producer** publishes — the source's schema, calendar, structure, and any context the producer themselves gives about the data.
- **Garden `description_processing`** describes what **OWID** does to that data — aggregation, relabeling, deduplication, derivations, date conversion.

If the same sentence could fit in both, it belongs in garden — not in `.dvc`. Don't repeat producer-side facts in `description_processing`, and don't put OWID-side transformations in the `.dvc`.

## Sanity checks

Silent data corruption is one of the easier bugs to miss: a step can run cleanly, pass type checks, and ship wrong numbers to staging. Every garden step that does more than a straight load-and-format asserts its assumptions about its inputs and its output — and so does any snapshot that parses non-trivially (PDF tables, custom binary formats, scraping). Where they go, what they look like, and which categories are worth checking are in `.claude/rules/sanity-checks.md`, which loads on its own when you open a step or snapshot script.

## Querying MySQL, and verifying charts on staging

Use the `query-grapher-db` skill — it covers the local dev DB, a branch's staging DB, production via the public Datasette, and the staging indicator/SVG endpoints.

## Package Management

**Never run bare `uv sync`** — it prunes optional deps the repo needs (streamlit, etc.) and breaks `etl`/`etl pr`. The full environment is `uv sync --all-extras --group dev` (what `make .venv` runs); use that to install or repair the venv.

## GitHub Actions

When editing `.github/workflows/**` or `.github/actions/**`, follow the SHA-pinning rule imported below:

@.github/instructions/github-actions.instructions.md

## Extended Documentation

See `.claude/docs/` for:
- `debugging.md` - Data quality debugging approach
- `pipeline-stages.md` - Pipeline architecture details
- `cloud-sandbox.md` - Claude Code on the web: what a cloud session can and can't do
- `open-items.md` - What tends to be left open at the end of multi-step work

If you are running in a Claude Code cloud sandbox (`CLAUDE_CODE_REMOTE=true`), read
`.claude/docs/cloud-sandbox.md` **before starting work** — it covers the pre-created
branch name, spurious `uv.lock` diffs, the absence of a database, which OWID hosts
the egress proxy blocks, and how to resolve an `admin.owid.io` link you can't open.

## Individual Preferences

Personal, non-shared preferences go in `CLAUDE.local.md` at the repo root (already gitignored). Claude Code also auto-loads your own `~/.claude/CLAUDE.md`, so there is nothing to import here.
