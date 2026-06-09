# Agent Guide

Our World in Data's ETL system - a content-addressable data pipeline with DAG-based execution.

## Critical Rules

- **Always use `.venv/bin/`** for all Python commands (`etl`, `python`, `pytest`)
- **Never mask problems** - no empty tables, no commented-out code, no silent exceptions
- **Trace issues upstream**: snapshot → meadow → garden → grapher
- **`dag/archive/*.yml` is a generated record** — it is reconstructed from git history by `etl archive-dag`, so never hand-edit it. It lists steps that were once active (with the commit where they were last active) purely for recovery; to bring one back, `git checkout` that commit.
- **Never push/commit** unless explicitly told to
- **Ask the user** if unsure - don't guess
- **Always run `make check` before committing**
- If not told otherwise, save outputs to `ai/` directory.
- **Notebooks**: Always create AND execute immediately using `uv run jupyter nbconvert --to notebook --execute --inplace <path>`
- **Skills**: When creating new skills in `.claude/skills/`, always include `metadata: { internal: true }` in the SKILL.md frontmatter unless the user explicitly asks for the skill to be public. This prevents external skill indexes from crawling and listing our internal skills.

## Team

When generating user-facing prose (PR descriptions, Slack messages, PR comments, review responses, etc.):

1. **Attribute the work** with a single italicized blockquote at the very top of the PR body, and as the opening line of any standalone Slack draft or long PR comment you generate:

   ```
   > _Written by Claude Code — @<handle> at the wheel._
   ```

   Use the handle of the human directing the work (usually the current git user; fall back to asking if ambiguous). Skip the disclosure on tiny mechanical comments (e.g. a one-line `@codex review` ping) — it's meant for substantive prose.

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

## Pipeline Overview

**snapshot** → **meadow** → **garden** → **grapher** → **export**

| Stage | Location | Purpose |
|-------|----------|---------|
| snapshot | `snapshots/` | DVC-tracked raw data |
| meadow | `etl/steps/data/meadow/` | Basic cleaning |
| garden | `etl/steps/data/garden/` | Business logic, harmonization |
| grapher | `etl/steps/data/grapher/` | MySQL ingestion |

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
- **Avoid `--force`** — `etlr` has built-in change detection and re-runs steps whose **code, dag entries, or data** changed. Editing a step's `.py`/`.yml` or its dag dependency line is enough to trigger a rebuild — don't add `--force`. Reserve `--force --only` for the narrow case where nothing in the repo changed but you still need to re-run (e.g., upstream data was patched out-of-band). Never use `--force` alone.
- **`--only` requires deps on disk.** It skips dep resolution and won't download missing deps — even with `PREFER_DOWNLOAD=1`. If you hit a `FileNotFoundError` on a dep's `index.json`, drop `--only` and let etlr resolve the chain.
- **`PREFER_DOWNLOAD=1`** — Download already-built datasets from the OWID catalog instead of recomputing locally. Useful when verifying a downstream step still works after a dag edit (the upstream deps get fetched, not rebuilt). Doesn't help if you've edited the dataset's own code.
- For `grapher://` steps, always add `--grapher` flag
- Some steps support **`SUBSET`** env var for fast dev iterations: `SUBSET='France,Germany' .venv/bin/etlr namespace/version/dataset --private`

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
- **`Table.format()`** needs both `country` and `year`. For year-less tables: `set_index("country")` + set `tb.metadata.short_name`
- **`*.meta.yml`**: omit `dataset:` block — inherited from origin. Only define `tables:` → `variables:`
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

## VSCode Extensions

Extensions live in `vscode_extensions/<name>/`. After **every** code change, you must compile, package, and install — just compiling is NOT enough:

```bash
cd vscode_extensions/<name>
npm run compile
npx @vscode/vsce package --out install/<name>-<version>.vsix
code --install-extension install/<name>-<version>.vsix --force
```

Then tell the user to reload: `Cmd+Shift+P` → "Developer: Reload Window".

## Extended Documentation

See `.claude/docs/` for:
- `debugging.md` - Data quality debugging approach
- `pipeline-stages.md` - Pipeline architecture details

## Individual Preferences

- @~/.claude/instructions/etl.md
