# Agent Guide

Our World in Data's ETL system - a content-addressable data pipeline with DAG-based execution.

## Critical Rules

- **Always use `.venv/bin/`** for all Python commands (`etl`, `python`, `pytest`)
- **Never mask problems** - no empty tables, no commented-out code, no silent exceptions
- **Trace issues upstream**: snapshot → meadow → garden → grapher
- **Never push/commit** unless explicitly told to
- **Ask the user** if unsure - don't guess
- **Always run `make check` before committing**
- If not told otherwise, save outputs to `ai/` directory.


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

**Important:**
- Never use `--force` alone - always pair with `--only`
- For `grapher://` steps, always add `--grapher` flag

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
```

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

### Standard Garden Step
```python
from etl.helpers import PathFinder
from etl.data_helpers import geo

paths = PathFinder(__file__)

def run() -> None:
    ds_input = paths.load_dataset("input_dataset")
    tb = ds_input["table_name"].reset_index()
    tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)
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


### YAML Editing (preserve comments)
```python
from etl.files import ruamel_load, ruamel_dump
data = ruamel_load(file_path)
data['key'] = new_value
with open(file_path, 'w') as f:
    f.write(ruamel_dump(data))
```

## Additional Tools

Get `--help` for details on any command.

## Package Management

Use `uv` (not pip):
```bash
uv add package_name
uv remove package_name
```

## Extended Documentation

See `.claude/docs/` for:
- `debugging.md` - Data quality debugging approach
- `performance.md` - Profiling and optimization
- `pipeline-stages.md` - Pipeline architecture details

## Individual Preferences

- @~/.claude/instructions/etl.md
