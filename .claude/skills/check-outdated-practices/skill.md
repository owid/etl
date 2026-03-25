---
name: check-outdated-practices
description: Check ETL step files for outdated coding patterns and offer to fix them. Use when user mentions outdated practices, legacy code patterns, modernizing steps, or wants to check code quality of ETL steps.
---

# Check Outdated Practices

Scan ETL step files for outdated coding patterns and offer to fix them.

## Scope

By default, check the files involved in the **current task** (e.g., the steps being updated). If the user provides explicit paths or asks for a broader scan, use those instead.

Accept any of:
- A step path: `etl/steps/data/garden/wb/2026-03-25/poverty_projections.py`
- A namespace/version/short_name: `wb/2026-03-25/poverty_projections`
- A glob: `etl/steps/data/garden/wb/2026-03-25/*.py`
- `all` — scan all non-archived steps (slow)

## Outdated patterns to check

### 1. Snapshot scripts: `if __name__ == "__main__"` + click decorators

**Outdated:**
```python
from pathlib import Path
import click
from etl.snapshot import Snapshot

SNAPSHOT_VERSION = Path(__file__).parent.name

@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def run(path_to_file: str, upload: bool) -> None:
    snap = Snapshot(f"namespace/{SNAPSHOT_VERSION}/file.ext")
    snap.create_snapshot(filename=path_to_file, upload=upload)

if __name__ == "__main__":
    run()
```

**Modern:**
```python
from etl.helpers import PathFinder

paths = PathFinder(__file__)

def run(upload: bool = True, path_to_file: str | None = None) -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
```

Key changes:
- Use `PathFinder` instead of manual `SNAPSHOT_VERSION` / `Snapshot(...)` construction
- Use `paths.init_snapshot()` instead of `Snapshot(f"namespace/{SNAPSHOT_VERSION}/...")`
- Remove `click` decorators and `if __name__ == "__main__"` block
- Function signature: `run(upload: bool = True, path_to_file: str | None = None)`

### 2. Snapshot scripts: `snap.dvc_add()` instead of `snap.create_snapshot()`

**Outdated:**
```python
snap.dvc_add(upload=upload)
```

**Modern:**
```python
snap.create_snapshot(filename=path_to_file, upload=upload)
# or for URL-based snapshots:
snap.create_snapshot(upload=upload)
```

### 3. Garden steps: `geo.harmonize_countries()` instead of `paths.regions.harmonize_names()`

**Outdated:**
```python
from etl.data_helpers import geo
tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
```

**Modern:**
```python
tb = paths.regions.harmonize_names(tb)
```

Note: `country_col` and `countries_file` are inferred by default — only pass them if you need to override.

### 4. Meadow/Garden steps: `if __name__ == "__main__"` block

Step files (meadow, garden, grapher) should only define a `run()` function. They should not have `if __name__ == "__main__"` blocks — the ETL runner calls `run()` directly.

## Workflow

1. Identify the files to scan based on the scope
2. For each file, check for all outdated patterns listed above
3. Report findings as a summary table:
   ```
   | File | Issue | Line |
   |------|-------|------|
   | snapshots/wb/2026-03-25/poverty_projections.py | `if __name__` + click pattern | 29 |
   ```
4. Ask the user: "Found N outdated patterns. Fix them?"
5. If yes, apply fixes and show a summary of changes
