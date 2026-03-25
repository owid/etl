---
name: check-outdated-practices
description: Check ETL step files for outdated coding patterns and offer to fix them. Use when user mentions outdated practices, legacy code patterns, modernizing steps, or wants to check code quality of ETL steps.
---

# Check Outdated Practices

Scan ETL step files for outdated coding patterns and offer to fix them.

## Source of truth

The canonical list of outdated patterns is defined in the VSCode extension at `vscode_extensions/detect-outdated-practices/src/extension.ts`. **Always read this file first** to get the current patterns, their scopes, and messages. Do not hardcode patterns — the extension is the single source of truth and may be updated independently.

## Scope

By default, check the files involved in the **current task** (e.g., the steps being updated). If the user provides explicit paths or asks for a broader scan, use those instead.

Accept any of:
- A step path: `etl/steps/data/garden/wb/2026-03-25/poverty_projections.py`
- A namespace/version/short_name: `wb/2026-03-25/poverty_projections`
- A glob: `etl/steps/data/garden/wb/2026-03-25/*.py`
- `all` — scan all non-archived steps (slow)

## Modern replacements

When fixing detected patterns, apply these replacements:

### Snapshot scripts: `if __name__ == "__main__"` + click decorators → PathFinder

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

### `snap.dvc_add()` → `snap.create_snapshot()`

### `geo.harmonize_countries()` → `paths.regions.harmonize_names(tb)`

`country_col` and `countries_file` are inferred by default — only pass them if you need to override. Preserve extra kwargs like `warn_on_unused_countries`.

### `paths.load_dependency()` → `paths.load_dataset()` or `paths.load_snapshot()`

### `dest_dir` → remove (use `paths.create_dataset` which doesn't need it)

## Workflow

1. Read `vscode_extensions/detect-outdated-practices/src/extension.ts` to get the current pattern list
2. Identify the files to scan based on the scope, respecting each pattern's `scope` field
3. Grep for each pattern in the scoped files
4. Report findings as a summary table:
   ```
   | File | Issue | Line |
   |------|-------|------|
   | snapshots/wb/2026-03-25/poverty_projections.py | `if __name__` block in snapshot | 29 |
   ```
5. Ask the user: "Found N outdated patterns. Fix them?"
6. If yes, apply fixes and show a summary of changes
