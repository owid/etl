---
name: check-outdated-practices
description: Check ETL step files for outdated coding patterns and offer to fix them. Use when user mentions outdated practices, legacy code patterns, modernizing steps, or wants to check code quality of ETL steps.
metadata:
  internal: true
  owner: paarriagadap
---

# Check Outdated Practices

Scan ETL step files for outdated coding patterns and offer to fix them.

## Source of truth

The rules live in `vscode_extensions/detect-outdated-practices/src/detector.mts`, the same module the VS Code extension uses for its editor warnings. **Run the detector; never re-implement it with grep.** Not every rule is a per-line regex: the `ds["table"]` subscript-read rule tracks which variables hold a Dataset across the file, so grepping the `OUTDATED_PATTERNS` regexes silently misses it.

```bash
node vscode_extensions/detect-outdated-practices/src/cli.mts <file-or-dir> [...]          # file:line:column: problem
node vscode_extensions/detect-outdated-practices/src/cli.mts <file-or-dir> [...] --json   # adds each rule's full fix guidance
```

It takes files or directories (recursing into `.py` files), applies each rule's path scope against the repo-relative path, and needs only Node 22.18 or later — no build step, no `npm install`. If it can't run (older Node), say so and stop rather than falling back to grep.

## Scope

By default, check the files involved in the **current task** (e.g., the steps being updated). If the user provides explicit paths or asks for a broader scan, use those instead.

Accept any of:
- A step path: `etl/steps/data/garden/wb/2026-03-25/poverty_projections.py`
- A namespace/version/short_name: `wb/2026-03-25/poverty_projections`
- A glob: `etl/steps/data/garden/wb/2026-03-25/*.py`
- `all` — pass `etl/steps/data snapshots` (a few seconds). That includes step files of retired versions still in the repo, so filter the report to active steps (`dag/*.yml`) unless the user wants everything.

## Fix guidance

When applying fixes, keep these notes in mind:

- **`paths.regions.harmonize_names(tb)`**: `country_col`, `countries_file`, **and `excluded_countries_file`** are all inferred by default — it assumes the column is `"country"`, uses the step's `.countries.json`, and (when the file exists) the step's `.excluded_countries.json`. So a call that passes only those three defaults collapses to `paths.regions.harmonize_names(tb)`; only pass an argument when you're overriding the default. The fallbacks come from the `Regions` instance `PathFinder` builds (`self.countries_file` / `self.excluded_countries_file` in `etl/helpers.py`, defaulted inside `harmonize_names`) — `excluded_countries_file` is wired only when the `.excluded_countries.json` is present, so dropping it is behavior-preserving exactly when the file exists. Preserve any non-default kwargs (`warn_on_unused_countries`, `make_missing_countries_nan`, etc.).
- **`ds["table"]` → `ds.read("table")`**: `read()` resets the index by default, so `ds["table"].reset_index()` becomes plain `ds.read("table")` — keeping the trailing `.reset_index()` adds a spurious `index` column. Where the step needs the index kept (typically a grapher step passing tables straight to `create_dataset`), use `ds.read("table", reset_index=False)`. Keep any `safe_types=False` the step already relies on for large tables. After the swap, rebuild the step and confirm the output tables keep their index and shape.
- **Linting**: After fixing patterns, always run `make check` (or let the code-quality-fixer agent handle it). In particular, don't leave extra blank lines between imports — follow the project's import style (no blank lines within import groups, one blank line between standard library and third-party groups).

## Workflow

1. Identify the files to scan based on the user's scope (the detector applies each rule's path scope itself)
2. Run `node vscode_extensions/detect-outdated-practices/src/cli.mts <paths> --json` on them
3. Report findings as a summary table, using each finding's `message` for the issue:
   ```
   | File | Issue | Line |
   |------|-------|------|
   | snapshots/wb/.../file.py | <message from extension> | 29 |
   ```
4. Ask the user: "Found N outdated patterns. Fix them?"
5. If yes, apply the modern replacements described in each finding's `message` and show a summary of changes. Re-run the detector on the edited files to confirm they come back clean.
