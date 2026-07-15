---
name: check-outdated-practices
description: Check ETL step files for outdated coding patterns and offer to fix them. Use when user mentions outdated practices, legacy code patterns, modernizing steps, or wants to check code quality of ETL steps.
metadata:
  internal: true
---

# Check Outdated Practices

Scan ETL step files for outdated coding patterns and offer to fix them.

## Source of truth

The canonical list of outdated patterns, their scopes, severities, and messages is defined in the VSCode extension at `vscode_extensions/detect-outdated-practices/src/extension.ts`. **Always read this file first** to get the current patterns. Do not assume you know the patterns — the extension is the single source of truth and may be updated independently.

Each pattern in the extension has:
- `pattern`: regex to detect the outdated practice
- `message`: describes what's wrong and what the modern replacement is
- `severity`: warning level
- `scope`: which file paths the pattern applies to (e.g., `snapshots/**`, `etl/steps/data/**`)

## Scope

By default, check the files involved in the **current task** (e.g., the steps being updated). If the user provides explicit paths or asks for a broader scan, use those instead.

Accept any of:
- A step path: `etl/steps/data/garden/wb/2026-03-25/poverty_projections.py`
- A namespace/version/short_name: `wb/2026-03-25/poverty_projections`
- A glob: `etl/steps/data/garden/wb/2026-03-25/*.py`
- `all` — scan all non-archived steps (slow)

## Fix guidance

When applying fixes, keep these notes in mind:

- **`paths.regions.harmonize_names(tb)`**: `country_col`, `countries_file`, **and `excluded_countries_file`** are all inferred by default — it assumes the column is `"country"`, uses the step's `.countries.json`, and (when the file exists) the step's `.excluded_countries.json`. So a call that passes only those three defaults collapses to `paths.regions.harmonize_names(tb)`; only pass an argument when you're overriding the default. The fallbacks come from the `Regions` instance `PathFinder` builds (`self.countries_file` / `self.excluded_countries_file` in `etl/helpers.py`, defaulted inside `harmonize_names`) — `excluded_countries_file` is wired only when the `.excluded_countries.json` is present, so dropping it is behavior-preserving exactly when the file exists. Preserve any non-default kwargs (`warn_on_unused_countries`, `make_missing_countries_nan`, etc.).
- **Linting**: After fixing patterns, always run `make check` (or let the code-quality-fixer agent handle it). In particular, don't leave extra blank lines between imports — follow the project's import style (no blank lines within import groups, one blank line between standard library and third-party groups).

## Workflow

1. Read `vscode_extensions/detect-outdated-practices/src/extension.ts` to get the current pattern list
2. Identify the files to scan based on the user's scope, respecting each pattern's `scope` field
3. Grep for each pattern in the scoped files
4. Report findings as a summary table:
   ```
   | File | Issue | Line |
   |------|-------|------|
   | snapshots/wb/.../file.py | <message from extension> | 29 |
   ```
5. Ask the user: "Found N outdated patterns. Fix them?"
6. If yes, apply the modern replacements described in each pattern's `message` field and show a summary of changes
