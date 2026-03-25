---
name: check-outdated-practices
description: Check ETL step files for outdated coding patterns and offer to fix them. Use when user mentions outdated practices, legacy code patterns, modernizing steps, or wants to check code quality of ETL steps.
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

- **`paths.regions.harmonize_names(tb)`**: `country_col` and `countries_file` are inferred by default — it assumes the column is `"country"` and uses the step's `.countries.json` file. Only pass these arguments if you need to override the defaults. Preserve extra kwargs like `warn_on_unused_countries`.

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
