---
argument-hint: <namespace>/<old-version>/<name> [branch]
description: End-to-end dataset update workflow using project subagents with progress tracking and a mandatory checkpoint. New version is set to today's date automatically.
---

# Update dataset (PR → snapshot → steps → grapher)

Use this command to run a complete dataset update with Claude Code subagents, keep a live progress checklist, and pause for approval at the checkpoint before committing changes.

## Context probes

- Current branch: !`git branch --show-current`

## Inputs

Primary syntax (preferred):

- `<namespace>/<old-version>/<name>`
   - The new version will be computed as today's date (YYYY-MM-DD).
   - Example: `irena/2023-11-15/renewable_power_generation_costs`

Also supported (for flexibility):

1) Catalog URI
   - `data://<channel>/<namespace>/<yyyy-mm-dd>/<name>`
   - Example: `data://snapshot/irena/2024-11-15/renewable_power_generation_costs`

2) Key-value form
   - channel=<snapshot|meadow|garden|grapher> namespace=<ns> version=<yyyy-mm-dd> short_name=<name> [dataset=<name>]

3) Minimal
   - short_name=<short_name> channel=<meadow|garden|grapher>
   - Infer namespace/version/name from repo context when possible

Optional trailing args:
- branch: The working branch name (defaults to current branch)

Assumptions:
- All artifacts are written to `workbench/<short_name>/`.
- Persist progress to `workbench/<short_name>/progress.md` and update it after each step.

## Progress checklist (maintain, tick live, and persist to progress.md)

- [ ] Parse inputs and resolve: channel, namespace, version, short_name, old-version, branch
- [ ] Create or reuse draft PR and work branch
- [ ] Update snapshot and compare to previous version; capture summary
- [ ] Meadow step: run + fix + diff + summarize
- [ ] Garden step: run + fix + diff + summarize
- [ ] Grapher step: run + verify (skip diffs), or explicitly mark N/A
- [ ] CHECKPOINT — present consolidated summary and request approval
- [ ] If approved, commit, push, and update PR description
- [ ] Optional: run indicator upgrade on staging and persist report

Persistence:
- After ticking each item, update `workbench/<short_name>/progress.md` with the current checklist state and a timestamp.

## Workflow orchestration

1) PR and branch setup — use dataset-update-pr subagent
   - Create or reuse a draft PR and work branch.
   - Compute `new_version = TODAY (YYYY-MM-DD)`.
   - Run `etl update snapshot://<namespace>/<new_version>/<short_name> --include-usages` under this subagent’s rules.
   - Ensure branch length < 28 chars if interacting with DB-backed systems.

2) Snapshot update & compare — use snapshot-updater subagent
   - Inputs: `<namespace>/<new_version>/<short_name>` and `<old-version>`.
   - Save summary to `workbench/<short_name>/snapshot-updater.md`.
   - Do not modify the old snapshot. Use collapsible sections for detail.
   - This step is part of the approval checkpoint below.

3) Meadow step repair/verify — use step-fixer subagent
   - Invoke with `channel=meadow namespace=<ns> version=<ver> dataset=<name> short_name=<short_name>` (or the data:// form).
   - Run the step, read tracebacks if any, minimally fix code/metadata, and re-run.
   - Diff against REMOTE for a representative country and save to:
     - `workbench/<short_name>/meadow_diff_raw.txt`
     - Summarize to `workbench/<short_name>/meadow_diff.md`

4) Garden step repair/verify — use step-fixer subagent
   - Same as Meadow, but `channel=garden`.
   - Save to `workbench/<short_name>/garden_diff_raw.txt` and `garden_diff.md`.

5) Grapher step run/verify — use step-fixer subagent
   - Invoke with `channel=grapher` and add `--grapher` flag inside the agent’s run command.
   - Skip the diff step (as per agent’s notes) but verify variables/metadata integrity.

6) Indicator upgrade (optional, staging only) — use indicator-upgrader subagent
   - Inputs: `<short_name> <branch>`.
   - Resolve NEW and OLD dataset ids, map perfect variable matches, emit manual mapping TODOs.
   - Dry-run, then apply upgrade, verify no charts reference OLD dataset.
   - Persist `workbench/<short_name>/indicator_upgrade.json`.

## CHECKPOINT (mandatory user approval)

- Present a consolidated summary of key changes across snapshot, meadow, garden, and grapher.
- Ask explicitly: “Proceed? reply: yes/no”.
- Only proceed to commit/push if the user replies “yes”.
- After approval:
  - Commit changes and push.
  - Update PR description with a collapsed section containing the snapshot diff and a link to incremental changes.

## Guardrails and tips

- ⚠️ Never return empty tables or comment out logic as a workaround — fix the parsing/transformations instead.
- Column name changes: update garden processing code and metadata YAMLs (garden/grapher) to match schema changes.
- Indexing: avoid leaking index columns from `reset_index()`; format tables with `tb.format(["country", "year"])` as appropriate.
- Metadata validation errors are guidance — update YAML to add/remove variables as indicated.

## Artifacts (expected)

- `workbench/<short_name>/snapshot-updater.md`
- `workbench/<short_name>/progress.md`
- `workbench/<short_name>/meadow_diff_raw.txt` and `meadow_diff.md`
- `workbench/<short_name>/garden_diff_raw.txt` and `garden_diff.md`
- `workbench/<short_name>/indicator_upgrade.json` (if indicator-upgrader was used)

## Example usage

- Minimal catalog URI with explicit old version:
  - `/update-dataset data://snapshot/irena/2024-11-15/renewable_power_generation_costs 2023-11-15 update-irena-costs`

---

### Common issues when data structure changes

- ⚠️ SILENT FAILURES WARNING: Never return empty tables or comment code as workarounds!
- Column name changes: If columns are renamed/split (e.g., single cost → local currency + PPP), update:
  - Python code references in the garden step
  - Garden metadata YAML (e.g., `food_prices_for_nutrition.meta.yml`)
  - Grapher metadata YAML (if it exists)
- Index issues: Check for unwanted `index` columns from `reset_index()` — ensure proper indexing with `tb.format(["country", "year"])`.
- Metadata validation: Use error messages as a guide — they show exactly which variables to add/remove from YAML files.
