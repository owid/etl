---
name: dataset-updater
description: End-to-end dataset update workflow with progress tracking and mandatory checkpoints after every step. Handles ETL update, PR creation, snapshot comparison, meadow/garden/grapher step execution and fixes, and optional indicator upgrade. Use when user requests to "update dataset", "run dataset update", "refresh dataset", or update any dataset specified as namespace/version/name format. New version is automatically set to today's date.
---

# Dataset Updater

Complete workflow for updating an OWID dataset from an old version to a new version. This skill orchestrates the entire pipeline: ETL update → PR creation → snapshot update → meadow → garden → grapher → indicator upgrade.

## When to Use This Skill

Use this skill when the user requests:
- "Update the [dataset name] dataset"
- "Refresh [dataset name] to the latest version"
- "Run a dataset update for [namespace]/[version]/[name]"
- Any variation requesting a complete dataset update workflow

## Input Format

Required: `<namespace>/<old_version>/<short_name>`

Optional: `[branch]` - The working branch name (defaults to current branch)

Examples:
- `energy/2023-10-01/electricity_mix`
- `biodiversity/2025-04-07/cherry_blossom update-cherry`

## Core Workflow Principles

### Progress Tracking
- Maintain a live checklist throughout the workflow
- Persist progress to `workbench/<short_name>/progress.md` after each step
- Update with timestamps after completing each item

### CHECKPOINT System
After completing EACH numbered workflow step (1-7):
1. Present a concise summary of what changed, key diffs/issues resolved, and what the next step will do
2. Ask exactly: "Proceed? reply: yes/no"
3. Only continue if user replies exactly "yes" (case-insensitive)
4. Any other reply = no; stop and wait for clarification
5. On approval:
   - Update progress checklist (tick completed item)
   - Write `workbench/<short_name>/progress.md` with timestamp
   - Commit related changes if any, then push
   - Update PR description: add collapsed section titled with step name containing the summary

**NEVER chain multiple steps inside a single approval.**

### Artifact Storage
All outputs write to: `workbench/<short_name>/`

Expected artifacts:
- `progress.md` - Live progress checklist
- `snapshot-runner.md` - Snapshot comparison summary
- `meadow_diff_raw.txt` and `meadow_diff.md` - Meadow diffs
- `garden_diff_raw.txt` and `garden_diff.md` - Garden diffs
- `indicator_upgrade.json` - Indicator upgrade report (if executed)

## Initial Setup (Step 0)

### Parse Inputs and Resolve Variables

Extract from input:
- `namespace` - Dataset namespace (e.g., "energy")
- `old_version` - Current version date (e.g., "2023-10-01")
- `short_name` - Dataset short name (e.g., "electricity_mix")
- `branch` - Working branch (from arg or current branch)

Calculate:
- `new_version` - Today's date in UTC: `date -u +"%Y-%m-%d"`
- `channel` - Typically "garden" (infer from context)

### Check for Existing Update

Run: `ls workbench/<short_name>/progress.md 2>/dev/null`

If exists: Ask user if continuing existing update or starting fresh

If starting fresh:
- Delete entire `workbench/<short_name>` directory
- Create fresh `workbench/<short_name>` directory

If continuing: Keep existing workbench and resume from last uncompleted item

### Initialize Progress Checklist

Create `workbench/<short_name>/progress.md` with:

```markdown
# Dataset Update Progress: <namespace>/<short_name>

Started: <timestamp>

- [ ] Parse inputs and resolve variables
- [ ] Clean workbench directory
- [ ] Run ETL update workflow
- [ ] Create or reuse draft PR and work branch
- [ ] Update snapshot and compare to previous version
- [ ] Meadow step: run + fix + diff + summarize
- [ ] Garden step: run + fix + diff + summarize
- [ ] Grapher step: run + verify (skip diffs)
- [ ] CHECKPOINT — consolidated summary and approval
- [ ] Commit, push, and update PR description
- [ ] Optional: indicator upgrade on staging
```

## Workflow Step 1: ETL Update

Run the `etl update` command to prepare the dataset for the new version.

### Understand the Interface

Run: `.venv/bin/etl update --help`

Review available flags and options.

### Construct the Command

Typical format:
```bash
.venv/bin/etl update "snapshot://<namespace>/<old_version>/<short_name>*" --include-usages --dry-run
```

Adapt flags as needed based on the help output.

### Execute Dry Run

Run the command with `--dry-run` flag and summarize:
- What files will be updated
- What new versions will be created
- Any warnings or issues flagged

### Seek User Approval

Ask: "The dry run shows [summary]. Proceed with the real update? Reply: yes/no"

Wait for explicit "yes" response.

### Execute Real Update

Remove `--dry-run` flag and execute:
```bash
.venv/bin/etl update "snapshot://<namespace>/<old_version>/<short_name>*" --include-usages
```

Report:
- Success or failure clearly
- Files modified
- Any follow-up actions needed

**CHECKPOINT:** Summarize ETL update results and get approval before proceeding.

## Workflow Step 2: PR Creation

Create or reuse a draft pull request for the dataset update.

### Check Current Branch

Run: `git branch --show-current`

Store result as `current_branch`.

### Construct PR Command

Base format:
```bash
.venv/bin/etl pr "Update <namespace>/<old_version>/<short_name>" data --work-branch update-<short_name>
```

**Branch name rule:** Keep under 28 characters for database compatibility.

**Base branch handling:** If NOT on master branch, add `--base-branch <current_branch>` flag.

Example:
```bash
.venv/bin/etl pr "Update energy/2023-10-01/electricity_mix" data \
  --work-branch update-elec-mix \
  --base-branch my-feature-branch
```

### Execute PR Creation

Run the constructed command.

Capture:
- PR number from output
- PR URL
- Branch name created

### Version Control Management

1. Stage all changes:
```bash
git add .
```

2. Commit with message:
```bash
git commit -m "Update dataset to new version"
```

3. Add empty commit for tracking:
```bash
git commit -m "Finish init" --allow-empty
```

Capture the commit hash of this empty commit.

4. Push changes:
```bash
git push
```

### Update PR Description

Extract PR number from creation output (e.g., `#1234`).

Add incremental diff link to PR description:
```
https://github.com/owid/etl/pull/<pr_number>/files/<last_commit>..HEAD
```

This allows reviewers to see exactly what changed after initial update.

**CHECKPOINT:** Summarize PR creation and get approval before proceeding.

## Workflow Step 3: Snapshot Update

Update the snapshot step and compare with the previous version.

### Update Snapshot Metadata

Check `.dvc` file at `snapshots/<namespace>/<new_version>/<short_name>.*.dvc`

Update if newer version available:
- `version_producer` - Producer's version identifier
- `date_published` - When producer published this data
- Years in citations if applicable

Keep own `date_accessed` but align version info with actual data.

### Execute Snapshot Step

Run:
```bash
.venv/bin/etls <namespace>/<new_version>/<short_name>
```

This fetches the latest raw data from the source.

### Load Previous Version

Ensure old snapshot is available:
```bash
.venv/bin/etlr snapshot://<namespace>/<old_version>/<short_name>
```

### Perform Comprehensive Comparison

Programmatically analyze both snapshot files to identify:

**Structural changes:**
- Sheet names (for Excel files)
- Column headers
- Data schema modifications

**Content changes:**
- Date ranges (min/max years)
- Data coverage (countries, indicators)
- New or removed data series
- Record counts

**Format changes:**
- File structure
- Encoding
- Data types

**Size differences:**
- File sizes
- Number of rows/columns

### Document Key Differences

Create clear, concise summary in `workbench/<short_name>/snapshot-runner.md`:

```markdown
# Snapshot Comparison: <namespace>/<short_name>

Old version: <old_version>
New version: <new_version>

## Structural Changes
- List sheet/table changes
- Column additions/removals
- Schema modifications

## Content Changes
- Date range: <old_min>-<old_max> → <new_min>-<new_max>
- New series: [list]
- Removed series: [list]
- Coverage changes: [details]

## Format Changes
- [Any encoding, structure, or type changes]

## Size Differences
- File size: <old_size> → <new_size>
- Row count: <old_rows> → <new_rows>

## Summary
[Brief 2-3 sentence summary of the update]
```

**Critical:** Never modify the old snapshot version!

**CHECKPOINT:** Present snapshot comparison summary and get approval before proceeding.

## Workflow Step 4: Meadow Step

Run, fix if needed, and validate the meadow processing step.

### Run Meadow Step

Execute:
```bash
.venv/bin/etlr data://meadow/<namespace>/<new_version>/<short_name>
```

### Handle Failures

If the step fails:

1. Read the full traceback carefully
2. Identify the root cause (parsing error, schema change, missing column, etc.)
3. Open relevant code files
4. Apply minimal, surgical fixes to address the issue
5. Re-run from the beginning of this step

**Common issues:**
- Column name changes → Update code references
- Schema changes → Adjust parsing logic
- Date format changes → Update date parsing
- Missing/renamed sheets → Update sheet references

**NEVER:**
- Return empty tables as workarounds
- Comment out failing code
- Catch and ignore exceptions without fixing root cause

### Run ETL Diff

Choose a representative country (e.g., "United States", "United Kingdom", "World").

Execute:
```bash
.venv/bin/etl diff REMOTE data/ --include "meadow/<namespace>/.*/<short_name>" \
  --verbose --country "<country>" > workbench/<short_name>/meadow_diff_raw.txt
```

**IMPORTANT:** Use regex pattern format `"meadow/<namespace>/.*/<short_name>"` NOT `--channel` parameter.

Example:
```bash
# ✅ CORRECT:
etl diff REMOTE data/ --include "meadow/worldbank_wdi/.*/wdi" \
  --verbose --country "United States"

# ❌ WRONG:
etl diff REMOTE data/ --include "worldbank_wdi/2025-09-08/wdi" \
  --verbose --country "United States" --channel meadow
```

### Validate Diff Output

Read `workbench/<short_name>/meadow_diff_raw.txt`.

**Check for problems:**
- Large amounts of removed data (indicates parsing issue)
- Unexpected variable removals
- Data type mismatches

If problems found:
- Fix the step logic
- Re-run from beginning of this step
- Don't proceed until output looks correct

### Summarize Differences

Create `workbench/<short_name>/meadow_diff.md`:

```markdown
# Meadow Diff Summary

Command:
```bash
etl diff REMOTE data/ --include "meadow/<namespace>/.*/<short_name>" \
  --verbose --country <country> > workbench/<short_name>/meadow_diff_raw.txt
```

## Additions / Removals
- **Added tables:** [list]
- **Added variables:** [list]
- **Removed tables:** [list]
- **Removed variables:** [list]

## Data Changes
- [Summarize value changes, ranges, significant shifts]

## Metadata Changes
- [List metadata additions/updates/removals]

## Summary
[2-3 sentence summary of meadow processing changes]
```

**CHECKPOINT:** Present meadow diff summary and get approval before proceeding.

## Workflow Step 5: Garden Step

Run, fix if needed, and validate the garden processing step.

### Run Garden Step

Execute:
```bash
.venv/bin/etlr data://garden/<namespace>/<new_version>/<short_name>
```

### Handle Failures

If the step fails, follow same process as Meadow step:

1. Read full traceback
2. Identify root cause
3. Open relevant files (Python code + YAML metadata)
4. Apply minimal fixes
5. Re-run until successful

**Common garden-specific issues:**
- Column name changes → Update garden code AND both garden + grapher metadata YAMLs
- Index leakage → Fix with `tb.format(["country", "year"])`
- Metadata validation errors → Add/remove variables in YAML as indicated
- Harmonization failures → Update country mapping files

**Metadata locations:**
- Garden: `etl/steps/data/garden/<namespace>/<new_version>/<short_name>.meta.yml`
- Grapher: `etl/steps/data/grapher/<namespace>/<new_version>/<short_name>.meta.yml`

### Run ETL Diff

Execute:
```bash
.venv/bin/etl diff REMOTE data/ --include "garden/<namespace>/.*/<short_name>" \
  --verbose --country "<country>" > workbench/<short_name>/garden_diff_raw.txt
```

### Validate Diff Output

Read `workbench/<short_name>/garden_diff_raw.txt`.

Check for:
- Reasonable variable additions/removals
- Expected data changes
- No unexpected data loss

If problems found, fix and re-run from beginning of this step.

### Summarize Differences

Create `workbench/<short_name>/garden_diff.md`:

```markdown
# Garden Diff Summary

Command:
```bash
etl diff REMOTE data/ --include "garden/<namespace>/.*/<short_name>" \
  --verbose --country <country> > workbench/<short_name>/garden_diff_raw.txt
```

## Additions / Removals
- **Added tables:** [list]
- **Added variables:** [list]
- **Removed tables:** [list]
- **Removed variables:** [list]

## Data Changes
- [Summarize value changes, indicator transformations, significant shifts]

## Metadata Changes
- [List metadata additions/updates/removals]

## Summary
[2-3 sentence summary of garden processing changes]
```

**CHECKPOINT:** Present garden diff summary and get approval before proceeding.

## Workflow Step 6: Grapher Step

Run and verify the grapher database ingestion step.

### Run Grapher Step

Execute with `--grapher` flag:
```bash
.venv/bin/etlr data://grapher/<namespace>/<new_version>/<short_name> --grapher
```

This uploads the dataset to the grapher MySQL database.

### Handle Failures

If the step fails:

1. Read full traceback
2. Common causes:
   - Database connection issues
   - Metadata validation errors
   - Schema mismatches
3. Apply minimal fixes
4. Re-run until successful

### Skip Diff for Grapher

**Do NOT run `etl diff` for grapher channel.** The data is now in the database and comparison is handled differently.

### Verify Upload Success

Check for success messages in output indicating:
- Tables uploaded
- Variables ingested
- No errors

**CHECKPOINT:** Confirm grapher step completed successfully and get approval before proceeding.

## Workflow Step 7: Consolidated Review

Present a comprehensive summary for final approval before committing.

### Compile Summary

Review all artifacts created:
- ETL update summary
- PR details
- Snapshot changes
- Meadow diff summary
- Garden diff summary
- Grapher upload status

### Present to User

Create consolidated summary:

```markdown
# Dataset Update Summary: <namespace>/<short_name>

## Overview
- Old version: <old_version>
- New version: <new_version>
- PR: <pr_url>
- Branch: <branch_name>

## Snapshot Changes
[Key points from snapshot-runner.md]

## Meadow Changes
[Key points from meadow_diff.md]

## Garden Changes
[Key points from garden_diff.md]

## Grapher Status
[Upload success confirmation]

## Files Modified
[List key files changed]

## Ready to Commit
All steps completed successfully. Proceed with commit and push?
```

**CHECKPOINT:** Get final approval before committing and pushing.

## Workflow Step 8: Commit and Push

After approval, commit all changes and update the PR.

### Stage All Changes

```bash
git add .
```

### Create Commit

```bash
git commit -m "Complete dataset update: <namespace>/<short_name> <old_version> → <new_version>

- Updated snapshot
- Fixed meadow step
- Fixed garden step
- Uploaded to grapher

🤖 Generated with Claude Code"
```

### Push to Remote

```bash
git push
```

### Update PR Description

Add or append to PR description with expanded summary including:
- Snapshot section (collapsed) with snapshot-runner.md content
- Meadow section (collapsed) with meadow_diff.md content
- Garden section (collapsed) with garden_diff.md content
- Grapher section with confirmation

Use GitHub's `<details>` tags for collapsible sections:

```markdown
## Update Summary

<details>
<summary>Snapshot Changes</summary>

[Content from snapshot-runner.md]

</details>

<details>
<summary>Meadow Changes</summary>

[Content from meadow_diff.md]

</details>

<details>
<summary>Garden Changes</summary>

[Content from garden_diff.md]

</details>

## Grapher Upload
✅ Successfully uploaded to grapher database
```

Update checklist in `progress.md` and mark as complete.

## Workflow Step 9: Indicator Upgrade (Optional)

Only execute if explicitly requested or if this is a staging deployment.

### Preconditions

- Must operate on staging, not production
- Host must match: `staging-site-<branch>`
- Dataset must have same `shortName` for old and new versions
- Datasets must not be archived

### Resolve Dataset IDs

Query staging database:

```sql
mysql -h staging-site-<branch> -u owid --port 3306 -D owid -e "
SELECT id, catalogPath, name, createdAt
FROM datasets
WHERE shortName = '<short_name>' AND NOT isArchived
ORDER BY id DESC;"
```

**Resolution rules:**
- `NEW_DATASET_ID` = highest `id` from results
- `OLD_DATASET_ID` = next highest `id` from results
- If fewer than 2 rows, fail with clear message

Verify `catalogPath` looks correct for new release path.

### Run Automatic Perfect Matching

First, dry run:
```bash
.venv/bin/etl indicator-upgrade match \
  --old-dataset-id <OLD_DATASET_ID> \
  --new-dataset-id <NEW_DATASET_ID> \
  --dry-run
```

Review output, then apply:
```bash
.venv/bin/etl indicator-upgrade match \
  --old-dataset-id <OLD_DATASET_ID> \
  --new-dataset-id <NEW_DATASET_ID>
```

### Check for Unmapped Variables

Parse output from match command to identify unmapped variables used in charts.

Look for:
- "Found X unmapped variables used in charts"
- Variable listings with IDs and names needing manual mapping

### Handle Unmapped Variables (Conditional)

**If unmapped variables found:**

1. Count unmapped variables from command output
2. **STOP and tell user:**
   - "Found [N] unmapped variables that are used in charts"
   - "Please run `make wizard` and use the Indicator Upgrade page to create variable mappings"
   - "Reply 'done' when you have finished creating the mappings"
3. **Wait** for user to reply exactly "done" (case-insensitive)
4. Only then proceed to next step

**If no unmapped variables:** Continue immediately.

### Dry Run Upgrade

Preview changes:
```bash
.venv/bin/etl indicator-upgrade upgrade --dry-run
```

Summarize planned chart updates in output.

### Apply Upgrade

Execute:
```bash
.venv/bin/etl indicator-upgrade upgrade
```

### Verify Results

Check no charts still reference old dataset:

```sql
mysql -h staging-site-<branch> -u owid --port 3306 -D owid -e "
SELECT DISTINCT c.id
FROM charts c
JOIN chart_dimensions cd ON c.id = cd.chartId
JOIN variables v ON cd.variableId = v.id
WHERE v.datasetId = <OLD_DATASET_ID>
ORDER BY c.id;"
```

**Expected:** Zero rows. If any remain, list chart IDs and fail.

Count variables from new dataset used by charts:

```sql
SELECT COUNT(DISTINCT v.id) AS new_used
FROM variables v
JOIN chart_dimensions cd ON v.id = cd.variableId
WHERE v.datasetId = <NEW_DATASET_ID>;
```

### Output and Persistence

Write `workbench/<short_name>/indicator_upgrade.json`:

```json
{
  "short_name": "<short_name>",
  "branch": "<branch>",
  "old_dataset_id": <OLD_DATASET_ID>,
  "new_dataset_id": <NEW_DATASET_ID>,
  "charts_updated": <K>,
  "old_charts_remaining": 0,
  "new_variables_used": <N>,
  "status": "success"
}
```

Print human summary:
```
Indicator Upgrade Complete

Dataset: <short_name>
Branch: staging-site-<branch>
Old dataset ID: <OLD_DATASET_ID> → New dataset ID: <NEW_DATASET_ID>
Charts updated: <K>
New variables in use: <N>
Old charts remaining: 0 ✅

Status: SUCCESS
```

**CHECKPOINT:** Present indicator upgrade results and confirm completion.

## Critical Guardrails

### Never Mask Problems

- **NEVER** return empty tables as workarounds for parsing failures
- **NEVER** comment out failing code to bypass errors
- **NEVER** catch and silently ignore exceptions without fixing root cause
- Let errors surface with full tracebacks for proper diagnosis
- Fix issues at their source, not with downstream patches

### Trace Issues Upstream

When encountering data quality issues (NaT values, missing data, missing indicators):
- Check snapshot first - external providers may have truncated data
- Examine snapshot history: `git log --oneline --follow snapshots/<dataset>.csv.dvc`
- Verify upstream data source still provides complete data
- Work backwards: garden → meadow → snapshot → source
- Fix at the earliest possible stage

### Data Quality Checks

- Add assertions that fail fast with clear error messages
- Verify date ranges, null counts, expected columns exist
- Check for duplicate records
- Validate data types match expectations

### Metadata Management

When data structure changes:
- **Column renames:** Update Python code AND all YAML files (garden + grapher)
- **Index issues:** Use `tb.format(["country", "year"])` to prevent index leakage
- **Metadata validation:** Use error messages as guides - they show exactly which variables to add/remove

### SQL Query Best Practices

Always enclose SQL in triple quotes for readability:

```sql
mysql -h staging-site-<branch> -u owid --port 3306 -D owid -e """
SELECT id, name, catalogPath
FROM datasets
WHERE shortName = '<short_name>'
ORDER BY id DESC;
"""
```

## Progress Persistence Format

Keep `workbench/<short_name>/progress.md` updated after each step:

```markdown
# Dataset Update Progress: <namespace>/<short_name>

Started: 2025-01-15 14:23:00 UTC
Last updated: 2025-01-15 15:45:00 UTC

- [x] Parse inputs and resolve variables - ✅ 14:23
- [x] Clean workbench directory - ✅ 14:24
- [x] Run ETL update workflow - ✅ 14:30
- [x] Create draft PR and work branch - ✅ 14:35
- [x] Update snapshot and compare versions - ✅ 14:42
- [x] Meadow step: run + fix + diff - ✅ 15:12
- [ ] Garden step: run + fix + diff
- [ ] Grapher step: run + verify
- [ ] Consolidated review and approval
- [ ] Commit, push, update PR
- [ ] Optional: indicator upgrade

## Notes
- Snapshot: Added 2 years of data (2023-2024)
- Meadow: Fixed column name change (cost → cost_local)
- Garden: Updated both garden and grapher YAML files
```

## Common Error Patterns

### Snapshot Issues
- **File size drastically different** → Check if source truncated data
- **Date range shortened** → Verify upstream source still has historical data
- **Missing sheets/columns** → Update parsing logic to match new structure

### Meadow Issues
- **Column not found** → Column was renamed, update references
- **Date parsing fails** → Format changed, update date parsing logic
- **Encoding errors** → Check file encoding, update read parameters

### Garden Issues
- **Index column appears** → Add `tb.format(["country", "year"])`
- **Metadata validation fails** → Add/remove variables in YAML as error indicates
- **Harmonization fails** → Update country mappings in `*.countries.json`

### Grapher Issues
- **Upload fails** → Check metadata YAML syntax
- **Variables missing** → Ensure grapher YAML includes all garden variables
- **Database connection** → Verify staging environment access

## Success Criteria

The update is successful when:
- ✅ All steps execute without errors
- ✅ Diffs show reasonable, expected changes
- ✅ No data loss or empty tables
- ✅ Metadata updated correctly
- ✅ Grapher upload completes
- ✅ All artifacts saved to workbench
- ✅ Changes committed and pushed
- ✅ PR updated with comprehensive summary
- ✅ (Optional) Indicator upgrade completes with zero old charts remaining

## Summary

This skill provides a comprehensive, checkpoint-driven workflow for updating OWID datasets. Execute each step methodically, validate results, seek approval at checkpoints, and never mask underlying problems. The result is a thoroughly tested, well-documented dataset update ready for review and deployment.
