---
name: update-dataset
description: End-to-end dataset update workflow with PR creation, snapshot, meadow, garden, and grapher steps. Use when user wants to update a dataset, refresh data, run ETL update, or mentions updating dataset versions.
---

# Update Dataset (PR → snapshot → steps → grapher)

Use this skill to run a complete dataset update with Claude Code subagents, keep a live progress checklist, and pause for user approval only when something needs attention.

## Inputs

- `<namespace>/<old_version>/<name>`
- Get `<new_version>` as today's date by running `date -u +"%Y-%m-%d"`

Optional trailing args:
- branch: The working branch name (defaults to current branch)

Assumptions:
- All artifacts are written to `workbench/<short_name>/`.
- Persist progress to `workbench/<short_name>/progress.md` and update it after each step.

## Progress checklist (maintain, tick live, and persist to progress.md)

- [ ] Parse inputs and resolve: channel, namespace, version, short_name, old_version, branch
- [ ] Clean workbench directory: delete `workbench/<short_name>` unless continuing existing update
- [ ] Run ETL update workflow via `etl-update` subagent (help → dry run → approval → real run)
- [ ] Create or reuse draft PR and work branch
- [ ] Update snapshot and compare to previous version; capture summary
- [ ] Meadow step: run + fix + diff + summarize
- [ ] Garden step: run + fix + diff + summarize
- [ ] Grapher step: run + verify (skip diffs), or explicitly mark N/A
- [ ] Commit, push, and update PR description
- [ ] Run indicator upgrade on staging and persist report
- [ ] Pick 1–3 chart views for the public announcement
- [ ] Draft Slack announcement, add to PR description, post `@codex review` as a separate PR comment, and notify user to post it to #data-updates-comms
- [ ] Address Codex review comments (fix valid ones + resolve all threads)

Persistence:
- After ticking each item, update `workbench/<short_name>/progress.md` with the current checklist state and a timestamp.

## Checkpoints — when to pause

**Default: keep going.** Run through the full workflow (steps 1–8) without stopping unless one of the conditions below is met.

**Stop and ask the user when:**
- A step fails and the fix is ambiguous (multiple reasonable approaches, or you're unsure of the correct one)
- Data structure changed significantly (columns removed/renamed, large row count drops, schema changes that may affect charts)
- Country harmonization has new unmatched countries that need manual decisions
- The snapshot requires a manual download or credentials you don't have
- Indicator upgrade had imperfect matches (< 100% similarity) that need human review
- Anything that could silently break charts or lose data

**Don't stop for:**
- Routine assertion count updates (just update them and note in the summary)
- Clean step runs with only row increases
- Expected warnings (SettingWithCopyWarning, known unmapped territories)
- Straightforward filename/version reference updates

When you do stop, present a concise summary of the issue and what options exist.

## Workflow orchestration

0) Initial setup
   - Check if `workbench/<short_name>/progress.md` exists to determine if continuing existing update
   - If starting fresh: delete `workbench/<short_name>` directory if it exists
   - Create fresh `workbench/<short_name>` directory for artifacts

1) Run ETL update command (etl-update subagent)
   - Inputs: `<namespace>/<old_version>/<short_name>` plus any required flags
   - **CRITICAL**: Run `etl update` ONCE for the full step URI (e.g., `data://garden/namespace/old_version/short_name`). Do NOT run it separately per channel (snapshot, meadow, garden, grapher). Running it once ensures all cross-step DAG dependencies are updated together. Running it per-channel leaves stale version references in `dag/main.yml` (e.g., garden pointing to old meadow version).
   - Perform help check, dry run, approval, then real execution; capture summary for later PR notes
   - After running, **always verify `dag/main.yml`**: grep for the old version and confirm all internal references between the new steps point to the new version (e.g., garden depends on new meadow, not old meadow).

2) Create PR and integrate update via subagent (etl-pr)
   - Inputs: `<namespace>/<old_version>/<short_name>`
   - Create or reuse draft PR, set up work branch, and incorporate the ETL update outputs

3) Snapshot run & compare (snapshot-runner subagent)
   - Inputs: `<namespace>/<new_version>/<short_name>` and `<old_version>`

4) Meadow step repair/verify (step-fixer subagent, channel=meadow)
   - Run, fix, re-run; produce diffs
   - Save diffs and summaries

5) Garden step repair/verify (step-fixer subagent, channel=garden)
   - Run, fix, re-run; produce diffs
   - Save diffs and summaries

6) Grapher step run/verify (step-fixer subagent, channel=grapher, add --grapher)
   - Skip diff

7) Indicator upgrade (optional, staging only)
   - First upload the new grapher dataset to the staging DB (required before the upgrader can detect it):
     ```bash
     STAGING=<branch> .venv/bin/etlr data://grapher/<namespace>/<new_version>/<short_name> --grapher --private
     ```
   - Then run the automatic upgrader:
     ```bash
     STAGING=<branch> .venv/bin/etl indicator-upgrade auto
     ```
   - **CRITICAL**: After the upgrader finishes, always verify it actually worked by querying staging:
     ```bash
     mysql -h "staging-site-<branch>" -u owid --port 3306 -D owid -e "SELECT COUNT(*) FROM chart_dimensions cd JOIN variables v ON cd.variableId = v.id WHERE v.catalogPath LIKE '%<namespace>/<new_version>%'"
     ```
     If the count is 0, the upgrade did not run — re-run it.

8) Pick chart views for the public announcement
   - Query the staging DB for all charts using the new dataset:
     ```sql
     SELECT c.id, cc.slug, cc.full->>'$.title' as title, cc.full->>'$.type' as type, cc.full->>'$.hasMapTab' as hasMapTab
     FROM charts c
     JOIN chart_configs cc ON cc.id = c.configId
     JOIN chart_dimensions cd ON cd.chartId = c.id
     JOIN variables v ON cd.variableId = v.id
     WHERE v.catalogPath LIKE '%<namespace>/<new_version>%'
     GROUP BY c.id
     ```
   - Pick 1–3 views using these criteria (in order of preference):
     - **Map views** — immediately visual, readers can find their own country
     - **Charts with punchy, standalone headlines** — titles that make a clear claim work best for social sharing
     - **Global trend charts** (StackedArea / World) — show the big picture over time
     - **Skip**: population-weighted variants (harder to read quickly), within-regime breakdowns (too niche), country-specific views
   - Add the selected charts with brief rationale to the Slack announcement draft

9) Slack announcement & PR update
   - Fill out the template at `.claude/skills/update-dataset/slack-announcement-template.md` using facts gathered during the update (coverage, chart count, key changes, etc.)
   - Include the 1–3 selected chart views from step 8
   - Ask user if unsure about any details
   - Save the draft to `workbench/<short_name>/slack-announcement.md`
   - **Add the announcement to the PR description** as a collapsed section titled "Slack Announcement"
   - **Post `@codex review` as a separate PR comment** (not in the PR description) to trigger an automated code review. Use:
     ```bash
     gh pr comment <pr_number> --body "@codex review"
     ```
   - Tell the user: "Slack announcement drafted at `workbench/<short_name>/slack-announcement.md` and added to the PR description. Please review and post it to **#data-updates-comms**."

10) Codex review: address comments and resolve threads
   - Wait ~60 seconds after posting `@codex review`, then poll for inline review comments:
     ```bash
     gh api repos/owid/etl/pulls/<pr_number>/comments | python3 -m json.tool
     ```
   - Fetch open review thread IDs via GraphQL:
     ```bash
     gh api graphql -f query='{ repository(owner:"owid", name:"etl") { pullRequest(number:<pr_number>) { reviewThreads(first:20) { nodes { id isResolved comments(first:1) { nodes { body } } } } } } }'
     ```
   - For each unresolved Codex comment:
     - **If valid**: apply the fix, commit, push, then resolve the thread:
       ```bash
       gh api graphql -f query='mutation { resolveReviewThread(input:{threadId:"<thread_id>"}) { thread { id isResolved } } }'
       ```
     - **If not valid / not applicable**: reply explaining why, then resolve the thread:
       ```bash
       gh api repos/owid/etl/pulls/<pr_number>/comments/<comment_id>/replies -f body="<explanation>"
       gh api graphql -f query='mutation { resolveReviewThread(input:{threadId:"<thread_id>"}) { thread { id isResolved } } }'
       ```
   - If Codex hasn't posted yet after 60 s, wait another 60 s and retry (up to ~5 min total).

## Committing and pushing

Commit and push incrementally as you go — after each step that produces code changes. Don't wait until the end. Use descriptive commit messages with appropriate emojis (📊🤖 for data updates).

At the end of the workflow, update the PR description with:
- A summary of key changes at the top
- Collapsed sections for each pipeline step (Snapshot, Meadow, Garden, Grapher)
- A collapsed section for the Slack announcement

## Downstream dependency check

After completing the update, check if any other datasets depend on the **old** version of the updated dataset:

```bash
rg "<namespace>/<old_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"
```

Filter out the old dataset's own DAG entries (snapshot → meadow → garden → grapher chain). Any remaining references are **downstream dependents** that still point to the old version.

If downstream dependents exist:
- **Tell the user** which datasets depend on the old version and need updating in a follow-up PR
- **Add a "Downstream dependencies" section to the PR description** (not collapsed — this is important) listing the dependent datasets with a note that they should be updated to point to the new version in a follow-up PR

## Guardrails and tips

- **DAG consistency**: After `etl update`, always verify that all new steps in `dag/main.yml` reference each other with the new version. A common bug is garden depending on old meadow or old snapshot — this silently loads stale data.
- Never return empty tables or comment out logic as a workaround — fix the parsing/transformations instead.
- Column name changes: update garden processing code and metadata YAMLs (garden/grapher) to match schema changes.
- Indexing: avoid leaking index columns from `reset_index()`; format tables with `tb.format(["country", "year"])` as appropriate.
- Metadata validation errors are guidance — update YAML to add/remove variables as indicated.

## Artifacts (expected)

- `workbench/<short_name>/snapshot-runner.md`
- `workbench/<short_name>/progress.md`
- `workbench/<short_name>/meadow_diff_raw.txt` and `meadow_diff.md`
- `workbench/<short_name>/garden_diff_raw.txt` and `garden_diff.md`
- `workbench/<short_name>/indicator_upgrade.json` (if indicator-upgrader was used)
- `workbench/<short_name>/slack-announcement.md`

## Example usage

- Minimal catalog URI with explicit old version:
  - `update-dataset data://snapshot/irena/2024-11-15/renewable_power_generation_costs 2023-11-15 update-irena-costs`

---

### Common issues when data structure changes

- SILENT FAILURES WARNING: Never return empty tables or comment code as workarounds!
- Column name changes: If columns are renamed/split (e.g., single cost → local currency + PPP), update:
  - Python code references in the garden step
  - Garden metadata YAML (e.g., `food_prices_for_nutrition.meta.yml`)
  - Grapher metadata YAML (if it exists)
- Index issues: Check for unwanted `index` columns from `reset_index()` — ensure proper indexing with `tb.format(["country", "year"])`.
- Metadata validation: Use error messages as a guide — they show exactly which variables to add/remove from YAML files.
