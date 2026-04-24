---
name: update-dataset
description: End-to-end dataset update workflow with PR creation, snapshot, meadow, garden, and grapher steps. Use when user wants to update a dataset, refresh data, run ETL update, or mentions updating dataset versions.
metadata:
  internal: true
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
- [ ] Catalog `# NOTE:` / `# TODO:` comments carried over from the old step files into `notes_to_check.md`
- [ ] Detect any `sanity_checks` functions and their log-control flags; append to `notes_to_check.md`
- [ ] Create or reuse draft PR and work branch
- [ ] Update snapshot and compare to previous version; capture summary
- [ ] Meadow step: run + fix + diff + summarize
- [ ] Garden step: run + fix + diff + summarize
- [ ] Review `sanity_checks` output (enable log flag, re-run, scan log, revert flag) — skip if none found
- [ ] Grapher step: run + verify (skip diffs), or explicitly mark N/A
- [ ] Re-evaluate each catalogued `# NOTE:` / `# TODO:` against fresh data; delete resolved workarounds + comments together, or record status in PR body
- [ ] Check metadata: typos, Jinja spacing, style guide compliance
- [ ] Commit, push, and update PR description
- [ ] Run indicator upgrade on staging and persist report
- [ ] Pick 1–3 chart views for the public announcement
- [ ] Draft Slack announcement, add to PR description, post `@codex review` as a separate PR comment, and notify user to post it to #data-updates-comms
- [ ] Address Codex review comments (fix valid ones + resolve all threads)
- [ ] Ask the user whether to archive the old DAG entries; if yes, move them to `dag/archive/` AND relocate the new entries into the old slot (see "DAG archiving & reordering") — don't forget this step

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

1b) Check for outdated practices (check-outdated-practices skill)
   - After `etl update` creates new step files, run the `/check-outdated-practices` skill on the newly created files
   - This catches patterns like `if __name__ == "__main__"`, `geo.harmonize_countries()`, `dest_dir`, `paths.load_dependency()`, etc. that were copied from old versions
   - Fix any findings before proceeding — this avoids propagating legacy patterns into new versions

1c) Catalog `# NOTE:` / `# TODO:` comments in the copied step files (don't resolve yet)
   - Run `rg -n "#\s*(NOTE|TODO|FIXME|HACK|XXX):" snapshots/<namespace>/<new_version>/ etl/steps/data/{meadow,garden,grapher}/<namespace>/<new_version>/`.
   - Filter out generic boilerplate (e.g. `# NOTE: To learn more about the fields, hover over their names.` at the top of `.meta.yml`).
   - Save the remaining actionable items to `workbench/<short_name>/notes_to_check.md` — one entry per annotation, recording file path, line number, which step it lives in (meadow/garden/grapher), and what the workaround does.
   - Don't act on them yet. Resolution requires fresh data and happens **after** each step's run — see step 6a.

1d) Detect sanity-check logic in the copied step files
   Sanity checks live in two different forms — detect **both**:

   - **Function form** — `def sanity_check…` / `sanity_check…(` call sites. Often gated by a module-level boolean flag (`DEBUG`, `SHOW_SANITY_CHECK_LOGS`, `LONG_FORMAT`) that defaults to `False` to keep normal runs quiet. Examples: `etl/steps/data/garden/wb/.../world_bank_pip.py` (`SHOW_SANITY_CHECK_LOGS`), `etl/steps/data/garden/wid/.../world_inequality_database.py` (`DEBUG` + `LONG_FORMAT`), `etl/steps/data/garden/lis/.../luxembourg_income_study.py` (no flag; prints unconditionally via `tabulate`).
   - **Inline comment form** — `# Sanity check` / `# Sanity checks` / `# sanity check` marking an inline assertion block that isn't wrapped in a dedicated function. Very common: `etl/steps/data/garden/emdat/.../natural_disasters.py`, `etl/steps/data/garden/emissions/.../national_contributions.py`, `etl/steps/data/garden/irena/.../renewable_capacity_statistics.py`. These usually have no log flag — the block simply runs on every step execution and either passes or raises.

   Run a combined sweep:
   ```bash
   rg -n -i "def sanity_check|sanity_check\(|#\s*sanity check" \
       snapshots/<namespace>/<new_version>/ \
       etl/steps/data/{meadow,garden,grapher}/<namespace>/<new_version>/
   ```

   Append a "Sanity checks" section to `workbench/<short_name>/notes_to_check.md` listing each hit — for each, record: file path + line number, which form (function vs. inline comment), the name of any log-control flag (function form only), and a one-line description of what's being asserted (read the surrounding 5–10 lines).

   Don't act yet — the review happens in step 5b once the garden step has been run on the new data.

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

5b) Review sanity-checks output (only if step 1d catalogued any)
   Handling depends on the form catalogued in step 1d.

   **Function form with a log-control flag** (e.g. `SHOW_SANITY_CHECK_LOGS`, `DEBUG`):
   1. Flip the flag to `True` at the top of the garden step file.
   2. Re-run the garden step, capturing output:
      ```bash
      .venv/bin/etlr data://garden/<namespace>/<new_version>/<short_name> --private --force --only \
          > workbench/<short_name>/sanity_checks.log 2>&1
      ```
   3. Review the log: scan for `AssertionError`, `error`, `warning`, `dropped`, outliers flagged by country/year, unexpected totals. Surface actionable findings in the PR description under a "Sanity-check findings" collapsed section.
   4. **Revert the flag to its original value** (usually `False`) before committing. Verify with `git diff` that the garden file has no unintended changes.

   **Function form with no flag, or inline `# Sanity check(s)` comment blocks**:
   1. Since the checks always run, any `AssertionError` would have already blown up the step — so the fact that step 5 passed means all assertions held. Focus on *interpreting* what the checks cover.
   2. Read each catalogued block (pull 5–15 lines of context around the hit) and, for the ones that look non-trivial, verify the invariant still holds qualitatively on the new data. Examples: "asserts that no country has > 2× last year's value" — spot-check via `.venv/bin/python` against the fresh garden output.
   3. Record any anomalies under "Sanity-check findings" in the PR description. No log artifact to keep here since the step's own output is the evidence.

   In either form: if sanity_checks raise `AssertionError` on the new data (not just log warnings), stop and decide with the user whether the assertion needs a threshold bump, whether upstream data genuinely broke, or whether the invariant being enforced is obsolete.

6) Grapher step run/verify (step-fixer subagent, channel=grapher, add --grapher)
   - Skip diff

6a) Re-evaluate `# NOTE:` / `# TODO:` items from step 1c against fresh data
   Now that meadow, garden, and grapher have run on the **new** data, go back to `workbench/<short_name>/notes_to_check.md` and decide each item's fate. For each entry:

   - Identify what the workaround does (read the surrounding code).
   - Load the affected step's output with `owid.catalog.Dataset` (or inspect the raw snapshot) and compare **corrected vs. uncorrected** values. Cross-check the producer's release notes / changelog if available.
   - If the upstream issue is fixed → delete the workaround **and** its `# NOTE:` / `# TODO:` comments **in the same commit**, then re-run the affected step (use `--force --only`, add `--grapher` for grapher) so downstream artifacts pick up the change.
   - If the workaround is still needed → leave it and add a one-line status under "Phase 2 TODOs" in the PR description (e.g. "Sierra Leone ×1000 correction still required — raw value in the 2026 file is still ~1/1000 of plausible").
   - If you're uncertain → keep it, flag it in the PR description, and ask the user.

   Do this **before** step 6b (metadata checks) so any re-runs triggered by comment-removal happen before the metadata sweep, not after.

6b) Metadata quality checks — run after all ETL steps are built
   Run all three checks on the newly built garden and grapher datasets so every issue surfaces together. Each skill writes results to the terminal; fix what comes up before moving on.

   - **Typos** — `/check-metadata-typos` scoped to the current step. Run on each of the new `.meta.yml` files (garden first, then grapher). Accept or skip each suggested fix.
   - **Jinja spacing** — `/check-metadata-spacing` on the built garden and grapher datasets. Catches template artifacts like doubled spaces or stray newlines that only appear after Jinja rendering.
   - **Style guide** — `/check-metadata-style` on the grapher step. Audits user-facing fields (title, subtitle, description_short, display.name, presentation.*) against OWID's Writing and Style Guide. Rules live in `.claude/skills/check-metadata-style/STYLE_GUIDE.md`, so no Notion access is needed — but if the guide looks out of date, refresh that file from Notion in a separate PR.

   If any skill rewrites a `.meta.yml`, re-run the affected step so the built catalog reflects the edits. **Add `--grapher` when the affected step is on the grapher channel** — without it the local catalog is updated but staging stays stale, so the step 7 indicator upgrade sees the old text.
   ```bash
   # garden / meadow:
   .venv/bin/etlr <channel>/<namespace>/<new_version>/<short_name> --private --force --only
   # grapher:
   .venv/bin/etlr grapher/<namespace>/<new_version>/<short_name> --grapher --private --force --only
   ```
   Then re-run the relevant check to confirm zero remaining violations.

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
   - Query the staging DB for **published** charts using the new dataset (filter on `c.publishedAt IS NOT NULL`). Draft/unlisted charts must not be counted in the announcement:
     ```sql
     SELECT c.id, cc.slug, cc.full->>'$.title' as title, cc.full->>'$.type' as type, cc.full->>'$.hasMapTab' as hasMapTab
     FROM charts c
     JOIN chart_configs cc ON cc.id = c.configId
     JOIN chart_dimensions cd ON cd.chartId = c.id
     JOIN variables v ON cd.variableId = v.id
     WHERE v.catalogPath LIKE '%<namespace>/<new_version>%'
       AND c.publishedAt IS NOT NULL
     GROUP BY c.id
     ```
   - The number reported in the Slack announcement's "How many charts did this update affect?" section must be this **published** count, not the total. It's fine to mention draft remaps separately in the PR description for completeness, but never in the Slack copy.
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

Commit and push incrementally as you go — after each step that produces code changes. Don't wait until the end. Use descriptive commit messages with appropriate emojis (the one auto-prepended by `etl pr` for the chosen category + 🤖 for AI-written code).

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

## DAG archiving & reordering

After the ETL update, `etl update` appends the new version entries to the **bottom** of the main DAG file while the old version's entries stay in their original slot. **Always ask the user** whether to archive — but never skip this checklist item, and when the user agrees, always do the reorder too (not just the archive).

Workflow when the user agrees:

1. **Archive the old version.** Move its entries (snapshot → meadow → garden → grapher) from the main DAG file (e.g., `dag/poverty_inequality.yml`) to the **bottom** of the corresponding archive file (`dag/archive/<same_file>.yml`). Include the original section comment (e.g., `# 1000 Binned Global Distribution (World Bank PIP)`) above the archived entries.
2. **Move the new entries into the old slot** so the dataset stays grouped with its neighbours and section comment. The new entries should not remain at the bottom of the main DAG.
3. Preserve the original section comment (same indentation as the old block) above the new entries.
4. Verify: `rg "<namespace>/<old_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"` returns nothing, and `rg "<namespace>/<new_version>/<short_name>" dag/ -g "*.yml"` shows the entries only in the main file (under the section comment), not at the bottom.
5. Run `make check` and commit with `🔨🤖 Archive old <name> entries and reorder DAG`.

## Guardrails and tips

- **DAG consistency**: After `etl update`, always verify that all new steps in `dag/main.yml` reference each other with the new version. A common bug is garden depending on old meadow or old snapshot — this silently loads stale data.
- Never return empty tables or comment out logic as a workaround — fix the parsing/transformations instead.
- Column name changes: update garden processing code and metadata YAMLs (garden/grapher) to match schema changes.
- Indexing: avoid leaking index columns from `reset_index()`; format tables with `tb.format(["country", "year"])` as appropriate.
- Metadata validation errors are guidance — update YAML to add/remove variables as indicated.

## Artifacts (expected)

- `workbench/<short_name>/snapshot-runner.md`
- `workbench/<short_name>/progress.md`
- `workbench/<short_name>/notes_to_check.md` (one entry per carried-over `# NOTE:` / `# TODO:`, plus detected `sanity_checks` functions and their log-control flags)
- `workbench/<short_name>/sanity_checks.log` (only if step 5b ran)
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
