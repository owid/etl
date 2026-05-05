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
- [ ] Country harmonization audit: validate `.countries.json` against canonical regions, audit `.excluded_countries.json`, scan garden log for missing/unused/unknown warnings
- [ ] Grapher step: run + verify (skip diffs), or explicitly mark N/A
- [ ] Re-evaluate each catalogued `# NOTE:` / `# TODO:` against fresh data; delete resolved workarounds + comments together, or record status in PR body
- [ ] Check metadata: typos, Jinja spacing, style guide compliance
- [ ] Verify indicator-metadata coverage, `dataset.update_period_days`, and that all URLs resolve (HEAD-check)
- [ ] Commit, push, and update PR description
- [ ] Run indicator upgrade on staging and persist report
- [ ] Pick 1–3 chart views for the public announcement
- [ ] Gather editorial context from snapshot DVC + garden `.meta.yml` (and `url_main` via WebFetch if needed) — shared input for the Slack and Data update steps
- [ ] Draft Slack announcement, add to PR description, post `@codex review` as a separate PR comment, and notify user to post it to #data-updates-comms
- [ ] Draft public-facing "Data update" post for OWID /latest, add to PR description, hand to user for review and publication
- [ ] Address Codex review comments (fix valid ones + resolve all threads)
- [ ] Ask the user whether to archive the old DAG entries; if yes, move them to `dag/archive/` AND relocate the new entries into the old slot (see "DAG archiving & reordering") — don't forget this step
- [ ] Hand off Wizard QA links to the user (Anomalist + Chart Diff on the staging branch) — this is the final step

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
   1. Read each catalogued block (pull 5–15 lines of context around the hit) to understand what invariant is being tested.
   2. Important: a sanity check can enforce its finding either by **raising** (`assert`, `raise`) or by **logging** (`paths.log.warning`, `.critical`, even `.fatal`). Logging variants do NOT fail the step — so "step 5 passed" is not proof that every invariant held. If the block uses logging, re-run the step and scan stdout/stderr for the relevant keywords; don't trust the exit code alone.
   3. For non-trivial invariants (monotonicity, totals, bounds), also spot-check qualitatively against the fresh garden output via a short `.venv/bin/python` snippet.
   4. Record any anomalies under "Sanity-check findings" in the PR description. No log artifact to keep here since the step's own output is the evidence.

   In either form: if sanity_checks raise `AssertionError` on the new data, stop and decide with the user whether the assertion needs a threshold bump, whether upstream data genuinely broke, or whether the invariant is obsolete. If the check only *logs*, treat a new/expanding set of warnings the same way — they're the signal the sanity check was written to produce.

   **Watch for silent-delete patterns.** Some sanity_checks functions also mutate the table — e.g. `world_bank_pip`'s `sanity_checks` drops rows that fail invariants and reports the count via the log-control flag. With the flag off the deletions still happen; the reviewer just never learns which rows disappeared. When reading a sanity_checks function, scan for `drop`, `filter`, `tb = tb[...]` — anything that removes rows — and list every deletion in the PR body, not just the warning counts. If the deletion seems newly applicable to upstream fixes (e.g. the row should no longer be anomalous in the new release), that's a candidate for removing the workaround entirely.

5c) Country harmonization audit
   Run after the garden step completes (and after 5b if it ran). Verifies that the country mappings consumed by `paths.regions.harmonize_names(...)` are well-formed and surfaces any harmonization warnings the garden run produced. Output: `workbench/<short_name>/harmonization_audit.md`.

   **Modern API.** Garden steps should be calling `paths.regions.harmonize_names(tb, country_col=..., countries_file=..., excluded_countries_file=...)` — the wrapper in `etl/data_helpers/geo.py:1874`. If you find a step still using the deprecated `geo.harmonize_countries(...)` directly, step 1b's `/check-outdated-practices` should already have flagged it; treat that as a separate cleanup. The audit below is API-agnostic — both call sites end up emitting the same three warning strings.

   **Source of truth.** Canonical names come from **two** datasets, both consulted by the harmonizer:

   - `data/garden/regions/2023-01-01/regions` — countries, continents, and OWID-defined aggregates. The runtime authority is `paths.regions.tb_regions["name"]`. This is built from `etl/steps/data/garden/regions/2023-01-01/regions.yml` plus a merge with `regions.codes.csv` and field defaults — **don't parse the YAML in isolation** or you'll miss the legacy entries and produce false positives.
   - `data/garden/wb/<latest>/income_groups` — the four World Bank income-group aggregates (`High-income countries`, `Upper-middle-income countries`, `Lower-middle-income countries`, `Low-income countries`). OWID treats the **latest** version of this dataset as the official one, so the audit must resolve the version dynamically (don't pin a date — it goes stale when WB publishes a refresh). The names live in the `classification` column of the `income_groups_latest` table.

   The audit's "canonical" set is the union of these two. A `.countries.json` entry looks like `"Source name": "Target name"` — the audit checks that every **target name** (the value the source gets harmonized to) appears in *either* dataset. Anything else is flagged.

   1. **Capture a fresh garden log:**
      ```bash
      .venv/bin/etlr data://garden/<namespace>/<new_version>/<short_name> --private --force --only \
          > workbench/<short_name>/harmonization.log 2>&1
      ```

   2. **Scan the log for the three harmonization warnings.** These are emitted by `etl/data_helpers/geo.py` (excluded list) and `lib/datautils/owid/datautils/dataframes.py` (mapping warnings) — the wording is stable:
      ```bash
      rg -n "missing values in mapping\.|unused values in mapping\.|Unknown country names in excluded countries file:" \
          workbench/<short_name>/harmonization.log
      ```
      For each warning, the entity list follows on subsequent lines (because `harmonize_countries()` is called with `show_full_warning=True` by default). Capture them.

   3. **Validate `.countries.json` target names against canonical regions + income groups.** Each entry maps a source name (key) to a target / harmonized name (value); this check looks at the values. For each garden step in this update:
      ```python
      import json
      from pathlib import Path
      from owid.catalog import Dataset

      tb_regions = Dataset("data/garden/regions/2023-01-01/regions")["regions"]
      canonical_regions = set(tb_regions["name"].dropna().astype(str))

      # Resolve the latest income_groups dataset dynamically — OWID treats latest as official.
      ig_dirs = sorted(Path("data/garden/wb").glob("*/income_groups"))
      assert ig_dirs, "No data/garden/wb/<version>/income_groups dataset built locally"
      ds_ig = Dataset(str(ig_dirs[-1]))
      canonical_income = set(ds_ig["income_groups_latest"]["classification"].dropna().astype(str).unique())

      canonical = canonical_regions | canonical_income

      mapping = json.loads(Path("etl/steps/data/garden/<namespace>/<new_version>/<short_name>.countries.json").read_text())
      not_in_canonical = sorted({v for v in mapping.values() if v and v not in canonical})
      print("Targets not in OWID's canonical regions or income groups:", not_in_canonical)
      ```
      A non-empty `not_in_canonical` list means the mapping points at entities that aren't registered in either the regions catalog or the income-groups dataset. This isn't automatically a bug — it's a heads-up. **Stop and decide with the user before proceeding** — same pattern as the global "Checkpoints — when to pause" section at the top of this skill. Common causes (in order from "fix" to "accept"): typo, retired alias used as canonical, casing/whitespace mismatch, or a legitimately custom aggregate the source defines that OWID has no equivalent for (e.g. ILO's `" (ILO)"`-suffixed regions, World Bank's `" (WB)"`-suffixed sub-Saharan splits, BRICS, G7, G20). For typos/casing — fix the JSON. For legitimately custom aggregates — accept and note in the PR description that those entities live outside the canonical system and won't merge with population/regions infrastructure. For a real new historical region — add an entry to `regions.yml` in a separate PR.

   4. **Audit `.excluded_countries.json`.** The file is optional; skip if it doesn't exist:
      ```python
      excluded_path = Path("etl/steps/data/garden/<namespace>/<new_version>/<short_name>.excluded_countries.json")
      if excluded_path.exists():
          excluded = json.loads(excluded_path.read_text())
          suspicious_canonical = sorted(set(excluded) & canonical)
          # Also surface continents and aggregates separately for review
          aggregates = set(tb_regions[tb_regions["region_type"].isin(["continent", "aggregate"])]["name"].dropna().astype(str))
          suspicious_aggregates = sorted(set(excluded) & aggregates)
          print("Excluded entries that ARE canonical regions:", suspicious_canonical)
          print("Excluded entries that are continents/aggregates:", suspicious_aggregates)
          print("Full excluded list for review:", sorted(excluded))
      ```
      `suspicious_canonical` is the actionable signal: each entry is a known country/region that we are dropping. Sometimes this is intentional (e.g. dropping "World" rows because the source double-counts them) — surface, don't auto-fix. **Pause and ask the user** if the list is non-empty. The full list is dumped so the LLM can also eyeball it for entities that aren't in `canonical` but look like real countries (typos, alternative names) we should be mapping rather than dropping.

   5. **Write findings** to `workbench/<short_name>/harmonization_audit.md` with five sections, populated only when non-empty. **Each section must list every flagged entity**, not just a count — counts alone aren't actionable, the user (or you) needs to read the actual names to judge whether each is intentional. For long lists (>20 entries) group by pattern when the grouping is obvious (e.g. ILO's `" (ILO)"`-suffixed regions vs. international orgs vs. derived "World ..." aggregates) so the reviewer can scan categories instead of one flat list. Sections:
      - `## Missing in mapping` — countries in source data not in `.countries.json` (from log warning #1) — list each missing source name
      - `## Unused mappings` — `.countries.json` entries the data never used (warning #2) — list each unused source→target pair
      - `## Unknown excluded entries` — `.excluded_countries.json` entries not present in source data (warning #3) — list each
      - `## Targets not in OWID's canonical regions or income groups` — target names from `.countries.json` that aren't registered in either dataset (Python check #3) — list each target name and the source names that map to it
      - `## Excluded entries matching canonical regions` — possible over-exclusion (Python check #4) — list each

   6. **Surface in PR.** If any section was populated, add a collapsed "Harmonization audit" section to the PR description (after the per-step sections, before the Slack announcement) **with the same listings**, not just a summary. Empty sections can be omitted.

   **When you report progress to the user during the workflow, never just give a count — always include the list (or grouped categories) so they can judge in one glance.**

   **Checkpoint summary:**
   - "Targets not in OWID's canonical regions or income groups" or "Missing in mapping" non-empty ⇒ stop, decide with user.
   - "Excluded entries matching canonical regions" non-empty ⇒ stop, ask whether each exclusion is intentional.
   - "Unused mappings" or "Unknown excluded entries" non-empty ⇒ surface in PR description; not a blocker.

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
   Run all four checks on the newly built garden and grapher datasets so every issue surfaces together. Each skill writes results to the terminal; fix what comes up before moving on.

   - **Typos** — `/check-metadata-typos` scoped to the current step. Run on each of the new `.meta.yml` files (garden first, then grapher). Accept or skip each suggested fix.
   - **Jinja spacing** — `/check-metadata-spacing` on the built garden and grapher datasets. Catches template artifacts like doubled spaces or stray newlines that only appear after Jinja rendering.
   - **Style guide** — `/check-metadata-style` on the grapher step. Audits user-facing fields (title, subtitle, description_short, display.name, presentation.*) against OWID's Writing and Style Guide. Rules live in `.claude/skills/check-metadata-style/STYLE_GUIDE.md`, so no Notion access is needed — but if the guide looks out of date, refresh that file from Notion in a separate PR.
   - **Clarity for a general audience** — read every user-facing field with non-specialist eyes. The other three skills enforce structure and style; this one judges whether the text is *understandable*.

   ### Clarity checklist (do manually, no skill yet)

   OWID readers are not domain experts. Walk each indicator's user-facing fields and flag anything that requires inside knowledge to parse.

   | Field | Clarity check |
   |---|---|
   | `title` / `presentation.title_public` | A non-specialist should know what the indicator measures from the title alone. Expand acronyms unless universally known (skip GDP; expand GWIS, MFI, SDG, IHME). Don't cram units into the title. |
   | `description_short` | One or two short sentences: what the metric is and what it covers. No jargon without a gloss. Active voice. The chart subtitle is short by design — no run-on or stacked clauses. |
   | `description_key` | Each bullet should land a distinct, useful fact. Skip filler ("this dataset is widely used"); prefer substantive caveats (coverage gaps, methodology limits, what counts/doesn't count). |
   | `display.name` | Short legend label. Reads naturally on a chart axis/legend; doesn't restate the title. |
   | `presentation.grapher_config.note` | Concise footnote, ≤1 sentence ideally. |

   Flag and rewrite when you find:
   - Acronyms or technical terms that aren't expanded the first time they appear
   - Sentences that only make sense if you already know the data source
   - Quantitative claims with no unit context (e.g. "burned area" without "in hectares" surfacing somewhere in the user-facing text)
   - Inconsistent terminology between indicators in the same dataset (e.g. "wildfires" in one, "vegetation fires" in another)
   - Domain phrases that have a plain-English equivalent (e.g. "anthropogenic emissions" → "human-caused emissions")

   When a phrasing is ambiguous, propose a concrete rewrite — don't just flag it.

   If any skill rewrites a `.meta.yml`, re-run the affected step so the built catalog reflects the edits. **Add `--grapher` when the affected step is on the grapher channel** — without it the local catalog is updated but staging stays stale, so the step 7 indicator upgrade sees the old text.
   ```bash
   # garden / meadow:
   .venv/bin/etlr <channel>/<namespace>/<new_version>/<short_name> --private --force --only
   # grapher:
   .venv/bin/etlr grapher/<namespace>/<new_version>/<short_name> --grapher --private --force --only
   ```
   Then re-run the relevant check to confirm zero remaining violations.

6c) Indicator metadata coverage, dataset block, and link verification
   The other quality checks catch *content* issues; this step catches *missing fields* and *broken URLs* before they reach review.

   **Mandatory fields per indicator.** For every indicator in the garden `.meta.yml`, confirm these are set (either on `definitions.common` or per-indicator):

   | Field | Notes |
   |---|---|
   | `title` | Per-indicator |
   | `unit` | Common is fine |
   | `short_unit` | Common is fine |
   | `description_short` | Per-indicator |
   | `description_key` | At least one bullet; usually common |
   | `processing_level` | `minor` or `major` |
   | `presentation.topic_tags` | At least one tag |
   | `display.numDecimalPlaces` | Common is fine |
   | `display.tolerance` | Common is fine — chart tolerance for missing years |
   | `display.name` | **Per-indicator** — required for legend labels |
   | `presentation.attribution_short` | **Set explicitly** — does NOT inherit from the origin's `attribution_short` (verified: MySQL `variables.attributionShort` stays `NULL` if it's only on the origin). Place under `definitions.common.presentation` for the common case. |

   Conditional: if `processing_level: major`, every indicator with that level MUST also have `description_processing`.

   Not mandatory (skip if you don't need them): `presentation.title_public`, `presentation.title_variant`, `presentation.attribution`.

   **Dataset block.** Garden `.meta.yml` MUST include `update_period_days`:
   ```yaml
   dataset:
     update_period_days: <N>
   ```
   This controls the auto-update cadence. Even when the rest of the `dataset:` block is empty, **never strip `update_period_days`** — leave the block in place with just that field.

   **Link verification.** Run a HEAD request on every URL in the new `.dvc` and `.meta.yml` files (all channels — meadow `.meta.yml` files matter when they exist). Anything non-2xx is a hard blocker:
   ```bash
   for url in $(rg -No "https?://[^\"' ]+" snapshots/<namespace>/<new_version>/ etl/steps/data/{meadow,garden,grapher}/<namespace>/<new_version>/ \
       | sed -E 's/[).,;:>]+$//' \
       | sort -u); do
       printf "%s  %s\n" "$(curl -sI -o /dev/null -w '%{http_code}' --max-time 10 "$url")" "$url"
   done
   ```
   The `sed` strips trailing markdown/punctuation chars (`)`, `.`, `,`, `;`, `:`, `>`) so URLs inside `[text](url)` aren't reported as broken because of a stray closing paren. Fix any non-2xx hit on `url_main`, `url_download`, `license.url`, or URLs referenced from `description` / `description_key` before continuing.

   **Verification.** After editing, re-run the affected step (with `--grapher` if grapher) so the catalog reflects the changes. Then confirm `presentation.attribution_short` actually landed:
   ```python
   from owid.catalog import Dataset
   ds = Dataset("data/grapher/<ns>/<v>/<short_name>")
   tb = ds["<table>"]
   print(tb["<col>"].metadata.presentation.attribution_short)  # must NOT be None
   ```
   Or after the staging upload:
   ```bash
   make query SQL="SELECT shortName, attributionShort FROM variables WHERE catalogPath LIKE '%<ns>/<v>/<short_name>%'"
   ```

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

8b) Gather editorial context (shared by steps 9 and 9b)
   Both the Slack announcement (step 9) and the public Data update post (step 9b) need richer framing than the producer name alone. Read the following before drafting either, and keep the extracted material on hand (a short scratch list in the workbench is fine — `workbench/<short_name>/editorial-context.md` is a reasonable spot if it helps):

   - **Snapshot DVC files** at `snapshots/<namespace>/<new_version>/*.dvc` — read every file's `meta.origin` block. The richest fields are:
     - `description` — multi-paragraph source-level summary, usually the best raw material for "what the dataset shows" / "why it matters".
     - `description_snapshot` — per-table flavour, often a single sentence that names the specific slice (inequality, poverty, incomes, etc.).
     - `producer` and `attribution_short` — for the closing line of the public post and for the search URL.
     - `url_main` — the producer's landing page. **Visit it via WebFetch** when the existing metadata is thin or the post would benefit from a release-notes detail (what changed in this release, coverage extension, methodology change). Don't invent facts; lift them from the producer's page.
     - `citation_full` — gives the release date/version.
     - `date_published` — the actual release date for the Slack form's "When was this released?" field.
   - **Garden `.meta.yml`** at `etl/steps/data/garden/<namespace>/<new_version>/<short_name>.meta.yml` — read `dataset.description` plus per-indicator `description_short`, `description_key`, and `presentation.title_public`. These are OWID's user-facing framing of the same data — pick a phrase or finding from here if it lands more cleanly than the producer's wording.
   - **Step 8's chart picks** — for the closing-line link choice in step 9b and to anchor any specific finding either announcement highlights.

   Don't dump every field verbatim into the announcements — extract the 2–3 sentences that actually frame the dataset for a reader, then choose voice and format separately for the Slack form (step 9) vs. the public post (step 9b).

9) Slack announcement & PR update
   - Use the editorial context gathered in step 8b (snapshot DVC fields, garden `.meta.yml`, optionally `url_main` via WebFetch) to fill the template at `.claude/skills/update-dataset/slack-announcement-template.md`. Mechanical fields (producer, dates, coverage, chart count, search URL) come straight from the snapshot DVC + garden meta + step 8 count. Editorial fields ("what does this help users understand", caveats, anything interesting) come from the same context, rephrased for a stakeholder audience.
   - Include the 1–3 selected chart views from step 8
   - Ask user if unsure about any details
   - Save the draft to `workbench/<short_name>/slack-announcement.md`
   - **Add the announcement to the PR description** as a collapsed section titled "Slack Announcement"
   - **Post `@codex review` as a separate PR comment** (not in the PR description) to trigger an automated code review. Use:
     ```bash
     gh pr comment <pr_number> --body "@codex review"
     ```
   - Tell the user: "Slack announcement drafted at `workbench/<short_name>/slack-announcement.md` and added to the PR description. Please review and post it to **#data-updates-comms**."

9b) Data update post (for OWID /latest)
   Draft the short reader-facing post that gets published on [https://ourworldindata.org/latest](https://ourworldindata.org/latest). The team drafts these in **Google Docs** in the shared `/Data updates` Drive folder (`https://drive.google.com/drive/folders/1oL0uLHKI6f2qi1rJA6-qFFRYEBw_-rfm`), and OWID's CMS ingests the doc into the published feed.

   **The skill's job is to produce paste-ready Google Doc content** in the exact CMS format the team uses (frontmatter `title` / `excerpt` / `type` / `authors` / `kicker` → `\[+body\]` marker → body prose with inline markdown links → `{.cta}` block → `{.image}` block → `\[\]` end marker). Don't invent your own format — every published post in the Drive folder follows the same shape.

   This is **separate from the Slack announcement** — that one is a 10-field form for the internal channel; this one is a mini-blog-post for OWID readers, and the format is structured for CMS ingestion.

   Steps:
   - Open `.claude/skills/update-dataset/data-update-template.md` and follow it — the template has the exact paste-ready format plus three worked examples (NVIDIA, H5N1, World Bank PIP) lifted verbatim from the Drive folder.
   - Use the editorial context sources gathered in step 8b (snapshot DVC fields, garden `.meta.yml`, optionally `url_main` via WebFetch). Also pull from `workbench/<short_name>/slack-announcement.md` (step 9 output) — the editorial framing already drafted there is the closest cousin.
   - **Title shape** — a punchy finding/claim, a question, or an action/invitation. Not just the dataset name. See the template's "Field-by-field guidance" for examples and decision logic.
   - **Body** — 100–200 words, first-person, conversational. Sample: ATUS ~105, NVIDIA ~140, robots ~110, OECD Government at a Glance ~155, US data centers ~145, UNU-WIDER ~155, World Bank PIP ~190, ozone ~165, mobile money ~180, fertilizers ~170, H5N1 ~135. The body should give a reader a reason to care and at least one concrete number — not "I refreshed our charts".
   - **Inline markdown links** throughout the body for the producer's page, methodology pages, and related OWID articles. `*italics*` for emphasis, sparingly.
   - **CTA URL choice**:
     - One chart focus ⇒ grapher URL `https://ourworldindata.org/grapher/<slug>`.
     - Multiple charts (default) ⇒ search URL `https://ourworldindata.org/search?datasetProducts=<URL-encoded dataset title>` — value is the **dataset title**, resolved with this priority: (a) the `dataset.title` field in the garden `.meta.yml` if it's set there (an override), otherwise (b) the `meta.origin.title` field in the snapshot `.dvc`. Often includes a parenthetical acronym like `Luxembourg Income Study (LIS)` or `World Bank Poverty and Inequality Platform (PIP)`. **Not** the bare `producer` field.
     - Topic has an existing OWID explorer ⇒ `https://ourworldindata.org/explorers/<name>`.
     - Curated topic page exists ⇒ topic URL (e.g. `/sdgs`).
     - **Do not use** `/collection/custom?charts=…` URLs.
   - **CTA text** — descriptive: "Explore the updated data in our interactive charts" (default), "Explore all of the updated data in our interactive charts" (broad), "Explore the interactive version of this chart" (single chart), "Explore this data going back to YYYY in our interactive chart" (single chart with date depth).
   - **Image filename** — `YYYY-MM-data-update-<slug>.png` (e.g. `2026-04-data-update-h5n1-flu.png`). The skill doesn't generate the image; the user adds it to the Doc separately.
   - Save the draft to `workbench/<short_name>/data-update.md`.
   - **Add a collapsed `<details>` section titled "Data update post (for OWID /latest)"** to the PR description, placed *after* the Slack-announcement section.
   - Tell the user: `"Data update post drafted at workbench/<short_name>/data-update.md in the Google Docs CMS format. Please create a new Google Doc in /Data updates, paste the draft, attach the chart screenshot, and share for review."`

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

## Final QA hand-off — Anomalist + Chart Diff in Wizard

This is the **last step**, after the DAG archive has been committed. Don't auto-run these — they're human-judgment tools. Hand off the two staging links so the user can review and click through:

- **Anomalist** — flags variables whose new values diverge from the old version beyond statistical thresholds. Catches accidental scale changes, base-year rebases that propagated the wrong way, and silent drops.
  ```
  http://staging-site-<container_branch>/etl/wizard/anomalist
  ```
- **Chart Diff** — shows side-by-side before/after thumbnails for every chart that uses an upgraded indicator. Catches visual regressions the schema-level checks miss (axis ranges, color steps, legend changes).
  ```
  http://staging-site-<container_branch>/etl/wizard/chart-diff
  ```

**Important: derive `<container_branch>` correctly.** The staging hostname is **not** simply `staging-site-<branch>`. The container name is produced by `get_container_name(branch)` in `etl/config.py`:

1. Replace `/`, `.`, `_` with `-` in the branch name.
2. Strip a leading `staging-site-` if present.
3. **Truncate to the first 28 characters** (Cloudflare DNS limit).
4. Strip any trailing `-`.

Branches over 28 chars therefore get clipped. Example: `data-military-expenditure-2026` (30 chars) → container `data-military-expenditure-20` → hostname `staging-site-data-military-expenditure-20`. The simplest way to get the correct value is to call the helper:

```bash
.venv/bin/python -c "from etl.config import get_container_name; print(get_container_name('<branch>'))"
```

Tell the user something like: "Final QA: please review **[Anomalist](http://<container_name>/etl/wizard/anomalist)** and **[Chart Diff](http://<container_name>/etl/wizard/chart-diff)** in the Wizard. If anything looks off, let me know and I'll investigate."

These pages need a fresh staging build, so they're only meaningful after the PR's grapher upload to staging has completed and the staging server has rebuilt.

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
- `workbench/<short_name>/harmonization.log` and `harmonization_audit.md` (from step 5c)
- `workbench/<short_name>/indicator_upgrade.json` (if indicator-upgrader was used)
- `workbench/<short_name>/slack-announcement.md`
- `workbench/<short_name>/data-update.md` (public-facing post draft for OWID /latest, from step 9b)

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
