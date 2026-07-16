---
name: update-dataset
description: End-to-end dataset update workflow with PR creation, snapshot, meadow, garden, and grapher steps. Use when user wants to update a dataset, refresh data, run ETL update, or mentions updating dataset versions.
metadata:
  internal: true
---

# Update Dataset (PR → snapshot → steps → grapher)

Use this skill to run a complete dataset update with Claude Code subagents, keep a live progress checklist, and pause for user approval only when something needs attention.

> **Paired skill — keep in sync.** [`/review-data-pr`](../review-data-pr/SKILL.md) is the reviewer-side counterpart of this skill: it verifies the *outcomes* of the author-side steps defined here. Whenever you add, remove, or change a workflow step in this file, check whether `review-data-pr/SKILL.md` needs a matching reviewer-side check (and add it in the same commit if so). The reverse also holds — see the mirror note there.

## Inputs

- `<namespace>/<old_version>/<name>`
- Get `<new_version>` as today's date by running `date -u +"%Y-%m-%d"`
- A bare `<short_name>` (no namespace/version) is also valid — it's what owid-issues reminder bodies use. Resolve it to `<namespace>/<old_version>/<short_name>` via the DAG: `rg "/<short_name>:?$" dag/ -g "*.yml" | grep -v "^dag/archive"` — the `:?` matters because active entries are YAML keys ending in `:` (a `$`-anchored pattern without it only matches dependency lines), and archived entries must never be resolution targets. Take the latest active version; ask the user if the short name is ambiguous across namespaces. Several space-separated short names (`/update-dataset <short_name1> <short_name2>`) mean a grouped update of related datasets: run the full workflow for each, on one shared branch/PR.

Optional trailing args:
- branch: The working branch name (defaults to current branch)

Assumptions:
- All artifacts are written to `workbench/<short_name>/`.
- Persist progress to `workbench/<short_name>/progress.md` and update it after each step.
- Persist reusable update facts to `workbench/<short_name>/update-context.yml` as they are discovered. This is the canonical context artifact for the PR description, review handoff, and `data-updates-comms`.

## Progress checklist (maintain, tick live, and persist to progress.md)

- [ ] Parse inputs and resolve: channel, namespace, version, short_name, old_version, branch
- [ ] Clean workbench directory: delete `workbench/<short_name>` unless continuing existing update
- [ ] Run ETL update workflow via `etl-update` subagent (help → dry run → approval → real run)
- [ ] Add yourself to `dataset.owners` in the new garden `.meta.yml` (don't reorder; preserve existing names and markers)
- [ ] Catalog `# NOTE:` / `# TODO:` comments carried over from the old step files into `notes_to_check.md`
- [ ] Detect any `sanity_checks` functions and their log-control flags; append to `notes_to_check.md`
- [ ] Create or reuse draft PR and work branch
- [ ] Update snapshot and compare to previous version; capture summary
- [ ] Meadow step: run + fix + diff + summarize
- [ ] Garden step: run + fix + diff + summarize
- [ ] Review `sanity_checks` output (enable log flag, re-run, scan log, revert flag) — if none found and the garden step does non-trivial logic, recommend adding them; if present but missing value bounds (positive / [0,1] / [0,100] per indicator type), suggest those too (see 5b-bis)
- [ ] Country harmonization audit: validate `.countries.json` against canonical regions (flag provider regions not yet in the regions dataset → `/add-provider-regions`), audit `.excluded_countries.json`, scan garden log for missing/unused/unknown warnings
- [ ] Region-provider drift: if this dataset's aggregates are in `regions.yml` (`defined_by: <provider>`), check whether the new version changed the provider's region set or country membership; if so, update `regions.yml` and re-propagate via `/add-provider-regions`
- [ ] Grapher step: run + verify (skip diffs), or explicitly mark N/A
- [ ] Re-evaluate each catalogued `# NOTE:` / `# TODO:` against fresh data; delete resolved workarounds + comments together, or record status in PR body
- [ ] Check metadata: typos, Jinja spacing, style guide compliance
- [ ] Verify indicator-metadata coverage, `dataset.update_period_days`, snapshot DVC `date_published` and `citation_full` year (`etl update` copies both verbatim — bump to the producer's real release date / year, or to `date_accessed` / current year if the source doesn't publish one), and that all URLs resolve (HEAD-check)
- [ ] Scheduled-issue workflow check (owid-issues): locate the dataset's `update-*.yml` (exact / fuzzy / group match), verify cron vs the observed release cadence + `update_period_days`, filename convention, and that the issue body says to run `/update-dataset <short_name>`; auto-fix body/title, ask before cron changes or new workflows — commits go straight to owid-issues main (see 6d)
- [ ] Commit, push, and update PR description
- [ ] Run indicator upgrade on staging and persist report
- [ ] Update `update-context.yml` with published chart count and 1–3 chart views for the public announcement
- [ ] Render Slack announcement via `data-updates-comms`, save to workbench, post `@codex review` as a separate PR comment, and notify user to post it to #data-updates-comms
- [ ] Draft public-facing "Data update" post for OWID /latest, get the user's sign-off on the markdown, create the Google Doc in /Data updates, and hand the user the link (not added to the PR)
- [ ] Address Codex review comments (fix valid ones + resolve all threads)
- [ ] Run downstream-dependency check (`rg "<namespace>/<old_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"`); for each consumer outside the dataset's own chain, decide with the user whether to bump in this PR or document under "Downstream dependencies" for a follow-up PR (see "Downstream dependency check" section below for details)
- [ ] Run the silent-breakage check whenever downstream consumers were repointed in this PR: confirm the `buildkite/etl-automated-staging-environment` PR check is green (red = a consumer crashed on staging, and the report under-reports until it's fixed; `.venv/bin/etlr --modified --continue-on-failure --private` is the optional local equivalent for small fan-outs), then triage the data-diff report — every red "− lost N data point(s)" entry in its Top-changes list and every 🔴-tier dataset (see "Silent-breakage check" section) and run the full-report audit probes (structural / World / raw-country / >30% / wipe-vs-edge per loss)
- [ ] Ask the user whether to remove the old DAG entries; if yes, delete them and their files AND relocate the new entries into the old slot (see "Removing the old version & reordering the DAG") — don't forget this step
- [ ] Hand off the QA links to the user (Anomalist + Chart Diff on the staging branch, plus the data-diff report) — this is the final step

Persistence:
- After ticking each item, update `workbench/<short_name>/progress.md` with the current checklist state and a timestamp.

## Checkpoints — when to pause

**Default: keep going.** Run through the full workflow without stopping unless one of the conditions below is met.

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

## When the update isn't a drop-in version bump

Some updates carry structural changes that make the standard rename-only flow the wrong tool. Recognise them up front and adjust the workflow.

**Triggers** — any of these means you're in restructure territory, not a version bump:
- `short_name` changes (producer rebranded the dataset).
- File format/schema changes (wide → long, different file extension with a different column set, new dimensions).
- Policy/indicator set changes substantially (splits, dropped composites, newly added areas).
- Score semantics change (e.g. binary → continuous with subnational coverage).

**Workflow adjustments:**

1. **Skip `etl update`.** The rename-only flow copies the old step files into a new folder — useless when the schema is different. Author the new step chain by hand, using the old version as inspiration but not as a starting copy.
2. **Add the new chain to the DAG before removing the old.** Leave both chains active while you build and validate v2; remove the v1 entries only once v2 is on staging and the chart remap is queued or done.
3. **Decide on naming convention upfront.** Ask the user whether to preserve v1 short_names where they map cleanly, or to adopt the source's fresh naming scheme. Fresh naming is cleaner but means the auto-Indicator-Upgrader can't help.
4. **Hand-curate the v1 → v2 indicator mapping.** When short_names change entirely, the auto-upgrader has nothing to match on, but the Indicator Upgrader also matches on **`title`** — so if v2 titles are descriptive (full sentences rather than the bare short_name), you can hand the user a table of v1 title → v2 title pairs and they can drive the chart remap from there. Generate this table from the v1 meta.yml + the v2 grapher catalog.
5. **Defer the Slack and `/latest` announcements until charts have been remapped.** Both posts depend on `charts.published_count` and `charts.selected_views` from the v2 chain. Drafting them before the remap gives the wrong count (zero) and no representative views. Tell the user to ping you when the chart remap is done, then run steps 8 / 9 / 9b.

For the **long-format with dimensions** sub-case specifically (e.g. one row per `(country, year, <dim1>, <dim2>)`), use the modern OWID pattern:
- Meadow + garden: `tb.format(["country", "year", <dim1>, <dim2>, ...], sort_columns=True)`.
- Aggregations: `paths.regions.add_aggregates(tb, index_columns=[...full key...], regions=REGIONS, aggregations={...})`.
- Grapher: pass long tables through unchanged; the framework auto-expands them into per-cell variables.
- Metadata: variables are keyed by the long-column name, with `<% if <dim> == "X" and <dim2> == "Y" %>...<% endif %>` Jinja blocks inside `title`, `description_short`, `display.name`. Grep this repo for `tb.format(["country", "year"` with more than two index entries to find current reference examples.
- Jinja coverage: after building the grapher dataset, verify every active `(dim1, dim2)` cell renders a non-empty value — read every column from the built grapher dataset and assert `metadata.title` is non-empty. A dimension combination with no matching `<% if %>` branch ships an untitled indicator.

## Workflow orchestration

0) Initial setup
   - Check if `workbench/<short_name>/progress.md` exists to determine if continuing existing update
   - If starting fresh: delete `workbench/<short_name>` directory if it exists
   - Create fresh `workbench/<short_name>` directory for artifacts

1) Run ETL update command (etl-update subagent)
   - Inputs: `<namespace>/<old_version>/<short_name>` plus any required flags
   - **Pick the URI that matches what's actually changing:**
     - If the source data is changing — new source files, modified extractor, anything that affects the snapshot output — run from the **snapshot URI** with `--include-usages`. This bumps the whole chain (snapshot → meadow → garden → grapher) to the new version together:
       ```
       etl update snapshot://<ns>/<old_v>/<short>.<ext> --include-usages
       ```
     - **Foundational / widely-used datasets (e.g. `wb/*/income_groups`, `regions`, `population`): add `--direct-only`.** Plain `--include-usages` follows usages *transitively* and would try to version-bump every downstream consumer (income_groups has ~85 across 15 dag files). `--direct-only` restricts the bump to steps sharing the dataset's own `namespace/version/short_name`, i.e. just its chain. Caveat: `--direct-only` **excludes sibling steps with a different short_name** that belong to the same chain (e.g. `income_groups_aggregations`, which the grapher step also depends on) — pass those as **extra seed steps** so the grapher doesn't end up mixing a new-version garden with an old-version sibling. Dry-run and confirm the proposed set is exactly the chain before executing:
       ```
       etl update snapshot://<ns>/<old_v>/<short>.<ext> data://garden/<ns>/<old_v>/<sibling> --include-usages --direct-only --dry-run
       ```
     - If only garden logic / metadata is changing and the source data is unchanged, run from the **garden URI**. This bumps garden and grapher only; snapshot and meadow stay on the old version.
   - Either way, run `etl update` **once**. Don't call it separately per channel — that leaves stale version references in the DAG (e.g., new garden pointing to old meadow).
   - Perform help check, dry run, approval, then real execution; capture summary for later PR notes
   - After running, **always verify the dag file**: grep for the old version and confirm all internal references between the new steps point to the new version (e.g., garden depends on new meadow, not old meadow).
   - **`etl update` writes the new entries in the *flat* DAG form — convert them to the nested (compact) form now**, while you're in the file, rather than leaving it until archiving (otherwise the flat block tends to survive the whole update unnoticed). See the example and `load_dag()` parse-check under "DAG archiving & reordering" step 4.

1a-bis) Add yourself to `dataset.owners` in the new garden `.meta.yml`

   You've just become a contributor to this dataset, so add your canonical OWID name to its `owners:` list. Don't reorder — keep the existing primary first; append yourself at the end. Skip if you're already there.

   Your canonical name must match an entry in the schema enum (`schemas/dataset-schema.json`). Resolve it from `git config user.name` via `etl.owners.resolve_owner`; if that returns `None`, add a mapping in `etl/owners.py` and a row in the schema enum before continuing.

   Edit the YAML in place, preserving comments and the existing `# review` / `# backport` / `# fasttrack` markers on other entries.

1b) Check for outdated practices (check-outdated-practices skill)
   - After `etl update` creates new step files, run the `/check-outdated-practices` skill on **every** new step file — including helper modules that `etl update` doesn't generate but you copied by hand (e.g. `*_omms.py`), since those carry legacy patterns too
   - The skill reads the extension as the source of truth for the full pattern set (the `geo.add_*` aggregation/population helpers are flagged, not just `geo.harmonize_countries`) — don't rely on a remembered subset
   - Fix any findings before proceeding — this avoids propagating legacy patterns into new versions
   - **`geo.harmonize_countries` → `paths.regions.harmonize_names`** is mechanical and safe. **`geo.add_regions_to_table` → `paths.regions.add_aggregates`** changes the aggregation core — prove equivalence with a *controlled A/B test*, not a diff against the old feather. Build the new garden **both ways against the same current catalog** (swap the call, rebuild, save output; revert, rebuild, save output) and diff the two. Do NOT conclude "the helper shifts aggregates across all years" from a new-vs-old-feather diff — that conflates the helper with upstream-dataset drift (see step 5). In practice the two helpers are equivalent bar tiny historical edge cases (e.g. one region-year's population residual); if so, modernize. `add_aggregates` also auto-resolves income groups from the DAG, so it's the right tool when you later need WB income-group aggregates.

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

   **Hand-maintained snapshots + editorial data edits.** Some snapshots have no `url_download` — the `.py` prompts for `--path-to-file` and the docstring says the data was "provided by email" / curated by hand. When the update is a small editorial correction (the user gives you the facts directly, e.g. "country X did Y in year Z"), you can produce the new snapshot yourself: copy the *previous* version's data file (`data/snapshots/<ns>/<old_version>/<file>`), change only the specific cells, and **assert in a quick script exactly which rows/cells changed** (and that all others are byte-identical) before running `etls ... --path-to-file <edited>`. Then update the `.py` docstring to document the edit and bump the `.dvc` `date_published` / `citation_full` year (ask the user whether it's a new producer release or an OWID-applied edit — see step 6c). **Verify the user's stated facts against the existing data first** — some may already be encoded from a prior release (in this update, one of the two reported events was already in the live snapshot; only the other was a genuine change). Tell the user what's already present rather than blindly re-adding it.

4) Meadow step repair/verify (step-fixer subagent, channel=meadow)
   - Run, fix, re-run; produce diffs
   - Save diffs and summaries
   - **Watch for meadow input checks keyed on absolute row/column positions.** Producers quietly restructure their files (e.g. this session, the WB dropped the legend rows above the first country, shifting the row count 239→234 and moving "Afghanistan" from row 10 to row 5). Data extraction that keys off content (drop rows without an id, then melt) survives, but hardcoded `tb.loc[N]` / exact-row-count asserts break — update them to the new positions/counts and drop a `# NOTE` so the next maintainer re-checks. The break is the check doing its job; don't loosen it into uselessness.

5) Garden step repair/verify (step-fixer subagent, channel=garden)
   - Run, fix, re-run; produce diffs
   - Save diffs and summaries

   **Diff against a freshly-rebuilt old version, not the stale feather on disk.** The old version's `data/garden/.../` feather was built whenever it last ran — possibly against an *earlier* snapshot of a shared upstream dataset (population, regions, income_groups). A fresh build of the new version uses the *current* upstream, so a naive new-vs-old-feather diff shows differences in **every population-weighted cell across all years and regions** — pure upstream drift that has nothing to do with your change. Before trusting any diff, rebuild the old version on the current catalog and diff against *that*:
   ```bash
   .venv/bin/etlr data://meadow/<ns>/<old_version>/<short> --private --force --only
   .venv/bin/etlr data://garden/<ns>/<old_version>/<short> --private --force --only
   ```
   The apples-to-apples diff should collapse to just your intended change. Mention the drift separately in the PR (Chart Diff on staging *will* show it, because the live data is also stale relative to current upstream). This bit me twice in one update — don't skip it.

   **When NaN can appear on one side, don't let `.fillna(False)` hide it.** In a cell-by-cell diff, `(a - b).abs() <= tol` evaluates to `NaN` when exactly one side is NaN; a downstream `.fillna(False)` then silently drops that real one-sided change. Treat "one side NaN, other side not" as a difference explicitly.

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

5b-bis) Recommend sanity checks when the garden step lacks them (or lacks value bounds)
   Runs after the garden step is built. Two triggers:
   - Step 1d catalogued **no** sanity checks and the garden step does more than a straight load-and-format (harmonization, column drops/renames, aggregations, derivations) — recommend adding `sanity_check_inputs` / `sanity_check_outputs`. A Codex review will flag this anyway; better to handle it during the update.
   - The step **has** checks but you can spot missing value-bound coverage of the kinds below — suggest the additions.

   **Ground every threshold in the built data before writing it.** Run `min`/`max`/`nunique`/`value_counts` per indicator on the built garden table first — never write plausible-sounding bounds. The classic trap: "% of GDP" indicators look like percentages but legitimately exceed 100 (IMF PFMH: gross debt up to 495%, expenditure 595% in crisis years; UK's post-Napoleonic 260% debt is on a published chart). A blind 0–100 assert fails the build on day one.

   **Suggested checks by indicator type** (propose the applicable ones, not all):
   | Indicator type | Check |
   |---|---|
   | Share / proportion | values in [0, 1] |
   | Percentage of a whole | values in [0, 100] |
   | "% of GDP" and similar ratios | non-negative for levels (revenue, spending, debt); can exceed 100 — verify against data before capping. Where a ≤100 bound *mostly* holds, enforce it with a **documented exception set** (e.g. PFMH expenditure ≤ 100 outside `{Equatorial Guinea, Kuwait, Kiribati}` — each with a comment explaining why it's legitimate) so a new country crossing the line fails for review |
   | Balances, growth rates, interest rates | can legitimately be negative — do NOT impose ≥ 0 |
   | Mutually exclusive share categories | components sum to 100% (or 1) per (country, year), within a small rounding tolerance (e.g. `abs(sum - 100) < 0.1`) — first verify against the data that the source's categories are actually exhaustive (some breakdowns ship without an "other" remainder, or only for a subset of rows) |
   | Categorical flags / codes | exact value set (e.g. `<= {0, 1}`) and non-null |
   | Input schema | set-equality on expected columns — catches the next source rename (PFMH: `debt` → `d`) with a clear message |
   | Coverage | country-count floors (≥ previous version); a drop is usually a parsing/mapping regression — re-audit before bumping the constant |

   Implementation conventions: constants at the top, `run()` first, check functions **below** `run()`; `sanity_check_inputs(tb)` right after loading meadow, `sanity_check_outputs(tb)` right before `paths.create_dataset(...)`; plain `assert` with messages that name the offending values. Reference example: [`imf/2026-06-12/public_finances_modern_history.py`](../../../etl/steps/data/garden/imf/2026-06-12/public_finances_modern_history.py).

   **`sanity_check_outputs` runs *before* `tb.format(...)`, so column names are still as the code produced them — `format()` lowercases/underscores them afterward.** If the step builds columns like `f"{col}_{status}_pop"` where `status` is `"Legal"`/`"Illegal"`/`"missing"`, the pre-format columns are `status_Legal_pop` (mixed case), not `status_legal_pop`. Select such columns case-robustly (e.g. `[c for c in tb.columns if c.endswith("_pop")]` then filter on `c.lower()`), or you'll get a `KeyError` that only surfaces at runtime. Keep update-specific facts (e.g. "country X is Legal in year Y") out of the committed checks — verify those via the garden diff at update time so the checks stay valid across future releases.

   **Negative-test the checks**: after the step passes on real data, simulate each failure mode (rename a column, corrupt a flag, push a value out of bounds) and confirm the matching assertion fires — a check that never trips is untested code.

5c) Country harmonization audit
   Run after the garden step completes (and after 5b if it ran). Verifies that the country entities reaching the garden output are canonical, and that the mappings/exclusions consumed by `paths.regions.harmonize_names(...)` are well-formed. Output: `workbench/<short_name>/harmonization_audit.md`.

   **Modern API.** Garden steps should be calling `paths.regions.harmonize_names(tb, country_col=..., countries_file=..., excluded_countries_file=...)` — the wrapper in `etl/data_helpers/geo.py:1874`. If you find a step still using the deprecated `geo.harmonize_countries(...)` directly, step 1b's `/check-outdated-practices` should already have flagged it; treat that as a separate cleanup. The audit below is API-agnostic — both call sites end up emitting the same three warning strings. Some garden steps don't use the harmonizer at all and instead assign `country` inline in Python (no `.countries.json` involved); for those, the JSON checks below have nothing to look at — the garden-output check in step 5 is what catches non-canonical entities, so always run it.

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

      # Resolve the canonical regions dataset dynamically (latest built version).
      # Don't pin a date — when the regions step version advances, a hard-coded path
      # would validate against a stale catalog and flag valid targets as non-canonical.
      regions_dirs = sorted(Path("data/garden/regions").glob("*/regions"))
      if not regions_dirs:
          raise RuntimeError(
              "No data/garden/regions/<version>/regions built locally — the audit can't "
              "run without the canonical regions catalog. Build it first with "
              "`.venv/bin/etlr data://garden/regions/<latest>/regions --private`."
          )
      tb_regions = Dataset(str(regions_dirs[-1]))["regions"]
      canonical_regions = set(tb_regions["name"].dropna().astype(str))

      # Add OWID's official income-group aggregates to the canonical set, if available.
      # OWID treats the latest income_groups version as official. This artifact is
      # often not built locally during a non-income-groups dataset refresh — degrade
      # gracefully (warn and skip) rather than aborting the audit.
      ig_dirs = sorted(Path("data/garden/wb").glob("*/income_groups"))
      if ig_dirs:
          ds_ig = Dataset(str(ig_dirs[-1]))
          canonical_income = set(ds_ig["income_groups_latest"]["classification"].dropna().astype(str).unique())
      else:
          print(
              "[WARN] No data/garden/wb/<version>/income_groups built locally — "
              "skipping income-group enrichment. The four WB income-group aggregates "
              "(High/Upper-middle/Lower-middle/Low-income countries) may surface as "
              "'not in canonical' until you build that dataset."
          )
          canonical_income = set()

      canonical = canonical_regions | canonical_income

      mapping = json.loads(Path("etl/steps/data/garden/<namespace>/<new_version>/<short_name>.countries.json").read_text())
      not_in_canonical = sorted({v for v in mapping.values() if v and v not in canonical})
      print("Targets not in OWID's canonical regions or income groups:", not_in_canonical)

      # Provider regional groupings the source ships (e.g. "Europe (WB)", "Asia and the Pacific (ILO)")
      # surface here when OWID hasn't added that provider's regions yet. Flag the "(Provider)"-suffixed ones:
      import re
      provider_region_candidates = [v for v in not_in_canonical if re.search(r"\([\w .&-]+\)$", str(v))]
      print("Look like provider regions not yet in the regions dataset:", provider_region_candidates)
      ```
      A non-empty `not_in_canonical` list means the mapping points at entities that aren't registered in either the regions catalog or the income-groups dataset. This isn't automatically a bug — it's a heads-up. **Stop and decide with the user before proceeding** — same pattern as the global "Checkpoints — when to pause" section at the top of this skill. Common causes (in order from "fix" to "accept"): typo, retired alias used as canonical, casing/whitespace mismatch, or a custom aggregate the source defines that OWID has no equivalent for. For typos/casing — fix the JSON.

      **If the unmatched targets are a provider's regional grouping that OWID doesn't define yet** — the `provider_region_candidates` above (`"(Provider)"`-suffixed names like `"Europe (WB)"`, `"Sub-Saharan Africa (ILO)"`), or the source ships a `region`/`subregion` column feeding these — don't just accept them as outside-the-system. The proper fix is to **add that provider's regions to the regions dataset with the `/add-provider-regions` skill** (separate PR): they then become canonical, merge with regions/population infrastructure, and get a `{provider}_region` map indicator, instead of living outside the system. Surface this to the user and offer to run `/add-provider-regions`. Only for genuinely one-off, non-geographic groupings the regions system shouldn't own (BRICS, G7, G20) — accept and note in the PR description that those live outside the canonical system. For a real new historical region — add an entry to `regions.yml` in a separate PR.

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

   5. **Audit garden output entities.** Always run this check, regardless of whether `.countries.json` exists or is populated — JSON mappings describe *inputs* to the harmonizer, but the entities that actually reach Grapher are whatever sits in the `country` column/index of the built garden tables. Inline `country` assignments (e.g. hardcoded `tb["country"] = "England and Wales"`) and post-harmonization mutations both bypass the JSON check entirely; this is the only step that catches them.
      ```python
      from pathlib import Path

      from owid.catalog import Dataset

      garden_dir = Path("data/garden/<namespace>/<new_version>/<short_name>")
      ds_garden = Dataset(str(garden_dir))

      entities: set[str] = set()
      for tname in ds_garden.table_names:
          tb = ds_garden[tname]
          # `country` can live in the index (after .format()) or as a regular column.
          if "country" in tb.index.names:
              entities.update(tb.index.get_level_values("country").dropna().astype(str).unique())
          elif "country" in tb.columns:
              entities.update(tb["country"].dropna().astype(str).unique())
          # tables with no country column are silently skipped (e.g. reference tables)

      output_not_in_canonical = sorted(entities - canonical)
      print("Garden output entities not in OWID's canonical regions or income groups:",
            output_not_in_canonical)
      ```
      Same triage rules as the JSON-targets check (Python check #3): typo / casing / alias / legitimately custom aggregate. A non-empty list means at least one entity that ships to Grapher isn't registered in either the regions catalog or the income-groups dataset. **Stop and decide with the user before proceeding.** Common fixes: typo or casing → patch the inline assignment (or `.countries.json`, whichever is the source) so the value matches the canonical name; alias → switch to the canonical name; legitimate custom aggregate → accept and note in the PR description that the entity lives outside the canonical system.

   6. **Write findings** to `workbench/<short_name>/harmonization_audit.md` with six sections, populated only when non-empty. **Each section must list every flagged entity**, not just a count — counts alone aren't actionable, the user (or you) needs to read the actual names to judge whether each is intentional. For long lists (>20 entries) group by pattern when the grouping is obvious (e.g. ILO's `" (ILO)"`-suffixed regions vs. international orgs vs. derived "World ..." aggregates) so the reviewer can scan categories instead of one flat list. Sections:
      - `## Missing in mapping` — countries in source data not in `.countries.json` (from log warning #1) — list each missing source name
      - `## Unused mappings` — `.countries.json` entries the data never used (warning #2) — list each unused source→target pair
      - `## Unknown excluded entries` — `.excluded_countries.json` entries not present in source data (warning #3) — list each
      - `## Targets not in OWID's canonical regions or income groups` — target names from `.countries.json` that aren't registered in either dataset (Python check #3) — list each target name and the source names that map to it
      - `## Excluded entries matching canonical regions` — possible over-exclusion (Python check #4) — list each
      - `## Garden output entities not in OWID's canonical regions or income groups` — distinct `country` values found in the built garden tables that aren't in canonical regions or income groups (Python check #5) — list each entity

   7. **Surface in PR.** If any section was populated, add a collapsed "Harmonization audit" section to the PR description (after the per-step sections) **with the same listings**, not just a summary. Empty sections can be omitted.

   **When you report progress to the user during the workflow, never just give a count — always include the list (or grouped categories) so they can judge in one glance.**

   **Checkpoint summary:**
   - "Targets not in OWID's canonical regions or income groups" or "Garden output entities not in OWID's canonical regions or income groups" or "Missing in mapping" non-empty ⇒ stop, decide with user.
   - "Excluded entries matching canonical regions" non-empty ⇒ stop, ask whether each exclusion is intentional.
   - "Unused mappings" or "Unknown excluded entries" non-empty ⇒ surface in PR description; not a blocker.

5d) Region-provider drift — if this dataset *defines* OWID regions, propagate the change
   The harmonization audit (5c-3) catches providers **not yet** in `regions.yml`. This check is the counterpart: a provider whose aggregates are **already** in `regions.yml` can change its regions on a version bump (new/removed/renamed regions, or shifted country membership), and `regions.yml` won't update itself.

   - **Does this dataset own regions?** `grep "defined_by: <provider>" etl/steps/data/garden/regions/2023-01-01/regions.yml` (e.g. `ilo_1`/`ilo_2`, `maddison`, `wid`, `wb`, `who`). No matches ⇒ skip this step.
   - **Re-extract** the provider's region→country mapping from the **new** version — from the same source the regions were originally derived from (a `region`/`subregion` column, region entities, or a table-of-contents tier; see `/add-provider-regions` Step 1) — and **diff it against `regions.yml`** (`defined_by: <provider>` members), set-equality per region, plus new/removed/renamed regions. If the new version no longer carries the composition, the provider's section in the [world-region-map-definitions](https://ourworldindata.org/world-region-map-definitions) article links to the defining source — use it to re-derive the provider's **existing** regions (regions newly introduced by this update won't be documented there yet — get those from the source or ask the user).
   - **If it drifted:** **stop and decide with the user** (composition changes move every region-aggregated value downstream), then update `regions.yml` and re-propagate via **`/add-provider-regions`** — rebuild garden regions, refresh the `{provider}_region` grapher indicators + metadata, regenerate owid-grapher `regions.data.ts` (`runRegionsUpdater`), and update the article section.
   - **If unchanged:** note it and move on.

6) Grapher step run/verify (step-fixer subagent, channel=grapher, add --grapher)
   - Skip diff

6a) Re-evaluate `# NOTE:` / `# TODO:` items from step 1c against fresh data
   Now that meadow, garden, and grapher have run on the **new** data, go back to `workbench/<short_name>/notes_to_check.md` and decide each item's fate. For each entry:

   - Identify what the workaround does (read the surrounding code).
   - Load the affected step's output with `owid.catalog.Dataset` (or inspect the raw snapshot) and compare **corrected vs. uncorrected** values. Cross-check the producer's release notes / changelog if available.
   - If the upstream issue is fixed → delete the workaround **and** its `# NOTE:` / `# TODO:` comments **in the same commit**, then re-run the affected step (use `--force --only`, add `--grapher` for grapher) so downstream artifacts pick up the change.
   - If the workaround is still needed → leave it and add a one-line status under a PR-description section titled **"Not covered in this PR"** (e.g. "Sierra Leone ×1000 correction still required — raw value in the 2026 file is still ~1/1000 of plausible"). These are deliberately deferred items the next updater should re-check. Delete the whole section if its last item gets resolved mid-PR.
   - If you're uncertain → keep it, flag it in the PR description, and ask the user.

   Do this **before** step 6b (metadata checks) so any re-runs triggered by comment-removal happen before the metadata sweep, not after.

6b) Metadata quality checks — run after all ETL steps are built
   Run all four checks on the newly built garden and grapher datasets so every issue surfaces together. Each skill writes results to the terminal; fix what comes up before moving on.

   - **Typos** — `/check-metadata-typos` scoped to the current step. Run on each of the new `.meta.yml` files (garden first, then grapher). Accept or skip each suggested fix.
   - **Jinja spacing** — `/check-metadata-spacing` on the built garden and grapher datasets. Catches template artifacts like doubled spaces or stray newlines that only appear after Jinja rendering.
   - **Style guide** — `/check-metadata-style` on the grapher step. Audits user-facing fields (title, subtitle, description_short, display.name, presentation.*) against OWID's Writing and Style Guide. Rules live in `.claude/skills/check-metadata-style/STYLE_GUIDE.md`, so no Notion access is usually needed — the skill checks the file's `Last synced from Notion` date and refreshes it from Notion (in a separate PR) when it is more than two months old.
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

   **Snapshot DVC freshness.** `etl update` clones the previous snapshot's `.dvc` content verbatim except for `date_accessed`. Always re-check `date_published` and the year in `citation_full` / `attribution` under `snapshots/<ns>/<new_version>/*.dvc` — they will otherwise silently ship the old version's values. Set `date_published` to the producer's real release date when discoverable; otherwise copy `date_accessed`. Bump the year in `citation_full` and `attribution` to match.

   - **Citation year vs `date_published` year.** After setting both, check whether the year inside `citation_full` / `attribution` matches `date_published`'s year. If they differ, confirm it's intentional before shipping: it's legitimate when the producer labels the release by *edition* rather than publish date (e.g. UN IGME's "2025 report" published `2026-03-17` → `citation_full` `(2025)`, `date_published` `2026`), but otherwise it's a stale citation. When the gap is deliberate, leave a one-line note for the reviewer so they don't re-flag it.

   - **`Last-Modified` header as `date_published` source.** When the producer's page states no release date (common on fully JS-rendered sites like the IMF Datamapper), the download URL's HTTP `Last-Modified` header is a defensible source — it's the server's own timestamp for the file, not an inference. Corroborate it against a release-named filename (e.g. `…Dec 2025.xlsx` + `Last-Modified: Fri, 12 Dec 2025`) and note the provenance when reporting to the user.
   - **Stale producer description on a JS-rendered page.** If the `.dvc` `meta.origin.description` is producer text that may have changed but the page is an SPA shell (static HTML empty, WebFetch 403s, Wayback archives only the shell), don't burn time probing API endpoints and don't rewrite the producer's text to match the data — the blurb can legitimately lag their own releases (FPP shipped 153 countries while the page said 151). **Ask the user to paste the page text from their browser**, then diff it against the existing `.dvc` text and apply only the substantive changes. Clipboard pastes flatten typography (curly quotes → straight, en-dashes → hyphens) — keep the existing typographic punctuation unless the words themselves changed.

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

   **Link verification.** Run a HEAD request on every URL in the new `.dvc` and `.meta.yml` files (all channels — meadow `.meta.yml` files matter when they exist). Anything non-2xx is a *signal*, not a guaranteed break — always double-check before acting:
   ```bash
   for url in $(rg --no-filename -No "https?://[^\"' ]+" snapshots/<namespace>/<new_version>/ etl/steps/data/{meadow,garden,grapher}/<namespace>/<new_version>/ \
       | sed -E 's/[).,;:>]+$//' \
       | sort -u); do
       printf "%s  %s\n" "$(curl -sI -L -o /dev/null -w '%{http_code}' --max-time 15 -A 'Mozilla/5.0' "$url")" "$url"
   done
   ```
   The `--no-filename` flag prevents `rg` from prepending `path:` to each match (otherwise the for-loop tries to curl `path:url` and every check returns 000). `-A 'Mozilla/5.0'` sometimes coaxes a real response out of Cloudflare-fronted hosts, but it doesn't always work — see the next note.

   **`curl` non-2xx ≠ broken.** Cloudflare-fronted sites (notably `ourworldindata.org`) can return **404** to curl on URLs that work fine in a browser, depending on edge-node routing, IP geolocation, and cached state. Before treating a 4xx as a real failure:

   1. **Re-check with `WebFetch`** (the built-in tool). It uses a different code path and a `Mozilla/5.0` UA that Cloudflare usually accepts. A `200` with a coherent page body is authoritative — trust it over curl.
   2. **If `WebFetch` also fails**, sanity-check the Wayback Machine: `https://web.archive.org/web/<year>/<url>`. A recent successful snapshot means the URL is reachable on the public internet and your local route is the problem.
   3. **Only act on a true failure** — both `WebFetch` *and* Wayback unable to reach the URL — and even then **flag and ask the user before silently rewriting an external link in metadata**. Replacing a working link with a "safer" alternative because of a curl false-positive is worse than leaving the original. Apply the same restraint here as the global "Checkpoints — when to pause" section.

   Fix any genuinely-non-2xx hit on `url_main`, `url_download`, `license.url`, or URLs referenced from `description` / `description_key` before continuing. The `sed` strips trailing markdown/punctuation chars (`)`, `.`, `,`, `;`, `:`, `>`) so URLs inside `[text](url)` aren't reported as broken because of a stray closing paren.

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

6d) Scheduled-issue workflow check (owid-issues)
   Every recurring data update is driven by a scheduled GitHub Actions workflow in the `owid/owid-issues` repo (`.github/workflows/update-*.yml`) that periodically opens a "Data update" issue. The conventions live in the Notion page ["Scheduled data issues"](https://app.notion.com/p/owid/Scheduled-data-issues-f166359059634634b0053f78101bca81): schedule anything updated at least once per year but less than daily; filename `update-{namespace}-{short_name}.yml`; a cron `schedule:` trigger + `imjohnbo/issue-bot` creating the issue. This step runs now because 6c just established the two cadence facts the cron must match — `dataset.update_period_days` and the producer's actual release rhythm (`source.release_date` / `next_release` in `update-context.yml`).

   **Locate the workflow.** The filename convention is loosely followed in practice, so search in widening circles — a miss on the exact name proves nothing:
   1. Use the local checkout `~/owid-issues` if present (`git -C ~/owid-issues pull` first); otherwise clone it (`gh repo clone owid/owid-issues ~/owid-issues`). Don't fall back to a `gh api …/contents` filename listing — the checks below need file *contents* (content grep for group workflows, cron/body/assignees parsing), and the commit step needs a working tree anyway.
   2. Exact conventional name `update-<namespace>-<short_name>.yml` → fuzzy filename match (hyphen/underscore swaps, dataset-title words — e.g. `update-gallup-ai-indicator.yml` covers `gallup/ai_indicator`) → content grep (`rg -il "<short_name>|<namespace>|<title words>" ~/owid-issues/.github/workflows/`).
   3. **Group workflows count.** One workflow may cover a family of related datasets (e.g. `update-climate.yml` → `/climate-update`; the quarterly `update-war-ucdp-preview-q*.yml` set; an "… + OMM" title covering a derived chain). If a group workflow covers this dataset, verify that workflow — don't create a per-dataset duplicate.

   **If found, verify three things:**
   1. **Frequency + timing.** Parse the `cron:` line. The implied period must be consistent with `update_period_days` *and* with the release cadence observed this update. Check the timing within the cycle too: the issue should fire shortly **after** the producer's expected publication window, never before (existing precedents: Gallup `0 8 15 */3 *` — mid-month, just after the wave publishes; OECD health expenditure `0 0 8 7 *` — right after the early-July release). If this update revealed that the cadence or window shifted, propose a new cron with a `#` comment in the YAML explaining the timing choice (matching the existing style) — **cron changes need user sign-off before committing**.
   2. **Naming.** The filename should be `update-{namespace}-{short_name}.yml`. Deviations → flag in the report only; don't rename (churn, and behavior doesn't depend on the filename). Exception: a file missing its `.yml`/`.yaml` extension is genuinely broken — GitHub Actions silently ignores it — fix that without asking.
   3. **Actionable issue body.** The body should name the dataset (no version — versions go stale) and tell the next updater to activate the Claude skill:
      ````yaml
      title: "Data update: <dataset title>"
      body: |
        Update the <dataset title> dataset (`<namespace>/<short_name>`).

        To run the update with Claude Code, run:

        ```
        /update-dataset <short_name>
        ```
      ````
      For group workflows, keep it a **single command listing every member dataset** — `/update-dataset <short_name1> <short_name2>` — or point at the family skill (e.g. `/climate-update`). If the body lacks the `/update-dataset` pointer or references a renamed path, refresh it — body/title fixes are auto-applied and reported afterwards, no need to ask. Also check `assignees:` still points at the dataset's current owner; flag a mismatch, don't auto-change.

   **If not found:** per the Notion rule, any dataset with `update_period_days` roughly in [2, 366] should have a scheduled issue (err on the side of scheduling too much). Propose creating one: copy an existing workflow as template (`imjohnbo/issue-bot@v3.3.6` shape; keep `close-previous: false` and its WARNING comment), cron shortly after the expected release window, `assignees:` = the GitHub handle of the human directing this update (team table in CLAUDE.md), filename per the convention, title/body per the template above. If this update touched several related datasets, propose **one grouped workflow** rather than several. **Creating a new workflow needs user sign-off.**

   **Committing.** Commit in `~/owid-issues` straight to `main` — the standing exception to the branch-first rule; no branch, no PR — with an emoji+🤖 message (e.g. `🔨🤖 Point <short_name> update issue at /update-dataset`), and push. Record the outcome (workflow file, cron, verdict, changes made) in `progress.md` and set `source.scheduled_issue_workflow` in `update-context.yml`. Nothing about this lands in the etl PR body beyond the existing tracking-issue link.

7) Indicator upgrade (optional, staging only)
   - First upload the new grapher dataset to the staging DB (required before the upgrader can detect it):
     ```bash
     STAGING=<branch> .venv/bin/etlr data://grapher/<namespace>/<new_version>/<short_name> --grapher --private
     ```
     **Then confirm the variables actually landed in MySQL** — `data://grapher/... --grapher` sometimes only builds the feather without upserting (observed: 0 rows in `variables` afterward). If the count is 0, run the separate `grapher://` step, which does the MySQL upsert:
     ```bash
     # verify
     STAGING=<branch> .venv/bin/python -c "from etl.config import OWIDEnv; print(OWIDEnv.from_staging('<branch>').read_sql(\"SELECT COUNT(*) n FROM variables WHERE catalogPath LIKE %(p)s\", params={'p':'%<namespace>/<new_version>/<short_name>%'}).n[0])"
     # if 0, force the upsert:
     STAGING=<branch> .venv/bin/etlr grapher://grapher/<namespace>/<new_version>/<short_name> --grapher --private
     ```
   - Then run the automatic upgrader:
     ```bash
     STAGING=<branch> .venv/bin/etl indicator-upgrade auto
     ```
   - **`auto` can detect nothing for a legitimate version bump** ("No dataset migrations detected. Nothing to do."). Don't conclude there's nothing to remap — fall back to an explicit mapping: pair old/new variable ids by `shortName` across the two dataset ids, store with `WizardDB.add_variable_mapping(...)`, preview with `cli_upgrade_indicators(dry_run=True)`, then apply (mechanics under "Indicator Upgrader CLI for one-shot chart remaps" in Guardrails). Map **all** indicators, not just charted ones — the full mapping is also what gives Anomalist's upgrade detectors complete coverage (see Final QA).
   - **CRITICAL**: After the upgrader finishes, always verify it actually worked by querying staging:
     ```bash
     mysql -h "staging-site-<branch>" -u owid --port 3306 -D owid -e "SELECT COUNT(*) FROM chart_dimensions cd JOIN variables v ON cd.variableId = v.id WHERE v.catalogPath LIKE '%<namespace>/<new_version>%'"
     ```
     If the count is 0, the upgrade did not run — re-run it.
   - **The auto-upgrader only remaps grapher charts — NOT ETL-defined explorers or MDims.** Explorers (`export://explorers/...`) and multidims (`export://multidim/...`) reference indicators by catalog path and are rebuilt by running their **export steps**, which the indicator-upgrader never touches. If the dataset has any (check the DAG: `rg "export://(explorers|multidim)/.*/<short_name>" dag/ -g "*.yml"`), they'll still point at the **old** variables on staging until you re-run them:
     ```bash
     STAGING=<branch> .venv/bin/etlr export://explorers/<ns>/latest/<short> export://multidim/<ns>/latest/<short> ... --export --private
     ```
     Verify none still reference the old version (both queries should return empty):
     ```bash
     # explorers
     mysql -h "staging-site-<branch>" -u owid -P 3306 -D owid -e "SELECT DISTINCT ev.explorerSlug FROM explorer_variables ev JOIN variables v ON ev.variableId=v.id WHERE v.catalogPath LIKE '%<ns>/<old_version>%'"
     # mdims
     mysql -h "staging-site-<branch>" -u owid -P 3306 -D owid -e "SELECT DISTINCT mdp.slug FROM multi_dim_x_chart_configs mx JOIN variables v ON mx.variableId=v.id JOIN multi_dim_data_pages mdp ON mdp.id=mx.multiDimId WHERE v.catalogPath LIKE '%<ns>/<old_version>%'"
     ```
   - **Also verify narrative charts.** Narrative-chart configs can pin a `variableId` in their own patch (not inherited from the parent chart), and that id can date from a version *older* than the one this update started from — left stale by a previous cycle. The auto-upgrader only carries `old_version → new_version` mappings, so it can never remap those, and the `chart_dimensions` count above can't catch them either: narrative-chart variable ids live only inside `chart_configs`. The upgrader **warns** about this case ("was NOT remapped: it pins indicators from a version of the upgraded dataset that the mapping does not cover") — watch its output for that warning. But the upgrader only visits narrative charts whose **parent chart was affected** by the mapping; a stale narrative chart whose parent no longer uses any mapped indicator is never visited and stays silent. So always run this catch-all scan over all narrative-chart configs:
     ```python
     # STAGING=<branch> .venv/bin/python — scan narrative chart configs for variables on ANY old version
     import json
     from etl.config import OWID_ENV
     old_vars = set(OWID_ENV.read_sql(
         "SELECT v.id FROM variables v JOIN datasets d ON d.id = v.datasetId "
         "WHERE d.catalogPath LIKE %(p)s AND d.catalogPath NOT LIKE %(new)s",
         params={"p": "%/<short_name>", "new": "%<new_version>%"})["id"])
     df = OWID_ENV.read_sql("SELECT nc.id, nc.name, nc.parentChartId, JSON_EXTRACT(cc.full, '$.dimensions') AS dims "
                            "FROM narrative_charts nc JOIN chart_configs cc ON cc.id = nc.chartConfigId")
     stale = [(r["id"], r["name"], d["variableId"]) for _, r in df.iterrows() if r["dims"]
              for d in json.loads(r["dims"]) if d.get("variableId") in old_vars]
     print(stale)  # must be empty
     ```
     If any are found, remap them with an explicit mapping via the upgrader's own CLI helpers (load by parent chart id; `cli_upgrade_indicators` won't reach them because it finds charts via `chart_dimensions`):
     ```python
     from sqlalchemy.orm import Session
     import etl.grapher.model as gm
     from apps.indicator_upgrade.upgrade import push_new_narrative_charts_cli
     from etl.config import OWID_ENV
     with Session(OWID_ENV.engine) as session:
         ncs = gm.NarrativeChart.load_narrative_charts_by_parent_chart_ids(session, [<parent_chart_id>])
     ncs = [nc for nc in ncs if nc.id in {<stale_nc_ids>}]
     errors = push_new_narrative_charts_cli(ncs, {<old_var_id>: <new_var_id>})
     ```
   - **Pick `<new_var_id>` by matching the PARENT chart, not by short_name.** A stale narrative pin is often a *legacy, pre-dimensional* variable — an old PPP year, or a flat short_name like `headcount_215` — with **no short_name twin** in the new dataset, so the version-bump mapping (and any `shortName` join) can't reach it. A narrative chart is just a framing of its parent, so the correct target is **whatever indicator the parent chart uses now** (after the regular-chart upgrade has run). Read the parent's current `variableId`s and map each stale pin to the parent's equivalent by indicator role:
     ```python
     import json
     pc = OWID_ENV.read_sql("SELECT cc.full FROM charts c JOIN chart_configs cc ON c.configId=cc.id WHERE c.id=%(i)s", params={"i": <parent_chart_id>})["full"].iloc[0]
     parent_var_ids = [d["variableId"] for d in json.loads(pc)["dimensions"]]  # the targets to map onto
     ```
     Different narrative charts (and even one chart's multiple pins) can come from *different* stale versions, so build the mapping **per parent**, not with one global dict. In a real run this meant e.g. a $2.15/2017-PPP legacy pin → the parent's current $3/2021-PPP variable.
   - **`push_new_narrative_charts_cli` takes the mapping directly** (no `WizardDB.add_variable_mapping` needed) — pass `{stale_id: parent_var_id}`.
   - **The auto `upgrade` only visits narrative charts when ≥1 regular chart is upgraded in the *same* run.** Re-running `etl indicator-upgrade` after the regular charts are already remapped is a silent no-op for narratives — call `push_new_narrative_charts_cli` directly instead.
   - **Verify with the merged config, not the stored one:** `AdminAPI(OWID_ENV).get_narrative_chart(id)["configFull"]` reflects the live (parent+patch) state; `chart_configs.full` can be stale and will mislead you into thinking nothing changed.
   - **Watch for stale FAUST overrides.** These charts often also pin an old subtitle/footnote (e.g. "$2.15 per day" / "2017 prices") that no longer matches the parent. `push_new_narrative_charts_cli` migrates the *indicator* but only **warns** about the text — the fix is to reset the flagged field to the parent's exact text to restore inheritance (identical values drop out of the patch). **Always ask the user before changing FAUST**: it's reader-facing editorial text (Footnote, Axis, Unit, Subtitle, Title), so confirm the reset/rewrite first — never fold a FAUST change silently into the indicator upgrade. (Use `_find_stale_faust_overrides(patch, parent_config, mapping)` to list exactly which fields are stale; set only those to the parent's value and PUT via `AdminAPI.update_narrative_chart`. Leave numeric display overrides like `tolerance` alone unless asked — they may be intentional.)
     Then re-run the scan and confirm it's empty.

8) Update context for public announcement
   - Maintain `workbench/<short_name>/update-context.yml` as the canonical record of facts discovered during the update. Do not wait until the end if a fact is already known; append/update as each step completes.
   - At minimum, record:
     ```yaml
     dataset:
       namespace: <namespace>
       old_version: <old_version>
       new_version: <new_version>
       short_name: <short_name>
       title: <public dataset title, if known>
       producer: <producer, if known>
     source:
       release_date: <snapshot origin date_published, if known>
       next_release: <best-effort, or null>
       url_main: <source page, if known>
       citation_full: <citation, if known>
       scheduled_issue_workflow: <owid-issues update-*.yml filename, or null (from 6d)>
     coverage:
       year_min: <garden min year>
       year_max: <garden max year>
       countries: <distinct countries/entities>
       includes_regions: <true/false>
       sparse_recent_year_note: <note, or null>
     charts:
       published_count: <published chart count>
       size_qualifier: <handful|moderate|large|massive>
       explorers: <list of published explorer slugs using this data, or []>
       mdims: <list of MDim slugs using this data, with published flag, or []>
       selected_views:
         - title: <chart title>
           slug: <chart slug>
           rationale: <why this represents the dataset>
     update_summary:
       snapshot_diff: <short summary or artifact path>
       meadow_diff: <short summary or artifact path>
       garden_diff: <short summary or artifact path>
       notable_changes: []
       sanity_check_findings: []
       resolved_workarounds: []
     editorial_context:
       why_it_matters_snippets: []
       caveat_snippets: []
       interesting_update_snippets: []
     ```
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
   - **Charts are not the only surface — also count the explorers and MDims that use this data.** Many datasets feed published OWID explorers and multi-dimensional collections, which the grapher-`charts` query above misses entirely. Run the export steps first (step 7) so these point at the new variables, then query both:
     ```sql
     -- Explorers (note isPublished — only published ones count for the announcement)
     SELECT DISTINCT ev.explorerSlug, e.isPublished
     FROM explorer_variables ev
     JOIN variables v ON ev.variableId = v.id
     JOIN explorers e ON e.slug = ev.explorerSlug
     WHERE v.catalogPath LIKE '%<namespace>/<new_version>%';

     -- MDims (note published flag — drafts are published=0)
     SELECT DISTINCT mdp.slug, mdp.published
     FROM multi_dim_x_chart_configs mx
     JOIN variables v ON mx.variableId = v.id
     JOIN multi_dim_data_pages mdp ON mdp.id = mx.multiDimId
     WHERE v.catalogPath LIKE '%<namespace>/<new_version>%';
     ```
     Chart-based explorers can also attach via `explorer_charts.chartId` (join through `chart_dimensions`) rather than `explorer_variables` — check that table too if the variable-based query comes up empty but the DAG shows an explorer step. Record published explorers/MDims under `charts.explorers` / `charts.mdims` in `update-context.yml`, and fold them into the "How many charts did this update affect?" answer (e.g. "10 published charts, 3 explorers, plus 3 draft MDims"). **Only count published surfaces** (`isPublished=1` / `published=1`) toward the public announcement; note unpublished ones for QA.
   - Map the published **chart** count to `size_qualifier`: 1–9 = `handful`, 10–49 = `moderate`, 50–199 = `large`, 200+ = `massive`.
   - Pick 1–3 `selected_views` using these criteria (in order of preference):
     - **Map views** — immediately visual, readers can find their own country
     - **Charts with punchy, standalone headlines** — titles that make a clear claim work best for social sharing
     - **Global trend charts** (StackedArea / World) — show the big picture over time
     - **Skip**: population-weighted variants (harder to read quickly), within-regime breakdowns (too niche), country-specific views
   - Add snippets for the editorial prompts from source metadata, garden/grapher metadata, resolved sanity-check/workaround notes, and non-routine PR changes. Keep these as snippets/facts, not polished Slack prose.

9) Slack announcement
   - Run the `data-updates-comms` skill with `workbench/<short_name>/update-context.yml` as input. `data-updates-comms` is the canonical owner of the Slack form wording, copy-paste format, editorial framing, search URL, and any standalone fallback gathering. Do not duplicate that rendering logic here.
   - Save the rendered draft to `workbench/<short_name>/slack-announcement.md`.
   - If `data-updates-comms` reports missing mechanical fields, gather them, update `update-context.yml`, and re-render rather than inventing values. Ask the user if a missing field requires judgment.
   - **Do not put the announcement in the PR at all** — no embed and no pointer. The draft stays as the `workbench/<short_name>/slack-announcement.md` file (the user copies from there); comms drafts are internal and are kept out of the public data-update PR.
   - **Post `@codex review` as a separate PR comment** (not in the PR description) to trigger an automated code review. Use:
     ```bash
     gh pr comment <pr_number> --body "@codex review"
     ```
   - At the end of the update, tell the user, with a **markdown link to the saved file** so they can click through to open it: `"Slack announcement drafted at [workbench/<short_name>/slack-announcement.md](workbench/<short_name>/slack-announcement.md). Please review and post it to #data-updates-comms."` Always render the path as a markdown link `[…](…)`, not as inline-code — the chat UI renders it as clickable that way. (Slack can't be auto-posted — the user posts it.)

9b) Data update post (for OWID /latest)
   Draft the short reader-facing post that gets published on [https://ourworldindata.org/latest](https://ourworldindata.org/latest). The team drafts these in **Google Docs** in the shared `/Data updates` Drive folder (`https://drive.google.com/drive/folders/1oL0uLHKI6f2qi1rJA6-qFFRYEBw_-rfm`), and OWID's CMS ingests the doc into the published feed.

   **The skill's job is to produce paste-ready Google Doc content** in the exact CMS format the team uses (frontmatter `title` / `excerpt` / `type` / `authors` / `kicker` → `[+body]` marker → body prose with inline markdown links → `{.cta}` block → `{.image}` block → `[]` end marker). Don't invent your own format — every published post in the Drive folder follows the same shape.

   This is **separate from the Slack announcement** — that one is a 10-field form for the internal channel; this one is a mini-blog-post for OWID readers, and the format is structured for CMS ingestion.

   Steps:
   - Open `.claude/skills/update-dataset/data-update-template.md` and follow it — the template has the exact paste-ready format plus three worked examples (NVIDIA, H5N1, World Bank PIP) lifted verbatim from the Drive folder.
   - Use the facts already gathered in `workbench/<short_name>/update-context.yml` (step 8) — `dataset.title`, `dataset.producer`, `source.url_main`, `source.citation_full`, `coverage.*`, `charts.published_count`, `charts.selected_views`, and the `editorial_context.*` snippet lists. Also pull from `workbench/<short_name>/slack-announcement.md` (step 9 output) — the editorial framing already drafted there is the closest cousin. If a field needed for the post isn't yet in `update-context.yml`, gather it (snapshot DVC, garden `.meta.yml`, or `url_main` via WebFetch) **and persist it back** to the YAML so the next consumer doesn't re-do the work.
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
   - **Propose the draft as markdown and get sign-off before creating the Doc.** Show the user the full post rendered as markdown (in chat) so they can read it and request edits inline, and ask whether they're happy to publish it as a Google Doc. Iterate on `workbench/<short_name>/data-update.md` until they approve — *then* create the Doc. This ordering matters: the Drive API can't edit or delete a Doc once created, so getting the content approved first avoids orphaned drafts. (Like the Slack draft, the /latest post is **not** added to the PR.)
   - **Once the user approves, create the Google Doc** in the `/Data updates` folder so they don't have to copy-paste, using the Drive MCP `create_file` tool. Match the folder's title convention (e.g. "2026-06 Data update: Homicides").
     - **Upload styled HTML** (`contentMimeType: "text/html"`), applying the OWID CMS colors and indents from the template's "OWID CMS Doc styling" table (blue keys, black frontmatter values, grey body, orange `[+body]`/`[]`, green `{...}` markers, blue-underline links; body indent 10pt, `url:`/`text:`/`filename:` 20pt). Verified: HTML import preserves colors + `margin-left` indents + hyperlinks, and lands a fully-styled doc that needs no add-on pass. This is preferred over a `text/markdown` upload (which gives hyperlinks but no OWID colors/indents).
     - **Encode all non-ASCII as HTML entities** (e.g. `&mdash;` for an em-dash). Entities decode correctly on import; **raw 4-byte chars (emoji) mojibake** to `ð`, so use the entity form (`&#NNNNN;`) if you ever need one.
     - Each CMS line is its own `<p>` (one paragraph per line, no empty "spacer" paragraphs — the team's docs are compact). Empty `<p></p>` between sections is fine and matches the reference docs.
     - **Verify** with `download_file_content(exportMimeType="text/html")` and confirm the `color:` / `margin-left:` styles survived. The MCP has **no delete or edit-content tool**, so get it right on the first `create_file`; a bad create leaves an orphan Doc the user must delete manually — don't recreate on a trivial issue (tell the user the one-line fix instead).
     - The **OWID GDocs Add-on** (Extensions menu in the Doc) is the team's canonical formatter; it can't be run via the API, so it's an optional human validation pass on top of the styled upload. If the shared folder rejects the write, create in My Drive and share the link.
   - **Register the Doc in the CMS.** A Google Doc only reaches the `/latest` feed once it's added at the OWID admin GDocs page — [https://admin.owid.io/admin/gdocs](https://admin.owid.io/admin/gdocs) — where the doc ID is registered so it can be previewed and published. This is a human action (admin login required); the skill can't do it via the API, so include it in the handoff.
   - At the end of the update, tell the user with **markdown links** to both the created Doc and the local draft, and **make clear it's a first draft for people to modify** (not a finished post): `"Data update post created at [<Doc title>](<doc viewUrl>) in /Data updates — this is a first draft for you and the team to review and edit. Please refine the copy as needed, add the chart screenshot, add the doc at https://admin.owid.io/admin/gdocs to bring it into the CMS, then preview, publish, and share. Draft source: [workbench/<short_name>/data-update.md](workbench/<short_name>/data-update.md)."`

10) Codex review: address comments and resolve threads
   - **Codex's delivery channel depends on the verdict — poll both.** A **clean pass** arrives as an *issue comment* ("Didn't find any major issues") from `chatgpt-codex-connector[bot]`, with zero inline comments and no formal review object. A review **with findings** arrives as a formal review ("💡 Codex Review") with inline comments, and *no* issue comment. A watcher polling only one channel waits forever on the other outcome — treat a hit on either as completion.
   - Wait ~60 seconds after posting `@codex review`, then poll both channels:
     ```bash
     gh api repos/owid/etl/issues/<pr_number>/comments | python3 -m json.tool   # clean-pass summary lands here
     gh api repos/owid/etl/pulls/<pr_number>/comments | python3 -m json.tool    # findings land here as inline comments
     ```
   - **Codex posts in one of two places — always check both.** When it finds issues, it leaves *inline review comments* (the endpoint above) with resolvable threads. When it finds **nothing**, it posts a single top-level **PR (issue) comment** instead — no inline comments, no threads — e.g. "Codex Review: Didn't find any major issues. Keep it up!". So if the inline-comments endpoint is empty, check the issue comments before concluding Codex hasn't run yet. A third shape exists: a findings review whose finding lives **only in the review body** (no inline comments, no resolvable threads) — list `gh api repos/owid/etl/pulls/<n>/reviews` and read each new review's `body`; polling only the two comment endpoints misses it (there is no thread to resolve — reply via a normal PR comment instead):
     ```bash
     # clean-pass summaries land in the issue comments:
     gh api repos/owid/etl/issues/<pr_number>/comments \
       --jq '.[] | select(.user.login | test("codex";"i")) | .body'
     # review-body-only findings (the third shape) — no thread, no issue comment:
     gh api repos/owid/etl/pulls/<pr_number>/reviews \
       --jq '.[] | select(.user.login | test("codex";"i")) | .body'
     ```
     A "no issues" / 👍 comment from `chatgpt-codex-connector[bot]` means the review is done and there's nothing to address — don't keep polling for inline comments that will never come.
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
   - If neither the inline-comments endpoint nor the issue-comments endpoint shows a Codex post after 60 s, wait another 60 s and retry (up to ~5 min total). Codex can take 5–10 min — a clean review often arrives only as the top-level "no issues" comment.

## Committing and pushing

Commit and push incrementally as you go — after each step that produces code changes. Don't wait until the end. Use descriptive commit messages with appropriate emojis (the one auto-prepended by `etl pr` for the chosen category + 🤖 for AI-written code).

**Verify the branch immediately before every commit** (`git branch --show-current`). The session shares its checkout with the user's IDE — a branch switch there silently moves your shell too, and a commit then lands on whatever branch the IDE left behind. The failure is quiet: `git push -u origin <branch>` pushes the (empty) local PR branch, reports "Everything up-to-date", and the PR stays empty. Recover by cherry-picking the stray commit onto the right branch (don't force-move the other branch's pointer).

Keep the PR-body draft under `workbench/<short_name>/` (or re-fetch it with `gh pr view <num> --json body --jq .body` before each edit) — not in the session scratchpad. The body gets re-edited throughout the update, and scratchpad files don't survive session resumes.

At the end of the workflow, update the PR description with:
- A **tracking-issue link** as the first line of the Summary — e.g. `Tracks: [owid/owid-issues#NNNN](https://github.com/owid/owid-issues/issues/NNNN)`. Most data updates have a corresponding `owid-issues` ticket; try to find it by searching the title or `<short_name>` first, and **ask the user for the issue number if you can't locate one** rather than skipping the link silently.
- A summary of key changes at the top
- Collapsed `<details>` sections **only for the pipeline steps that changed in a non-obvious way**. Skip any step that's just the boilerplate generated by `etl update` — don't add a placeholder like "unchanged from boilerplate". The Summary already explains the why; per-step sections are only for the how, when the how isn't obvious from the diff.

## Downstream dependency check

After completing the update, check if any other datasets depend on the **old** version of the updated dataset:

```bash
rg "<namespace>/<old_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"
```

Filter out the old dataset's own DAG entries (snapshot → meadow → garden → grapher chain). Any remaining references are **downstream dependents** that still point to the old version.

If downstream dependents exist, **decide with the user** whether to bump them in this PR or defer to a follow-up:
- **Tell the user** which datasets depend on the old version.
- **Follow-up PR (default for a big fan-out):** add a "Downstream dependencies" section to the PR description (not collapsed) listing the dependents, to be repointed in a separate PR. This mirrors the historical two-PR pattern for foundational datasets (e.g. income_groups: chain-update PR, then a "🐝 Update all datasets to latest …" bulk-bump PR).
- **Bump in this PR (if the user wants it self-contained):** repoint every downstream ref and remove/archive the old chain in the same PR. Mechanics that bit this session:
  - Bulk-replace with a **negative-lookahead** so a prefix match doesn't corrupt sibling short_names — e.g. `re.sub(r"garden/wb/<old_v>/income_groups(?!_)", "garden/wb/<new_v>/income_groups", text)` leaves `income_groups_aggregations` alone.
  - **Remove the old own-chain block from `dag/main.yml` *before* the bulk sweep**, or the sweep turns the old definition into a duplicate of the new key. Relocate the new block into the old slot (nested form) as part of the same edit.
  - Downstream datasets **keep their own version and variable IDs** — only their *dependency* on the updated dataset changes — so **no chart remapping is needed for them**; their aggregates just recompute against the new data (visible in Chart Diff). The indicator upgrade (step 7) still only concerns charts that use the updated dataset's *own* variables.
  - This is the only case where "Removing the old version" happens in the same PR — otherwise the old chain must stay until the follow-up repoints its consumers.

## Silent-breakage check (downstream builds + value diff)

A foundational-dataset update can leave a downstream step **building cleanly while quietly dropping data** — a region whose aggregate can no longer be computed goes NaN, a reclassified country disappears, a join stops matching. Nothing raises; the feather is written; the gap only surfaces on a chart weeks later. Two existing commands cover it — no bespoke tool:

**1. Do all downstream consumers still build?** Staging answers this on every push — run the command locally only when you want the answer before pushing:

```bash
.venv/bin/etlr --modified --continue-on-failure --private              # add --dry-run to list scope first
```

`--modified` detects the steps changed vs `origin/master` and expands to their **full transitive downstream** via the branch DAG (same machinery as chart-diff), runs them in dependency order, skips dependents of failed steps, and ends with a failure summary + non-zero exit. **Staging runs the same check on every push**: its bake (`ops/templates/owid-site-staging/etl-build.sh`) is `etl run garden grapher explorers --modified --grapher --private --continue-on-failure` with `PREFER_DOWNLOAD=1`, and `--continue-on-failure` re-raises the first failure at the end — so any consumer crash turns the **`buildkite/etl-automated-staging-environment`** PR check red. That makes the local run optional fast feedback, not the primary net. The caveat that matters: **while that check is red, the data-diff report under-reports** — dependents of the failed step are skipped, stay stale in the catalog, and diff as unchanged. Always confirm the check is green before trusting the report. Use `--workers N` to parallelize a big local fan-out.

**Size the fan-out first** (`--dry-run` lists the scope). Small (≲50 steps): the local run gives you the crash check in minutes, before burning a staging cycle. Foundational-dataset scale (hundreds of steps — an income-groups bump is ~560): skip the local build and rely on the staging check — locally it costs ~35 min and ~7 GB of `data/` to duplicate what the bake does anyway (if you do run it, delete the builds afterwards; `data/` is regenerable cache). Skip the **local** `etl diff` at that scale too — another ~25 min and a JSON in the hundreds of MB; owidbot's hosted report on the PR is the same comparison for free.

**2. How much did their outputs change?** The data-diff **report** answers this directly — it ranks everything by anomaly score (BARD, the metric Anomalist uses) and makes data loss unmissable, so you read its verdicts instead of scanning the diff yourself:

- **On the PR (zero effort):** open owidbot's **data-diff** HTML report (`https://catalog.ourworldindata.org/diffs/<sanitized_branch>/data-diff.html`, easiest via the **full report** link in owidbot's PR comment — the path keeps the branch name's dots and underscores and replaces only characters outside `[A-Za-z0-9._-]` (e.g. `/`) with `-` — unlike the staging *subdomain*, which does replace `.`/`_`) — it compares the staging build (new dependency) against production (old dependency). For a dependency bump the consumers' code is unchanged, so this is a clean old-dep-vs-new-dep comparison.
- **Locally, after step 1:** `.venv/bin/etl diff REMOTE data/ --changed --include garden --output-html data-diff.html`.

How to read it, in order:

1. **"Top changes — what to watch"**: red **"− lost N data point(s): labels…"** entries lead both the Datasets and Indicators lists — each one is a coverage loss (the silent-breakage signature) and must be triaged: legitimate churn, or a silent drop? Below the losses come the biggest value changes with their median anomaly scores.
2. **Tier strip + coverage chips**: 🔴 datasets (score ≥ 15% or any coverage loss) need review; 🟡 a skim; 🟢 is rounding-level noise. A red `− N row(s) removed: …` chip on a dataset row always means data points disappeared, regardless of how small the share.
3. **Filters**: the tier dropdowns isolate 🔴 datasets/indicators; **📝 metadata-only** separates pure metadata edits from value changes.

**Full-report audit — deciding whether code changes are needed.** For a dependency bump, "churn or bug?" is answerable mechanically, because only the dependency changed and consumer code didn't. Load the report JSON (`DiffReport.from_json`) and run these probes over **all** changed datasets, not just the Top-changes lists — each should come back empty or confined to dep-derived entities; anything else is a code-change candidate:

1. **Structural**: new/removed tables or columns anywhere → a dep bump must never do this.
2. **"World" rows changed** → some step computes World by summing the dep's groups (anti-pattern: World should come from the source or the full country set).
3. **Raw-country rows changed** → consumer code entangled with the dep beyond aggregation. (In bilateral datasets, verify every changed pair has an aggregate side; the country names that show up in samples are often just other dimension values — disaster types, element codes, age groups — so check dim *columns*, not string looks.)
4. **Any indicator with >30% of rows changed** → more than the aggregate rows moved.
5. **Wipe vs edge, for every coverage loss**: does the lossy entity still hold rows in the new build? Losing a few % at sparse edges (an aggregate-year-item combo emptying because members moved out, or dropping below a coverage threshold) is churn; an entity losing **all** its rows or going all-NaN is the bug signature — see the must-have trap below.

The samples cap at ~100 rows/diff, so pair the probes with the mechanism argument (only the dep changed) rather than treating them as a row-by-row proof.

**The must-have trap (real case: `population` + `unaids`, FY2027 income groups).** Some steps pin specific member countries as *required* for a group's aggregate. When the producer reclassifies a pinned country out of its group, the **entire aggregate nulls for all years** — a total-entity wipe, not edge churn. Two nets catch it: the geo must-have guard raises `ValueError` at build time (so the build check above fails loudly), and probe 5 is the backstop. The fix is updating the pinned list to the new membership — so whenever bumping income groups, check the repointed consumers' pinned member lists up front. Beware the rationalization risk: a wiped income-group aggregate *looks like* the expected churn the rest of the report is full of, and on a foundational dataset (population) it also announces itself as a dataset-count explosion — hundreds of consumers suddenly changed. Run probe 5 on every loss, however plausible the churn story.

**Don't diff against stale local builds** — a consumer built at an unknown earlier time (or downloaded from the catalog) conflates code drift with the dependency change and produces false positives; `REMOTE`/production is the trustworthy baseline because CI built it from master.

**Complementary, not a replacement:** data-diff sees every dataset (including ones with no charts), while **[Chart Diff](#final-qa-hand-off--anomalist-chart-diff-and-data-diff)** shows how the same changes land on actual published charts and **Anomalist** flags per-country anomalies in the new data — the final QA hand-off (last checklist step) covers all three. Use data-diff to find *which* datasets/indicators to worry about, then Chart Diff to judge what readers would actually see.

- **When you deferred consumers to a follow-up PR:** the checks above belong to that follow-up PR; here just confirm the "Downstream dependencies" list is complete (`.venv/bin/etlr --modified --dry-run` shows the affected set).

## Removing the old version & reordering the DAG

After the ETL update, `etl update` appends the new version entries to the **bottom** of the main DAG file while the old version's entries stay in their original slot. **Always ask the user** whether to remove the old version — but never skip this checklist item, and when the user agrees, always do the reorder too.

Workflow when the user agrees:

1. **Delete the old version.** Remove its entries (snapshot → meadow → garden → grapher) from the main DAG file (e.g., `dag/poverty_inequality.yml`) and delete its files (`etl/steps/...`, `snapshots/...`). The archive dag (`dag/archive/*.yml`) is **not** edited by hand — `etl archive-dag` reconstructs it from git history, recording each removed step with the commit where it was last active (for recovery via `git checkout`).
   - **Commit the removal BEFORE running `etl archive-dag`.** The tool reconstructs from *committed* git history — an uncommitted working-tree removal is invisible to it, and it will instead pick up whatever earlier removals are already committed. Sequence: edit dag + delete files → commit → `etl archive-dag` → scope → commit the archive.
   - **Branch runs are squash-safe (since #6412).** When run on a feature branch, the recovery marker points at the merge-base with `origin/master` (a master commit that survives the squash-merge), branch-only transient steps (created and reverted within the PR) are skipped automatically (`archive_dag.skip_branch_only_step` in the log), and re-runs don't churn markers that are already valid — only markers whose SHA is unreachable from master get replaced. No post-merge fixup pass is needed; committing more to the PR after archiving is fine.
   - **`etl archive-dag` reconciles the *entire* archive, not just your dataset.** If the archive was stale, one run can append **hundreds of unrelated lines** (e.g. this session pulled in ~180 lines across `climate.yml`/`education.yml`/etc. plus marker comments) — noise that doesn't belong in a data-update PR and that Codex will question. Keep the commit scoped: after running it, `git checkout -- dag/archive/` to drop the unrelated files, then re-add **only** your dataset's block to `dag/archive/main.yml` (copy the exact entry `archive-dag` generated, including its `# archived; last active in <sha> on <date>` marker). The block you keep is genuine tool output, so it stays consistent with future full regenerations.
2. **Move the new entries into the old slot** so the dataset stays grouped with its neighbours and section comment. The new entries should not remain at the bottom of the main DAG.
3. Preserve the original section comment (same indentation as the old block) above the new entries.
4. **Prefer the nested (compact) DAG format.** `etl update` emits the *flat* form (each step a separate top-level key with a flat dep list); the loader (`etl/dag_helpers.py:_parse_dag_yaml`) also accepts the **nested** form, where the chain is declared inline and flattens to the same graph. The nested form is the team's preferred style and is usually what the archived old block already used:
   ```yaml
   data://grapher/<ns>/<v>/<short>:
     - data://garden/<ns>/<v>/<short>:
       - data://garden/regions/2023-01-01/regions
       - data://meadow/<ns>/<v>/<short>:
         - snapshot://<ns>/<v>/<short>.csv
   ```
   Convert the relocated new entries to nested while reordering, so the active and archived blocks match. Verify it parses with `python -c "from etl.dag_helpers import load_dag; load_dag()"` (a malformed nesting raises).
5. Verify: `rg "<namespace>/<old_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"` returns nothing, and `rg "<namespace>/<new_version>/<short_name>" dag/ -g "*.yml"` shows the entries only in the main file (under the section comment), not at the bottom.
6. Run `make check` and commit with `🔨🤖 Remove old <name> entries and reorder DAG`.

**Expect a Codex false-positive on the archive edit.** Because this step touches `dag/archive/*.yml`, Codex often flags it ("avoid updating archived DAG entries" — the AGENTS.md rule against editing archived files). This is expected: archiving *is* the explicitly-requested workflow step, and the rule's own "unless explicitly asked" exception applies. Reply citing that and resolve the thread — don't revert the archive. A second recurring flag on this step: Codex warns that removing the old chain will "leave the published chart on archived variables" / "remap charts before versioning the grapher step". If the indicator upgrade already ran on staging (step 7) and the old-variable scan came back empty, reply with that verification (the remapped configs sync to production on merge) and resolve.

## Final QA hand-off — Anomalist, Chart Diff and data-diff

This is the **last step**, after the DAG archive has been committed. Don't auto-run these — they're human-judgment tools. Hand off the three links so the user can review and click through:

- **Anomalist** — flags variables whose new values diverge from the old version beyond statistical thresholds. Catches accidental scale changes, base-year rebases that propagated the wrong way, and silent drops.
  ```
  http://staging-site-<container_branch>/etl/wizard/anomalist
  ```

  **Check the upgrade detectors' coverage before handing off.** Anomalist's `upgrade_missing` / `upgrade_change` detectors only compare old→new variable pairs from the wizard's variable-mapping table — and the indicator upgrader persists mappings **only for charted indicators**. If only some of the dataset's indicators are used in charts (the common case), the upgrade detectors silently skip the rest, and a partial mapping suppresses the shortName-inference fallback that would otherwise cover everything. Verify with `WizardDB.get_variable_mapping_raw()`: if it has fewer pairs than the dataset has indicators, rebuild the full mapping by shortName (old vs. new `variables` rows by `datasetId`) and re-run:
  ```bash
  STAGING=<branch> .venv/bin/etl anomalist --anomaly-types upgrade_missing --anomaly-types upgrade_change \
      --dataset-ids <new_dataset_id> --variable-mapping '<full json mapping>' --force
  ```
  Then spot-check the stored `anomalies.dfReduced` rows include indicators beyond the charted ones.

  The upgrade detectors also need the **old** grapher dataset in the local catalog (`data/grapher/<ns>/<old_version>/<short>`) — the `FileNotFoundError` names the *new* dataset id, but the missing files are usually the old version's. If the old chain has already been removed from the DAG (so `etlr` can't rebuild it), fetch its files straight from the public catalog:
  ```bash
  mkdir -p data/grapher/<ns>/<old_v>/<short> && cd data/grapher/<ns>/<old_v>/<short> && \
    for f in index.json <short>.feather <short>.meta.json; do \
      curl -sL -O "https://catalog.ourworldindata.org/grapher/<ns>/<old_v>/<short>/$f"; done
  ```
- **Chart Diff** — shows side-by-side before/after thumbnails for every chart that uses an upgraded indicator. Catches visual regressions the schema-level checks miss (axis ranges, color steps, legend changes).
  ```
  http://staging-site-<container_branch>/etl/wizard/chart-diff
  ```
- **data-diff report** — dataset/indicator-level value comparison of the staging build against production, ranked by anomaly score, with data-point losses leading its Top-changes list and coverage loss forcing the 🔴 tier (see "Silent-breakage check" for how to read it). Covers every dataset the update touched, including ones with no charts — the perspective Anomalist and Chart Diff don't have. Easiest access: the **full report** link in owidbot's data-diff PR comment. The direct URL keeps the branch name's dots and underscores and replaces only characters outside `[A-Za-z0-9._-]` (e.g. `/`) with `-` — unlike the staging subdomain, which does replace `.`/`_` (and it is not the truncated container name either):
  ```
  https://catalog.ourworldindata.org/diffs/<sanitized_branch>/data-diff.html
  ```

**Bulk-approve the easy chart diffs with `etl approve` before handing the rest to the human.** On a dataset with many charts (e.g. WDI has 400+), most pending diffs exist only because the update changed no values a human needs to eyeball — either the underlying data is byte-identical (a version bump minted new variable IDs but the values didn't change) or it changed by a negligible source-revision amount. Reviewing those by hand in Chart Diff is wasted effort; let `etl approve` clear them first:

```bash
.venv/bin/etl approve --dry-run                        # exact data match only — safe default, see counts first
.venv/bin/etl approve --dry-run --allow-small-changes   # also count tiny source revisions (see below)
.venv/bin/etl approve --allow-small-changes             # apply for real once the dry-run counts look right
```

- Plain `etl approve` only approves a chart when every dimension's underlying data is byte-identical between staging and prod (it hashes each dimension's actual data, not the raw variable ID — so a version bump that changed no values still gets approved).
- `--allow-small-changes` additionally approves charts where the only remaining difference is a handful of small-magnitude value changes (typical source revisions) — tune with `--tolerance-pct` (default 1% relative change per point), `--tolerance-abs-floor` (default 1e-6, guards near-zero values), `--max-changed-points` (default 5 per dimension, above which it's sent to manual review regardless of magnitude), and `--max-new-points` (default 1000 per dimension — new-coverage points, e.g. a fresh year, are given a generous allowance since they're expected from a routine update, but an unexpectedly large coverage jump still gets sent to manual review). It still requires every other part of the chart's config (title, subtitle, everything but the dimension values) to be identical.
- `--show-data-diff` prints the actual before/after values for skipped charts (per dimension: y/x/size/color) instead of just a hash mismatch — useful to see *why* a specific chart didn't qualify, or to sanity-check whether raising `--tolerance-pct` would be safe. Combine with `--chart-id <id>` to inspect one chart.
- Whatever's left after `etl approve` is what's actually worth a human's attention in Chart Diff — genuine content changes, added/removed country-year coverage, or other config differences.

**Important: derive `<container_branch>` correctly.** The staging hostname is **not** simply `staging-site-<branch>`. The container name is produced by `get_container_name(branch)` in `etl/config.py`:

1. Replace `/`, `.`, `_` with `-` in the branch name.
2. Strip a leading `staging-site-` if present.
3. **Truncate to the first 28 characters** (Cloudflare DNS limit).
4. Strip any trailing `-`.

Branches over 28 chars therefore get clipped. Example: `data-military-expenditure-2026` (30 chars) → container `data-military-expenditure-20` → hostname `staging-site-data-military-expenditure-20`. The simplest way to get the correct value is to call the helper:

```bash
.venv/bin/python -c "from etl.config import get_container_name; print(get_container_name('<branch>'))"
```

Include owidbot's data-diff summary in the hand-off so the user knows the scale before clicking: pull the `<summary><b>data-diff</b>: …</summary>` line from owidbot's PR comment (`gh pr view <num> --json comments`) — e.g. `❌ 21 changed · 2 new · 4 identical · 16 skipped`. If it reports removed datasets or errors, call those out explicitly.

Tell the user something like: "Final QA: please review **[Anomalist](http://<container_name>/etl/wizard/anomalist)** and **[Chart Diff](http://<container_name>/etl/wizard/chart-diff)** in the Wizard, and the **[data-diff report](https://catalog.ourworldindata.org/diffs/<sanitized_branch>/data-diff.html)** for the dataset-level view — owidbot's summary: *❌ 21 changed · 2 new · 4 identical · 16 skipped*. If anything looks off, let me know and I'll investigate."

These pages need a fresh staging build, so they're only meaningful after the PR's grapher upload to staging has completed and the staging server has rebuilt.

## Guardrails and tips

- **`END_YEAR` / "as of" framing for status/event datasets.** When a dataset records *events* (and derives a status time series) and its latest event year lags the release date, you face a choice: forward-fill the latest status to the release year, or stop the series at the last event year and note the "as of" date in metadata. **Prefer the latter** — forward-filling invents data points for years with no source information (and shifts an `END_YEAR`-style constant ripples through the whole series). Keep the series at the last real year and add the currency note to `description_processing` and a `description_key` bullet (e.g. "The legal status shown for each country reflects the situation as of <Month Year>."). Confirm the choice with the user; they may change their mind (in this update we forward-filled to the release year, then reverted to the last event year + an "as of" note).
- **OECD SDMX dataflow versions bump on new releases — a pinned URL goes 404/`NoRecordsFound`.** The Data Explorer's "Developer API" links pin `df[vs]`/the REST path to a dataflow version (e.g. `DSD_SHA@DF_SHA,1.0`); when the producer publishes a new edition they may mint `1.1` and empty the old version, so last cycle's known-good URL returns `NoRecordsFound`. On that error, list versions with `GET /public/rest/dataflow/<agency>/<id>/all` and retry with the newest. For reader-facing links (url_main, /latest posts) prefer the **version-less** explorer deep link (`data-explorer.oecd.org/vis?df[ds]=DisseminateFinalDMZ&df[id]=<id>&df[ag]=<agency>`), which always resolves to the latest release; in the snapshot's `url_download`, pinning the version is fine (deterministic) — just expect to bump it each cycle.
- **Re-test "manual upload" snapshots — the blocking may be inverse-UA.** When a snapshot's docstring says the file is uploaded manually because "the website blocks the download request", verify that claim before carrying it into the new version. Some hosts (e.g. the IMF Datamapper) reject *browser-like* User-Agents with 403 while letting plain, honestly-identified clients through — the inverse of the usual bot-blocking — and the ETL downloader's default UA (`DEFAULT_USER_AGENT` in `etl/download_helpers.py`) is browser-like, so the original author may have misdiagnosed an automatable source. Test both directions (plain `requests` vs. browser UA) against the direct file URL. If the plain UA works: set `url_download` in the `.dvc` and pass `user_agent="owid-etl/1.0 (https://ourworldindata.org)"` (or similar plain UA) to `snap.create_snapshot(...)`. **Keep the snapshot `.py` script in that case** — the script-less `.dvc`-only path (`run_snapshot_dvc_only`) calls `create_snapshot()` without a `user_agent` and would 403 — and say so in the docstring so nobody deletes it as "redundant".
- **Manual-upload snapshots: also re-check for a stable download endpoint.** Distinct from the inverse-UA case above: producers add direct links over time, so a snapshot that genuinely required a manual download last cycle may be automatable now. Check the producer's download page or API for a stable (ideally version-less) URL before carrying the manual flow forward; if one exists, convert the snapshot to a script-less `url_download` `.dvc`. Multi-file archives stay script-less too: snapshot the archive itself and read the member file in meadow via `snap.extracted()`. If the bundle includes a codebook or series-metadata file, consider passing it through meadow as an extra table and attaching per-indicator `description_from_producer` in garden — inventory its fill rates first, and skip fields owned elsewhere (units, license, dataset-level boilerplate).
- **Audit blanket transformation rules per indicator before trusting them.** Any garden rule that applies one transformation to a pattern-matched *group* of indicators — unit scaling by title keywords, sign conventions, currency or magnitude conversions — assumes the source stores the whole group in a single convention. Sources mix conventions, especially when different indicators come from different upstream providers. Scan each matched indicator's raw range to confirm it fits the assumed convention, split the rule where it doesn't, and guard both sides with sanity checks (inputs within the assumed convention; outputs within a data-grounded bound). **"Matches the previous version" is not evidence of correctness** — magnitude bugs are inherited from the old step; judge absolute plausibility against real-world values, not just old-vs-new equality. (This caught a long-standing 100× inflation: fraction-stored share indicators sharing a ×100 rule with ratios the source already stored in percent.)
- **Programmatic metadata: still curate the charted indicator(s).** When a dataset's indicator metadata is generated in code (titles/units inferred from source names), the indicators actually used in charts deserve an explicit per-indicator block in the garden `.meta.yml` — `description_short`, `description_key`, `display` — layered on top (YAML merges per-field, so code-set fields like `description_from_producer` survive). Ground the bullets in the producer's codebook/methodology PDF rather than inferring from the data: the codebook yields the precise scope, the exact numerator/denominator, and caveats you won't guess (e.g. WWBI approximates the EEA 2004–2018 public sector from industry classifications). Leave a `# NOTE:` in the YAML explaining the programmatic/curated split for the next maintainer.
- **Scraped chart embeds: the page's own data tables are the canonical source — embed CDNs lag.** When a snapshot's data lives in a chart embedded on the producer's page (Datawrapper and similar), don't fetch the chart platform's CDN endpoint (`datawrapper.dwcdn.net/<id>/<version>/dataset.csv`): the latest *published* chart version can trail the page by a full release (observed with Gallup's AI indicator: the page's tables already carried the May 2026 survey wave while the chart CDN's newest version still ended at February 2026 — caught by Codex, not by the snapshot diff). Producer pages server-render each embed's data as an HTML fallback table (`<noscript><table>`), so parse that instead: whole-page `pd.read_html(io.StringIO(resp.text))`, select the table whose columns exactly match the expected header, assert exactly one match. Related trap: the producer's visible "Updated" stamp and prose can lag their own data tables — trust the data, and when the newest rows have no stamped release date, `date_published` falls back to `date_accessed` (document why in a `.dvc` comment).
- **DAG consistency**: After `etl update`, always verify that all new steps in `dag/main.yml` reference each other with the new version. A common bug is garden depending on old meadow or old snapshot — this silently loads stale data.
- Never return empty tables or comment out logic as a workaround — fix the parsing/transformations instead.
- Column name changes: update garden processing code and metadata YAMLs (garden/grapher) to match schema changes.
- Indexing: avoid leaking index columns from `reset_index()`; format tables with `tb.format(["country", "year"])` as appropriate.
- Metadata validation errors are guidance — update YAML to add/remove variables as indicated.
- **Mixed-type object columns at meadow**: when `pd.read_csv` produces an `object` column that mixes strings and `NaN` (common for sparse text columns like sources/comments/punishments), the feather repacker rejects it. Cast those columns to pandas `"string"` dtype before `tb.format(...)`.
- **`paths.regions` auto-resolves DAG dependencies**: `paths.regions.add_population(tb)` and `paths.regions.add_aggregates(tb, regions=[...])` pick up the `population` and `income_groups` datasets directly from the DAG. Don't `paths.load_dataset("population")` and pass it through unless the helper specifically asks for the dataset — the parameter is unused.
- **WB income-group aggregates**: add the four classification names (`High-income countries`, `Upper-middle-income countries`, `Lower-middle-income countries`, `Low-income countries`) to your `REGIONS` list and add `data://garden/wb/<latest>/income_groups` to the DAG. `paths.regions.add_aggregates(...)` auto-resolves the classification.
- **Detect structural placeholders dynamically**: when a source ships "balanced panel" rows that are zero everywhere by design (status combos that exist only for completeness), detect them at runtime (`groupby(...).max() == 0`) and assert the count matches the codebook. A coding change in the source then surfaces as a test failure instead of silently shipping noise.
- **In-place source revisions: compare file dates/hashes, not version labels.** Some producers replace the published file (and codebook) without bumping their stated version and without a changelog. When checking whether a source has a new release, look at the hosting platform's file-modification dates or hashes (e.g. the OSF API's `date_modified` per file) against the previous snapshot's `date_accessed`/md5 — an unchanged version label proves nothing. If you ingest such a revision, set `date_published` to the replacement date and leave a `.dvc` NOTE naming the behavior so the next updater re-checks. (Velasco LGBTI: 6,813 cells changed under the same "Version 2.0" label.)
- **Category-state churn in combined categorical indicators.** A source revision can add or remove the *states* a categorical indicator takes (a recode wave eliminating an enforcement state, a new cross-combination appearing). The net that catches it is metadata validation failing with "extra variables in YAML file / in table" on per-category regional variables. Fix all four surfaces together — the category map in step code, the `sort:` lists, the coding-description sentences, and the per-category regional `_count`/`_pop` YAML blocks — then re-run the phantom-category audit. Never keep a label the new data cannot produce.
- **Guard silent catch-all buckets with an assert.** A lookup that routes labeled source values into an *existing* fallback category ("requirement unknown", "other") degrades silently when the source adds a new label — the row lands in the fallback and nothing fires (unlike unmapped values that leak as *new* categories, which metadata validation catches). Assert `observed labels ⊆ map keys`, letting only genuinely-blank values reach the fallback. (LGBTI: two new requirement labels published 40 country-years as "requirement unknown"; Codex caught it, the build didn't.)
- **Grep metadata prose for numbers carried from the old release.** Validated fields are covered by checks; *prose* numbers in `description_key`/descriptions (country counts, category counts, year ranges) are not. After a revision that changes panel composition, `rg` the new `.meta.yml` for the previous release's counts. (LGBTI: "197 countries" survived in the dataset-level bullet after the Vatican was dropped.)
- **corrections.yml overrides on categorical columns must use the source's *current* vocabulary.** Assigning a retired label raises `TypeError: Cannot setitem on a Categorical with a new category` — pick the current-vocabulary value that maps to the same published output, and note the substitution in the entry.
- **Codebook-vs-data inconsistencies**: when the codebook documents one thing but the actual CSV shows another (placeholder claimed but non-zero rows present, etc.), preserve the data as-shipped and flag it in the PR description for the producer to confirm. Don't silently force the data to match the codebook.
- **Grapher `.meta.yml` only when it adds something**: the grapher step inherits everything via `default_metadata=ds_garden.metadata`, so drop the grapher `.meta.yml` if it only duplicates the garden values. Keep it only for genuine grapher-side overrides.
- **`processing_level: major` requires `description_processing`**: keep `processing_level: minor` as the common default and override to `major` only on indicators that have a `description_processing` field. Don't blanket-set `major` on the common block and then leave country-level proportions without their own processing note.
- **Per-indicator description_processing reads better than a generic shared note**: when an indicator is derived (combined-categorical buckets, regional aggregates, computed counts), spell out *that indicator's* derivation. Reusing named definitions for shared boilerplate is fine; just compose them into per-indicator sentences rather than dropping a single generic note across all indicators.
- **`description_key` in `definitions.common` propagates only to indicators without their own list**: if you want a bullet to appear on every indicator, either keep it on `common.description_key` and don't define per-indicator lists (it inherits), or prepend it explicitly to each per-indicator list (treats it as a "first bullet" pattern).
- **Phantom-category audit on categorical indicators**: after building categorical indicators, sweep every indicator and compare YAML `sort:` labels against the unique values that actually appear in the data. Phantom labels (declared in `sort:` or in a category map but never produced) clutter chart legends with empty buckets. Either drop them from `sort:` and `description_key`, or remove them from the map if they can never occur given the data shape. Re-run the audit on every data refresh — phantoms can reappear when a category is dropped upstream.
- **`NOTE:` comments for the next maintainer when behaviour is data-conditional**: when something in the code holds only because of the current data shape (e.g. "only 4 indicators have an EoE=0 row", "only Brazil 2025 is a transition-year artefact"), leave a `# NOTE:` comment near the relevant block asking the next data update to re-audit. Helps future maintainers spot which assumptions might decay before they bite.
- **Indicator Upgrader CLI for one-shot chart remaps**: when v1 → v2 short_names change so much that the auto-upgrader can't match them, drive the remap manually. Write a small script that calls `WizardDB.add_variable_mapping(mapping={old_id: new_id, ...}, dataset_id_old=..., dataset_id_new=..., comments="...")` with the explicit pairs, then run `from apps.indicator_upgrade.upgrade import cli_upgrade_indicators; cli_upgrade_indicators(dry_run=True)` to preview affected charts, and `(dry_run=False)` to apply. Mappings stay in the wizard DB until `WizardDB.delete_variable_mapping()` is called, so a slug-collision failure can be recovered by fixing the slug and rerunning the upgrade — only un-upgraded charts get reattempted. The active staging DB is inferred from the current git branch.
- **Drop-in vs restructure decision point**: when the new dataset has a different shape (long vs wide, more policies, changed score semantics, dropped composite measures), `etl update --rename` is the wrong starting point — the structure of meadow/garden/grapher needs to follow the new shape, and the rename flow will only produce confusion. Spot this fork early at the snapshot/codebook stage, before running `etl update`. Scaffold the new chain via the [`create-etl-steps`](../create-etl-steps/SKILL.md) skill (wraps the wizard's cookiecutter templates) or launch the wizard UI with `etlwiz` and use its "ETL Steps" page — both produce a consistent meadow/garden/grapher skeleton to fill in. Once scaffolded, **read the v1 scripts as a reference** for the source-specific logic that's still relevant (column-rename maps, status/category normalisations, country harmonisation map, sanity checks, codebook-driven structural assertions) — don't copy the v1 structure blindly, but port the bits that still apply to the new schema.

When the update is review-heavy and you need iterative back-and-forth with a topic owner over staging, see the [`report-indicator-changes`](../report-indicator-changes/SKILL.md) skill for drafting the message.

## Artifacts (expected)

- `workbench/<short_name>/snapshot-runner.md`
- `workbench/<short_name>/progress.md`
- `workbench/<short_name>/notes_to_check.md` (one entry per carried-over `# NOTE:` / `# TODO:`, plus detected `sanity_checks` functions and their log-control flags)
- `workbench/<short_name>/sanity_checks.log` (only if step 5b ran)
- `workbench/<short_name>/meadow_diff_raw.txt` and `meadow_diff.md`
- `workbench/<short_name>/garden_diff_raw.txt` and `garden_diff.md`
- `workbench/<short_name>/harmonization.log` and `harmonization_audit.md` (from step 5c)
- `workbench/<short_name>/indicator_upgrade.json` (if indicator-upgrader was used)
- `workbench/<short_name>/update-context.yml` (canonical facts gathered during the update; consumed by `data-updates-comms`)
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
