---
name: review-data-pr
description: Review an OWID ETL data update PR end-to-end — runs the pipeline, compares snapshot fields against the previous version, verifies links, audits indicator metadata coverage, and cross-checks workflow items from /update-dataset. Trigger when the user asks to "review this PR", "review the data PR", or invokes this on an open dataset-update branch.
metadata:
  internal: true
---

# Review Data PR

End-to-end review of a dataset-update PR. Goes deeper than `/review`: actually runs the steps, compares to the previous version, audits metadata coverage against a fixed checklist, and reports on `/update-dataset` workflow status (Slack draft, Codex review, indicator upgrade, downstream deps).

> **Paired skill — keep in sync.** [`/update-dataset`](../update-dataset/SKILL.md) is the author-side counterpart of this skill: the steps it defines are the outcomes verified here. Whenever you add, remove, or change a check in this file, check whether `update-dataset/SKILL.md` needs a matching author-side step (and add it in the same commit if so). The reverse also holds — see the mirror note there.

## Inputs

- Optional PR number. If omitted, derive it from the current branch via `gh pr list --head <branch>`.

## Workflow

### 1. PR metadata

```bash
gh pr view <num> --json title,body,isDraft,mergeable,statusCheckRollup,comments,reviews
```

Flag if **PR description is empty** (per user's standing rule: keep PR body in sync with substantial changes).

Flag 🟡 if the Summary doesn't open with a **tracking-issue link** (`Tracks: owid/owid-issues#NNNN`) — `/update-dataset` requires it as the first line; most data updates have a corresponding `owid-issues` ticket.

### 2. Diff and changed files

```bash
gh pr view <num> --json files --jq '.files[] | "\(.additions)+ \(.deletions)- \(.path)"'
```

For very large diffs (>1MB) skip `gh pr diff` and read the changed files directly with `Read`.

### 3. Locate the new dataset

From the changed files, identify:
- New snapshot path: `snapshots/<namespace>/<new_version>/<short_name>.<ext>.dvc` (a `.py` upload script is **optional** — `.dvc` + `url_download` or a local file path is enough)
- New step files: `etl/steps/data/{meadow,garden,grapher}/<namespace>/<new_version>/<short_name>.{py,meta.yml}`
- Old version (from `dag/archive/*.yml` or by grepping for the same `<short_name>`)

### 3b. Update shape — version bump vs restructure

Before running the pipeline, classify the PR. If any of the following are true, you're reviewing a **restructure**, not a version bump, and several downstream checks apply differently:

- The `short_name` changed (old version uses one name, new version uses another).
- The schema changed (wide ↔ long, different file format with a different column set, new dimensions).
- The set of policies/indicators changed substantially (splits, dropped composites, newly added areas).
- Score semantics changed (e.g. binary → continuous 0–1, units/scale changed).

When it's a restructure:

- **Don't expect the auto-Indicator-Upgrader to have remapped charts.** When short_names differ entirely, the upgrader has nothing to match on. Look for a hand-curated v1 title → v2 title mapping table in the PR description (or a follow-up PR thread). 🟡 if charts on the old chain are still published but no mapping plan exists.
- **Don't expect a `.py` step copy from the old version.** Step files should be authored from scratch, not produced by `etl update` rename. If the new step files look mechanically renamed (same logic, just version-bumped strings), flag 🟡 — the author may have skipped restructure-specific decisions.
- **Slack + `/latest` drafts are not expected in the PR body at all.** `/update-dataset` keeps them in the author's `workbench/` (steps 9 / 9b), so their absence from the PR is correct — don't flag it.

### 4. Run the full pipeline end-to-end

```bash
.venv/bin/etlr data://grapher/<namespace>/<new_version>/<short_name>
.venv/bin/etlr grapher://grapher/<namespace>/<new_version>/<short_name> --grapher --force --only
```

The `--grapher` upload is required to verify MySQL ingestion and to enable later checks (chart count, indicator upgrade verification). Confirm:
- All four steps run cleanly (snapshot pulled from S3 if `.dvc` is committed, otherwise re-fetched)
- MySQL upload returns a `dataset id` and shows variable upserts
- No errors / no empty tables

**Shortcut: read DB checks off the populated staging server.** OWID provisions a `staging-site-<branch>` server (via Buildkite) that runs the ETL chain and uploads to its MySQL. Once it's built, you can read the DB-dependent checks (chart count, `attributionShort`, rendered titles/Jinja coverage, indicator-upgrade, ghost variables) straight off staging instead of re-running `--grapher` locally — which also avoids re-triggering step side-effects (e.g. a grapher step that exports to Google Sheets). **Confirm the staging ETL build actually ran and finished** before trusting it: query `staging-site-<branch>` for the new dataset's variables (they exist) **and** check the `owidbot` PR comment shows a chart-diff block (✅) — that comment is produced *after* the staging build. ⚠️ Do **not** use the GitHub **`build-and-deploy`** check as that signal — it's the *docs* Cloudflare Pages deploy (`.github/workflows/deploy-docs-cf.yml`: `make docs.build` → deploys `site/`), with **no** ETL chain or Grapher upload, so a green `build-and-deploy` says nothing about pipeline correctness or the data DB. Reserve a local build for what the staging DB can't answer — chiefly **entity-level canonicalization (§8c #2)** (data lives outside MySQL). If you can't confirm staging is populated, run the pipeline locally per the steps above, and say in the report whether correctness rests on the staging build or a local run.

**Review the actual PR head, not a stale local checkout.** The local branch can lag `origin` (or carry an in-progress merge). Before reading step files locally, `git fetch` and confirm your tree matches the PR head — `git diff HEAD origin/<branch> --stat` should be empty, and `gh pr view <num> --json headRefOid` should match `git rev-parse HEAD`. `gh pr view --files` / `gh pr diff` and the staging DB always reflect origin; local `Read`s do not. If they diverge, sync (or review via `gh pr diff`) before trusting local files.

### 5. Snapshot field comparison

Read both `.dvc` files (old and new) and produce a side-by-side table for these fields:

| Field | Check |
|---|---|
| `title` | Reasonable update if scope changed |
| `description` | Updated to reflect new source / scope |
| `date_published` | **Must differ from `date_accessed`** — source from `url_main` or the file. If unsure, ask. |
| `date_accessed` | Updated to today (or run-date) |
| `producer` / `attribution_short` | Same source, same values (unless changed deliberately) |
| `citation_full` / `attribution` | **Year bumped to the new release year** — `etl update` copies both verbatim from the old `.dvc`, so a stale year ships silently. 🔴 if still the old version's year. |
| `citation_full` year vs `date_published` year | **Warn (🟡) if they differ.** The year inside `citation_full` (and `attribution`) should normally match `date_published`'s year. A mismatch is sometimes legitimate — the producer labels the release by *edition* rather than publish date (e.g. UN IGME's "2025 report" published `2026-03-17`, so `citation_full` `(2025)` ≠ `date_published` `2026`) — but it's just as often a stale citation the author forgot to bump. Surface it for the author to confirm; don't silently pass it. |
| `url_main` | Status check — see step 6 |
| `url_download` | Status check; OK to remove if data is now fetched via API |
| `license.url` | Status check |

### 6. Verify all links

Run the HEAD-check loop from `/update-dataset` § 6c on every URL in the new `.dvc` and `.meta.yml` files. A curl non-2xx is a *signal*, not proof — Cloudflare-fronted hosts return false 404s to curl. Apply the same escalation as `/update-dataset` § 6c: re-check with `WebFetch`, then the Wayback Machine. Only a URL that fails **all three** is a 🔴 blocker; a curl-only failure that WebFetch resolves is 🟢 informational.

- **`docs.google.com` 200 ≠ publicly viewable.** Google Sheets/Docs links return HTTP 200 even when they're behind a permission wall (the 200 is the "request access"/sign-in page). When a user-facing `description_key`/`description_processing` links a Google Sheet, confirm real public access with `WebFetch` (ask whether the page shows data or a "you need access"/sign-in wall) — curl status alone will pass a private sheet.
- **Cross-check the same sheet is cited consistently.** If a dataset links a "source per data point" sheet from more than one field, verify they're the *same* sheet ID — divergent IDs (one current, one stale) is a 🟡.

### 7. Code clarity & docs

For each step file, check:
- **Snapshot script (if present)**: docstring explains source choice; no hidden hardcoded year/date constants without `--cli-flag` parametrization (or at minimum a clear update comment). Note: a `.py` upload script is optional — many snapshots ship with only the `.dvc` and a `url_download`. Don't flag the absence of a script.
- **Meadow / garden / grapher**: clear top-level docstrings; no commented-out code; no silent exception handlers
- **Garden**: harmonization uses `paths.regions.harmonize_names(tb, ...)` (the new API), not the legacy `geo.harmonize_countries`
- **Garden assertions**: sanity checks present when the step does non-trivial logic (harmonization, renames, aggregations, derivations) and not overly brittle (e.g. avoid hard-coded "X must always exceed Y" if it's not a true invariant). Check value-bound coverage per indicator type (shares in [0,1], percentages-of-a-whole in [0,100], non-negativity for level indicators, mutually exclusive share categories summing to 100 within rounding tolerance, exception sets for documented outliers) — but verify any bound against the actual data before suggesting it: "% of GDP" indicators legitimately exceed 100 (see `/update-dataset` §5b-bis)
- **Unit-branched aggregation — verify every count sums and every rate averages.** When a regional-aggregation step routes rows by a *unit string* — e.g. counts (`unit == "Number of deaths"`) get summed while everything else gets population-weighted-averaged — a count series carrying a *different* unit label silently falls into the averaging branch and produces a meaningless regional "total". (Real case: IGME summed `"Number of deaths"` but `"Number of stillbirths"` fell through to the rates path, so regional stillbirth totals became population-weighted averages — Africa showed ~60k instead of ~1M.) Enumerate the distinct units, confirm each is routed correctly (a region's count value should be ≫ any member country's, not a mid-range average), and prefer a robust predicate (`unit.startswith("Number of")`) over an exact match. Catches a class of bug a green pipeline + Jinja-renders-fine review will miss.
- **Blanket title-based unit scaling — audit the raw range of every affected indicator.** When garden scales values by pattern-matching indicator *titles* (e.g. "titles containing 'share' or 'percentage' get ×100"), verify the source actually stores every matched indicator in the assumed convention: compute each matched indicator's raw min/max and flag any whose range contradicts the rule (a "fraction" with raw max ≫ 1, or a "percent" with raw max ≈ 1). (Real case: WWBI's 134 "share"-titled indicators are fractions, but its 2 "percentage"-titled wage-bill ratios are already percent — sourced from IMF FAD, not WB surveys — so a blanket ×100 shipped them 100× too large across versions.) A quick output-side check: no %-unit column's max should exceed a grounded bound (~150 for genuine percentages-of-a-whole). **"Same as the previous version" is not a pass** — magnitude bugs are inherited; judge absolute plausibility (a wage bill is not 1,242% of GDP). 🔴 if a scaled indicator's convention is contradicted by its raw range.
- **External-write helpers may be env-guarded — read the helper before flagging.** An unconditional call to something like `export_table_to_gsheet(...)` / `get_team_folder_id()` in a garden/grapher step looks like a CI/deploy risk, but several OWID helpers early-return unless `OWID_ENV.env_local == "dev"` (so they no-op on staging/prod). Check the helper's guard before flagging — "unconditional call" ≠ "runs everywhere". If it *is* guarded, it's at most a 🟢/style note (intent could be made explicit at the call site; the dev-only side-effect can leave the exported artifact stale relative to prod), not a blocker.
- **Grapher meta.yml**: drop it if it only duplicates the garden values — the grapher step inherits via `default_metadata=ds_garden.metadata`

### 8. Outdated practices

**Run the `/check-outdated-practices` skill on every new step file** (snapshot, meadow, garden, _and_ any helper modules like `*_omms.py`). It reads [vscode_extensions/detect-outdated-practices/src/extension.ts](vscode_extensions/detect-outdated-practices/src/extension.ts) as the single source of truth and greps the full pattern set — don't hand-maintain a copy of the patterns here, and don't eyeball helper calls and decide they look current (the `geo.add_*` family looks fine but is flagged). Report every hit it returns as 🟡.

Separately, the metadata/origin-stripping patterns from CLAUDE.md (`pd.concat`→`pr.concat`, `pd.to_numeric`/`pd.to_datetime`→`pr.*`, `np.where`, `index.map(...)`, `pd.DataFrame(tb)` re-wrap) are **not** part of the extension — they're covered by the §7 code-clarity pass. Flag them there even when `copy_metadata`/`fillna` appears to mitigate.

### 8b. Carried-over annotations & sanity_checks (review side)

`/update-dataset` steps 1c+6a (annotations) and 1d+5b (sanity_checks) define the catalog/resolve procedure. As reviewer, verify the **outcome**:

- **Annotations**: scan the diff for any `# NOTE:` / `# TODO:` / `# FIXME:` / `# HACK:` / `# XXX:` that are unchanged from the old version. For each, confirm the PR body mentions whether the workaround is still needed, or that it was deleted with its code. Unresolved + undocumented = 🟡.
- **Sanity-check log flags**: grep the diff for `SHOW_SANITY_CHECK_LOGS`, `DEBUG`, `LONG_FORMAT` set to `True`. If a debug flag was left enabled, that's a 🔴 — must be reverted.
- **Silent deletes**: in any `sanity_checks` function, scan for `drop`, `filter`, `tb = tb[...]` — row removals that the user might miss. Make sure the PR body lists them.
- **Findings surfaced, not just flags reverted**: if the step has any sanity-check logic (function or inline `# Sanity check` block), the PR body should carry a "Sanity-check findings" section reporting what the checks said on the new data. A green pipeline run is **not** proof the invariants held — checks that `paths.log.warning(...)`/`.critical(...)` instead of `assert`/`raise` pass silently. If the new garden chain has logging-style checks and the PR body has no findings section, re-run the garden step (`--private --force --only`) and scan stdout/stderr for `warning`, `dropped`, `outlier`, `AssertionError`. Undocumented findings = 🟡; a check that newly raises on the new data = 🔴 (must be triaged with the author per `/update-dataset` §5b).

### 8c. Country harmonization audit (review side)

`/update-dataset` §5c defines the full audit (validate `.countries.json` targets against the canonical regions + income-groups catalogs, audit `.excluded_countries.json`, scan the garden log for the three warnings, and confirm garden-output entities are canonical). As reviewer, verify the **outcome** — every entity reaching Grapher must be canonical, and any that isn't must be documented in the PR body.

Run after the §4 pipeline build. Three checks:

1. **Garden log warnings.** Re-run the garden step capturing output and scan for the three stable warning strings:
   ```bash
   .venv/bin/etlr data://garden/<namespace>/<new_version>/<short_name> --private --force --only \
       > /tmp/<short_name>_harmon.log 2>&1
   rg -n "missing values in mapping\.|unused values in mapping\.|Unknown country names in excluded countries file:" /tmp/<short_name>_harmon.log
   ```
   `missing values in mapping` (source countries not in `.countries.json`) is the actionable one — 🟡 unless the PR body documents the gap. `unused values in mapping` / `Unknown … excluded` are informational 🟢.

2. **Garden-output entities are canonical.** This is the check that catches inline `tb["country"] = "…"` assignments and post-harmonization mutations the `.countries.json` review can't see. **This one needs a local build** — entity lists aren't in MySQL (modern grapher stores indicator data outside the DB), so `make query` can't answer it; build the garden step and load it with `owid.catalog.Dataset("data/garden/<ns>/<v>/<short>")`. Build the canonical set (regions + latest income groups) and diff against the entities actually in the built garden tables — see the Python snippet in `/update-dataset` §5c (Python checks #3 + #5). Note `geo.REGIONS` already includes the four WB income groups, so an `isin(REGIONS)` filter de-dups them too. Any entity in the garden output that isn't in canonical regions or income groups is 🔴 **unless** it's a legitimately custom source aggregate (e.g. `" (ILO)"`/`" (WB)"`-suffixed regions, BRICS, G7) that the PR body explicitly notes lives outside the canonical system.

3. **Over-exclusion.** If `.excluded_countries.json` exists, flag any entry that *is* a canonical region/aggregate (`/update-dataset` §5c Python check #4) — dropping a real country/region silently is 🟡 unless the PR body says why (e.g. source double-counts "World").

If the garden step doesn't use the harmonizer at all (no `.countries.json`; `country` assigned inline), checks #2 and #3 still apply — #2 is the only thing that catches non-canonical inline values.

### 9. Indicator metadata coverage & dataset block

The mandatory-fields checklist, the `dataset.update_period_days` requirement, and the `presentation.attribution_short` non-inheritance gotcha all live in `/update-dataset` § 6c. As reviewer, build the indicator × field matrix from that checklist and flag any missing field as 🔴.

Quick verification that `presentation.attribution_short` actually landed on the produced indicators (origin's value does NOT propagate):

```bash
make query SQL="SELECT shortName, attributionShort FROM variables WHERE catalogPath LIKE '%<ns>/<v>/<short_name>%'"
```
Any `NULL` row is a 🔴.

**Staging query mechanics.** `make query` re-interprets `%` and single quotes via shell+make and breaks on the `LIKE` patterns / quoted strings these checks need. Connect directly instead and feed SQL via stdin or a `.sql` file: `mysql -h staging-site-<normalized-branch> -u owid --port 3306 -D owid < /tmp/q.sql` (host = branch lowercased, `/._` → `-`, `staging-site-` prefix stripped, first 28 chars — see the `query:` target in the `Makefile`). Note **`datasets.catalogPath` has no channel prefix** — it's `<ns>/<v>/<short>` (e.g. `un/2026-06-09/igme`), *not* `grapher/un/...`; match `catalogPath LIKE '<ns>/<v>/%'`. Batch the metadata-gap checks in one file: counts of `name=''`, `name LIKE '%<\%%'`/`'%{definitions%'`/`'%<<%'` (unrendered Jinja), `attributionShort IS NULL`, `descriptionShort IS NULL`, plus a double-space/leading-space scan on `name` and `descriptionShort` (Jinja whitespace artifacts).

**Distinguish regressions from inherited gaps.** Before flagging `update_period_days` / `attributionShort` / `description_short` gaps as 🔴, check the *previous* version's `.meta.yml` — if the gap was already there, it's pre-existing (carried over by `etl update`), not introduced by this PR. Still report it (the fix is cheap and the standing convention wants it), but say so: a pre-existing gap is a 🟡 "worth adding while you're here", not a regression that blocks the update. A field that *was* set and is now missing is the real 🔴.

**Additional reviewer-side metadata checks:**

- **`dataset.owners` lists the PR author.** `/update-dataset` step 1a-bis requires the author — the person who ran the update / opened the PR — to append their canonical OWID name (an entry in the `schemas/dataset-schema.json` enum) to the garden `.meta.yml` `owners:` list, preserving the existing order and any `# review` / `# backport` / `# fasttrack` markers. Running an update makes you a contributor, so the author belongs there *alongside* the original owners (don't drop the originals). Verify the **PR author** is present: missing = 🟡; existing owners reordered or dropped = 🔴. Note the actor running *this review skill* is usually a different person (the reviewer) — the reviewer does **not** add themselves; the check is only that the author is listed. The exception is a self-review (author reviewing their own PR), where reviewer and author are the same person.
- **`processing_level: major` must come with `description_processing`.** Grep the new garden meta.yml for `processing_level: major`. Each occurrence (whether on `definitions.common` or per-indicator) requires a `description_processing` field on that same scope. 🟡 mismatch.
- **Per-indicator `description_processing` should describe the indicator's own derivation, not just point at a shared generic note.** When every aggregate indicator's `description_processing` is the exact same string (e.g. all four region indicators just reference `{definitions.description_regions_processing}` with no per-indicator detail), 🟡 flag — author should compose per-indicator sentences.
- **Long-format-with-dimensions Jinja coverage.** When variables are keyed by a long-column name (e.g. `proportion`) with `<% if <dim> == "X" %>...<% endif %>` blocks for `title`, `description_short`, `display.name`, verify every active `(dim1, dim2)` cell renders a non-empty value. Easiest check: read every column from the grapher dataset and assert `metadata.title` is non-empty.
- **`paths.regions.add_population(tb)` / `paths.regions.add_aggregates(tb, regions=[...])` auto-resolve their DAG dependencies.** If the garden step loads `population` (or `income_groups`) via `paths.load_dataset(...)` but never passes the dataset to anything, that's dead code — 🟡. The DAG dependency still needs to be declared either way.
- **WB income groups in regional aggregates.** When the dataset is suitable for cross-country aggregation, check that the four WB income groups (`High-income countries`, `Upper-middle-income countries`, `Lower-middle-income countries`, `Low-income countries`) are in the `REGIONS` list, the `income_groups` DAG dep is declared, and `description_regions_processing` references the [income groups article](https://ourworldindata.org/world-bank-income-groups-explained). 🟢 informational if absent — not all datasets need this, but it's worth surfacing.
- **Phantom-category audit on categorical indicators.** For any categorical/ordinal indicator (one whose meta declares a `sort:` label order or a category map), compare the declared labels against the values that actually appear in the built grapher data. Labels declared in `sort:` (or in a category map) but never produced clutter chart legends with empty buckets. Load each categorical column from the grapher dataset, take its unique values, and diff against the `sort:` list:
  ```python
  from owid.catalog import Dataset
  ds = Dataset("data/grapher/<ns>/<v>/<short_name>")
  tb = ds["<table>"]
  present = set(tb["<col>"].dropna().astype(str).unique())
  # compare `present` against the `sort:` labels in the .meta.yml
  ```
  Any `sort:`/map label with no backing value is 🟡 — author should drop it from `sort:`/`description_key` (or from the map if it can never occur). Re-check on every refresh: phantoms reappear when a category drops out upstream.

### 10. Metadata quality skills

Run `/check-metadata-typos`, `/check-metadata-spacing`, `/check-metadata-style` against the new garden + grapher `.meta.yml` files. See `/update-dataset` § 6b for the full procedure (typos / spacing / style + a manual clarity checklist for general-audience readability — apply that checklist here too). Report findings as 🟡 (or 🔴 if a violation breaks rendering or makes the text outright misleading).

### 11. DAG checks

The remove-and-reorder procedure is in `/update-dataset` § "Removing the old version & reordering the DAG". As reviewer, verify the **outcome** (the archive dag is regenerated separately by `etl archive-dag`, so don't expect the old version under `dag/archive/` in this PR):

```bash
rg "<namespace>/<old_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"   # should be empty (old version removed from active dag)
rg "<namespace>/<new_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"   # should be in old slot, not at bottom
```

**Internal version consistency (the silent-stale-data bug).** `etl update` occasionally leaves a new step depending on an *old*-version dep (e.g. new garden still pointing at old meadow or old snapshot), which silently loads stale data. Verify every dep *inside* the new chain's block is on the new version. Read the new chain's DAG block and confirm none of its dependency lines reference `<old_version>`:

```bash
# Print the new chain's block and eyeball the dependency lines — none should contain <old_version>.
rg -n -A8 "<namespace>/<new_version>/<short_name>" dag/ -g "*.yml" | rg "<old_version>"   # should be empty
```

Any hit here is a 🔴 — the new step is wired to a stale dependency.

Visual inspection of the diff for:
- Comment headers (`# Source — dataset name.`) preserved above the new entries
- Indentation consistent (` #` vs `  #` is a frequent typo)
- New entries placed in the old block's slot, not orphaned at the bottom (🟡 if at the bottom)
- **Both the flat and nested (compact) DAG forms are valid** (the loader accepts either). The nested form (chain declared inline, grapher → garden → meadow → snapshot) is the preferred style — don't flag a nested block as wrong, and don't flag the archive edit itself (archiving is the explicitly-requested workflow step; Codex's "don't edit archived DAG" warning is a known false-positive here).

### 12. Downstream dependency check

Procedure in `/update-dataset` § "Downstream dependency check". One-liner:

```bash
rg "<namespace>/<old_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"
```

After excluding the dataset's own chain, any remaining hits are downstream consumers — flag 🟡 unless the PR body already documents them under a "Downstream dependencies" section.

**Silent-breakage check (when consumers were repointed in this PR).** Mirrors `/update-dataset` § "Silent-breakage check". If the PR bumps downstream consumers to the new version (rather than deferring them), a consumer can still build green while quietly losing data — a region aggregate that goes NaN, a reclassified country that disappears, a join that stops matching. A green pipeline run does **not** prove coverage held. Verify with the two existing instruments:

1. **Downstream builds:** check the **`buildkite/etl-automated-staging-environment`** status on the PR — the staging bake runs `etl run ... --modified --continue-on-failure`, which re-raises the first failure at the end, so any consumer crash turns the check red. A red check is a 🔴 in itself, and it also means the data-diff report **under-reports** (dependents of the failed step are skipped, stay stale in the catalog, and diff as unchanged) — don't accept report verdicts until the check is green. `.venv/bin/etlr --modified --private --dry-run` lists the affected scope locally when you need the list.
2. **Value changes:** open owidbot's **data-diff** HTML report (`https://catalog.ourworldindata.org/diffs/<sanitized_branch>/data-diff.html`, easiest via the **full report** link in owidbot's PR comment — the path keeps the branch name's dots and underscores and replaces only characters outside `[A-Za-z0-9._-]` (e.g. `/`) with `-` — unlike the staging *subdomain*, which does replace `.`/`_`) — staging (new dep) vs production (old dep) — and read its verdicts rather than scanning the diff: every red **"− lost N data point(s)"** entry in the "Top changes" list and every dataset with a red coverage chip is a coverage loss to triage (legitimate churn vs. a silent drop); 🔴-tier datasets (median anomaly score ≥ 15%) get a review, 🟡 a skim, 🟢 is noise. The 📝 metadata-only filter separates pure metadata edits. Locally: `.venv/bin/etl diff REMOTE data/ --changed --include garden --output-html data-diff.html`. (Local build+diff only for small fan-outs; at foundational-dataset scale — hundreds of downstream steps — use owidbot's hosted report and treat any local rebuild as a one-time gate, ~35 min / ~7 GB.) Then run the **full-report audit probes** from `/update-dataset` § "Silent-breakage check" over all changed datasets: structural changes / "World" rows / raw-country rows / >30%-of-rows indicators / **wipe-vs-edge for every coverage loss**. An entity losing *all* its rows is the bug signature — classically a stale pinned-country requirement nulling a whole income-group aggregate after a reclassification (build-time `ValueError` since the geo guard, but verify) — while sparse-edge losses are membership churn.

Any downstream build failure, dropped table/column/entity, or all-NaN series is a 🔴 (the update silently dropped data downstream) unless the author has already triaged it in the PR body. If consumers were **deferred** to a follow-up PR, these checks belong to that PR — here just confirm the "Downstream dependencies" list is complete.

### 13. /update-dataset workflow status

Verify the author completed each post-step item from `/update-dataset`. The procedures live there — here we just confirm the **outcomes**:

| Item | Verify by |
|---|---|
| Indicator upgrade ran (§7) | `make query SQL="SELECT COUNT(*) FROM chart_dimensions cd JOIN variables v ON cd.variableId=v.id WHERE v.catalogPath LIKE '%<ns>/<new_v>/%'"` — non-zero |
| Explorers / MDims re-exported (§7) | Only if the DAG has `export://explorers/...` or `export://multidim/...` steps for this dataset (`rg -e "export://explorers/.*/<short_name>" -e "export://multidim/.*/<short_name>" dag/ -g "*.yml"`). The indicator-upgrader never touches these, so run the two staging queries from `/update-dataset` §7 (old-version references in `explorer_variables` / `multi_dim_x_chart_configs`) — both must return empty. A hit = 🔴, the export step wasn't re-run. |
| Chart-diff bot result | PR comments include `<!--chart-diff-start-->` block ✅. A diff that shifts **every** historical year/region by a tiny amount is usually **upstream-dataset drift** (the live data was built against an older population/regions/income_groups snapshot), not a regression — don't flag it as a 🔴; the real change should be isolable by rebuilding the old version on the current catalog (see `/update-dataset` §5). |
| `@codex review` posted (§9) | `gh pr view <num> --json comments` shows the trigger comment + a Codex review |
| Codex threads resolved (§10) | Write the query to a file and pass `-F query=@file.graphql` (see GraphQL note below) — list `reviewThreads(first:20){ nodes { isResolved } }`; all `isResolved: true`. |

A **clean Codex review has a different shape**: no inline comments and zero review threads — just a single top-level "no issues" comment from `chatgpt-codex-connector[bot]` in the issue comments. That counts as reviewed (the threads row passes vacuously); don't flag the absence of inline threads as "review missing". Codex review state is `COMMENTED` (not `APPROVED`); fetch its inline notes via `gh api repos/owid/etl/pulls/<num>/comments --paginate --jq '.[] | select(.user.login|test("codex";"i"))'`. After `@codex review`, it usually responds in ~2–5 min — poll with a backgrounded `until` loop rather than blocking.

**GitHub API gotchas (when posting/fetching review comments):**
- **GraphQL via `gh api graphql -f query='...'` breaks on shell quoting** (the inline query gets truncated → `Expected NAME` parse errors). Write the query/mutation to a file and use `gh api graphql -F query=@/tmp/q.graphql` with variables as separate `-f name=val` (string) / `-F name=val` (typed int/file). Read bodies from a file with `-F body=@/tmp/comment.md`.
- **Editing/adding comments inside a *pending* review** (the author's in-progress review) needs GraphQL, not REST: REST `PATCH pulls/comments/{id}` 404s on a draft, and `POST pulls/{n}/comments` errors `one pending review per pull request`. Add with `addPullRequestReviewThread(input:{pullRequestReviewId, path, line, side:RIGHT, body})`, edit with `updatePullRequestReviewComment(input:{pullRequestReviewCommentId, body})`. Get the pending review id via `reviews(first:50, states:PENDING)` and a draft comment's node id via that review's `comments`.
- **Disclosure on review comments.** Open any inline review comment you author with `> _Written by Claude <model name> — @<handle> at the wheel._` (model name = the model actually generating the content, e.g. "Sonnet 5"; handle = the human directing the work), same as PR descriptions. Skip it only on genuinely trivial one-liners (an `@codex review` ping).

**Out of scope for review:** Slack announcement and the QA hand-off (Anomalist, Chart Diff, data-diff report) are author-side concerns, not reviewer checks.

**Producer-docs vs. data consistency.** If the PR description notes a discrepancy between the producer's documentation (codebook, methodology page, README, release notes — whatever is available) and the actual file shipped, that's a 🟢 informational item — the author has surfaced it for producer follow-up. **Don't ask them to "fix" the data to match the docs**; the PR should preserve what the source shipped and flag the discrepancy.

### 14. Final report

Structure the review with:

1. **Overview** — one-paragraph summary of what the PR does
2. **Pipeline test result** — ✅/❌ for each step + grapher upload
3. **Snapshot comparison table** — old vs new
4. **Indicator metadata table** — fields × indicators, ✓/❌ matrix
5. **🔴 Blockers** — must-fix before merge
6. **🟡 Suggestions** — nice-to-have
7. **🟢 Informational** — observations, no action needed
8. **Workflow gaps from /update-dataset** — PR description, Codex review, indicator upgrade, downstream deps, etc. (The Slack + `/latest` drafts live in `workbench/`, not the PR — don't expect them here.)

## Severity rubric

- 🔴 **Blocker**: missing mandatory metadata field, genuinely broken link (fails curl + WebFetch + Wayback), failing pipeline step, breaking change to chart data, missing `update_period_days`, missing `presentation.attribution_short`, stale year in `citation_full`/`attribution`, outdated `__main__` block in snapshot, DAG reference to old version that should be archived, new step wired to a stale (old-version) DAG dependency, explorer/MDim still referencing old-version variables on staging, non-canonical garden-output entity that isn't a documented custom aggregate, sanity check that newly raises on the new data, downstream consumer that fails to build or silently lost coverage after a same-PR repoint (a "− lost N data point(s)" entry / red coverage chip in the data-diff report) without triage in the PR body
- 🟡 **Suggestion**: brittle assertion, hardcoded year that should be dynamic, duplicated grapher meta.yml that could be removed, non-blocking style issues, undocumented sanity-check findings, phantom `sort:` labels with no backing value, over-exclusion of a canonical region, undocumented `missing values in mapping` countries, count series mis-routed into a rate/average aggregation branch, `citation_full` year ≠ `date_published` year (verify), pre-existing inherited metadata gap (`update_period_days`/`attributionShort`/`description_short` already missing in the prior version), PR author missing from `dataset.owners`, missing tracking-issue link in the PR body
- 🟢 **Informational**: things to be aware of but not action items

## Notes

- The `/review` skill is for general PR review — this skill is the dataset-specific superset.
- If the user explicitly asks to skip the pipeline run (e.g. "don't run it, just look"), still do steps 1–3 and 5–13, but skip step 4 and note that pipeline correctness is unverified.
- Always include the **`--grapher`** flag when running the grapher step end-to-end — without it, MySQL ingestion is not exercised and indicator metadata in the DB is not verified.
