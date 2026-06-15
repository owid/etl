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
- **Slack + `/latest` drafts can legitimately be empty** in the first PR of a multi-PR restructure (chart remap pending). 🟢 if PR body explicitly defers them.

### 4. Run the full pipeline end-to-end

```bash
.venv/bin/etlr data://grapher/<namespace>/<new_version>/<short_name>
.venv/bin/etlr grapher://grapher/<namespace>/<new_version>/<short_name> --grapher --force --only
```

The `--grapher` upload is required to verify MySQL ingestion and to enable later checks (chart count, indicator upgrade verification). Confirm:
- All four steps run cleanly (snapshot pulled from S3 if `.dvc` is committed, otherwise re-fetched)
- MySQL upload returns a `dataset id` and shows variable upserts
- No errors / no empty tables

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
| `url_main` | Status check — see step 6 |
| `url_download` | Status check; OK to remove if data is now fetched via API |
| `license.url` | Status check |

### 6. Verify all links

Run the HEAD-check loop from `/update-dataset` § 6c on every URL in the new `.dvc` and `.meta.yml` files. A curl non-2xx is a *signal*, not proof — Cloudflare-fronted hosts return false 404s to curl. Apply the same escalation as `/update-dataset` § 6c: re-check with `WebFetch`, then the Wayback Machine. Only a URL that fails **all three** is a 🔴 blocker; a curl-only failure that WebFetch resolves is 🟢 informational.

### 7. Code clarity & docs

For each step file, check:
- **Snapshot script (if present)**: docstring explains source choice; no hidden hardcoded year/date constants without `--cli-flag` parametrization (or at minimum a clear update comment). Note: a `.py` upload script is optional — many snapshots ship with only the `.dvc` and a `url_download`. Don't flag the absence of a script.
- **Meadow / garden / grapher**: clear top-level docstrings; no commented-out code; no silent exception handlers
- **Garden**: harmonization uses `paths.regions.harmonize_names(tb, ...)` (the new API), not the legacy `geo.harmonize_countries`
- **Garden assertions**: sanity checks present when the step does non-trivial logic (harmonization, renames, aggregations, derivations) and not overly brittle (e.g. avoid hard-coded "X must always exceed Y" if it's not a true invariant). Check value-bound coverage per indicator type (shares in [0,1], percentages-of-a-whole in [0,100], non-negativity for level indicators, mutually exclusive share categories summing to 100 within rounding tolerance, exception sets for documented outliers) — but verify any bound against the actual data before suggesting it: "% of GDP" indicators legitimately exceed 100 (see `/update-dataset` §5b-bis)
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

2. **Garden-output entities are canonical.** This is the check that catches inline `tb["country"] = "…"` assignments and post-harmonization mutations the `.countries.json` review can't see. Build the canonical set (regions + latest income groups) and diff against the entities actually in the built garden tables — see the Python snippet in `/update-dataset` §5c (Python checks #3 + #5). Any entity in the garden output that isn't in canonical regions or income groups is 🔴 **unless** it's a legitimately custom source aggregate (e.g. `" (ILO)"`/`" (WB)"`-suffixed regions, BRICS, G7) that the PR body explicitly notes lives outside the canonical system.

3. **Over-exclusion.** If `.excluded_countries.json` exists, flag any entry that *is* a canonical region/aggregate (`/update-dataset` §5c Python check #4) — dropping a real country/region silently is 🟡 unless the PR body says why (e.g. source double-counts "World").

If the garden step doesn't use the harmonizer at all (no `.countries.json`; `country` assigned inline), checks #2 and #3 still apply — #2 is the only thing that catches non-canonical inline values.

### 9. Indicator metadata coverage & dataset block

The mandatory-fields checklist, the `dataset.update_period_days` requirement, and the `presentation.attribution_short` non-inheritance gotcha all live in `/update-dataset` § 6c. As reviewer, build the indicator × field matrix from that checklist and flag any missing field as 🔴.

Quick verification that `presentation.attribution_short` actually landed on the produced indicators (origin's value does NOT propagate):

```bash
make query SQL="SELECT shortName, attributionShort FROM variables WHERE catalogPath LIKE '%<ns>/<v>/<short_name>%'"
```
Any `NULL` row is a 🔴.

**Additional reviewer-side metadata checks:**

- **`dataset.owners` includes the PR author.** `/update-dataset` step 1a-bis requires the author to append their canonical OWID name (an entry in the `schemas/dataset-schema.json` enum) to the garden `.meta.yml` `owners:` list, preserving the existing order and any `# review` / `# backport` / `# fasttrack` markers. Author missing from the list = 🟡; existing owners reordered or dropped = 🔴.
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

The archive-and-reorder procedure is in `/update-dataset` § "DAG archiving & reordering". As reviewer, verify the **outcome**:

```bash
rg "<namespace>/<old_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"   # should be empty
rg "<namespace>/<old_version>/<short_name>" dag/archive/ -g "*.yml"                    # should match
rg "<namespace>/<new_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"   # should be in old slot, not at bottom
```

**Internal version consistency (the silent-stale-data bug).** `etl update` occasionally leaves a new step depending on an *old*-version dep (e.g. new garden still pointing at old meadow or old snapshot), which silently loads stale data. Verify every dep *inside* the new chain's block is on the new version. Read the new chain's DAG block and confirm none of its dependency lines reference `<old_version>`:

```bash
# Print the new chain's block and eyeball the dependency lines — none should contain <old_version>.
rg -n -A8 "<namespace>/<new_version>/<short_name>" dag/ -g "*.yml" | rg "<old_version>"   # should be empty
```

Any hit here is a 🔴 — the new step is wired to a stale dependency.

Visual inspection of the diff for:
- Comment headers (`# Source — dataset name.`) preserved above both archived and new entries
- Indentation consistent (` #` vs `  #` is a frequent typo)
- Trailing newline on the archive YAML
- New entries placed in the old block's slot, not orphaned at the bottom (🟡 if at the bottom)

### 12. Downstream dependency check

Procedure in `/update-dataset` § "Downstream dependency check". One-liner:

```bash
rg "<namespace>/<old_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"
```

After excluding the dataset's own chain, any remaining hits are downstream consumers — flag 🟡 unless the PR body already documents them under a "Downstream dependencies" section.

### 13. /update-dataset workflow status

Verify the author completed each post-step item from `/update-dataset`. The procedures live there — here we just confirm the **outcomes**:

| Item | Verify by |
|---|---|
| Indicator upgrade ran (§7) | `make query SQL="SELECT COUNT(*) FROM chart_dimensions cd JOIN variables v ON cd.variableId=v.id WHERE v.catalogPath LIKE '%<ns>/<new_v>/%'"` — non-zero |
| Explorers / MDims re-exported (§7) | Only if the DAG has `export://explorers/...` or `export://multidim/...` steps for this dataset (`rg -e "export://explorers/.*/<short_name>" -e "export://multidim/.*/<short_name>" dag/ -g "*.yml"`). The indicator-upgrader never touches these, so run the two staging queries from `/update-dataset` §7 (old-version references in `explorer_variables` / `multi_dim_x_chart_configs`) — both must return empty. A hit = 🔴, the export step wasn't re-run. |
| Chart-diff bot result | PR comments include `<!--chart-diff-start-->` block ✅ |
| `@codex review` posted (§9) | `gh pr view <num> --json comments` shows the trigger comment + a Codex review |
| Codex threads resolved (§10) | `gh api graphql -f query='{ repository(owner:"owid", name:"etl") { pullRequest(number:<num>) { reviewThreads(first:20) { nodes { isResolved } } } } }'` — all `isResolved: true` |

A **clean Codex review has a different shape**: no inline comments and zero review threads — just a single top-level "no issues" comment from `chatgpt-codex-connector[bot]` in the issue comments. That counts as reviewed (the threads row passes vacuously); don't flag the absence of inline threads as "review missing".

**Out of scope for review:** Slack announcement and Anomalist + Chart Diff hand-off are author-side concerns, not reviewer checks.

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
8. **Workflow gaps from /update-dataset** — PR description, Slack draft, Codex review, etc.

## Severity rubric

- 🔴 **Blocker**: missing mandatory metadata field, genuinely broken link (fails curl + WebFetch + Wayback), failing pipeline step, breaking change to chart data, missing `update_period_days`, missing `presentation.attribution_short`, stale year in `citation_full`/`attribution`, outdated `__main__` block in snapshot, DAG reference to old version that should be archived, new step wired to a stale (old-version) DAG dependency, explorer/MDim still referencing old-version variables on staging, non-canonical garden-output entity that isn't a documented custom aggregate, sanity check that newly raises on the new data
- 🟡 **Suggestion**: brittle assertion, hardcoded year that should be dynamic, duplicated grapher meta.yml that could be removed, non-blocking style issues, undocumented sanity-check findings, phantom `sort:` labels with no backing value, over-exclusion of a canonical region, undocumented `missing values in mapping` countries, PR author missing from `dataset.owners`, missing tracking-issue link in the PR body
- 🟢 **Informational**: things to be aware of but not action items

## Notes

- The `/review` skill is for general PR review — this skill is the dataset-specific superset.
- If the user explicitly asks to skip the pipeline run (e.g. "don't run it, just look"), still do steps 1–3 and 5–13, but skip step 4 and note that pipeline correctness is unverified.
- Always include the **`--grapher`** flag when running the grapher step end-to-end — without it, MySQL ingestion is not exercised and indicator metadata in the DB is not verified.
