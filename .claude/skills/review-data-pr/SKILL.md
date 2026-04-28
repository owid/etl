---
name: review-data-pr
description: Review an OWID ETL data update PR end-to-end — runs the pipeline, compares snapshot fields against the previous version, verifies links, audits indicator metadata coverage, and cross-checks workflow items from /update-dataset. Trigger when the user asks to "review this PR", "review the data PR", or invokes this on an open dataset-update branch.
metadata:
  internal: true
---

# Review Data PR

End-to-end review of a dataset-update PR. Goes deeper than `/review`: actually runs the steps, compares to the previous version, audits metadata coverage against a fixed checklist, and reports on `/update-dataset` workflow status (Slack draft, Codex review, indicator upgrade, downstream deps).

## Inputs

- Optional PR number. If omitted, derive it from the current branch via `gh pr list --head <branch>`.

## Workflow

### 1. PR metadata

```bash
gh pr view <num> --json title,body,isDraft,mergeable,statusCheckRollup,comments,reviews
```

Flag if **PR description is empty** (per user's standing rule: keep PR body in sync with substantial changes).

### 2. Diff and changed files

```bash
gh pr view <num> --json files --jq '.files[] | "\(.additions)+ \(.deletions)- \(.path)"'
```

For very large diffs (>1MB) skip `gh pr diff` and read the changed files directly with `Read`.

### 3. Locate the new dataset

From the changed files, identify:
- New snapshot path: `snapshots/<namespace>/<new_version>/<short_name>.<ext>.dvc` and `.py`
- New step files: `etl/steps/data/{meadow,garden,grapher}/<namespace>/<new_version>/<short_name>.{py,meta.yml}`
- Old version (from `dag/archive/*.yml` or by grepping for the same `<short_name>`)

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
| `url_main` | Status check — see step 6 |
| `url_download` | Status check; OK to remove if data is now fetched via API |
| `license.url` | Status check |

### 6. Verify all links

Run the HEAD-check loop from `/update-dataset` § 6c on every URL in the new `.dvc` and `.meta.yml` files. Anything non-2xx is a 🔴 blocker.

### 7. Code clarity & docs

For each step file, check:
- **Snapshot script**: docstring explains source choice; no hidden hardcoded year/date constants without `--cli-flag` parametrization (or at minimum a clear update comment)
- **Meadow / garden / grapher**: clear top-level docstrings; no commented-out code; no silent exception handlers
- **Garden**: harmonization uses `paths.regions.harmonize_names(tb, ...)` (the new API), not the legacy `geo.harmonize_countries`
- **Garden assertions**: sanity checks present and not overly brittle (e.g. avoid hard-coded "X must always exceed Y" if it's not a true invariant)
- **Grapher meta.yml**: drop it if it only duplicates the garden values — the grapher step inherits via `default_metadata=ds_garden.metadata`

### 8. Outdated practices

Run the canonical detector. The source of truth is [vscode_extensions/detect-outdated-practices/src/extension.ts](vscode_extensions/detect-outdated-practices/src/extension.ts). Highlights to grep manually if the extension isn't running:

- `if __name__ == "__main__":` in **snapshot files** — outdated. Remove it; snapshots run via `etls` / `etl snapshot`.
- `geo.harmonize_countries(...)` in step files — replaced by `paths.regions.harmonize_names(...)`.
- `dest_dir` argument, `paths.load_dependency(...)`, `np.where(...)` (strips origins), `index.map(...)` — all flagged.

When in doubt, run the `/check-outdated-practices` skill on the new files.

### 8b. Carried-over annotations & sanity_checks (review side)

`/update-dataset` steps 1c+6a (annotations) and 1d+5b (sanity_checks) define the catalog/resolve procedure. As reviewer, verify the **outcome**:

- **Annotations**: scan the diff for any `# NOTE:` / `# TODO:` / `# FIXME:` / `# HACK:` / `# XXX:` that are unchanged from the old version. For each, confirm the PR body mentions whether the workaround is still needed, or that it was deleted with its code. Unresolved + undocumented = 🟡.
- **Sanity-check log flags**: grep the diff for `SHOW_SANITY_CHECK_LOGS`, `DEBUG`, `LONG_FORMAT` set to `True`. If a debug flag was left enabled, that's a 🔴 — must be reverted.
- **Silent deletes**: in any `sanity_checks` function, scan for `drop`, `filter`, `tb = tb[...]` — row removals that the user might miss. Make sure the PR body lists them.

### 9. Indicator metadata coverage & dataset block

The mandatory-fields checklist, the `dataset.update_period_days` requirement, and the `presentation.attribution_short` non-inheritance gotcha all live in `/update-dataset` § 6c. As reviewer, build the indicator × field matrix from that checklist and flag any missing field as 🔴.

Quick verification that `presentation.attribution_short` actually landed on the produced indicators (origin's value does NOT propagate):

```bash
make query SQL="SELECT shortName, attributionShort FROM variables WHERE catalogPath LIKE '%<ns>/<v>/<short_name>%'"
```
Any `NULL` row is a 🔴.

### 10. Metadata quality skills

Run `/check-metadata-typos`, `/check-metadata-spacing`, `/check-metadata-style` against the new garden + grapher `.meta.yml` files. See `/update-dataset` § 6b for the full procedure (typos / spacing / style + a manual clarity checklist for general-audience readability — apply that checklist here too). Report findings as 🟡 (or 🔴 if a violation breaks rendering or makes the text outright misleading).

### 11. DAG checks

The archive-and-reorder procedure is in `/update-dataset` § "DAG archiving & reordering". As reviewer, verify the **outcome**:

```bash
rg "<namespace>/<old_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"   # should be empty
rg "<namespace>/<old_version>/<short_name>" dag/archive/ -g "*.yml"                    # should match
rg "<namespace>/<new_version>/<short_name>" dag/ -g "*.yml" | grep -v "^dag/archive"   # should be in old slot, not at bottom
```

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
| Chart-diff bot result | PR comments include `<!--chart-diff-start-->` block ✅ |
| `@codex review` posted (§9) | `gh pr view <num> --json comments` shows the trigger comment + a Codex review |
| Codex threads resolved (§10) | `gh api graphql -f query='{ repository(owner:"owid", name:"etl") { pullRequest(number:<num>) { reviewThreads(first:20) { nodes { isResolved } } } } }'` — all `isResolved: true` |

**Out of scope for review:** Slack announcement and Anomalist + Chart Diff hand-off are author-side concerns, not reviewer checks.

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

- 🔴 **Blocker**: missing mandatory metadata field, broken link, failing pipeline step, breaking change to chart data, missing `update_period_days`, missing `presentation.attribution_short`, outdated `__main__` block in snapshot, DAG reference to old version that should be archived
- 🟡 **Suggestion**: brittle assertion, hardcoded year that should be dynamic, duplicated grapher meta.yml that could be removed, non-blocking style issues
- 🟢 **Informational**: things to be aware of but not action items

## Notes

- The `/review` skill is for general PR review — this skill is the dataset-specific superset.
- If the user explicitly asks to skip the pipeline run (e.g. "don't run it, just look"), still do steps 1–3 and 5–13, but skip step 4 and note that pipeline correctness is unverified.
- Always include the **`--grapher`** flag when running the grapher step end-to-end — without it, MySQL ingestion is not exercised and indicator metadata in the DB is not verified.
