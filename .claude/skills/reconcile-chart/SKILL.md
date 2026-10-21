---
name: reconcile-chart
description: >-
  Bring admin edits of ETL-authored single charts (viz://chart steps with chart_config_id) back
  into their ETL config, so the chart is defined in ETL again and the admin's ETL badge stops
  showing it as drifted. Wraps the deterministic `etl chart-reconcile` command: copies every field
  set in the admin into the chart's .config.yml, and fixes a slug changed in the admin by renaming
  the step. Works on all ETL-authored charts, on the charts of one dataset, or on chosen charts.
  Use when the user says a chart "drifted", "is out of sync with ETL", "has admin edits", asks to
  "copy the admin config to ETL", "reconcile", "undrift", or pastes an admin link of an
  ETL-authored chart; /update-dataset runs it on every update. Not for multidims or explorers.
metadata:
  internal: true
  owner: paarriagadap
---

# Reconcile ETL-authored charts with the admin

An ETL-authored single chart renders as merge(indicator config, ETL layer, admin layer), and the admin layer wins. An edit in the chart editor therefore overrides ETL quietly, and the admin flags the chart with a drifted "ETL" badge. `etl chart-reconcile` reads the admin layer and writes it back into the step, the same way every time. This skill runs it, judges what it can't, and ships the result.

> **Called from:** `/update-dataset` step 7, before the dataset's `viz://chart` steps are pushed. Keep that pointer in sync with this file.

## What counts as drift

These are the same two checks the admin badge makes:

- **Field drift.** Any top-level key in the admin patch besides the identity keys every patch carries (`$schema`, `id`, `slug`, `version`, `isPublished`). The command copies each one into the chart's `.config.yml`:
  - `dimensions` replace `views[0].indicators`, in the admin's order and with its `display`.
  - Variable ids (`dimensions`, `sortColumnSlug`, `map.columnSlug`, `colorVariableId`, `xVariableId`, `sizeVariableId`) are written back as the catalog paths the YAML already uses.
  - Everything else is merged into `views[0].config`: nested objects merge, arrays and scalars replace.
  - Values always go to the chart YAML, never to the garden `.meta.yml`, so no other chart or data page changes.

  On the next push, grapher re-diffs the admin patch against the new ETL layer and drops every field ETL now holds.
- **Admin leftovers.** When an existing chart moves into ETL, its whole old admin config stays in the admin layer, and a push only drops the values that equal ETL's. Every value ETL changed during the move stays masked, so the new ETL text never reaches readers. The command treats an override as a leftover when nobody has saved the chart in the admin since its ETL layer was created (no `chart_revisions` row by a user other than ETL), and when the override would replace a value the YAML already sets. It leaves those out (`--pull-leftovers` copies them anyway). Clear them in the admin so the ETL values show. Overrides on fields the YAML doesn't set are still copied, since they mask nothing.
- **Slug drift.** The admin slug differs from the one ETL derives from the step's short name (underscores to dashes). Grapher never lets ETL change an existing chart's slug, so the command renames the step: `<short>.config.yml`, `<short>.py` and any other `<short>.*` file, plus every mention of the step URI in `dag/*.yml`. `chart_config_id` stays, so pushes keep landing on the same chart. This is interim: once chart configs carry a `slug` field, slug drift will be fixed by writing that field instead.

Out of scope: multidims and explorers (no `chart_config_id`), and charts that exist only in the admin (adopt them first with `/create-chart`).

## Run it

The admin layers are read from `staging-site-master`, whose database mirrors production's, so no production credentials are needed. When `ENV_FILE_PROD` is set, production is read directly. To read a different server, pass `--env <staging-branch | env file>`.

```bash
# 1. Dry run: prints a YAML diff per chart and a status table.
.venv/bin/etl chart-reconcile --all
.venv/bin/etl chart-reconcile --dataset <namespace>/<short_name>          # any version
.venv/bin/etl chart-reconcile --dataset <namespace>/<version>/<short_name>
.venv/bin/etl chart-reconcile <config.yml | viz://chart/...> --chart-id <id>

# 2. Write it (fields and slug renames). Add --json <file> for a machine-readable report.
.venv/bin/etl chart-reconcile <same selection> --apply
```

Don't run step 1 against the branch's own staging server. That server's database was copied when the branch was created and has since received the branch's own pushes, so it misses admin edits made later on production. Use `--env <branch>` only to verify the branch's changes (see "Ship it").

Statuses in the report:

| Status | Meaning | What to do |
|---|---|---|
| `in sync` | No admin override, slug matches | Nothing |
| `fields pulled` | Admin fields copied into the YAML | Review the diff (below) |
| `slug drift` / `slug fixed` | Step needs renaming / was renamed | Archive the old name (below) |
| `already in ETL` | The YAML already holds the admin value; the admin layer clears on the next push | Push. On `dimensions` right after a version bump, the patch still names the old variable ids: run the indicator upgrade, then push the step again |
| `needs manual` | An indicator from a dataset the step doesn't depend on, a dimension field ETL can't express, or an invalid slug | Fix by hand, then re-run |
| `fields skipped` | Left out with `--skip-field`; still set in the admin | Clear it in the admin (below) |
| `admin leftovers` | Set in the admin before the move to ETL, masking a different ETL value; left out | Clear it in the admin (below) |

## Judge the diff before committing

Pulling never changes what readers see, since the admin value is what renders today. Leftovers are the opposite case. Clearing them changes the live chart to the ETL text, so list them with both values and get the user's agreement before clearing them.

The leftover check only covers charts nobody has edited since the move. When someone has, a field the report lists under **"admin value replaces an ETL value"** still deserves a look, because that admin value may also be stale. For each such field, compare the two values in the diff:

- **The admin value is the intended one** (someone edited the chart on purpose): keep it.
- **The ETL value is the intended one**: re-run with `--skip-field <field>`, and clear the field in the admin editor (the chip next to the field unlinks it). Clearing it changes the live chart, so ask the user first.
- **Unclear**: ask the user, showing both values and the chart's admin link.

Also check:

- **A step with a `.py`** (the report warns): the code may recompute a pulled field from the data, so make sure the YAML value is the one the code reads, or move the fix into the code.
- **Pulled reader-facing text** (title, subtitle, note, display names): run `/check-metadata-style` on it. A style fix changes the live chart, so it needs the user's agreement and goes in the YAML after the pull.

## Ship it

1. Work in a worktree and open the PR with the usual flow. If nothing but reconciled charts changes, it's a small standalone PR.
2. If steps were renamed: commit, run `.venv/bin/etl archive-dag`, check that it only archived the old step names (`git checkout` anything else it swept in), and commit `dag/archive/*.yml`.
3. PR description: for each chart, its admin link, the fields pulled, the skipped fields and leftovers with both values, and any rename (old step → new step = live slug). Say that nothing reader-visible changes, except fields cleared in the admin.
4. **Clear leftovers and agreed skipped fields on the branch's staging server**, never on production. Run a dry run first and show the user the diff:

   ```bash
   .venv/bin/python .claude/skills/edit-faust-metadata/scripts/update_chart_config.py \
       --branch <branch> --chart-id <id> --unset <field> [--unset <field> ...] --dry-run
   ```

   Then run it again without `--dry-run`. The chart then renders the ETL values on staging and shows up in chart-diff. Once a reviewer approves it there, chart-sync carries the cleared admin layer to production after merge.
5. **Verify on staging** once the branch's staging server has rebuilt (or push the steps with `STAGING=<branch> .venv/bin/etlr viz://chart/... --grapher`):

   ```bash
   .venv/bin/etl chart-reconcile <same selection> --env <branch>
   ```

   Every chart should report `in sync`. Chart-diff lists only the charts whose admin fields were cleared, because pulled fields don't change how a chart renders. Report the reconcile result too.
6. After merge, the production run pushes the steps. Re-run the dry run without `--env` to confirm the badge is cleared. `staging-site-master` picks up production's changes shortly after.

## Close with what's still open

Follow `.claude/docs/open-items.md`. Typical items:

- leftovers and skipped fields cleared on staging, waiting for chart-diff approval;
- `needs manual` findings;
- the post-merge production check.
