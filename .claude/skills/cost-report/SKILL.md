---
name: cost-report
description: Retrospectively measure how long and how many tokens a past Claude Code task actually cost — a /update-dataset run, a /review-data-pr run, or anything else with session transcripts. Reconstructs a timeline after the fact rather than requiring live instrumentation. Trigger when the user asks "how long did that update take", "how many tokens did X cost", "run the cost report", or wants to compare cost across several past updates.
metadata:
  internal: true
---

# Cost Report

Answers "how long / how much did this cost" for work Claude Code already did — without requiring the original task to have logged anything special while it ran. Works after the fact, on any session transcript that still exists.

This is deliberately **not** wired into `/update-dataset` or `/review-data-pr` themselves — earlier versions logged live timing during those workflows, but that adds bookkeeping to every run for a question that's only occasionally asked. Reconstructing from the transcript afterwards is just as good and has zero cost on the common path.

## Inputs

- What to analyze: a dataset short_name (`workbench/<short_name>/` may already exist from `/update-dataset`), a PR number/branch, a date range, or explicit session ID(s) if you already know them.
- Optional: whether the user wants one total number ("how much did the whole thing cost") or a per-step breakdown ("which part was expensive").

## Workflow

1) **Locate the session transcript(s).**
   - If a `workbench/<short_name>/progress.md` already exists (from `/update-dataset`), grep the project's transcripts for `workbench/<short_name>` — see `discover_sessions()` in `cost_report.py` for the exact matching logic.
   - Otherwise, search `~/.claude/projects/<encoded-cwd>/*.jsonl` for the PR number, branch name, or dataset/short_name text. A task can span several sessions (e.g. resumed after a multi-day gap) — find all of them, not just one.
   - **Check transcripts still exist before promising a report.** Claude Code's session logs rotate out over time. Compare the task's start date against the earliest transcript timestamp still on disk across the relevant project dirs (`jq -r 'select(.timestamp) | .timestamp' <file>.jsonl | sort | head -1` for each file, take the min). If the task predates that floor, say so plainly — there is no reconstruction path, the data is gone, not just hard to find.

2) **Build (or reuse) a `## Step timing log`** in `<workbench_dir>/progress.md` — this is the only input `cost_report.py` needs beyond the transcripts themselves. Two levels of effort depending on what the user wants:

   - **Single total** (the common case — "how long/much did the whole thing take"): two lines suffice.
     ```
     ## Step timing log
     - 2026-01-29T13:32:12Z START
     - 2026-02-02T05:58:17Z DONE whole-task
     ```
     Use the first session's first timestamp as `START` and the last session's last timestamp (or a specific "done"/"complete" message) as `DONE`.

   - **Per-step breakdown** ("which part was expensive"): read the session's own narrative for natural transitions — messages like "Step N: ...", checkpoint summaries, "Now let's run the garden step" — and use their real timestamps as `DONE <slug>` lines, one per transition, in chronological order. This is a reconstruction, not a live log — say so in the progress.md (a one-line note is enough) so nobody mistakes it for exact instrumentation. Pick short, stable kebab-case slugs (e.g. `etl-update`, `snapshot`, `meadow`, `garden`, `grapher`, `indicator-upgrade`) — matching the conventional names from `/update-dataset`'s workflow numbering makes cross-update aggregation (step 4 below) actually merge rows.

   If `<short_name>` isn't obvious (e.g. analyzing a `/review-data-pr` run), use a clear directory name like `workbench/review-<short_name>/` or `workbench/analysis-<topic>/` — any path works, `cost_report.py` doesn't care about naming beyond what you pass it.

3) **Run the report:**
   ```bash
   .venv/bin/python .claude/skills/cost-report/scripts/cost_report.py <workbench_dir> \
       [--session <id> ...] [--project-dir <dir>]
   ```
   Pass `--session` explicitly when auto-discovery might pick up unrelated sessions (common terms, or a dataset name that recurs as an example elsewhere). Pass `--project-dir` when the task ran in a different worktree than the one you're running the script from — each worktree has its own encoded transcript directory (`~/.claude/projects/<encoded-absolute-path>/`).

   Writes `<workbench_dir>/cost_report.md` (and a `cost_report.json` sidecar) with, per step: **wall time** (raw calendar delta between step boundaries — balloons across a multi-day pause between sessions) and **active time** (sum of gaps between consecutive requests, each capped at 5 minutes, to approximate real work and exclude idle waiting), plus request/agent counts, token categories, and an input-equivalent weighted total (output ×5, cache write ×1.25, cache read ×0.1 — a relative cost proxy, not USD). Requests outside the logged window (before `START` / after the last `DONE`) are excluded from totals and called out separately, not silently folded in.

4) **Aggregating across several past tasks.** Once more than one task has its own `cost_report.json`, roll them up — cheap to re-run, since it only reads the JSON sidecars, no transcripts:
   ```bash
   .venv/bin/python .claude/skills/cost-report/scripts/aggregate_cost_reports.py \
       [--workbench-root workbench] [--output workbench/aggregate_cost_report.md]
   ```
   Produces a per-task comparison table and a per-step rollup (e.g. "garden steps cost X active-minutes / Y tokens across all updates combined") — useful for spotting which *kind* of step is consistently expensive. Rollup rows only merge on an exact slug match, so consistent slugs (step 2) matter here.

## Notes

- **Never self-report token numbers from memory or estimation** — the transcript is the only reliable source. If a script fails or transcripts are missing, say so rather than guessing.
- The report is a workbench artifact for the user's own reference — don't post it to a PR description or comment unless asked.
- Wall time includes any time the user wasn't actively working (a multi-day pause reads as huge wall time but near-zero active time) — read active time as the more trustworthy cost signal, wall time as elapsed calendar time including idle waiting.
