"""Roll up cost_report.json sidecars from multiple dataset updates into one summary.

Reads every `workbench/*/cost_report.json` written by `cost_report.py` (main
update-dataset workbenches) and `workbench/review-*/cost_report.json` (review-data-pr
workbenches), and writes a combined markdown report with two views:

1. Per-update totals — one row per update, for comparing updates against each other.
2. Per-step rollup — sums grouped by step label across every update, for seeing which
   *kind* of step (e.g. "garden") is expensive on average, not just within one update.

No transcripts are re-read and no session discovery happens here — this only
aggregates numbers already computed by cost_report.py, so it's cheap to re-run.

Usage:
    .venv/bin/python .claude/skills/update-dataset/scripts/aggregate_cost_reports.py \
        [--workbench-root workbench] [--output workbench/aggregate_cost_report.md]

Step-label rollup only merges rows whose slug strings match exactly across updates —
follow the skill's conventional slugs (etl-update, snapshot, meadow, garden, grapher,
indicator-upgrade, ...) so updates stay comparable; an update run before the naming
was settled just won't merge into the same rollup row.
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path


def fmt_timedelta(seconds: int) -> str:
    secs = int(seconds)
    return f"{secs // 3600}:{secs % 3600 // 60:02d}:{secs % 60:02d}"


def load_reports(workbench_root: Path) -> list[dict]:
    reports = []
    for path in sorted(workbench_root.glob("*/cost_report.json")):
        try:
            reports.append(json.loads(path.read_text()))
        except json.JSONDecodeError:
            continue
    return reports


def build_per_update_table(reports: list[dict]) -> list[str]:
    lines = [
        "## Per-update totals",
        "",
        "| Update | Sessions | Active time | Requests | Agents | Weighted tokens |",
        "|---|---:|---|---:|---:|---:|",
    ]
    grand = defaultdict(int)
    grand_active = 0
    for r in reports:
        t = r["total"]
        grand_active += t["active_seconds"]
        for k in ("requests", "agents", "input", "output", "cache_read", "cache_write", "weighted"):
            grand[k] += t[k]
        name = Path(r["workbench_dir"]).name
        lines.append(
            f"| {name} | {len(r['sessions'])} | {fmt_timedelta(t['active_seconds'])} "
            f"| {t['requests']:,} | {t['agents']:,} | {t['weighted']:,} |"
        )
    lines.append(
        f"| **Total ({len(reports)} updates)** | | {fmt_timedelta(grand_active)} "
        f"| **{grand['requests']:,}** | **{grand['agents']:,}** | **{grand['weighted']:,}** |"
    )
    return lines


def build_step_rollup_table(reports: list[dict]) -> list[str]:
    by_label: dict[str, dict] = defaultdict(lambda: defaultdict(int))
    updates_touching: dict[str, set] = defaultdict(set)
    for r in reports:
        name = Path(r["workbench_dir"]).name
        for step in r["steps"]:
            label = step["label"]
            updates_touching[label].add(name)
            by_label[label]["active_seconds"] += step["active_seconds"]
            for k in ("requests", "agents", "input", "output", "cache_read", "cache_write", "weighted"):
                by_label[label][k] += step[k]

    lines = [
        "## Per-step rollup (across all updates)",
        "",
        "| Step | Updates | Active time | Requests | Weighted tokens |",
        "|---|---:|---|---:|---:|",
    ]
    for label, agg in sorted(by_label.items(), key=lambda kv: -kv[1]["weighted"]):
        lines.append(
            f"| {label} | {len(updates_touching[label])} | {fmt_timedelta(agg['active_seconds'])} "
            f"| {agg['requests']:,} | {agg['weighted']:,} |"
        )
    return lines


def build_report(reports: list[dict], workbench_root: Path) -> str:
    if not reports:
        return "# Aggregate cost report\n\nNo `cost_report.json` files found.\n"
    lines = [
        "# Aggregate cost report",
        "",
        f"{len(reports)} instrumented update(s) found under `{workbench_root}`.",
        "",
        *build_per_update_table(reports),
        "",
        *build_step_rollup_table(reports),
        "",
        "Step-label rows only merge exact string matches — an update whose timing log used a "
        "different slug for the same kind of step won't roll up into the same row. Figures here "
        "are copied straight from each update's own `cost_report.json`, not recomputed from "
        "transcripts, so re-running this is cheap and always reflects the latest per-update reports.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workbench-root", type=Path, default=Path("workbench"), help="default: workbench")
    parser.add_argument("--output", type=Path, default=None, help="default: <workbench-root>/aggregate_cost_report.md")
    args = parser.parse_args()

    if not args.workbench_root.exists():
        raise SystemExit(f"{args.workbench_root} not found — run from the repo root.")

    reports = load_reports(args.workbench_root)
    output = args.output or args.workbench_root / "aggregate_cost_report.md"
    output.write_text(build_report(reports, args.workbench_root))
    print(f"Wrote {output} ({len(reports)} update(s) aggregated)")


if __name__ == "__main__":
    main()
