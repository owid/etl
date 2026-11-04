"""Restore the y=x comparison lines that were deleted from 103 published scatter charts.

The owid-grapher migration `RemoveEmptyComparisonLines1776176663970` (2026-04-14) dropped
`comparisonLines` from every chart config whose value was `[]` or `[{}]`, on the assumption
that both are no-ops. `[{}]` is not: a comparison line with no `yEquals` renders as y = x
(`generateComparisonLinePoints(lineFunction: string = "x", ...)`), and the admin's
"Add comparison line" button used to write exactly `{}`. Every scatter chart whose author
accepted the default diagonal therefore lost its line. The migration's `down()` is a no-op.

The affected charts were recovered from archive.ourworldindata.org by comparing each chart's
last archived version before the migration against its config today; see the accompanying
`restore_comparison_lines.csv`. The same sweep found 6 non-scatter charts, excluded here:
y = x over a time axis draws nothing meaningful, and one had a genuinely empty `[]`.

This writes `comparisonLines: [{"yEquals": "x"}]` — the explicit form the admin writes today,
identical in rendering to the `{}` that was removed.

Run against a staging server, review the diffs in chart-diff, then merge the PR so that
chart-sync carries the approved charts to production:

    python scripts/restore_comparison_lines.py --staging restore-comparison-lines --dry-run
    python scripts/restore_comparison_lines.py --staging restore-comparison-lines
"""

import csv
from pathlib import Path

import click
import structlog
from rich_click.rich_command import RichCommand

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWIDEnv

log = structlog.get_logger()

CHARTS_FILE = Path(__file__).parent / "restore_comparison_lines.csv"
COMPARISON_LINES = [{"yEquals": "x"}]


@click.command(name="restore-comparison-lines", cls=RichCommand, help=__doc__)
@click.option("--staging", required=True, type=str, help="Staging branch name to write to.")
@click.option("--dry-run/--no-dry-run", default=False, help="Report what would change without writing.")
def cli(staging: str, dry_run: bool) -> None:
    env = OWIDEnv.from_staging(staging)
    api = AdminAPI(env)

    with CHARTS_FILE.open() as f:
        charts = list(csv.DictReader(f))

    updated, already_set, failed = 0, 0, []
    for chart in charts:
        chart_id, slug = int(chart["chart_id"]), chart["slug"]
        try:
            config = api.get_chart_config(chart_id)
            if config.get("comparisonLines"):
                log.info("skip", chart_id=chart_id, slug=slug, reason="comparisonLines already set")
                already_set += 1
                continue
            config["comparisonLines"] = COMPARISON_LINES
            if not dry_run:
                api.update_chart(chart_id, config)
            log.info("restored" if not dry_run else "would restore", chart_id=chart_id, slug=slug)
            updated += 1
        except Exception as e:
            log.error("failed", chart_id=chart_id, slug=slug, error=str(e))
            failed.append(chart_id)

    log.info("done", updated=updated, already_set=already_set, failed=len(failed), failed_ids=failed)


if __name__ == "__main__":
    cli()
