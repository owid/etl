"""One-off repair of charts whose `map.columnSlug` / `sortColumnSlug` names no column they plot.

`etl.grapher.column_slugs` explains what a dangling slug is and why it matters. This script is
the *backlog* half: charts that already carry one, from years of admin edits and indicator
upgrades applied straight to production. The forward half — not creating new ones — lives in
`etl.collection.chart_upsert._repair_dangling_column_slugs`, and this script shares its
matching rules, so both remap to the same indicator's current variable where they can and drop
the field where they can't.

    # what would change, no writes (the default)
    .venv/bin/python scripts/repair_dangling_column_slugs.py --target .env.prod.write

    # write it
    .venv/bin/python scripts/repair_dangling_column_slugs.py --target .env.prod.write --no-dry-run

WHAT IT WRITES. One `PUT /admin/api/charts/:id` per repaired chart — the same call the chart
editor makes on save, so Grapher recomputes the chart's admin patch against its current parent
stack. Per chart that means a new `chart_configs` version, a chart revision, an R2 re-upload
and a static rebuild of the pages that embed it. Nothing else is touched: dimensions, and every
config field other than the two slugs, are sent back exactly as they came.

Run it against a staging server first and read the report. `--dry-run` is the default and
prints the same report without writing.
"""

from typing import Any

import click
import structlog
from rich import print
from rich_click.rich_command import RichCommand
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

import etl.grapher.model as gm
from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWIDEnv
from etl.grapher.column_slugs import SlugRepair, repair_column_slugs, variable_ids_to_resolve

log = structlog.get_logger()


@click.command(name="repair-dangling-column-slugs", cls=RichCommand, help=__doc__)
@click.option(
    "--target",
    required=True,
    type=str,
    help="Staging server name (e.g. `staging-site-mybranch`) or path to an .env file. Use `.env.prod.write` for live.",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=True,
    type=bool,
    help="Report what would change without writing. On by default.",
)
@click.option(
    "--chart-id",
    type=int,
    default=None,
    help="Repair **_only_** the chart with this id. Useful for trying one before the full run.",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Repair at most this many charts, in ascending chart-id order.",
)
def cli(target: str, dry_run: bool, chart_id: int | None, limit: int | None) -> None:
    owid_env = OWIDEnv.from_staging_or_env_file(target)
    admin_api = AdminAPI(owid_env)

    charts = _load_charts_with_column_slugs(owid_env, chart_id)
    print(f"[bold]{len(charts)}[/bold] charts declare a column-slug field in {target}")

    repairs_by_chart = _plan_repairs(owid_env, charts)
    if not repairs_by_chart:
        print("[green]Nothing dangling — no chart names a column it doesn't plot.[/green]")
        return

    _report(repairs_by_chart)

    if limit is not None:
        repairs_by_chart = dict(sorted(repairs_by_chart.items())[:limit])
        print(f"[yellow]--limit {limit}: writing only chart ids {sorted(repairs_by_chart)}[/yellow]")

    if dry_run:
        print(f"\n[yellow]Dry run — nothing written. Re-run with --no-dry-run to repair {len(repairs_by_chart)} charts.[/yellow]")
        return

    for chart_id_, (repaired_config, repairs) in sorted(repairs_by_chart.items()):
        admin_api.update_chart(chart_id_, repaired_config)
        log.info("column_slug_repair.updated", chart_id=chart_id_, repairs=[str(r) for r in repairs])

    print(f"\n[green]Repaired {len(repairs_by_chart)} charts.[/green]")


def _load_charts_with_column_slugs(owid_env: OWIDEnv, chart_id: int | None) -> dict[int, tuple[str | None, dict]]:
    """Charts whose *rendered* config sets either column-slug field, as `{id: (slug, config)}`.

    Filtering in SQL keeps this to the ~1,300 charts that could possibly be affected rather than
    every chart in the database; whether a slug is actually *dangling* is decided in Python,
    against the same rules the ETL push uses. Addressed through the ORM rather than raw SQL so
    it tracks `gm.ChartConfig` — grapher has reshaped these tables before, and a stale query
    would fail on the column name instead of quietly reading the wrong layer.
    """
    query = (
        select(gm.Chart.id, gm.ChartConfig.slug, gm.ChartConfig.config)
        .join(gm.ChartConfig, gm.ChartConfig.id == gm.Chart.configId)
        .where(
            or_(
                func.json_extract(gm.ChartConfig.config, "$.map.columnSlug").isnot(None),
                func.json_extract(gm.ChartConfig.config, "$.sortColumnSlug").isnot(None),
            )
        )
        .order_by(gm.Chart.id)
    )
    if chart_id is not None:
        query = query.where(gm.Chart.id == chart_id)

    with Session(owid_env.engine) as session:
        return {row.id: (row.slug, row.config) for row in session.execute(query)}


def _plan_repairs(
    owid_env: OWIDEnv,
    charts: dict[int, tuple[str | None, dict]],
) -> dict[int, tuple[dict[str, Any], list[SlugRepair]]]:
    """Work out the repair for every chart that needs one, without writing anything."""
    # One lookup for every variable referenced across all the charts, rather than a query per
    # chart. Ids that come back missing were deleted, which `repair_column_slugs` reads as
    # "nothing to match on".
    wanted: set[int] = set()
    for _, config in charts.values():
        wanted |= variable_ids_to_resolve(config)
    catalog_paths = _load_catalog_paths(owid_env, wanted)

    planned = {}
    for chart_id, (slug, config) in charts.items():
        repaired_config, repairs = repair_column_slugs(config, catalog_paths)
        if repairs:
            planned[chart_id] = (repaired_config, repairs)
            log.info(
                "column_slug_repair.planned",
                chart_id=chart_id,
                slug=slug,
                repairs=[str(r) for r in repairs],
            )
    return planned


def _load_catalog_paths(owid_env: OWIDEnv, variable_ids: set[int]) -> dict[int, str | None]:
    if not variable_ids:
        return {}
    with Session(owid_env.engine) as session:
        return gm.Variable.variable_ids_to_catalog_paths(session, variable_ids)


def _report(repairs_by_chart: dict[int, tuple[dict[str, Any], list[SlugRepair]]]) -> None:
    remapped = [r for _, repairs in repairs_by_chart.values() for r in repairs if r.new_id is not None]
    dropped = [r for _, repairs in repairs_by_chart.values() for r in repairs if r.new_id is None]

    print(f"\n[bold]{len(repairs_by_chart)}[/bold] charts name a column they don't plot:")
    print(f"  [green]{len(remapped)}[/green] fields remapped to the same indicator's current variable")
    print(f"  [yellow]{len(dropped)}[/yellow] fields dropped, by reason:")
    for reason in sorted({r.reason for r in dropped}):
        print(f"    {sum(1 for r in dropped if r.reason == reason)}  {reason}")

    print("\n[bold]Per chart:[/bold]")
    for chart_id, (_, repairs) in sorted(repairs_by_chart.items()):
        print(f"  {chart_id}: " + "; ".join(str(r) for r in repairs))


if __name__ == "__main__":
    cli()
