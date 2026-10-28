"""Clear admin fields that have no effect on how a chart renders.

A chart renders as merge(indicator config, ETL layer, admin layer), and the admin layer (the fields set
in the chart editor) wins. Some admin fields do nothing at all: a color scale on a line chart without a
color dimension, a map indicator that repeats the default, scatter settings on a bar chart, map settings
on a chart without a map tab. They change nothing on screen, but the admin's "ETL" badge counts them as
drift, a chart with inheritance enabled seems to override its indicator, and `etl chart-pull` would copy
them into ETL. This command finds them with fixed rules taken from grapher's code
(apps/chart_clean/rules.py) and clears them.

    # Report on chosen charts, or on every chart using a dataset (dry run).
    etl chart-clean 7118 8406
    etl chart-clean --dataset demography/efr_malani_jacob
    # Every chart, or only ETL-authored ones (as before `etl chart-pull`), or only those inheriting
    # their indicator's config.
    etl chart-clean --all --etl-only
    etl chart-clean --all --inheritance-only --published-only
    # Clear the fields on a branch's staging server.
    etl chart-clean 7118 8406 --apply --branch <staging-branch>

A dry run reads production when credentials are configured (ENV_FILE_PROD, or `--env <production env
file>`), otherwise staging-site-master, a copy that can lag production by hours. `--apply` reads and
writes the named branch's staging server only, never production; chart-diff carries the changes to
production, and each changed chart needs approving there. `--apply` therefore stops at 50 charts unless
`--max` raises the limit: clean thousands of charts with a grapher migration instead, using the
`--json` report.
"""

import json
import re
import subprocess
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import rich_click as click
from rich.console import Console
from rich.table import Table
from sqlalchemy import bindparam, text

from apps.chart_clean.rules import Finding, find_noop_fields, keep_paths, remove_paths
from apps.chart_pull.source import fetch_rows, source_env, warn_master_copy_age
from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWIDEnv

console = Console()

DEFAULT_MAX_APPLY = 50


@dataclass
class ChartFindings:
    """A chart and its admin fields that have no effect."""

    chart_id: int
    slug: str | None
    etl_authored: bool
    inheritance: bool
    published: bool
    findings: list[Finding] = field(default_factory=list)
    cleared: bool = False

    @property
    def kind(self) -> str:
        return "ETL" if self.etl_authored else ("inheritance" if self.inheritance else "admin")

    def to_dict(self, admin_site: str | None) -> dict[str, Any]:
        return {
            "chart_id": self.chart_id,
            "slug": self.slug,
            "admin_url": f"{admin_site}/charts/{self.chart_id}/edit",
            "kind": self.kind,
            "published": self.published,
            "fields": [{"path": f.path, "rule": f.rule, "reason": f.reason} for f in self.findings],
            "cleared": self.cleared,
        }


@click.command(name="chart-clean", help=__doc__, cls=click.RichCommand)
@click.argument("chart_ids", nargs=-1, type=int)
@click.option("--all", "select_all", is_flag=True, help="Every chart.")
@click.option(
    "--dataset",
    default=None,
    help="Charts plotting an indicator of this grapher dataset (`ns/version/short` or `ns/short`).",
)
@click.option("--etl-only", is_flag=True, help="Only ETL-authored charts (the ones `etl chart-pull` handles).")
@click.option("--inheritance-only", is_flag=True, help="Only charts that inherit their indicator's config.")
@click.option("--published-only", is_flag=True, help="Only published charts.")
@click.option(
    "--env",
    "env_name",
    default=None,
    help="Staging server name or env file to read from in a dry run. Defaults to production if configured.",
)
@click.option("--apply", is_flag=True, help="Clear the fields on the --branch staging server.")
@click.option("--branch", default=None, help="Staging branch to clean. Defaults to the current branch.")
@click.option("--max", "max_apply", type=int, default=DEFAULT_MAX_APPLY, help="Most charts --apply may change.")
@click.option(
    "--json",
    "json_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Also write the report as JSON.",
)
def cli(
    chart_ids: tuple[int, ...],
    select_all: bool,
    dataset: str | None,
    etl_only: bool,
    inheritance_only: bool,
    published_only: bool,
    env_name: str | None,
    apply: bool,
    branch: str | None,
    max_apply: int,
    json_path: Path | None,
) -> None:
    if not (select_all or dataset or chart_ids):
        raise click.UsageError("Name the charts: chart ids, --dataset or --all.")

    if apply:
        owid_env = staging_env(branch or _current_branch())
    else:
        owid_env, is_master_copy = source_env(env_name)
        if is_master_copy:
            warn_master_copy_age(owid_env)
    console.print(f"Reading chart layers from [bold]{owid_env.conf.DB_NAME}[/bold] on {owid_env.conf.DB_HOST}")

    charts = fetch_charts(owid_env, chart_ids, dataset, select_all, etl_only, inheritance_only, published_only)
    flagged = [c for c in charts if c.findings]

    if apply:
        if len(flagged) > max_apply:
            raise click.ClickException(
                f"{len(flagged)} charts would change, more than --max {max_apply}. Each one needs approving in "
                "chart-diff: narrow the selection, raise --max, or clean them with a grapher migration instead."
            )
        clear_fields(AdminAPI(owid_env), flagged)
        flagged = [c for c in flagged if c.cleared]

    _print_report(charts, flagged, apply)
    if json_path:
        json_path.write_text(json.dumps([c.to_dict(owid_env.admin_site) for c in flagged], indent=2) + "\n")
        console.print(f"Report written to {json_path}")


def staging_env(branch: str) -> OWIDEnv:
    """The branch's staging server. Refuses master and anything that isn't a staging server."""
    if branch in ("master", "main", ""):
        raise click.UsageError("--apply writes to a work branch's staging server only, never master or production.")
    owid_env = OWIDEnv.from_staging(branch)
    if owid_env.env_remote != "staging":
        raise click.UsageError(f"{branch} resolves to '{owid_env.env_remote}', not a staging server.")
    return owid_env


def fetch_charts(
    owid_env: OWIDEnv,
    chart_ids: tuple[int, ...],
    dataset: str | None,
    select_all: bool,
    etl_only: bool,
    inheritance_only: bool,
    published_only: bool,
) -> list[ChartFindings]:
    """The selected charts, each with its admin fields that have no effect."""
    conditions, params = [], {}
    if not select_all:
        selectors = []
        if chart_ids:
            selectors.append("c.id IN :chart_ids")
            params["chart_ids"] = list(chart_ids)
        if dataset:
            selectors.append(
                "c.id IN (SELECT cd.chartId FROM chart_dimensions cd JOIN variables v ON v.id = cd.variableId "
                "WHERE v.catalogPath LIKE :dataset_path ESCAPE '!')"
            )
            params["dataset_path"] = dataset_path_pattern(dataset)
        conditions.append("(" + " OR ".join(selectors) + ")")
    if etl_only:
        conditions.append("c.patchConfigIdETL IS NOT NULL")
    if inheritance_only:
        conditions.append("c.isInheritanceEnabled = 1")
    if published_only:
        conditions.append("c.publishedAt IS NOT NULL")
    statement = text(
        "SELECT c.id, cc.slug, c.patchConfigIdETL IS NOT NULL AS etl_authored, "
        "c.isInheritanceEnabled AS inheritance, c.publishedAt IS NOT NULL AS published, "
        "cc.config AS full, pa.config AS patch "
        "FROM charts c JOIN chart_configs cc ON cc.id = c.configId "
        "LEFT JOIN chart_configs pa ON pa.id = c.patchConfigId"
        + (" WHERE " + " AND ".join(conditions) if conditions else "")
        + " ORDER BY c.id"
    )
    if "chart_ids" in params:
        statement = statement.bindparams(bindparam("chart_ids", expanding=True))
    return [
        ChartFindings(
            chart_id=row.id,
            slug=row.slug,
            etl_authored=bool(row.etl_authored),
            inheritance=bool(row.inheritance),
            published=bool(row.published),
            findings=find_noop_fields(_as_dict(row.full), _as_dict(row.patch)),
        )
        for row in fetch_rows(owid_env, statement, params)
    ]


def dataset_path_pattern(dataset: str) -> str:
    """LIKE pattern for the catalog paths of a grapher dataset's variables (any version for `ns/short`).

    `_` and `%` in the names are escaped with `!` (the query's ESCAPE character), so they match literally.
    """
    dataset = dataset.removeprefix("data://grapher/").removeprefix("data-private://grapher/")
    parts = [re.sub(r"([!%_])", r"!\1", part) for part in dataset.split("/")]
    if len(parts) == 3:
        return f"grapher/{'/'.join(parts)}/%"
    if len(parts) == 2:
        return f"grapher/{parts[0]}/%/{parts[1]}/%"
    raise click.UsageError(
        f"Dataset must look like `namespace/version/short_name` or `namespace/short_name`: {dataset}"
    )


def clear_fields(admin_api: AdminAPI, charts: list[ChartFindings]) -> None:
    """Remove each chart's no-op fields from its config; grapher recomputes the admin patch on save.

    The rules run again on the config fetched right before the write, so a field the chart started reading
    after the selection (say `map.time` on a chart whose map tab was switched on since) is kept. Each
    chart's findings become the fields actually cleared.
    """
    for chart in charts:
        config = admin_api.get_chart_config(chart.chart_id)
        flagged = [f.path for f in chart.findings]
        chart.findings = [f for f in find_noop_fields(config, keep_paths(config, flagged)) if f.path in flagged]
        if chart.findings:
            admin_api.update_chart(chart.chart_id, remove_paths(config, [f.path for f in chart.findings]))
            chart.cleared = True


def _print_report(charts: list[ChartFindings], flagged: list[ChartFindings], apply: bool) -> None:
    table = Table(title="Admin fields with no effect")
    for column in ("Chart", "Slug", "Kind", "Fields"):
        table.add_column(column)
    for chart in flagged:
        table.add_row(
            str(chart.chart_id),
            chart.slug or "-",
            chart.kind,
            "\n".join(f"{f.path} ({f.reason})" for f in chart.findings),
        )
    console.print(table)

    kinds = Counter(c.kind for c in flagged)
    console.print(
        f"{len(flagged)} of {len(charts)} charts have no-op admin fields "
        f"({', '.join(f'{n} {k}' for k, n in kinds.most_common()) or 'none'})."
    )
    for rule, count in Counter(f.rule for c in flagged for f in c.findings).most_common():
        console.print(f"  {rule}: {count}")
    if flagged and not apply:
        console.print(f"Dry run: --apply --branch <staging-branch> would clear them on {len(flagged)} charts.")
    if apply and flagged:
        console.print(
            f"Cleared on staging: {len(flagged)} charts. Approve them in chart-diff so the changes reach production."
        )


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return json.loads(value or "{}")


def _current_branch() -> str:
    return subprocess.check_output(["git", "branch", "--show-current"], text=True).strip()
