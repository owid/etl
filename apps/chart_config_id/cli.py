"""Set the `chart_config_id` of an ETL-authored single-chart config YAML.

A single chart's identity is its config UUID (`charts.configId` in grapher), declared as
`chart_config_id` in its `.config.yml`. ETL never resolves it per environment, so the value in
the file decides which chart every push lands on — getting it wrong either abandons a chart or
creates a duplicate. This command writes it for you, in the two situations that come up:

    # Brand-new chart: mint a UUIDv7 (same shape as the ones grapher generates).
    etl chart-config-id new etl/steps/export/multidim/animal_welfare/latest/my_chart.config.yml

    # Existing chart moving into ETL: take the UUID from the chart already in grapher,
    # identified by slug or by the numeric id from the admin URL.
    etl chart-config-id lookup <config.yml> --slug banning-of-chick-culling
    etl chart-config-id lookup <config.yml> --chart-id 7118

`lookup` reads the chart from the configured grapher database (`OWID_ENV`, or `--env` to point at
a staging server or env file). The chart is always named explicitly — nothing is inferred from the
file name, since silently picking the wrong chart is exactly the failure this field exists to
prevent.
"""

from pathlib import Path
from typing import cast

import rich_click as click
from rich.console import Console
from ruamel.yaml.comments import CommentedMap
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from etl.collection.chart_upsert import new_chart_config_id
from etl.config import OWID_ENV, OWIDEnv
from etl.files import ruamel_dump, ruamel_load

console = Console()

FIELD = "chart_config_id"


def _load_config(path: Path) -> CommentedMap:
    """Read the config YAML and refuse anything that isn't a single-chart config.

    `chart_config_id` identifies one chart, so it is only meaningful on a collection that pushes
    as one chart: no dimensions and exactly one view. Mdims — including the ones that declare
    `dimensions: []` in the YAML and fill dimensions/views programmatically — are identified by
    their catalog path instead, and `Collection.validate_chart_config_id()` rejects the field on
    them.
    """
    # `ruamel_load` is annotated as returning a plain dict, but in round-trip mode it hands back a
    # CommentedMap — that's what carries the comments and lets us reorder keys below.
    config = cast(CommentedMap, ruamel_load(path))  # ty: ignore[invalid-argument-type]
    if not isinstance(config, CommentedMap):
        raise click.ClickException(f"{path} does not contain a YAML mapping.")

    dimensions = config.get("dimensions")
    views = config.get("views")
    if dimensions or not isinstance(views, list) or len(views) != 1:
        raise click.ClickException(
            f"{path} is not a single-chart config (expected `dimensions: []` and exactly one view, "
            f"got {len(dimensions or [])} dimensions and {len(views or [])} views). Multi-dimensional "
            f"collections are identified by their catalog path and must not declare `{FIELD}`."
        )
    return config


def _write_field(path: Path, config: CommentedMap, chart_config_id: str, force: bool) -> None:
    """Write `chart_config_id` into the config YAML, preserving comments and key order."""
    existing = config.get(FIELD)
    if existing == chart_config_id:
        console.print(f"[yellow]{path} already has `{FIELD}: {existing}` — nothing to do.[/yellow]")
        return
    # Same UUID in a different case is the same chart — rewriting it to the canonical
    # lower-case form (the only form `validate_chart_config_id` accepts) needs no --force.
    same_chart = isinstance(existing, str) and existing.lower() == chart_config_id.lower()
    if existing and not same_chart and not force:
        raise click.ClickException(
            f"{path} already declares `{FIELD}: {existing}`. That UUID is the identity of an existing "
            f"chart — replacing it makes the next push abandon that chart and create a new one. "
            f"Pass --force if that is really what you want."
        )

    config[FIELD] = chart_config_id
    # Put the identity first; it reads as the header of the file rather than being buried in it.
    config.move_to_end(FIELD, last=False)

    path.write_text(ruamel_dump(config))
    verb = "Replaced" if existing else "Wrote"
    console.print(f"[green]{verb} `{FIELD}: {chart_config_id}` in {path}[/green]")


@click.group(name="chart-config-id", help=__doc__, cls=click.RichGroup)
def cli() -> None:
    pass


@cli.command(name="new", cls=click.RichCommand)
@click.argument("config_file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--force", is_flag=True, help=f"Overwrite an existing `{FIELD}` (abandons the chart it points at).")
def new(config_file: Path, force: bool) -> None:
    """Mint a new chart config UUID and write it to CONFIG_FILE.

    Use this for a chart that does not exist in grapher yet: the first push creates it, carrying
    this UUID as its identity in every environment it is later synced to.
    """
    config = _load_config(config_file)
    _write_field(config_file, config, new_chart_config_id(), force)


@cli.command(name="lookup", cls=click.RichCommand)
@click.argument("config_file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--slug", default=None, help="Slug of the chart to look up, e.g. banning-of-chick-culling.")
@click.option("--chart-id", type=int, default=None, help="Numeric chart id to look up, e.g. 7118 (`charts.id`).")
@click.option(
    "--env",
    "env_name",
    default=None,
    help="Staging server name or path to an env file. Defaults to the configured environment (OWID_ENV).",
)
@click.option("--force", is_flag=True, help=f"Overwrite an existing `{FIELD}` (abandons the chart it points at).")
def lookup(config_file: Path, slug: str | None, chart_id: int | None, env_name: str | None, force: bool) -> None:
    """Look up an existing chart's config UUID and write it to CONFIG_FILE.

    Use this when bringing a chart that already exists in grapher under ETL authorship, so pushes
    land on that chart instead of creating a duplicate. Identify the chart by `--slug` or by
    `--chart-id` (the numeric id in the admin URL, and what older automation tends to carry).
    """
    if (slug is None) == (chart_id is None):
        raise click.UsageError("Pass exactly one of --slug or --chart-id.")

    config = _load_config(config_file)
    owid_env = OWIDEnv.from_staging_or_env_file(env_name) if env_name else OWID_ENV

    target = f"chart {chart_id}" if chart_id is not None else f"chart '{slug}'"
    console.print(f"Looking up {target} in [bold]{owid_env.conf.DB_NAME}[/bold] on {owid_env.conf.DB_HOST}")

    # Deliberately a plain SELECT of the columns we need rather than loading the ORM `Chart`: some
    # environments (production, until the grapher release lands) lack the `etlConfigCatalogPath` /
    # `patchConfigIdETL` columns the model declares, and selecting those would fail with "Unknown column".
    if chart_id is not None:
        # `charts.id` is the primary key, so this matches at most one row — no ambiguity to resolve.
        query = (
            "SELECT c.id, c.configId, c.publishedAt IS NOT NULL AS is_published FROM charts c WHERE c.id = :chart_id"
        )
        params: dict[str, object] = {"chart_id": chart_id}
    else:
        query = (
            "SELECT c.id, c.configId, c.publishedAt IS NOT NULL AS is_published "
            "FROM charts c JOIN chart_configs cf ON cf.id = c.configId "
            "WHERE cf.slug = :slug"
        )
        params = {"slug": slug}

    try:
        with owid_env.engine.connect() as con:
            rows = con.execute(text(query), params).all()
    except OperationalError as e:
        raise click.ClickException(
            f"Could not reach the grapher database at {owid_env.conf.DB_HOST} "
            f"(db '{owid_env.conf.DB_NAME}'): {e.orig}\n"
            "Start your local grapher DB, or point at another environment with "
            "`--env <staging-branch>` / `--env <path/to/.env>`."
        ) from None

    if not rows:
        raise click.ClickException(
            f"No {target} in {owid_env.conf.DB_NAME}. Check the chart (--slug / --chart-id) or the "
            f"environment (--env); use `etl chart-config-id new` if the chart doesn't exist yet."
        )
    if len(rows) > 1:
        # Only reachable via --slug: drafts can share a slug with a published chart, so prefer the
        # published one and say so.
        published = [row for row in rows if row.is_published]
        if len(published) != 1:
            listed = ", ".join(f"chart {row.id} ({row.configId})" for row in rows)
            raise click.ClickException(
                f"Slug '{slug}' matches {len(rows)} charts and none is unambiguously published: {listed}. "
                f"Re-run with --chart-id to pick one."
            )
        console.print(f"[yellow]Slug '{slug}' matches {len(rows)} charts; using the published one.[/yellow]")
        rows = published

    row = rows[0]
    console.print(f"Found chart {row.id} → {row.configId} ({owid_env.admin_site}/charts/{row.id}/edit)")
    # Canonicalize: UUIDs are case-insensitive and the DB may hold an upper-case one,
    # but the config YAML declares the identity in canonical lower-case dashed form.
    _write_field(config_file, config, str(row.configId).lower(), force)
