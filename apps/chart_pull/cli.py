"""Pull a chart's config from grapher into ETL.

Two use cases:

- **Resolve admin drift.** A chart already has an ETL config; a researcher edited it
  in admin. This rewrites the YAML to match the chart's current state, so the next
  `etlr` run is in sync.
- **Migrate from admin to ETL.** The chart has no ETL config yet. This creates a new
  zero-dim mdim export step (and adds it to the DAG) so future edits can flow through
  ETL.

Usage:

    etl chart-pull <slug-or-id> [--namespace <ns>] [--dag <file>] [--dry-run]

Namespace and DAG-file resolution:

1. If an existing `*.config.yml` for this chart is found, use its location for both.
2. Else use `--namespace` and `--dag` if given.
3. Else infer namespace from the chart's variables (must all share one). Default DAG
   path to `dag/<namespace>.yml`; error out if it doesn't exist and `--dag` wasn't
   passed.
4. Else fail with a clear message.
"""

from pathlib import Path
from typing import Any

import rich_click as click
import sqlalchemy as sa
import structlog
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

from etl import paths
from etl.config import OWID_ENV
from etl.dag_helpers import write_to_dag_file
from etl.files import ruamel_dump
from etl.grapher.model import Chart

log = structlog.get_logger()


# Fields written by grapher itself (or that live on the chart row rather than in the
# chart config); we don't want them in the YAML.
_DB_MANAGED_FIELDS = {
    "id",
    "version",
    "createdAt",
    "updatedAt",
    "lastEditedAt",
    "lastEditedByUserId",
    "publishedAt",
    "publishedByUserId",
    "bakedGrapherURL",
    "adminBaseUrl",
    # Chart-row flags merged in by etl.grapher.model.Chart.config — not part of the YAML.
    "isInheritanceEnabled",
    "forceDatapage",
}


@click.command(name="chart-pull", help=__doc__)
@click.argument("identifier")
@click.option(
    "--namespace",
    default=None,
    help="Namespace for the new ETL config (e.g. animal_welfare). Inferred from the chart's variables if not given.",
)
@click.option(
    "--dag",
    "dag_file",
    default=None,
    help="DAG file to add the new step to (path relative to dag/ or absolute). Defaults to dag/<namespace>.yml.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print what would be written without making changes.",
)
def cli(identifier: str, namespace: str | None, dag_file: str | None, dry_run: bool) -> None:
    """Pull a chart's config from grapher into ETL."""
    with Session(OWID_ENV.engine) as session:
        chart = _load_chart(session, identifier)
        full_config = dict(chart.config)
        var_id_to_path = _variable_id_to_catalog_path(session, _collect_variable_ids(full_config))
        tags = chart.tags(session)

    slug = full_config.get("slug")
    if not slug:
        raise click.ClickException(f"Chart {chart.id} has no slug; can't determine a YAML path.")
    short_name = slug.replace("-", "_")

    # Where does an existing config (if any) live?
    existing_yaml = _find_existing_config(short_name)

    # Resolve namespace.
    if existing_yaml is not None:
        ns = existing_yaml.parent.parent.name
    elif namespace:
        ns = namespace
    else:
        candidates = _namespaces_from_paths(var_id_to_path.values())
        if len(candidates) == 1:
            ns = candidates[0]
        else:
            raise click.ClickException(
                f"Couldn't infer namespace (variables span: {candidates or 'none'}). Pass --namespace."
            )

    # Resolve DAG file (only needed when creating a new step).
    dag_path: Path | None = None
    if existing_yaml is None:
        if dag_file:
            user_path = Path(dag_file)
            dag_path = user_path if user_path.is_absolute() else paths.DAG_DIR / user_path
        else:
            dag_path = paths.DAG_DIR / f"{ns}.yml"
        if not dag_path.exists():
            raise click.ClickException(
                f"DAG file not found: {dag_path}. Pass --dag <file> to point to an existing one."
            )

    # Compose the YAML.
    yaml_data = _build_yaml(full_config, var_id_to_path, tags)
    yaml_text = ruamel_dump(yaml_data)

    # Resolve config file path.
    config_path = existing_yaml or (
        paths.BASE_DIR / "etl" / "steps" / "export" / "multidim" / ns / "latest" / f"{short_name}.config.yml"
    )

    # DAG dependencies (only when creating a new step).
    step_uri = f"export://multidim/{ns}/latest/{short_name}"
    dependencies: set[str] = _dependencies_from_var_paths(var_id_to_path.values()) if dag_path is not None else set()

    if dry_run:
        click.echo(f"# Would write {config_path}:\n")
        click.echo(yaml_text)
        if dag_path is not None:
            click.echo(f"\n# Would add to {dag_path}:\n")
            click.echo(f"{step_uri}:")
            for dep in sorted(dependencies):
                click.echo(f"  - {dep}")
        return

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml_text)
    click.echo(f"Wrote {config_path}")
    log.info("chart_pull.wrote_config", path=str(config_path), slug=slug)

    if dag_path is not None:
        write_to_dag_file(dag_path, {step_uri: dependencies})
        click.echo(f"Updated {dag_path} (added {step_uri})")
        log.info("chart_pull.updated_dag", path=str(dag_path), step=step_uri)


def _load_chart(session: Session, identifier: str) -> Chart:
    """Load a chart by numeric id or by slug."""
    if identifier.isdigit():
        try:
            return Chart.load_chart(session, chart_id=int(identifier))
        except NoResultFound:
            raise click.ClickException(f"Chart id {identifier} not found.")
    try:
        return Chart.load_chart(session, slug=identifier)
    except NoResultFound:
        raise click.ClickException(f"Chart slug '{identifier}' not found.")


def _collect_variable_ids(config: dict) -> list[int]:
    ids = [int(d["variableId"]) for d in config.get("dimensions", []) if "variableId" in d]
    # `sortColumnSlug` and `map.columnSlug` also store variable ids (as stringified ints).
    for raw in (config.get("sortColumnSlug"), (config.get("map") or {}).get("columnSlug")):
        if raw and str(raw).isdigit():
            ids.append(int(raw))
    return ids


def _variable_id_to_catalog_path(session: Session, var_ids: list[int]) -> dict[int, str]:
    """Look up catalog paths for a list of variable ids. Errors out if any are missing."""
    if not var_ids:
        return {}
    rows = session.execute(
        sa.text("SELECT id, catalogPath FROM variables WHERE id IN :ids"),
        {"ids": tuple(var_ids)},
    ).fetchall()
    out: dict[int, str] = {}
    for vid, path in rows:
        if path is None:
            raise click.ClickException(
                f"Variable {vid} has no catalogPath. Pull is only supported for ETL-managed indicators."
            )
        out[int(vid)] = path
    missing = set(var_ids) - set(out)
    if missing:
        raise click.ClickException(f"Variables not found: {sorted(missing)}.")
    return out


def _short_indicator(catalog_path: str) -> str:
    """`grapher/ns/v/ds/table#col` → `table#col` (the YAML short form)."""
    return catalog_path.split("/")[-1]


def _namespaces_from_paths(paths_: list[str]) -> list[str]:
    seen = set()
    for p in paths_:
        parts = p.split("/")
        if len(parts) >= 2 and parts[0] == "grapher":
            seen.add(parts[1])
    return sorted(seen)


def _dependencies_from_var_paths(var_paths: list[str]) -> set[str]:
    """For each variable catalogPath, derive the `data://grapher/...` step that produced it."""
    deps: set[str] = set()
    for p in var_paths:
        stem = p.split("#")[0]  # grapher/ns/v/ds/table
        parts = stem.split("/")
        # Drop the trailing table name.
        ds_path = "/".join(parts[:-1])  # grapher/ns/v/ds
        deps.add(f"data://{ds_path}")
    return deps


def _find_existing_config(short_name: str) -> Path | None:
    """Find an existing `<short_name>.config.yml` under etl/steps/export/multidim/, if any."""
    root = paths.BASE_DIR / "etl" / "steps" / "export" / "multidim"
    if not root.exists():
        return None
    for match in root.rglob(f"{short_name}.config.yml"):
        return match
    return None


def _build_yaml(config: dict, var_id_to_path: dict[int, str], tags: list[dict]) -> dict:
    """Convert a grapher chart config into the ETL mdim YAML structure."""
    # Group indicators by axis (y/x/size/color), preserving order.
    indicators: dict[str, list[str]] = {}
    for dim in config.get("dimensions", []):
        prop = dim.get("property", "y")
        path = var_id_to_path[int(dim["variableId"])]
        indicators.setdefault(prop, []).append(_short_indicator(path))

    # View config: everything except DB-managed fields, the dimensions block (we
    # express them via `indicators`), and `selectedEntityNames` (lifted to top-level).
    view_config: dict[str, Any] = {
        k: v for k, v in config.items() if k not in _DB_MANAGED_FIELDS and k not in ("dimensions",)
    }
    selected = view_config.pop("selectedEntityNames", None)

    # Reverse the variable-id resolution for `sortColumnSlug` and `map.columnSlug`.
    if str(view_config.get("sortColumnSlug", "")).isdigit():
        view_config["sortColumnSlug"] = _short_indicator(var_id_to_path[int(view_config["sortColumnSlug"])])
    if isinstance(view_config.get("map"), dict):
        col_slug = view_config["map"].get("columnSlug")
        if col_slug and str(col_slug).isdigit():
            view_config["map"]["columnSlug"] = _short_indicator(var_id_to_path[int(col_slug)])

    yaml_data: dict[str, Any] = {}

    title = config.get("title")
    if title:
        yaml_data["title"] = {"title": title, "title_variant": ""}

    if selected:
        yaml_data["default_selection"] = list(selected)

    tag_names = [t["name"] for t in tags if t.get("name")]
    if tag_names:
        yaml_data["topic_tags"] = tag_names

    # Zero-dim mdim with a single view.
    yaml_data["dimensions"] = []
    yaml_data["views"] = [
        {
            "dimensions": {},
            "indicators": indicators,
            "config": view_config,
        }
    ]

    return yaml_data
