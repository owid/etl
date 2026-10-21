"""Bring admin edits of ETL-authored charts back into their ETL config.

An ETL-authored single chart (a `viz://chart` step with `chart_config_id`) can still be edited in
the admin. Those edits live in the chart's admin layer and win over ETL, and the admin's ETL badge
flags the chart as drifted. This command reads the admin layer and writes it back into the step:

- Field drift: every field set in the admin is copied into the chart's `.config.yml`. After the
  next push, grapher drops those fields from the admin layer, since ETL now holds them. This never
  changes what readers see. Where the admin value replaces one the YAML set, the report says so.
- Admin leftovers: when a chart moves into ETL, its old admin config stays in the admin layer and
  keeps masking any ETL value that differs. If no person has saved the chart since it moved to ETL,
  such values are leftovers: they are left out (`--pull-leftovers` copies them anyway), and should
  be cleared in the admin so the ETL values show. `--skip-field` leaves out any other field.
- Slug drift: an admin slug that differs from the one ETL derives from the step name. Until chart
  configs carry a `slug` field, the fix is renaming the step (its files and DAG entry) to match.
  Commit the rename, then run `etl archive-dag` and commit the archived old name.

    # Report drift for every ETL-authored chart (dry run).
    etl chart-reconcile --all
    # Only the charts built from one dataset; any version with `namespace/short_name`.
    etl chart-reconcile --dataset oecd/2026-10-07/climate_related_official_development_assistance
    # Specific charts, by config file, step URI or chart id; --apply writes the changes.
    etl chart-reconcile viz://chart/oecd/latest/foreign_aid_given_climate --chart-id 8406 --apply
    # After pushing the changes to a branch's staging server: every chart should be in sync.
    etl chart-reconcile --dataset oecd/climate_related_official_development_assistance --env <branch>

The admin layers are read from staging-site-master, whose database mirrors production's (or from
production itself when ENV_FILE_PROD is set), so no production credentials are needed. `--env`
reads another staging server or env file instead. Multi-dimensional charts and explorers are out
of scope.
"""

import difflib
import json
from pathlib import Path

import rich_click as click
from rich.console import Console
from rich.table import Table
from sqlalchemy import bindparam, text
from sqlalchemy.exc import OperationalError

from apps.chart_config_id.cli import _load_config as load_single_chart_config
from apps.chart_reconcile.reconcile import (
    ChartLayers,
    ChartStep,
    Result,
    depends_on_dataset,
    dump_config,
    find_chart_steps,
    reconcile_fields,
    rename_step,
    slug_target,
    variable_ids_in_patch,
)
from etl.config import ETL_GRAPHER_USER_ID, OWIDEnv
from etl.db import production_or_master_env
from etl.paths import BASE_DIR, DAG_DIR, STEP_DIR

console = Console()


@click.command(name="chart-reconcile", help=__doc__, cls=click.RichCommand)
@click.argument("targets", nargs=-1)
@click.option("--all", "select_all", is_flag=True, help="Every ETL-authored single chart in the active DAG.")
@click.option(
    "--dataset",
    default=None,
    help="Charts whose step depends on this grapher dataset (`ns/version/short` or `ns/short`).",
)
@click.option("--chart-id", "chart_ids", type=int, multiple=True, help="Numeric chart id (`charts.id`). Repeatable.")
@click.option("--apply", is_flag=True, help="Write the changes. Without it, only report them.")
@click.option("--keep-slug", is_flag=True, help="With --apply: pull fields but don't rename steps for slug drift.")
@click.option(
    "--skip-field",
    "skip_fields",
    multiple=True,
    help="Top-level field to leave out, e.g. when the admin value is a stale leftover and ETL's should win. Repeatable.",
)
@click.option(
    "--pull-leftovers",
    is_flag=True,
    help="Also copy admin values set before the chart moved to ETL that would replace a newer ETL value.",
)
@click.option(
    "--env",
    "env_name",
    default=None,
    help="Staging server name or env file to read from. Defaults to staging-site-master (a mirror of production).",
)
@click.option(
    "--json",
    "json_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Also write the report as JSON.",
)
def cli(
    targets: tuple[str, ...],
    select_all: bool,
    dataset: str | None,
    chart_ids: tuple[int, ...],
    apply: bool,
    keep_slug: bool,
    skip_fields: tuple[str, ...],
    pull_leftovers: bool,
    env_name: str | None,
    json_path: Path | None,
) -> None:
    if not (select_all or dataset or chart_ids or targets):
        raise click.UsageError("Name the charts: --all, --dataset, --chart-id, or config files / step URIs.")

    owid_env = _source_env(env_name)
    console.print(f"Reading admin layers from [bold]{owid_env.conf.DB_NAME}[/bold] on {owid_env.conf.DB_HOST}")

    steps = find_chart_steps(Path(STEP_DIR), Path(DAG_DIR))
    selected = _select(steps, select_all, dataset, targets)
    if chart_ids:
        selected += _select_by_chart_id(owid_env, steps, chart_ids)
    selected = list({s.uri: s for s in selected}.values())
    if not selected:
        raise click.ClickException("No ETL-authored single chart matches that selection.")

    layers = _fetch_layers(owid_env, [s.chart_config_id for s in selected])
    results = [
        _reconcile(step, layers.get(step.chart_config_id), owid_env, apply, keep_slug, skip_fields, pull_leftovers)
        for step in selected
    ]

    _print_report(results, apply)
    if json_path:
        json_path.write_text(json.dumps([r.to_dict() for r in results], indent=2) + "\n")
        console.print(f"Report written to {json_path}")


def _source_env(env_name: str | None) -> OWIDEnv:
    if env_name:
        return OWIDEnv.from_staging_or_env_file(env_name)
    # Production when credentials are configured, otherwise staging-site-master, whose database
    # mirrors production's. Either way no production credentials are needed.
    return production_or_master_env()


def _select(steps: list[ChartStep], select_all: bool, dataset: str | None, targets: tuple[str, ...]) -> list[ChartStep]:
    if select_all:
        # Only steps in the active DAG: a superseded config can still sit in the repo next to its
        # replacement, sharing its `chart_config_id`. A config file named explicitly is still taken.
        return [s for s in steps if s.dag_file is not None]
    selected = []
    if dataset:
        try:
            selected += [s for s in steps if s.dag_file is not None and depends_on_dataset(s, dataset)]
        except ValueError as e:
            raise click.UsageError(str(e)) from None
    by_uri = {s.uri: s for s in steps}
    by_path = {s.config_path.resolve(): s for s in steps}
    for target in targets:
        step = by_uri.get(target) or by_path.get(Path(target).resolve())
        if step is None:
            raise click.UsageError(
                f"{target} is not an ETL-authored single chart (a viz://chart step with `chart_config_id`)."
            )
        selected.append(step)
    return selected


def _select_by_chart_id(owid_env: OWIDEnv, steps: list[ChartStep], chart_ids: tuple[int, ...]) -> list[ChartStep]:
    query = text("SELECT id, configId FROM charts WHERE id IN :ids").bindparams(bindparam("ids", expanding=True))
    rows = _query(owid_env, query, {"ids": list(chart_ids)})
    found = {row.id: _step_for_config_id(steps, str(row.configId).lower()) for row in rows}
    unknown = [str(i) for i in chart_ids if found.get(i) is None]
    if unknown:
        raise click.UsageError(f"No ETL-authored chart config in the repo for chart id(s) {', '.join(unknown)}.")
    return [found[i] for i in chart_ids]  # ty: ignore[invalid-return-type]


def _step_for_config_id(steps: list[ChartStep], chart_config_id: str) -> ChartStep | None:
    """The step with this `chart_config_id`, preferring one in the active DAG over a superseded copy."""
    matches = [s for s in steps if s.chart_config_id == chart_config_id]
    return next((s for s in matches if s.dag_file is not None), matches[0] if matches else None)


def _fetch_layers(owid_env: OWIDEnv, config_ids: list[str]) -> dict[str, ChartLayers]:
    """Admin patch and rendered slug for each chart, plus the catalog paths of the variables it names."""
    # A plain SELECT, not the ORM `Chart`, as in `etl chart-config-id lookup`.
    # `last_human_edit` is the latest admin save by a person; ETL pushes are saved as the ETL user.
    query = text(
        "SELECT c.id, c.configId, cc.slug, pa.config AS patch, pe.createdAt AS etl_created, "
        "(SELECT MAX(r.createdAt) FROM chart_revisions r WHERE r.chartId = c.id AND r.userId <> :etl_user) "
        "AS last_human_edit "
        "FROM charts c JOIN chart_configs cc ON cc.id = c.configId "
        "LEFT JOIN chart_configs pa ON pa.id = c.patchConfigId "
        "LEFT JOIN chart_configs pe ON pe.id = c.patchConfigIdETL "
        "WHERE c.configId IN :ids"
    ).bindparams(bindparam("ids", expanding=True))
    layers = {}
    for row in _query(owid_env, query, {"ids": config_ids, "etl_user": ETL_GRAPHER_USER_ID}):
        patch = row.patch if isinstance(row.patch, dict) else json.loads(row.patch or "{}")
        edited_after_adoption = None
        if row.etl_created is not None:
            edited_after_adoption = row.last_human_edit is not None and row.last_human_edit > row.etl_created
        layers[str(row.configId).lower()] = ChartLayers(
            chart_id=row.id, patch=patch, slug=row.slug, edited_after_adoption=edited_after_adoption
        )

    variable_ids = sorted(set().union(*(variable_ids_in_patch(layer.patch) for layer in layers.values())))
    if variable_ids:
        query = text("SELECT id, catalogPath FROM variables WHERE id IN :ids").bindparams(
            bindparam("ids", expanding=True)
        )
        paths = {row.id: row.catalogPath for row in _query(owid_env, query, {"ids": variable_ids})}
        for layer in layers.values():
            layer.variable_paths = {i: paths.get(i) for i in variable_ids_in_patch(layer.patch)}
    return layers


def _query(owid_env: OWIDEnv, query, params: dict):
    try:
        with owid_env.engine.connect() as con:
            return con.execute(query, params).all()
    except OperationalError as e:
        raise click.ClickException(
            f"Could not reach the grapher database at {owid_env.conf.DB_HOST} (db '{owid_env.conf.DB_NAME}'): {e.orig}"
        ) from None


def _reconcile(
    step: ChartStep,
    layers: ChartLayers | None,
    owid_env: OWIDEnv,
    apply: bool,
    keep_slug: bool,
    skip_fields: tuple[str, ...],
    pull_leftovers: bool,
) -> Result:
    result = Result(step=step)
    if step.dag_file is None:
        result.warnings.append(f"{step.uri} isn't in the active DAG; reconcile it only if you are adopting it.")
    if layers is None:
        result.missing = True
        return result
    result.chart_id = layers.chart_id
    result.admin_url = f"{owid_env.admin_site}/charts/{layers.chart_id}/edit"
    result.admin_slug = layers.patch.get("slug") or layers.slug

    original = step.config_path.read_text()
    config_yaml = load_single_chart_config(step.config_path)
    reconcile_fields(step, config_yaml, layers, result, skip_fields, pull_leftovers)
    new_text = dump_config(config_yaml) if result.pulled else original

    new_short_name, problem = slug_target(step, layers)
    if problem:
        result.needs_manual.append(problem)
    elif new_short_name:
        result.new_short_name = new_short_name

    if new_text != original:
        if apply:
            step.config_path.write_text(new_text)
        else:
            _print_diff(step.config_path, original, new_text)
    if apply and result.new_short_name and not keep_slug:
        try:
            rename_step(step, result.new_short_name, Path(DAG_DIR))
            result.slug_fixed = True
        except FileExistsError as e:
            result.needs_manual.append(str(e))
            result.new_short_name = None
    return result


def _print_diff(path: Path, before: str, after: str) -> None:
    name = str(path.relative_to(BASE_DIR)) if path.is_relative_to(BASE_DIR) else str(path)
    diff = difflib.unified_diff(before.splitlines(), after.splitlines(), f"a/{name}", f"b/{name}", lineterm="")
    # Plain echo: rich would wrap long lines and mangle the diff.
    click.echo("\n".join(diff))


def _print_report(results: list[Result], apply: bool) -> None:
    table = Table(title="ETL-authored charts vs. admin")
    for column in ("Chart", "Step", "Status", "Details"):
        table.add_column(column)
    for r in results:
        details = []
        if r.pulled:
            details.append(f"{'pulled' if apply else 'to pull'}: {', '.join(r.pulled)}")
        if r.overrides_etl:
            details.append(f"admin value replaces an ETL value (check it isn't stale): {', '.join(r.overrides_etl)}")
        if r.skipped:
            details.append(f"skipped, still set in the admin: {', '.join(r.skipped)}")
        if r.leftovers:
            details.append(
                f"left out, set in the admin before the move to ETL and masking ETL's value; "
                f"clear in the admin: {', '.join(r.leftovers)}"
            )
        if r.stale_pins:
            details.append(
                f"admin pins an older version of an indicator ETL already plots, which no push clears; "
                f"clear in the admin: {', '.join(r.stale_pins)}"
            )
        if r.already_in_etl:
            details.append(f"already in ETL, clears on next push: {', '.join(r.already_in_etl)}")
        if r.new_short_name:
            verb = "renamed to" if r.slug_fixed else "rename to"
            details.append(f"admin slug `{r.admin_slug}`: {verb} {r.new_short_name}")
        details += [f"manual: {m}" for m in r.needs_manual]
        details += [f"warning: {w}" for w in r.warnings]
        if r.missing:
            details.append("no chart with this chart_config_id in the source environment")
        table.add_row(str(r.chart_id or "-"), r.step.uri, r.status, "\n".join(details))
    console.print(table)

    if not apply and any(r.pulled or r.new_short_name for r in results):
        console.print("Dry run: re-run with --apply to write these changes.")
    if any(r.slug_fixed for r in results):
        console.print(
            "Steps were renamed. Commit, then run `etl archive-dag` so the old step names are archived, and commit "
            "the regenerated `dag/archive/*.yml`."
        )
