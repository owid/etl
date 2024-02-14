import copy
import datetime as dt
import re
from pathlib import Path
from typing import Any, Dict, Optional, Set

import click
import pandas as pd
import pytz
import requests
import structlog
from dotenv import dotenv_values
from rich import print
from rich_click.rich_command import RichCommand
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlmodel import Session

from etl import grapher_model as gm
from etl.config import GRAPHER_USER_ID
from etl.datadiff import _dict_diff
from etl.db import Engine, get_engine

from .admin_api import AdminAPI

log = structlog.get_logger()


@click.command(cls=RichCommand, help=__doc__)
@click.argument("source", type=Path)
@click.argument("target", type=Path)
@click.option(
    "--chart-id",
    type=int,
    help="Sync only this chart id.",
)
@click.option(
    "--publish/--no-publish",
    default=False,
    type=bool,
    help="Automatically publish new charts.",
)
@click.option(
    "--approve-revisions/--keep-revisions",
    default=False,
    type=bool,
    help="""Directly update existing charts with approved revisions
    (skip chart revision). Useful for large updates. This still
    creates a chart revision if the target chart has been modified.""",
)
@click.option(
    "--staging-created-at",
    default=None,
    type=str,
    help="""Staging server UTC creation date. It is used to warn about charts that have been
    updated in production. Default is branch creation date.""",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Do not write to target database.",
)
def cli(
    source: Path,
    target: Path,
    chart_id: Optional[int],
    publish: bool,
    approve_revisions: bool,
    staging_created_at: Optional[dt.datetime],
    dry_run: bool,
) -> None:
    """Syncs grapher charts and revisions from SOURCE (e.g. staging-site-mybranch or just
    mybranch) to TARGET (e.g. .env.prod.write).

    SOURCE and TARGET can be either name of servers like staging-site-mybranch or paths
    to .env files. You have to use `.env.prod.write` as TARGET to sync to live.

    This is especially useful for syncing work from staging servers to production.

    Staging servers are destroyed after 1 day of merging to master, so this script should be
    run before that, but after the dataset has been built by ETL in production.

    SOURCE and TARGET can be either paths to .env file or name of a staging server.

    The dataset from source must exist in target (i.e. you have to merge your work to master and wait for the ETL to finish).

    Usage:
        # Run staging-sync in dry-run mode to see what charts will be updated
        etlcli chart-sync staging-site-my-branch .env.prod.write --dry-run

        # Run it for real
        etlcli chart-sync staging-site-my-branch .env.prod.write

        # Sync only one chart
        etlcli chart-sync staging-site-my-branch .env.prod.write --chart-id 123 --dry-run

        # Update charts directly without creating chart revisions (useful for large datasets
        # updates like population)
        etlcli chart-sync staging-site-my-branch .env.prod.write --approve-revisions

    Charts:
        - Only **published charts** from staging are synced.
        - New charts are synced as **drafts** in target (unless `--publish` flag is used).
        - Existing charts (with the same slug) are added as chart revisions in target. (Revisions could be pre-approved with --approve-revisions flag)
        - You get a warning if the chart **has been modified on live** after staging server was created.
        - Deleted charts are **not synced**.

    Chart revisions:
        - Approved chart revisions on staging are automatically applied in target, assuming the chart has not been modified.

    Tags:
        - Tags are synced only for **new charts**, any edits to tags in existing charts are ignored.
    """
    source_engine = _get_engine_for_env(source)
    target_engine = _get_engine_for_env(target)

    staging_created_at = _get_staging_created_at(source, staging_created_at)  # type: ignore

    # go through Admin API as creating / updating chart has side effects like
    # adding entries to chart_dimensions. We can't directly update it in MySQL
    target_api: AdminAPI = AdminAPI(target_engine) if not dry_run else None  # type: ignore

    with Session(source_engine) as source_session:
        with Session(target_engine) as target_session:
            if chart_id:
                chart_ids = {chart_id}
            else:
                chart_ids = _modified_chart_ids_by_admin(source_session)

            log.info("staging_sync.start", n=len(chart_ids), chart_ids=chart_ids)

            for chart_id in chart_ids:
                source_chart = gm.Chart.load_chart(source_session, chart_id)

                _remove_nonexisting_column_slug(source_chart, source_session)

                target_chart = source_chart.migrate_to_db(source_session, target_session)

                # try getting chart with the same slug
                try:
                    existing_chart = gm.Chart.load_chart(target_session, slug=source_chart.config["slug"])
                except NoResultFound:
                    existing_chart = None

                # it's possible that slug is different, but chart id is the same
                if existing_chart is None:
                    try:
                        existing_chart = gm.Chart.load_chart(target_session, chart_id=chart_id)

                        # make sure createdAt matches and double check with createdAt
                        if existing_chart.createdAt != source_chart.createdAt:
                            log.warning("staging_sync.different_chart_with_same_id", chart_id=chart_id)
                            existing_chart = None
                    except NoResultFound:
                        existing_chart = None

                if existing_chart:
                    if _charts_configs_are_equal(existing_chart.config, target_chart.config):
                        log.info(
                            "staging_sync.skip",
                            slug=target_chart.config["slug"],
                            reason="identical chart already exists",
                            chart_id=chart_id,
                        )
                        continue

                    # warn if chart has been updated in production after the staging server got created
                    if existing_chart.updatedAt > min(staging_created_at, source_chart.updatedAt):
                        log.warning(
                            "staging_sync.chart_modified_in_target",
                            slug=target_chart.config["slug"],
                            target_updatedAt=str(existing_chart.updatedAt),
                            source_updatedAt=str(source_chart.updatedAt),
                            staging_created_at=str(staging_created_at),
                            chart_id=chart_id,
                        )
                        print(
                            f"[bold red]WARNING[/bold red]: [bright_cyan]Chart [bold]{target_chart.config['slug']}[/bold] has been modified in target[/bright_cyan]"
                        )
                        print("[yellow]\tDifferences between SOURCE (-) and TARGET (+) chart[/yellow]")
                        print(_chart_config_diff(target_chart.config, existing_chart.config))

                    # if the chart has gone through a revision, update it directly
                    revs = gm.SuggestedChartRevisions.load_revisions(source_session, chart_id=chart_id)

                    # revision must be approved and be created after chart latest edit
                    revs = [
                        rev
                        for rev in revs
                        if rev.status == "approved"
                        and rev.updatedBy == 1
                        # min(rev.createdAt, rev.updatedAt) is needed because of a bug in chart revisions, it should be fixed soon
                        and min(rev.createdAt, rev.updatedAt) > existing_chart.updatedAt
                    ]

                    # if chart has gone through revision in source and --approve-revisions is set and
                    # chart hasn't been updated in production, update it directly
                    if approve_revisions and revs and staging_created_at > existing_chart.updatedAt:
                        log.info(
                            "staging_sync.update_chart", slug=target_chart.config["slug"], chart_id=existing_chart.id
                        )
                        if not dry_run:
                            target_chart.config["id"] = existing_chart.id
                            assert existing_chart.id
                            target_api.update_chart(existing_chart.id, target_chart.config)

                    # create chart revision
                    else:
                        # there's already a chart with the same slug, create a new revision
                        chart_revision = gm.SuggestedChartRevisions(
                            chartId=existing_chart.id,
                            createdBy=int(GRAPHER_USER_ID),  # type: ignore
                            originalConfig=existing_chart.config,
                            suggestedConfig=target_chart.config,
                            status="pending",
                            createdAt=dt.datetime.utcnow(),
                            updatedAt=dt.datetime.utcnow(),
                        )
                        if not dry_run:
                            # delete previously submitted revisions
                            (
                                target_session.query(gm.SuggestedChartRevisions)
                                .filter_by(chartId=existing_chart.id, status="pending", createdBy=int(GRAPHER_USER_ID))  # type: ignore
                                .filter(gm.SuggestedChartRevisions.createdAt > staging_created_at)
                                .delete()
                            )

                            try:
                                target_session.add(chart_revision)
                                target_session.commit()
                            except IntegrityError:
                                # chart revision already exists
                                target_session.rollback()
                                log.info(
                                    "staging_sync.skip",
                                    reason="revision already exists",
                                    slug=target_chart.config["slug"],
                                    chart_id=chart_id,
                                )
                                continue

                        log.info(
                            "staging_sync.create_chart_revision", slug=target_chart.config["slug"], chart_id=chart_id
                        )
                else:
                    # create new chart
                    if not publish:
                        # only published charts are synced
                        assert target_chart.config["isPublished"]
                        del target_chart.config["isPublished"]

                    chart_tags = source_chart.tags(source_session)

                    if not dry_run:
                        resp = target_api.create_chart(target_chart.config)
                        target_api.set_tags(resp["chartId"], chart_tags)
                    else:
                        resp = {"chartId": None}
                    log.info(
                        "staging_sync.create_chart", slug=target_chart.config["slug"], new_chart_id=resp["chartId"]
                    )

    print("\n[bold yellow]Follow-up instructions:[/bold yellow]")
    print("[green]1.[/green] New charts were created as drafts, don't forget to publish them")
    print("[green]2.[/green] Chart updates were added as chart revisions, you still have to manually approve them")


def _get_staging_created_at(source: Path, staging_created_at: Optional[str]) -> dt.datetime:
    if staging_created_at is None:
        if not _is_env(source):
            return _get_git_branch_creation_date(str(source).replace("staging-site-", ""))
        else:
            raise click.BadParameter("staging-created-at is required when source is not a staging server name")
    else:
        return pd.to_datetime(staging_created_at)


def _is_env(env: Path) -> bool:
    return env.exists()


def _normalise_branch(branch_name):
    return re.sub(r"[\/\._]", "-", branch_name)


def _get_container_name(branch_name):
    normalized_branch = _normalise_branch(branch_name)

    # Strip staging-site- prefix to add it back later
    normalized_branch = normalized_branch.replace("staging-site-", "")

    # Ensure the container name is less than 63 characters
    container_name = f"staging-site-{normalized_branch[:50]}"
    # Remove trailing hyphens
    return container_name.rstrip("-")


def _get_engine_for_env(env: Path) -> Engine:
    # env exists as a path
    if _is_env(env):
        config = dotenv_values(str(env))
    # env could be server name
    else:
        staging_name = str(env)

        # add staging-site- prefix
        if not staging_name.startswith("staging-site-"):
            staging_name = "staging-site-" + staging_name

        # generate config for staging server
        config = {
            "DB_USER": "owid",
            "DB_NAME": "owid",
            "DB_PASS": "",
            "DB_PORT": "3306",
            "DB_HOST": _get_container_name(staging_name),
        }

    return get_engine(config)


def _remove_nonexisting_column_slug(source_chart: gm.Chart, source_session: Session) -> None:
    # remove map.columnSlug if the variable doesn't exist
    column_slug = source_chart.config.get("map", {}).get("columnSlug", None)
    if column_slug:
        try:
            gm.Variable.load_variable(source_session, int(column_slug))
        except NoResultFound:
            # When there are multiple indicators in a chart and it also has a map then this field tells the map which indicator to use.
            # If the chart doesn't have the map tab active then it can be invalid quite often
            log.warning(
                "staging_sync.remove_missing_map_column_slug",
                chart_id=source_chart.id,
                column_slug=column_slug,
            )
            source_chart.config["map"].pop("columnSlug")


def _prune_chart_config(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config)
    config = {k: v for k, v in config.items() if k not in ("version",)}
    for dim in config["dimensions"]:
        dim.pop("variableId", None)
    return config


def _chart_config_diff(source_config: Dict[str, Any], target_config: Dict[str, Any]) -> str:
    return _dict_diff(_prune_chart_config(source_config), _prune_chart_config(target_config), tabs=1, width=500)


def _charts_configs_are_equal(config_1, config_2):
    """Compare two chart configs, ignoring their version."""
    exclude_keys = ("version", "id", "isPublished")
    config_1 = {k: v for k, v in config_1.items() if k not in exclude_keys}
    config_2 = {k: v for k, v in config_2.items() if k not in exclude_keys}
    return config_1 == config_2


def _modified_chart_ids_by_admin(session: Session) -> Set[int]:
    """Get charts published by Admin user with ID = 1 on a staging server. These charts have been
    modified on the staging server and are candidates for syncing back to production."""
    q = """
    -- modified charts
    select
        id as chartId
    from charts
    where publishedAt is not null and (publishedByUserId = 1 or lastEditedByUserId = 1)

    union

    -- charts revisions that were approved on staging, such charts would have publishedByUserId
    -- of the user that ran etl-wizard locally, but would be updated by Admin
    select
        chartId
    from suggested_chart_revisions
    where updatedBy = 1 and status = 'approved'
    """
    return set(pd.read_sql(q, session.bind).chartId.tolist())


def _get_git_branch_creation_date(branch_name: str) -> dt.datetime:
    js = requests.get(f"https://api.github.com/repos/owid/etl/pulls?state=all&head=owid:{branch_name}").json()
    assert len(js) == 1

    return dt.datetime.fromisoformat(js[0]["created_at"].rstrip("Z")).astimezone(pytz.utc).replace(tzinfo=None)


if __name__ == "__main__":
    cli()
