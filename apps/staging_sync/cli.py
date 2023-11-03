import datetime as dt
from pathlib import Path
from typing import Optional, Set

import click
import pandas as pd
import structlog
from dotenv import dotenv_values
from rich import print
from rich_click.rich_command import RichCommand
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlmodel import Session

from etl import grapher_model as gm
from etl.config import GRAPHER_USER_ID
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
    "--apply-revisions/--no-apply-revisions",
    default=False,
    type=bool,
    help="""Directly update existing charts with approved revisions
    (skip chart revision). Useful for large updates. This still
    creates a chart revision if the target chart has been modified.""",
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
    apply_revisions: bool,
    dry_run: bool,
) -> None:
    """Syncs grapher charts and revisions modified by Admin user from source_env to target_env. (Admin user is used by staging servers).

    SOURCE and TARGET can be either paths to .env file or name of a staging server.

    The dataset from source must exist in target (i.e. you have to merge your work to master and wait for the ETL to finish).

    Usage:
        # run staging-sync in dry-run mode to see what charts will be updated
        etl-staging-sync staging-site-my-branch .env.prod.write --dry-run

        # run it for real
        etl-staging-sync staging-site-my-branch .env.prod.write

        # sync only one chart
        etl-staging-sync staging-site-my-branch .env.prod.write --chart-id 123 --dry-run

        # WARNING: skip chart revisions and update charts directly
        etl-staging-sync staging-site-my-branch .env.prod.write --apply-revisions

    Charts:
        - New charts are automatically created in target_env.
        - Existing charts (with the same slug) are queued as chart revisions.
        - Deleted charts are **not synced**.

        Only syncs charts that are **published** on staging server. They are **created as drafts** in target and must be published
        manually, unless the --publish flag is used.

    Chart revisions:
        - Approved chart revisions on staging are automatically applied in target, assuming the chart has not been modified.

    Tags:
        - Tags are synced only for **new charts**.
    """
    source_engine = _get_engine_for_env(source)
    target_engine = _get_engine_for_env(target)

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
                target_chart = source_chart.migrate_to_db(source_session, target_session)

                try:
                    existing_chart = gm.Chart.load_chart(target_session, slug=source_chart.config["slug"])
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

                    # if chart has been updated in production after our change, warn about it
                    if existing_chart.updatedAt > source_chart.updatedAt:
                        log.warning(
                            "staging_sync.chart_modified_in_target",
                            slug=target_chart.config["slug"],
                            target_updatedAt=str(existing_chart.updatedAt),
                            source_updatedAt=str(source_chart.updatedAt),
                            chart_id=chart_id,
                        )

                    # if the chart has gone through a revision, update it directly
                    revs = gm.SuggestedChartRevisions.load_revisions(source_session, chart_id=chart_id)

                    # revision must be approved and be created after chart latest edit
                    revs = [
                        rev
                        for rev in revs
                        if rev.status == "approved" and rev.updatedBy == 1
                        # min(rev.createdAt, rev.updatedAt) is needed because of a bug in chart revisions, it should be fixed soon
                        and min(rev.createdAt, rev.updatedAt) > existing_chart.updatedAt
                    ]

                    # if chart has gone through revision in source and --apply-revisions is set, update it directly
                    if apply_revisions and revs:
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
    print(f"[green]1.[/green] New charts were created as drafts, don't forget to publish them")
    print(f"[green]2.[/green] Chart updates were added as chart revisions, you still have to manually approve them")


def _get_engine_for_env(env: Path) -> Engine:
    # env exists as a path
    if env.exists():
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
            "DB_HOST": staging_name,
        }

    return get_engine(config)


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


if __name__ == "__main__":
    cli()
