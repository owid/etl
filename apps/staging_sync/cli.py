import datetime as dt
from pathlib import Path
from typing import Optional, Set

import click
import pandas as pd
import structlog
from dotenv import dotenv_values
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session

from etl import grapher_model as gm
from etl.config import GRAPHER_USER_ID
from etl.db import get_engine

from .admin_api import AdminAPI

log = structlog.get_logger()


@click.command()
@click.argument("source_env", type=Path)
@click.argument("target_env", type=Path)
@click.option(
    "--chart-id",
    type=int,
    help="Sync only this chart id.",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Do not write to target database.",
)
def cli(
    source_env: Path,
    target_env: Path,
    chart_id: Optional[int],
    dry_run: bool,
) -> None:
    """Syncs grapher charts modified by Admin user from source_env to target_env (Admin user is used by staging servers).
    Only sync charts that have been published.

    Charts:
    - New charts are automatically created in target_env.
    - Existing charts (with the same slug) are queued as chart revisions.
    - Deleted charts are **not synced**.

    Chart revisions:
    - Approved chart revisions on staging are automatically applied in target, assuming the chart has not been modified.
    """
    source_engine = get_engine(dotenv_values(str(source_env)))
    target_engine = get_engine(dotenv_values(str(target_env)))

    target_api = AdminAPI(target_engine)

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

                    if _charts_configs_are_equal(existing_chart.config, target_chart.config):
                        log.info(
                            "staging_sync.skip",
                            reason="identical chart already exists",
                            slug=target_chart.config["slug"],
                            chart_id=chart_id,
                        )
                        continue

                    # if the chart has gone through a revision, update it directly
                    revs = gm.SuggestedChartRevisions.load_revisions(source_session, chart_id=chart_id)

                    # revision must be approved and be created after chart latest edit
                    revs = [
                        rev for rev in revs if rev.status == "approved" and rev.createdAt > existing_chart.updatedAt
                    ]

                    # update existing chart directly
                    if revs:
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
                            createdAt=dt.datetime.now(),
                            updatedAt=dt.datetime.now(),
                        )
                        if not dry_run:
                            target_session.add(chart_revision)
                            target_session.commit()
                        log.info("staging_sync.create_chart_revision", slug=target_chart.config["slug"])
                except NoResultFound:
                    # create new chart
                    if not dry_run:
                        resp = target_api.create_chart(target_chart.config)
                    else:
                        resp = {"chartId": None}
                    log.info(
                        "staging_sync.create_chart", slug=target_chart.config["slug"], new_chart_id=resp["chartId"]
                    )


def _charts_configs_are_equal(config_1, config_2):
    """Compare two chart configs, ignoring their version."""
    exclude_keys = ("version", "id")
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
