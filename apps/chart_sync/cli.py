import copy
import datetime as dt
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
import pandas as pd
import pytz
import requests
import structlog
from rich import print
from rich_click.rich_command import RichCommand
from slack_sdk import WebClient
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from apps.chart_sync.admin_api import AdminAPI
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiff
from apps.wizard.utils.env import OWIDEnv
from etl import config
from etl import grapher_model as gm
from etl.config import get_container_name
from etl.datadiff import _dict_diff
from etl.db import read_sql

log = structlog.get_logger()


@click.command(name="chart-sync", cls=RichCommand, help=__doc__)
@click.argument("source")
@click.argument("target")
@click.option(
    "--chart-id",
    type=int,
    help="Sync **_only_** the chart with this id.",
)
@click.option(
    "--approve-revisions/--keep-revisions",
    default=False,
    type=bool,
    help="""Directly update existing charts with approved revisions (i.e. skip chart revision). Useful for large updates. This still
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
    "--include",
    default=None,
    type=str,
    help="""Include only charts with variables whose catalogPath matches the provided string.""",
)
@click.option(
    "--exclude",
    default=None,
    type=str,
    help="""Exclude charts with variables whose catalogPath matches the provided string.""",
)
@click.option(
    "--chartdiff/--no-chartdiff",
    default=False,
    type=bool,
    help="""Use approvals from chart-diff for syncing charts.""",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Do not write to target database.",
)
def cli(
    source: str,
    target: str,
    chart_id: Optional[int],
    approve_revisions: bool,
    staging_created_at: Optional[dt.datetime],
    include: Optional[str],
    exclude: Optional[str],
    chartdiff: bool,
    dry_run: bool,
) -> None:
    """Sync Grapher charts and revisions from an environment to the main environment.

    It syncs the charts and revisions from `SOURCE` to `TARGET`. This is especially useful for syncing work from staging servers to production.

    `SOURCE` and `TARGET` can be either name of staging servers (e.g. "staging-site-mybranch") or paths to .env files or repo/commit hash if you
    want to get the branch name from merged pull request. Use ".env.prod.write" as TARGET to sync to live.

    - **Note 1:** The dataset (with the new chart's underlying indicators) from `SOURCE` must exist in `TARGET`. This means that you have to merge your work to master and wait for the ETL to finish running all steps.

    - **Note 2:** Staging servers are destroyed after 7 days of merging to master, so this script should be run before that, but after the dataset has been built by ETL in production.

    **Considerations on charts:**

    - Both published charts and drafts from staging are synced.
    - Existing charts (with the same slug) are added as chart revisions in target. (Revisions could be pre-approved with `--approve-revisions` flag)
    - You get a warning if the chart **_has been modified on live_** after staging server was created.
    - If the chart is pending in chart-diff, you'll get a warning and Slack notification
    - Deleted charts are **_not synced_**.

    **Considerations on chart revisions:**

    - Approved chart revisions on staging are automatically applied in target, assuming the chart has not been modified.

    **Considerations on tags:**

    - Tags are synced only for **_new charts_**, any edits to tags in existing charts are ignored.

    **Example 1:** Run chart-sync in dry-run mode to see what charts will be updated

    ```
    $ etl chart-sync staging-site-my-branch .env.prod.write --dry-run
    ```

    **Example 2:** Run it for real

    ```
    etl chart-sync staging-site-my-branch .env.prod.write
    ```

    **Example 3:** Sync only one chart

    ```
    etl chart-sync staging-site-my-branch .env.prod.write --chart-id 123 --dry-run
    ```

    **Example 4:** Update charts directly without creating chart revisions (useful for large datasets updates like population)

    ```
    etl chart-sync staging-site-my-branch .env.prod.write --approve-revisions
    ```
    """
    if _is_commit_sha(source):
        source = _get_git_branch_from_commit_sha(source)
        log.info("chart_sync.use_branch", branch=source)

    source_engine = OWIDEnv.from_staging_or_env_file(source).get_engine()
    target_engine = OWIDEnv.from_staging_or_env_file(target).get_engine()

    staging_created_at = _get_staging_created_at(source, staging_created_at)  # type: ignore

    # go through Admin API as creating / updating chart has side effects like
    # adding entries to chart_dimensions. We can't directly update it in MySQL
    target_api: AdminAPI = AdminAPI(target_engine) if not dry_run else None  # type: ignore

    with Session(source_engine) as source_session:
        with Session(target_engine) as target_session:
            if chart_id:
                chart_ids = {chart_id}
            else:
                diffs = modified_charts_by_admin(source_session, target_session)
                chart_ids = set(diffs.index[diffs.configEdited])

            log.info("chart_sync.start", n=len(chart_ids), chart_ids=chart_ids)

            charts_synced = 0

            for chart_id in chart_ids:
                diff = ChartDiff.from_chart_id(chart_id, source_session, target_session)

                chart_slug = diff.source_chart.config["slug"]

                # Fix charts with non-existing map column slugs. We should ideally fix this
                # for all charts and make sure it doesn't happen.
                diff.source_chart.remove_nonexisting_map_column_slug(source_session)

                # Exclude charts with variables whose catalogPath matches the provided string
                if not _matches_include_exclude(diff.source_chart, source_session, include, exclude):
                    log.info(
                        "chart_sync.skip",
                        slug=chart_slug,
                        reason="filtered by --include/--exclude",
                        chart_id=chart_id,
                    )
                    continue

                # Rejected diffs are skipped
                if diff.is_rejected:
                    log.info(
                        "chart_sync.is_rejected",
                        slug=chart_slug,
                        chart_id=chart_id,
                    )
                    continue

                # Map variable IDs from source to target
                diff.source_chart = diff.source_chart.migrate_to_db(source_session, target_session)

                # Chart in target exists, update it
                if diff.target_chart:
                    # Configs are equal, no need to update
                    if diff.configs_are_equal():
                        log.info(
                            "chart_sync.skip",
                            slug=diff.target_chart.config["slug"],
                            reason="identical chart already exists",
                            chart_id=chart_id,
                        )
                        continue

                    ### New chart-diff workflow ###
                    if chartdiff:
                        # Change has been approved, update the chart
                        if diff.is_approved:
                            log.info("chart_sync.chart_update", slug=chart_slug, chart_id=chart_id)
                            charts_synced += 1
                            if not dry_run:
                                target_api.update_chart(chart_id, diff.source_chart.config)

                        # Rejected chart diff
                        elif diff.is_rejected:
                            raise ValueError("Rejected chart diff should have been skipped")

                        # Pending chart, notify us about it
                        elif diff.is_pending:
                            log.warning(
                                "chart_sync.pending_chart",
                                slug=chart_slug,
                                chart_id=chart_id,
                                source_updatedAt=str(diff.source_chart.updatedAt),
                                target_updatedAt=str(diff.target_chart.updatedAt),
                                staging_created_at=str(staging_created_at),
                            )
                            _notify_slack_chart_update(chart_id, str(source), diff, dry_run)
                        else:
                            raise ValueError("Invalid chart diff state")

                    ### Old workflow ###
                    else:
                        # warn if chart has been updated in production after the staging server got created
                        if diff.target_chart.updatedAt > min(staging_created_at, diff.source_chart.updatedAt):
                            log.warning(
                                "chart_sync.chart_modified_in_target",
                                slug=chart_slug,
                                target_updatedAt=str(diff.target_chart.updatedAt),
                                source_updatedAt=str(diff.source_chart.updatedAt),
                                staging_created_at=str(staging_created_at),
                                chart_id=chart_id,
                            )
                            print(
                                f"[bold red]WARNING[/bold red]: [bright_cyan]Chart [bold]{chart_slug}[/bold] has been modified in target[/bright_cyan]"
                            )
                            print("[yellow]\tDifferences between SOURCE (-) and TARGET (+) chart[/yellow]")
                            print(_chart_config_diff(diff.source_chart.config, diff.target_chart.config))

                        # if the chart has gone through a revision, update it directly
                        revs = _load_revisions(source_session, chart_id, diff)

                        # if chart has gone through revision in source and --approve-revisions is set and
                        # chart hasn't been updated in production, update it directly
                        if approve_revisions and revs and staging_created_at > diff.target_chart.updatedAt:
                            log.info(
                                "chart_sync.update_chart",
                                slug=chart_slug,
                                chart_id=chart_id,
                            )
                            charts_synced += 1
                            if not dry_run:
                                target_api.update_chart(chart_id, diff.source_chart.config)
                        else:
                            assert config.GRAPHER_USER_ID
                            GRAPHER_USER_ID = int(config.GRAPHER_USER_ID)

                            # there's already a chart with the same slug, create a new revision
                            chart_revision = gm.SuggestedChartRevisions(
                                chartId=chart_id,
                                createdBy=GRAPHER_USER_ID,
                                updatedBy=GRAPHER_USER_ID,
                                originalConfig=diff.target_chart.config,
                                suggestedConfig=diff.source_chart.config,
                                status="pending",
                                createdAt=dt.datetime.utcnow(),
                                updatedAt=dt.datetime.utcnow(),
                            )
                            charts_synced += 1
                            if not dry_run:
                                # delete previously submitted revisions
                                (
                                    target_session.query(gm.SuggestedChartRevisions)
                                    .filter_by(
                                        chartId=chart_id,
                                        status="pending",
                                        createdBy=GRAPHER_USER_ID,
                                    )
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
                                        "chart_sync.skip",
                                        reason="revision already exists",
                                        slug=chart_slug,
                                        chart_id=chart_id,
                                    )
                                    continue

                            log.info("chart_sync.create_chart_revision", slug=chart_slug, chart_id=chart_id)

                # Chart is new, create it
                else:
                    chart_tags = diff.source_chart.tags(source_session)

                    if chartdiff:
                        # New chart has been approved
                        if diff.is_approved:
                            charts_synced += 1
                            if not dry_run:
                                resp = target_api.create_chart(diff.source_chart.config)
                                target_api.set_tags(resp["chartId"], chart_tags)
                            else:
                                resp = {"chartId": None}
                            log.info(
                                "chart_sync.create_chart",
                                published=diff.source_chart.config.get("isPublished"),
                                slug=chart_slug,
                                new_chart_id=resp["chartId"],
                            )
                        # Rejected chart diff
                        elif diff.is_rejected:
                            raise ValueError("Rejected chart diff should have been skipped")

                        # Not approved, create the chart, but notify us about it
                        elif diff.is_pending:
                            log.warning(
                                "chart_sync.new_unapproved_chart",
                                slug=chart_slug,
                                chart_id=chart_id,
                            )
                            _notify_slack_chart_create(chart_id, str(source), dry_run)

                        else:
                            raise ValueError("Invalid chart diff state")
                    else:
                        charts_synced += 1
                        if not dry_run:
                            resp = target_api.create_chart(diff.source_chart.config)
                            target_api.set_tags(resp["chartId"], chart_tags)
                        else:
                            resp = {"chartId": None}

                        log.info(
                            "chart_sync.create_chart",
                            published=diff.source_chart.config.get("isPublished"),
                            slug=chart_slug,
                            new_chart_id=resp["chartId"],
                        )

    if charts_synced > 0:
        print(f"\n[bold green]Charts synced: {charts_synced}[/bold green]")
    else:
        print("\n[bold green]No charts synced[/bold green]")

    if not chartdiff:
        print("\n[bold yellow]Follow-up instructions:[/bold yellow]")
        print("[green]1.[/green] Chart updates were added as chart revisions, you still have to manually approve them")


def _is_commit_sha(source: str) -> bool:
    return re.match(r"[0-9a-f]{40}", source) is not None


def _get_git_branch_from_commit_sha(commit_sha: str) -> str:
    """Get the branch name from a merged pull request commit sha. This is useful for Buildkite jobs where we only have the commit sha."""
    # get all pull requests for the commit
    pull_requests = requests.get(f"https://api.github.com/repos/owid/etl/commits/{commit_sha}/pulls").json()

    # filter the closed ones
    closed_pull_requests = [pr for pr in pull_requests if pr["state"] == "closed"]

    # get the branch of the most recent one
    if closed_pull_requests:
        return closed_pull_requests[0]["head"]["ref"]
    else:
        raise ValueError(f"No closed pull requests found for commit {commit_sha}")


def _load_revisions(source_session: Session, chart_id: int, diff: ChartDiff) -> List[gm.SuggestedChartRevisions]:
    assert diff.target_chart

    revs = gm.SuggestedChartRevisions.load_revisions(source_session, chart_id=chart_id)

    # revision must be approved and be created after chart latest edit
    revs = [
        rev
        for rev in revs
        if rev.status == "approved"
        and rev.updatedBy == 1
        # min(rev.createdAt, rev.updatedAt) is needed because of a bug in chart revisions, it should be fixed soon
        and min(rev.createdAt, rev.updatedAt) > diff.target_chart.updatedAt
    ]

    return revs


def _notify_slack_chart_update(chart_id: int, source: str, diff: ChartDiff, dry_run: bool) -> None:
    assert diff.target_chart

    message = f"""
:warning: *ETL chart-sync: Pending Chart Update Not Synced* from `{source}`
<http://{get_container_name(source)}/admin/charts/{chart_id}/edit|View Staging Chart> | <https://admin.owid.io/admin/charts/{chart_id}/edit|View Admin Chart>
*Staging        Edited*: {str(diff.source_chart.updatedAt)} UTC
*Production Edited*: {str(diff.target_chart.updatedAt)} UTC
```
{_chart_config_diff(diff.target_chart.config, diff.source_chart.config, tabs=0, color=False)}
```
    """.strip()

    print(message)

    if config.SLACK_API_TOKEN and not dry_run:
        assert diff.target_chart
        slack_client = WebClient(token=config.SLACK_API_TOKEN)
        slack_client.chat_postMessage(channel="#data-architecture-github", text=message)


def _notify_slack_chart_create(source_chart_id: int, source: str, dry_run: bool) -> None:
    message = f"""
:warning: *ETL chart-sync: Pending New Chart Not Synced* from `{source}`
<http://{get_container_name(source)}/admin/charts/{source_chart_id}/edit|View Staging Chart>
    """.strip()

    print(message)

    if config.SLACK_API_TOKEN and not dry_run:
        slack_client = WebClient(token=config.SLACK_API_TOKEN)
        slack_client.chat_postMessage(channel="#data-architecture-github", text=message)


def _matches_include_exclude(chart: gm.Chart, session: Session, include: Optional[str], exclude: Optional[str]):
    source_variables = chart.load_chart_variables(session)

    # if chart contains a variable that is excluded, skip the whole chart
    if exclude:
        for source_var in source_variables.values():
            if source_var.catalogPath and re.search(exclude, source_var.catalogPath):
                return False

    # a chart must contain at least one variable matching include, otherwise skip it
    if include:
        matching = False
        for source_var in source_variables.values():
            if source_var.catalogPath and re.search(include, source_var.catalogPath):
                matching = True
        if not matching:
            return False

    return True


def _get_staging_created_at(source: str, staging_created_at: Optional[str]) -> dt.datetime:
    if staging_created_at is None:
        if not Path(source).exists():
            return _get_git_branch_creation_date(str(source).replace("staging-site-", ""))
        else:
            log.warning(
                "--staging-created-at is not provided while using the local environment, it's assumed that you began working less than one week ago."
            )
            return dt.datetime.now() - dt.timedelta(weeks=1)
            # raise click.BadParameter("staging-created-at is required when source is not a staging server name")
    else:
        return pd.to_datetime(staging_created_at)


def _prune_chart_config(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config)
    config = {k: v for k, v in config.items() if k not in ("version",)}
    for dim in config["dimensions"]:
        dim.pop("variableId", None)
    return config


def _chart_config_diff(
    source_config: Dict[str, Any], target_config: Dict[str, Any], tabs: int = 1, color: bool = True
) -> str:
    return _dict_diff(
        _prune_chart_config(source_config), _prune_chart_config(target_config), tabs=tabs, color=color, width=500
    )


def modified_charts_by_admin(source_session: Session, target_session: Session) -> pd.DataFrame:
    """Get charts that have been modified in staging. It includes chart with different
    config, data or metadata checksums. It assumes that all changes on staging server are
    done by Admin user with ID = 1."""
    # TODO: we could aggregate it by charts and get a combined checksum, for now we want
    #   a view with variable granularity
    # get modified charts and charts from modified datasets
    base_q = """
    select
        v.id as variableId,
        cd.chartId,
        v.dataChecksum,
        v.metadataChecksum,
        MD5(c.config) as chartChecksum,
        c.lastEditedByUserId as chartLastEditedByUserId,
        c.publishedByUserId as chartPublishedByUserId,
        d.dataEditedByUserId,
        d.metadataEditedByUserId
    from chart_dimensions as cd
    join charts as c on cd.chartId = c.id
    join variables as v on cd.variableId = v.id
    join datasets as d on v.datasetId = d.id
    where
    """
    # NOTE: We assume that all changes on staging server are done by Admin user with ID = 1. This is
    #   set automatically if you use STAGING env variable.
    where = """
        -- only compare datasets or charts that have been updated on staging server
        -- by Admin user
        (
            (c.lastEditedByUserId = 1 or c.publishedByUserId = 1)
            or
            -- include all charts from datasets that have been updated
            (d.dataEditedByUserId = 1 or d.metadataEditedByUserId = 1)
        )
    """
    source_df = read_sql(base_q + where, source_session)

    # no charts, return empty dataframe
    if source_df.empty:
        return pd.DataFrame(columns=["chartId", "dataEdited", "metadataEdited", "configEdited"]).set_index("chartId")

    # read those charts from target
    where = """
        c.id in %(chart_ids)s
    """
    target_df = read_sql(base_q + where, target_session, params={"chart_ids": tuple(source_df.chartId.unique())})

    source_df = source_df.set_index(["chartId", "variableId"])
    target_df = target_df.set_index(["chartId", "variableId"])

    # align dataframes with left join (so that source has non-null values)
    # NOTE: new charts will be already in source
    source_df, target_df = source_df.align(target_df, join="left")

    # return differences in data / metadata / config
    diff = (
        (source_df != target_df)
        .groupby("chartId")
        .max()
        .rename(
            columns={
                "dataChecksum": "dataEdited",
                "metadataChecksum": "metadataEdited",
                "chartChecksum": "configEdited",
            }
        )
    )
    diff = diff[["dataEdited", "metadataEdited", "configEdited"]]

    # If chart hasn't been edited by Admin, then make `configEdited` false
    # This can happen when you merge master to your branch and staging rebuilds a dataset.
    # Then dataset will be edited by Admin and will be included, but your charts might be outdated
    # compared to production. Hence, only consider config updates for charts edited by Admin.
    chart_ids = source_df[
        (source_df.chartLastEditedByUserId != 1) & (source_df.chartPublishedByUserId != 1)
    ].index.get_level_values("chartId")
    diff.loc[chart_ids, "configEdited"] = False

    # Remove charts with no changes
    diff = diff[diff.any(axis=1)]

    return diff


def _get_git_branch_creation_date(branch_name: str) -> dt.datetime:
    js = requests.get(f"https://api.github.com/repos/owid/etl/pulls?state=all&head=owid:{branch_name}").json()
    assert len(js) > 0, f"Branch {branch_name} not found in owid/etl repository"

    # There could be multiple old branches from the past, pick the most recent one
    created_ats = [
        dt.datetime.fromisoformat(pr["created_at"].rstrip("Z")).astimezone(pytz.utc).replace(tzinfo=None) for pr in js
    ]
    return max(created_ats)


if __name__ == "__main__":
    cli()
