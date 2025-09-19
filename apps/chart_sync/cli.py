import copy
import datetime as dt
import re
from typing import Any, Dict, Optional

import click
import pandas as pd
import structlog
from rich import print
from rich_click.rich_command import RichCommand
from sqlalchemy.orm import Session

from apps.chart_sync.admin_api import AdminAPI
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiff, ChartDiffsLoader, configs_are_equal, tags_are_equal
from apps.wizard.utils import get_staging_creation_time
from etl import config
from etl.config import OWIDEnv, get_container_name
from etl.datadiff import _dict_diff
from etl.git_api_helpers import GithubApiRepo
from etl.grapher import model as gm
from etl.slack_helpers import send_slack_message

config.enable_sentry()

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
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Do not write to target database.",
)
@click.option(
    "--ignore-conflicts/--no-ignore-conflicts",
    default=False,
    type=bool,
    help="Sync approved charts even when conflicts are detected. Useful when syncing between staging servers.",
)
def cli(
    source: str,
    target: str,
    chart_id: Optional[int],
    include: Optional[str],
    exclude: Optional[str],
    dry_run: bool,
    ignore_conflicts: bool,
) -> None:
    # TODO: keep this docstring in sync with apps/wizard/app_pages/chart_diff/app.py
    """Sync Grapher charts and revisions from an environment to the main environment.

    It syncs the charts and revisions from `SOURCE` to `TARGET`. This is especially useful for syncing work from staging servers to production.

    `SOURCE` and `TARGET` can be either name of staging servers (e.g. "staging-site-mybranch") or paths to .env files or repo/commit hash if you
    want to get the branch name from merged pull request. Use ".env.prod.write" as TARGET to sync to live.

    - **Note 1:** The dataset (with the new chart's underlying indicators) from `SOURCE` must exist in `TARGET`. This means that you have to merge your work to master and wait for the ETL to finish running all steps.

    **Considerations on charts:**

    - You get a notification if the chart **_has been modified on live_** after staging server was created.
    - If the chart is pending in chart-diff, you'll get a warning and Slack notification
    - Deleted charts are **_not synced_**.
    - Use `--ignore-conflicts` to sync approved charts ignoring conflicts. Useful when syncing between staging servers.

    **Considerations on tags:**

    - Tags are synced for both **_new and existing charts_**.

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

    **Example 4:** Ignore conflicts when syncing between staging servers (useful if conflicts with master have already been dealt with in a subbranch server)

    ```
    etl chart-sync staging-site-my-subbranch staging-site-baseline-branch --ignore-conflicts --dry-run
    ```
    """
    if _is_commit_sha(source):
        repo = GithubApiRepo(repo_name="etl")
        source = repo.get_git_branch_from_commit_sha(source)
        log.info("chart_sync.use_branch", branch=source)

    source_engine = OWIDEnv.from_staging_or_env_file(source).get_engine()
    target_env = OWIDEnv.from_staging_or_env_file(target)
    target_engine = target_env.get_engine()

    # Safety warning when using ignore-conflicts flag
    if ignore_conflicts:
        log.warning(
            "chart_sync.ignore_conflicts_enabled",
            message="Ignore-conflicts flag is enabled. Approved charts will be synced even if conflicts are detected.",
        )

    # go through Admin API as creating / updating chart has side effects like
    # adding entries to chart_dimensions. We can't directly update it in MySQL
    target_api: AdminAPI = AdminAPI(target_env) if not dry_run else None  # type: ignore

    with Session(source_engine) as source_session:
        with Session(target_engine) as target_session:
            # Get all chart diffs between source and target
            # NOTE: We're creating two paris of sessions here, it'd be nicer to only create a single one
            cd_loader = ChartDiffsLoader(source_engine, target_engine, chart_ids=[chart_id] if chart_id else None)
            chart_diffs = cd_loader.get_diffs(
                config=True,
                tags=True,
                metadata=False,
                data=False,
                source_session=source_session,
                target_session=target_session,
                ignore_conflicts=ignore_conflicts,
            )

            if chart_id:
                chart_diffs = [cd for cd in chart_diffs if cd.source_chart.id == chart_id]

            # Get staging server creation time
            SERVER_CREATION_TIME = get_staging_creation_time(source_session)

            chart_ids = [cd.source_chart.id for cd in chart_diffs]
            log.info("chart_sync.start", n=len(chart_diffs), chart_ids=chart_ids)

            charts_synced = 0
            dods_synced = 0

            # Iterate over all chart diffs
            for diff in chart_diffs:
                chart_slug = diff.slug
                chart_id = diff.source_chart.id

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
                migrated_config = diff.source_chart.migrate_config(source_session, target_session)

                # Get user who edited the chart
                user_id = diff.source_chart.lastEditedByUserId

                # Get source chart tags (needed for both new and existing charts)
                source_tags = diff.source_chart.tags(source_session)

                # Chart in target exists, update it
                if diff.target_chart:
                    # Check if configs and tags are equal
                    target_tags = diff.target_chart.tags(target_session)
                    configs_equal = configs_are_equal(migrated_config, diff.target_chart.config)
                    tags_equal = tags_are_equal(source_tags, target_tags)

                    # Skip if both configs and tags are equal
                    if configs_equal and tags_equal:
                        log.info(
                            "chart_sync.skip",
                            slug=diff.target_chart.slug,
                            reason="identical chart already exists",
                            chart_id=chart_id,
                        )
                        continue

                    # Change has been approved, update the chart
                    if diff.is_approved:
                        log.info("chart_sync.chart_update", slug=chart_slug, chart_id=chart_id)
                        charts_synced += 1
                        if not dry_run:
                            target_api.update_chart(chart_id, migrated_config, user_id=user_id)
                            target_api.set_tags(chart_id, source_tags, user_id=user_id)

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
                            staging_created_at=SERVER_CREATION_TIME,
                        )
                        _notify_slack_chart_update(chart_id, str(source), diff, dry_run)
                    else:
                        raise ValueError("Invalid chart diff state")

                # Chart is new, create it
                else:
                    # New chart has been approved
                    if diff.is_approved:
                        charts_synced += 1
                        if not dry_run:
                            resp = target_api.create_chart(migrated_config, user_id=user_id)
                            target_api.set_tags(resp["chartId"], source_tags, user_id=user_id)
                        else:
                            resp = {"chartId": None}
                        log.info(
                            "chart_sync.create_chart",
                            published=migrated_config.get("isPublished"),
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

            # Sync DoDs
            dods_synced = _sync_dods(source_session, target_session, target_api, dry_run, SERVER_CREATION_TIME)

    if charts_synced > 0:
        print(f"\n[bold green]Charts synced: {charts_synced}[/bold green]")
    if dods_synced > 0:
        print(f"[bold green]DoDs synced: {dods_synced}[/bold green]")
    if charts_synced == 0 and dods_synced == 0:
        print("\n[bold green]No charts or DoDs need to be synced[/bold green]")


def _is_commit_sha(source: str) -> bool:
    return re.match(r"[0-9a-f]{40}", source) is not None


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
        send_slack_message(channel="#data-architecture-github", message=message)


def _notify_slack_chart_create(source_chart_id: int, source: str, dry_run: bool) -> None:
    message = f"""
:warning: *ETL chart-sync: Pending New Chart Not Synced* from `{source}`
<http://{get_container_name(source)}/admin/charts/{source_chart_id}/edit|View Staging Chart>
    """.strip()

    print(message)

    if config.SLACK_API_TOKEN and not dry_run:
        send_slack_message(channel="#data-architecture-github", message=message)


def _notify_slack_dod_conflict(
    dod_name: str, source: str, source_updated_at: Any, target_updated_at: Any, server_creation_time: Any, dry_run: bool
) -> None:
    message = f"""
:warning: *ETL chart-sync: DoD Conflict Not Synced* from `{source}`
DoD "{dod_name}" has been updated in production after staging server was created.
*Staging Updated*: {str(source_updated_at)} UTC
*Production Updated*: {str(target_updated_at)} UTC
*Staging Created*: {str(server_creation_time)} UTC
    """.strip()

    print(message)

    if config.SLACK_API_TOKEN and not dry_run:
        send_slack_message(channel="#data-architecture-github", message=message)


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


def _prune_chart_config(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config)
    config = {k: v for k, v in config.items() if k not in ("version",)}
    for dim in config.get("dimensions", []):
        dim.pop("variableId", None)
    return config


def _chart_config_diff(
    source_config: Dict[str, Any], target_config: Dict[str, Any], tabs: int = 1, color: bool = True
) -> str:
    return _dict_diff(
        _prune_chart_config(source_config), _prune_chart_config(target_config), tabs=tabs, color=color, width=500
    )


def _sync_dods(
    source_session: Session,
    target_session: Session,
    target_api: AdminAPI | None,
    dry_run: bool,
    server_creation_time: Any,
) -> int:
    """Sync DoDs from source to target."""
    dods_synced = 0

    # DoDs were added on 2025-05-16
    server_creation_time = max(server_creation_time, pd.to_datetime(dt.date(2025, 5, 18)))

    # Get DoDs from source with updatedAt timestamp higher than staging server creation time
    source_dods = source_session.query(gm.DoD).filter(gm.DoD.updatedAt > server_creation_time).all()

    log.info("dod_sync.start", n=len(source_dods), dod_ids=[dod.id for dod in source_dods])

    for source_dod in source_dods:
        # Check if DoD exists in target
        target_dod = target_session.query(gm.DoD).filter(gm.DoD.name == source_dod.name).first()

        if target_dod:
            # First check if content matches - if yes, no need to sync
            if source_dod.content == target_dod.content and source_dod.updatedAt <= target_dod.updatedAt:
                log.info("dod_sync.skip", name=source_dod.name, reason="no changes detected")
                continue

            # Content differs, check if target DoD was updated after staging server creation time
            if target_dod.updatedAt > server_creation_time:
                log.warning(
                    "dod_sync.production_conflict",
                    name=source_dod.name,
                    dod_id=source_dod.id,
                    source_updatedAt=str(source_dod.updatedAt),
                    target_updatedAt=str(target_dod.updatedAt),
                    staging_created_at=str(server_creation_time),
                )
                _notify_slack_dod_conflict(
                    source_dod.name,
                    str(source_session.bind.url).split("@")[-1],  # type: ignore
                    source_dod.updatedAt,
                    target_dod.updatedAt,
                    server_creation_time,
                    dry_run,
                )
                continue

            # DoD exists and needs updating (no conflict)
            log.info("dod_sync.update", name=source_dod.name, dod_id=source_dod.id)
            dods_synced += 1

            if not dry_run and target_api:
                target_api.update_dod(target_dod.id, source_dod.content, source_dod.lastUpdatedUserId)
        else:
            # DoD doesn't exist, create it
            log.info("dod_sync.create", name=source_dod.name, dod_id=source_dod.id)
            dods_synced += 1

            if not dry_run and target_api:
                target_api.create_dod(source_dod.name, source_dod.content, source_dod.lastUpdatedUserId)

    return dods_synced


if __name__ == "__main__":
    cli()
