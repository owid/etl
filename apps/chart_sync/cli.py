import copy
import re
from typing import Any, Dict, Optional

import click
import requests
import structlog
from rich import print
from rich_click.rich_command import RichCommand
from slack_sdk import WebClient
from sqlalchemy.orm import Session

from apps.chart_sync.admin_api import AdminAPI
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiff, ChartDiffsLoader
from apps.wizard.utils import get_staging_creation_time
from etl import config
from etl import grapher_model as gm
from etl.config import OWIDEnv, get_container_name
from etl.datadiff import _dict_diff

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
def cli(
    source: str,
    target: str,
    chart_id: Optional[int],
    include: Optional[str],
    exclude: Optional[str],
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

    # go through Admin API as creating / updating chart has side effects like
    # adding entries to chart_dimensions. We can't directly update it in MySQL
    target_api: AdminAPI = AdminAPI(target_engine) if not dry_run else None  # type: ignore

    # Get all chart diffs between source and target
    cd_loader = ChartDiffsLoader(source_engine, target_engine)
    chart_diffs = cd_loader.get_diffs(config=True, metadata=False, data=False)

    if chart_id:
        chart_diffs = [cd for cd in chart_diffs if cd.source_chart.id == chart_id]

    with Session(source_engine) as source_session:
        with Session(target_engine) as target_session:
            # Get staging server creation time
            SERVER_CREATION_TIME = get_staging_creation_time(source_session)

            chart_ids = [cd.source_chart.id for cd in chart_diffs]
            log.info("chart_sync.start", n=len(chart_diffs), chart_ids=chart_ids)

            charts_synced = 0

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
                            staging_created_at=SERVER_CREATION_TIME,
                        )
                        _notify_slack_chart_update(chart_id, str(source), diff, dry_run)
                    else:
                        raise ValueError("Invalid chart diff state")

                # Chart is new, create it
                else:
                    chart_tags = diff.source_chart.tags(source_session)

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

    if charts_synced > 0:
        print(f"\n[bold green]Charts synced: {charts_synced}[/bold green]")
    else:
        print("\n[bold green]No charts synced[/bold green]")


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


if __name__ == "__main__":
    cli()
