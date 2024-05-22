import datetime as dt
import re
import time
from typing import Any, Dict, List, Literal, get_args

import click
import structlog
from rich import print
from rich_click.rich_command import RichCommand

from apps.owidbot import chart_diff, data_diff, grapher
from apps.wizard.utils.env import get_container_name

from . import github_utils as gh_utils

log = structlog.get_logger()

REPOS = Literal["etl", "owid-grapher"]
SERVICES = Literal["data-diff", "chart-diff", "grapher"]


@click.command(cls=RichCommand, help=__doc__)
@click.argument("repo_branch", type=str)
@click.option("--services", type=click.Choice(get_args(SERVICES)), multiple=True)
@click.option(
    "--include",
    type=str,
    default="garden",
    help="Include datasets matching this regex.",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Print to console, do not post to Github.",
)
def cli(
    repo_branch: str,
    services: List[Literal[SERVICES]],
    include: str,
    dry_run: bool,
) -> None:
    """Post result of `etl diff` to Github PR.

    Example:

    ```
    $ etl owidbot etl/my-branch --services data-diff chart-diff --dry-run
    $ etl owidbot owid-grapher/my-branch --services grapher --dry-run
    ```
    """
    start_time = time.time()

    repo_name, branch = repo_branch.split("/", 1)

    if repo_name not in get_args(REPOS):
        raise AssertionError("Invalid repo")

    repo = gh_utils.get_repo(repo_name)
    pr = gh_utils.get_pr(repo, branch)

    comment = gh_utils.get_comment_from_pr(pr)

    # prefill services from existing PR comment
    if comment:
        services_body = services_from_comment(comment)
    else:
        services_body = {}

    # recalculate services
    for service in services:
        if service == "data-diff":
            services_body["data-diff"] = data_diff.run(include)

        elif service == "chart-diff":
            charts_df = chart_diff.call_chart_diff(branch)
            services_body["chart-diff"] = chart_diff.run(branch, charts_df)

            # update github check run
            chart_diff.create_check_run(repo_name, branch, charts_df, dry_run)

        elif service == "grapher":
            services_body["grapher"] = grapher.run(branch)
        else:
            raise AssertionError("Invalid service")

    body = create_comment_body(branch, services_body, start_time)

    if dry_run:
        print(body)
    else:
        if not comment:
            pr.create_issue_comment(body=body)
        elif strip_appendix(comment.body) == strip_appendix(body):
            log.info("No changes detected, skipping.", repo=repo, branch=branch, services=services)
        else:
            log.info("Updating comment.", repo=repo, branch=branch, services=services)
            comment.edit(body=body)


def strip_appendix(body: str) -> str:
    """Strip variable parts of the body so that we can compare two different comments."""
    # replace line with _Edited: 2024-05-06 11:21:29 UTC_
    body = re.sub(r"_Edited:.*UTC_", "", body)

    # replace line with _Execution time: 1.78 seconds_
    body = re.sub(r"_Execution time:.*seconds_", "", body)

    return body


def services_from_comment(comment: Any) -> Dict[str, str]:
    services = {}

    for service_name in get_args(SERVICES):
        if f"{service_name}-start" in comment.body:
            services[service_name] = (
                comment.body.split(f"<!--{service_name}-start-->")[1].split(f"<!--{service_name}-end-->")[0].strip()
            )

    return services


def create_comment_body(branch: str, services: Dict[str, str], start_time: float):
    container_name = get_container_name(branch) if branch else "dry-run"

    body = f"""
<b>Quick links (staging server)</b>:
[Site](http://{container_name}/) | [Admin](http://{container_name}/admin/login) | [Wizard](http://{container_name}/etl/wizard/)
|--------------------------------|---|---|

**Login**: `ssh owid@{container_name}`

<!--grapher-start-->
{services.get('grapher', '')}
<!--grapher-end-->
<!--chart-diff-start-->
{services.get('chart-diff', '')}
<!--chart-diff-end-->
<!--data-diff-start-->
{services.get('data-diff', '')}
<!--data-diff-end-->

_Edited: {dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")} UTC_
_Execution time: {time.time() - start_time:.2f} seconds_
    """.strip()

    return body
