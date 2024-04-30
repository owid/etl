import datetime as dt
import time
from typing import Any, Dict, List, Literal, Optional, get_args

import click
import structlog
from github import Auth, Github
from rich import print
from rich_click.rich_command import RichCommand

from apps.owidbot import chart_diff, data_diff, grapher
from apps.staging_sync.cli import _get_container_name
from etl import config

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

    repo, branch = repo_branch.split("/", 1)

    if repo not in get_args(REPOS):
        raise AssertionError("Invalid repo")

    pr = get_pr(repo, branch)
    comment = get_comment_from_pr(pr)

    # prefill services from existing PR comment
    if comment:
        services_body = services_from_comment(comment)
    else:
        services_body = {}

    # recalculate services
    for service in services:
        if service == "data-diff":
            services_body["data_diff"] = data_diff.run(include)
        elif service == "chart-diff":
            services_body["chart_diff"] = chart_diff.run(branch)
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
        else:
            comment.edit(body=body)


def services_from_comment(comment: Any) -> Dict[str, str]:
    services = {}

    for service_name in get_args(SERVICES):
        if f"{service_name}-start" in comment.body:
            services[service_name] = (
                comment.body.split(f"<!--{service_name}-start-->")[1].split(f"<!--{service_name}-end-->")[0].strip()
            )

    return services


def create_comment_body(branch: str, services: Dict[str, str], start_time: float):
    container_name = _get_container_name(branch) if branch else "dry-run"

    body = f"""
<b>Quick links (staging server)</b>:
[Site](http://{container_name}/) | [Admin](http://{container_name}/admin/login) | [Wizard](http://{container_name}/etl/wizard/)
|--------------------------------|---|---|
**Login**: `ssh owid@{container_name}`

<!--grapher-start-->
{services.get('grapher', '')}
<!--grapher-end-->
<!--chart-diff-start-->
{services.get('chart_diff', '')}
<!--chart-diff-end-->
<!--data-diff-start-->
{services.get('data_diff', '')}
<!--data-diff-end-->

_Edited: {dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")} UTC_
_Execution time: {time.time() - start_time:.2f} seconds_
    """.strip()

    return body


def get_pr(repo_name: str, branch_name: str) -> Any:
    assert config.OWIDBOT_ACCESS_TOKEN
    auth = Auth.Token(config.OWIDBOT_ACCESS_TOKEN)
    g = Github(auth=auth)

    repo = g.get_repo(f"owid/{repo_name}")

    # Find pull requests for the branch (assuming you're looking for open PRs)
    pulls = repo.get_pulls(state="open", sort="created", head=f"{repo.owner.login}:{branch_name}")
    pulls = list(pulls)

    if len(pulls) == 0:
        raise AssertionError(f"No open PR found for branch {branch_name}")
    elif len(pulls) > 1:
        raise AssertionError(f"More than one open PR found for branch {branch_name}")

    return pulls[0]


def get_comment_from_pr(pr: Any) -> Optional[Any]:
    comments = pr.get_issue_comments()

    owidbot_comments = [comment for comment in comments if comment.user.login == "owidbot"]

    if len(owidbot_comments) == 0:
        return None
    elif len(owidbot_comments) == 1:
        return owidbot_comments[0]
    else:
        raise AssertionError("More than one owidbot comment found.")
