import datetime as dt
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Literal, get_args

import click
import structlog
from github import UnknownObjectException
from rich import print
from rich_click.rich_command import RichCommand

from apps.owidbot import anomalist, chart_diff, data_diff, grapher, metadata_diff
from etl.config import OWIDBOT_ACCESS_TOKEN, get_container_name
from etl.git_api_helpers import GithubApiRepo

# Cloudflare Pages subdomain limit (see https://community.cloudflare.com/t/algorithm-to-generate-a-preview-dns-subdomain-from-a-branch-name/477633)
CLOUDFLARE_SUBDOMAIN_LIMIT = 28

# Every status comment written by create_comment_body carries this marker; other comments posted as
# owidbot don't, so they are never edited or deleted.
STATUS_COMMENT_MARKER = "<!--grapher-start-->"


def get_cloudflare_subdomain(branch_name: str) -> str:
    """Get the Cloudflare Pages subdomain for a branch.

    Cloudflare normalizes branch names by replacing special characters with dashes
    and truncating to 28 characters.
    """
    # Replace special characters with dashes (same as _normalise_branch in etl/config.py)
    subdomain = re.sub(r"[\/\._]", "-", branch_name)
    # Truncate to Cloudflare's limit and remove trailing hyphens
    return subdomain[:CLOUDFLARE_SUBDOMAIN_LIMIT].rstrip("-")


log = structlog.get_logger()

REPOS = Literal["etl", "owid-grapher"]
SERVICES = Literal["data-diff", "chart-diff", "metadata-diff", "grapher", "anomalist"]


@click.command("owidbot", cls=RichCommand, help=__doc__)
@click.argument("repo_branch", type=str)
# @click.option("--services", type=click.Choice(get_args(SERVICES)), multiple=True)
@click.option("--services", type=str, multiple=True)
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
    services: list[str],
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

    repo = GithubApiRepo(repo_name=repo_name, access_token=OWIDBOT_ACCESS_TOKEN)
    pr = repo.get_pr(branch)
    if pr is None:
        log.warning(f"No open PR found for branch {branch}")
        return

    # Recalculate services in parallel: they are independent and spend their time waiting on
    # databases, the catalog and GitHub, so the run takes as long as the slowest one.
    def run_service(service: str) -> str | None:
        if service == "data-diff":
            return data_diff.run(include, branch=branch)

        elif service == "chart-diff":
            charts_df = chart_diff.call_chart_diff(branch)
            # update github check run
            chart_diff.create_check_run(repo_name, branch, charts_df, dry_run)
            return chart_diff.run(branch, charts_df)

        elif service == "metadata-diff":
            # No check run on purpose: metadata ships through ETL on merge and gates nothing, so a
            # red check here would be noise rather than a signal.
            return metadata_diff.run(branch, metadata_diff.call_metadata_diff(branch))

        elif service == "grapher":
            return grapher.run(branch, head_sha=pr.head.sha)

        elif service == "anomalist":
            # TODO: anomalist could post a summary of anomalies to the PR
            anomalist.run(branch)
            return None

        else:
            # We raise a warning instead of an error to make it backward compatible on old
            # staging servers when adding a new service.
            log.warning("Invalid service", service=service)
            return None

    services_body = {}
    if services:
        with ThreadPoolExecutor(max_workers=len(services)) as executor:
            futures = {service: executor.submit(run_service, service) for service in services}
        for service, future in futures.items():
            body = future.result()
            if body is not None:
                services_body[service] = body

    # get existing status comments (do this as late as possible to avoid race conditions)
    comments = [c for c in repo.get_comments_from_pr(pr) if STATUS_COMMENT_MARKER in c.body]
    comment = comments[0] if comments else None

    # fill in services from existing comments if not run via --services to avoid deleting them.
    # Concurrent runs can each create a comment, so read them all, newest first; every comment
    # carries every section, so skip empty ones or they'd hide another comment's content.
    for existing in reversed(comments):
        for service, content in services_from_comment(existing).items():
            if content:
                services_body.setdefault(service, content)

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

    # Keep the oldest comment and delete duplicates, only once their content is merged and posted.
    if len(comments) > 1:
        log.warning("Found duplicate owidbot comments.", branch=branch, duplicates=len(comments) - 1)
        if not dry_run:
            for duplicate in comments[1:]:
                try:
                    duplicate.delete()
                except UnknownObjectException:
                    # A concurrent run already deleted it.
                    pass


def strip_appendix(body: str) -> str:
    """Strip variable parts of the body so that we can compare two different comments."""
    # replace line with _Edited: 2024-05-06 11:21:29 UTC_
    body = re.sub(r"_Edited:.*UTC_", "", body)

    # replace line with _Execution time: 1.78 seconds_
    body = re.sub(r"_Execution time:.*seconds_", "", body)

    return body


def services_from_comment(comment: Any) -> dict[str, str]:
    services = {}

    for service_name in get_args(SERVICES):
        if f"{service_name}-start" in comment.body:
            services[service_name] = (
                comment.body.split(f"<!--{service_name}-start-->")[1].split(f"<!--{service_name}-end-->")[0].strip()
            )

    return services


def create_comment_body(branch: str, services: dict[str, str], start_time: float):
    container_name = get_container_name(branch) if branch else "dry-run"
    cloudflare_subdomain = get_cloudflare_subdomain(branch) if branch else "dry-run"

    body = f"""
<b>Quick links (staging server)</b>:
[Site Dev](http://{container_name}/) | [Site Preview](https://{cloudflare_subdomain}.owid.pages.dev/) | [Admin](http://{container_name}/admin) | [Wizard](http://{container_name}/etl/wizard/) | [Docs](http://{container_name}/etl/docs/) | [Docs Preview](https://{cloudflare_subdomain}.owid-etl-docs.pages.dev/projects/etl/)
|--------------------------------|----------------------------------|---|---|---|---|

**Login**: `ssh owid@{container_name}`

<!--grapher-start-->
{services.get("grapher", "")}
<!--grapher-end-->
<!--chart-diff-start-->
{services.get("chart-diff", "")}
<!--chart-diff-end-->
<!--metadata-diff-start-->
{services.get("metadata-diff", "")}
<!--metadata-diff-end-->
<!--data-diff-start-->
{services.get("data-diff", "")}
<!--data-diff-end-->

_Edited: {dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")} UTC_
_Execution time: {time.time() - start_time:.2f} seconds_
    """.strip()

    return body
